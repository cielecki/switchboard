from __future__ import annotations

import fcntl
import os
import signal
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any, TextIO

from . import core
from .adapters import run_inbound_leads, run_ingest_shadow
from .db import Database
from .web import handler_for


class SupervisorAlreadyRunning(ValueError):
    pass


@contextmanager
def supervisor_lock(db: Database) -> Iterator[TextIO]:
    lock_path = Path(f"{db.path}.supervisor.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    handle = lock_path.open("a+")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SupervisorAlreadyRunning(f"supervisor already holds {lock_path}") from exc
        handle.seek(0)
        handle.truncate()
        handle.write(f"{os.getpid()}\n")
        handle.flush()
        yield handle
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _delivery_is_due(delivery: dict[str, Any], at: datetime, retry_seconds: int) -> bool:
    attempts = delivery.get("attempts") or []
    if not attempts:
        return True
    latest = attempts[-1]
    finished = latest.get("finished_at") or latest.get("started_at")
    return finished is None or _parse_timestamp(finished) <= at - timedelta(seconds=retry_seconds)


def run_cycle(
    db: Database,
    *,
    relay: str | Path | None,
    cli_command: list[str],
    activate_inactive: bool = False,
    delivery_timeout: int = 30,
    delivery_retry_seconds: int = 60,
    at: datetime | None = None,
    ingest_runner: Callable[..., dict[str, Any]] = run_ingest_shadow,
    inbound_runner: Callable[..., dict[str, Any]] = run_inbound_leads,
    delivery_runner: Callable[..., dict[str, Any]] = core.dispatch_delivery,
) -> dict[str, Any]:
    cycle_at = at or datetime.now(UTC)
    cycle_timestamp = cycle_at.isoformat()
    schedule_results: list[dict[str, Any]] = []
    delivery_results: list[dict[str, Any]] = []
    errors: list[str] = []

    for schedule in core.due_schedules(db, cycle_timestamp):
        core.mark_schedule_started(db, schedule["id"], cycle_timestamp)
        try:
            if schedule["adapter"] == "ingest-shadow":
                config = schedule["config"]
                result = ingest_runner(
                    db,
                    status_script=config["status_script"],
                    space_id=config["space_id"],
                    timeout=config["timeout"],
                )
            elif schedule["adapter"] == "inbound-leads":
                config = schedule["config"]
                result = inbound_runner(
                    db,
                    ledger_script=config["ledger_script"],
                    profile=config["profile"],
                    space_id=config["space_id"],
                    discovery_script=config.get("discovery_script"),
                    timeout=config["timeout"],
                )
            else:
                raise ValueError(f"unsupported scheduled adapter: {schedule['adapter']}")
            terminal = core.mark_schedule_finished(
                db, schedule["id"], state="completed", finished_at=core.now()
            )
            schedule_results.append(
                {"id": schedule["id"], "state": "completed", "run": result["run"], "schedule": terminal}
            )
        except Exception as exc:  # noqa: BLE001 - isolate one source from the daemon
            detail = str(exc)
            terminal = core.mark_schedule_finished(
                db,
                schedule["id"],
                state="failed",
                error=detail,
                finished_at=core.now(),
            )
            schedule_results.append({"id": schedule["id"], "state": "failed", "error": detail, "schedule": terminal})
            errors.append(f"schedule {schedule['id']}: {detail}")

    if relay is not None:
        for pending in core.list_deliveries(db, "pending"):
            delivery = core.get_delivery(db, pending["id"])
            if not _delivery_is_due(delivery, cycle_at, delivery_retry_seconds):
                continue
            try:
                result = delivery_runner(
                    db,
                    delivery["id"],
                    relay=relay,
                    cli_command=cli_command,
                    activate_inactive=activate_inactive,
                    timeout=delivery_timeout,
                )
                delivery_results.append(result)
            except Exception as exc:  # noqa: BLE001 - retryable delivery failure
                detail = str(exc)
                delivery_results.append(
                    {"id": delivery["id"], "state": "failed", "error": detail}
                )
                errors.append(f"delivery {delivery['id']}: {detail}")

    return {
        "at": cycle_timestamp,
        "schedules": schedule_results,
        "deliveries": delivery_results,
        "errors": errors,
    }


def run_once(
    db: Database,
    *,
    relay: str | Path | None,
    cli_command: list[str],
    activate_inactive: bool = False,
    delivery_timeout: int = 30,
    delivery_retry_seconds: int = 60,
) -> dict[str, Any]:
    with supervisor_lock(db):
        core.recover_interrupted_runs(db)
        started_at = core.now()
        core.update_supervisor_state(
            db,
            state="running",
            pid=os.getpid(),
            started_at=started_at,
            heartbeat_at=started_at,
            dispatch_enabled=relay is not None,
        )
        result = run_cycle(
            db,
            relay=relay,
            cli_command=cli_command,
            activate_inactive=activate_inactive,
            delivery_timeout=delivery_timeout,
            delivery_retry_seconds=delivery_retry_seconds,
        )
        stopped_at = core.now()
        core.update_supervisor_state(
            db,
            state="stopped",
            pid=None,
            heartbeat_at=stopped_at,
            stopped_at=stopped_at,
            dispatch_enabled=relay is not None,
            last_cycle_at=result["at"],
            last_error="; ".join(result["errors"]) or None,
        )
        return result


def run_forever(
    db: Database,
    *,
    relay: str | Path | None,
    cli_command: list[str],
    host: str = "127.0.0.1",
    port: int = 8765,
    poll_seconds: int = 5,
    activate_inactive: bool = False,
    delivery_timeout: int = 30,
    delivery_retry_seconds: int = 60,
    stop_event: threading.Event | None = None,
) -> None:
    if poll_seconds < 1:
        raise ValueError("poll interval must be at least one second")
    stop = stop_event or threading.Event()
    with supervisor_lock(db):
        core.recover_interrupted_runs(db)
        server = ThreadingHTTPServer((host, port), handler_for(db))
        web_url = f"http://{host}:{server.server_address[1]}"
        web_thread = threading.Thread(target=server.serve_forever, daemon=True)
        web_thread.start()

        if threading.current_thread() is threading.main_thread():
            def request_stop(_signum: int, _frame: object) -> None:
                stop.set()

            signal.signal(signal.SIGINT, request_stop)
            signal.signal(signal.SIGTERM, request_stop)

        started_at = core.now()
        core.update_supervisor_state(
            db,
            state="running",
            pid=os.getpid(),
            started_at=started_at,
            heartbeat_at=started_at,
            web_url=web_url,
            dispatch_enabled=relay is not None,
        )
        print(f"Switchboard supervisor: {web_url}", flush=True)
        last_cycle_at: str | None = None
        last_error: str | None = None
        try:
            while not stop.is_set():
                result = run_cycle(
                    db,
                    relay=relay,
                    cli_command=cli_command,
                    activate_inactive=activate_inactive,
                    delivery_timeout=delivery_timeout,
                    delivery_retry_seconds=delivery_retry_seconds,
                )
                last_cycle_at = result["at"]
                last_error = "; ".join(result["errors"]) or None
                core.update_supervisor_state(
                    db,
                    state="running",
                    pid=os.getpid(),
                    heartbeat_at=core.now(),
                    web_url=web_url,
                    dispatch_enabled=relay is not None,
                    last_cycle_at=last_cycle_at,
                    last_error=last_error,
                )
                stop.wait(poll_seconds)
        except BaseException as exc:
            core.update_supervisor_state(
                db,
                state="failed",
                pid=None,
                heartbeat_at=core.now(),
                stopped_at=core.now(),
                web_url=web_url,
                dispatch_enabled=relay is not None,
                last_cycle_at=last_cycle_at,
                last_error=str(exc),
            )
            raise
        finally:
            server.shutdown()
            server.server_close()
            web_thread.join(timeout=5)
            if core.supervisor_status(db)["state"] != "failed":
                stopped_at = core.now()
                core.update_supervisor_state(
                    db,
                    state="stopped",
                    pid=None,
                    heartbeat_at=stopped_at,
                    stopped_at=stopped_at,
                    web_url=web_url,
                    dispatch_enabled=relay is not None,
                    last_cycle_at=last_cycle_at,
                    last_error=last_error,
                )
