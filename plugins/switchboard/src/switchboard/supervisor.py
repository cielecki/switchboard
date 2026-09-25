from __future__ import annotations

import fcntl
import json
import os
import signal
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from http.server import ThreadingHTTPServer
from pathlib import Path
from queue import Empty, SimpleQueue
from typing import Any, TextIO

from . import core
from .adapters import run_inbound_leads, run_ingest_shadow, run_stream_command, run_timer
from .db import Database
from .process import run_bounded, terminate_active_process_groups
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
    attempts = delivery.get("current_attempts", delivery.get("attempts")) or []
    if not attempts:
        return True
    latest = attempts[-1]
    finished = latest.get("finished_at") or latest.get("started_at")
    return finished is None or _parse_timestamp(finished) <= at - timedelta(seconds=retry_seconds)


def _processor_delivery_issue(
    delivery: dict[str, Any], at: datetime, alert_after_seconds: int
) -> str | None:
    if not delivery.get("binding") or delivery["binding"]["state"] != "enabled":
        return None
    threshold = at - timedelta(seconds=alert_after_seconds)
    run = delivery["run"]
    if delivery["state"] == "accepted" and run["state"] == "pending":
        accepted_at = delivery.get("accepted_at")
        if accepted_at and _parse_timestamp(accepted_at) <= threshold:
            return "wake accepted but the processor run was not claimed"
    if delivery["state"] == "pending":
        attempts = delivery.get("current_attempts", delivery.get("attempts")) or []
        if attempts and attempts[-1]["state"] == "failed":
            finished = attempts[-1].get("finished_at") or attempts[-1]["started_at"]
            if _parse_timestamp(finished) <= threshold:
                return "chat delivery has remained unreachable"
    return None


def _run_alert_command(command: list[str], payload: dict[str, Any]) -> dict[str, Any]:
    result = run_bounded(
        command,
        input=json.dumps(payload, ensure_ascii=False),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if result.returncode != 0:
        raise ValueError(
            f"alert command exited {result.returncode}: "
            + (result.stderr or result.stdout)[-2000:]
        )
    return {"stdout": result.stdout[-2000:]}


def _execute_schedule(
    db: Database,
    schedule: dict[str, Any],
    *,
    ingest_runner: Callable[..., dict[str, Any]] = run_ingest_shadow,
    inbound_runner: Callable[..., dict[str, Any]] = run_inbound_leads,
    timer_runner: Callable[..., dict[str, Any]] = run_timer,
    stream_runner: Callable[..., dict[str, Any]] = run_stream_command,
    started_at: str | None = None,
) -> dict[str, Any]:
    if schedule.get("schedule_kind") == "calendar":
        try:
            return core.execute_due_calendar_schedule(
                db, schedule["id"], triggered_at=started_at or core.now()
            )
        except Exception as exc:  # noqa: BLE001 - one source must not stop the coordinator
            detail = str(exc)
            terminal = core.mark_calendar_schedule_failed(
                db, schedule["id"], detail, failed_at=core.now()
            )
            return {
                "id": schedule["id"],
                "state": "failed",
                "error": detail,
                "schedule": terminal,
            }
    core.mark_schedule_started(db, schedule["id"], started_at or core.now())
    try:
        config = schedule["config"]
        if schedule["adapter"] == "ingest-shadow":
            result = ingest_runner(
                db,
                status_script=config["status_script"],
                discovery_script=config.get("discovery_script"),
                space_id=config["space_id"],
                timeout=config["timeout"],
            )
        elif schedule["adapter"] == "inbound-leads":
            result = inbound_runner(
                db,
                ledger_script=config["ledger_script"],
                profile=config["profile"],
                space_id=config["space_id"],
                discovery_script=config.get("discovery_script"),
                slack_discovery_script=config.get("slack_discovery_script"),
                source_mode=config.get("source_mode", "both"),
                timeout=config["timeout"],
            )
        elif schedule["adapter"] == "timer":
            result = timer_runner(
                db,
                space_id=config["space_id"],
                source_id=config["source_id"],
                event_type=config["event_type"],
                scheduled_for=schedule["next_run_at"],
                attributes=config.get("attributes") or {},
            )
        elif schedule["adapter"] == "command-stream":
            result = stream_runner(
                db,
                command=config["command"],
                environment=config.get("environment") or {},
            )
        else:
            raise ValueError(f"unsupported scheduled adapter: {schedule['adapter']}")
        terminal = core.mark_schedule_finished(
            db, schedule["id"], state="completed", finished_at=core.now()
        )
        return {
            "id": schedule["id"],
            "state": "completed",
            "run": result["run"],
            "schedule": terminal,
        }
    except Exception as exc:  # noqa: BLE001 - one source must not stop the coordinator
        detail = str(exc)
        terminal = core.mark_schedule_finished(
            db,
            schedule["id"],
            state="failed",
            error=detail,
            finished_at=core.now(),
        )
        return {
            "id": schedule["id"],
            "state": "failed",
            "error": detail,
            "schedule": terminal,
        }


class ScheduleWorkers:
    """Run persistent schedules independently from the coordinator's delivery loop."""

    def __init__(
        self,
        db: Database,
        *,
        ingest_runner: Callable[..., dict[str, Any]] = run_ingest_shadow,
        inbound_runner: Callable[..., dict[str, Any]] = run_inbound_leads,
        timer_runner: Callable[..., dict[str, Any]] = run_timer,
        stream_runner: Callable[..., dict[str, Any]] = run_stream_command,
    ) -> None:
        self.db = db
        self.ingest_runner = ingest_runner
        self.inbound_runner = inbound_runner
        self.timer_runner = timer_runner
        self.stream_runner = stream_runner
        self._active: dict[str, threading.Thread] = {}
        self._active_lock = threading.Lock()
        self._results: SimpleQueue[dict[str, Any]] = SimpleQueue()

    def _run(self, schedule: dict[str, Any], started_at: str) -> None:
        try:
            self._results.put(
                _execute_schedule(
                    self.db,
                    schedule,
                    ingest_runner=self.ingest_runner,
                    inbound_runner=self.inbound_runner,
                    timer_runner=self.timer_runner,
                    stream_runner=self.stream_runner,
                    started_at=started_at,
                )
            )
        finally:
            with self._active_lock:
                self._active.pop(schedule["id"], None)

    def poll(self, at: str | None = None) -> list[dict[str, Any]]:
        timestamp = at or core.now()
        results: list[dict[str, Any]] = []
        while True:
            try:
                results.append(self._results.get_nowait())
            except Empty:
                break
        for schedule in core.due_schedules(self.db, timestamp):
            thread = threading.Thread(
                target=self._run,
                args=(schedule, timestamp),
                name=f"switchboard-schedule-{schedule['id']}",
                daemon=True,
            )
            with self._active_lock:
                if schedule["id"] in self._active:
                    continue
                self._active[schedule["id"]] = thread
            thread.start()
            results.append({"id": schedule["id"], "state": "running"})
        return results

    def shutdown(self) -> None:
        terminate_active_process_groups()
        with self._active_lock:
            active = list(self._active.values())
        for thread in active:
            thread.join(timeout=5)


def process_processor_alerts(
    db: Database,
    *,
    at: datetime,
    alert_after_seconds: int,
    alert_command: list[str],
    alert_runner: Callable[[list[str], dict[str, Any]], dict[str, Any]] = _run_alert_command,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    timestamp = at.isoformat()
    current = {
        delivery["id"]: core.get_processor_delivery(db, delivery["id"])
        for delivery in core.list_processor_deliveries(db)
    }
    issues_by_consumer: dict[str, list[tuple[dict[str, Any], str]]] = {}
    for delivery in current.values():
        issue = _processor_delivery_issue(delivery, at, alert_after_seconds)
        if issue is None:
            continue
        issues_by_consumer.setdefault(delivery["consumer"], []).append((delivery, issue))

    for consumer, issues in sorted(issues_by_consumer.items()):
        issues.sort(key=lambda item: (item[0]["created_at"], item[0]["id"]))
        representative, issue = issues[0]
        alert_id: str | None = None
        with db.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM processor_alerts WHERE consumer=? AND state='open'",
                (consumer,),
            ).fetchone()
            if existing is None:
                alert_id = core.make_id("palert")
                connection.execute(
                    "INSERT INTO processor_alerts("
                    "id, delivery_id, generation, consumer, state, opened_at, "
                    "notification_claimed_at, last_seen_at, affected_delivery_count, detail"
                    ") VALUES(?,?,?,?,'open',?,?,?,?,?)",
                    (
                        alert_id,
                        representative["id"],
                        representative["generation"],
                        consumer,
                        timestamp,
                        timestamp,
                        timestamp,
                        len(issues),
                        issue,
                    ),
                )
                core.audit(
                    connection,
                    command="processor.alert-open",
                    entity_type="processor_alert",
                    entity_id=alert_id,
                    payload={
                        "consumer": consumer,
                        "delivery_id": representative["id"],
                        "affected_delivery_count": len(issues),
                        "detail": issue,
                    },
                    actor="supervisor",
                )
            else:
                connection.execute(
                    "UPDATE processor_alerts SET last_seen_at=?, affected_delivery_count=?, "
                    "detail=? WHERE id=?",
                    (timestamp, len(issues), issue, existing["id"]),
                )
        if alert_id is None:
            continue
        processors = sorted({item[0]["run"]["processor"] for item in issues})
        payload = {
            "kind": "processor-unreachable",
            "message": (
                f"Switchboard cannot reach {consumer}; {len(issues)} processor "
                f"delivery{' is' if len(issues) == 1 else 'ies are'} affected: {issue}."
            ),
            "consumer": consumer,
            "delivery_id": representative["id"],
            "processor_run_id": representative["processor_run_id"],
            "affected_delivery_count": len(issues),
            "processors": processors,
        }
        try:
            result = alert_runner(alert_command, payload)
            outcome = {"id": alert_id, "state": "notified", **result}
            error = None
        except Exception as exc:  # noqa: BLE001 - record once to prevent alert storms
            error = str(exc)
            outcome = {"id": alert_id, "state": "failed", "error": error}
        with db.transaction() as connection:
            connection.execute(
                "UPDATE processor_alerts SET notified_at=?, notification_error=? WHERE id=?",
                (timestamp, error, alert_id),
            )
        results.append(outcome)

    open_alerts = db.rows(
        "SELECT * FROM processor_alerts WHERE state='open' AND consumer IS NOT NULL "
        "ORDER BY opened_at"
    )
    for alert in open_alerts:
        if alert["consumer"] in issues_by_consumer or alert["recovery_claimed_at"] is not None:
            continue
        with db.transaction() as connection:
            claimed = connection.execute(
                "UPDATE processor_alerts SET recovery_claimed_at=? "
                "WHERE id=? AND state='open' AND recovery_claimed_at IS NULL",
                (timestamp, alert["id"]),
            ).rowcount
        if not claimed:
            continue
        payload = {
            "kind": "processor-recovered",
            "message": f"Switchboard recovered consumer {alert['consumer']}.",
            "consumer": alert["consumer"],
            "delivery_id": alert["delivery_id"],
        }
        try:
            result = alert_runner(alert_command, payload)
            outcome = {"id": alert["id"], "state": "recovery-notified", **result}
            error = None
        except Exception as exc:  # noqa: BLE001 - record once to prevent alert storms
            error = str(exc)
            outcome = {"id": alert["id"], "state": "recovery-failed", "error": error}
        with db.transaction() as connection:
            connection.execute(
                "UPDATE processor_alerts SET state='recovered', recovered_at=?, "
                "recovery_notified_at=?, recovery_error=? WHERE id=?",
                (timestamp, timestamp, error, alert["id"]),
            )
            core.audit(
                connection,
                command="processor.alert-recovered",
                entity_type="processor_alert",
                entity_id=alert["id"],
                payload={
                    "consumer": alert["consumer"],
                    "delivery_id": alert["delivery_id"],
                },
                actor="supervisor",
            )
        results.append(outcome)
    return results


def run_cycle(
    db: Database,
    *,
    relay: str | Path | None,
    cli_command: list[str],
    activate_inactive: bool = False,
    delivery_timeout: int = 30,
    delivery_retry_seconds: int = 60,
    accepted_retry_seconds: int = 120,
    delivery_batch_size: int = 1,
    at: datetime | None = None,
    ingest_runner: Callable[..., dict[str, Any]] = run_ingest_shadow,
    inbound_runner: Callable[..., dict[str, Any]] = run_inbound_leads,
    timer_runner: Callable[..., dict[str, Any]] = run_timer,
    delivery_runner: Callable[..., dict[str, Any]] = core.dispatch_delivery,
    processor_delivery_runner: Callable[..., dict[str, Any]] = core.dispatch_processor_delivery,
    alert_command: list[str] | None = None,
    alert_after_seconds: int = 900,
    alert_runner: Callable[[list[str], dict[str, Any]], dict[str, Any]] = _run_alert_command,
    process_schedules: bool = True,
) -> dict[str, Any]:
    if delivery_batch_size < 1:
        raise ValueError("delivery batch size must be at least one")
    cycle_at = at or datetime.now(UTC)
    cycle_timestamp = cycle_at.isoformat()
    schedule_results: list[dict[str, Any]] = []
    delivery_results: list[dict[str, Any]] = []
    processor_delivery_results: list[dict[str, Any]] = []
    alert_results: list[dict[str, Any]] = []
    errors: list[str] = []

    core.recover_expired_processor_attempts(db, cycle_timestamp)
    recovered_unacknowledged = core.recover_unacknowledged_deliveries(
        db, cycle_timestamp, after_seconds=accepted_retry_seconds
    )
    recovered_unclaimed = core.recover_unclaimed_processor_deliveries(
        db, cycle_timestamp, after_seconds=accepted_retry_seconds
    )
    core.coalesce_processor_deliveries(db)

    if process_schedules:
        for schedule in core.due_schedules(db, cycle_timestamp):
            result = _execute_schedule(
                db,
                schedule,
                ingest_runner=ingest_runner,
                inbound_runner=inbound_runner,
                timer_runner=timer_runner,
                started_at=cycle_timestamp,
            )
            schedule_results.append(result)
            if result["state"] == "failed":
                errors.append(f"schedule {schedule['id']}: {result['error']}")

    core.sync_processor_deliveries(db)
    if relay is not None:
        candidates: list[tuple[str, str, dict[str, Any]]] = []
        for pending in core.list_deliveries(db, "pending"):
            delivery = core.get_delivery(db, pending["id"])
            if _delivery_is_due(delivery, cycle_at, delivery_retry_seconds):
                candidates.append((delivery["created_at"], "wait", delivery))

        for pending in core.list_processor_deliveries(db, "pending"):
            delivery = core.get_processor_delivery(db, pending["id"])
            if (
                _delivery_is_due(delivery, cycle_at, delivery_retry_seconds)
                and not core.processor_consumer_is_busy(
                    db, delivery["consumer"], exclude_delivery=delivery["id"]
                )
                and not any(
                    item[1] == "processor"
                    and item[2]["consumer"] == delivery["consumer"]
                    for item in candidates
                )
            ):
                candidates.append((delivery["created_at"], "processor", delivery))

        candidates.sort(key=lambda candidate: (candidate[0], candidate[2]["id"]))
        for _created_at, kind, delivery in candidates[:delivery_batch_size]:
            try:
                if kind == "wait":
                    result = delivery_runner(
                        db,
                        delivery["id"],
                        relay=relay,
                        cli_command=cli_command,
                        activate_inactive=activate_inactive,
                        timeout=delivery_timeout,
                    )
                    delivery_results.append(result)
                else:
                    result = processor_delivery_runner(
                        db,
                        delivery["id"],
                        relay=relay,
                        cli_command=cli_command,
                        activate_inactive=activate_inactive,
                        timeout=delivery_timeout,
                    )
                    processor_delivery_results.append(result)
            except Exception as exc:  # noqa: BLE001 - retryable delivery failure
                detail = str(exc)
                result = {"id": delivery["id"], "state": "failed", "error": detail}
                if kind == "wait":
                    delivery_results.append(result)
                    errors.append(f"delivery {delivery['id']}: {detail}")
                else:
                    processor_delivery_results.append(result)
                    errors.append(f"processor delivery {delivery['id']}: {detail}")

    if alert_command is not None:
        alert_results = process_processor_alerts(
            db,
            at=cycle_at,
            alert_after_seconds=alert_after_seconds,
            alert_command=alert_command,
            alert_runner=alert_runner,
        )
        errors.extend(
            f"processor alert {item['id']}: {item['error']}"
            for item in alert_results
            if "error" in item
        )

    return {
        "at": cycle_timestamp,
        "schedules": schedule_results,
        "deliveries": delivery_results,
        "processor_deliveries": processor_delivery_results,
        "recovered_unacknowledged_deliveries": recovered_unacknowledged,
        "recovered_unclaimed_processor_deliveries": recovered_unclaimed,
        "alerts": alert_results,
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
    accepted_retry_seconds: int = 120,
    delivery_batch_size: int = 1,
    alert_command: list[str] | None = None,
    alert_after_seconds: int = 900,
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
            accepted_retry_seconds=accepted_retry_seconds,
            delivery_batch_size=delivery_batch_size,
            alert_command=alert_command,
            alert_after_seconds=alert_after_seconds,
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
    accepted_retry_seconds: int = 120,
    delivery_batch_size: int = 1,
    alert_command: list[str] | None = None,
    alert_after_seconds: int = 900,
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
        schedule_workers = ScheduleWorkers(db)
        try:
            while not stop.is_set():
                schedule_results = schedule_workers.poll()
                result = run_cycle(
                    db,
                    relay=relay,
                    cli_command=cli_command,
                    activate_inactive=activate_inactive,
                    delivery_timeout=delivery_timeout,
                    delivery_retry_seconds=delivery_retry_seconds,
                    accepted_retry_seconds=accepted_retry_seconds,
                    delivery_batch_size=delivery_batch_size,
                    alert_command=alert_command,
                    alert_after_seconds=alert_after_seconds,
                    process_schedules=False,
                )
                result["schedules"] = schedule_results
                result["errors"].extend(
                    f"schedule {item['id']}: {item['error']}"
                    for item in schedule_results
                    if item["state"] == "failed"
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
            schedule_workers.shutdown()
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
