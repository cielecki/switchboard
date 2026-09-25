from __future__ import annotations

import json
import os
import threading
from collections import deque
from collections.abc import Sequence
from typing import Any

from .. import core
from ..db import Database
from ..process import managed_process
from .base import AdapterError, validate_snapshot


def _apply_stream_snapshot(db: Database, snapshot: dict[str, Any]) -> tuple[int, int, int]:
    snapshot = validate_snapshot(snapshot)
    space = snapshot["space"]
    core.ensure_space(db, space["id"], space.get("name"))
    created_sources = 0
    for source in snapshot["sources"]:
        _, created = core.ensure_source(
            db,
            source["id"],
            space["id"],
            source["kind"],
            source.get("config") or {},
        )
        created_sources += int(created)
        core.record_source_health(db, source["id"], source["state"], source.get("detail", ""))

    emitted = deduplicated = 0
    for event in snapshot["events"]:
        result = core.emit_event(
            db,
            source_id=event["source_id"],
            external_id=event["external_id"],
            event_type=event["event_type"],
            occurred_at=event.get("occurred_at"),
            attributes=event.get("attributes") or {},
        )
        deduplicated += int(result["deduplicated"])
        emitted += int(not result["deduplicated"])
    return created_sources, emitted, deduplicated


def run_stream_command(
    db: Database,
    *,
    command: Sequence[str],
    environment: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Apply newline-delimited adapter snapshots from one persistent source process."""
    if not command or not os.path.isabs(command[0]):
        raise AdapterError("stream command must start with an absolute executable path")
    if not os.path.isfile(command[0]):
        raise AdapterError(f"stream executable not found: {command[0]}")
    if environment and any(not isinstance(key, str) or not isinstance(value, str)
                           for key, value in environment.items()):
        raise AdapterError("stream environment must contain string keys and values")

    run = core.start_adapter_run(db, "command-stream")
    created_sources = emitted_events = deduplicated_events = 0
    diagnostics: deque[str] = deque(maxlen=40)
    env = dict(os.environ)
    env.update(environment or {})
    try:
        with managed_process(command, env=env) as process:
            assert process.stdout is not None
            assert process.stderr is not None

            def drain_stderr() -> None:
                for line in process.stderr:
                    diagnostics.append(line.rstrip())

            stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
            stderr_thread.start()
            for line_number, line in enumerate(process.stdout, start=1):
                if not line.strip():
                    continue
                try:
                    snapshot = json.loads(line)
                    created, emitted, deduplicated = _apply_stream_snapshot(db, snapshot)
                except Exception as exc:
                    raise AdapterError(
                        f"stream output line {line_number} is not a valid adapter snapshot: {exc}"
                    ) from exc
                created_sources += created
                emitted_events += emitted
                deduplicated_events += deduplicated
            returncode = process.wait()
            stderr_thread.join(timeout=1)
            if returncode != 0:
                detail = "\n".join(diagnostics)[-2000:]
                raise AdapterError(f"stream command exited {returncode}: {detail}")

        terminal = core.finish_adapter_run(
            db,
            run["id"],
            state="completed",
            discovered_sources=created_sources,
            emitted_events=emitted_events,
            deduplicated_events=deduplicated_events,
        )
        return {"run": terminal}
    except Exception as exc:
        core.finish_adapter_run(
            db,
            run["id"],
            state="failed",
            discovered_sources=created_sources,
            emitted_events=emitted_events,
            deduplicated_events=deduplicated_events,
            detail=str(exc),
        )
        raise
