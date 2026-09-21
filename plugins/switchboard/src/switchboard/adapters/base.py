from __future__ import annotations

import json
import subprocess
from typing import Any

from .. import core
from ..db import Database
from ..process import run_bounded


class AdapterError(ValueError):
    pass


def validate_snapshot(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise AdapterError("adapter output must be one JSON object")
    adapter = value.get("adapter")
    space = value.get("space")
    sources = value.get("sources")
    events = value.get("events")
    if not isinstance(adapter, str) or not adapter:
        raise AdapterError("adapter output requires a non-empty adapter")
    if not isinstance(space, dict) or not isinstance(space.get("id"), str):
        raise AdapterError("adapter output requires space.id")
    if not isinstance(sources, list) or not isinstance(events, list):
        raise AdapterError("adapter output requires sources and events arrays")
    for source in sources:
        if not isinstance(source, dict) or not all(
            isinstance(source.get(field), str) and source.get(field)
            for field in ("id", "kind", "state")
        ):
            raise AdapterError("every source requires string id, kind, and state")
    source_ids = {source["id"] for source in sources}
    for event in events:
        if not isinstance(event, dict) or not all(
            isinstance(event.get(field), str) and event.get(field)
            for field in ("source_id", "external_id", "event_type")
        ):
            raise AdapterError("every event requires source_id, external_id, and event_type")
        if event["source_id"] not in source_ids:
            raise AdapterError(f"event names undiscovered source: {event['source_id']}")
        if not isinstance(event.get("attributes", {}), dict):
            raise AdapterError("event attributes must be a JSON object")
    return value


def apply_snapshot(
    db: Database, snapshot: dict[str, Any], *, run_id: str | None = None
) -> dict[str, Any]:
    try:
        snapshot = validate_snapshot(snapshot)
    except Exception as exc:
        if run_id:
            core.finish_adapter_run(db, run_id, state="failed", detail=str(exc))
        raise
    run = {"id": run_id} if run_id else core.start_adapter_run(db, snapshot["adapter"])
    created_sources = 0
    emitted_events = 0
    deduplicated_events = 0
    try:
        space = snapshot["space"]
        core.ensure_space(db, space["id"], space.get("name"))
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
        results = []
        for event in snapshot["events"]:
            result = core.emit_event(
                db,
                source_id=event["source_id"],
                external_id=event["external_id"],
                event_type=event["event_type"],
                occurred_at=event.get("occurred_at"),
                attributes=event.get("attributes") or {},
            )
            deduplicated_events += int(result["deduplicated"])
            emitted_events += int(not result["deduplicated"])
            results.append(result)
        terminal = core.finish_adapter_run(
            db,
            run["id"],
            state="completed",
            discovered_sources=len(snapshot["sources"]),
            emitted_events=emitted_events,
            deduplicated_events=deduplicated_events,
        )
        return {
            "run": terminal,
            "created_sources": created_sources,
            "events": results,
        }
    except Exception as exc:
        core.finish_adapter_run(
            db,
            run["id"],
            state="failed",
            discovered_sources=len(snapshot.get("sources") or []),
            emitted_events=emitted_events,
            deduplicated_events=deduplicated_events,
            detail=str(exc),
        )
        raise


def run_command_adapter(
    db: Database,
    command: list[str],
    *,
    adapter_name: str = "external",
    timeout: int = 120,
    runner: Any = run_bounded,
) -> dict[str, Any]:
    if not command or not all(isinstance(part, str) and part for part in command):
        raise AdapterError("adapter command must be a non-empty JSON array of strings")
    run = core.start_adapter_run(db, adapter_name)
    try:
        result = runner(command, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        core.finish_adapter_run(db, run["id"], state="failed", detail=str(exc))
        raise AdapterError(f"adapter command failed: {exc}") from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout)[-2000:]
        core.finish_adapter_run(db, run["id"], state="failed", detail=detail)
        raise AdapterError(f"adapter command exited {result.returncode}: {detail}")
    try:
        snapshot = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        core.finish_adapter_run(db, run["id"], state="failed", detail=str(exc))
        raise AdapterError(f"adapter output is not valid JSON: {exc}") from exc
    if isinstance(snapshot, dict) and snapshot.get("adapter") != adapter_name:
        core.finish_adapter_run(
            db,
            run["id"],
            state="failed",
            detail=f"adapter identity mismatch: expected {adapter_name}",
        )
        raise AdapterError(f"adapter identity mismatch: expected {adapter_name}")
    return apply_snapshot(db, snapshot, run_id=run["id"])
