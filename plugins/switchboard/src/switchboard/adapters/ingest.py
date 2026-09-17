from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from .. import core
from ..db import Database
from .base import AdapterError, apply_snapshot


def _source_id(name: str) -> str:
    return f"ingest/{name}"


def snapshot_from_rows(rows: object, *, space_id: str = "personal-ingest") -> dict[str, Any]:
    if not isinstance(rows, list):
        raise AdapterError("ingest status output must be a JSON array")
    sources: dict[str, dict[str, Any]] = {}
    events: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise AdapterError("every ingest status row must be a JSON object")
        ingest_id = row.get("ingest_id")
        source = row.get("source")
        status = row.get("status")
        if not all(isinstance(value, str) and value for value in (ingest_id, source, status)):
            raise AdapterError("ingest rows require ingest_id, source, and status")
        source_id = _source_id(source)
        sources[source_id] = {
            "id": source_id,
            "kind": f"ingest.{source}",
            "state": "ready",
            "detail": "observed through ingest status.py --all --json",
            "config": {"adapter": "ingest-shadow", "upstream_source": source},
        }
        attributes = {
            key: row.get(key)
            for key in (
                "id",
                "ingest_id",
                "source",
                "title",
                "date",
                "status",
                "routing_note",
                "attention_reason",
                "mean_confidence",
            )
        }
        digest = hashlib.sha256(
            json.dumps(attributes, ensure_ascii=False, sort_keys=True).encode()
        ).hexdigest()[:20]
        events.append(
            {
                "source_id": source_id,
                "external_id": f"{ingest_id}:{digest}",
                "event_type": "ingest.capture.observed",
                "occurred_at": row.get("date"),
                "attributes": attributes,
            }
        )
    return {
        "adapter": "ingest-shadow",
        "space": {"id": space_id, "name": "Personal ingest"},
        "sources": sorted(sources.values(), key=lambda item: item["id"]),
        "events": events,
    }


def run_ingest_shadow(
    db: Database,
    *,
    status_script: str | Path,
    python: str = sys.executable,
    space_id: str = "personal-ingest",
    timeout: int = 120,
    runner: Any = subprocess.run,
) -> dict[str, Any]:
    script = Path(status_script).expanduser().resolve()
    if not script.is_file():
        raise AdapterError(f"ingest status script not found: {script}")
    run = core.start_adapter_run(db, "ingest-shadow")
    command = [python, str(script), "--all", "--json"]
    try:
        result = runner(command, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        core.finish_adapter_run(db, run["id"], state="failed", detail=str(exc))
        raise AdapterError(f"ingest status failed: {exc}") from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout)[-2000:]
        core.finish_adapter_run(db, run["id"], state="failed", detail=detail)
        if result.returncode == 3:
            raise AdapterError(f"ingest source is stale; shadow import refused: {detail}")
        raise AdapterError(f"ingest status exited {result.returncode}: {detail}")
    try:
        rows = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        core.finish_adapter_run(db, run["id"], state="failed", detail=str(exc))
        raise AdapterError(f"ingest status returned invalid JSON: {exc}") from exc
    try:
        snapshot = snapshot_from_rows(rows, space_id=space_id)
    except Exception as exc:
        core.finish_adapter_run(db, run["id"], state="failed", detail=str(exc))
        raise
    return apply_snapshot(db, snapshot, run_id=run["id"])
