from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .. import core
from ..db import Database
from .base import AdapterError, apply_snapshot


def _occurred_at(value: object) -> str | None:
    try:
        milliseconds = int(str(value))
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(milliseconds / 1000, UTC).isoformat()


def snapshot_from_pending(
    rows: object, *, profile: str, space_id: str = "inbound-leads"
) -> dict[str, Any]:
    if not isinstance(rows, list):
        raise AdapterError("inbound ledger pending output must be a JSON array")
    source_id = f"inbound/{profile}"
    events: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise AdapterError("every inbound ledger row must be a JSON object")
        pointer = row.get("id")
        row_profile = row.get("profile")
        if not isinstance(pointer, str) or not pointer:
            raise AdapterError("inbound ledger rows require a stable id")
        if row_profile != profile:
            raise AdapterError(f"inbound ledger row belongs to unexpected profile: {row_profile}")
        attributes: dict[str, Any] = {
            "pointer": pointer,
            "profile": profile,
            "queue_state": "pending",
            "has_verdict": bool(row.get("verdict")),
            "has_deal": bool(row.get("deal_id")),
            "posted_slack": bool(row.get("posted_slack")),
        }
        events.append(
            {
                "source_id": source_id,
                "external_id": pointer,
                "event_type": "inbound.lead.pending",
                "occurred_at": _occurred_at(row.get("internal_date")),
                "attributes": attributes,
            }
        )
    return {
        "adapter": "inbound-leads",
        "space": {"id": space_id, "name": "Inbound leads"},
        "sources": [
            {
                "id": source_id,
                "kind": "inbound-leads.ledger",
                "state": "ready",
                "detail": "observed through the inbound-leads ledger pending command",
                "config": {"adapter": "inbound-leads", "profile": profile},
            }
        ],
        "events": events,
    }


def run_inbound_leads(
    db: Database,
    *,
    ledger_script: str | Path,
    profile: str,
    space_id: str = "inbound-leads",
    discovery_script: str | Path | None = None,
    python: str = sys.executable,
    timeout: int = 240,
    runner: Any = subprocess.run,
) -> dict[str, Any]:
    if not profile:
        raise AdapterError("inbound profile cannot be empty")
    ledger = Path(ledger_script).expanduser().resolve()
    if not ledger.is_file():
        raise AdapterError(f"inbound ledger script not found: {ledger}")
    run = core.start_adapter_run(db, "inbound-leads")
    discovery: dict[str, Any] | None = None
    if discovery_script is not None:
        script = Path(discovery_script).expanduser().resolve()
        if not script.is_file():
            core.finish_adapter_run(
                db, run["id"], state="failed", detail=f"discovery script not found: {script}"
            )
            raise AdapterError(f"inbound discovery script not found: {script}")
        environment = dict(os.environ)
        environment.update({"MAX_POLLS": "1", "PROFILE": profile})
        try:
            discovered = runner(
                ["bash", str(script)],
                capture_output=True,
                text=True,
                timeout=timeout,
                env=environment,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            core.finish_adapter_run(db, run["id"], state="failed", detail=str(exc))
            raise AdapterError(f"inbound discovery failed: {exc}") from exc
        if discovered.returncode != 0:
            detail = (discovered.stderr or discovered.stdout)[-2000:]
            core.finish_adapter_run(db, run["id"], state="failed", detail=detail)
            raise AdapterError(f"inbound discovery exited {discovered.returncode}: {detail}")
        discovery = {
            "enabled": True,
            "lead_lines": sum(
                line.startswith("LEAD\t") for line in discovered.stdout.splitlines()
            ),
        }

    try:
        pending = runner(
            [python, str(ledger), "pending", "--profile", profile, "--json"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        core.finish_adapter_run(db, run["id"], state="failed", detail=str(exc))
        raise AdapterError(f"inbound ledger read failed: {exc}") from exc
    if pending.returncode != 0:
        detail = (pending.stderr or pending.stdout)[-2000:]
        core.finish_adapter_run(db, run["id"], state="failed", detail=detail)
        raise AdapterError(f"inbound ledger exited {pending.returncode}: {detail}")
    try:
        import json

        rows = json.loads(pending.stdout)
        snapshot = snapshot_from_pending(rows, profile=profile, space_id=space_id)
    except Exception as exc:
        core.finish_adapter_run(db, run["id"], state="failed", detail=str(exc))
        raise
    result = apply_snapshot(db, snapshot, run_id=run["id"])
    result["discovery"] = discovery or {"enabled": False}
    return result
