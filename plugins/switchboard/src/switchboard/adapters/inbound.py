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
    rows: object,
    *,
    profile: str,
    space_id: str = "inbound-leads",
    slack_lines: list[str] | None = None,
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
    sources = [
        {
            "id": source_id,
            "kind": "inbound-leads.ledger",
            "state": "ready",
            "detail": "observed through the inbound-leads ledger pending command",
            "config": {"adapter": "inbound-leads", "profile": profile},
        }
    ]
    if slack_lines is not None:
        slack_source_id = f"inbound/{profile}-slack"
        sources.append(
            {
                "id": slack_source_id,
                "kind": "inbound-leads.slack-mention",
                "state": "ready",
                "detail": "observed through one bounded Slack mention poll",
                "config": {"adapter": "inbound-leads", "profile": profile},
            }
        )
        for line in slack_lines:
            fields = line.split("\t", 4)
            if len(fields) != 5 or fields[0] != "MENTION":
                raise AdapterError("Slack discovery returned an invalid mention line")
            _kind, message_ts, thread_root, _user_id, _text = fields
            if not message_ts or not thread_root:
                raise AdapterError("Slack mention lines require message and thread pointers")
            events.append(
                {
                    "source_id": slack_source_id,
                    "external_id": message_ts,
                    "event_type": "inbound.slack.mention",
                    "occurred_at": None,
                    "attributes": {
                        "pointer": message_ts,
                        "thread_root": thread_root,
                        "profile": profile,
                        "queue_state": "pending",
                    },
                }
            )
    return {
        "adapter": "inbound-leads",
        "space": {"id": space_id, "name": "Inbound leads"},
        "sources": sources,
        "events": events,
    }


def run_inbound_leads(
    db: Database,
    *,
    ledger_script: str | Path,
    profile: str,
    space_id: str = "inbound-leads",
    discovery_script: str | Path | None = None,
    slack_discovery_script: str | Path | None = None,
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
    discovery: dict[str, Any] = {"gmail": {"enabled": False}, "slack": {"enabled": False}}
    if discovery_script is not None:
        script = Path(discovery_script).expanduser().resolve()
        if not script.is_file():
            core.finish_adapter_run(
                db, run["id"], state="failed", detail=f"discovery script not found: {script}"
            )
            raise AdapterError(f"inbound discovery script not found: {script}")
        environment = dict(os.environ)
        environment.update(
            {"MAX_POLLS": "1", "INTERVAL": "0", "STOP_AT": "", "PROFILE": profile}
        )
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
        discovery["gmail"] = {
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
        # Validate the ledger before polling Slack. The Slack source advances its own cursor, so
        # a malformed or unavailable ledger must fail before any mention can be consumed.
        snapshot_from_pending(rows, profile=profile, space_id=space_id)
    except Exception as exc:
        core.finish_adapter_run(db, run["id"], state="failed", detail=str(exc))
        raise

    slack_lines: list[str] | None = None
    if slack_discovery_script is not None:
        slack_script = Path(slack_discovery_script).expanduser().resolve()
        if not slack_script.is_file():
            core.finish_adapter_run(
                db,
                run["id"],
                state="failed",
                detail=f"Slack discovery script not found: {slack_script}",
            )
            raise AdapterError(f"inbound Slack discovery script not found: {slack_script}")
        environment = dict(os.environ)
        environment.update(
            {"MAX_POLLS": "1", "INTERVAL": "0", "STOP_AT": "", "SET_STATUS": "0"}
        )
        try:
            discovered_slack = runner(
                ["bash", str(slack_script)],
                capture_output=True,
                text=True,
                timeout=timeout,
                env=environment,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            core.finish_adapter_run(db, run["id"], state="failed", detail=str(exc))
            raise AdapterError(f"inbound Slack discovery failed: {exc}") from exc
        if discovered_slack.returncode != 0:
            detail = (discovered_slack.stderr or discovered_slack.stdout)[-2000:]
            core.finish_adapter_run(db, run["id"], state="failed", detail=detail)
            raise AdapterError(
                f"inbound Slack discovery exited {discovered_slack.returncode}: {detail}"
            )
        slack_lines = [
            line for line in discovered_slack.stdout.splitlines() if line.startswith("MENTION\t")
        ]
        discovery["slack"] = {"enabled": True, "mention_lines": len(slack_lines)}

    try:
        snapshot = snapshot_from_pending(
            rows, profile=profile, space_id=space_id, slack_lines=slack_lines
        )
    except Exception as exc:
        core.finish_adapter_run(db, run["id"], state="failed", detail=str(exc))
        raise
    result = apply_snapshot(db, snapshot, run_id=run["id"])
    result["discovery"] = discovery
    return result
