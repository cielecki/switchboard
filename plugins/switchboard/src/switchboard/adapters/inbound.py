from __future__ import annotations

import math
import os
import re
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .. import core
from ..db import Database
from ..process import run_bounded
from .base import AdapterError, apply_snapshot


def _gmail_environment(profile: str, timeout: float) -> dict[str, str]:
    """One search attempt fits inside the adapter deadline; schedules own retries."""
    environment = dict(os.environ)
    try:
        requested = float(environment.get("GMAIL_TIMEOUT") or "180")
    except ValueError as exc:
        raise AdapterError("GMAIL_TIMEOUT must be a finite positive number") from exc
    if not math.isfinite(timeout) or timeout <= 0:
        raise AdapterError("inbound timeout must be a finite positive number")
    if not math.isfinite(requested) or requested <= 0:
        raise AdapterError("GMAIL_TIMEOUT must be a finite positive number")
    # Leave setup, ledger persistence and process cleanup a bounded share of the
    # existing deadline. The default 180-second search still fits a 240-second poll.
    search_timeout = min(requested, timeout - min(30, timeout / 4))
    environment.update(
        {"MAX_POLLS": "1", "INTERVAL": "0", "STOP_AT": "", "PROFILE": profile,
         "GMAIL_TIMEOUT": f"{search_timeout:g}", "GMAIL_POLL_RETRIES": "1",
         "GMAIL_RETRY_SLEEP": "0", "INBOUND_GMAIL_DIAGNOSTICS": "1"}
    )
    return environment


def _gmail_phases(stderr: str) -> list[tuple[str, str]]:
    return re.findall(
        r"^INBOUND_GMAIL_PHASE\t(setup|search|search-result|ledger-discover|ledger-done|"
        r"queue-reconcile|complete)\t([0-9]{1,10})$", stderr[-8000:], re.MULTILINE)


def _gmail_failure(stderr: str | bytes | None, *, outcome: str,
                   timeout: float, search_timeout: str,
                   phase: str = "gmail-discovery") -> str:
    """Keep diagnostic categories, never copied commands, mail or credential text."""
    if isinstance(stderr, bytes):
        stderr = stderr.decode("utf-8", errors="replace")
    tail = (stderr or "")[-8000:]
    for prefix, name in (
        ("Gmail search failed", "gmail-search"),
        ("Gmail search returned an invalid payload", "gmail-search-result"),
        ("failed to persist discovery", "ledger-discover"),
        ("ledger done-ids read failed", "ledger-done"),
        ("could not reconcile the inbox", "queue-reconcile"),
    ):
        if f"inbound-gmail-watch: {prefix}" in tail:
            phase = name
    markers = _gmail_phases(tail)
    phase_start = "unknown"
    if markers:
        phase, phase_start = markers[-1]
    causes = sorted(set(re.findall(
        r"\b(?:TimeoutError|TimeoutExpired|ConnectionError|URLError|HTTPError|"
        r"FileNotFoundError|PermissionError|JSONDecodeError)\b", tail)))
    if "command timed out after " in tail:
        causes.append("search-timeout")
    cause = ",".join(causes) or "unspecified"
    return (f"phase={phase}; phase_started_after={phase_start}s; outcome={outcome}; outer_timeout={timeout:g}s; "
            f"search_timeout={search_timeout}s; attempts=1; causes={cause}")


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
    source_mode: str = "both",
    python: str = sys.executable,
    timeout: int = 240,
    runner: Any = run_bounded,
    run_id: str | None = None,
    finalize_run: bool = True,
) -> dict[str, Any]:
    if not profile:
        raise AdapterError("inbound profile cannot be empty")
    if source_mode not in {"both", "gmail", "slack"}:
        raise AdapterError("inbound source mode must be both, gmail, or slack")
    if source_mode == "gmail" and discovery_script is None:
        raise AdapterError("gmail source mode requires a discovery script")
    if source_mode == "slack" and slack_discovery_script is None:
        raise AdapterError("slack source mode requires a Slack discovery script")
    ledger = Path(ledger_script).expanduser().resolve()
    if not ledger.is_file():
        raise AdapterError(f"inbound ledger script not found: {ledger}")
    if run_id is None and not finalize_run:
        raise ValueError("deferred adapter finalization requires a run id")
    run = {"id": run_id} if run_id else core.start_adapter_run(db, "inbound-leads")

    def fail(detail: str) -> None:
        if finalize_run:
            core.finish_adapter_run(db, run["id"], state="failed", detail=detail)

    discovery: dict[str, Any] = {"gmail": {"enabled": False}, "slack": {"enabled": False}}
    gmail_deadline: float | None = None
    if source_mode in {"both", "gmail"} and discovery_script is not None:
        script = Path(discovery_script).expanduser().resolve()
        if not script.is_file():
            fail(f"discovery script not found: {script}")
            raise AdapterError(f"inbound discovery script not found: {script}")
        try:
            environment = _gmail_environment(profile, timeout)
        except AdapterError as exc:
            fail(str(exc))
            raise
        try:
            gmail_deadline = time.monotonic() + timeout
            discovered = runner(
                ["bash", str(script)],
                capture_output=True,
                text=True,
                timeout=timeout,
                env=environment,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            detail = _gmail_failure(
                (getattr(exc, "stderr", None) or b"")
                if isinstance(exc, subprocess.TimeoutExpired)
                else type(exc).__name__,
                outcome="timeout" if isinstance(exc, subprocess.TimeoutExpired) else "start-error",
                timeout=timeout, search_timeout=environment["GMAIL_TIMEOUT"],
            )
            fail(detail)
            raise AdapterError(f"inbound discovery failed: {detail}") from exc
        if discovered.returncode != 0:
            detail = _gmail_failure(
                discovered.stderr, outcome=f"exit-{discovered.returncode}",
                timeout=timeout, search_timeout=environment["GMAIL_TIMEOUT"],
            )
            fail(detail)
            raise AdapterError(f"inbound discovery exited {discovered.returncode}: {detail}")
        discovery["gmail"] = {
            "enabled": True,
            "lead_lines": sum(
                line.startswith("LEAD\t") for line in discovered.stdout.splitlines()
            ),
            "phases": _gmail_phases(discovered.stderr or ""),
        }
        counts = re.findall(r"^INBOUND_GMAIL_COUNT\t([0-9]{1,10})$",
                            (discovered.stderr or "")[-8000:], re.MULTILINE)
        if counts:
            discovery["gmail"]["polled_count"] = int(counts[-1])

    rows: list[dict[str, Any]] = []
    if source_mode in {"both", "gmail"}:
        pending_timeout = timeout
        if gmail_deadline is not None:
            pending_timeout = gmail_deadline - time.monotonic()
            if pending_timeout <= 0:
                detail = _gmail_failure(None, outcome="budget-exhausted", timeout=timeout,
                                        search_timeout=environment["GMAIL_TIMEOUT"],
                                        phase="ledger-pending")
                fail(detail)
                raise AdapterError(f"inbound ledger read failed: {detail}")
        try:
            pending = runner(
                [python, str(ledger), "pending", "--profile", profile, "--json"],
                capture_output=True,
                text=True,
                timeout=pending_timeout,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            if gmail_deadline is not None:
                detail = _gmail_failure(
                    getattr(exc, "stderr", None) if isinstance(exc, subprocess.TimeoutExpired)
                    else type(exc).__name__,
                    outcome="timeout" if isinstance(exc, subprocess.TimeoutExpired) else "start-error",
                    timeout=timeout, search_timeout=environment["GMAIL_TIMEOUT"],
                    phase="ledger-pending",
                )
                fail(detail)
                raise AdapterError(f"inbound ledger read failed: {detail}") from exc
            fail(str(exc))
            raise AdapterError(f"inbound ledger read failed: {exc}") from exc
        if pending.returncode != 0:
            detail = (_gmail_failure(
                pending.stderr, outcome=f"exit-{pending.returncode}", timeout=timeout,
                search_timeout=environment["GMAIL_TIMEOUT"], phase="ledger-pending",
            ) if gmail_deadline is not None else (pending.stderr or pending.stdout)[-2000:])
            fail(detail)
            raise AdapterError(f"inbound ledger exited {pending.returncode}: {detail}")
        try:
            import json

            rows = json.loads(pending.stdout)
            snapshot_from_pending(rows, profile=profile, space_id=space_id)
        except Exception as exc:
            if gmail_deadline is not None:
                detail = _gmail_failure(type(exc).__name__, outcome="invalid-ledger-output",
                                        timeout=timeout, search_timeout=environment["GMAIL_TIMEOUT"],
                                        phase="ledger-pending")
                fail(detail)
                raise AdapterError(f"inbound ledger read failed: {detail}") from exc
            fail(str(exc))
            raise

    slack_lines: list[str] | None = None
    if source_mode in {"both", "slack"} and slack_discovery_script is not None:
        slack_script = Path(slack_discovery_script).expanduser().resolve()
        if not slack_script.is_file():
            fail(f"Slack discovery script not found: {slack_script}")
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
            fail(str(exc))
            raise AdapterError(f"inbound Slack discovery failed: {exc}") from exc
        if discovered_slack.returncode != 0:
            detail = (discovered_slack.stderr or discovered_slack.stdout)[-2000:]
            fail(detail)
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
        if phases := discovery["gmail"].get("phases"):
            # Source health is the existing read-only diagnostic surface; do not
            # persist the watcher's arbitrary stderr or message data.
            snapshot["sources"][0]["detail"] += "; gmail phases: " + ",".join(
                f"{phase}@{elapsed}s" for phase, elapsed in phases[-16:]
            )
            if "polled_count" in discovery["gmail"]:
                snapshot["sources"][0]["detail"] += (
                    f"; polled_count={discovery['gmail']['polled_count']}"
                )
    except Exception as exc:
        fail(str(exc))
        raise
    result = apply_snapshot(
        db,
        snapshot,
        run_id=run["id"],
        finalize_run=finalize_run,
    )
    result["discovery"] = discovery
    return result
