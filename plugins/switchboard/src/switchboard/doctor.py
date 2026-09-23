from __future__ import annotations

import json
import plistlib
import re
import sqlite3
import sys
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from . import __version__, core
from .db import SCHEMA_VERSION, Database
from .service import launch_agent_path, service_status


def _finding(
    findings: list[dict[str, Any]],
    severity: str,
    code: str,
    message: str,
    **details: Any,
) -> None:
    findings.append({"severity": severity, "code": code, "message": message, **details})


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _service_arguments() -> tuple[list[str], dict[str, Any] | None]:
    path = launch_agent_path()
    if not path.is_file():
        return [], None
    try:
        payload = plistlib.loads(path.read_bytes())
    except (OSError, plistlib.InvalidFileException):
        return [], None
    arguments = payload.get("ProgramArguments")
    return (arguments if isinstance(arguments, list) else []), payload


def _argument(arguments: list[str], name: str) -> str | None:
    try:
        index = arguments.index(name)
    except ValueError:
        return None
    return arguments[index + 1] if index + 1 < len(arguments) else None


def run_doctor(db: Database, *, now_at: datetime | None = None) -> dict[str, Any]:
    findings: list[dict[str, Any]] = []
    checked_at = (now_at or datetime.now(UTC)).astimezone(UTC)
    db.initialize()
    try:
        with db.session() as connection:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            version = connection.execute(
                "SELECT value FROM schema_meta WHERE key='schema_version'"
            ).fetchone()
    except sqlite3.Error as exc:
        _finding(findings, "error", "database.unreadable", str(exc), path=str(db.path))
        integrity, version = "unreadable", None
    if integrity != "ok":
        _finding(
            findings, "error", "database.integrity", f"integrity check: {integrity}"
        )
    if version is None or int(version[0]) != SCHEMA_VERSION:
        _finding(
            findings,
            "error",
            "database.schema",
            f"expected schema {SCHEMA_VERSION}, found {version[0] if version else 'missing'}",
        )

    schedules = core.list_schedules(db)
    for schedule in schedules:
        config = schedule["config"]
        for key in (
            "status_script",
            "discovery_script",
            "ledger_script",
            "slack_discovery_script",
        ):
            value = config.get(key)
            if value and not Path(value).is_file():
                _finding(
                    findings,
                    "error",
                    "schedule.path-missing",
                    f"{schedule['id']} {key} is missing",
                    path=value,
                )
        if schedule["enabled"] and schedule["last_state"] == "failed":
            _finding(
                findings,
                "warning",
                "schedule.last-run-failed",
                f"{schedule['id']} last run failed",
                detail=schedule["last_error"],
            )
        next_run = _parse(schedule["next_run_at"])
        if (
            schedule["enabled"]
            and next_run
            and next_run
            < checked_at - timedelta(seconds=max(60, schedule["every_seconds"] * 2))
        ):
            _finding(
                findings,
                "warning",
                "schedule.overdue",
                f"{schedule['id']} is overdue",
                next_run_at=schedule["next_run_at"],
            )

    supervisor = core.supervisor_status(db) or {}
    heartbeat = _parse(supervisor.get("heartbeat_at"))
    if supervisor.get("state") == "running" and (
        heartbeat is None or heartbeat < checked_at - timedelta(seconds=60)
    ):
        _finding(
            findings,
            "error",
            "supervisor.stale",
            "supervisor reports running with a stale heartbeat",
            heartbeat_at=supervisor.get("heartbeat_at"),
        )
    if supervisor.get("last_error"):
        _finding(
            findings,
            "warning",
            "supervisor.last-error",
            "supervisor recorded an error",
            detail=supervisor["last_error"],
        )

    arguments: list[str] = []
    service: dict[str, Any] = {"installed": False, "loaded": False}
    if sys.platform == "darwin":
        service = service_status()
        arguments, payload = _service_arguments()
        if service["installed"] and not service["loaded"]:
            _finding(
                findings,
                "error",
                "service.not-loaded",
                "launch agent is installed but not loaded",
            )
        if service["installed"] and payload is None:
            _finding(
                findings,
                "error",
                "service.invalid-plist",
                "launch agent plist cannot be parsed",
            )
        if arguments:
            launcher_index = (
                1
                if len(arguments) > 1 and arguments[1].endswith("/bin/switchboard")
                else 0
            )
            launcher = Path(arguments[launcher_index]).expanduser()
            if not launcher.is_file():
                _finding(
                    findings,
                    "error",
                    "service.launcher-missing",
                    "configured launcher is missing",
                    path=str(launcher),
                )
            expected_launcher = (
                Path(__file__).resolve().parents[2] / "bin" / "switchboard"
            )
            if launcher.is_file() and launcher.resolve() != expected_launcher.resolve():
                _finding(
                    findings,
                    "error",
                    "service.launcher-drift",
                    "service points at a different Switchboard installation",
                    configured=str(launcher.resolve()),
                    expected=str(expected_launcher.resolve()),
                )
            configured_db = _argument(arguments, "--db")
            if configured_db and Path(configured_db).expanduser().resolve() != db.path:
                _finding(
                    findings,
                    "error",
                    "service.database-drift",
                    "service points at a different database",
                    configured=configured_db,
                    active=str(db.path),
                )
            relay = _argument(arguments, "--relay")
            if relay and not Path(relay).is_file():
                _finding(
                    findings,
                    "error",
                    "service.relay-missing",
                    "configured chats relay is missing",
                    path=relay,
                )
            alert_json = _argument(arguments, "--alert-command-json")
            if alert_json:
                try:
                    alert_command = json.loads(alert_json)
                except json.JSONDecodeError:
                    alert_command = []
                if not alert_command or not Path(alert_command[0]).is_file():
                    _finding(
                        findings,
                        "error",
                        "service.alert-command-missing",
                        "configured alert command is invalid",
                    )

    bindings = core.list_processor_bindings(db, state="enabled")
    for binding in bindings:
        if not re.fullmatch(r"chat:(claude|codex|opencode):[^:]+", binding["consumer"]):
            _finding(
                findings,
                "error",
                "binding.invalid-consumer",
                f"binding {binding['id']} has an invalid consumer",
            )
    pending = core.list_processor_deliveries(db, "pending")
    accepted = core.list_processor_deliveries(db, "accepted")
    accepted_waits = core.list_deliveries(db, "accepted")
    open_alerts = core.list_processor_alerts(db, "open")
    active_leases = db.row(
        "SELECT count(*) AS count FROM processor_attempts WHERE state='running' AND lease_expires_at>?",
        (checked_at.isoformat(),),
    )["count"]
    accepted_retry = int(_argument(arguments, "--accepted-retry") or 120)
    stale_accepted = [
        delivery
        for delivery in accepted
        if delivery.get("accepted_at")
        and delivery.get("processor_state") == "pending"
        and _parse(delivery["accepted_at"])
        <= checked_at - timedelta(seconds=accepted_retry)
    ]
    if stale_accepted:
        _finding(
            findings,
            "warning",
            "processor.unclaimed-wake",
            f"{len(stale_accepted)} accepted processor wake(s) were not claimed",
            delivery_ids=[delivery["id"] for delivery in stale_accepted],
        )
    stale_waits = [
        delivery
        for delivery in accepted_waits
        if delivery.get("accepted_at")
        and _parse(delivery["accepted_at"])
        <= checked_at - timedelta(seconds=accepted_retry)
    ]
    if stale_waits:
        _finding(
            findings,
            "warning",
            "delivery.unacknowledged-wake",
            f"{len(stale_waits)} accepted wait delivery wake(s) were not acknowledged",
            delivery_ids=[delivery["id"] for delivery in stale_waits],
        )
    for source in core.list_sources(db):
        if source["state"] not in {"enabled", "ready", "healthy"}:
            _finding(
                findings,
                "warning",
                "source.unhealthy",
                f"source {source['id']} reports {source['state']}",
                detail=(source.get("health") or {}).get("detail"),
            )
    if open_alerts:
        _finding(
            findings,
            "warning",
            "processor.open-alerts",
            f"{len(open_alerts)} processor alert episode(s) are open",
        )

    web_url = supervisor.get("web_url")
    if supervisor.get("state") == "running" and web_url:
        try:
            with urllib.request.urlopen(web_url, timeout=1) as response:
                if response.status != 200:
                    raise ValueError(f"HTTP {response.status}")
        except (OSError, ValueError, urllib.error.URLError) as exc:
            _finding(
                findings,
                "warning",
                "dashboard.unreachable",
                f"dashboard is unreachable: {exc}",
                url=web_url,
            )

    errors = sum(item["severity"] == "error" for item in findings)
    warnings = sum(item["severity"] == "warning" for item in findings)
    return {
        "status": "error" if errors else "warning" if warnings else "ok",
        "checked_at": checked_at.isoformat(),
        "runtime_version": __version__,
        "database": {
            "path": str(db.path),
            "integrity": integrity,
            "schema_version": int(version[0]) if version else None,
        },
        "service": {
            key: service.get(key) for key in ("installed", "loaded", "label", "plist")
        },
        "queues": {
            "accepted_deliveries": len(accepted_waits),
            "pending_processor_deliveries": len(pending),
            "accepted_processor_deliveries": len(accepted),
            "active_leases": active_leases,
            "open_alert_episodes": len(open_alerts),
        },
        "findings": findings,
        "errors": errors,
        "warnings": warnings,
    }
