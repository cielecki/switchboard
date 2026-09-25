from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from switchboard.db import SCHEMA_VERSION


class CliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db = str(Path(self.directory.name) / "switchboard.sqlite3")
        self.environment = dict(
            os.environ,
            PYTHONPATH=str(Path(__file__).parents[1] / "plugins" / "switchboard" / "src"),
        )

    def invoke(
        self, *arguments: str, stdin: int | None = None, timeout: float = 30, db: str | None = None
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "switchboard.cli", "--db", db or self.db, "--json", *arguments],
            stdin=stdin,
            capture_output=True,
            text=True,
            env=self.environment,
            check=False,
            timeout=timeout,
        )

    def run_cli(self, *arguments: str, stdin: int | None = None, timeout: float = 30) -> dict:
        result = self.invoke(*arguments, stdin=stdin, timeout=timeout)
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
        payload = json.loads(result.stdout)
        self.assertTrue(payload["ok"])
        return payload["data"]

    def test_cli_vertical_slice(self) -> None:
        self.run_cli("space", "create", "demo")
        self.run_cli("source", "register", "mail", "--space", "demo", "--kind", "mail")
        route = self.run_cli(
            "route",
            "create",
            "--space",
            "demo",
            "--name",
            "mail triage",
            "--processor",
            "mail-triage",
            "--event-type",
            "message.received",
        )
        wait = self.run_cli(
            "wait",
            "create",
            "--space",
            "demo",
            "--consumer",
            "chat:123",
            "--source",
            "mail",
            "--event-type",
            "message.received",
        )
        event = self.run_cli(
            "event",
            "emit",
            "--source",
            "mail",
            "--external-id",
            "message-1",
            "--type",
            "message.received",
            "--attributes",
            '{"subject":"Hello"}',
        )

        self.assertEqual(event["matched_waits"], [wait["id"]])
        self.assertEqual(event["matched_routes"], [route["id"]])
        processor = self.run_cli("processor", "show", event["processor_runs"][0])
        self.assertEqual(processor["state"], "pending")
        status = self.run_cli("status")
        self.assertEqual(status["counts"]["pending_deliveries"], 1)
        self.assertEqual(status["counts"]["open_processor_runs"], 1)
        self.assertEqual(status["schema_version"], SCHEMA_VERSION)

    def seed_read_only_commands(self) -> tuple[tuple[str, ...], ...]:
        self.run_cli("space", "create", "demo")
        self.run_cli("source", "register", "mail", "--space", "demo", "--kind", "mail")
        self.run_cli(
            "route", "create", "--space", "demo", "--name", "triage",
            "--processor", "mail-triage", "--event-type", "message.received",
        )
        self.run_cli(
            "wait", "create", "--space", "demo", "--consumer", "chat:123",
            "--event-type", "message.received",
        )
        event = self.run_cli(
            "event", "emit", "--source", "mail", "--external-id", "message-1",
            "--type", "message.received",
        )
        return (
            ("status",),
            ("space", "list"),
            ("source", "list"),
            ("event", "list"),
            ("event", "show", event["event"]["id"]),
            ("wait", "list"),
            ("route", "list"),
            ("delivery", "list"),
            ("delivery", "show", event["deliveries"][0]),
            ("processor", "list"),
            ("processor", "show", event["processor_runs"][0]),
            ("processor", "bindings"),
            ("processor", "delivery-list"),
            ("schedule", "list"),
            ("adapter", "runs"),
            ("supervisor", "status"),
        )

    def test_read_only_commands_ignore_open_silent_stdin(self) -> None:
        # Agent harnesses can hand a command a non-TTY stdin that is never written
        # or closed; any implicit stdin read would block these commands forever.
        commands = self.seed_read_only_commands()
        read_end, write_end = os.pipe()
        self.addCleanup(os.close, read_end)
        self.addCleanup(os.close, write_end)

        for command in commands:
            with self.subTest(command=command):
                self.run_cli(*command, stdin=read_end)

    def test_read_only_commands_do_not_wait_for_the_write_lock(self) -> None:
        # The supervisor writes to the same database. A read that needed the write lock
        # would sit out the 30 s busy timeout and then fail with "database is locked".
        commands = self.seed_read_only_commands()
        writer = sqlite3.connect(self.db, isolation_level=None)
        self.addCleanup(writer.close)
        writer.execute("BEGIN IMMEDIATE")
        self.addCleanup(writer.execute, "ROLLBACK")

        for command in commands:
            with self.subTest(command=command):
                started = time.monotonic()
                self.run_cli(*command, timeout=10)
                self.assertLess(time.monotonic() - started, 5)

    def test_json_supervisor_status_without_state_reports_null(self) -> None:
        self.run_cli("init")

        self.assertIsNone(self.run_cli("supervisor", "status"))

    def test_add_stream_schedule_keeps_command_as_argv(self) -> None:
        executable = Path(self.directory.name) / "watcher"
        executable.touch()
        schedule = self.run_cli(
            "schedule", "add-stream", "slack-socket",
            "--command-json", json.dumps([str(executable), "--switchboard-stream"]),
            "--environment", '{"CHANNEL":"C123"}',
            "--restart-after", "7",
        )

        self.assertEqual(schedule["adapter"], "command-stream")
        self.assertEqual(schedule["every_seconds"], 7)
        self.assertEqual(schedule["config"]["command"], [
            str(executable.resolve()), "--switchboard-stream",
        ])
        self.assertEqual(schedule["config"]["environment"], {"CHANNEL": "C123"})

    def test_calendar_schedule_add_preview_and_update(self) -> None:
        created = self.run_cli(
            "schedule",
            "add-calendar",
            "work-inbox",
            "--space",
            "work-inbox",
            "--source",
            "timer/work-inbox",
            "--event-type",
            "inbox.sweep.due",
            "--at",
            "07:00",
            "--timezone",
            "Europe/Warsaw",
            "--weekdays",
            "mon,tue,wed",
            "--weekdays",
            "thu,fri",
        )
        preview = self.run_cli(
            "schedule",
            "preview",
            "work-inbox",
            "--from",
            "2026-09-24T12:00:00+00:00",
            "--count",
            "2",
        )
        updated = self.run_cli(
            "schedule",
            "update-calendar",
            "work-inbox",
            "--space",
            "work-inbox",
            "--source",
            "timer/work-inbox",
            "--event-type",
            "inbox.sweep.due",
            "--at",
            "08:00",
            "--timezone",
            "Europe/Warsaw",
            "--weekdays",
            "mon,tue,wed,thu,fri",
        )

        self.assertEqual(created["schedule_kind"], "calendar")
        self.assertEqual(
            created["config"]["calendar"]["weekdays"],
            ["mon", "tue", "wed", "thu", "fri"],
        )
        self.assertEqual(
            [item["scheduled_for"] for item in preview["occurrences"]],
            ["2026-09-25T05:00:00+00:00", "2026-09-28T05:00:00+00:00"],
        )
        self.assertEqual(updated["revision"], created["revision"] + 1)

    def test_json_reports_database_errors_instead_of_a_traceback(self) -> None:
        # A directory cannot be opened as a database, which sqlite3 reports as an
        # OperationalError, the same class as "database is locked".
        result = self.invoke("status", db=self.directory.name)

        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(
            json.loads(result.stdout), {"ok": False, "error": "unable to open database file"}
        )
        self.assertNotIn("Traceback", result.stderr)


if __name__ == "__main__":
    unittest.main()
