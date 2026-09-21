from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class CliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db = str(Path(self.directory.name) / "switchboard.sqlite3")
        self.environment = dict(
            os.environ,
            PYTHONPATH=str(Path(__file__).parents[1] / "plugins" / "switchboard" / "src"),
        )

    def run_cli(self, *arguments: str, stdin: int | None = None) -> dict:
        result = subprocess.run(
            [sys.executable, "-m", "switchboard.cli", "--db", self.db, "--json", *arguments],
            stdin=stdin,
            capture_output=True,
            text=True,
            env=self.environment,
            check=False,
            timeout=30,
        )
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
        self.assertEqual(status["schema_version"], 5)

    def test_read_only_commands_ignore_open_silent_stdin(self) -> None:
        # Agent harnesses can hand a command a non-TTY stdin that is never written
        # or closed; any implicit stdin read would block these commands forever.
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
        read_end, write_end = os.pipe()
        self.addCleanup(os.close, read_end)
        self.addCleanup(os.close, write_end)

        for command in (
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
        ):
            with self.subTest(command=command):
                self.run_cli(*command, stdin=read_end)


if __name__ == "__main__":
    unittest.main()
