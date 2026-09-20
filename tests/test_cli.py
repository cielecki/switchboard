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

    def run_cli(self, *arguments: str) -> dict:
        result = subprocess.run(
            [sys.executable, "-m", "switchboard.cli", "--db", self.db, "--json", *arguments],
            capture_output=True,
            text=True,
            env=self.environment,
            check=False,
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
        self.assertEqual(status["schema_version"], 4)


if __name__ == "__main__":
    unittest.main()
