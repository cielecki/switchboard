from __future__ import annotations

import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from switchboard import core
from switchboard.db import Database


class DeliveryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.db = Database(self.root / "switchboard.sqlite3")
        core.create_space(self.db, "demo")
        core.register_source(self.db, "mail", "demo", "mail")
        wait = core.create_wait(
            self.db,
            space_id="demo",
            consumer="chat:codex:task-123",
            predicate={"event_type": "message.received"},
            purpose="Continue after the reply",
        )
        event = core.emit_event(
            self.db,
            source_id="mail",
            external_id="message-1",
            event_type="message.received",
            attributes={"sender": "person@example.com"},
        )
        self.wait_id = wait["id"]
        self.delivery_id = event["deliveries"][0]
        self.relay = self.root / "send-message.py"
        self.relay.touch()

    def test_dispatch_uses_stable_broker_request_and_requires_ack(self) -> None:
        calls = []

        def runner(command, **_kwargs):
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, stdout="accepted", stderr="")

        result = core.dispatch_delivery(
            self.db,
            self.delivery_id,
            relay=self.relay,
            cli_command=["/plugin/bin/switchboard"],
            runner=runner,
        )
        self.assertEqual(result["state"], "accepted")
        self.assertTrue(result["request_id"].startswith("msg_broker_"))
        self.assertIn("--delivery-only", calls[0])
        self.assertIn("task-123", calls[0])
        self.assertIn("delivery show", " ".join(calls[0]))

        repeated = core.dispatch_delivery(
            self.db,
            self.delivery_id,
            relay=self.relay,
            cli_command=["/plugin/bin/switchboard"],
            runner=runner,
        )
        self.assertTrue(repeated["idempotent"])
        self.assertEqual(len(calls), 1)

        acknowledged = core.acknowledge_delivery(self.db, self.delivery_id)
        self.assertEqual(acknowledged["state"], "acknowledged")

    def test_failed_dispatch_stays_pending_for_same_id_retry(self) -> None:
        def runner(command, **_kwargs):
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="unavailable")

        with self.assertRaisesRegex(ValueError, "unavailable"):
            core.dispatch_delivery(
                self.db,
                self.delivery_id,
                relay=self.relay,
                cli_command=["/plugin/bin/switchboard"],
                runner=runner,
            )

        delivery = core.get_delivery(self.db, self.delivery_id)
        self.assertEqual(delivery["state"], "pending")
        self.assertIn("unavailable", delivery["last_error"])
        self.assertEqual(delivery["attempts"][0]["state"], "failed")

    def test_timeout_exit_is_accepted_when_broker_confirms_delivery(self) -> None:
        result = core.dispatch_delivery(
            self.db,
            self.delivery_id,
            relay=self.relay,
            cli_command=["/plugin/bin/switchboard"],
            runner=lambda command, **_kwargs: subprocess.CompletedProcess(
                command,
                2,
                stdout='{"status":"timeout","delivery_status":"accepted"}',
                stderr="",
            ),
        )

        self.assertEqual(result["state"], "accepted")
        self.assertEqual(core.get_delivery(self.db, self.delivery_id)["state"], "accepted")

    def test_unacknowledged_accepted_delivery_retries_with_same_request_id(self) -> None:
        calls: list[list[str]] = []

        def runner(command, **_kwargs):
            calls.append(command)
            return subprocess.CompletedProcess(
                command,
                2,
                stdout=(
                    '{"status":"timeout","delivery_status":"accepted",'
                    '"receipt_status":null}'
                ),
                stderr="",
            )

        first = core.dispatch_delivery(
            self.db,
            self.delivery_id,
            relay=self.relay,
            cli_command=["/plugin/bin/switchboard"],
            runner=runner,
        )
        accepted_at = datetime.fromisoformat(
            core.get_delivery(self.db, self.delivery_id)["accepted_at"]
        )
        recovered = core.recover_unacknowledged_deliveries(
            self.db,
            (accepted_at + timedelta(seconds=121)).isoformat(),
            after_seconds=120,
        )
        self.assertEqual(recovered, 1)
        self.assertEqual(core.get_delivery(self.db, self.delivery_id)["state"], "pending")

        second = core.dispatch_delivery(
            self.db,
            self.delivery_id,
            relay=self.relay,
            cli_command=["/plugin/bin/switchboard"],
            runner=runner,
        )
        self.assertEqual(first["request_id"], second["request_id"])
        self.assertEqual(calls[0], calls[1])

    def test_held_receipt_stays_pending(self) -> None:
        with self.assertRaisesRegex(ValueError, "relay exited 2"):
            core.dispatch_delivery(
                self.db,
                self.delivery_id,
                relay=self.relay,
                cli_command=["/plugin/bin/switchboard"],
                runner=lambda command, **_kwargs: subprocess.CompletedProcess(
                    command,
                    2,
                    stdout=(
                        '{"status":"held","delivery_status":"accepted",'
                        '"receipt_status":"held"}'
                    ),
                    stderr="",
                ),
            )
        delivery = core.get_delivery(self.db, self.delivery_id)
        self.assertEqual(delivery["state"], "pending")
        self.assertEqual(delivery["attempts"][-1]["state"], "failed")


if __name__ == "__main__":
    unittest.main()
