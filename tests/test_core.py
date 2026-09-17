from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from switchboard import core
from switchboard.db import Database


class SwitchboardTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db = Database(Path(self.directory.name) / "switchboard.sqlite3")
        core.create_space(self.db, "demo", "Demo")
        core.register_source(self.db, "mail", "demo", "mail")

    def test_event_matches_wait_and_creates_delivery(self) -> None:
        wait = core.create_wait(
            self.db,
            space_id="demo",
            consumer="chat:123",
            predicate={
                "source_id": "mail",
                "event_type": "message.received",
                "attributes": {"sender": "person@example.com"},
            },
            purpose="Continue after a reply",
        )

        result = core.emit_event(
            self.db,
            source_id="mail",
            external_id="message-1",
            event_type="message.received",
            attributes={"sender": "person@example.com", "subject": "Ready"},
        )

        self.assertFalse(result["deduplicated"])
        self.assertEqual(result["matched_waits"], [wait["id"]])
        self.assertEqual(len(result["deliveries"]), 1)
        self.assertEqual(core.list_waits(self.db)[0]["state"], "matched")
        self.assertEqual(core.list_deliveries(self.db)[0]["state"], "pending")

    def test_duplicate_external_event_is_idempotent(self) -> None:
        first = core.emit_event(
            self.db,
            source_id="mail",
            external_id="same-message",
            event_type="message.received",
            attributes={"sender": "person@example.com"},
        )
        second = core.emit_event(
            self.db,
            source_id="mail",
            external_id="same-message",
            event_type="message.received",
            attributes={"sender": "changed@example.com"},
        )

        self.assertFalse(first["deduplicated"])
        self.assertTrue(second["deduplicated"])
        self.assertEqual(first["event"]["id"], second["event"]["id"])
        self.assertEqual(len(core.list_events(self.db)), 1)

    def test_contains_match_is_case_insensitive(self) -> None:
        wait = core.create_wait(
            self.db,
            space_id="demo",
            consumer="chat:123",
            predicate={"contains": {"subject": "password RESET"}},
        )
        result = core.emit_event(
            self.db,
            source_id="mail",
            external_id="message-2",
            event_type="message.received",
            attributes={"subject": "Your password reset is complete"},
        )
        self.assertEqual(result["matched_waits"], [wait["id"]])

    def test_cancel_wait_cancels_its_pending_delivery(self) -> None:
        wait = core.create_wait(
            self.db,
            space_id="demo",
            consumer="chat:123",
            predicate={"event_type": "message.received"},
            repeating=True,
        )
        core.emit_event(
            self.db,
            source_id="mail",
            external_id="message-3",
            event_type="message.received",
            attributes={},
        )

        core.cancel_wait(self.db, wait["id"])

        self.assertEqual(core.list_waits(self.db)[0]["state"], "cancelled")
        self.assertEqual(core.list_deliveries(self.db)[0]["state"], "cancelled")


if __name__ == "__main__":
    unittest.main()
