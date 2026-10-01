from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from switchboard import core
from switchboard.db import Database


class CalendarSourceRepairTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db = Database(Path(self.directory.name) / "switchboard.sqlite3")

    def create_schedule(
        self,
        schedule_id: str = "inbox",
        *,
        source_id: str = "timer/inbox",
        space_id: str = "inbox",
    ) -> dict:
        return core.upsert_calendar_schedule(
            self.db,
            schedule_id,
            space_id=space_id,
            source_id=source_id,
            event_type="inbox.sweep.due",
            local_time="07:00",
            timezone="Europe/Warsaw",
            at="2026-09-24T04:00:00+00:00",
        )

    def make_legacy_source(
        self,
        source_id: str = "timer/inbox",
        *,
        adapter: str | None = None,
        extra: dict | None = None,
    ) -> None:
        config = dict(extra or {})
        if adapter is not None:
            config["adapter"] = adapter
        with self.db.transaction() as connection:
            connection.execute(
                "UPDATE sources SET kind='calendar', state='disabled', config_json=? "
                "WHERE id=?",
                (json.dumps(config, sort_keys=True), source_id),
            )

    def test_repairs_legacy_source_and_preserves_schedule_and_history(self) -> None:
        schedule = self.create_schedule()
        route = core.create_route(
            self.db,
            space_id="inbox",
            name="Inbox",
            predicate={"source_id": "timer/inbox"},
            processor="inbox:process",
        )
        event = core.emit_event(
            self.db,
            source_id="timer/inbox",
            external_id="before-repair",
            event_type="inbox.sweep.due",
            attributes={"marker": "history"},
        )
        self.make_legacy_source(adapter="calendar", extra={"marker": "preserve"})
        source_before = self.db.row("SELECT * FROM sources WHERE id='timer/inbox'")
        schedule_before = self.db.row("SELECT * FROM adapter_schedules WHERE id='inbox'")
        counts_before = {
            table: self.db.row(f"SELECT COUNT(*) AS value FROM {table}")["value"]
            for table in ("events", "routes", "route_matches", "processor_runs")
        }

        result = core.repair_calendar_source(self.db, "inbox")

        self.assertTrue(result["repaired"])
        self.assertEqual(result["state"], "repaired")
        self.assertEqual(result["shared_schedule_ids"], ["inbox"])
        self.assertEqual(
            result["before"],
            {"space_id": "inbox", "kind": "calendar", "adapter": "calendar"},
        )
        self.assertEqual(
            result["after"],
            {"space_id": "inbox", "kind": "timer", "adapter": "timer"},
        )
        source_after = self.db.row("SELECT * FROM sources WHERE id='timer/inbox'")
        self.assertEqual(source_after["kind"], "timer")
        self.assertEqual(source_after["state"], "disabled")
        self.assertEqual(source_after["created_at"], source_before["created_at"])
        self.assertEqual(
            json.loads(source_after["config_json"]),
            {"adapter": "timer", "marker": "preserve"},
        )
        self.assertEqual(
            self.db.row("SELECT * FROM adapter_schedules WHERE id='inbox'"),
            schedule_before,
        )
        self.assertEqual(
            core.get_schedule(self.db, "inbox")["revision"], schedule["revision"]
        )
        self.assertEqual(
            core.get_schedule(self.db, "inbox")["next_run_at"], schedule["next_run_at"]
        )
        self.assertEqual(
            core.get_event(self.db, event["event"]["id"])["route_match"]["route_id"],
            route["id"],
        )
        self.assertEqual(
            {
                table: self.db.row(f"SELECT COUNT(*) AS value FROM {table}")["value"]
                for table in counts_before
            },
            counts_before,
        )
        audit = self.db.row(
            "SELECT * FROM audit_log WHERE command='schedule.repair-calendar-source'"
        )
        self.assertEqual(audit["entity_type"], "source")
        self.assertEqual(audit["entity_id"], "timer/inbox")
        self.assertEqual(
            json.loads(audit["payload_json"]),
            {
                "after": {"adapter": "timer", "kind": "timer", "space_id": "inbox"},
                "before": {
                    "adapter": "calendar",
                    "kind": "calendar",
                    "space_id": "inbox",
                },
                "schedule_id": "inbox",
                "shared_schedule_ids": ["inbox"],
                "source_id": "timer/inbox",
            },
        )

    def test_second_repair_is_an_unaudited_no_op(self) -> None:
        self.create_schedule()
        self.make_legacy_source()
        first = core.repair_calendar_source(self.db, "inbox")
        source_after_first = self.db.row("SELECT * FROM sources WHERE id='timer/inbox'")
        audit_count = self.db.row(
            "SELECT COUNT(*) AS value FROM audit_log "
            "WHERE command='schedule.repair-calendar-source'"
        )["value"]

        second = core.repair_calendar_source(self.db, "inbox")

        self.assertTrue(first["repaired"])
        self.assertFalse(second["repaired"])
        self.assertEqual(second["state"], "already-repaired")
        self.assertEqual(
            self.db.row("SELECT * FROM sources WHERE id='timer/inbox'"), source_after_first
        )
        self.assertEqual(
            self.db.row(
                "SELECT COUNT(*) AS value FROM audit_log "
                "WHERE command='schedule.repair-calendar-source'"
            )["value"],
            audit_count,
        )

    def test_allows_compatible_timer_schedules_to_share_the_source(self) -> None:
        self.create_schedule("morning")
        self.create_schedule("evening")
        core.upsert_timer_schedule(
            self.db,
            "heartbeat",
            space_id="inbox",
            source_id="timer/inbox",
            event_type="inbox.heartbeat.due",
            every_seconds=60,
            first_run_at="2026-09-24T04:00:00+00:00",
        )
        self.make_legacy_source()

        result = core.repair_calendar_source(self.db, "morning")

        self.assertEqual(
            result["shared_schedule_ids"], ["evening", "heartbeat", "morning"]
        )

    def test_rejections_roll_back_without_an_audit_record(self) -> None:
        self.create_schedule()
        self.make_legacy_source(extra={"marker": "preserve"})
        source_before = self.db.row("SELECT * FROM sources WHERE id='timer/inbox'")
        with self.db.transaction() as connection:
            connection.execute(
                "INSERT INTO adapter_schedules("
                "id, adapter, config_json, schedule_kind, every_seconds, enabled, "
                "next_run_at, created_at, updated_at) "
                "VALUES('foreign', 'ingest-shadow', ?, 'interval', 60, 1, ?, ?, ?)",
                (
                    json.dumps({"source_id": "timer/inbox", "space_id": "inbox"}),
                    "2026-09-24T04:00:00+00:00",
                    "2026-09-24T04:00:00+00:00",
                    "2026-09-24T04:00:00+00:00",
                ),
            )

        with self.assertRaisesRegex(
            ValueError, "shared with incompatible schedule foreign"
        ):
            core.repair_calendar_source(self.db, "inbox")

        self.assertEqual(
            self.db.row("SELECT * FROM sources WHERE id='timer/inbox'"), source_before
        )
        self.assertIsNone(
            self.db.row(
                "SELECT * FROM audit_log WHERE command='schedule.repair-calendar-source'"
            )
        )

    def test_rejects_missing_wrong_or_unrelated_contract_shapes(self) -> None:
        with self.subTest("missing schedule"):
            with self.assertRaisesRegex(ValueError, "schedule not found: missing"):
                core.repair_calendar_source(self.db, "missing")

        cases = (
            (
                "wrong schedule",
                "UPDATE adapter_schedules SET adapter='ingest-shadow'",
                "not a timer-backed",
            ),
            ("missing source", "DELETE FROM sources", "source not found"),
            ("wrong source space", "UPDATE sources SET space_id='other'", "already bound"),
            ("wrong source kind", "UPDATE sources SET kind='mail'", "does not match"),
            (
                "wrong source adapter",
                "UPDATE sources SET kind='calendar', config_json='{\"adapter\":\"mail\"}'",
                "does not match",
            ),
        )
        for name, statement, message in cases:
            with self.subTest(name):
                directory = tempfile.TemporaryDirectory()
                self.addCleanup(directory.cleanup)
                db = Database(Path(directory.name) / "switchboard.sqlite3")
                core.upsert_calendar_schedule(
                    db,
                    "case",
                    space_id="inbox",
                    source_id="timer/inbox",
                    event_type="inbox.sweep.due",
                    local_time="07:00",
                    timezone="Europe/Warsaw",
                    at="2026-09-24T04:00:00+00:00",
                )
                if name == "wrong source space":
                    core.create_space(db, "other")
                with db.transaction() as connection:
                    connection.execute(statement)
                with self.assertRaisesRegex(ValueError, message):
                    core.repair_calendar_source(db, "case")


if __name__ == "__main__":
    unittest.main()
