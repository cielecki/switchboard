from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from switchboard import core
from switchboard.db import Database


class ScheduleSourceContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db = Database(Path(self.directory.name) / "switchboard.sqlite3")

    def test_timer_and_calendar_upserts_provision_timer_sources(self) -> None:
        core.upsert_timer_schedule(
            self.db,
            "interval",
            space_id="interval-space",
            source_id="timer/interval",
            event_type="interval.due",
            every_seconds=60,
        )
        core.upsert_calendar_schedule(
            self.db,
            "calendar",
            space_id="calendar-space",
            source_id="timer/calendar",
            event_type="calendar.due",
            local_time="07:00",
            timezone="Europe/Warsaw",
            at="2026-09-24T04:00:00+00:00",
        )

        sources = {source["id"]: source for source in core.list_sources(self.db)}
        self.assertEqual(sources["timer/interval"]["space_id"], "interval-space")
        self.assertEqual(sources["timer/interval"]["kind"], "timer")
        self.assertEqual(sources["timer/interval"]["state"], "enabled")
        self.assertEqual(sources["timer/calendar"]["space_id"], "calendar-space")
        self.assertEqual(sources["timer/calendar"]["kind"], "timer")
        self.assertEqual(sources["timer/calendar"]["state"], "enabled")
        self.assertEqual(
            {space["id"] for space in core.list_spaces(self.db)},
            {"calendar-space", "interval-space"},
        )

    def test_compatible_existing_timer_source_is_not_modified(self) -> None:
        core.create_space(self.db, "inbox")
        core.register_source(
            self.db,
            "timer/inbox",
            "inbox",
            "timer",
            {"marker": "preserve"},
        )
        core.record_source_health(self.db, "timer/inbox", "degraded", "temporary")
        before = self.db.row("SELECT * FROM sources WHERE id=?", ("timer/inbox",))

        core.upsert_calendar_schedule(
            self.db,
            "inbox",
            space_id="inbox",
            source_id="timer/inbox",
            event_type="inbox.sweep.due",
            local_time="07:00",
            timezone="Europe/Warsaw",
            at="2026-09-24T04:00:00+00:00",
        )

        self.assertEqual(
            self.db.row("SELECT * FROM sources WHERE id=?", ("timer/inbox",)),
            before,
        )

    def test_incompatible_source_space_rejects_add_without_partial_writes(self) -> None:
        core.create_space(self.db, "other")
        core.register_source(self.db, "timer/shared", "other", "timer")

        with self.assertRaisesRegex(
            ValueError, "source timer/shared is already bound to other / timer"
        ):
            core.upsert_timer_schedule(
                self.db,
                "daily",
                space_id="requested",
                source_id="timer/shared",
                event_type="daily.due",
                every_seconds=86400,
            )

        self.assertEqual(core.list_schedules(self.db), [])
        self.assertIsNone(self.db.row("SELECT * FROM spaces WHERE id='requested'"))
        source = self.db.row("SELECT * FROM sources WHERE id='timer/shared'")
        self.assertEqual((source["space_id"], source["kind"]), ("other", "timer"))

    def test_incompatible_source_kind_rejects_update_without_changing_schedule(self) -> None:
        original = core.upsert_calendar_schedule(
            self.db,
            "inbox",
            space_id="inbox",
            source_id="timer/inbox",
            event_type="inbox.sweep.due",
            local_time="07:00",
            timezone="Europe/Warsaw",
            at="2026-09-24T04:00:00+00:00",
        )
        core.register_source(self.db, "legacy/inbox", "inbox", "calendar")

        with self.assertRaisesRegex(
            ValueError, "source legacy/inbox is already bound to inbox / calendar"
        ):
            core.upsert_calendar_schedule(
                self.db,
                "inbox",
                space_id="inbox",
                source_id="legacy/inbox",
                event_type="inbox.sweep.due",
                local_time="08:00",
                timezone="Europe/Warsaw",
                at="2026-09-24T04:30:00+00:00",
            )

        self.assertEqual(core.get_schedule(self.db, "inbox"), original)


if __name__ == "__main__":
    unittest.main()
