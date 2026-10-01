from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from switchboard import core
from switchboard.db import Database


class ScheduleRetryStateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.db = Database(self.root / "switchboard.sqlite3")
        self.status_script = self.root / "status.py"
        self.status_script.touch()

    def _create_ingest(self, *, enabled: bool = True, timeout: int = 120) -> dict:
        with patch("switchboard.core.now", return_value="2026-10-01T08:00:00+00:00"):
            return core.upsert_ingest_schedule(
                self.db,
                "ingest",
                status_script=self.status_script,
                every_seconds=60,
                timeout=timeout,
                enabled=enabled,
            )

    def _seed_retry(
        self,
        schedule_id: str,
        *,
        episode_id: str = "schedule-alert-1",
        retry_not_before: str = "2026-10-01T08:00:30+00:00",
    ) -> None:
        with self.db.transaction() as connection:
            connection.execute(
                "UPDATE adapter_schedules SET failure_streak=2, "
                "last_failure_at='2026-10-01T08:00:00+00:00', "
                "last_failure_detail='temporarily unavailable', retry_not_before=? "
                "WHERE id=?",
                (retry_not_before, schedule_id),
            )
            connection.execute(
                "INSERT INTO schedule_alert_episodes("
                "id, schedule_id, state, opened_at, updated_at, failure_count, "
                "last_failure_at, detail) VALUES(?,?,'open',?,?,?,?,?)",
                (
                    episode_id,
                    schedule_id,
                    "2026-10-01T08:00:00+00:00",
                    "2026-10-01T08:00:00+00:00",
                    2,
                    "2026-10-01T08:00:00+00:00",
                    "temporarily unavailable",
                ),
            )

    def test_retry_delay_sequence_is_fixed_and_capped(self) -> None:
        self.assertEqual(
            [core.schedule_retry_delay_seconds(streak) for streak in range(1, 9)],
            [30, 60, 120, 240, 480, 900, 900, 900],
        )
        with self.assertRaisesRegex(ValueError, "at least one"):
            core.schedule_retry_delay_seconds(0)

    def test_due_selection_waits_for_persisted_deadline_after_reopen(self) -> None:
        self._create_ingest()
        self._seed_retry("ingest")

        self.assertEqual(
            core.due_schedules(self.db, "2026-10-01T08:00:29+00:00"), []
        )
        reopened = Database(self.db.path)
        due = core.due_schedules(reopened, "2026-10-01T08:00:30+00:00")
        self.assertEqual([item["id"] for item in due], ["ingest"])
        self.assertEqual(due[0]["retry"]["state"], "active")
        self.assertEqual(
            due[0]["retry"]["episode"]["id"], "schedule-alert-1"
        )

    def test_stream_schedule_is_not_gated_by_retry_state(self) -> None:
        with patch("switchboard.core.now", return_value="2026-10-01T08:00:00+00:00"):
            core.upsert_stream_schedule(
                self.db,
                "stream",
                command=[sys.executable, "-c", "print('{}')"],
            )
        self._seed_retry(
            "stream",
            retry_not_before="2026-10-01T09:00:00+00:00",
        )

        self.assertEqual(
            [
                item["id"]
                for item in core.due_schedules(
                    self.db, "2026-10-01T08:00:00+00:00"
                )
            ],
            ["stream"],
        )

    def test_identical_upsert_preserves_retry_and_material_change_recovers(self) -> None:
        created = self._create_ingest()
        self._seed_retry("ingest")

        with patch("switchboard.core.now", return_value="2026-10-01T08:05:00+00:00"):
            identical = core.upsert_ingest_schedule(
                self.db,
                "ingest",
                status_script=self.status_script,
                every_seconds=60,
            )
        self.assertEqual(identical["failure_streak"], 2)
        self.assertEqual(identical["retry_not_before"], "2026-10-01T08:00:30+00:00")
        self.assertEqual(identical["next_run_at"], created["next_run_at"])
        self.assertEqual(
            self.db.row(
                "SELECT state FROM schedule_alert_episodes WHERE id='schedule-alert-1'"
            )["state"],
            "open",
        )

        with patch("switchboard.core.now", return_value="2026-10-01T08:06:00+00:00"):
            changed = core.upsert_ingest_schedule(
                self.db,
                "ingest",
                status_script=self.status_script,
                every_seconds=60,
                timeout=121,
            )
        self.assertEqual(changed["failure_streak"], 0)
        self.assertIsNone(changed["last_failure_at"])
        self.assertIsNone(changed["last_failure_detail"])
        self.assertIsNone(changed["retry_not_before"])
        episode = self.db.row(
            "SELECT * FROM schedule_alert_episodes WHERE id='schedule-alert-1'"
        )
        self.assertEqual(episode["state"], "recovered")
        self.assertEqual(episode["recovered_at"], "2026-10-01T08:06:00+00:00")
        self.assertEqual(episode["recovery_reason"], "material-update")

    def test_disable_preserves_retry_and_reenable_resets_without_backfill(self) -> None:
        self._create_ingest()
        self._seed_retry("ingest")

        with patch("switchboard.core.now", return_value="2026-10-01T08:10:00+00:00"):
            disabled = core.set_schedule_enabled(self.db, "ingest", False)
        self.assertEqual(disabled["failure_streak"], 2)
        self.assertEqual(disabled["next_run_at"], "2026-10-01T08:00:00+00:00")

        with patch("switchboard.core.now", return_value="2026-10-01T09:00:00+00:00"):
            enabled = core.set_schedule_enabled(self.db, "ingest", True)
        self.assertEqual(enabled["failure_streak"], 0)
        self.assertIsNone(enabled["retry_not_before"])
        self.assertEqual(enabled["next_run_at"], "2026-10-01T09:00:00+00:00")
        episode = self.db.row(
            "SELECT * FROM schedule_alert_episodes WHERE id='schedule-alert-1'"
        )
        self.assertEqual(episode["state"], "recovered")
        self.assertEqual(episode["recovery_reason"], "re-enabled")

    def test_calendar_material_update_and_reenable_reset_retry(self) -> None:
        arguments = {
            "space_id": "demo",
            "source_id": "timer/calendar",
            "event_type": "calendar.due",
            "local_time": "07:00",
            "timezone": "Europe/Warsaw",
            "at": "2026-10-01T08:00:00+00:00",
        }
        core.upsert_calendar_schedule(self.db, "calendar", **arguments)
        self._seed_retry("calendar")

        identical = core.upsert_calendar_schedule(self.db, "calendar", **arguments)
        self.assertEqual(identical["failure_streak"], 2)
        changed = core.upsert_calendar_schedule(
            self.db, "calendar", **{**arguments, "local_time": "08:00"}
        )
        self.assertEqual(changed["failure_streak"], 0)

        self._seed_retry("calendar", episode_id="schedule-alert-2")
        with patch("switchboard.core.now", return_value="2026-10-01T10:00:00+00:00"):
            core.set_schedule_enabled(self.db, "calendar", False)
            enabled = core.set_schedule_enabled(self.db, "calendar", True)
        self.assertEqual(enabled["failure_streak"], 0)
        self.assertGreater(enabled["next_run_at"], "2026-10-01T10:00:00+00:00")
        self.assertEqual(
            self.db.row(
                "SELECT recovery_reason FROM schedule_alert_episodes "
                "WHERE id='schedule-alert-2'"
            )["recovery_reason"],
            "re-enabled",
        )


if __name__ == "__main__":
    unittest.main()
