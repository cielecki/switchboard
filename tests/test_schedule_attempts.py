from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from switchboard import core
from switchboard.db import Database
from switchboard.supervisor import run_cycle


class ScheduledAttemptTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.db = Database(self.root / "switchboard.sqlite3")
        self.status_script = self.root / "status.py"
        self.status_script.touch()

    def create_ingest(self, *, at: str = "2026-10-01T08:00:00+00:00") -> dict:
        with patch("switchboard.core.now", return_value=at):
            return core.upsert_ingest_schedule(
                self.db,
                "ingest",
                status_script=self.status_script,
                every_seconds=60,
            )

    def run_ingest_at(self, at: str, **kwargs) -> dict:
        with patch("switchboard.core.now", return_value=at):
            return run_cycle(
                self.db,
                relay=None,
                cli_command=["switchboard"],
                at=datetime.fromisoformat(at),
                **kwargs,
            )

    def test_failure_and_retry_share_one_episode_and_one_run_per_attempt(self) -> None:
        created = self.create_ingest()
        alerts: list[dict] = []

        first = self.run_ingest_at(
            created["next_run_at"],
            alert_command=["/unused-alert"],
            alert_runner=lambda _command, payload: alerts.append(payload) or {},
        )
        schedule = core.get_schedule(self.db, "ingest")
        episodes = self.db.rows("SELECT * FROM schedule_alert_episodes")

        self.assertEqual(first["schedules"][0]["state"], "failed")
        self.assertEqual(len(core.list_adapter_runs(self.db)), 1)
        self.assertEqual(schedule["next_run_at"], created["next_run_at"])
        self.assertEqual(schedule["failure_streak"], 1)
        self.assertEqual(schedule["retry_not_before"], "2026-10-01T08:00:30+00:00")
        self.assertEqual(len(episodes), 1)
        self.assertEqual(episodes[0]["state"], "open")
        self.assertEqual(episodes[0]["failure_count"], 1)
        self.assertEqual(alerts, [])

        before_deadline = self.run_ingest_at("2026-10-01T08:00:29+00:00")
        self.assertEqual(before_deadline["schedules"], [])
        self.assertEqual(len(core.list_adapter_runs(self.db)), 1)

        second = self.run_ingest_at("2026-10-01T08:00:30+00:00")
        schedule = core.get_schedule(self.db, "ingest")
        repeated_episode = self.db.rows("SELECT * FROM schedule_alert_episodes")
        self.assertEqual(second["schedules"][0]["state"], "failed")
        self.assertEqual(len(core.list_adapter_runs(self.db)), 2)
        self.assertEqual(schedule["failure_streak"], 2)
        self.assertEqual(schedule["retry_not_before"], "2026-10-01T08:01:30+00:00")
        self.assertEqual(len(repeated_episode), 1)
        self.assertEqual(repeated_episode[0]["id"], episodes[0]["id"])
        self.assertEqual(repeated_episode[0]["failure_count"], 2)

    def test_success_atomically_terminalizes_run_and_recovers_episode_once(self) -> None:
        self.create_ingest()
        self.run_ingest_at("2026-10-01T08:00:00+00:00")
        received: list[dict] = []

        def succeed(_db, **kwargs):
            received.append(kwargs)
            return {
                "run": {
                    "discovered_sources": 2,
                    "emitted_events": 3,
                    "deduplicated_events": 1,
                }
            }

        result = self.run_ingest_at(
            "2026-10-01T08:00:30+00:00", ingest_runner=succeed
        )
        schedule = core.get_schedule(self.db, "ingest")
        runs = core.list_adapter_runs(self.db)
        episode = self.db.row("SELECT * FROM schedule_alert_episodes")

        self.assertEqual(result["schedules"][0]["state"], "completed")
        self.assertFalse(received[0]["finalize_run"])
        self.assertEqual(received[0]["run_id"], runs[0]["id"])
        self.assertEqual(runs[0]["state"], "completed")
        self.assertEqual(runs[0]["discovered_sources"], 2)
        self.assertEqual(runs[0]["emitted_events"], 3)
        self.assertEqual(runs[0]["deduplicated_events"], 1)
        self.assertEqual(schedule["failure_streak"], 0)
        self.assertIsNone(schedule["retry_not_before"])
        self.assertEqual(schedule["next_run_at"], "2026-10-01T08:01:30+00:00")
        self.assertEqual(episode["state"], "recovered")
        self.assertEqual(episode["recovered_at"], "2026-10-01T08:00:30+00:00")
        self.assertEqual(episode["recovery_reason"], "scheduled-success")

        quiet = self.run_ingest_at(
            "2026-10-01T08:00:31+00:00", ingest_runner=succeed
        )
        self.assertEqual(quiet["schedules"], [])
        self.assertEqual(len(received), 1)
        self.assertEqual(
            self.db.row("SELECT recovered_at FROM schedule_alert_episodes")["recovered_at"],
            "2026-10-01T08:00:30+00:00",
        )

    def test_failed_finalization_rolls_back_run_schedule_and_episode_together(self) -> None:
        self.create_ingest()
        attempt = core.start_scheduled_adapter_run(
            self.db, "ingest", started_at="2026-10-01T08:00:00+00:00"
        )

        with patch(
            "switchboard.core._record_schedule_failure",
            side_effect=RuntimeError("injected rollback"),
        ):
            with self.assertRaisesRegex(RuntimeError, "injected rollback"):
                core.finish_scheduled_adapter_run(
                    self.db,
                    "ingest",
                    attempt["id"],
                    state="failed",
                    error="offline",
                    finished_at="2026-10-01T08:00:00+00:00",
                )

        run = self.db.row("SELECT * FROM adapter_runs WHERE id=?", (attempt["id"],))
        schedule = core.get_schedule(self.db, "ingest")
        self.assertEqual(run["state"], "running")
        self.assertIsNone(run["completed_at"])
        self.assertEqual(schedule["failure_streak"], 0)
        self.assertIsNone(schedule["retry_not_before"])
        self.assertEqual(self.db.rows("SELECT * FROM schedule_alert_episodes"), [])

    def test_restart_recovers_supervisor_owned_attempt_into_backoff(self) -> None:
        self.create_ingest()
        attempt = core.start_scheduled_adapter_run(
            self.db, "ingest", started_at="2026-10-01T08:00:00+00:00"
        )

        reopened = Database(self.db.path)
        with patch("switchboard.core.now", return_value="2026-10-01T08:00:10+00:00"):
            recovered = core.recover_interrupted_runs(reopened)

        run = reopened.row("SELECT * FROM adapter_runs WHERE id=?", (attempt["id"],))
        schedule = core.get_schedule(reopened, "ingest")
        episode = reopened.row("SELECT * FROM schedule_alert_episodes")
        self.assertEqual(recovered, 1)
        self.assertEqual(run["state"], "failed")
        self.assertEqual(run["schedule_id"], "ingest")
        self.assertEqual(schedule["failure_streak"], 1)
        self.assertEqual(schedule["retry_not_before"], "2026-10-01T08:00:40+00:00")
        self.assertEqual(episode["state"], "open")
        self.assertEqual(episode["failure_count"], 1)

    def test_one_failed_schedule_does_not_block_another_due_schedule(self) -> None:
        self.create_ingest()
        with patch("switchboard.core.now", return_value="2026-10-01T08:00:00+00:00"):
            core.upsert_timer_schedule(
                self.db,
                "healthy",
                space_id="demo",
                source_id="timer/healthy",
                event_type="demo.due",
                every_seconds=60,
                first_run_at="2026-10-01T08:00:00+00:00",
            )

        result = self.run_ingest_at("2026-10-01T08:00:00+00:00")

        self.assertEqual(
            [(item["id"], item["state"]) for item in result["schedules"]],
            [("healthy", "completed"), ("ingest", "failed")],
        )
        self.assertEqual(len(core.list_events(self.db)), 1)
        self.assertEqual(core.get_schedule(self.db, "healthy")["last_state"], "completed")
        self.assertEqual(core.get_schedule(self.db, "ingest")["failure_streak"], 1)

    def test_timer_event_run_cursor_reset_and_recovery_share_one_transaction(self) -> None:
        with patch("switchboard.core.now", return_value="2026-10-01T08:00:00+00:00"):
            core.upsert_timer_schedule(
                self.db,
                "timer",
                space_id="demo",
                source_id="timer/demo",
                event_type="demo.due",
                every_seconds=60,
                first_run_at="2026-10-01T08:00:00+00:00",
            )
        core.mark_timer_schedule_failed(
            self.db,
            "timer",
            "temporary failure",
            failed_at="2026-10-01T08:00:00+00:00",
        )

        result = self.run_ingest_at("2026-10-01T08:00:30+00:00")
        schedule = core.get_schedule(self.db, "timer")
        episode = self.db.row("SELECT * FROM schedule_alert_episodes")

        self.assertEqual(result["schedules"][0]["state"], "completed")
        self.assertEqual(len(core.list_events(self.db)), 1)
        self.assertEqual(len(core.list_adapter_runs(self.db)), 2)
        self.assertEqual(schedule["next_run_at"], "2026-10-01T08:01:00+00:00")
        self.assertEqual(schedule["failure_streak"], 0)
        self.assertEqual(episode["state"], "recovered")
        self.assertEqual(episode["recovery_reason"], "scheduled-success")

    def test_calendar_occurrence_clears_retry_and_recovers_the_episode(self) -> None:
        core.upsert_calendar_schedule(
            self.db,
            "calendar",
            space_id="demo",
            source_id="timer/calendar",
            event_type="demo.due",
            local_time="07:00",
            timezone="Europe/Warsaw",
            at="2026-09-20T12:00:00+00:00",
        )
        core.mark_calendar_schedule_failed(
            self.db,
            "calendar",
            "temporary failure",
            failed_at="2026-09-21T06:00:00+00:00",
        )

        result = self.run_ingest_at("2026-09-21T06:00:30+00:00")
        schedule = core.get_schedule(self.db, "calendar")
        episode = self.db.row("SELECT * FROM schedule_alert_episodes")

        self.assertEqual(result["schedules"][0]["state"], "completed")
        self.assertEqual(len(core.list_events(self.db)), 1)
        self.assertEqual(len(core.list_adapter_runs(self.db)), 2)
        self.assertEqual(schedule["failure_streak"], 0)
        self.assertIsNone(schedule["retry_not_before"])
        self.assertEqual(schedule["next_run_at"], "2026-09-22T05:00:00+00:00")
        self.assertEqual(episode["state"], "recovered")
        self.assertEqual(episode["recovered_at"], "2026-09-21T06:00:30+00:00")


if __name__ == "__main__":
    unittest.main()
