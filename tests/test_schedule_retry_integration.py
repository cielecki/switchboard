from __future__ import annotations

import tempfile
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from switchboard import core
from switchboard.db import Database
from switchboard.supervisor import ScheduleWorkers, run_cycle


class ScheduleRetryIntegrationTest(unittest.TestCase):
    worker = "chat:claude:retry-worker"

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "switchboard.sqlite3"
        self.db = Database(self.path)
        self.status_script = Path(self.directory.name) / "status.py"
        self.status_script.touch()

    def _create_pull_schedule(self, schedule_id: str = "pull") -> dict:
        with patch("switchboard.core.now", return_value="2026-10-01T08:00:00+00:00"):
            return core.upsert_ingest_schedule(
                self.db,
                schedule_id,
                status_script=self.status_script,
                every_seconds=60,
            )

    def _run_cycle(
        self,
        db: Database,
        at: str,
        *,
        ingest_runner,
    ) -> dict:
        with patch("switchboard.core.now", return_value=at):
            return run_cycle(
                db,
                relay=None,
                cli_command=["switchboard"],
                at=datetime.fromisoformat(at),
                ingest_runner=ingest_runner,
            )

    def _run_workers(
        self,
        db: Database,
        at: str,
        *,
        ingest_runner,
    ) -> list[dict]:
        """Run one deterministic persistent-worker poll and drain its result."""

        with patch("switchboard.core.now", return_value=at):
            workers = ScheduleWorkers(db, ingest_runner=ingest_runner)
            try:
                results = workers.poll(at)
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline:
                    with workers._active_lock:
                        active = bool(workers._active)
                    if not active:
                        break
                    time.sleep(0.005)
                else:
                    self.fail("schedule worker did not finish")
                results.extend(workers.poll(at))
                return results
            finally:
                workers.shutdown()

    def _adapter_run_count(self, db: Database, schedule_id: str) -> int:
        return db.row(
            "SELECT COUNT(*) AS count FROM adapter_runs WHERE schedule_id=?",
            (schedule_id,),
        )["count"]

    @staticmethod
    def _fail(*_args, **_kwargs):
        raise RuntimeError("controlled outage")

    @staticmethod
    def _succeed(*_args, **_kwargs):
        return {
            "run": {
                "discovered_sources": 1,
                "emitted_events": 0,
                "deduplicated_events": 0,
                "detail": "controlled recovery",
            }
        }

    def test_backoff_survives_reopen_and_worker_restart_through_every_delay(self) -> None:
        created = self._create_pull_schedule()
        with patch("switchboard.core.now", return_value=created["next_run_at"]):
            core.upsert_timer_schedule(
                self.db,
                "healthy",
                space_id="health",
                source_id="timer/healthy",
                event_type="health.due",
                every_seconds=3600,
                first_run_at=created["next_run_at"],
            )

        first = self._run_cycle(
            self.db,
            created["next_run_at"],
            ingest_runner=self._fail,
        )
        self.assertEqual(
            [(item["id"], item["state"]) for item in first["schedules"]],
            [("healthy", "completed"), ("pull", "failed")],
        )
        self.assertEqual(self._adapter_run_count(self.db, "pull"), 1)
        self.assertEqual(len(core.list_events(self.db)), 1)

        failure_at = datetime.fromisoformat(created["next_run_at"])
        expected_delays = [30, 60, 120, 240, 480, 900, 900]
        for streak, delay in enumerate(expected_delays, start=1):
            schedule = core.get_schedule(Database(self.path), "pull")
            deadline = failure_at + timedelta(seconds=delay)
            self.assertEqual(schedule["failure_streak"], streak)
            self.assertEqual(schedule["retry_not_before"], deadline.isoformat())

            count_before = self._adapter_run_count(Database(self.path), "pull")
            quiet_at = (deadline - timedelta(microseconds=1)).isoformat()
            quiet = self._run_workers(
                Database(self.path), quiet_at, ingest_runner=self._fail
            )
            self.assertEqual(quiet, [])
            self.assertEqual(
                self._adapter_run_count(Database(self.path), "pull"), count_before
            )

            if streak == len(expected_delays):
                break
            failure_at = deadline
            retried = self._run_workers(
                Database(self.path), deadline.isoformat(), ingest_runner=self._fail
            )
            self.assertEqual(
                [(item["id"], item["state"]) for item in retried],
                [("pull", "running"), ("pull", "failed")],
            )
            self.assertEqual(
                self._adapter_run_count(Database(self.path), "pull"), count_before + 1
            )

        recovery_at = datetime.fromisoformat(
            core.get_schedule(Database(self.path), "pull")["retry_not_before"]
        )
        recovered = self._run_workers(
            Database(self.path), recovery_at.isoformat(), ingest_runner=self._succeed
        )
        self.assertEqual(
            [(item["id"], item["state"]) for item in recovered],
            [("pull", "running"), ("pull", "completed")],
        )
        schedule = core.get_schedule(Database(self.path), "pull")
        self.assertEqual(schedule["failure_streak"], 0)
        self.assertIsNone(schedule["retry_not_before"])
        self.assertEqual(
            schedule["next_run_at"], (recovery_at + timedelta(seconds=60)).isoformat()
        )
        episodes = Database(self.path).rows(
            "SELECT * FROM schedule_alert_episodes WHERE schedule_id='pull' ORDER BY opened_at"
        )
        self.assertEqual(len(episodes), 1)
        self.assertEqual(episodes[0]["state"], "recovered")
        self.assertEqual(episodes[0]["failure_count"], len(expected_delays))
        self.assertEqual(episodes[0]["recovered_at"], recovery_at.isoformat())

        quiet_at = (recovery_at + timedelta(seconds=59)).isoformat()
        count_after_recovery = self._adapter_run_count(Database(self.path), "pull")
        self.assertEqual(
            self._run_workers(Database(self.path), quiet_at, ingest_runner=self._fail), []
        )
        self.assertEqual(
            self._adapter_run_count(Database(self.path), "pull"), count_after_recovery
        )

        later_due = recovery_at + timedelta(seconds=60)
        later = self._run_workers(
            Database(self.path), later_due.isoformat(), ingest_runner=self._fail
        )
        self.assertEqual(
            [(item["id"], item["state"]) for item in later],
            [("pull", "running"), ("pull", "failed")],
        )
        episodes = Database(self.path).rows(
            "SELECT * FROM schedule_alert_episodes WHERE schedule_id='pull' ORDER BY opened_at"
        )
        self.assertEqual(len(episodes), 2)
        self.assertEqual([item["state"] for item in episodes], ["recovered", "open"])
        self.assertEqual(episodes[1]["failure_count"], 1)

    def _configure_routing(self, *, space_id: str, source_id: str, event_type: str) -> None:
        core.create_route(
            self.db,
            space_id=space_id,
            name=f"Process {source_id}",
            predicate={"source_id": source_id, "event_type": event_type},
            processor="fixture:process",
        )
        core.bind_processor(
            self.db,
            space_id=space_id,
            processor="fixture:process",
            consumer=self.worker,
        )

    def _assert_one_claim_then_none(self, db: Database) -> None:
        runs = core.list_processor_runs(db)
        self.assertEqual(len(runs), 1)
        run_id = runs[0]["id"]
        with patch("switchboard.core.now", return_value="2026-10-01T12:00:00+00:00"):
            claimed = core.claim_next_processor_run(db, worker=self.worker)
            self.assertEqual(claimed["id"], run_id)
            completed = core.finish_processor_run(
                db,
                run_id,
                state="completed",
                summary="fixture completed",
                worker=self.worker,
            )
        self.assertEqual(completed["delivery"]["state"], "acknowledged")
        restarted = Database(self.path)
        self.assertIsNone(core.claim_next_processor_run(restarted, worker=self.worker))
        with self.assertRaisesRegex(ValueError, "pending processor run not found"):
            core.claim_processor_run(restarted, run_id, worker=self.worker)

    def test_recovered_interval_timer_occurrence_is_routed_exactly_once(self) -> None:
        due = "2026-10-01T08:00:00+00:00"
        with patch("switchboard.core.now", return_value=due):
            created = core.upsert_timer_schedule(
                self.db,
                "interval",
                space_id="timer-space",
                source_id="timer/interval",
                event_type="timer.due",
                every_seconds=3600,
                first_run_at=due,
            )
            self._configure_routing(
                space_id="timer-space",
                source_id="timer/interval",
                event_type="timer.due",
            )
        core.mark_timer_schedule_failed(
            self.db, "interval", "controlled failure", failed_at=due
        )

        recovered_at = "2026-10-01T08:00:30+00:00"
        result = self._run_cycle(
            Database(self.path), recovered_at, ingest_runner=self._succeed
        )
        self.assertEqual(result["schedules"][0]["state"], "completed")
        event = core.list_events(Database(self.path))[0]
        self.assertEqual(
            event["external_id"],
            f"schedule:interval:r{created['revision']}:{due}",
        )
        self.assertEqual(event["attributes"]["scheduled_for"], due)
        self.assertEqual(event["attributes"]["schedule_id"], "interval")
        self.assertEqual(event["attributes"]["schedule_revision"], created["revision"])
        self.assertEqual(event["attributes"]["triggered_at"], recovered_at)
        self.assertEqual(event["attributes"]["late_by_seconds"], 30)
        self.assertEqual(len(core.list_processor_runs(Database(self.path))), 1)
        self.assertEqual(len(core.list_processor_deliveries(Database(self.path))), 1)

        next_run = core.get_schedule(Database(self.path), "interval")["next_run_at"]
        repeated = self._run_cycle(
            Database(self.path),
            "2026-10-01T08:59:59+00:00",
            ingest_runner=self._succeed,
        )
        self.assertEqual(repeated["schedules"], [])
        self.assertEqual(next_run, "2026-10-01T09:00:00+00:00")
        self.assertEqual(len(core.list_events(Database(self.path))), 1)
        self._assert_one_claim_then_none(Database(self.path))

    def test_recovered_calendar_catch_up_occurrence_is_routed_exactly_once(self) -> None:
        core.upsert_calendar_schedule(
            self.db,
            "calendar",
            space_id="calendar-space",
            source_id="timer/calendar",
            event_type="calendar.due",
            local_time="07:00",
            timezone="Europe/Warsaw",
            missed_policy="catch-up-once",
            at="2026-09-20T12:00:00+00:00",
        )
        self._configure_routing(
            space_id="calendar-space",
            source_id="timer/calendar",
            event_type="calendar.due",
        )
        due = "2026-09-21T05:00:00+00:00"
        core.mark_calendar_schedule_failed(
            self.db, "calendar", "controlled failure", failed_at=due
        )

        recovered_at = "2026-09-24T12:00:00+00:00"
        result = self._run_cycle(
            Database(self.path), recovered_at, ingest_runner=self._succeed
        )
        self.assertEqual(result["schedules"][0]["state"], "completed")
        schedule = core.get_schedule(Database(self.path), "calendar")
        scheduled_for = "2026-09-24T05:00:00+00:00"
        event = core.list_events(Database(self.path))[0]
        self.assertEqual(
            event["external_id"],
            f"schedule:calendar:r{schedule['revision']}:{scheduled_for}",
        )
        self.assertEqual(event["occurred_at"], scheduled_for)
        self.assertEqual(len(core.list_processor_runs(Database(self.path))), 1)
        self.assertEqual(len(core.list_processor_deliveries(Database(self.path))), 1)

        repeated = self._run_cycle(
            Database(self.path),
            "2026-09-24T12:10:00+00:00",
            ingest_runner=self._succeed,
        )
        self.assertEqual(repeated["schedules"], [])
        self.assertEqual(len(core.list_events(Database(self.path))), 1)
        self._assert_one_claim_then_none(Database(self.path))

    def test_failed_pull_finalization_rolls_back_terminal_run_and_retry_state(self) -> None:
        self._create_pull_schedule()
        attempt = core.start_scheduled_adapter_run(
            self.db, "pull", started_at="2026-10-01T08:00:00+00:00"
        )

        with patch(
            "switchboard.core._record_schedule_failure",
            side_effect=RuntimeError("injected failure rollback"),
        ):
            with self.assertRaisesRegex(RuntimeError, "injected failure rollback"):
                core.finish_scheduled_adapter_run(
                    self.db,
                    "pull",
                    attempt["id"],
                    state="failed",
                    error="offline",
                    finished_at="2026-10-01T08:00:00+00:00",
                )

        run = self.db.row("SELECT * FROM adapter_runs WHERE id=?", (attempt["id"],))
        schedule = core.get_schedule(self.db, "pull")
        self.assertEqual(run["state"], "running")
        self.assertEqual(schedule["failure_streak"], 0)
        self.assertIsNone(schedule["retry_not_before"])
        self.assertEqual(self.db.rows("SELECT * FROM schedule_alert_episodes"), [])

    def test_occurrence_failure_rolls_back_event_cursor_reset_and_recovery(self) -> None:
        due = "2026-10-01T08:00:00+00:00"
        with patch("switchboard.core.now", return_value=due):
            core.upsert_timer_schedule(
                self.db,
                "atomic-timer",
                space_id="atomic",
                source_id="timer/atomic",
                event_type="atomic.due",
                every_seconds=60,
                first_run_at=due,
            )
        core.mark_timer_schedule_failed(
            self.db, "atomic-timer", "temporary failure", failed_at=due
        )
        before = core.get_schedule(self.db, "atomic-timer")

        with patch("switchboard.core.audit", side_effect=RuntimeError("injected rollback")):
            with self.assertRaisesRegex(RuntimeError, "injected rollback"):
                core.execute_due_timer_schedule(
                    self.db,
                    "atomic-timer",
                    triggered_at="2026-10-01T08:00:30+00:00",
                )

        after = core.get_schedule(self.db, "atomic-timer")
        episode = self.db.row(
            "SELECT * FROM schedule_alert_episodes WHERE schedule_id='atomic-timer'"
        )
        self.assertEqual(core.list_events(self.db), [])
        self.assertEqual(self._adapter_run_count(self.db, "atomic-timer"), 1)
        self.assertEqual(after["next_run_at"], before["next_run_at"])
        self.assertEqual(after["failure_streak"], 1)
        self.assertEqual(after["retry_not_before"], before["retry_not_before"])
        self.assertEqual(episode["state"], "open")
        self.assertIsNone(episode["recovered_at"])


if __name__ == "__main__":
    unittest.main()
