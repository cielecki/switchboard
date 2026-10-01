from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from switchboard import core
from switchboard.db import Database
from switchboard.supervisor import run_cycle


class CalendarOccurrenceOnceTest(unittest.TestCase):
    worker = "chat:claude:inbox-worker"

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "switchboard.sqlite3"
        self.db = Database(self.path)

    def seed_legacy_calendar(
        self,
        *,
        schedule_id: str,
        first_due: str,
        missed_policy: str,
        revision: int = 3,
    ) -> None:
        """Reproduce the deployed pre-contract shape without calling schedule upsert."""
        source_id = f"timer/{schedule_id}"
        timestamp = "2026-09-20T12:00:00+00:00"
        calendar = {
            "ambiguous_time_policy": "first",
            "local_time": "07:00",
            "missed_policy": missed_policy,
            "nonexistent_time_policy": "next-valid",
            "timezone": "Europe/Warsaw",
            "weekdays": ["mon", "tue", "wed", "thu", "fri", "sat", "sun"],
        }
        config = {
            "attributes": {"fixture": "legacy-calendar"},
            "calendar": calendar,
            "event_type": "inbox.sweep.due",
            "source_id": source_id,
            "space_id": "inbox",
        }
        self.db.initialize()
        with self.db.transaction() as connection:
            connection.execute(
                "INSERT INTO spaces(id, name, created_at) VALUES(?,?,?)",
                ("inbox", "Inbox", timestamp),
            )
            connection.execute(
                "INSERT INTO sources(id, space_id, kind, state, config_json, created_at) "
                "VALUES(?,?,'calendar','enabled',?,?)",
                (
                    source_id,
                    "inbox",
                    json.dumps({"adapter": "calendar", "fixture": "preserve"}),
                    timestamp,
                ),
            )
            connection.execute(
                "INSERT INTO adapter_schedules("
                "id, adapter, config_json, schedule_kind, every_seconds, revision, "
                "enabled, next_run_at, created_at, updated_at"
                ") VALUES(?,'timer',?,'calendar',NULL,?,1,?,?,?)",
                (
                    schedule_id,
                    json.dumps(config, sort_keys=True),
                    revision,
                    first_due,
                    timestamp,
                    timestamp,
                ),
            )
        core.create_route(
            self.db,
            space_id="inbox",
            name=f"Process {schedule_id}",
            predicate={
                "source_id": source_id,
                "event_type": "inbox.sweep.due",
            },
            processor="inbox:process",
        )
        core.bind_processor(
            self.db,
            space_id="inbox",
            processor="inbox:process",
            consumer=self.worker,
        )

    def run_at(self, db: Database, at: str) -> dict:
        return run_cycle(
            db,
            relay=None,
            cli_command=["switchboard"],
            at=datetime.fromisoformat(at),
        )

    def assert_one_logical_delivery(
        self,
        db: Database,
        *,
        schedule_id: str,
        scheduled_for: str,
        revision: int = 3,
    ) -> str:
        events = core.list_events(db)
        runs = core.list_processor_runs(db)
        deliveries = core.list_processor_deliveries(db)

        self.assertEqual(len(events), 1)
        self.assertEqual(
            events[0]["external_id"],
            f"schedule:{schedule_id}:r{revision}:{scheduled_for}",
        )
        self.assertEqual(events[0]["occurred_at"], scheduled_for)
        self.assertEqual(events[0]["attributes"]["scheduled_for"], scheduled_for)
        self.assertEqual(events[0]["attributes"]["schedule_revision"], revision)
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["event_id"], events[0]["id"])
        event_detail = core.get_event(db, events[0]["id"])
        self.assertEqual(event_detail["route_match"]["route_id"], runs[0]["route_id"])
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0]["processor_run_id"], runs[0]["id"])
        return runs[0]["id"]

    def test_catch_up_once_survives_reopen_and_supervisor_style_restart(self) -> None:
        self.seed_legacy_calendar(
            schedule_id="personal-inbox",
            first_due="2026-09-21T05:00:00+00:00",
            missed_policy="catch-up-once",
        )
        repaired = core.repair_calendar_source(self.db, "personal-inbox")

        first = self.run_at(self.db, "2026-09-24T12:00:00+00:00")

        self.assertTrue(repaired["repaired"])
        self.assertEqual(first["errors"], [])
        self.assertEqual(first["schedules"][0]["state"], "completed")
        run_id = self.assert_one_logical_delivery(
            self.db,
            schedule_id="personal-inbox",
            scheduled_for="2026-09-24T05:00:00+00:00",
        )
        schedule = core.get_schedule(self.db, "personal-inbox")
        self.assertEqual(schedule["last_scheduled_for"], "2026-09-24T05:00:00+00:00")
        self.assertEqual(schedule["next_run_at"], "2026-09-25T05:00:00+00:00")

        reopened = Database(self.path)
        repeated = self.run_at(reopened, "2026-09-24T12:10:00+00:00")
        restarted = Database(self.path)
        core.recover_interrupted_runs(restarted)
        after_restart = self.run_at(restarted, "2026-09-24T12:20:00+00:00")

        self.assertEqual(repeated["schedules"], [])
        self.assertEqual(after_restart["schedules"], [])
        self.assert_one_logical_delivery(
            restarted,
            schedule_id="personal-inbox",
            scheduled_for="2026-09-24T05:00:00+00:00",
        )
        self.assertEqual(
            core.get_schedule(restarted, "personal-inbox")["next_run_at"],
            "2026-09-25T05:00:00+00:00",
        )

        claimed = core.claim_processor_run(restarted, run_id, worker=self.worker)
        self.assertEqual(claimed["state"], "running")
        completed = core.finish_processor_run(
            restarted, run_id, state="completed", worker=self.worker
        )
        self.assertEqual(completed["state"], "completed")
        self.assertEqual(completed["delivery"]["state"], "acknowledged")
        self.assertIsNone(core.claim_next_processor_run(restarted, worker=self.worker))
        with self.assertRaisesRegex(ValueError, "pending processor run not found"):
            core.claim_processor_run(restarted, run_id, worker=self.worker)

    def test_normal_due_occurrence_creates_one_event_run_and_delivery(self) -> None:
        due = "2026-09-21T05:00:00+00:00"
        self.seed_legacy_calendar(
            schedule_id="work-inbox",
            first_due=due,
            missed_policy="catch-up-once",
        )
        core.repair_calendar_source(self.db, "work-inbox")

        result = self.run_at(self.db, due)

        self.assertEqual(result["errors"], [])
        self.assertEqual(result["schedules"][0]["state"], "completed")
        self.assert_one_logical_delivery(
            self.db,
            schedule_id="work-inbox",
            scheduled_for=due,
        )
        event = core.list_events(self.db)[0]
        self.assertEqual(event["attributes"]["late_by_seconds"], 0)
        self.assertEqual(
            core.get_schedule(self.db, "work-inbox")["next_run_at"],
            (datetime.fromisoformat(due) + timedelta(days=1)).isoformat(),
        )


if __name__ == "__main__":
    unittest.main()
