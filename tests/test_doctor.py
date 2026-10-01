from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from switchboard import core
from switchboard.db import Database
from switchboard.doctor import run_doctor


class DoctorTest(unittest.TestCase):
    def test_clean_database_is_ok_without_a_platform_service(self) -> None:
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("switchboard.doctor.sys.platform", "linux"),
        ):
            result = run_doctor(Database(Path(directory) / "switchboard.sqlite3"))
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["errors"], 0)

    def test_missing_schedule_path_is_an_error(self) -> None:
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("switchboard.doctor.sys.platform", "linux"),
        ):
            root = Path(directory)
            script = root / "status.py"
            script.touch()
            db = Database(root / "switchboard.sqlite3")
            core.upsert_ingest_schedule(
                db, "ingest", status_script=script, every_seconds=60
            )
            script.unlink()
            result = run_doctor(db)
        self.assertEqual(result["status"], "error")
        self.assertIn(
            "schedule.path-missing", {item["code"] for item in result["findings"]}
        )

    def test_calendar_schedule_uses_a_fixed_overdue_grace(self) -> None:
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("switchboard.doctor.sys.platform", "linux"),
        ):
            db = Database(Path(directory) / "switchboard.sqlite3")
            core.upsert_calendar_schedule(
                db,
                "daily",
                space_id="demo",
                source_id="timer/daily",
                event_type="daily.due",
                local_time="07:00",
                timezone="Europe/Warsaw",
                at="2026-09-24T04:00:00+00:00",
            )
            result = run_doctor(
                db, now_at=datetime.fromisoformat("2026-09-24T07:00:01+00:00")
            )
        self.assertIn("schedule.overdue", {item["code"] for item in result["findings"]})

    def test_timer_source_mismatches_are_errors_even_when_schedules_are_disabled(self) -> None:
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("switchboard.doctor.sys.platform", "linux"),
        ):
            db = Database(Path(directory) / "switchboard.sqlite3")
            for schedule_id, enabled in (
                ("wrong-kind", False),
                ("wrong-space", True),
                ("missing", False),
            ):
                core.upsert_calendar_schedule(
                    db,
                    schedule_id,
                    space_id="inbox",
                    source_id=f"timer/{schedule_id}",
                    event_type="inbox.sweep.due",
                    local_time="07:00",
                    timezone="Europe/Warsaw",
                    enabled=enabled,
                    at="2026-09-24T04:00:00+00:00",
                )
            core.create_space(db, "other")
            with db.transaction() as connection:
                connection.execute(
                    "UPDATE sources SET kind='calendar' WHERE id='timer/wrong-kind'"
                )
                connection.execute(
                    "UPDATE sources SET space_id='other' WHERE id='timer/wrong-space'"
                )
                connection.execute("DELETE FROM sources WHERE id='timer/missing'")

            result = run_doctor(
                db, now_at=datetime.fromisoformat("2026-09-24T04:30:00+00:00")
            )

        findings = {
            finding["schedule_id"]: finding
            for finding in result["findings"]
            if finding["code"] == "schedule.timer-source-mismatch"
        }
        self.assertEqual(result["status"], "error")
        self.assertEqual(set(findings), {"missing", "wrong-kind", "wrong-space"})
        self.assertEqual(findings["missing"]["observed_state"], "missing")
        self.assertEqual(findings["wrong-kind"]["expected_kind"], "timer")
        self.assertEqual(findings["wrong-kind"]["observed_kind"], "calendar")
        self.assertEqual(findings["wrong-space"]["expected_space_id"], "inbox")
        self.assertEqual(findings["wrong-space"]["observed_space_id"], "other")

    def test_running_stream_is_not_reported_as_overdue(self) -> None:
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("switchboard.doctor.sys.platform", "linux"),
        ):
            root = Path(directory)
            executable = root / "stream"
            executable.touch()
            db = Database(root / "switchboard.sqlite3")
            core.upsert_stream_schedule(
                db, "socket", command=[str(executable)], every_seconds=5
            )
            core.mark_schedule_started(db, "socket", "2026-09-24T05:00:00+00:00")
            result = run_doctor(
                db, now_at=datetime.fromisoformat("2026-09-24T07:00:01+00:00")
            )
        self.assertNotIn("schedule.overdue", {item["code"] for item in result["findings"]})

    def test_schedule_backoff_is_reported_without_a_redundant_overdue_warning(self) -> None:
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("switchboard.doctor.sys.platform", "linux"),
            patch("switchboard.core.now", return_value="2026-10-01T06:00:00+00:00"),
        ):
            root = Path(directory)
            script = root / "status.py"
            script.touch()
            db = Database(root / "switchboard.sqlite3")
            core.upsert_ingest_schedule(
                db, "ingest", status_script=script, every_seconds=60
            )
            run = core.start_scheduled_adapter_run(
                db, "ingest", started_at="2026-10-01T08:00:00+00:00"
            )
            core.finish_scheduled_adapter_run(
                db,
                "ingest",
                run["id"],
                state="failed",
                error="mailbox offline",
                finished_at="2026-10-01T08:00:00+00:00",
            )
            result = run_doctor(
                db, now_at=datetime.fromisoformat("2026-10-01T08:00:10+00:00")
            )

        findings = {item["code"]: item for item in result["findings"]}
        self.assertIn("schedule.retry-backoff", findings)
        self.assertNotIn("schedule.last-run-failed", findings)
        self.assertNotIn("schedule.overdue", findings)
        self.assertEqual(findings["schedule.retry-backoff"]["failure_streak"], 1)
        self.assertEqual(
            findings["schedule.retry-backoff"]["retry_not_before"],
            "2026-10-01T08:00:30+00:00",
        )
        self.assertEqual(result["queues"]["open_schedule_alert_episodes"], 1)

    def test_inconsistent_schedule_retry_state_is_an_error(self) -> None:
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("switchboard.doctor.sys.platform", "linux"),
            patch("switchboard.core.now", return_value="2026-10-01T08:00:00+00:00"),
        ):
            root = Path(directory)
            script = root / "status.py"
            script.touch()
            db = Database(root / "switchboard.sqlite3")
            core.upsert_ingest_schedule(
                db, "ingest", status_script=script, every_seconds=60
            )
            with db.transaction() as connection:
                connection.execute(
                    "UPDATE adapter_schedules SET failure_streak=1 WHERE id='ingest'"
                )
            result = run_doctor(
                db, now_at=datetime.fromisoformat("2026-10-01T08:00:00+00:00")
            )

        finding = next(
            item
            for item in result["findings"]
            if item["code"] == "schedule.retry-state-inconsistent"
        )
        self.assertEqual(result["status"], "error")
        self.assertEqual(finding["schedule_id"], "ingest")
        self.assertIn(
            "failure streak has no retry deadline", finding["inconsistencies"]
        )

    def test_unclaimed_accepted_wake_is_a_warning(self) -> None:
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("switchboard.doctor.sys.platform", "linux"),
        ):
            root = Path(directory)
            db = Database(root / "switchboard.sqlite3")
            core.create_space(db, "demo")
            core.register_source(db, "mail", "demo", "mail")
            core.create_route(
                db,
                space_id="demo",
                name="triage",
                predicate={"event_type": "message.received"},
                processor="mail-triage",
            )
            core.bind_processor(
                db,
                space_id="demo",
                processor="mail-triage",
                consumer="chat:claude:session-1",
            )
            emitted = core.emit_event(
                db,
                source_id="mail",
                external_id="message-1",
                event_type="message.received",
                attributes={},
            )
            delivery = core.list_processor_deliveries(db)[0]
            relay = root / "send-message.py"
            relay.touch()
            core.dispatch_processor_delivery(
                db,
                delivery["id"],
                relay=relay,
                cli_command=["switchboard"],
                runner=lambda *_args, **_kwargs: SimpleNamespace(
                    returncode=2,
                    stdout=(
                        '{"status":"timeout","delivery_status":"accepted",'
                        '"receipt_status":null}'
                    ),
                    stderr="",
                ),
            )
            accepted_at = datetime.fromisoformat(
                core.get_processor_run(db, emitted["processor_runs"][0])["delivery"][
                    "accepted_at"
                ]
            )
            result = run_doctor(db, now_at=accepted_at + timedelta(seconds=121))
        self.assertEqual(result["status"], "warning")
        self.assertIn(
            "processor.unclaimed-wake", {item["code"] for item in result["findings"]}
        )


if __name__ == "__main__":
    unittest.main()
