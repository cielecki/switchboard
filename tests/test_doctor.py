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
