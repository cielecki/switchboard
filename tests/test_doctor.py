from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
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


if __name__ == "__main__":
    unittest.main()
