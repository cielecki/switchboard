from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from switchboard import core
from switchboard.backup import create_backup, verify_backup
from switchboard.db import SCHEMA_VERSION, Database


class BackupTest(unittest.TestCase):
    def test_online_backup_is_verified_and_never_overwrites(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = Database(root / "live.sqlite3")
            core.create_space(db, "demo")
            target = root / "backups" / "switchboard.sqlite3"

            created = create_backup(db, target)
            verified = verify_backup(target)

            self.assertEqual(created["integrity"], "ok")
            self.assertEqual(verified["schema_version"], SCHEMA_VERSION)
            self.assertEqual(
                Database(target).rows("SELECT id FROM spaces"), [{"id": "demo"}]
            )
            with self.assertRaisesRegex(ValueError, "already exists"):
                create_backup(db, target)

    def test_backup_does_not_migrate_an_existing_database_first(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "live.sqlite3"
            Database(path).initialize()
            with sqlite3.connect(path) as connection:
                connection.execute(
                    "UPDATE schema_meta SET value='5' WHERE key='schema_version'"
                )

            created = create_backup(Database(path), root / "before-upgrade.sqlite3")

            with sqlite3.connect(path) as connection:
                live_version = connection.execute(
                    "SELECT value FROM schema_meta WHERE key='schema_version'"
                ).fetchone()[0]
            self.assertEqual(created["schema_version"], 5)
            self.assertEqual(live_version, "5")


if __name__ == "__main__":
    unittest.main()
