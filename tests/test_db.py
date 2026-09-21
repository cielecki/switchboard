from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from typing import Any

from switchboard import core
from switchboard.db import SCHEMA_VERSION, Database


class TrackingConnection:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        self.closed = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self.connection, name)

    def close(self) -> None:
        self.connection.close()
        self.closed = True


class TrackingDatabase(Database):
    def __init__(self, path: Path):
        super().__init__(path)
        self.connections: list[TrackingConnection] = []

    def connect(self) -> TrackingConnection:
        connection = TrackingConnection(super().connect())
        self.connections.append(connection)
        return connection


class ImpatientDatabase(Database):
    """Fails at once, instead of after 30 s, on any statement that needs a held lock."""

    def connect(self) -> sqlite3.Connection:
        connection = super().connect()
        connection.execute("PRAGMA busy_timeout = 0")
        return connection


class DatabaseTest(unittest.TestCase):
    def test_initialize_and_rows_close_every_connection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = TrackingDatabase(Path(directory) / "switchboard.sqlite3")

            rows = db.rows("SELECT 1 AS value")
            db.rows("SELECT 1 AS value")

            self.assertEqual(rows, [{"value": 1}])
            # One schema check for the instance, then one connection per query.
            self.assertEqual(len(db.connections), 3)
            self.assertTrue(all(connection.closed for connection in db.connections))

    def test_reads_do_not_need_the_write_lock(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "switchboard.sqlite3"
            db = Database(path)
            core.create_space(db, "demo")
            core.register_source(db, "mail", "demo", "mail")
            core.create_route(
                db,
                space_id="demo",
                name="triage",
                predicate={"event_type": "message.received"},
                processor="mail-triage",
            )
            event = core.emit_event(
                db,
                source_id="mail",
                external_id="message-1",
                event_type="message.received",
                attributes={},
            )
            writer = sqlite3.connect(path, isolation_level=None)
            self.addCleanup(writer.close)
            writer.execute("BEGIN IMMEDIATE")
            self.addCleanup(writer.execute, "ROLLBACK")

            # A fresh instance, as each CLI invocation creates, starts with an unchecked schema.
            reader = ImpatientDatabase(path)
            run = core.get_processor_run(reader, event["processor_runs"][0])
            counts = core.status(reader)["counts"]

            self.assertEqual(run["state"], "pending")
            self.assertEqual(counts["open_processor_runs"], 1)
            with self.assertRaisesRegex(sqlite3.OperationalError, "locked"):
                core.create_space(reader, "other")

    def test_initialize_repairs_an_outdated_or_incomplete_schema(self) -> None:
        for damage in (
            "UPDATE schema_meta SET value='4' WHERE key='schema_version'",
            "DROP INDEX idx_adapter_schedules_due",
        ):
            with self.subTest(damage=damage), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "switchboard.sqlite3"
                Database(path).initialize()
                with sqlite3.connect(path) as connection:
                    connection.execute(damage)

                Database(path).initialize()

                with sqlite3.connect(path) as connection:
                    version = connection.execute(
                        "SELECT value FROM schema_meta WHERE key='schema_version'"
                    ).fetchone()[0]
                    index = connection.execute(
                        "SELECT name FROM sqlite_master WHERE name='idx_adapter_schedules_due'"
                    ).fetchone()
                self.assertEqual(version, str(SCHEMA_VERSION))
                self.assertIsNotNone(index)


if __name__ == "__main__":
    unittest.main()
