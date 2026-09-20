from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from typing import Any

from switchboard.db import Database


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


class DatabaseTest(unittest.TestCase):
    def test_initialize_and_rows_close_every_connection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = TrackingDatabase(Path(directory) / "switchboard.sqlite3")

            rows = db.rows("SELECT 1 AS value")

            self.assertEqual(rows, [{"value": 1}])
            self.assertEqual(len(db.connections), 2)
            self.assertTrue(all(connection.closed for connection in db.connections))


if __name__ == "__main__":
    unittest.main()
