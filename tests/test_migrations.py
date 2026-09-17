from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from switchboard.db import Database


class MigrationTest(unittest.TestCase):
    def test_v1_delivery_table_migrates_to_accepted_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "switchboard.sqlite3"
            connection = sqlite3.connect(path)
            connection.executescript(
                """
                CREATE TABLE spaces (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE sources (
                    id TEXT PRIMARY KEY,
                    space_id TEXT NOT NULL REFERENCES spaces(id),
                    kind TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'enabled',
                    config_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE events (
                    id TEXT PRIMARY KEY,
                    space_id TEXT NOT NULL REFERENCES spaces(id),
                    source_id TEXT NOT NULL REFERENCES sources(id),
                    external_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    occurred_at TEXT,
                    observed_at TEXT NOT NULL,
                    attributes_json TEXT NOT NULL,
                    UNIQUE(source_id, external_id)
                );

                CREATE TABLE waits (
                    id TEXT PRIMARY KEY,
                    space_id TEXT NOT NULL REFERENCES spaces(id),
                    consumer TEXT NOT NULL,
                    predicate_json TEXT NOT NULL,
                    purpose TEXT NOT NULL DEFAULT '',
                    mode TEXT NOT NULL CHECK(mode IN ('one-shot', 'repeating')),
                    state TEXT NOT NULL CHECK(state IN ('active', 'matched', 'cancelled', 'expired')),
                    created_at TEXT NOT NULL,
                    expires_at TEXT,
                    matched_event_id TEXT REFERENCES events(id)
                );

                CREATE TABLE deliveries (
                    id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    wait_id TEXT NOT NULL,
                    consumer TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL CHECK(state IN ('pending', 'acknowledged', 'failed', 'cancelled')),
                    created_at TEXT NOT NULL,
                    acknowledged_at TEXT
                );
                """
            )
            connection.close()

            db = Database(path)
            db.initialize()

            with db.connect() as migrated:
                sql = migrated.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='deliveries'"
                ).fetchone()[0]
                version = migrated.execute(
                    "SELECT value FROM schema_meta WHERE key='schema_version'"
                ).fetchone()[0]
            self.assertIn("'accepted'", sql)
            self.assertEqual(version, "2")


if __name__ == "__main__":
    unittest.main()
