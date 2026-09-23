from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from switchboard.db import SCHEMA_VERSION, Database


class MigrationTest(unittest.TestCase):
    def test_v7_review_backfill_groups_shared_decision_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "switchboard.sqlite3"
            db = Database(path)
            db.initialize()
            with sqlite3.connect(path) as connection:
                timestamp = "2026-09-23T10:00:00+00:00"
                connection.execute("INSERT INTO spaces VALUES(?,?,?)", ("demo", "Demo", timestamp))
                connection.execute(
                    "INSERT INTO sources(id,space_id,kind,created_at) VALUES(?,?,?,?)",
                    ("mail", "demo", "mail", timestamp),
                )
                connection.execute(
                    "INSERT INTO events(id,space_id,source_id,external_id,event_type,observed_at,attributes_json) "
                    "VALUES(?,?,?,?,?,?,?)",
                    ("event-1", "demo", "mail", "external-1", "mail", timestamp, "{}"),
                )
                connection.execute(
                    "INSERT INTO routes(id,space_id,name,predicate_json,target_json,state,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,'enabled',?,?)",
                    ("route-1", "demo", "Route", "{}", '{"processor":"triage"}', timestamp, timestamp),
                )
                for index in (1, 2):
                    connection.execute(
                        "INSERT INTO processor_runs(id,event_id,route_id,processor,idempotency_key,state,summary,facts_json,decision_json,actions_json,created_at,updated_at) "
                        "VALUES(?,?,?,?,?,'needs-review',?,?,?,?,?,?)",
                        (
                            f"run-{index}", "event-1", "route-1", "triage", f"key-{index}",
                            "One shared choice", '{"shared_decision_task":"task_shared choice"}',
                            "{}", "[]", timestamp, timestamp,
                        ),
                    )
                connection.execute("DROP TABLE processor_review_links")
                connection.execute("DROP TABLE review_groups")
                connection.execute("UPDATE schema_meta SET value='7' WHERE key='schema_version'")

            Database(path).initialize()
            groups = Database(path).rows("SELECT * FROM review_groups")
            links = Database(path).rows("SELECT * FROM processor_review_links")
            self.assertEqual(len(groups), 1)
            self.assertEqual(groups[0]["review_key"], "task_shared")
            self.assertEqual(len(links), 2)

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
                schedule_table = migrated.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='adapter_schedules'"
                ).fetchone()
            self.assertIn("'accepted'", sql)
            self.assertEqual(version, str(SCHEMA_VERSION))
            self.assertIsNotNone(schedule_table)
            with db.connect() as migrated:
                route_table = migrated.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='routes'"
                ).fetchone()
            self.assertIsNotNone(route_table)

    def test_v5_alert_table_gains_consumer_episode_columns(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "switchboard.sqlite3"
            db = Database(path)
            db.initialize()
            with sqlite3.connect(path) as connection:
                connection.execute("DROP INDEX idx_processor_alerts_open_consumer")
                connection.execute("ALTER TABLE processor_alerts RENAME TO processor_alerts_new")
                connection.execute(
                    "CREATE TABLE processor_alerts ("
                    "id TEXT PRIMARY KEY, delivery_id TEXT NOT NULL, generation INTEGER NOT NULL, "
                    "state TEXT NOT NULL, opened_at TEXT NOT NULL, notified_at TEXT, "
                    "recovered_at TEXT, recovery_notified_at TEXT, detail TEXT NOT NULL, "
                    "UNIQUE(delivery_id, generation))"
                )
                connection.execute("DROP TABLE processor_alerts_new")
                connection.execute("UPDATE schema_meta SET value='5' WHERE key='schema_version'")

            Database(path).initialize()

            with sqlite3.connect(path) as connection:
                columns = {
                    row[1] for row in connection.execute("PRAGMA table_info(processor_alerts)")
                }
                version = connection.execute(
                    "SELECT value FROM schema_meta WHERE key='schema_version'"
                ).fetchone()[0]
            self.assertIn("consumer", columns)
            self.assertIn("recovery_claimed_at", columns)
            self.assertEqual(version, str(SCHEMA_VERSION))


if __name__ == "__main__":
    unittest.main()
