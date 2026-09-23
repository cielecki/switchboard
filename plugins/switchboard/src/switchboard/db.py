from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 8

SCHEMA = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS spaces (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    space_id TEXT NOT NULL REFERENCES spaces(id),
    kind TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'enabled',
    config_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
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

CREATE TABLE IF NOT EXISTS waits (
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

CREATE TABLE IF NOT EXISTS matches (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events(id),
    wait_id TEXT NOT NULL REFERENCES waits(id),
    matched_at TEXT NOT NULL,
    reason_json TEXT NOT NULL,
    UNIQUE(event_id, wait_id)
);

CREATE TABLE IF NOT EXISTS routes (
    id TEXT PRIMARY KEY,
    space_id TEXT NOT NULL REFERENCES spaces(id),
    name TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 100,
    predicate_json TEXT NOT NULL,
    target_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('enabled', 'disabled')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS route_matches (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE REFERENCES events(id),
    route_id TEXT NOT NULL REFERENCES routes(id),
    matched_at TEXT NOT NULL,
    reason_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS processor_runs (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events(id),
    route_id TEXT NOT NULL REFERENCES routes(id),
    processor TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('pending', 'running', 'completed', 'failed', 'needs-review')),
    summary TEXT NOT NULL DEFAULT '',
    facts_json TEXT NOT NULL DEFAULT '{}',
    decision_json TEXT NOT NULL DEFAULT '{}',
    actions_json TEXT NOT NULL DEFAULT '[]',
    error TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS processor_bindings (
    id TEXT PRIMARY KEY,
    space_id TEXT NOT NULL REFERENCES spaces(id),
    processor TEXT NOT NULL,
    consumer TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('enabled', 'disabled')),
    activate_inactive INTEGER NOT NULL DEFAULT 0 CHECK(activate_inactive IN (0, 1)),
    lease_seconds INTEGER NOT NULL DEFAULT 1800 CHECK(lease_seconds > 0),
    label TEXT,
    url TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(space_id, processor)
);

CREATE TABLE IF NOT EXISTS review_groups (
    id TEXT PRIMARY KEY,
    space_id TEXT NOT NULL REFERENCES spaces(id),
    review_key TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT NOT NULL DEFAULT '',
    url TEXT,
    state TEXT NOT NULL CHECK(state IN ('open', 'resolved')),
    resolution_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    resolved_at TEXT,
    UNIQUE(space_id, review_key)
);

CREATE TABLE IF NOT EXISTS processor_review_links (
    review_id TEXT NOT NULL REFERENCES review_groups(id),
    processor_run_id TEXT NOT NULL UNIQUE REFERENCES processor_runs(id),
    created_at TEXT NOT NULL,
    PRIMARY KEY(review_id, processor_run_id)
);

CREATE TABLE IF NOT EXISTS processor_deliveries (
    id TEXT PRIMARY KEY,
    processor_run_id TEXT NOT NULL UNIQUE REFERENCES processor_runs(id),
    binding_id TEXT NOT NULL REFERENCES processor_bindings(id),
    consumer TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    generation INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL CHECK(state IN ('pending', 'accepted', 'acknowledged', 'cancelled')),
    created_at TEXT NOT NULL,
    accepted_at TEXT,
    acknowledged_at TEXT,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS processor_delivery_attempts (
    id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL REFERENCES processor_deliveries(id),
    request_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('attempting', 'accepted', 'failed')),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    detail TEXT
);

CREATE TABLE IF NOT EXISTS processor_attempts (
    id TEXT PRIMARY KEY,
    processor_run_id TEXT NOT NULL REFERENCES processor_runs(id),
    worker TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('running', 'completed', 'failed', 'needs-review', 'released', 'expired')),
    lease_expires_at TEXT NOT NULL,
    started_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    finished_at TEXT,
    detail TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS processor_alerts (
    id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL REFERENCES processor_deliveries(id),
    generation INTEGER NOT NULL,
    consumer TEXT,
    state TEXT NOT NULL CHECK(state IN ('open', 'recovered')),
    opened_at TEXT NOT NULL,
    notification_claimed_at TEXT,
    notified_at TEXT,
    notification_error TEXT,
    last_seen_at TEXT,
    affected_delivery_count INTEGER NOT NULL DEFAULT 1,
    recovery_claimed_at TEXT,
    recovered_at TEXT,
    recovery_notified_at TEXT,
    recovery_error TEXT,
    detail TEXT NOT NULL,
    UNIQUE(delivery_id, generation)
);

CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events(id),
    wait_id TEXT NOT NULL REFERENCES waits(id),
    consumer TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('pending', 'accepted', 'acknowledged', 'failed', 'cancelled')),
    created_at TEXT NOT NULL,
    accepted_at TEXT,
    acknowledged_at TEXT,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL REFERENCES deliveries(id),
    request_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('attempting', 'accepted', 'failed')),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    detail TEXT
);

CREATE TABLE IF NOT EXISTS source_health (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT NOT NULL REFERENCES sources(id),
    state TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    observed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS adapter_runs (
    id TEXT PRIMARY KEY,
    adapter TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('running', 'completed', 'failed')),
    started_at TEXT NOT NULL,
    completed_at TEXT,
    discovered_sources INTEGER NOT NULL DEFAULT 0,
    emitted_events INTEGER NOT NULL DEFAULT 0,
    deduplicated_events INTEGER NOT NULL DEFAULT 0,
    detail TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS adapter_schedules (
    id TEXT PRIMARY KEY,
    adapter TEXT NOT NULL,
    config_json TEXT NOT NULL,
    every_seconds INTEGER NOT NULL CHECK(every_seconds > 0),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0, 1)),
    next_run_at TEXT NOT NULL,
    last_started_at TEXT,
    last_finished_at TEXT,
    last_state TEXT CHECK(last_state IS NULL OR last_state IN ('completed', 'failed')),
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS supervisor_state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    state TEXT NOT NULL CHECK(state IN ('starting', 'running', 'stopped', 'failed')),
    pid INTEGER,
    started_at TEXT,
    heartbeat_at TEXT,
    stopped_at TEXT,
    web_url TEXT,
    dispatch_enabled INTEGER NOT NULL DEFAULT 0 CHECK(dispatch_enabled IN (0, 1)),
    last_cycle_at TEXT,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS managed_resources (
    owner TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_key TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    checksum TEXT NOT NULL,
    applied_at TEXT NOT NULL,
    PRIMARY KEY(owner, resource_type, resource_key),
    UNIQUE(resource_type, entity_id)
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor TEXT NOT NULL,
    command TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_observed ON events(observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_waits_state ON waits(state, space_id);
CREATE INDEX IF NOT EXISTS idx_deliveries_state ON deliveries(state, created_at);
CREATE INDEX IF NOT EXISTS idx_routes_order ON routes(space_id, state, priority, id);
CREATE INDEX IF NOT EXISTS idx_processor_runs_state ON processor_runs(state, created_at);
CREATE INDEX IF NOT EXISTS idx_processor_runs_event ON processor_runs(event_id);
CREATE INDEX IF NOT EXISTS idx_processor_bindings_lookup ON processor_bindings(space_id, processor, state);
CREATE INDEX IF NOT EXISTS idx_review_groups_state ON review_groups(state, space_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_processor_review_links_review ON processor_review_links(review_id);
CREATE INDEX IF NOT EXISTS idx_processor_deliveries_state ON processor_deliveries(state, created_at);
CREATE INDEX IF NOT EXISTS idx_processor_delivery_attempts_delivery ON processor_delivery_attempts(delivery_id, started_at);
CREATE INDEX IF NOT EXISTS idx_processor_attempts_run ON processor_attempts(processor_run_id, started_at);
CREATE INDEX IF NOT EXISTS idx_processor_attempts_lease ON processor_attempts(state, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_processor_alerts_state ON processor_alerts(state, opened_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_processor_alerts_open_consumer
ON processor_alerts(consumer) WHERE state='open' AND consumer IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_delivery_attempts_delivery ON delivery_attempts(delivery_id, started_at);
CREATE INDEX IF NOT EXISTS idx_source_health_source ON source_health(source_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_adapter_runs_started ON adapter_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_adapter_schedules_due ON adapter_schedules(enabled, next_run_at);
CREATE INDEX IF NOT EXISTS idx_managed_resources_owner
ON managed_resources(owner, resource_type);
""" + f"""
INSERT OR REPLACE INTO schema_meta(key, value) VALUES('schema_version', '{SCHEMA_VERSION}');
"""

# Every table and index SCHEMA creates. A database holding all of them at SCHEMA_VERSION
# needs no schema pass, so reads never have to take the write lock.
SCHEMA_OBJECTS = frozenset(re.findall(r"CREATE (?:TABLE|INDEX) IF NOT EXISTS (\w+)", SCHEMA))


DELIVERIES_V2 = """
CREATE TABLE deliveries (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events(id),
    wait_id TEXT NOT NULL REFERENCES waits(id),
    consumer TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('pending', 'accepted', 'acknowledged', 'failed', 'cancelled')),
    created_at TEXT NOT NULL,
    accepted_at TEXT,
    acknowledged_at TEXT,
    last_error TEXT
);
"""


def default_db_path() -> Path:
    configured = os.environ.get("SWITCHBOARD_DB")
    if configured:
        return Path(configured).expanduser().resolve()
    root = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local/share"))
    return (root / "switchboard" / "switchboard.sqlite3").resolve()


def schema_is_current(connection: sqlite3.Connection) -> bool:
    present = {
        row["name"]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'index')"
        )
    }
    if not SCHEMA_OBJECTS <= present:
        return False
    version = connection.execute(
        "SELECT value FROM schema_meta WHERE key='schema_version'"
    ).fetchone()
    return version is not None and version["value"] == str(SCHEMA_VERSION)


def migrate(connection: sqlite3.Connection) -> None:
    existing = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='deliveries'"
    ).fetchone()
    if existing is not None and "'accepted'" not in existing["sql"]:
        connection.executescript(
            "ALTER TABLE deliveries RENAME TO deliveries_v1;\n"
            + DELIVERIES_V2
            + """
            INSERT INTO deliveries(
                id, event_id, wait_id, consumer, idempotency_key, state,
                created_at, acknowledged_at
            )
            SELECT id, event_id, wait_id, consumer, idempotency_key, state,
                   created_at, acknowledged_at
            FROM deliveries_v1;
            DROP TABLE deliveries_v1;
            """
        )
    alert_columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(processor_alerts)").fetchall()
    }
    for name, declaration in (
        ("consumer", "TEXT"),
        ("notification_claimed_at", "TEXT"),
        ("notification_error", "TEXT"),
        ("last_seen_at", "TEXT"),
        ("affected_delivery_count", "INTEGER NOT NULL DEFAULT 1"),
        ("recovery_claimed_at", "TEXT"),
        ("recovery_error", "TEXT"),
    ):
        if alert_columns and name not in alert_columns:
            connection.execute(f"ALTER TABLE processor_alerts ADD COLUMN {name} {declaration}")
    if alert_columns:
        duplicates = connection.execute(
            "SELECT pd.consumer, group_concat(pa.id) AS ids FROM processor_alerts pa "
            "JOIN processor_deliveries pd ON pd.id=pa.delivery_id "
            "WHERE pa.state='open' GROUP BY pd.consumer HAVING count(*) > 1"
        ).fetchall()
        for duplicate in duplicates:
            ids = duplicate["ids"].split(",")
            for alert_id in ids[1:]:
                connection.execute(
                    "UPDATE processor_alerts SET state='recovered', "
                    "recovered_at=COALESCE(recovered_at, opened_at), "
                    "detail=detail || '; superseded during consumer alert migration' "
                    "WHERE id=?",
                    (alert_id,),
                )
        connection.execute(
            "UPDATE processor_alerts SET consumer=("
            "SELECT pd.consumer FROM processor_deliveries pd "
            "WHERE pd.id=processor_alerts.delivery_id) WHERE consumer IS NULL"
        )
        connection.execute(
            "UPDATE processor_alerts SET "
            "notification_claimed_at=COALESCE(notification_claimed_at, notified_at, opened_at), "
            "last_seen_at=COALESCE(last_seen_at, opened_at), "
            "recovery_claimed_at=CASE WHEN state='recovered' THEN "
            "COALESCE(recovery_claimed_at, recovery_notified_at, recovered_at) "
            "ELSE recovery_claimed_at END"
        )
    binding_columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(processor_bindings)").fetchall()
    }
    for name in ("label", "url"):
        if binding_columns and name not in binding_columns:
            connection.execute(f"ALTER TABLE processor_bindings ADD COLUMN {name} TEXT")
    connection.executescript(SCHEMA)
    _backfill_review_groups(connection)
    connection.commit()


def _review_key(value: Any, run_id: str) -> str:
    if isinstance(value, str) and value.strip():
        cleaned = value.strip()
        if re.match(r"^(?:local_|task_|[0-9a-f]{8}-)", cleaned):
            return cleaned.split()[0]
        return cleaned
    return f"run:{run_id}"


def _backfill_review_groups(connection: sqlite3.Connection) -> None:
    rows = connection.execute(
        "SELECT pr.*, e.space_id FROM processor_runs pr "
        "JOIN events e ON e.id=pr.event_id WHERE pr.state='needs-review' "
        "ORDER BY pr.created_at, pr.id"
    ).fetchall()
    for row in rows:
        decision = json.loads(row["decision_json"])
        facts = json.loads(row["facts_json"])
        key = _review_key(
            facts.get("shared_decision_task")
            or decision.get("shared_decision_task")
            or decision.get("decision_task")
            or facts.get("decision_task"),
            row["id"],
        )
        digest = hashlib.sha256(f"{row['space_id']}\0{key}".encode()).hexdigest()[:16]
        review_id = f"review_{digest}"
        title = str(decision.get("review_title") or row["summary"] or key).split(".", 1)[0]
        title = title[:180]
        url = decision.get("review_url") or facts.get("review_url")
        connection.execute(
            "INSERT INTO review_groups(id, space_id, review_key, title, summary, url, state, "
            "created_at, updated_at) VALUES(?,?,?,?,?,?,'open',?,?) "
            "ON CONFLICT(space_id, review_key) DO UPDATE SET "
            "updated_at=MAX(review_groups.updated_at, excluded.updated_at), "
            "url=COALESCE(review_groups.url, excluded.url)",
            (
                review_id,
                row["space_id"],
                key,
                title,
                row["summary"],
                url,
                row["created_at"],
                row["updated_at"],
            ),
        )
        actual = connection.execute(
            "SELECT id FROM review_groups WHERE space_id=? AND review_key=?",
            (row["space_id"], key),
        ).fetchone()["id"]
        connection.execute(
            "INSERT INTO processor_review_links(review_id, processor_run_id, created_at) "
            "VALUES(?,?,?) ON CONFLICT(processor_run_id) DO NOTHING",
            (actual, row["id"], row["updated_at"]),
        )


class Database:
    def __init__(self, path: str | Path | None = None):
        self.path = Path(path).expanduser().resolve() if path else default_db_path()
        self._schema_ready = False

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    @contextmanager
    def session(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        # The schema script writes schema_meta, so running it needs the write lock. Check
        # read-only first and run it only for a new or outdated database, at most once per
        # instance; otherwise read commands stall behind the supervisor's transactions.
        if self._schema_ready:
            return
        with self.session() as connection:
            if not schema_is_current(connection):
                migrate(connection)
        self._schema_ready = True

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def rows(self, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        self.initialize()
        with self.session() as connection:
            return [dict(row) for row in connection.execute(query, params).fetchall()]

    def row(self, query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        rows = self.rows(query, params)
        return rows[0] if rows else None


def decode_json_fields(record: dict[str, Any], *fields: str) -> dict[str, Any]:
    result = dict(record)
    for field in fields:
        raw = result.pop(field, None)
        if raw is not None:
            result[field.removesuffix("_json")] = json.loads(raw)
    return result
