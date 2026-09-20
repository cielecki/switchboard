from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 5

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
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(space_id, processor)
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
    state TEXT NOT NULL CHECK(state IN ('open', 'recovered')),
    opened_at TEXT NOT NULL,
    notified_at TEXT,
    recovered_at TEXT,
    recovery_notified_at TEXT,
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
CREATE INDEX IF NOT EXISTS idx_processor_deliveries_state ON processor_deliveries(state, created_at);
CREATE INDEX IF NOT EXISTS idx_processor_delivery_attempts_delivery ON processor_delivery_attempts(delivery_id, started_at);
CREATE INDEX IF NOT EXISTS idx_processor_attempts_run ON processor_attempts(processor_run_id, started_at);
CREATE INDEX IF NOT EXISTS idx_processor_attempts_lease ON processor_attempts(state, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_processor_alerts_state ON processor_alerts(state, opened_at);
CREATE INDEX IF NOT EXISTS idx_delivery_attempts_delivery ON delivery_attempts(delivery_id, started_at);
CREATE INDEX IF NOT EXISTS idx_source_health_source ON source_health(source_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_adapter_runs_started ON adapter_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_adapter_schedules_due ON adapter_schedules(enabled, next_run_at);

INSERT OR REPLACE INTO schema_meta(key, value) VALUES('schema_version', '5');
"""


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


class Database:
    def __init__(self, path: str | Path | None = None):
        self.path = Path(path).expanduser().resolve() if path else default_db_path()

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
        with self.session() as connection:
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
            connection.executescript(SCHEMA)
            connection.commit()

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
