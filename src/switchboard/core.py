from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any

from .db import Database, decode_json_fields


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def audit(
    connection: sqlite3.Connection,
    *,
    command: str,
    entity_type: str,
    entity_id: str,
    payload: dict[str, Any],
    actor: str = "cli",
) -> None:
    connection.execute(
        "INSERT INTO audit_log(actor, command, entity_type, entity_id, payload_json, created_at) "
        "VALUES(?,?,?,?,?,?)",
        (actor, command, entity_type, entity_id, json.dumps(payload, sort_keys=True), now()),
    )


def create_space(db: Database, space_id: str, name: str | None = None) -> dict[str, Any]:
    db.initialize()
    created_at = now()
    with db.transaction() as connection:
        connection.execute(
            "INSERT INTO spaces(id, name, created_at) VALUES(?,?,?)",
            (space_id, name or space_id, created_at),
        )
        audit(
            connection,
            command="space.create",
            entity_type="space",
            entity_id=space_id,
            payload={"name": name or space_id},
        )
    return {"id": space_id, "name": name or space_id, "created_at": created_at}


def list_spaces(db: Database) -> list[dict[str, Any]]:
    return db.rows("SELECT * FROM spaces ORDER BY id")


def register_source(
    db: Database, source_id: str, space_id: str, kind: str, config: dict[str, Any] | None = None
) -> dict[str, Any]:
    db.initialize()
    created_at = now()
    config = config or {}
    with db.transaction() as connection:
        connection.execute(
            "INSERT INTO sources(id, space_id, kind, config_json, created_at) VALUES(?,?,?,?,?)",
            (source_id, space_id, kind, json.dumps(config, sort_keys=True), created_at),
        )
        audit(
            connection,
            command="source.register",
            entity_type="source",
            entity_id=source_id,
            payload={"space_id": space_id, "kind": kind, "config": config},
        )
    return {
        "id": source_id,
        "space_id": space_id,
        "kind": kind,
        "state": "enabled",
        "config": config,
        "created_at": created_at,
    }


def list_sources(db: Database) -> list[dict[str, Any]]:
    return [decode_json_fields(row, "config_json") for row in db.rows("SELECT * FROM sources ORDER BY id")]


def create_wait(
    db: Database,
    *,
    space_id: str,
    consumer: str,
    predicate: dict[str, Any],
    purpose: str = "",
    repeating: bool = False,
    expires_at: str | None = None,
) -> dict[str, Any]:
    db.initialize()
    wait_id = make_id("wait")
    created_at = now()
    mode = "repeating" if repeating else "one-shot"
    with db.transaction() as connection:
        connection.execute(
            "INSERT INTO waits(id, space_id, consumer, predicate_json, purpose, mode, state, "
            "created_at, expires_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                wait_id,
                space_id,
                consumer,
                json.dumps(predicate, sort_keys=True),
                purpose,
                mode,
                "active",
                created_at,
                expires_at,
            ),
        )
        audit(
            connection,
            command="wait.create",
            entity_type="wait",
            entity_id=wait_id,
            payload={
                "space_id": space_id,
                "consumer": consumer,
                "predicate": predicate,
                "purpose": purpose,
                "mode": mode,
                "expires_at": expires_at,
            },
        )
    return {
        "id": wait_id,
        "space_id": space_id,
        "consumer": consumer,
        "predicate": predicate,
        "purpose": purpose,
        "mode": mode,
        "state": "active",
        "created_at": created_at,
        "expires_at": expires_at,
    }


def list_waits(db: Database, state: str | None = None) -> list[dict[str, Any]]:
    query = "SELECT * FROM waits"
    params: tuple[Any, ...] = ()
    if state:
        query += " WHERE state=?"
        params = (state,)
    query += " ORDER BY created_at DESC"
    return [decode_json_fields(row, "predicate_json") for row in db.rows(query, params)]


def cancel_wait(db: Database, wait_id: str) -> dict[str, Any]:
    db.initialize()
    with db.transaction() as connection:
        changed = connection.execute(
            "UPDATE waits SET state='cancelled' WHERE id=? AND state='active'", (wait_id,)
        ).rowcount
        if not changed:
            raise ValueError(f"active wait not found: {wait_id}")
        connection.execute(
            "UPDATE deliveries SET state='cancelled' WHERE wait_id=? AND state='pending'", (wait_id,)
        )
        audit(
            connection,
            command="wait.cancel",
            entity_type="wait",
            entity_id=wait_id,
            payload={},
        )
    return {"id": wait_id, "state": "cancelled"}


def predicate_matches(
    predicate: dict[str, Any], *, source_id: str, event_type: str, attributes: dict[str, Any]
) -> tuple[bool, dict[str, Any]]:
    checks: list[dict[str, Any]] = []

    expected_source = predicate.get("source_id")
    if expected_source is not None:
        checks.append({"field": "source_id", "expected": expected_source, "actual": source_id})

    expected_type = predicate.get("event_type")
    if expected_type is not None:
        checks.append({"field": "event_type", "expected": expected_type, "actual": event_type})

    for field, expected in (predicate.get("attributes") or {}).items():
        checks.append({"field": field, "expected": expected, "actual": attributes.get(field)})

    for field, needle in (predicate.get("contains") or {}).items():
        actual = attributes.get(field)
        matched = isinstance(actual, str) and str(needle).casefold() in actual.casefold()
        checks.append({"field": field, "contains": needle, "actual": actual, "matched": matched})

    matched = all(
        check.get("matched", check.get("actual") == check.get("expected")) for check in checks
    )
    return matched, {"operator": "and", "checks": checks}


def emit_event(
    db: Database,
    *,
    source_id: str,
    external_id: str,
    event_type: str,
    attributes: dict[str, Any],
    occurred_at: str | None = None,
) -> dict[str, Any]:
    db.initialize()
    observed_at = now()
    with db.transaction() as connection:
        source = connection.execute("SELECT * FROM sources WHERE id=?", (source_id,)).fetchone()
        if source is None:
            raise ValueError(f"source not found: {source_id}")

        existing = connection.execute(
            "SELECT * FROM events WHERE source_id=? AND external_id=?", (source_id, external_id)
        ).fetchone()
        if existing is not None:
            return {
                "event": decode_json_fields(dict(existing), "attributes_json"),
                "deduplicated": True,
                "matched_waits": [],
                "deliveries": [],
            }

        event_id = make_id("evt")
        connection.execute(
            "INSERT INTO events(id, space_id, source_id, external_id, event_type, occurred_at, "
            "observed_at, attributes_json) VALUES(?,?,?,?,?,?,?,?)",
            (
                event_id,
                source["space_id"],
                source_id,
                external_id,
                event_type,
                occurred_at,
                observed_at,
                json.dumps(attributes, sort_keys=True),
            ),
        )

        waits = connection.execute(
            "SELECT * FROM waits WHERE space_id=? AND state='active' ORDER BY created_at",
            (source["space_id"],),
        ).fetchall()
        matched_waits: list[str] = []
        deliveries: list[str] = []
        for wait in waits:
            if wait["expires_at"] and wait["expires_at"] <= observed_at:
                connection.execute("UPDATE waits SET state='expired' WHERE id=?", (wait["id"],))
                continue
            predicate = json.loads(wait["predicate_json"])
            matched, reason = predicate_matches(
                predicate, source_id=source_id, event_type=event_type, attributes=attributes
            )
            if not matched:
                continue
            match_id = make_id("match")
            delivery_id = make_id("dlv")
            connection.execute(
                "INSERT INTO matches(id, event_id, wait_id, matched_at, reason_json) VALUES(?,?,?,?,?)",
                (match_id, event_id, wait["id"], observed_at, json.dumps(reason, sort_keys=True)),
            )
            connection.execute(
                "INSERT INTO deliveries(id, event_id, wait_id, consumer, idempotency_key, state, "
                "created_at) VALUES(?,?,?,?,?,'pending',?)",
                (
                    delivery_id,
                    event_id,
                    wait["id"],
                    wait["consumer"],
                    f"{event_id}:{wait['id']}",
                    observed_at,
                ),
            )
            if wait["mode"] == "one-shot":
                connection.execute(
                    "UPDATE waits SET state='matched', matched_event_id=? WHERE id=?",
                    (event_id, wait["id"]),
                )
            matched_waits.append(wait["id"])
            deliveries.append(delivery_id)

        audit(
            connection,
            command="event.emit",
            entity_type="event",
            entity_id=event_id,
            payload={
                "source_id": source_id,
                "external_id": external_id,
                "event_type": event_type,
                "matched_waits": matched_waits,
            },
            actor=f"source:{source_id}",
        )

    return {
        "event": {
            "id": event_id,
            "space_id": source["space_id"],
            "source_id": source_id,
            "external_id": external_id,
            "event_type": event_type,
            "occurred_at": occurred_at,
            "observed_at": observed_at,
            "attributes": attributes,
        },
        "deduplicated": False,
        "matched_waits": matched_waits,
        "deliveries": deliveries,
    }


def list_events(db: Database, limit: int = 50) -> list[dict[str, Any]]:
    return [
        decode_json_fields(row, "attributes_json")
        for row in db.rows("SELECT * FROM events ORDER BY observed_at DESC LIMIT ?", (limit,))
    ]


def get_event(db: Database, event_id: str) -> dict[str, Any]:
    row = db.row("SELECT * FROM events WHERE id=?", (event_id,))
    if row is None:
        raise ValueError(f"event not found: {event_id}")
    event = decode_json_fields(row, "attributes_json")
    event["matches"] = [
        decode_json_fields(match, "reason_json")
        for match in db.rows("SELECT * FROM matches WHERE event_id=? ORDER BY matched_at", (event_id,))
    ]
    event["deliveries"] = db.rows(
        "SELECT * FROM deliveries WHERE event_id=? ORDER BY created_at", (event_id,)
    )
    return event


def list_deliveries(db: Database, state: str | None = None) -> list[dict[str, Any]]:
    query = "SELECT * FROM deliveries"
    params: tuple[Any, ...] = ()
    if state:
        query += " WHERE state=?"
        params = (state,)
    query += " ORDER BY created_at DESC"
    return db.rows(query, params)


def acknowledge_delivery(db: Database, delivery_id: str) -> dict[str, Any]:
    db.initialize()
    acknowledged_at = now()
    with db.transaction() as connection:
        changed = connection.execute(
            "UPDATE deliveries SET state='acknowledged', acknowledged_at=? "
            "WHERE id=? AND state='pending'",
            (acknowledged_at, delivery_id),
        ).rowcount
        if not changed:
            raise ValueError(f"pending delivery not found: {delivery_id}")
        audit(
            connection,
            command="delivery.acknowledge",
            entity_type="delivery",
            entity_id=delivery_id,
            payload={"acknowledged_at": acknowledged_at},
        )
    return {"id": delivery_id, "state": "acknowledged", "acknowledged_at": acknowledged_at}


def status(db: Database) -> dict[str, Any]:
    db.initialize()
    counts: dict[str, int] = {}
    with db.connect() as connection:
        for table in ("spaces", "sources", "events", "waits", "deliveries"):
            counts[table] = connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        counts["active_waits"] = connection.execute(
            "SELECT count(*) FROM waits WHERE state='active'"
        ).fetchone()[0]
        counts["pending_deliveries"] = connection.execute(
            "SELECT count(*) FROM deliveries WHERE state='pending'"
        ).fetchone()[0]
    return {"database": str(db.path), "schema_version": 1, "counts": counts}
