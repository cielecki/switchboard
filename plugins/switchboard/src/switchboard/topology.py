from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

from . import core
from .db import Database

DOCUMENT_VERSION = 1
_VARIABLE = re.compile(r"\$\{([A-Z][A-Z0-9_]*)\}")


def _checksum(value: dict[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _resolve(value: Any, variables: dict[str, str], missing: set[str]) -> Any:
    if isinstance(value, dict):
        return {key: _resolve(item, variables, missing) for key, item in value.items()}
    if isinstance(value, list):
        return [_resolve(item, variables, missing) for item in value]
    if not isinstance(value, str):
        return value

    def replacement(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in variables:
            missing.add(name)
            return match.group(0)
        return variables[name]

    return _VARIABLE.sub(replacement, value)


def load_topology(
    path: str | Path, variables: dict[str, str] | None = None
) -> dict[str, Any]:
    source = Path(path).expanduser().resolve()
    try:
        raw = json.loads(source.read_text())
    except FileNotFoundError as exc:
        raise ValueError(f"topology file not found: {source}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid topology JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError("topology document must be a JSON object")  # noqa: TRY004
    supplied = {
        key: value for key, value in os.environ.items() if isinstance(value, str)
    }
    supplied.update(variables or {})
    missing: set[str] = set()
    document = _resolve(raw, supplied, missing)
    if missing:
        raise ValueError("missing topology variables: " + ", ".join(sorted(missing)))
    validate_topology(document)
    return document


def _object_list(document: dict[str, Any], name: str) -> list[dict[str, Any]]:
    value = document.get(name, [])
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError(f"topology {name} must be an array of objects")
    return value


def validate_topology(document: dict[str, Any]) -> None:
    if document.get("schema_version") != DOCUMENT_VERSION:
        raise ValueError(f"topology schema_version must be {DOCUMENT_VERSION}")
    owner = document.get("owner")
    if not isinstance(owner, str) or not owner.strip():
        raise ValueError("topology owner cannot be empty")
    spaces = _object_list(document, "spaces")
    sources = _object_list(document, "sources")
    routes = _object_list(document, "routes")
    schedules = _object_list(document, "schedules")
    bindings = _object_list(document, "bindings")
    space_ids = {item.get("id") for item in spaces}
    if None in space_ids or len(space_ids) != len(spaces):
        raise ValueError("topology space ids must be present and unique")
    source_ids = {item.get("id") for item in sources}
    if None in source_ids or len(source_ids) != len(sources):
        raise ValueError("topology source ids must be present and unique")
    for source in sources:
        if source.get("space") not in space_ids:
            raise ValueError(
                f"source {source.get('id')} references an undeclared space"
            )
        if not isinstance(source.get("kind"), str) or not source["kind"]:
            raise ValueError(f"source {source.get('id')} requires kind")
        if not isinstance(source.get("config", {}), dict):
            raise ValueError(  # noqa: TRY004
                f"source {source.get('id')} config must be an object"
            )
    route_keys = {item.get("key") for item in routes}
    if None in route_keys or len(route_keys) != len(routes):
        raise ValueError("topology route keys must be present and unique")
    for route in routes:
        if route.get("space") not in space_ids:
            raise ValueError(f"route {route.get('key')} references an undeclared space")
        if not isinstance(route.get("predicate"), dict) or not route["predicate"]:
            raise ValueError(f"route {route.get('key')} requires a predicate")
        if not isinstance(route.get("processor"), str) or not route["processor"]:
            raise ValueError(f"route {route.get('key')} requires a processor")
        routed_source = route["predicate"].get("source_id")
        if routed_source is not None and routed_source not in source_ids:
            raise ValueError(
                f"route {route.get('key')} references an undeclared source"
            )
    schedule_ids = {item.get("id") for item in schedules}
    if None in schedule_ids or len(schedule_ids) != len(schedules):
        raise ValueError("topology schedule ids must be present and unique")
    for schedule in schedules:
        adapter = schedule.get("adapter")
        config = schedule.get("config")
        if adapter not in {"ingest-shadow", "inbound-leads", "timer"}:
            raise ValueError(
                f"schedule {schedule.get('id')} has unsupported adapter {adapter}"
            )
        if not isinstance(config, dict):
            raise ValueError(  # noqa: TRY004
                f"schedule {schedule.get('id')} config must be an object"
            )
        if (
            not isinstance(schedule.get("every_seconds"), int)
            or schedule["every_seconds"] < 1
        ):
            raise ValueError(
                f"schedule {schedule.get('id')} requires a positive interval"
            )
        if config.get("space_id") not in space_ids:
            raise ValueError(
                f"schedule {schedule.get('id')} references an undeclared space"
            )
        if adapter == "timer" and config.get("source_id") not in source_ids:
            raise ValueError(
                f"schedule {schedule.get('id')} references an undeclared source"
            )
        file_keys = {
            "ingest-shadow": ("status_script", "discovery_script"),
            "inbound-leads": (
                "ledger_script",
                "discovery_script",
                "slack_discovery_script",
            ),
            "timer": (),
        }[adapter]
        for key in file_keys:
            value = config.get(key)
            if value is not None and not Path(value).expanduser().is_file():
                raise ValueError(
                    f"schedule {schedule.get('id')} path not found: {value}"
                )
    binding_keys = {item.get("key") for item in bindings}
    if None in binding_keys or len(binding_keys) != len(bindings):
        raise ValueError("topology binding keys must be present and unique")
    binding_targets: set[tuple[str, str]] = set()
    for binding in bindings:
        if binding.get("space") not in space_ids:
            raise ValueError(
                f"binding {binding.get('key')} references an undeclared space"
            )
        target = (binding["space"], binding.get("processor"))
        if not target[1] or target in binding_targets:
            raise ValueError("topology bindings must have unique space/processor pairs")
        binding_targets.add(target)
        if not re.fullmatch(
            r"chat:(claude|codex|opencode):[^:]+", str(binding.get("consumer", ""))
        ):
            raise ValueError(
                f"binding {binding.get('key')} has an invalid chat consumer"
            )
        for field in ("label", "url"):
            value = binding.get(field)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(
                    f"binding {binding.get('key')} {field} must be a non-empty string or null"
                )


def _redactor() -> tuple[Any, dict[str, dict[str, Any]]]:
    path_names: dict[str, str] = {}
    consumer_names: dict[str, str] = {}
    url_names: dict[str, str] = {}
    definitions: dict[str, dict[str, Any]] = {}

    def redact(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: redact(item) for key, item in value.items()}
        if isinstance(value, list):
            return [redact(item) for item in value]
        if not isinstance(value, str):
            return value
        group: dict[str, str] | None = None
        prefix = ""
        description = ""
        if re.fullmatch(r"chat:(claude|codex|opencode):[^:]+", value):
            group, prefix, description = (
                consumer_names,
                "CONSUMER",
                "Durable chat consumer",
            )
        elif re.match(r"^(?:https?|claude|codex):", value):
            group, prefix, description = url_names, "URL", "Local task link"
        elif os.path.isabs(value):
            group, prefix, description = path_names, "PATH", "Absolute local path"
        if group is None:
            return value
        if value not in group:
            name = f"{prefix}_{len(group) + 1}"
            group[value] = name
            definitions[name] = {"required": True, "description": description}
        return "${" + group[value] + "}"

    return redact, definitions


def export_topology(
    db: Database, *, owner: str = "exported", include_local_values: bool = False
) -> dict[str, Any]:
    if not owner.strip():
        raise ValueError("topology owner cannot be empty")
    redact, variables = _redactor()
    if include_local_values:

        def keep_local_value(value: Any) -> Any:
            return value

        redact = keep_local_value
    spaces = [{"id": item["id"], "name": item["name"]} for item in core.list_spaces(db)]
    sources = [
        {
            "id": item["id"],
            "space": item["space_id"],
            "kind": item["kind"],
            "state": item["state"],
            "config": redact(item["config"]),
        }
        for item in core.list_sources(db)
    ]
    routes = [
        {
            "key": item["id"],
            "space": item["space_id"],
            "name": item["name"],
            "priority": item["priority"],
            "predicate": item["predicate"],
            "processor": item["target"]["processor"],
            "enabled": item["state"] == "enabled",
        }
        for item in core.list_routes(db)
    ]
    schedules = [
        {
            "id": item["id"],
            "adapter": item["adapter"],
            "every_seconds": item["every_seconds"],
            "enabled": item["enabled"],
            "config": redact(item["config"]),
        }
        for item in core.list_schedules(db)
    ]
    bindings = [
        {
            "key": f"{item['space_id']}:{item['processor']}",
            "space": item["space_id"],
            "processor": item["processor"],
            "consumer": redact(item["consumer"]),
            "enabled": item["state"] == "enabled",
            "activate_inactive": item["activate_inactive"],
            "lease_seconds": item["lease_seconds"],
            "label": item["label"],
            "url": redact(item["url"]) if item["url"] else None,
        }
        for item in core.list_processor_bindings(db)
    ]
    return {
        "schema_version": DOCUMENT_VERSION,
        "owner": owner,
        "variables": variables,
        "spaces": spaces,
        "sources": sources,
        "routes": routes,
        "schedules": schedules,
        "bindings": bindings,
    }


def _managed(db: Database, owner: str) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (row["resource_type"], row["resource_key"]): row
        for row in db.rows("SELECT * FROM managed_resources WHERE owner=?", (owner,))
    }


def _normalize(document: dict[str, Any]) -> list[tuple[str, str, dict[str, Any]]]:
    resources: list[tuple[str, str, dict[str, Any]]] = []
    for item in document.get("spaces", []):
        resources.append(
            (
                "space",
                item["id"],
                {"id": item["id"], "name": item.get("name") or item["id"]},
            )
        )
    for item in document.get("sources", []):
        resources.append(
            (
                "source",
                item["id"],
                {
                    "id": item["id"],
                    "space": item["space"],
                    "kind": item["kind"],
                    "state": item.get("state", "enabled"),
                    "config": item.get("config", {}),
                },
            )
        )
    for item in document.get("routes", []):
        resources.append(
            (
                "route",
                item["key"],
                {
                    "key": item["key"],
                    "space": item["space"],
                    "name": item["name"],
                    "priority": item.get("priority", 100),
                    "predicate": item["predicate"],
                    "processor": item["processor"],
                    "enabled": item.get("enabled", True),
                },
            )
        )
    for item in document.get("schedules", []):
        config = dict(item["config"])
        if item["adapter"] == "timer":
            config.setdefault("attributes", {})
        elif item["adapter"] == "ingest-shadow":
            config.setdefault("discovery_script", None)
            config.setdefault("timeout", 120)
        elif item["adapter"] == "inbound-leads":
            config.setdefault("discovery_script", None)
            config.setdefault("slack_discovery_script", None)
            config.setdefault("source_mode", "both")
            config.setdefault("timeout", 240)
        resources.append(
            (
                "schedule",
                item["id"],
                {
                    "id": item["id"],
                    "adapter": item["adapter"],
                    "every_seconds": item["every_seconds"],
                    "enabled": item.get("enabled", True),
                    "config": config,
                },
            )
        )
    for item in document.get("bindings", []):
        resources.append(
            (
                "binding",
                item["key"],
                {
                    "key": item["key"],
                    "space": item["space"],
                    "processor": item["processor"],
                    "consumer": item["consumer"],
                    "enabled": item.get("enabled", True),
                    "activate_inactive": item.get("activate_inactive", False),
                    "lease_seconds": item.get("lease_seconds", 1800),
                    "label": item.get("label"),
                    "url": item.get("url"),
                },
            )
        )
    return resources


def _actual(
    db: Database, resource_type: str, entity_id: str | None, desired: dict[str, Any]
) -> tuple[str | None, dict[str, Any] | None]:
    if resource_type == "space":
        row = db.row("SELECT * FROM spaces WHERE id=?", (desired["id"],))
        return desired["id"], ({"id": row["id"], "name": row["name"]} if row else None)
    if resource_type == "source":
        rows = {item["id"]: item for item in core.list_sources(db)}
        row = rows.get(desired["id"])
        return desired["id"], (
            None
            if row is None
            else {
                "id": row["id"],
                "space": row["space_id"],
                "kind": row["kind"],
                "state": row["state"],
                "config": row["config"],
            }
        )
    if resource_type == "route":
        try:
            row = core.get_route(db, entity_id) if entity_id else None
        except ValueError:
            row = None
        if row is None:
            candidates = [
                item
                for item in core.list_routes(db, space_id=desired["space"])
                if item["name"] == desired["name"]
            ]
            if len(candidates) > 1:
                raise ValueError(
                    f"route {desired['key']} matches multiple unmanaged routes named "
                    f"{desired['name']}"
                )
            row = candidates[0] if len(candidates) == 1 else None
        return (row["id"] if row else None), (
            None
            if row is None
            else {
                "key": desired["key"],
                "space": row["space_id"],
                "name": row["name"],
                "priority": row["priority"],
                "predicate": row["predicate"],
                "processor": row["target"]["processor"],
                "enabled": row["state"] == "enabled",
            }
        )
    if resource_type == "schedule":
        row = db.row("SELECT id FROM adapter_schedules WHERE id=?", (desired["id"],))
        schedule = core.get_schedule(db, desired["id"]) if row else None
        return desired["id"], (
            None
            if schedule is None
            else {
                "id": schedule["id"],
                "adapter": schedule["adapter"],
                "every_seconds": schedule["every_seconds"],
                "enabled": schedule["enabled"],
                "config": schedule["config"],
            }
        )
    if resource_type == "binding":
        rows = [
            item
            for item in core.list_processor_bindings(db, space_id=desired["space"])
            if item["processor"] == desired["processor"]
        ]
        row = (
            next((item for item in rows if item["id"] == entity_id), None)
            if entity_id
            else None
        )
        row = row or (rows[0] if len(rows) == 1 else None)
        return (row["id"] if row else None), (
            None
            if row is None
            else {
                "key": desired["key"],
                "space": row["space_id"],
                "processor": row["processor"],
                "consumer": row["consumer"],
                "enabled": row["state"] == "enabled",
                "activate_inactive": row["activate_inactive"],
                "lease_seconds": row["lease_seconds"],
                "label": row["label"],
                "url": row["url"],
            }
        )
    raise ValueError(f"unsupported topology resource: {resource_type}")


def plan_topology(
    db: Database, document: dict[str, Any], *, prune: bool = False
) -> dict[str, Any]:
    validate_topology(document)
    owner = document["owner"]
    managed = _managed(db, owner)
    all_managed = {
        (row["resource_type"], row["entity_id"]): row
        for row in db.rows("SELECT * FROM managed_resources")
    }
    operations: list[dict[str, Any]] = []
    desired_keys: set[tuple[str, str]] = set()
    for resource_type, resource_key, desired in _normalize(document):
        desired_keys.add((resource_type, resource_key))
        record = managed.get((resource_type, resource_key))
        entity_id, actual = _actual(
            db, resource_type, record["entity_id"] if record else None, desired
        )
        foreign = all_managed.get((resource_type, entity_id)) if entity_id else None
        if actual is None:
            action = "create"
        elif record is not None:
            action = "noop" if actual == desired else "update"
        elif foreign is not None:
            action = "conflict"
        elif actual == desired:
            action = "adopt"
        else:
            action = "conflict"
        operations.append(
            {
                "resource_type": resource_type,
                "resource_key": resource_key,
                "entity_id": entity_id,
                "action": action,
                "desired": desired,
                **({"actual": actual} if action in {"update", "conflict"} else {}),
            }
        )
    if prune:
        for key, record in sorted(managed.items()):
            if key in desired_keys:
                continue
            action = "retain" if record["resource_type"] == "space" else "disable"
            operations.append(
                {
                    "resource_type": record["resource_type"],
                    "resource_key": record["resource_key"],
                    "entity_id": record["entity_id"],
                    "action": action,
                    **(
                        {
                            "reason": "spaces are retained because they may own durable evidence"
                        }
                        if action == "retain"
                        else {}
                    ),
                }
            )
    return {
        "schema_version": DOCUMENT_VERSION,
        "owner": owner,
        "prune": prune,
        "changes": sum(item["action"] not in {"noop", "retain"} for item in operations),
        "conflicts": sum(item["action"] == "conflict" for item in operations),
        "operations": operations,
    }


def _upsert_schedule(db: Database, desired: dict[str, Any]) -> dict[str, Any]:
    config = desired["config"]
    common = {"every_seconds": desired["every_seconds"], "enabled": desired["enabled"]}
    if desired["adapter"] == "ingest-shadow":
        return core.upsert_ingest_schedule(
            db,
            desired["id"],
            status_script=config["status_script"],
            discovery_script=config.get("discovery_script"),
            space_id=config["space_id"],
            timeout=config.get("timeout", 120),
            **common,
        )
    if desired["adapter"] == "inbound-leads":
        return core.upsert_inbound_schedule(
            db,
            desired["id"],
            ledger_script=config["ledger_script"],
            profile=config["profile"],
            discovery_script=config.get("discovery_script"),
            slack_discovery_script=config.get("slack_discovery_script"),
            source_mode=config.get("source_mode", "both"),
            space_id=config["space_id"],
            timeout=config.get("timeout", 240),
            **common,
        )
    return core.upsert_timer_schedule(
        db,
        desired["id"],
        space_id=config["space_id"],
        source_id=config["source_id"],
        event_type=config["event_type"],
        attributes=config.get("attributes"),
        first_run_at=config.get("first_run_at"),
        **common,
    )


def _apply_resource(db: Database, operation: dict[str, Any]) -> str:
    resource_type = operation["resource_type"]
    desired = operation["desired"]
    entity_id = operation.get("entity_id")
    if operation["action"] in {"noop", "adopt"}:
        return entity_id or desired.get("id")
    if resource_type == "space":
        if operation["action"] == "create":
            return core.create_space(db, desired["id"], desired["name"])["id"]
        with db.transaction() as connection:
            connection.execute(
                "UPDATE spaces SET name=? WHERE id=?", (desired["name"], desired["id"])
            )
            core.audit(
                connection,
                command="topology.space-update",
                entity_type="space",
                entity_id=desired["id"],
                payload=desired,
            )
        return desired["id"]
    if resource_type == "source":
        if operation["action"] == "create":
            source = core.register_source(
                db, desired["id"], desired["space"], desired["kind"], desired["config"]
            )
            if desired["state"] != source["state"]:
                with db.transaction() as connection:
                    connection.execute(
                        "UPDATE sources SET state=? WHERE id=?",
                        (desired["state"], desired["id"]),
                    )
            return source["id"]
        with db.transaction() as connection:
            connection.execute(
                "UPDATE sources SET space_id=?, kind=?, state=?, config_json=? WHERE id=?",
                (
                    desired["space"],
                    desired["kind"],
                    desired["state"],
                    json.dumps(desired["config"], sort_keys=True),
                    desired["id"],
                ),
            )
            core.audit(
                connection,
                command="topology.source-update",
                entity_type="source",
                entity_id=desired["id"],
                payload=desired,
            )
        return desired["id"]
    if resource_type == "route":
        if operation["action"] == "create":
            return core.create_route(
                db,
                space_id=desired["space"],
                name=desired["name"],
                priority=desired["priority"],
                predicate=desired["predicate"],
                processor=desired["processor"],
                enabled=desired["enabled"],
            )["id"]
        with db.transaction() as connection:
            connection.execute(
                "UPDATE routes SET space_id=?, name=?, priority=?, predicate_json=?, target_json=?, state=?, updated_at=? WHERE id=?",
                (
                    desired["space"],
                    desired["name"],
                    desired["priority"],
                    json.dumps(desired["predicate"], sort_keys=True),
                    json.dumps(
                        {"kind": "processor", "processor": desired["processor"]},
                        sort_keys=True,
                    ),
                    "enabled" if desired["enabled"] else "disabled",
                    core.now(),
                    entity_id,
                ),
            )
            core.audit(
                connection,
                command="topology.route-update",
                entity_type="route",
                entity_id=entity_id,
                payload=desired,
            )
        return entity_id
    if resource_type == "schedule":
        return _upsert_schedule(db, desired)["id"]
    binding = core.bind_processor(
        db,
        space_id=desired["space"],
        processor=desired["processor"],
        consumer=desired["consumer"],
        activate_inactive=desired["activate_inactive"],
        lease_seconds=desired["lease_seconds"],
        label=desired["label"],
        url=desired["url"],
    )
    if not desired["enabled"]:
        binding = core.set_processor_binding_enabled(db, binding["id"], False)
    return binding["id"]


def _disable_resource(db: Database, operation: dict[str, Any]) -> None:
    kind, entity_id = operation["resource_type"], operation["entity_id"]
    if kind == "route":
        core.set_route_enabled(db, entity_id, False)
    elif kind == "schedule":
        core.set_schedule_enabled(db, entity_id, False)
    elif kind == "binding":
        core.set_processor_binding_enabled(db, entity_id, False)
    elif kind == "source":
        with db.transaction() as connection:
            connection.execute(
                "UPDATE sources SET state='disabled' WHERE id=?", (entity_id,)
            )
            core.audit(
                connection,
                command="topology.source-disable",
                entity_type="source",
                entity_id=entity_id,
                payload={},
            )


def apply_topology(
    db: Database, document: dict[str, Any], *, prune: bool = False
) -> dict[str, Any]:
    plan = plan_topology(db, document, prune=prune)
    if plan["conflicts"]:
        names = [
            f"{item['resource_type']}:{item['resource_key']}"
            for item in plan["operations"]
            if item["action"] == "conflict"
        ]
        raise ValueError(
            "topology conflicts with unmanaged resources: " + ", ".join(names)
        )
    owner = plan["owner"]
    applied: list[dict[str, Any]] = []
    for operation in plan["operations"]:
        action = operation["action"]
        if action == "retain":
            applied.append({**operation, "applied": False})
            continue
        if action == "disable":
            _disable_resource(db, operation)
            applied.append({**operation, "applied": True})
            continue
        entity_id = _apply_resource(db, operation)
        desired = operation["desired"]
        with db.transaction() as connection:
            connection.execute(
                "INSERT INTO managed_resources(owner, resource_type, resource_key, entity_id, checksum, applied_at) "
                "VALUES(?,?,?,?,?,?) ON CONFLICT(owner, resource_type, resource_key) DO UPDATE SET "
                "entity_id=excluded.entity_id, checksum=excluded.checksum, applied_at=excluded.applied_at",
                (
                    owner,
                    operation["resource_type"],
                    operation["resource_key"],
                    entity_id,
                    _checksum(desired),
                    core.now(),
                ),
            )
        applied.append(
            {**operation, "entity_id": entity_id, "applied": action != "noop"}
        )
    return {**plan, "operations": applied}
