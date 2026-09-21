from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

from . import core
from .adapters import run_command_adapter, run_inbound_leads, run_ingest_shadow
from .db import SCHEMA_VERSION, Database
from .service import install_launch_agent, service_status, uninstall_launch_agent
from .supervisor import run_forever, run_once
from .web import serve


def json_object(value: str) -> dict[str, Any]:
    try:
        result = json.loads(value)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    if not isinstance(result, dict):
        raise argparse.ArgumentTypeError("value must be a JSON object")
    return result


def json_string_array(value: str) -> list[str]:
    try:
        result = json.loads(value)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    if not isinstance(result, list) or not result or not all(
        isinstance(item, str) and item for item in result
    ):
        raise argparse.ArgumentTypeError("value must be a non-empty JSON array of strings")
    return result


def json_array(value: str) -> list[Any]:
    try:
        result = json.loads(value)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    if not isinstance(result, list):
        raise argparse.ArgumentTypeError("value must be a JSON array")
    return result


def key_value(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("expected FIELD=VALUE")
    key, item = value.split("=", 1)
    if not key:
        raise argparse.ArgumentTypeError("field cannot be empty")
    return key, item


def add_list_filter(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--state")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="switchboard", description=__doc__)
    parser.add_argument("--db", help="SQLite database path (default: SWITCHBOARD_DB or XDG data)")
    parser.add_argument("--json", action="store_true", help="emit a stable JSON response")
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("init", help="initialize the database")
    commands.add_parser("status", help="show store and queue counts")

    space = commands.add_parser("space", help="manage spaces").add_subparsers(dest="verb", required=True)
    create = space.add_parser("create")
    create.add_argument("id")
    create.add_argument("--name")
    space.add_parser("list")

    source = commands.add_parser("source", help="manage sources").add_subparsers(dest="verb", required=True)
    register = source.add_parser("register")
    register.add_argument("id")
    register.add_argument("--space", required=True)
    register.add_argument("--kind", required=True)
    register.add_argument("--config", type=json_object, default={})
    source.add_parser("list")

    adapter = commands.add_parser("adapter", help="run and inspect source adapters").add_subparsers(
        dest="verb", required=True
    )
    run = adapter.add_parser("run", help="apply one external JSON adapter snapshot")
    run.add_argument("--name", required=True)
    run.add_argument("--command-json", required=True, type=json_string_array)
    run.add_argument("--timeout", type=int, default=120)
    ingest = adapter.add_parser(
        "ingest-shadow", help="run optional bounded discovery and observe ingest through status.py"
    )
    ingest.add_argument("--status-script", required=True)
    ingest.add_argument("--discovery-script")
    ingest.add_argument("--python", default=sys.executable)
    ingest.add_argument("--space", default="personal-ingest")
    ingest.add_argument("--timeout", type=int, default=120)
    inbound = adapter.add_parser(
        "inbound-leads", help="discover and observe pending inbound-leads pointers"
    )
    inbound.add_argument("--ledger-script", required=True)
    inbound.add_argument("--profile", required=True)
    inbound.add_argument("--discovery-script")
    inbound.add_argument("--slack-discovery-script")
    inbound.add_argument("--python", default=sys.executable)
    inbound.add_argument("--space", default="inbound-leads")
    inbound.add_argument("--timeout", type=int, default=240)
    runs = adapter.add_parser("runs", help="show recent adapter runs")
    runs.add_argument("--limit", type=int, default=50)

    schedule = commands.add_parser(
        "schedule", help="manage persistent adapter schedules"
    ).add_subparsers(dest="verb", required=True)
    add_ingest = schedule.add_parser(
        "add-ingest-shadow", help="schedule bounded ingest discovery and store observation"
    )
    add_ingest.add_argument("id")
    add_ingest.add_argument("--status-script", required=True)
    add_ingest.add_argument("--discovery-script")
    add_ingest.add_argument("--every", type=int, required=True, help="interval in seconds")
    add_ingest.add_argument("--space", default="personal-ingest")
    add_ingest.add_argument("--timeout", type=int, default=120)
    add_ingest.add_argument("--disabled", action="store_true")
    add_inbound = schedule.add_parser(
        "add-inbound-leads", help="schedule inbound discovery and ledger observation"
    )
    add_inbound.add_argument("id")
    add_inbound.add_argument("--ledger-script", required=True)
    add_inbound.add_argument("--profile", required=True)
    add_inbound.add_argument("--discovery-script")
    add_inbound.add_argument("--slack-discovery-script")
    add_inbound.add_argument("--every", type=int, required=True, help="interval in seconds")
    add_inbound.add_argument("--space", default="inbound-leads")
    add_inbound.add_argument("--timeout", type=int, default=240)
    add_inbound.add_argument("--disabled", action="store_true")
    schedule.add_parser("list")
    enable = schedule.add_parser("enable")
    enable.add_argument("id")
    disable = schedule.add_parser("disable")
    disable.add_argument("id")
    delete = schedule.add_parser("delete")
    delete.add_argument("id")

    wait = commands.add_parser("wait", help="manage durable waits").add_subparsers(dest="verb", required=True)
    create = wait.add_parser("create")
    create.add_argument("--space", required=True)
    create.add_argument("--consumer", required=True)
    create.add_argument("--source")
    create.add_argument("--event-type")
    create.add_argument("--attribute", action="append", type=key_value, default=[])
    create.add_argument("--contains", action="append", type=key_value, default=[])
    create.add_argument("--purpose", default="")
    create.add_argument("--repeat", action="store_true")
    create.add_argument("--expires")
    listing = wait.add_parser("list")
    add_list_filter(listing)
    cancel = wait.add_parser("cancel")
    cancel.add_argument("id")

    route = commands.add_parser(
        "route", help="manage deterministic processor routing"
    ).add_subparsers(dest="verb", required=True)
    create_route = route.add_parser("create")
    create_route.add_argument("--space", required=True)
    create_route.add_argument("--name", required=True)
    create_route.add_argument("--processor", required=True)
    create_route.add_argument("--priority", type=int, default=100)
    create_route.add_argument("--source")
    create_route.add_argument("--event-type")
    create_route.add_argument("--attribute", action="append", type=key_value, default=[])
    create_route.add_argument("--contains", action="append", type=key_value, default=[])
    create_route.add_argument("--disabled", action="store_true")
    list_routes = route.add_parser("list")
    list_routes.add_argument("--space")
    list_routes.add_argument("--state", choices=["enabled", "disabled"])
    for verb in ("enable", "disable", "delete"):
        route_mutation = route.add_parser(verb)
        route_mutation.add_argument("id")
    apply_route = route.add_parser("apply")
    apply_route.add_argument("event_id")

    processor = commands.add_parser(
        "processor", help="inspect and record structured processor outcomes"
    ).add_subparsers(dest="verb", required=True)
    list_processors = processor.add_parser("list")
    list_processors.add_argument("--state")
    list_processors.add_argument("--processor")
    list_processors.add_argument("--event")
    for verb in ("show", "start", "retry"):
        processor_mutation = processor.add_parser(verb)
        processor_mutation.add_argument("id")
    bind = processor.add_parser("bind", help="bind a processor queue to a durable chat")
    bind.add_argument("--space", required=True)
    bind.add_argument("--processor", required=True)
    bind.add_argument("--consumer", required=True)
    bind.add_argument("--lease", type=int, default=1800)
    bind.add_argument("--activate-inactive", action="store_true")
    bindings = processor.add_parser("bindings")
    bindings.add_argument("--space")
    bindings.add_argument("--state", choices=["enabled", "disabled"])
    binding_show = processor.add_parser("binding-show")
    binding_show.add_argument("id")
    for verb in ("binding-enable", "binding-disable"):
        binding_mutation = processor.add_parser(verb)
        binding_mutation.add_argument("id")
    claim = processor.add_parser("claim")
    claim.add_argument("id")
    claim.add_argument("--worker", required=True)
    claim.add_argument("--lease", type=int)
    heartbeat = processor.add_parser("heartbeat")
    heartbeat.add_argument("id")
    heartbeat.add_argument("--worker", required=True)
    heartbeat.add_argument("--lease", type=int)
    release = processor.add_parser("release")
    release.add_argument("id")
    release.add_argument("--worker", required=True)
    release.add_argument("--reason", default="")
    processor_delivery_list = processor.add_parser("delivery-list")
    processor_delivery_list.add_argument("--state")
    processor_delivery_show = processor.add_parser("delivery-show")
    processor_delivery_show.add_argument("id")
    processor_delivery_dispatch = processor.add_parser("delivery-dispatch")
    processor_delivery_dispatch.add_argument("id")
    processor_delivery_dispatch.add_argument(
        "--relay", default=os.environ.get("SWITCHBOARD_CHATS_RELAY")
    )
    processor_delivery_dispatch.add_argument("--activate-inactive", action="store_true")
    processor_delivery_dispatch.add_argument("--timeout", type=int, default=30)
    for verb in ("complete", "fail", "needs-review"):
        finish_processor = processor.add_parser(verb)
        finish_processor.add_argument("id")
        finish_processor.add_argument("--worker")
        finish_processor.add_argument("--summary", default="")
        finish_processor.add_argument("--facts", type=json_object, default={})
        finish_processor.add_argument("--decision", type=json_object, default={})
        finish_processor.add_argument("--actions", type=json_array, default=[])
        if verb in {"fail", "needs-review"}:
            finish_processor.add_argument("--error", required=verb == "fail")

    event = commands.add_parser("event", help="emit and inspect events").add_subparsers(dest="verb", required=True)
    emit = event.add_parser("emit")
    emit.add_argument("--source", required=True)
    emit.add_argument("--external-id", required=True)
    emit.add_argument("--type", required=True)
    emit.add_argument("--attributes", type=json_object, default={})
    emit.add_argument("--occurred-at")
    listing = event.add_parser("list")
    listing.add_argument("--limit", type=int, default=50)
    show = event.add_parser("show")
    show.add_argument("id")

    delivery = commands.add_parser("delivery", help="inspect and acknowledge deliveries").add_subparsers(dest="verb", required=True)
    listing = delivery.add_parser("list")
    add_list_filter(listing)
    show = delivery.add_parser("show")
    show.add_argument("id")
    dispatch_delivery = delivery.add_parser("dispatch")
    dispatch_delivery.add_argument("id")
    dispatch_delivery.add_argument("--relay", default=os.environ.get("SWITCHBOARD_CHATS_RELAY"))
    dispatch_delivery.add_argument("--activate-inactive", action="store_true")
    dispatch_delivery.add_argument("--timeout", type=int, default=30)
    acknowledge = delivery.add_parser("ack")
    acknowledge.add_argument("id")

    server = commands.add_parser("serve", help="serve the read-only operations UI")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8765)

    supervisor = commands.add_parser(
        "supervisor", help="run scheduled adapters and delivery dispatch"
    ).add_subparsers(dest="verb", required=True)
    supervisor.add_parser("status")
    for verb in ("once", "run"):
        supervise = supervisor.add_parser(verb)
        supervise.add_argument("--relay", default=os.environ.get("SWITCHBOARD_CHATS_RELAY"))
        supervise.add_argument("--activate-inactive", action="store_true")
        supervise.add_argument("--delivery-timeout", type=int, default=30)
        supervise.add_argument("--delivery-retry", type=int, default=60)
        supervise.add_argument("--delivery-batch", type=int, default=1)
        supervise.add_argument("--alert-command-json", type=json_string_array)
        supervise.add_argument("--alert-after", type=int, default=900)
        if verb == "run":
            supervise.add_argument("--host", default="127.0.0.1")
            supervise.add_argument("--port", type=int, default=8765)
            supervise.add_argument("--poll", type=int, default=5)

    service = commands.add_parser(
        "service", help="manage the persistent macOS launch agent"
    ).add_subparsers(dest="verb", required=True)
    install = service.add_parser("install")
    install.add_argument("--relay", default=os.environ.get("SWITCHBOARD_CHATS_RELAY"))
    install.add_argument("--activate-inactive", action="store_true")
    install.add_argument("--host", default="127.0.0.1")
    install.add_argument("--port", type=int, default=8765)
    install.add_argument("--poll", type=int, default=5)
    install.add_argument("--delivery-retry", type=int, default=60)
    install.add_argument("--delivery-batch", type=int, default=1)
    install.add_argument("--alert-command-json", type=json_string_array)
    install.add_argument("--alert-after", type=int, default=900)
    service.add_parser("uninstall")
    service.add_parser("status")
    return parser


def dispatch(args: argparse.Namespace, db: Database) -> Any:
    if args.command == "init":
        db.initialize()
        return {"database": str(db.path), "schema_version": SCHEMA_VERSION}
    if args.command == "status":
        return core.status(db)
    if args.command == "space":
        if args.verb == "create":
            return core.create_space(db, args.id, args.name)
        return core.list_spaces(db)
    if args.command == "source":
        if args.verb == "register":
            return core.register_source(db, args.id, args.space, args.kind, args.config)
        return core.list_sources(db)
    if args.command == "adapter":
        if args.verb == "run":
            return run_command_adapter(
                db,
                args.command_json,
                adapter_name=args.name,
                timeout=args.timeout,
            )
        if args.verb == "ingest-shadow":
            return run_ingest_shadow(
                db,
                status_script=args.status_script,
                discovery_script=args.discovery_script,
                python=args.python,
                space_id=args.space,
                timeout=args.timeout,
            )
        if args.verb == "inbound-leads":
            return run_inbound_leads(
                db,
                ledger_script=args.ledger_script,
                profile=args.profile,
                discovery_script=args.discovery_script,
                slack_discovery_script=args.slack_discovery_script,
                python=args.python,
                space_id=args.space,
                timeout=args.timeout,
            )
        return core.list_adapter_runs(db, args.limit)
    if args.command == "schedule":
        if args.verb == "add-ingest-shadow":
            return core.upsert_ingest_schedule(
                db,
                args.id,
                status_script=args.status_script,
                discovery_script=args.discovery_script,
                every_seconds=args.every,
                space_id=args.space,
                timeout=args.timeout,
                enabled=not args.disabled,
            )
        if args.verb == "add-inbound-leads":
            return core.upsert_inbound_schedule(
                db,
                args.id,
                ledger_script=args.ledger_script,
                profile=args.profile,
                discovery_script=args.discovery_script,
                slack_discovery_script=args.slack_discovery_script,
                every_seconds=args.every,
                space_id=args.space,
                timeout=args.timeout,
                enabled=not args.disabled,
            )
        if args.verb == "enable":
            return core.set_schedule_enabled(db, args.id, True)
        if args.verb == "disable":
            return core.set_schedule_enabled(db, args.id, False)
        if args.verb == "delete":
            return core.delete_schedule(db, args.id)
        return core.list_schedules(db)
    if args.command == "wait":
        if args.verb == "create":
            predicate = {
                key: value
                for key, value in (("source_id", args.source), ("event_type", args.event_type))
                if value is not None
            }
            if args.attribute:
                predicate["attributes"] = dict(args.attribute)
            if args.contains:
                predicate["contains"] = dict(args.contains)
            if not predicate:
                raise ValueError("wait predicate cannot be empty")
            return core.create_wait(
                db,
                space_id=args.space,
                consumer=args.consumer,
                predicate=predicate,
                purpose=args.purpose,
                repeating=args.repeat,
                expires_at=args.expires,
            )
        if args.verb == "cancel":
            return core.cancel_wait(db, args.id)
        return core.list_waits(db, args.state)
    if args.command == "route":
        if args.verb == "create":
            predicate = {
                key: value
                for key, value in (("source_id", args.source), ("event_type", args.event_type))
                if value is not None
            }
            if args.attribute:
                predicate["attributes"] = dict(args.attribute)
            if args.contains:
                predicate["contains"] = dict(args.contains)
            return core.create_route(
                db,
                space_id=args.space,
                name=args.name,
                predicate=predicate,
                processor=args.processor,
                priority=args.priority,
                enabled=not args.disabled,
            )
        if args.verb == "enable":
            return core.set_route_enabled(db, args.id, True)
        if args.verb == "disable":
            return core.set_route_enabled(db, args.id, False)
        if args.verb == "delete":
            return core.delete_route(db, args.id)
        if args.verb == "apply":
            return core.apply_routes_to_event(db, args.event_id)
        return core.list_routes(db, space_id=args.space, state=args.state)
    if args.command == "processor":
        if args.verb == "bind":
            return core.bind_processor(
                db,
                space_id=args.space,
                processor=args.processor,
                consumer=args.consumer,
                activate_inactive=args.activate_inactive,
                lease_seconds=args.lease,
            )
        if args.verb == "bindings":
            return core.list_processor_bindings(db, space_id=args.space, state=args.state)
        if args.verb == "binding-show":
            return core.get_processor_binding(db, args.id)
        if args.verb == "binding-enable":
            return core.set_processor_binding_enabled(db, args.id, True)
        if args.verb == "binding-disable":
            return core.set_processor_binding_enabled(db, args.id, False)
        if args.verb == "show":
            return core.get_processor_run(db, args.id)
        if args.verb == "start":
            return core.start_processor_run(db, args.id)
        if args.verb == "claim":
            return core.claim_processor_run(
                db, args.id, worker=args.worker, lease_seconds=args.lease
            )
        if args.verb == "heartbeat":
            return core.heartbeat_processor_run(
                db, args.id, worker=args.worker, lease_seconds=args.lease
            )
        if args.verb == "release":
            return core.release_processor_run(
                db, args.id, worker=args.worker, reason=args.reason
            )
        if args.verb == "retry":
            return core.retry_processor_run(db, args.id)
        if args.verb == "delivery-list":
            return core.list_processor_deliveries(db, args.state)
        if args.verb == "delivery-show":
            return core.get_processor_delivery(db, args.id)
        if args.verb == "delivery-dispatch":
            if not args.relay:
                raise ValueError("pass --relay or set SWITCHBOARD_CHATS_RELAY")
            return core.dispatch_processor_delivery(
                db,
                args.id,
                relay=args.relay,
                cli_command=own_cli_command(),
                activate_inactive=args.activate_inactive,
                timeout=args.timeout,
            )
        if args.verb in {"complete", "fail", "needs-review"}:
            state = {"complete": "completed", "fail": "failed"}.get(args.verb, args.verb)
            return core.finish_processor_run(
                db,
                args.id,
                state=state,
                summary=args.summary,
                facts=args.facts,
                decision=args.decision,
                actions=args.actions,
                error=getattr(args, "error", None),
                worker=args.worker,
            )
        return core.list_processor_runs(
            db, state=args.state, processor=args.processor, event_id=args.event
        )
    if args.command == "event":
        if args.verb == "emit":
            return core.emit_event(
                db,
                source_id=args.source,
                external_id=args.external_id,
                event_type=args.type,
                attributes=args.attributes,
                occurred_at=args.occurred_at,
            )
        if args.verb == "show":
            return core.get_event(db, args.id)
        return core.list_events(db, args.limit)
    if args.command == "delivery":
        if args.verb == "show":
            return core.get_delivery(db, args.id)
        if args.verb == "dispatch":
            if not args.relay:
                raise ValueError("pass --relay or set SWITCHBOARD_CHATS_RELAY")
            return core.dispatch_delivery(
                db,
                args.id,
                relay=args.relay,
                cli_command=own_cli_command(),
                activate_inactive=args.activate_inactive,
                timeout=args.timeout,
            )
        if args.verb == "ack":
            return core.acknowledge_delivery(db, args.id)
        return core.list_deliveries(db, args.state)
    if args.command == "serve":
        serve(db, args.host, args.port)
        return None
    if args.command == "supervisor":
        if args.verb == "status":
            return core.supervisor_status(db)
        options = {
            "relay": args.relay,
            "cli_command": own_cli_command(),
            "activate_inactive": args.activate_inactive,
            "delivery_timeout": args.delivery_timeout,
            "delivery_retry_seconds": args.delivery_retry,
            "delivery_batch_size": args.delivery_batch,
            "alert_command": args.alert_command_json,
            "alert_after_seconds": args.alert_after,
        }
        if args.verb == "once":
            return run_once(db, **options)
        run_forever(
            db,
            host=args.host,
            port=args.port,
            poll_seconds=args.poll,
            **options,
        )
        return core.supervisor_status(db)
    if args.command == "service":
        if args.verb == "install":
            return install_launch_agent(
                db,
                cli_command=service_cli_command(),
                relay=args.relay,
                host=args.host,
                port=args.port,
                poll_seconds=args.poll,
                delivery_retry_seconds=args.delivery_retry,
                delivery_batch_size=args.delivery_batch,
                alert_command=args.alert_command_json,
                alert_after_seconds=args.alert_after,
                activate_inactive=args.activate_inactive,
            )
        if args.verb == "uninstall":
            return uninstall_launch_agent()
        return service_status()
    raise ValueError(f"unsupported command: {args.command}")


def own_cli_command() -> list[str]:
    configured = os.environ.get("SWITCHBOARD_CLI")
    if configured:
        return [configured]
    bundled = Path(__file__).resolve().parents[2] / "bin" / "switchboard"
    if bundled.is_file():
        return [str(bundled)]
    return [sys.executable, "-m", "switchboard.cli"]


def service_cli_command() -> list[str]:
    bundled = Path(__file__).resolve().parents[2] / "bin" / "switchboard"
    if bundled.is_file():
        return [sys.executable, str(bundled)]
    return [sys.executable, "-m", "switchboard.cli"]


def print_result(value: Any, machine: bool) -> None:
    if value is None:
        return
    if machine:
        print(json.dumps({"ok": True, "data": value}, ensure_ascii=False, sort_keys=True))
        return
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    db = Database(args.db)
    try:
        result = dispatch(args, db)
        print_result(result, args.json)
        return 0
    except (ValueError, sqlite3.IntegrityError) as exc:
        if args.json:
            print(json.dumps({"ok": False, "error": str(exc)}, sort_keys=True))
        else:
            print(f"switchboard: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
