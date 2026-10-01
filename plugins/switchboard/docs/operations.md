# Portable operations

## Declarative topology

A topology is a JSON document with a stable `owner` and arrays for spaces, sources, routes,
schedules, and processor bindings. One owner can reconcile only the resources recorded for it.
Unmanaged state is never silently adopted when its configuration differs.

Export replaces absolute paths and durable chat consumers with `${VARIABLE}` placeholders:

```bash
switchboard --json topology export --owner example --output topology.json
```

An explicit `--include-local-values` produces a mode-`0600` private snapshot containing paths and
consumer IDs. It is suitable for restoring one machine and must not be committed or shared.

Supply every required value through `--var NAME=VALUE` or the environment, then inspect and apply
the same plan:

```bash
switchboard --json topology plan topology.json --var WORKER=chat:claude:session-id
switchboard --json topology apply topology.json --var WORKER=chat:claude:session-id
```

Repeated apply is a no-op when the database already matches. `--prune` disables routes, schedules,
bindings, and sources omitted by their owner. It retains spaces because they may anchor durable
events. Resources created by another owner or manually with different settings produce a conflict.

An interval schedule keeps the existing `every_seconds` form. A calendar schedule uses
`"schedule_kind":"calendar"`, omits `every_seconds`, and stores its normalized rule under
`config.calendar`. Export preserves this distinction, and repeated apply does not reset a calendar
schedule's next occurrence or revision.

Topology files may contain machine-local paths and chat identifiers after variables are resolved.
Keep live documents private. Public repositories should contain only generic templates.

## Timer-source contract and legacy repair

A schedule with `schedule_kind` set to `interval` or `calendar` and adapter `timer` requires its
configured source to belong to the schedule's space and have kind `timer`. When the source is
missing, schedule creation provisions it with that contract. If an existing source belongs to
another space or has another kind, Switchboard rejects the operation without writing the schedule.
Topology validation applies the same rule before it plans or applies resources. A wall-clock
schedule uses `schedule_kind: calendar`; `calendar` is not a source kind.

`doctor` reports a source that violates this contract as `schedule.timer-source-mismatch`. The
diagnostic includes the schedule and source IDs, the expected space and kind, and the observed
space, kind, and state. It also checks disabled schedules. Use this CLI-only sequence to diagnose
and repair a confirmed legacy source created before version 0.11.5:

```bash
switchboard --json doctor
switchboard --json schedule list
switchboard --json source list

switchboard --json backup create /absolute/private/path/switchboard-before-repair.sqlite3
switchboard --json backup verify /absolute/private/path/switchboard-before-repair.sqlite3

switchboard --json schedule repair-calendar-source <schedule-id>
switchboard --json schedule list
switchboard --json source list
switchboard --json doctor

switchboard --json topology export --owner <owner> --include-local-values \
  --output /absolute/private/path/repaired-topology.json
switchboard --json topology plan /absolute/private/path/repaired-topology.json
```

Choose unused paths and ensure no concurrent writer uses them. Both commands reject a destination
that already exists when checked; `topology export` overwrites only when `--force` is requested.
Keep the topology snapshot private because `--include-local-values` disables redaction and the
export may contain local paths, chat identifiers, URLs, and other private source configuration.
Read back the repair result. Treat the stored topology as reconciled only when the final topology
plan reports no changes or conflicts.

A mutating repair accepts only an existing timer-backed calendar schedule whose source kind is
exactly `calendar` and whose adapter marker is absent or set to `calendar`. Every schedule that
shares the source must be a compatible timer-backed calendar or interval schedule in the same
space.

On success, the repair changes the source kind and adapter marker to `timer` and records an audit
entry. It preserves all unrelated source configuration and state, the schedule revision and
`next_run_at`, routes, events, runs, and deliveries. It rejects invalid, unsupported, or
incompatible state without mutation. A second invocation returns `already-repaired` and adds no
audit entry.

## Doctor

`switchboard doctor` checks database integrity and schema, adapter paths, schedule failures and
overdue work, supervisor heartbeat, macOS launch-agent drift, relay and alert-command paths, source
health, queues, leases, alert episodes, and the configured dashboard URL. JSON output has stable
`errors`, `warnings`, `findings`, and `queues` fields. Errors produce exit status 1; invalid command
or database input produces exit status 2.

Doctor reports; it does not repair or reconfigure the service.

## Backup and restore

`backup create FILE` checks that the destination does not exist, uses SQLite's online backup API,
verifies the copy, then atomically installs it. Avoid concurrent writers to the destination.
`backup verify FILE` opens the copy read-only, runs `PRAGMA integrity_check`, and reports its schema
version.

Version 0.9 intentionally has no restore command. To restore:

1. stop the service with `switchboard service uninstall`;
2. preserve the current database and its `-wal`/`-shm` siblings;
3. verify the selected backup;
4. copy it to a new explicit database path;
5. run `switchboard --db NEW_PATH doctor`;
6. reinstall the service using that database only after the doctor result is acceptable.

Never replace the database underneath a running supervisor.
