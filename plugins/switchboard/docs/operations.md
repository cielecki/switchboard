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

Topology files may contain machine-local paths and chat identifiers after variables are resolved.
Keep live documents private. Public repositories should contain only generic templates.

## Doctor

`switchboard doctor` checks database integrity and schema, adapter paths, schedule failures and
overdue work, supervisor heartbeat, macOS launch-agent drift, relay and alert-command paths, source
health, queues, leases, alert episodes, and the configured dashboard URL. JSON output has stable
`errors`, `warnings`, `findings`, and `queues` fields. Errors produce exit status 1; invalid command
or database input produces exit status 2.

Doctor reports; it does not repair or reconfigure the service.

## Backup and restore

`backup create FILE` uses SQLite's online backup API, verifies the copy, then atomically moves it
into place. It refuses to overwrite an existing file. `backup verify FILE` opens the copy read-only,
runs `PRAGMA integrity_check`, and reports its schema version.

Version 0.9 intentionally has no restore command. To restore:

1. stop the service with `switchboard service uninstall`;
2. preserve the current database and its `-wal`/`-shm` siblings;
3. verify the selected backup;
4. copy it to a new explicit database path;
5. run `switchboard --db NEW_PATH doctor`;
6. reinstall the service using that database only after the doctor result is acceptable.

Never replace the database underneath a running supervisor.
