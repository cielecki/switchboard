# Switchboard 0.9: portable operations

## Outcome

Switchboard 0.9 makes an installed coordinator reproducible, diagnosable, and recoverable
without turning its read-only web dashboard into a control surface. A fresh local install can
plan and apply a non-secret topology, validate its runtime, back up its database, and prove that a
processor which was offline can resume without duplicate alert noise or lost work.

## Evidence and constraints

- The live 0.8.1 deployment recovers queued processor work after an unavailable worker returns.
- Processor alerts are currently stored per delivery generation, so several failed deliveries for
  one consumer can still produce several notifications during the same outage.
- The working deployment was assembled with individual CLI commands and contains machine-specific
  executable paths and durable chat identifiers.
- The plugin is distributed to Codex and Claude Code from this public repository. Captured content,
  credentials, personal routing rules, and machine-specific paths must remain outside it.
- The CLI remains the only mutation interface. The web dashboard remains read-only observability.
- Switchboard owns coordination mechanics. Adapters and processors retain domain policy.

## Scope

### Consumer-level alert episodes

Replace delivery-level notifications with durable consumer outage episodes:

- transition from zero to at least one qualifying delivery issue opens one episode;
- an open episode absorbs additional affected deliveries and restarts;
- transition back to zero qualifying issues recovers the episode once;
- notification and recovery claims are persisted before invoking the external alert command;
- an invocation failure is visible in the episode record and is not retried as an alert storm.

Existing delivery-level alert rows remain readable migration evidence.

### Declarative topology

Add these CLI commands:

- `switchboard topology export [--output FILE]`
- `switchboard topology plan FILE`
- `switchboard topology apply FILE [--prune]`

The JSON document covers spaces, sources, routes, schedules, and processor bindings. Exported
absolute paths and consumer identifiers are replaced with explicit `${VARIABLE}` placeholders
unless the caller supplies variable values. `plan` resolves variables from `--var NAME=VALUE` and
the environment, validates all references and paths, and reports deterministic create/update/noop
operations. `apply` executes the same plan idempotently.

Every topology has an `owner`. Apply only updates resources declared by that owner. Unmanaged
resources are never changed. Missing managed resources are retained unless `--prune` is explicit;
durable evidence that cannot safely be deleted is disabled instead.

### Doctor

Add `switchboard doctor` with human and `--json` output and a non-zero exit when errors exist. It
checks at least:

- database integrity and schema version;
- configured schedule executables and required files;
- schedule failure/staleness and supervisor heartbeat;
- launchd installation and launcher/database drift on macOS;
- relay and alert command paths from the installed service;
- enabled bindings, pending/accepted deliveries, active leases, and open alert episodes;
- read-only dashboard reachability when the service exposes a URL.

Warnings describe degraded or ambiguous state; errors identify conditions that prevent safe work.

### Backups

Add:

- `switchboard backup create FILE`
- `switchboard backup verify FILE`

Creation uses SQLite's online backup API, writes through a temporary sibling, runs an integrity
check, and atomically installs the verified file. Version 0.9 documents restoration but does not
automatically replace a live database.

### Distribution and acceptance

- Add CI for Python 3.12 and 3.13, unit tests, package build, and both plugin validators.
- Keep generic examples public; keep the live Nina/ingest topology private.
- Validate a fresh temporary install: apply an example topology, emit work, simulate an unavailable
  worker, restart the supervisor, recover the worker, and drain the backlog with one alert episode.
- Release 0.9.0, update both local plugin installations, reinstall the macOS service from the new
  installed launcher, and verify the live deployment without changing domain policy.

## Alternatives deferred

- More inbound adapters or replacing the ingest workflow: defer until topology portability and
  recovery are proven.
- A mutable web control plane: rejected for this product; CLI remains authoritative.
- An agent command center or visual workspace: separate product.
- Linux and Windows service managers: core and CLI stay portable, but 0.9 operational support stays
  macOS-first.
- MCP tooling: the plugin can remain a skill plus local CLI until a concrete non-CLI use case exists.

## Rollout and rollback

All new commands are additive. Database migrations preserve prior alert evidence. Before live
activation, create and verify a backup. If the upgraded service fails validation, stop it, reinstall
the 0.8.1 plugin/service, and retain the 0.9 database backup and logs for diagnosis. Never restore a
database over a running supervisor.

## Success criteria

- A fresh database reaches the same declared generic topology through repeated idempotent applies.
- A worker outage with multiple queued deliveries emits one unreachable notification and one
  recovery notification.
- Restarting during the outage does not duplicate either notification.
- `doctor` explains broken executable paths, stale service configuration, and queue health without
  mutating state.
- A created backup passes `PRAGMA integrity_check` and opens at the expected schema version.
- The public plugin installs and validates in Codex and Claude Code without private configuration.
