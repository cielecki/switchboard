# Switchboard 0.10: operations and decision inbox

## Outcome

Switchboard 0.10 turns the read-only web page into an operator-facing view without making it a
control plane. Operators can see what is healthy, what is queued, what is being processed, what is
waiting for a worker retry, and which distinct human decisions remain. Every mutation continues to
go through the CLI.

This is an event-coordination operations view. It is not an agent command center, task board, or
visual agent workspace.

## Evidence

- The 0.9.1 live deployment drains Nina work and recovers accepted-but-unclaimed wakes.
- The live Nina space has no stranded queue, while the ingest space contains several
  `needs-review` runs that collapse to fewer actual decisions.
- The 0.9 dashboard renders complete history tables by default. This preserves evidence but makes
  current work hard to distinguish from history.
- Claude and Codex consumers are durable chat identities, but the current binding view exposes only
  their raw identifiers.

## Scope

### Durable review groups

- Add a review group with a stable key inside a space.
- Link one or more `needs-review` processor runs to the group.
- Infer a safe initial key from structured `shared_decision_task` or `decision_task` fields, falling
  back to a per-run key.
- Backfill existing `needs-review` runs during schema migration without changing their run state.
- Resolve all still-open members of a group through one auditable CLI command.
- Keep the existing per-run review command for precise operations and compatibility.

### CLI-only control

Add processor review commands for listing, showing, linking, and resolving groups. Add optional
human-readable labels and local navigation URLs to processor bindings. URLs remain private runtime
configuration and are redacted by portable topology export.

### Read-only operations view

- Make the selected space the primary unit of navigation.
- Lead with actionable lanes: needs decision, queued, working, delivery/retry trouble, and recent
  completions.
- Group review work by decision instead of repeating every source item.
- Show source and schedule health without flooding the page with raw adapter history.
- Keep technical tables available in collapsed detail sections.
- Provide a read-only run detail view with its event, route, delivery, attempts, and outcome.
- Link to configured local consumer/review destinations without changing external state.

### Reliability and portability

- Preserve the accepted-delivery retry and stable request-ID behavior from 0.9.1.
- Extend tests and acceptance coverage across migration, grouped review, CLI behavior, read-only
  HTTP behavior, and an unavailable-worker recovery cycle.
- Export and adopt a private live topology after installation so the deployment is reproducible.

## Activation

1. Back up and verify the live database.
2. Release 0.10.0 from the public `cielecki/switchboard` repository.
3. Update the Codex and Claude plugin installations.
4. Reinstall the macOS service from the installed 0.10.0 launcher.
5. Let the schema migration backfill review groups, then inspect the plan and adopt the private live
   topology.
6. Configure private chat labels and links through the CLI.
7. Verify `doctor`, the web view, both spaces, and a bounded live source cycle.

The disabled legacy `inbound-watch` and `ingest` scheduled tasks remain available for this release
as rollback evidence. They are not re-enabled and can be removed after the 0.10 live soak.

## Rollback

The migration is additive. A verified pre-upgrade backup is retained. If activation fails, stop the
service, reinstall 0.9.1, and use the documented explicit restore procedure while the supervisor is
stopped. Never restore over a running database.
