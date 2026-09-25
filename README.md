# Switchboard

Switchboard is a local-first event coordination service for AI agents. Sources publish normalized
events, deterministic rules and durable waits decide what should receive them, and every match and
delivery remains inspectable.

The CLI is the control surface. The bundled web interface is read-only.

## Current alpha

The first vertical slice provides:

- spaces and registered sources;
- an immutable, deduplicated SQLite event ledger;
- deterministic wait predicates;
- pending delivery records when waits match;
- JSON output suitable for agent use;
- a local read-only status dashboard;
- a versioned JSON adapter contract and adapter-run history;
- a read-only shadow adapter for existing ingest stores;
- delivery through the existing local `chats` broker, with acceptance and completion tracked
  separately;
- persistent adapter schedules, automatic delivery dispatch, and supervisor heartbeats;
- a CLI-installed macOS launch agent that runs the supervisor and read-only web UI;
- ordered, first-match routing tables that create idempotent processor jobs;
- structured processor outcomes with separate facts, decisions, actions, and errors;
- durable processor-to-chat bindings, delivery history, and expiring work leases;
- consumer-level unreachable and recovery alert episodes through a configurable local command;
- a dedicated inbound-leads adapter that stores pointers and state, never message bodies;
- independently executing schedules with descendant-safe timeouts;
- deterministic timer events, grouped human-review resolution, and space-filtered queue views;
- an action-oriented dashboard with worker health, decision groups, queue lanes, and run lifecycles;
- declarative topology planning and idempotent apply through the CLI;
- self-diagnosis plus verified online SQLite backups.

Existing capture and lead-processing systems can integrate as adapters before any migration or
replacement. Domain processors remain responsible for their own policy and external writes.

## Try it

Switchboard has no runtime dependencies beyond Python 3.12. When loaded as a plugin, use the
bundled `bin/switchboard` launcher. For development, install the package locally:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .

switchboard init
switchboard space create demo --name "Demo"
switchboard source register demo-mail --space demo --kind mail
switchboard wait create --space demo --consumer chat:codex:example-task-id \
  --source demo-mail --event-type message.received --attribute sender=person@example.com
switchboard event emit --source demo-mail --external-id msg-1 \
  --type message.received --attributes '{"sender":"person@example.com","subject":"Hello"}'
switchboard delivery list
switchboard serve
```

Open <http://127.0.0.1:8765> for the read-only operations view.

Use `--json` before the command for stable machine-readable output:

```bash
switchboard --json status
```

By default, data lives at `~/.local/share/switchboard/switchboard.sqlite3`. Override it with
`SWITCHBOARD_DB` or the global `--db` option. Plugin updates never own or replace that state.

## Portable operations

Export a redacted topology template, resolve its explicit variables, inspect the plan, and apply it
idempotently:

```bash
switchboard --json topology export --owner my-switchboard --output topology.json
switchboard --json topology plan topology.json \
  --var WORKER=chat:claude:my-session --var PATH_1=/absolute/path/to/script.py
switchboard --json topology apply topology.json \
  --var WORKER=chat:claude:my-session --var PATH_1=/absolute/path/to/script.py
```

For a private machine-local snapshot that can be reapplied directly, add
`--include-local-values`. Switchboard writes topology output with mode `0600`; never commit that
form to a public repository.

An owner may update only resources it manages. Existing unmanaged resources cause an explicit
conflict. Missing managed resources are retained unless `--prune` is given; pruning disables
resources with operational history and retains spaces rather than destroying evidence. See the
[operations guide](plugins/switchboard/docs/operations.md) and
[generic example](examples/topology.json).

Diagnose the installed runtime and create a verified online backup without stopping the service:

```bash
switchboard --json doctor
switchboard --json backup create /absolute/path/to/switchboard-2026-09-22.sqlite3
switchboard --json backup verify /absolute/path/to/switchboard-2026-09-22.sqlite3
```

`doctor` exits non-zero when it finds a condition that prevents safe operation. Before upgrading a
live service, create a backup and verify its integrity. Restoration remains an explicit operator
procedure; never replace the live database while the supervisor runs.

## Adapters and delivery

Observe an existing ingest store without changing or synchronizing it:

```bash
switchboard --json adapter ingest-shadow --status-script /absolute/path/to/ingest/status.py
```

The adapter calls only `status.py --all --json`, auto-registers the observed ingest sources, and
stores immutable status observations. It refuses stale or failed status reads.

Deliver a matched wait through the existing local chats broker:

```bash
switchboard --json delivery dispatch <delivery-id> \
  --relay /absolute/path/to/chats/send-message.py
```

`--activate-inactive` (on a dispatch, service or processor binding) lets a Claude wake open an
inactive session; without it that wake fails closed. Codex wakes always activate: the relay loads
the exact thread so the turn starts now. A message that only reached a Codex thread's native queue
is not a delivery and stays pending for a retry with the same request ID; a thread already loaded
and mid-turn (`loaded-busy`) counts as accepted.

Broker acceptance changes the delivery to `accepted`, not `acknowledged`. The destination task
receives exact CLI commands for inspecting and acknowledging the delivery after completing its
work. See [the adapter contract](plugins/switchboard/docs/adapters.md) for third-party adapters.

## Routing and processor outcomes

Routes are evaluated in ascending priority order and the first match wins. A match creates one
idempotent processor run; it does not execute a model or mutate the source system.

```bash
switchboard --json route create --space nina-inbound --name "New inbound lead" \
  --priority 10 --source inbound/nina --event-type inbound.lead.pending \
  --processor inbound-leads:nina

switchboard --json processor bind --space nina-inbound \
  --processor inbound-leads:nina --consumer chat:claude:<claude-cli-session-id> \
  --activate-inactive --label "Nina / inbound leads" --url '<verified-local-task-url>'
switchboard --json processor claim-next \
  --worker chat:claude:<claude-cli-session-id>
switchboard --json processor complete <processor-run-id> \
  --worker chat:claude:<claude-cli-session-id> \
  --summary "Qualified and handed to the sales owner." \
  --facts '{"identity_checked":true}' \
  --decision '{"verdict":"question"}' \
  --actions '[{"kind":"crm","state":"created"}]'
```

The binding backfills existing pending runs and automatically creates a delivery for each new run.
Switchboard wakes a consumer only when it has no in-flight work, while the remaining backlog stays
durable in the database. `processor claim-next` atomically selects the oldest pending run, accepts
its delivery, and creates the worker's lease. A consumer can hold only one live run;
`processor heartbeat` renews it and
`processor release` safely requeues it. The read-only dashboard leads with decision groups,
failures, worker availability, backlog age, queue lanes, source health, and per-run lifecycles. Raw
topology remains available in a collapsed technical inventory. CLI commands remain the only
mutation interface.

Upgrades from a pre-0.8 database automatically coalesce multiple already-accepted wakes for the
same consumer into one in-flight wake plus a pending backlog. Operators can also run
`processor delivery-coalesce [--consumer <consumer>]` explicitly; it never drops a processor run.

Human review is an explicit, durable transition rather than an orphaned terminal state. Give
related runs the same stable key so one decision can cover the whole group:

```bash
switchboard --json processor needs-review <processor-run-id> \
  --summary "Choose the destination" --review-key task_shared-destination
switchboard --json processor review-list --state open
switchboard --json processor review-resolve-group <review-group-id> \
  --resolution retry --decision '{"choice":"approved destination"}'
```

The `review-link` command can regroup an existing `needs-review` run and attach a verified local
task URL. The pre-0.10 per-run `review-resolve` command remains supported. Use `--resolution
complete` when the decision itself closes the work. `processor list --space <space-id>` and the
dashboard's space links keep independent queues separate.

Observe the inbound-leads ledger without duplicating message content:

```bash
switchboard --json adapter inbound-leads \
  --ledger-script /absolute/path/to/ledger.py --profile nina --space nina-inbound
```

Add `--discovery-script /absolute/path/to/watch_inbound_gmail.sh` to perform one bounded canonical
Gmail discovery poll before reading the ledger. The supervisor forces `MAX_POLLS=1`; it never
triages or publishes a lead.

Add `--slack-discovery-script /absolute/path/to/watch_inbound_slack.sh` to perform one bounded
mention poll in the same schedule. Switchboard stores only stable message/thread pointers and
coarse state; message text remains in Slack.

For production, configure Gmail and Slack as separate schedules with `--source-mode gmail` and
`--source-mode slack`. They execute independently, so a slow Gmail call does not delay Slack,
ingest, chat deliveries, alerts, or the supervisor heartbeat.

When migrating an existing Slack watcher, create the schedule before its route or binding and let
one poll complete. Inspect or close any historical pointers imported from the old cursor, then
enable routing. This establishes the live baseline without delivering an old mention backlog to a
processor chat.

## Persistent supervisor

Create a recurring ingest observation schedule, then run one cycle:

```bash
switchboard --json schedule add-ingest-shadow ingest \
  --status-script /absolute/path/to/ingest/status.py \
  --discovery-script /absolute/path/to/ingest/watch.py --every 300 --timeout 900
switchboard --json supervisor once --relay /absolute/path/to/chats/send-message.py
```

The long-running supervisor executes due schedules independently, retries failed deliveries with
bounded cadence, terminates a timed-out command's complete descendant process group, limits chat
delivery work per cycle, records its heartbeat and errors, and serves the web UI:

```bash
switchboard supervisor run --relay /absolute/path/to/chats/send-message.py --delivery-batch 1
```

Create a recurring deterministic maintenance wake without embedding policy in Switchboard:

```bash
switchboard --json schedule add-timer nina-daily \
  --space nina-inbound --source timer/nina-daily \
  --event-type inbound.maintenance.due --every 86400 \
  --first-run-at 2026-09-21T23:50:00+02:00
```

On macOS, install it as a persistent per-user launch agent entirely through the CLI:

```bash
switchboard --json service install --relay /absolute/path/to/chats/send-message.py \
  --activate-inactive \
  --accepted-retry 120 \
  --alert-command-json '["/absolute/path/to/local-alert-adapter"]' \
  --alert-after 900
switchboard --json service status
```

The alert command receives one JSON payload on standard input. An outage opens one durable alert
episode per consumer, absorbs any additional affected deliveries, and emits one recovery after the
final issue clears. Claims are persisted before invoking the command, so restarts or command
failures cannot create notification storms. This keeps phone numbers, messenger
credentials, and machine-specific policy outside the public plugin. The default web address is
<http://127.0.0.1:8765>. Re-run `service install` after updating the
plugin so launchd points at the newly installed version. See
[the supervisor guide](plugins/switchboard/docs/supervisor.md).

## Plugin marketplace

The public repository is a dual-host marketplace containing one plugin under
`plugins/switchboard`. Install it in Codex with:

```bash
codex plugin marketplace add cielecki/switchboard
codex plugin add switchboard@switchboard
```

For Claude Code:

```bash
claude plugin marketplace add cielecki/switchboard
claude plugin install switchboard@switchboard
```

Both hosts discover the `switchboard` skill inside the plugin. The skill operates Switchboard
exclusively through the CLI; marketplace updates never own or replace the external database.

## Principles

- CLI-managed, web-observed.
- Events are evidence; routes, processing, and delivery are separate facts.
- Deterministic matching comes before model classification.
- External IDs make ingestion idempotent.
- Private source data and personal routing configuration stay outside the open-source repository.

## License

MIT
