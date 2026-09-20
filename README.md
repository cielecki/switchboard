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
- a CLI-installed macOS launch agent that runs the supervisor and read-only web UI.

Routing tables, processor outcomes, and additional source adapters are the next milestones.
Existing capture and lead-processing systems can integrate as adapters before any migration or
replacement.

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

Broker acceptance changes the delivery to `accepted`, not `acknowledged`. The destination task
receives exact CLI commands for inspecting and acknowledging the delivery after completing its
work. See [the adapter contract](plugins/switchboard/docs/adapters.md) for third-party adapters.

## Persistent supervisor

Create a recurring ingest observation schedule, then run one cycle:

```bash
switchboard --json schedule add-ingest-shadow ingest \
  --status-script /absolute/path/to/ingest/status.py --every 60
switchboard --json supervisor once --relay /absolute/path/to/chats/send-message.py
```

The long-running supervisor executes due schedules, retries failed deliveries with bounded cadence,
records its heartbeat and errors, and serves the web UI:

```bash
switchboard supervisor run --relay /absolute/path/to/chats/send-message.py
```

On macOS, install it as a persistent per-user launch agent entirely through the CLI:

```bash
switchboard --json service install --relay /absolute/path/to/chats/send-message.py
switchboard --json service status
```

The default web address is <http://127.0.0.1:8765>. Re-run `service install` after updating the
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
