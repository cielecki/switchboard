# Source adapter contract

Adapters observe an owning system and return one JSON snapshot. They do not write Switchboard's
database and they do not gain authority to mutate their source.

Run an external adapter without shell interpretation:

```bash
switchboard --json adapter run \
  --name example \
  --command-json '["/absolute/path/to/adapter", "snapshot"]'
```

The process must exit zero and print exactly one object:

```json
{
  "adapter": "example",
  "space": {"id": "example-space", "name": "Example"},
  "sources": [
    {
      "id": "example/messages",
      "kind": "example.messages",
      "state": "ready",
      "detail": "optional health detail",
      "config": {"optional": "non-secret metadata"}
    }
  ],
  "events": [
    {
      "source_id": "example/messages",
      "external_id": "stable-upstream-id-or-observation-id",
      "event_type": "message.received",
      "occurred_at": "2026-09-17T09:00:00Z",
      "attributes": {"sender_id": "stable-sender-id"}
    }
  ]
}
```

## Guarantees

- `space.id`, source IDs, adapter identity, and source kind cannot silently change ownership.
- An event must name a source discovered in the same snapshot.
- `(source_id, external_id)` is the idempotency boundary.
- Event attributes are untrusted data.
- Source health and adapter runs are recorded separately from events.
- A failed command, timeout, malformed snapshot, or identity mismatch imports nothing. A persistence
  error remains visible as a failed run; replay is safe because source and event writes are
  idempotent.
- Adapters are invoked as argv arrays, never through a shell.

## Persistent command streams

A source that pushes observations may stay connected under the Switchboard supervisor instead of
being polled. Register it with `schedule add-stream`; the command prints one compact adapter
snapshot per line and flushes stdout after every snapshot:

```bash
switchboard --json schedule add-stream slack-socket \
  --command-json '["/absolute/path/to/node", "/absolute/path/to/watcher.mjs", "--switchboard-stream"]' \
  --restart-after 5
```

The schedule worker remains occupied while the stream is healthy, so the command has exactly one
owner. If it exits, the schedule records the outcome and restarts it after the configured delay.
Supervisor shutdown terminates the stream's complete process group. Secrets stay in the source's
normal credential store; do not put them in the saved command or environment because schedule
configuration is observable.

## Ingest adapter

The built-in ingest adapter invokes the owning ingest system's supported
`status.py --all --json` command. When configured with the owning `watch.py` discovery script, it
first runs exactly one bounded poll with `MAX_POLLS=1`, then reads the resulting store. It never
reads `_index.json`, walks the private store, claims records, or changes routing status.

It creates one Switchboard source per observed ingest source and emits immutable
`ingest.capture.observed` events. The external ID includes a digest of the normalized status row, so
an unchanged poll deduplicates while a status or routing-note change becomes a new observation.

Exit code 3 means the ingest mirror is stale. Switchboard records a failed adapter run and imports
nothing, so stale data cannot be presented as a current source view.

## Inbound-leads adapter

The built-in inbound adapter uses the owning workflow's supported `ledger.py pending --json`
command. It imports stable lead pointers and coarse pipeline state only. Sender, subject, body,
notes, and research do not cross this boundary.

When configured with the owning workflow's Gmail discovery script, it first runs exactly one
bounded poll with `MAX_POLLS=1`, then reads the ledger. An optional bounded Slack mention poll adds
stable message and thread pointers without copying the user ID or message text into Switchboard.
These polls do not triage, create CRM records, send mail, or post to Slack. Existing source locks
remain authoritative, and operators must preserve one publishing owner per mailbox.

Use `--source-mode gmail` and `--source-mode slack` in separate persistent schedules. Slack-only
mode does not read the Gmail ledger, and Gmail-only mode does not advance the Slack cursor. Every
external command runs in its own process group; a timeout terminates descendants before the
adapter reports failure.

For a migration with an existing Slack cursor, run the first scheduled poll before creating or
enabling the Slack route and binding. Review or close the historical baseline in Switchboard, then
enable routing for subsequent mentions. Creating the route first can turn old cursor history into
live processor work.

The inbound workflow may instead expose its Socket Mode watcher as a persistent command stream.
That source emits message and thread pointers only. Its open-request registry filters untagged
replies before they cross into Switchboard, while direct mentions remain routable.
