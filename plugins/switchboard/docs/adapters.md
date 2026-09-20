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

## Ingest shadow adapter

The built-in ingest adapter invokes the owning ingest system's supported
`status.py --all --json` command. It never reads `_index.json`, walks the private store, calls sync,
claims records, or changes routing status.

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
bounded poll with `MAX_POLLS=1`, then reads the ledger. That poll may discover pointers in the
owning ledger; it does not triage, create CRM records, send mail, or post to Slack. Existing source
locks remain authoritative, and operators must preserve one publishing owner per mailbox.
