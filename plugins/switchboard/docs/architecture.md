# Architecture

## Boundaries

Switchboard is not an agent workspace, task board, or visual agent canvas. It coordinates external
events with consumers that have declared what they are waiting for.

```text
source adapters -> normalized event ledger -> routes and waits -> deliveries/processors
                                               |
                                               +-> read-only web observability
```

The server owns persistence and matching. The CLI owns operator and agent mutations. The web UI
owns no mutations. Delivery adapters wake agent hosts without taking ownership of their sessions.

## Core records

- **Space:** isolation boundary for sources, policies, events, and consumers.
- **Source:** registered producer with a stable identity inside one space.
- **Event:** immutable normalized observation, deduplicated by source and external ID.
- **Wait:** durable predicate registered by a consumer.
- **Match:** evidence that a particular event satisfied a particular wait revision.
- **Delivery:** work owed to a consumer, distinct from transport acceptance or acknowledgement.
- **Route:** ordered deterministic policy directing an event to a processor or destination.
- **Processor run:** idempotent work record created by the first matching route, with structured
  facts, decision, actions, summary, and error state recorded by the specialized workflow.
- **Processor binding:** CLI-managed association between a space/processor pair and one durable
  agent-chat consumer, including its default lease.
- **Processor delivery:** durable wake-up owed to that consumer. At most one wake is in flight per
  consumer; the remaining runs stay queued in the database. Transport acceptance is distinct from
  finishing the run, while `claim-next` atomically accepts the selected delivery and leases its run.
- **Processor attempt:** atomic worker claim with an expiring lease, heartbeat, and terminal state.
- **Processor alert:** deduplicated evidence that a delivery was unreachable or accepted but not
  claimed, followed by a recorded recovery.
- **Schedule:** CLI-managed cadence and private adapter configuration stored outside the plugin.
- **Supervisor:** single-instance loop that runs schedules, dispatches deliveries, records a
  heartbeat, and hosts read-only observability.

The [source adapter contract](adapters.md) is the only ingestion boundary. Adapters produce
snapshots; the core validates, records health, deduplicates events, and evaluates waits.

## Initial predicate language

Wait predicates are conjunctions over allowlisted facts:

- source ID;
- event type;
- exact normalized attributes;
- case-insensitive substring checks on normalized string attributes.

No predicate may execute code, SQL, shell commands, or a model prompt. A model may enrich an event
or recommend a route in a later processor stage, but it does not silently alter deterministic
routing policy.

Enabled routes are evaluated by ascending numeric priority and then stable route ID. The first
match wins. Routing creates a pending processor run and, when bound, a processor delivery. The
chat relay wakes the external host but does not execute or complete the run. The destination must
claim the lease and record its own structured outcome.

## Migration direction

Existing watcher-based systems first publish into Switchboard in shadow mode. Once event counts,
deduplication, health, and delivery evidence agree with their existing stores, Switchboard can take
over supervision. Source-specific business policy remains in processor adapters rather than being
folded into the generic core.

Delivery follows the same split. A broker accepting a stable request changes a delivery from
`pending` to `accepted`; only the destination consumer can change it to `acknowledged` after the
matched work is complete.

The supervisor never converts transport acceptance into task completion. It isolates adapter and
delivery failures, advances schedules deterministically, and exposes its last heartbeat and error
without making the web interface a control surface.
