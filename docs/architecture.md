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
- **Processor outcome:** structured record of what a specialized workflow changed.

## Initial predicate language

Wait predicates are conjunctions over allowlisted facts:

- source ID;
- event type;
- exact normalized attributes;
- case-insensitive substring checks on normalized string attributes.

No predicate may execute code, SQL, shell commands, or a model prompt. A model may enrich an event
or recommend a route in a later processor stage, but it does not silently alter deterministic
routing policy.

## Migration direction

Existing watcher-based systems first publish into Switchboard in shadow mode. Once event counts,
deduplication, health, and delivery evidence agree with their existing stores, Switchboard can take
over supervision. Source-specific business policy remains in processor adapters rather than being
folded into the generic core.
