# Plan: Fair and non-blocking delivery dispatch

- **Date:** 2026-10-12
- **Slug:** delivery-fairness
- **Issue:** 5
- **Branch:** main
- **Base:** main
- **Mode:** loop

## Tasks

> Step ledger. The first row whose **Status** is not `done` is the resume point; current state,
> blockers and the next action are comments on the issue.
> Step ids are immutable once a step has a commit. One step = exactly one code commit.

| Phase | Step | Title | Status | Commit |
|-------|------|-------|--------|--------|
| 1 | 1.1 | Fair candidate selection across consumers | done | 7a3a730 |
| 2 | 2.1 | Bounded non-blocking delivery workers | done | 967615d |
| 2 | 2.2 | Make delivery-worker shutdown quiescent | done | 706cfae |
| 3 | 3.1 | Release metadata and operator documentation for 0.11.7 | done | cbf9906 |

## Goal

Ready deliveries for one consumer no longer monopolize the shared wait/processor dispatch budget,
and a slow relay no longer stops supervisor polling, heartbeats, or dispatch to unrelated
consumers. Stable request IDs, acceptance versus acknowledgement, retry timing, and per-consumer
serialization remain intact. The verified fix ships as Switchboard 0.11.7 and is deployed without
restarting the Interactive Mode server.

## Scope

- Deterministic regressions for mixed retry contention, slow relays, processor oldest-first order,
  stable request IDs, and per-consumer serialization.
- Attempt-derived least-recently-served selection with one eligible candidate per consumer across
  wait and processor deliveries.
- A bounded persistent delivery-worker pool for the long-running supervisor, with an additive
  `--delivery-concurrency` setting and synchronous `supervisor once` behavior preserved.
- Process-group-bounded relay execution, CLI/service plumbing, operator documentation, and a
  patch release.
- Controlled deployment and live verification through the Switchboard CLI while keeping the
  Interactive Mode server and its volatile response queue running.

## Non-goals

- Changes to event, processor-run, delivery, request-ID, acknowledgement, or retry schemas.
- Persisted round-robin scheduler state or a database migration.
- Restarting or changing the Interactive Mode server, automatically starting the iPhone
  microphone, or implementing Interactive Mode issue #142.
- Clearing unrelated live deliveries, processor review groups, or domain backlogs.

## Risks

- Concurrent relays could overlap for one consumer; active consumer exclusion and existing core
  reservations must prevent that.
- Completion-order results differ from start order in long-running mode; durable delivery state,
  not result-list order, remains authoritative.
- SQLite write contention could surface under concurrency; workers must use short existing
  transactions and bounded parallelism.
- A Switchboard service reinstall reconnects its stream subprocess. The external Interactive Mode
  server must remain running, and source freshness must be verified before the physical test.

## External references

- issue: https://github.com/cielecki/switchboard/issues/5

## Implementation plan

### Phase 1 — Fair scheduling

- **1.1** Add failing mixed-queue regressions, select at most one due delivery per consumer, rank
  never-attempted consumers before retries and then by least-recent attempt, and correct processor
  selection to oldest-first. Reuse durable delivery-attempt evidence and avoid a schema change.

### Phase 2 — Relay isolation

- **2.1** Add a bounded persistent delivery-worker pool to `run_forever`, keep `run_once`
  synchronous, prevent duplicate active delivery/consumer launches, terminate timed-out relay
  process groups, and plumb/document `--delivery-concurrency` through CLI and service install.
- **2.2** Prevent a worker blocked before process spawn from launching a relay after `shutdown()`
  returns; add a deterministic pre-spawn lock regression and require quiescent shutdown.

### Phase 3 — Patch release

- **3.1** Bump all public and lockfile metadata to 0.11.7, update changelog/operator guidance, and
  retain compatibility coverage for existing schedule, delivery, and plugin contracts.
