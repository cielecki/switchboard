# Plan: Persist bounded schedule retry backoff

- **Date:** 2026-10-01
- **Slug:** schedule-retry-backoff
- **Issue:** 3
- **Branch:** main
- **Base:** main
- **Mode:** loop
- **Integration:** commit-to-main

## Tasks

> Authoritative state machine. The first row whose **Status** is not `done` is the resume point.
> Step ids are immutable once a step has a commit. One step = exactly one code commit.

| Phase | Step | Title | Status | Commit |
|-------|------|-------|--------|--------|
| 1 | 1.1 | Persist schedule retry state and deterministic due selection | done | 2dbaede |
| 1 | 1.2 | Make scheduled attempt finalization and alert episodes atomic | done | a630266 |
| 2 | 2.1 | Expose consistent retry state in CLI, doctor, web, and topology behavior | done | 2040609 |
| 2 | 2.2 | Prove retry, restart, recovery, and occurrence-once invariants | done | fd493fc |
| 3 | 3.1 | Document bounded retry and prepare release 0.11.6 | todo | — |

## Goal

Prevent a failed non-stream interval or calendar schedule from running on every supervisor poll.
Persist a deterministic, bounded retry deadline while leaving the logical occurrence cursor intact;
recover exactly once after restart or downtime; represent one consecutive failure streak as one
internal alert episode; and expose the same state through the CLI, doctor, and read-only dashboard.
Ship the behavior as 0.11.6, update both local plugin hosts, reinstall the persistent service with
its current settings, and verify failure, backoff, restart, and recovery with a controlled local
adapter that cannot post to Slack or a chat.

## Scope

- Add persisted retry state for every enabled non-stream interval or calendar schedule: failure
  streak, most recent failure time/detail, and `retry_not_before`, plus durable history for one
  internal alert episode per consecutive streak.
- Select a logically due schedule only when it has no retry deadline or the deadline has elapsed.
  Keep `next_run_at` unchanged after failure.
- Use fixed deterministic delays of 30 seconds, 1 minute, 2 minutes, 4 minutes, 8 minutes, and a
  15-minute cap for all later failures; do not add schedule-specific tuning.
- Clear stale retry state after a successful scheduled attempt, an actual material schedule change,
  or re-enable. Preserve it across an identical/idempotent upsert and while disabled; re-enable
  re-anchors according to the existing schedule-kind rules and does not backfill disabled time.
- Persist each terminal failed adapter run and its updated schedule retry state together. Persist a
  successful attempt and reset together; timer/calendar event persistence, route application,
  schedule advancement, and reset must retain their occurrence transaction.
- Open one durable internal alert episode for the first failure in a consecutive streak, update that
  episode during later failures, and record one recovery transition when the streak clears. This
  evidence is local observability only and never invokes the notification adapter.
- Show consistent retry/episode facts in `schedule list` output, doctor findings, and the read-only
  dashboard. Keep runtime retry state out of declarative desired topology while making topology
  material updates and re-enable follow the same reset contract as imperative CLI mutations.
- Cover interval pull adapters and timer/calendar schedules while explicitly excluding
  `command-stream` schedules from the new policy.
- Add regression coverage for backoff timing, cap behavior, atomicity, reopen/restart persistence,
  isolation between schedules, success/reset, re-enable/material-update reset, and one recovered
  logical occurrence/run/delivery with no duplication.
- Update public documentation, roadmap status, and all release metadata from 0.11.5 to 0.11.6.

## Non-goals

- No generic command schedule, ingest replacement, or other 0.12 producer architecture.
- No per-schedule retry configuration, randomized jitter, manual web controls, or mutable dashboard.
- No change to processor-delivery retry, accepted-wake rearming, consumer serialization, processor
  leases, stream restart semantics, or downstream side-effect guarantees.
- No Slack/chat notification for schedule failures or recovery. Existing processor-unreachable
  notification behavior remains separate and unchanged.
- No promise of exactly-once external side effects. The invariant is one durable logical event,
  processor run, and delivery for a recovered timer/calendar occurrence.
- No private topology, local command path, chat identifier, captured event, or production database
  content in Git.

## Risks

- Adapter helpers currently terminalize their own runs in separate transactions. Scheduled use must
  move terminal run ownership to a schedule-aware domain finalizer without weakening standalone
  adapter commands or leaving `running` evidence after exceptions.
- Interval schedules currently advance cadence even on failure. The new path must preserve their
  original logical `next_run_at` until success while keeping existing anchored timer cadence and
  pull-adapter cadence after recovery.
- Calendar occurrence execution already groups event insertion, deterministic routing, run evidence,
  and cursor advancement. Adding retry reset or alert recovery outside that transaction would create
  a crash window and violate the existing exactly-once boundary.
- A schema migration must preserve old schedules and historical adapter/processor alert records,
  permit read-only commands to avoid write locks once current, and support databases reopened by
  both Python 3.12 and 3.13.
- An open retry must not make `doctor` also misclassify the unchanged logical cursor as an ordinary
  overdue schedule, and one backed-off schedule must not delay unrelated due schedules.
- Live verification deliberately introduces a temporary failing local schedule. It must use an
  isolated private adapter with no matching route, verify every mutation through the CLI, and be
  removed after recovery without touching production inbox schedules or their workers.

## External references

- issue: https://github.com/cielecki/switchboard/issues/3
- tracking plan: `.ai/runs/2026-10-01-schedule-retry-backoff/PLAN.md`

## Implementation plan

### Phase 1 — Durable retry state and execution atomicity

- **1.1 Persist schedule retry state and deterministic due selection**
  - Advance the database schema and migrate existing `adapter_schedules` without changing their
    logical cursors. Add explicit, queryable retry fields and a dedicated internal schedule-alert
    episode store with safe defaults and indexes that support due/episode selection without making
    read-only commands take a write lock on a current DB.
  - Add a single domain helper for the fixed delay sequence
    `30, 60, 120, 240, 480, 900, 900, ...` and make `due_schedules()` require both logical due time
    and an elapsed/missing retry deadline. Exclude `command-stream` from this failure policy.
  - Centralize reset semantics used by every schedule upsert and `set_schedule_enabled()`: clear and
    record recovery of an open episode on an actual material adapter/config/kind/cadence change or
    disabled-to-enabled transition, but not on an identical upsert. Preserve existing calendar
    revision/re-anchor behavior and avoid disabled period backfill.
  - Add focused migration/core/topology tests for defaults, delay calculation and cap, pre-deadline
    exclusion, due-at-deadline inclusion, persisted reopen behavior, idempotent upsert preservation,
    and material-update/re-enable reset.
  - Verification: configured per-step unit suite and both plugin validators.

- **1.2 Make scheduled attempt finalization and alert episodes atomic**
  - Introduce schedule-aware terminal domain operations so one transaction records a failed adapter
    run, increments the streak, computes `retry_not_before`, updates last-failure facts, and opens or
    refreshes the schedule's single alert episode. Repeated failures in one streak must never create
    another episode.
  - Refactor scheduled non-stream adapters so the supervisor owns terminal run finalization. Preserve
    standalone adapter command behavior and one adapter-run record per attempt; avoid double-finish
    and orphaned-running rows for validation, process, timeout, and snapshot failures.
  - On success, atomically clear retry state and recover the open episode exactly once. Keep pull
    adapter terminalization/reset together; extend interval timer and calendar occurrence paths so
    event insertion, first-route processing, adapter-run evidence, cursor advancement, reset, and
    recovery remain a single transaction.
  - Keep `ScheduleWorkers` and one-shot cycles independent per schedule: a failure or active backoff
    is a reported schedule result, not a reason to stop another due schedule. Do not pass schedule
    episodes to `alert_command` or any delivery/chat path.
  - Add focused domain/supervisor/adapter tests for atomic rollback, one run per attempt, one open
    episode per streak, one recovery transition, no external alert invocation, and unaffected stream
    restart behavior.
  - Verification: configured per-step unit suite and both plugin validators.

### Phase 2 — Operator visibility and end-to-end invariants

- **2.1 Expose consistent retry state in CLI, doctor, web, and topology behavior**
  - Extend presented schedule data so JSON and the existing human-readable `schedule list` form
    expose failure streak, last failure, next retry, and episode state without leaking private
    configuration.
  - Add stable doctor findings for an enabled schedule currently backing off and for inconsistent
    retry/episode state. Suppress a redundant generic overdue warning while a future retry deadline
    intentionally blocks the unchanged logical cursor; doctor remains diagnostic and read-only.
  - Add schedule retry/episode columns and a visible needs-attention card to the read-only web UI.
    The API continues to reuse the domain schedule projection and gains no POST/control surface.
  - Keep retry and episode fields out of topology export/checksums as runtime state. Add topology
    tests proving identical plan/apply preserves active backoff, while a material managed update or
    re-enable clears it through the shared core mutation path.
  - Add CLI, doctor, API, HTML, and topology regression tests showing consistent values on every
    surface.
  - Verification: configured per-step unit suite and both plugin validators.

- **2.2 Prove retry, restart, recovery, and occurrence-once invariants**
  - Build deterministic-clock integration fixtures for a failing pull schedule and a routed
    timer/calendar occurrence. Assert polls before `retry_not_before` create no run; each due retry
    creates exactly one; consecutive failures advance through every fixed delay to the 15-minute
    cap; and another ready schedule still completes in the same cycle.
  - Reopen the database and reconstruct `ScheduleWorkers`/supervisor state between attempts. Verify
    the same streak/deadline survives and no restart creates an early or duplicate adapter run.
  - Recover at the deadline and assert the backoff clears, the one alert episode records one recovery,
    later polls/restart remain quiet, and a later independent failure begins a new episode.
  - With one enabled matching route and processor binding, prove a recovered normal and
    `catch-up-once` timer/calendar occurrence uses the stable
    `schedule:<id>:r<revision>:<scheduled-for>` identity and creates exactly one event, processor
    run, and processor delivery. Claim/complete it, reopen/restart, and prove there is no second
    claim or durable duplicate.
  - Add crash/transaction tests that reject partial states such as a failed run without its retry
    deadline, a cursor advance without its event/reset, or a recovered episode while retry state
    remains active.
  - Verification: configured per-step unit suite and both plugin validators.

### Phase 3 — Operator contract and release metadata

- **3.1 Document bounded retry and prepare release 0.11.6**
  - Document the fixed backoff sequence, unchanged logical cursor, success/update/re-enable reset,
    restart persistence, internal episode lifecycle, CLI/doctor/dashboard fields, and the explicit
    boundary from processor-delivery and stream retries in the README and plugin operations,
    architecture, supervisor, and adapter guides where relevant.
  - Update the 0.11 roadmap from its stale pre-deployment status to the actual released calendar
    worker state and record bounded schedule retry as the 0.11.6 hardening boundary; leave generic
    scheduled pull adapters and ingest replacement for 0.12.
  - Bump `pyproject.toml`, `uv.lock`, package `__version__`, Codex/Claude plugin manifests, and both
    marketplace catalogs from 0.11.5 to 0.11.6, keeping every release-bearing version aligned.
  - Verification: configured per-step unit suite and both plugin validators.

## Checkpoints and final gate

- Run a checkpoint at the end of each phase (and at the end of the run), rewrite `HANDOFF.md`, append
  `NOTIFY.md`, and commit checkpoint evidence separately from implementation commits.
- After all task rows are done, run the configured isolated Python 3.12 and 3.13 suites, wheel
  build/install acceptance test, and both plugin validators. Review the full issue diff for scope,
  private data, debug residue, schema compatibility, and web read-only drift before release.
- This repo integrates directly to `main`; no worktree or PR is created. Maciej already approved
  issue #3 and its 0.11.6 live-deployment scope. Any material expansion beyond this plan requires a
  new decision.

## Post-code release, install, and controlled live verification

These are operational actions after the final code gate; they are not implementation commits and
therefore do not appear as rows in the Tasks state machine.

1. Verify `main` is clean, equals `origin/main`, has no held commit in its push range, and passes the
   final gate. Publish tag/GitHub release `v0.11.6`, close issue #3 with the shipped SHA, and release
   its dev-ship lock.
2. Update both installed plugin hosts and verify their CLIs report 0.11.6. Read the installed service
   arguments, reinstall it through the 0.11.6 CLI with the same database, relay, activation, web,
   delivery-retry, batch, and alert settings, then verify the resulting service state and launcher.
3. Through the installed CLI, create and verify an online backup and capture pre-test schedule,
   adapter-run, doctor, service, and alert-episode facts in a private operation directory outside
   Git. Do not edit the database or private topology directly.
4. Create a temporary isolated pull schedule through the CLI whose private local adapter can be
   switched deterministically between failure and success. Give it no route or processor binding,
   so neither failure nor recovery can reach Slack or a chat. Record its exact schedule/run/episode
   identifiers before each transition.
5. Trigger one controlled failure and verify exactly one failed adapter run, streak 1, an unchanged
   logical `next_run_at`, a 30-second retry deadline, and one open internal episode. Observe several
   supervisor polls before the deadline and require zero additional runs. Restart the installed
   service during backoff and require the same persisted deadline/streak with no early attempt.
6. At a controlled retry deadline, keep the adapter failing long enough to verify one additional
   run, streak increment, and the next deterministic deadline without a second episode. Then switch
   the same adapter to valid output and allow the next eligible retry: require exactly one completed
   run, cleared retry state, and one recorded recovery transition. Subsequent polls and a restart
   before the schedule's next logical due time must add no run or episode transition.
7. Delete the temporary schedule through the CLI, remove only its private test files, and read back
   the resulting inventories. Finish with a loaded service on the 0.11.6 launcher, the production
   inbox schedules unchanged and healthy, empty/unaffected delivery queues, and a doctor result with
   no unexpected findings. Preserve the verified backup and concise evidence outside Git.
