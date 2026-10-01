# Plan: Repair calendar schedule sources

- **Date:** 2026-10-01
- **Slug:** calendar-source-repair
- **Issue:** 2
- **Branch:** main
- **Base:** main
- **Mode:** loop
- **Integration:** commit-to-main

## Tasks

> Authoritative state machine. The first row whose **Status** is not `done` is the resume point.
> Step ids are immutable once a step has a commit. One step = exactly one code commit.

| Phase | Step | Title | Status | Commit |
|-------|------|-------|--------|--------|
| 1 | 1.1 | Enforce the timer-source contract at schedule upsert and in doctor | done | 0fb11b9 |
| 1 | 1.2 | Add the audited legacy calendar-source repair command | done | 53a8c70 |
| 2 | 2.1 | Enforce the same timer-source contract in declarative topology | done | e349c06 |
| 2 | 2.2 | Prove due and catch-up occurrences are durable and processed once | todo | — |
| 3 | 3.1 | Document the repair workflow and prepare release 0.11.5 | todo | — |

## Goal

Make an incompatible source impossible to attach to a new or updated timer-backed calendar
schedule, diagnose already-deployed mismatches before they become retry storms, and provide a
strict, auditable CLI repair for the known legacy shape. Release the fix as 0.11.5, install it,
repair the two affected live schedules through the CLI, and prove that each eligible normal or
catch-up occurrence creates one durable event and one processor run without duplication after a
second cycle or restart.

## Scope

- Treat source kind `timer` as the canonical contract for interval and calendar schedules whose
  adapter is `timer`.
- Provision a missing timer source and validate an existing source's space and kind in the same
  transaction as schedule creation or update; incompatible input must leave no partial schedule or
  source mutation.
- Report a stable doctor error for enabled or disabled timer-backed schedules whose configured
  source is missing, in the wrong space, or not kind `timer`.
- Add `schedule repair-calendar-source <schedule-id>` with strict schedule/source/shared-source
  preconditions, an audit record, idempotent safe behavior, and preservation of unrelated source
  fields and all schedule/event/routing history.
- Reject topology documents in which a timer schedule points to a source in another space or to a
  source whose kind is not `timer`; keep CLI upsert and topology apply on the same domain path.
- Add integration coverage for normal due execution and `catch-up-once`, including stable external
  IDs, a single matched route/run/delivery, database reopen, repeated cycle, service-style restart,
  claim/complete, and the absence of a second claim.
- Update public operational documentation and all release metadata from 0.11.4 to 0.11.5.
- After the code gate, publish and install 0.11.5, update the private owning topology through CLI
  export/plan, repair only the two known live sources through the CLI, and verify the live catch-up.

## Non-goals

- No permanent compatibility alias that treats source kind `calendar` as `timer`.
- No broad schema migration or automatic rewrite of every source named or shaped like a calendar
  source; provenance supports an explicit bounded repair instead.
- No change to calendar recurrence, DST, missed-occurrence, route matching, delivery retry, or
  processor external-side-effect semantics beyond defects exposed by the new regression tests.
- No promise of exactly-once external side effects: the invariant is one durable logical event,
  processor run, and delivery per schedule occurrence; a run may still have multiple attempts.
- No private topology, machine path, chat identifier, captured message, or live database in Git.
- No command surface in the read-only web dashboard.

## Risks

- A shared legacy source may also serve a non-timer producer. The repair must refuse ambiguity
  rather than silently changing its meaning.
- Source provisioning and schedule upsert currently happen at different times. Moving the contract
  earlier must remain one SQLite transaction and must not reset an unchanged calendar revision or
  `next_run_at`.
- Topology plan/apply must reject invalid documents without partially adopting or updating managed
  resources.
- Live repair will make overdue schedules eligible immediately. Backup, pre/post inventories,
  serial verification, and a duplicate-free second cycle/restart are required before declaring it
  healthy.
- The two inbox processors perform real work. Live verification measures Switchboard's durable
  event/run/delivery facts, not exactly-once behavior in downstream systems.

## External references

- issue: https://github.com/cielecki/switchboard/issues/2
- tracking plan: `.ai/runs/2026-10-01-calendar-source-repair/PLAN.md`

## Implementation plan

### Phase 1 — Domain contract, diagnostics, and bounded repair

- **1.1 Enforce the timer-source contract at schedule upsert and in doctor**
  - Introduce a transaction-scoped domain helper in `core.py` that provisions a missing source as
    `{kind: timer, state: enabled}` or validates the existing source against the requested space and
    kind without mutating it.
  - Use that helper from both `upsert_timer_schedule()` and `upsert_calendar_schedule()` inside the
    same transaction as the schedule write. Preserve an identical calendar upsert's revision and
    `next_run_at`.
  - Extend `doctor.py` with a stable error finding that identifies the schedule, configured source,
    expected space/kind, and observed state without repairing anything.
  - Add focused core/CLI/doctor tests: missing source is provisioned as `timer`; incompatible source
    space or kind rejects add/update atomically; compatible existing timer source remains intact;
    doctor catches the deployed mismatch before execution.
  - Verification: configured per-step unit suite and both plugin validators.

- **1.2 Add the audited legacy calendar-source repair command**
  - Add a core operation and CLI surface `schedule repair-calendar-source <schedule-id>`.
  - Require an existing calendar schedule using adapter `timer`; require its source to exist in the
    schedule's space and to match the explicit legacy metadata shape. Inspect every schedule that
    references the source and refuse repair if any consumer is not a compatible timer-backed
    schedule for the same space. Refuse unrelated source kinds instead of coercing them.
  - In one transaction, change only source `kind` and `config.adapter` to `timer`, preserving all
    unrelated config keys, state, timestamps, routes, events, schedule revision, and `next_run_at`.
    Record an audit command with before/after contract facts. A second call must be a safe,
    explicitly reported no-op or idempotent success, not another mutation.
  - Add CLI/domain tests for happy path, idempotence, strict rejections, shared-source safety,
    rollback, audit evidence, and preservation of unrelated fields/history.
  - Verification: configured per-step unit suite and both plugin validators.

### Phase 2 — Declarative prevention and occurrence invariants

- **2.1 Enforce the same timer-source contract in declarative topology**
  - Make topology validation resolve each timer schedule's declared source and require the same
    declared space and kind `timer` before planning or applying.
  - Keep schedule application delegated to the shared core upsert path so imperative CLI and
    declarative topology cannot diverge.
  - Add tests that invalid calendar and interval timer documents fail before any resource is
    created/updated, while a valid calendar topology still applies idempotently, exports cleanly,
    and cannot later restore a repaired source to kind `calendar`.
  - Verification: configured per-step unit suite and both plugin validators.

- **2.2 Prove due and catch-up occurrences are durable and processed once**
  - Add a regression fixture that seeds the deployed legacy database shape directly (without using
    the now-validating public upsert), including one space, legacy `calendar` source, exactly one
    matching route, processor binding, and enabled daily calendar schedule.
  - Repair it, advance across several occurrences, and execute one supervisor cycle. Assert only
    the latest eligible `catch-up-once` occurrence is represented; its external ID is
    `schedule:<schedule-id>:r<revision>:<scheduled-for>`; and exactly one event, processor run, and
    processor delivery exist.
  - Reopen the database, run another cycle, simulate a supervisor restart, and run again before the
    next due instant; assert counts remain one and `next_run_at` stays at the next future occurrence.
  - Claim and complete the run, then prove it is not claimable again. Add the corresponding normal
    due case with one matching route and the same one-event/one-run/one-delivery invariant.
  - Preserve the existing atomic transaction around event insertion, routing, adapter-run evidence,
    and schedule advancement; fix only an in-scope defect if these tests expose one.
  - Verification: configured per-step unit suite and both plugin validators.

### Phase 3 — Operator contract and release metadata

- **3.1 Document the repair workflow and prepare release 0.11.5**
  - Document canonical timer-source ownership, doctor output, safe backup/repair/topology plan flow,
    and the logical exactly-once boundary in the README, operations guide, supervisor guide, and
    Switchboard skill where operators need it.
  - Bump `pyproject.toml`, `uv.lock`, package `__version__`, Codex and Claude plugin manifests, and
    marketplace metadata from 0.11.4 to 0.11.5; keep host manifests aligned.
  - Verification: configured per-step unit suite and both plugin validators.

## Checkpoints and final gate

- Run a checkpoint at the end of each phase (and at the end of the run), rewrite `HANDOFF.md`, append
  `NOTIFY.md`, and commit checkpoint evidence separately from implementation commits.
- After all task rows are done, run the configured isolated Python 3.12 and 3.13 suites, wheel
  build/install acceptance test, and both plugin validators. Review the full issue diff for scope,
  private data, debug residue, and contract drift before release.
- This repo integrates directly to `main`; no worktree or PR is created. The existing approved scope
  satisfies the plan gate, while any material expansion requires a new decision.

## Post-code release and live repair runbook

These are operational actions after the final code gate; they are not implementation commits and
therefore do not appear as rows in the Tasks state machine.

1. Verify `main` is clean, equals `origin/main`, has no held commit in its push range, and passes the
   final gate. Close issue #2 with the shipped SHA and release its dev-ship lock.
2. Tag and publish GitHub release `v0.11.5`; update both local plugin hosts and verify that their
   installed CLI reports 0.11.5. Reinstall the persistent service through the new CLI while
   preserving its existing explicit relay, activation, retry, web, and alert settings.
3. Through the installed CLI, capture and verify an online backup plus pre-repair inventories of
   the two schedules, sources, events, processor runs/deliveries, audit log, and next-run cursors.
4. Run `schedule repair-calendar-source` only for `personal-inbox` and `work-inbox`. Read both
   results back, verify the source contracts and preserved schedule revision/`next_run_at`, then
   export the repaired owner topology to a new private CLI-created snapshot and require a no-op
   `topology plan`. Keep that private file outside Git.
5. Run one controlled supervisor cycle. For each eligible overdue schedule, verify exactly one new
   latest catch-up event with the expected external ID and exactly one processor run/delivery;
   verify both schedule cursors advance to the next future local occurrence. Allow the shared worker
   to drain serially and verify terminal run plus acknowledged delivery state.
6. Before either schedule is due again, run a second cycle, restart the installed service, and run
   or observe another cycle. Compare the captured IDs/counts: no duplicate event, run, or delivery
   may appear. Finish with schedule previews, source/schedule readback, service status, and a clean
   `doctor` result.
