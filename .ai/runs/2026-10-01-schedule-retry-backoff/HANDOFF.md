# Handoff: Persist bounded schedule retry backoff

> Rewritten from scratch at every checkpoint. A brand-new agent should be able to resume in
> under 30 seconds from this file alone.

- **Last updated:** 2026-10-01T08:37:43Z
- **Branch:** main (`integration: commit-to-main`)
- **PR:** —
- **Current phase / step:** 3.1
- **Last commit:** `f699d15` (`test: prove schedule retry recovery invariants`)

## What just happened

Phase 2 is complete. CLI, doctor, the read-only dashboard, and topology now share one retry-state
projection without adding a mutation surface. Deterministic integration and rollback tests cover
every retry delay, reopen/restart, isolation, recovery, alert lifecycle, and routed interval and
calendar occurrences without duplicates. The tests also corrected interval timers to use the same
stable occurrence identity as calendar schedules. Checkpoint and UI runners had no configured
commands and were recorded as skipped; independent rendered visual QA remains required before
release.

## Next concrete action

Dispatch one executor for step 3.1. It should document bounded retry, update the deployed 0.11
roadmap status, align all release metadata at 0.11.6, run the configured per-step checks, commit
once, update the PLAN row, and push `main`.

## Blockers / open questions

- none; the approved issue body fixes the retry sequence, affected schedule classes, alert policy,
  observability surfaces, non-goals, release version, and live-verification boundary.

## Environment caveats

- The repository integrates directly to `main`; the scaffold's nominal fix branch is not used and
  no isolated worktree should be created.
- Private topology, machine paths, chat identifiers, controlled-adapter paths, and live evidence
  must remain outside Git.
- Scheduled adapter helpers currently finish their own adapter runs. Step 1.2 must preserve
  standalone adapter behavior while giving scheduled execution one atomic terminal owner.
- The web dashboard remains read-only. Schedule alert episodes are internal evidence and must not
  be passed to the configured external alert command.

## Worktree

- path: repository checkout on `main`
- created this run: no
