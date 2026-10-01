# Handoff: Persist bounded schedule retry backoff

> Rewritten from scratch at every checkpoint. A brand-new agent should be able to resume in
> under 30 seconds from this file alone.

- **Last updated:** 2026-10-01T08:23:06Z
- **Branch:** main (`integration: commit-to-main`)
- **PR:** —
- **Current phase / step:** 2.1
- **Last commit:** `a630266` (`fix: atomically finalize scheduled attempts`)

## What just happened

Phase 1 is complete. Schema v10 persists retry state and internal schedule-alert episodes;
`due_schedules()` respects the fixed retry deadline while preserving the logical cursor. Scheduled
attempt finalization now atomically records terminal run evidence, retry state, and one alert
episode per streak, while success atomically resets state and records recovery. Standalone adapters,
streams, and external notification paths retain their previous behavior. The checkpoint had no
configured commands and was recorded as skipped.

## Next concrete action

Dispatch one executor for step 2.1. It should expose the same retry and episode projection through
CLI, doctor, the read-only web dashboard, and topology behavior; run the configured per-step checks;
commit once; update the PLAN row; and push `main`.

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
