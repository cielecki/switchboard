# Handoff: Persist bounded schedule retry backoff

> Rewritten from scratch at every checkpoint. A brand-new agent should be able to resume in
> under 30 seconds from this file alone.

- **Last updated:** 2026-10-01T07:59:38Z
- **Branch:** main (`integration: commit-to-main`)
- **PR:** —
- **Current phase / step:** 1.1
- **Last commit:** `2d3e794` (completed issue #2 run)

## What just happened

Issue #3 is claimed and its loop run was scaffolded. Maciej approved the issue's exact scope and
0.11.6 live deployment before this plan was written. Repository research confirmed that failed
calendar schedules retain a due logical cursor and can be selected every five seconds, while failed
interval schedules currently advance their cadence. The durable execution plan now separates
schema/domain state, atomic supervisor finalization, observability, end-to-end regression evidence,
and release metadata into five sequential one-commit steps. No product source or live Switchboard
state has changed during planning.

## Next concrete action

Dispatch one executor for step 1.1. It should implement only persisted retry state, deterministic
delay/due selection, and reset semantics, update the PLAN row in the same commit, run the configured
per-step verification, and push `main`.

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
