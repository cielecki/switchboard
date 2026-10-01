# Handoff: Repair calendar schedule sources

> Rewritten from scratch at every checkpoint. A brand-new agent should be able to resume in
> under 30 seconds from this file alone.

- **Last updated:** 2026-10-01T05:06:58Z
- **Branch:** main (`integration: commit-to-main`)
- **PR:** —
- **Current phase / step:** Phase 1 / 1.1
- **Last commit:** `1e7216a` (`chore: configure dev-ship workflow`)

## What just happened

Issue #2 was claimed and the loop run was scaffolded. The approved implementation scope has been
translated into five sequential, single-commit steps. No product source or live state has been
changed by this planning step.

## Next concrete action

Dispatch one executor for step 1.1 only: implement the shared transaction-scoped timer-source
provisioning/validation contract, wire it into interval and calendar upserts, add the doctor
diagnostic, run the configured per-step checks, commit once, update the PLAN row in that commit,
and push `main`.

## Blockers / open questions

- none; Maciej approved the ticket and implementation scope before this plan was written.

## Environment caveats

- The live deployed runtime is still 0.11.4 until the final gate and release/install runbook.
- The repository integration mode is direct `commit-to-main`; the scaffold's nominal fix branch is
  not used and no isolated worktree should be created.
- Private topology, machine paths, chat identifiers, and live data must remain outside Git.
- The live repair can enqueue real inbox work immediately; do it only after backup and pre-repair
  inventories, then verify the shared worker serially.

## Worktree

- path: repository checkout on `main`
- created this run: no
