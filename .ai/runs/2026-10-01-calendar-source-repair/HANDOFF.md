# Handoff: Repair calendar schedule sources

> Rewritten from scratch at every checkpoint. A brand-new agent should be able to resume in
> under 30 seconds from this file alone.

- **Last updated:** 2026-10-01T05:19:55Z
- **Branch:** main (`integration: commit-to-main`)
- **PR:** —
- **Current phase / step:** Phase 2 / 2.1
- **Last commit:** `c68d651` (`feat: repair legacy calendar schedule sources`)

## What just happened

Phase 1 is complete. Schedule upserts now provision or validate canonical `timer` sources in the
same transaction, and doctor reports incompatible deployed sources. The bounded
`schedule repair-calendar-source` command validates the legacy shape and every shared consumer,
preserves unrelated state and history, writes audit evidence, and is idempotent. The checkpoint
verification had no configured commands and was recorded as skipped.

## Next concrete action

Dispatch one executor for step 2.1 only: enforce the same source contract during declarative
topology validation and application, add atomic rejection/idempotent export coverage, run the
configured per-step checks, commit once, update the PLAN row, and push `main`.

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
