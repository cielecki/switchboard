# Handoff: Repair calendar schedule sources

> Rewritten from scratch at every checkpoint. A brand-new agent should be able to resume in
> under 30 seconds from this file alone.

- **Last updated:** 2026-10-01T05:28:54Z
- **Branch:** main (`integration: commit-to-main`)
- **PR:** —
- **Current phase / step:** Phase 3 / 3.1
- **Last commit:** `6de32b6` (`test: prove calendar occurrence durability`)

## What just happened

Phase 2 is complete. Declarative topology now rejects wrong-space or non-`timer` sources before
mutation and valid calendar topology remains idempotent and exportable through the shared core
path. Regression coverage proves one durable event, processor run, and delivery for normal due and
latest-only `catch-up-once` occurrences across database reopen and supervisor-style restart. No
production defect was exposed. The checkpoint had no configured commands and was recorded as
skipped.

## Next concrete action

Dispatch one executor for step 3.1 only: document the operator contract and repair workflow, align
all public version metadata at 0.11.5, run the configured per-step checks, commit once, update the
PLAN row, and push `main`.

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
