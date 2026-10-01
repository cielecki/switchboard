# Handoff: Repair calendar schedule sources

> Rewritten from scratch at every checkpoint. A brand-new agent should be able to resume in
> under 30 seconds from this file alone.

- **Last updated:** 2026-10-01T05:44:58Z
- **Branch:** main (`integration: commit-to-main`)
- **PR:** —
- **Current phase / step:** implementation complete / final gate
- **Last commit:** `d33bf40` (`docs: prepare switchboard 0.11.5`)

## What just happened

All five implementation steps are complete and pushed. Public documentation now covers the
canonical timer-source contract, doctor diagnostic, CLI-only repair flow, and the database-scoped
exactly-once boundary. Package, plugin, marketplace, and lock metadata are aligned at 0.11.5. The
phase checkpoint had no configured commands and was recorded as skipped. No release, installation,
or live repair has happened yet.

## Next concrete action

Run the configured final gate. If it passes, review the complete issue diff and then follow the
post-code release/install/live-repair runbook without changing the approved scope.

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
