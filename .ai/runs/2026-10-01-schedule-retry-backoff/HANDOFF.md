# Handoff: Persist bounded schedule retry backoff

> Rewritten from scratch at every checkpoint. A brand-new agent should be able to resume in
> under 30 seconds from this file alone.

- **Last updated:** 2026-10-01T09:07:55Z
- **Branch:** main (`integration: commit-to-main`)
- **PR:** —
- **Current phase / step:** 2.4 visual-QA repair
- **Last commit:** `bcb2380` (`docs: prepare switchboard 0.11.6`)

## What just happened

Step 2.3 fixed the interval identity upgrade edge and added a regression that proves one legacy
event/run/delivery across retry and reopen. Independent visual QA then found release-blocking table
layout defects: identifier cells wrap one character per line, narrow Source health creates page-level
horizontal overflow, and absent schedule values render as raw `null`. The retry attention card and
read-only behavior passed. No release, installation, or live verification has occurred.

## Next concrete action

Dispatch one executor for step 2.4 only, then rerun the full gate, independent review, and a new
fresh-context visual QA pass on the final revision.

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
