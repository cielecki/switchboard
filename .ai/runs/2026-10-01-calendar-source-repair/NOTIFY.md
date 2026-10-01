# Notify: Repair calendar schedule sources

> Append-only operator inbox. Newest entries at the bottom. Log run start/end, every checkpoint,
> every blocker, every decision, every sub-agent delegation, and every skipped check (with reason).
> Do NOT log routine per-step progress here — that lives in the Tasks table.

## 2026-10-01T05:02:50Z — run created

Run folder scaffolded for issue 2 (loop mode). Branch `fix/calendar-source-repair` off `main`.

## 2026-10-01T05:06:58Z — decision and planning delegation

Maciej's existing `Proceed` approved issue #2 and its stated implementation/live-repair scope.
Planning was delegated to a dedicated sub-agent. The repository uses direct `commit-to-main`, so
the scaffold's nominal fix branch is not used. The durable plan has five sequential one-commit
implementation steps followed by the final gate, 0.11.5 release/install, and CLI-only live repair
verification. No source code or live Switchboard state changed during planning.

## 2026-10-01T05:19:55Z — phase 1 checkpoint

Steps 1.1 and 1.2 are complete and pushed to `main`. The shared timer-source contract, doctor
diagnostic, and strict audited legacy repair command passed 115 unit tests and both plugin
validators. The configured checkpoint verification contained no commands, so it completed as an
explicit skip. No live state has been repaired yet; the deployed runtime remains 0.11.4.

## 2026-10-01T05:28:54Z — phase 2 checkpoint

Steps 2.1 and 2.2 are complete and pushed to `main`. Topology now enforces the same timer-source
contract before mutation, and normal-due plus catch-up regression tests prove one logical
event/run/delivery across reopen and supervisor-style restart. The full suite has 119 passing
tests; both plugin validators pass. The checkpoint had no configured commands and was recorded as
an explicit skip. No live state has been repaired yet.

## 2026-10-01T05:44:58Z — phase 3 checkpoint

Step 3.1 is complete and pushed to `main`. Operator documentation and all release-bearing metadata
are aligned at 0.11.5; 119 tests, both plugin validators, prose lint, and factual review passed.
The checkpoint had no configured commands and was recorded as an explicit skip. Implementation is
complete, while release, installation, and live state remain untouched pending the final gate.

## 2026-10-01T06:45:05Z — run complete

The final gate passed 119 tests on Python 3.12 and 3.13, isolated wheel build/install acceptance,
and both plugin validators. Independent review passed after the PLAN ledger was corrected to point
at reachable amended commits. Release v0.11.5 was published at `b209274`; issue #2 was closed and
its ship lock released. Both plugin hosts now run 0.11.5.

The two approved live legacy sources were backed up and repaired through the CLI. Each overdue
schedule created one logical event, run, and delivery; both runs completed with one attempt, both
deliveries were acknowledged, and repeated polling plus service restart created no duplicates.
The private topology now plans with zero changes or conflicts, the persistent service uses the
0.11.5 launcher with its preserved settings, and doctor finishes with zero errors and warnings.
Private operational evidence and the verified backup remain outside Git.
