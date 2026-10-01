# Persistent supervisor

The supervisor is Switchboard's long-running control loop. The CLI owns every configuration write;
the web UI only reads the resulting state.

Each cycle:

1. selects enabled adapter schedules whose `next_run_at` has arrived; for non-stream schedules, a
   persisted retry deadline must also be absent or elapsed;
2. starts each adapter in an independent worker and records completion or failure without blocking
   delivery, alerts, or other schedules;
3. advances interval schedules by their configured cadence, while calendar schedules preserve an
   IANA-zone wall-clock rule and atomically persist the selected occurrence with their next cursor;
4. recovers expired processor leases and materializes missing bound deliveries;
   accepted wakes that are not acknowledged or claimed within the configured timeout are re-armed
   with the same stable request ID, and legacy databases with multiple accepted wakes per consumer
   are coalesced;
5. dispatches the oldest eligible wait or processor deliveries, up to the configured per-cycle
   batch limit, when a chats relay is configured, with at most one in-flight processor wake per
   consumer;
6. updates local schedule alert episodes after failed scheduled attempts without calling an external
   notifier. Separately, it opens one consumer-level alert episode after one or more processor
   deliveries remain unreachable or unclaimed for the threshold, and sends one recovery after the
   final issue clears;
7. writes a heartbeat, cycle time, and combined error summary.

A failed or timed-out source cannot stop the coordinator; Switchboard kills the whole external
process group so descendants cannot keep captured pipes open after the nominal timeout. Failed chat deliveries stay pending and
retain their stable broker request ID. The retry interval prevents tight failure loops.
Native `held`, `refused`, `dropped`, `denied`, and `expired` receipts are delivery failures and
remain eligible for retry. Socket acceptance without transcript delivery is provisional; if a
wait is not acknowledged or a processor has not claimed its run by `--accepted-retry`, Switchboard
retries the same request ID rather than creating a duplicate wake.
The default `--delivery-batch 1` keeps source polling and the health heartbeat responsive even when
the initial queue contains many chat wakes. Increase it only when the relay is known to return
quickly.
Processor requeues increment a delivery generation, producing a new stable request ID without
duplicating a prior accepted wake.
After a long consumer outage, the backlog remains durable without producing one chat message per
run. When the consumer resumes, `processor claim-next` leases the oldest pending run atomically;
finishing it makes the next queued wake eligible.

The optional alert adapter is an argv list supplied through `--alert-command-json`; it receives a
JSON object on standard input. Switchboard never stores messenger credentials or destinations in
the repository. Notification and recovery claims are stored before the external command runs.
An adapter failure remains visible on the episode but is not retried into a notification storm.
The alert adapter does not handle schedule alert episodes. Switchboard does not post messages about
those episodes to Slack or chats.

## Scheduled-attempt backoff

A failed non-stream interval or calendar attempt creates one terminal adapter run, keeps the logical
`next_run_at` unchanged, and sets `retry_not_before`. The deterministic delay sequence is 30, 60,
120, 240, 480, and then 900 seconds for every later failure. Until that deadline, ordinary supervisor
polls create no new adapter run. Because the streak and deadline live in SQLite, reopening the
database or restarting the supervisor does not trigger an early attempt.

The first failure in a consecutive streak opens one internal schedule alert episode. Later failures
increment the streak and update that episode. A successful scheduled attempt clears the retry state
and records one recovery. A material schedule update or disabled-to-enabled transition also clears
the streak and records recovery; an identical upsert does not.

Switchboard records the terminal adapter run and failed retry state in one transaction. For timer
and calendar occurrences, event persistence, first-match routing, any processor run and delivery,
cursor advance, retry reset, and episode recovery share the occurrence transaction. One schedule in
backoff does not block another ready schedule.

Use `schedule list`, `adapter runs`, and `doctor` to inspect the state. The JSON schedule view exposes
the failure streak, last failure, next retry, and episode under `retry`. The human table shows the
deadline, streak, episode state, and last failure; the read-only dashboard shows retry status in its
needs-attention area. For enabled schedules, `doctor` distinguishes active backoff from a retry
whose deadline has elapsed and reports inconsistent persisted state as an error.

This mechanism excludes `command-stream` schedules. It does not change `--delivery-retry`,
`--accepted-retry`, stream restart delay, processor attempts, or downstream idempotency requirements.
The fixed sequence is global and has no per-schedule tuning.

## CLI control

```bash
switchboard --json schedule list
switchboard --json schedule preview work-inbox --from 2026-10-23T12:00:00+00:00
switchboard --json schedule disable ingest
switchboard --json schedule enable ingest
switchboard --json schedule delete ingest
switchboard --json supervisor status
```

Schedule configuration and supervisor health live in the external Switchboard database. Private
paths and source data never belong in the plugin checkout.

Calendar schedules support `catch-up-once` and `skip`. Catch-up selects the latest eligible missed
occurrence and emits at most one event, even after long downtime. A material edit or re-enable
increments the schedule revision and re-anchors it to the next future occurrence. Ambiguous and
nonexistent local times use the explicit policies stored with the rule. Failed calendar execution
does not advance `next_run_at`, so the bounded schedule retry can recover without losing the
occurrence.

Each emitted calendar occurrence uses this durable event external ID, deduplicated within its
source: `schedule:<schedule-id>:r<revision>:<scheduled-for>`. In one database transaction,
Switchboard inserts the event, applies first-match routing, creates any matched processor run and
bound delivery, records adapter-run evidence, and advances the cursor to the next future
occurrence. Reopening the database, polling again, or restarting the supervisor before the next
due time does not create a duplicate event, run, or delivery. With `catch-up-once`, Switchboard
creates those records only for the latest missed occurrence.

The guarantee ends at the database boundary. External effects can repeat. A delivery may be
retried with its stable request ID, and a processor run may have multiple attempts. The destination
processor must make writes to mail, CRM, Slack, and other systems idempotent. It must claim the
durable run before starting work and complete the run only after it knows the external result. A
completed run cannot be claimed again.

## Process model

Only one supervisor may hold a database's lock. `supervisor run` also hosts the read-only HTTP UI.
SIGINT and SIGTERM cause a clean shutdown and a final `stopped` heartbeat. Interrupted adapter runs
are marked failed when the next supervisor starts.

`service install` is currently macOS-only. It writes and loads
`~/Library/LaunchAgents/io.github.cielecki.switchboard.plist` with `RunAtLoad` and `KeepAlive`, and
writes stdout/stderr logs under `~/.local/state/switchboard/`. Use `service uninstall` to unload and
remove it. Marketplace installs are versioned, so reinstall the service after a plugin update.
