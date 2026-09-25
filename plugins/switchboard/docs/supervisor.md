# Persistent supervisor

The supervisor is Switchboard's long-running control loop. The CLI owns every configuration write;
the web UI only reads the resulting state.

Each cycle:

1. selects enabled adapter schedules whose `next_run_at` has arrived;
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
6. opens one consumer-level alert episode after one or more processor deliveries remain unreachable
   or unclaimed for the threshold, and sends one recovery after the final issue clears;
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
does not advance `next_run_at`, so the supervisor can retry without losing the occurrence.

## Process model

Only one supervisor may hold a database's lock. `supervisor run` also hosts the read-only HTTP UI.
SIGINT and SIGTERM cause a clean shutdown and a final `stopped` heartbeat. Interrupted adapter runs
are marked failed when the next supervisor starts.

`service install` is currently macOS-only. It writes and loads
`~/Library/LaunchAgents/io.github.cielecki.switchboard.plist` with `RunAtLoad` and `KeepAlive`, and
writes stdout/stderr logs under `~/.local/state/switchboard/`. Use `service uninstall` to unload and
remove it. Marketplace installs are versioned, so reinstall the service after a plugin update.
