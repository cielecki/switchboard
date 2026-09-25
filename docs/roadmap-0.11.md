# Switchboard 0.11: calendar triggers and durable routine workers

## Status

The implementation is complete in the local checkout. Live topology, worker chats, legacy
schedulers, release, and deployment remain unchanged pending a separate migration decision.

## Outcome

Switchboard 0.11 can trigger recurring work at a local wall-clock time without owning the work
itself. A calendar trigger emits a durable event, routing selects a processor, and an existing
durable agent chat performs the domain workflow. Temporary service, plugin, or chat outages delay
delivery while the work remains durable. After downtime, Switchboard emits at most the latest
eligible missed occurrence and reuses the same chat.

The first migration covers these workflows:

- a daily personal inbox pass;
- a weekday work inbox pass;
- a weekly bills pass which may require a later human authorization step.

The CLI remains the only control plane. The web interface provides read-only observability. It
cannot command agents, mutate tasks, or act as a visual workspace.

## Evidence and prerequisites

- The three in-scope legacy scheduled tasks have accumulated 70 non-archived routine sessions: 34
  for the personal inbox, 24 for the work inbox, and 12 for bills. Durable chats stop that recurring
  session creation.
- Existing timer schedules are fixed UTC intervals. They cannot preserve a Warsaw wall-clock time
  across daylight-saving changes or express weekdays directly.
- Existing delivery state, same-generation request IDs, and accepted-but-unclaimed rearming already
  preserve queued work while a worker is unavailable.
- Interval timers currently emit the earliest overdue occurrence once and then advance past later
  missed times. Calendar triggers deliberately change this to the latest eligible occurrence.
- Commit `e0bc533` fixed read-only CLI locking. Commit `d770f15` added regression coverage for an
  open, silent stdin after the caller-side shell bug was fixed. Both are on the published main line.
- The current development head selects one active delivery per consumer in the normal
  single-supervisor path. Calendar-trigger tests must preserve this behavior when personal and work
  occurrences become due together.

## Product boundary

Switchboard owns when an occurrence becomes due, its durable identity, routing, delivery, retry,
and observable state. It does not read mail, decide what to archive, operate a bank, or execute an
agent skill directly. Those policies stay in the worker chat and its domain skill.

Calendar triggers add an event source while Switchboard remains a coordinator. Direct job execution
stays outside the product boundary. Existing interval schedules remain a separate,
backward-compatible schedule kind.

## Calendar schedule contract

A calendar schedule has:

- `kind: "calendar"`;
- `local_time`, such as `07:00`;
- an IANA `timezone`, such as `Europe/Warsaw`;
- `weekdays`, omitted for every day or containing selected weekday names;
- `missed_policy`, initially `skip` or `catch-up-once`;
- explicit `ambiguous_time_policy` and `nonexistent_time_policy`;
- an operator-managed `enabled` flag;
- a server-managed `revision` and absolute UTC `next_run_at`.

The recommended defaults are:

- `catch-up-once`: after downtime, emit only the latest eligible missed occurrence, then advance to
  the first future occurrence;
- no age cutoff for that latest occurrence: these workflows inspect current state, so one sweep is
  still useful after long downtime;
- an ambiguous local time runs once at its first valid instant;
- a nonexistent local time runs at the first valid instant after the clock gap;
- enabling or materially editing a schedule starts a new revision from the current instant and does
  not backfill the disabled or superseded revision.

Each emitted event records `scheduled_for`, `triggered_at`, `late_by_seconds`, schedule identity, and
schedule revision. Schedule identity, revision, and scheduled occurrence determine the external
identity. One database transaction inserts the event and advances `next_run_at`. After a crash, a
retry produces exactly one occurrence.

The CLI adds a calendar form of `schedule add`/`schedule update` plus a read-only occurrence preview.
Human and JSON schedule views show both the local rule and computed UTC next occurrence. All
schedule mutations remain CLI-only.

## Consumer serialization

The two inbox processors share one durable chat through a consumer-level delivery gate. The current
single-supervisor path selects at most one active delivery for the same host and consumer identity.
Other ready runs remain queued. A terminal outcome or `needs-review` releases the gate.

Version 0.11 must enforce that gate transactionally so independently concurrent dispatch calls
cannot race. Processor-level leases alone do not protect a chat shared by two processors.

Every run carries an immutable profile and occurrence identity. The worker claims the run before
acting and never infers whether it is processing the personal or work inbox from chat history.
Wake retries retain the same request ID within one delivery generation. An explicit release, lease
expiry, or review retry starts a new generation and request ID.

Calendar work must retain this invariant and add coverage for two processors becoming due together.
If the invariant cannot be preserved, the safe fallback is a separate durable chat for every
processor.

## Initial topology

Use two ordinary, durable Claude Code chats. Do not create them through routines:

1. **Inbox worker:** one serialized consumer for distinct personal and work inbox processors.
2. **Bills worker:** a separate consumer with its own authorization boundary.

The generic topology is:

| Space | Source | Event | Processor | Calendar |
| --- | --- | --- | --- | --- |
| Inbox | Personal inbox calendar | `inbox.triage.due` with `profile=personal` | Personal inbox | Daily 07:00 Europe/Warsaw |
| Inbox | Work inbox calendar | `inbox.triage.due` with `profile=work` | Work inbox | Monday-Friday 09:00 Europe/Warsaw |
| Personal finance | Bills calendar | `finance.bills-sweep.due` | Bills sweep | Friday 08:00 Europe/Warsaw |

Keep private chat identifiers and local navigation URLs outside this repository. The same rule
applies to account details and routing configuration.

Inbox actions already covered by standing domain rules may complete autonomously. When a run needs
a user decision, it enters `needs-review` and releases the consumer gate.

The bills worker scans both mail profiles and messages, then checks bank history before staging any
payment. It opens the bank and requests login when the interactive surface is available. Before
waiting for the user, it persists the scan result, enters `needs-review`, and releases its lease.
The continuation references the original run, rechecks the date, and scans again. If the interactive
surface is unavailable, the existing fallback parks the summary and sends an SMS only for a verified
real-clock condition: a near deadline, a collection notice, or an expiring service. Payment
deduplication remains grounded in the bank-history check. Preventing duplicate
urgent notifications is a new 0.11 domain requirement.

Technical Switchboard delivery state is visible in the local dashboard and is not posted to Slack.
Any business notification remains owned by the domain workflow.

## Web observability

The read-only schedule view must show:

- the local calendar rule and timezone;
- the next local and UTC occurrence;
- last scheduled and actual trigger times plus lateness;
- schedule health and the selected missed-occurrence policy;
- queued, active, delivery-troubled, and `needs-review` work;
- the durable consumer label and local link when configured.

Version 0.11 must keep an archived or unreachable consumer visible as a delivery problem. Operators
repair or rebind it through the CLI. Switchboard does not replace it automatically with a new chat.

## Migration

Migrate one workflow at a time:

1. Implement and test calendar semantics and transactional occurrence emission.
2. Add the transactional consumer gate, CLI commands, and read-only observability. Preserve existing
   interval timers.
3. Add the private spaces, sources, routes, schedules, processor bindings, and two durable chats with
   schedules disabled.
4. For one workflow, disable its legacy scheduler immediately before a controlled Switchboard
   occurrence. Never let both systems own the same occurrence.
5. Verify completed work. An inbox run must finish its pass or surface a real decision. A bills run
   must persist its scan and produce a verified review handoff.
6. If verification succeeds, enable the calendar schedule and retain the legacy task disabled as a
   short-lived rollback carrier. If it fails, disable the new schedule and re-enable the legacy task.
7. Repeat for the other workflows. Remove obsolete legacy carriers only after the live soak.

## Acceptance criteria

- Warsaw schedules retain their local clock time across both daylight-saving transitions.
- Daily, weekday, and Friday-only occurrence previews match the calendar rule.
- A long service outage creates at most the latest eligible catch-up occurrence.
- A crash between due detection and persistence cannot lose or duplicate an occurrence.
- Editing or re-enabling a schedule creates a fresh revision and does not backfill old revisions.
- When both inbox schedules are due after downtime, one shared chat receives them serially and each
  run retains the correct immutable profile.
- Two concurrent dispatch calls cannot activate the same consumer twice.
- A plugin, relay, or chat outage keeps work durable. Retries within one delivery generation use a
  stable request ID.
- An archived consumer remains visible and repairable without automatic chat creation.
- `needs-review` releases the consumer and preserves a link to the originating run.
- The bills continuation cannot duplicate a payment action or urgent notification.
- Existing interval schedules and their event identities remain backward-compatible.
- The dashboard performs no mutations and no technical Switchboard message is sent to Slack.

## Alternatives rejected

- Reusing fixed-second interval timers unchanged: fixed UTC intervals cannot preserve local time
  through daylight-saving changes or express weekday rules.
- Running mail and finance workflows inside the Switchboard service: crosses the coordinator's
  ownership and authority boundary and couples domain credentials to infrastructure.
- Keeping routine-created sessions: preserves the current session proliferation and loses the
  durable consumer identity that Switchboard is intended to coordinate.
- Using one chat without consumer-level serialization: simultaneous personal and work wakes can
  interleave and apply the wrong profile.

## Next release boundary

Review and publish the local 0.11 implementation before changing live state. Live migration then
proceeds one workflow at a time using private topology and stable worker identifiers; it is not
part of the implementation commit.
