from __future__ import annotations

import hashlib
import subprocess
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from switchboard import core
from switchboard.db import Database
from switchboard.supervisor import (
    DeliveryWorkers,
    ScheduleWorkers,
    process_processor_alerts,
    run_cycle,
    run_once,
)


class SupervisorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.db = Database(self.root / "switchboard.sqlite3")
        self.status_script = self.root / "status.py"
        self.status_script.touch()

    def wait_until(self, predicate, message: str) -> None:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.01)
        self.fail(message)

    def test_due_schedule_runs_once_and_advances(self) -> None:
        discovery_script = self.root / "watch.py"
        discovery_script.touch()
        created = core.upsert_ingest_schedule(
            self.db,
            "ingest",
            status_script=self.status_script,
            discovery_script=discovery_script,
            every_seconds=60,
        )
        calls: list[dict] = []

        def ingest_runner(_db, **kwargs):
            calls.append(kwargs)
            return {"run": {"id": "run-1", "state": "completed"}}

        at = datetime.fromisoformat(created["next_run_at"])
        first = run_cycle(
            self.db,
            relay=None,
            cli_command=["switchboard"],
            at=at,
            ingest_runner=ingest_runner,
        )
        second = run_cycle(
            self.db,
            relay=None,
            cli_command=["switchboard"],
            at=at,
            ingest_runner=ingest_runner,
        )

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["discovery_script"], str(discovery_script.resolve()))
        self.assertEqual(first["schedules"][0]["state"], "completed")
        self.assertEqual(second["schedules"], [])
        schedule = core.get_schedule(self.db, "ingest")
        self.assertEqual(schedule["last_state"], "completed")
        self.assertEqual(
            datetime.fromisoformat(schedule["next_run_at"]),
            datetime.fromisoformat(schedule["last_finished_at"]) + timedelta(seconds=60),
        )

    def test_slow_schedule_advances_from_completion_time(self) -> None:
        created = core.upsert_ingest_schedule(
            self.db,
            "slow-ingest",
            status_script=self.status_script,
            every_seconds=60,
        )
        started_at = datetime.fromisoformat(created["next_run_at"])
        finished_at = started_at + timedelta(seconds=90)

        with patch("switchboard.supervisor.core.now", return_value=finished_at.isoformat()):
            run_cycle(
                self.db,
                relay=None,
                cli_command=["switchboard"],
                at=started_at,
                ingest_runner=lambda *_args, **_kwargs: {
                    "run": {"id": "run-slow", "state": "completed"}
                },
            )

        schedule = core.get_schedule(self.db, "slow-ingest")
        self.assertEqual(schedule["last_finished_at"], finished_at.isoformat())
        self.assertEqual(
            datetime.fromisoformat(schedule["next_run_at"]),
            finished_at + timedelta(seconds=60),
        )

    def test_schedule_failure_is_recorded_without_crashing_cycle(self) -> None:
        core.upsert_ingest_schedule(
            self.db,
            "ingest",
            status_script=self.status_script,
            every_seconds=60,
        )

        def ingest_runner(*_args, **_kwargs):
            raise ValueError("source unavailable")

        result = run_cycle(
            self.db,
            relay=None,
            cli_command=["switchboard"],
            ingest_runner=ingest_runner,
        )

        self.assertEqual(result["schedules"][0]["state"], "failed")
        self.assertIn("source unavailable", result["errors"][0])
        self.assertEqual(core.get_schedule(self.db, "ingest")["last_state"], "failed")

    def test_due_inbound_schedule_uses_dedicated_runner(self) -> None:
        ledger_script = self.root / "ledger.py"
        slack_script = self.root / "watch-slack.sh"
        ledger_script.touch()
        slack_script.touch()
        created = core.upsert_inbound_schedule(
            self.db,
            "nina-inbound",
            ledger_script=ledger_script,
            profile="nina",
            every_seconds=120,
            space_id="nina-inbound",
            slack_discovery_script=slack_script,
        )
        calls: list[dict] = []

        def inbound_runner(_db, **kwargs):
            calls.append(kwargs)
            return {"run": {"id": "run-inbound", "state": "completed"}}

        result = run_cycle(
            self.db,
            relay=None,
            cli_command=["switchboard"],
            at=datetime.fromisoformat(created["next_run_at"]),
            inbound_runner=inbound_runner,
        )

        self.assertEqual(result["schedules"][0]["state"], "completed")
        self.assertEqual(calls[0]["profile"], "nina")
        self.assertEqual(calls[0]["slack_discovery_script"], str(slack_script.resolve()))

    def test_due_stream_schedule_uses_persistent_runner(self) -> None:
        executable = self.root / "stream"
        executable.touch()
        created = core.upsert_stream_schedule(
            self.db,
            "nina-slack-socket",
            command=[str(executable), "--switchboard"],
            environment={"CHANNEL": "C123"},
            every_seconds=5,
        )
        calls: list[dict] = []

        def stream_runner(_db, **kwargs):
            calls.append(kwargs)
            return {"run": {"id": "run-stream", "state": "completed"}}

        workers = ScheduleWorkers(self.db, stream_runner=stream_runner)
        self.addCleanup(workers.shutdown)
        launched = workers.poll(created["next_run_at"])
        self.assertEqual(launched, [{"id": "nina-slack-socket", "state": "running"}])

        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and not calls:
            time.sleep(0.01)
        self.assertEqual(calls, [{
            "command": [str(executable), "--switchboard"],
            "environment": {"CHANNEL": "C123"},
        }])

    def test_schedule_workers_do_not_block_other_sources(self) -> None:
        release = threading.Event()
        core.upsert_ingest_schedule(
            self.db,
            "slow-ingest",
            status_script=self.status_script,
            every_seconds=60,
        )
        core.upsert_timer_schedule(
            self.db,
            "daily",
            space_id="demo",
            source_id="timer/daily",
            event_type="maintenance.due",
            every_seconds=86400,
        )

        def slow_ingest(_db, **_kwargs):
            release.wait(3)
            return {"run": {"id": "slow", "state": "completed"}}

        workers = ScheduleWorkers(self.db, ingest_runner=slow_ingest)
        self.addCleanup(lambda: (release.set(), workers.shutdown()))
        launched = workers.poll()
        self.assertEqual({item["id"] for item in launched}, {"slow-ingest", "daily"})

        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            if core.get_schedule(self.db, "daily")["last_state"] == "completed":
                break
            time.sleep(0.02)
        else:
            self.fail("independent timer schedule was blocked by the slow source")
        self.assertIsNone(core.get_schedule(self.db, "slow-ingest")["last_state"])

    def test_timer_schedule_keeps_anchored_utc_cadence(self) -> None:
        created = core.upsert_timer_schedule(
            self.db,
            "daily",
            space_id="demo",
            source_id="timer/daily",
            event_type="maintenance.due",
            every_seconds=86400,
            first_run_at="2026-09-21T23:50:00+02:00",
        )
        self.assertEqual(created["next_run_at"], "2026-09-21T21:50:00+00:00")

        finished_at = "2026-09-21T21:55:00+00:00"
        with patch("switchboard.supervisor.core.now", return_value=finished_at):
            run_cycle(
                self.db,
                relay=None,
                cli_command=["switchboard"],
                at=datetime.fromisoformat(created["next_run_at"]),
            )

        schedule = core.get_schedule(self.db, "daily")
        self.assertEqual(schedule["next_run_at"], "2026-09-22T21:50:00+00:00")

    def test_calendar_schedule_catches_up_only_latest_missed_occurrence(self) -> None:
        created = core.upsert_calendar_schedule(
            self.db,
            "weekday-inbox",
            space_id="inbox",
            source_id="timer/weekday-inbox",
            event_type="inbox.sweep.due",
            local_time="07:00",
            timezone="Europe/Warsaw",
            weekdays=["mon", "tue", "wed", "thu", "fri"],
            at="2026-09-20T12:00:00+00:00",
        )
        self.assertEqual(created["next_run_at"], "2026-09-21T05:00:00+00:00")

        result = run_cycle(
            self.db,
            relay=None,
            cli_command=["switchboard"],
            at=datetime.fromisoformat("2026-09-24T12:00:00+00:00"),
        )

        self.assertEqual(result["schedules"][0]["state"], "completed")
        events = core.list_events(self.db)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["occurred_at"], "2026-09-24T05:00:00+00:00")
        self.assertEqual(events[0]["attributes"]["late_by_seconds"], 7 * 3600)
        schedule = core.get_schedule(self.db, "weekday-inbox")
        self.assertEqual(schedule["next_run_at"], "2026-09-25T05:00:00+00:00")
        self.assertEqual(schedule["last_scheduled_for"], "2026-09-24T05:00:00+00:00")

    def test_calendar_skip_policy_advances_without_emitting_old_backlog(self) -> None:
        core.upsert_calendar_schedule(
            self.db,
            "skip-old",
            space_id="inbox",
            source_id="timer/skip-old",
            event_type="inbox.sweep.due",
            local_time="07:00",
            timezone="Europe/Warsaw",
            weekdays=["mon", "tue", "wed", "thu", "fri"],
            missed_policy="skip",
            at="2026-09-20T12:00:00+00:00",
        )

        result = core.execute_due_calendar_schedule(
            self.db, "skip-old", triggered_at="2026-09-24T12:00:00+00:00"
        )

        self.assertEqual(result["run"]["emitted_events"], 0)
        self.assertEqual(core.list_events(self.db), [])
        self.assertEqual(
            core.get_schedule(self.db, "skip-old")["next_run_at"],
            "2026-09-25T05:00:00+00:00",
        )

    def test_identical_calendar_upsert_preserves_revision_and_next_run(self) -> None:
        arguments = {
            "space_id": "inbox",
            "source_id": "timer/inbox",
            "event_type": "inbox.sweep.due",
            "local_time": "07:00",
            "timezone": "Europe/Warsaw",
            "weekdays": ["mon", "tue", "wed", "thu", "fri"],
        }
        first = core.upsert_calendar_schedule(
            self.db, "inbox", **arguments, at="2026-09-20T12:00:00+00:00"
        )
        second = core.upsert_calendar_schedule(
            self.db, "inbox", **arguments, at="2026-09-22T12:00:00+00:00"
        )
        changed = core.upsert_calendar_schedule(
            self.db,
            "inbox",
            **{**arguments, "local_time": "08:00"},
            at="2026-09-22T12:00:00+00:00",
        )

        self.assertEqual(second["revision"], first["revision"])
        self.assertEqual(second["next_run_at"], first["next_run_at"])
        self.assertEqual(changed["revision"], first["revision"] + 1)
        self.assertEqual(changed["next_run_at"], "2026-09-23T06:00:00+00:00")

    def test_calendar_failure_does_not_advance_or_leave_event(self) -> None:
        created = core.upsert_calendar_schedule(
            self.db,
            "atomic",
            space_id="inbox",
            source_id="timer/atomic",
            event_type="inbox.sweep.due",
            local_time="07:00",
            timezone="Europe/Warsaw",
            at="2026-09-20T12:00:00+00:00",
        )
        with self.db.transaction() as connection:
            connection.execute(
                "INSERT INTO routes(id, space_id, name, predicate_json, target_json, state, "
                "created_at, updated_at) VALUES(?,?,?,?,?,'enabled',?,?)",
                (
                    "bad-route",
                    "inbox",
                    "Bad",
                    '{}',
                    '{"kind":"unknown"}',
                    "2026-09-20T12:00:00+00:00",
                    "2026-09-20T12:00:00+00:00",
                ),
            )

        result = run_cycle(
            self.db,
            relay=None,
            cli_command=["switchboard"],
            at=datetime.fromisoformat("2026-09-21T06:00:00+00:00"),
        )

        self.assertEqual(result["schedules"][0]["state"], "failed")
        self.assertEqual(core.list_events(self.db), [])
        schedule = core.get_schedule(self.db, "atomic")
        self.assertEqual(schedule["next_run_at"], created["next_run_at"])
        self.assertEqual(schedule["last_state"], "failed")
        self.assertEqual(core.list_adapter_runs(self.db)[0]["state"], "failed")

    def test_once_records_a_clean_supervisor_stop(self) -> None:
        result = run_once(self.db, relay=None, cli_command=["switchboard"])

        self.assertEqual(result["errors"], [])
        state = core.supervisor_status(self.db)
        self.assertEqual(state["state"], "stopped")
        self.assertIsNone(state["pid"])
        self.assertFalse(state["dispatch_enabled"])

    def test_cycle_dispatches_pending_delivery_when_relay_is_configured(self) -> None:
        core.create_space(self.db, "demo")
        core.register_source(self.db, "mail", "demo", "mail")
        core.create_wait(
            self.db,
            space_id="demo",
            consumer="chat:codex:task-123",
            predicate={"event_type": "message.received"},
        )
        emitted = core.emit_event(
            self.db,
            source_id="mail",
            external_id="message-1",
            event_type="message.received",
            attributes={},
        )
        relay = self.root / "send-message.py"
        relay.touch()
        calls: list[str] = []

        def delivery_runner(db, delivery_id, **_kwargs):
            calls.append(delivery_id)
            with db.transaction() as connection:
                connection.execute(
                    "UPDATE deliveries SET state='accepted' WHERE id=?", (delivery_id,)
                )
            return {"id": delivery_id, "state": "accepted"}

        result = run_cycle(
            self.db,
            relay=relay,
            cli_command=["switchboard"],
            delivery_runner=delivery_runner,
        )

        self.assertEqual(calls, emitted["deliveries"])
        self.assertEqual(result["deliveries"][0]["state"], "accepted")

    def test_cycle_bounds_delivery_work(self) -> None:
        core.create_space(self.db, "demo")
        core.register_source(self.db, "mail", "demo", "mail")
        core.create_wait(
            self.db,
            space_id="demo",
            consumer="chat:codex:task-123",
            predicate={"event_type": "message.received"},
        )
        core.create_wait(
            self.db,
            space_id="demo",
            consumer="chat:codex:task-456",
            predicate={"event_type": "message.received"},
        )
        emitted = core.emit_event(
            self.db,
            source_id="mail",
            external_id="message-1",
            event_type="message.received",
            attributes={},
            occurred_at="2026-01-01T00:00:00+00:00",
        )
        relay = self.root / "send-message.py"
        relay.touch()
        calls: list[str] = []

        def delivery_runner(db, delivery_id, **_kwargs):
            calls.append(delivery_id)
            with db.transaction() as connection:
                connection.execute(
                    "UPDATE deliveries SET state='accepted' WHERE id=?", (delivery_id,)
                )
            return {"id": delivery_id, "state": "accepted"}

        result = run_cycle(
            self.db,
            relay=relay,
            cli_command=["switchboard"],
            delivery_batch_size=1,
            delivery_runner=delivery_runner,
        )

        self.assertEqual(calls, [min(emitted["deliveries"])])
        self.assertEqual(len(result["deliveries"]), 1)
        self.assertEqual(len(core.list_deliveries(self.db, "pending")), 1)

    def test_cycle_favors_fresh_consumer_then_rotates_due_retries(self) -> None:
        relay = self.root / "send-message.py"
        relay.touch()
        retry_started = "2026-01-01T00:00:00+00:00"
        processor_ids: list[str] = []
        original_request_ids: dict[str, str] = {}

        for index, created_at in enumerate(
            (
                "2025-12-31T23:50:00+00:00",
                "2025-12-31T23:51:00+00:00",
            )
        ):
            space_id = f"processor-{index}"
            source_id = f"mail-{index}"
            processor = f"triage-{index}"
            core.create_space(self.db, space_id)
            core.register_source(self.db, source_id, space_id, "mail")
            core.create_route(
                self.db,
                space_id=space_id,
                name="triage",
                predicate={"event_type": "message.received"},
                processor=processor,
            )
            core.bind_processor(
                self.db,
                space_id=space_id,
                processor=processor,
                consumer=f"chat:claude:processor-{index}",
            )
            core.emit_event(
                self.db,
                source_id=source_id,
                external_id=f"processor-message-{index}",
                event_type="message.received",
                attributes={},
            )
            delivery = core.list_processor_deliveries(self.db)[0]
            request_material = f"{delivery['idempotency_key']}:{delivery['generation']}"
            request_id = "msg_broker_" + hashlib.sha256(
                request_material.encode()
            ).hexdigest()[:32]
            processor_ids.append(delivery["id"])
            original_request_ids[delivery["id"]] = request_id
            with self.db.transaction() as connection:
                connection.execute(
                    "UPDATE processor_deliveries SET created_at=? WHERE id=?",
                    (created_at, delivery["id"]),
                )
                connection.execute(
                    "INSERT INTO processor_delivery_attempts("
                    "id, delivery_id, request_id, state, started_at, finished_at, detail"
                    ") VALUES(?,?,?,'failed',?,?,?)",
                    (
                        f"seed-attempt-{index}",
                        delivery["id"],
                        request_id,
                        retry_started,
                        retry_started,
                        "offline",
                    ),
                )

        core.create_space(self.db, "fresh-wait")
        core.register_source(self.db, "fresh-mail", "fresh-wait", "mail")
        core.create_wait(
            self.db,
            space_id="fresh-wait",
            consumer="chat:codex:fresh-wait",
            predicate={"event_type": "message.received"},
        )
        fresh = core.emit_event(
            self.db,
            source_id="fresh-mail",
            external_id="fresh-message",
            event_type="message.received",
            attributes={},
        )["deliveries"][0]
        with self.db.transaction() as connection:
            connection.execute(
                "UPDATE deliveries SET created_at=? WHERE id=?",
                ("2026-01-01T00:02:00+00:00", fresh),
            )

        wait_calls: list[str] = []
        processor_calls: list[tuple[str, str]] = []

        def wait_runner(db, delivery_id, **kwargs):
            wait_calls.append(delivery_id)
            return core.dispatch_delivery(
                db,
                delivery_id,
                **kwargs,
                runner=lambda command, **_runner_kwargs: subprocess.CompletedProcess(
                    command, 0, stdout="accepted", stderr=""
                ),
            )

        def failing_processor_runner(db, delivery_id, **kwargs):
            def fail(command, **_runner_kwargs):
                request_id = command[command.index("--request-id") + 1]
                processor_calls.append((delivery_id, request_id))
                return subprocess.CompletedProcess(
                    command, 1, stdout="", stderr="offline"
                )

            return core.dispatch_processor_delivery(
                db, delivery_id, **kwargs, runner=fail
            )

        first_at = datetime.fromisoformat("2026-01-01T00:03:00+00:00")
        with patch("switchboard.core.now", return_value=first_at.isoformat()):
            first = run_cycle(
                self.db,
                relay=relay,
                cli_command=["switchboard"],
                at=first_at,
                delivery_batch_size=1,
                delivery_runner=wait_runner,
                processor_delivery_runner=failing_processor_runner,
            )
        self.assertEqual(wait_calls, [fresh])
        self.assertEqual(first["processor_deliveries"], [])
        self.assertEqual(core.get_delivery(self.db, fresh)["state"], "accepted")

        for offset in (0, 61, 122):
            cycle_at = first_at + timedelta(seconds=offset)
            with patch("switchboard.core.now", return_value=cycle_at.isoformat()):
                run_cycle(
                    self.db,
                    relay=relay,
                    cli_command=["switchboard"],
                    at=cycle_at,
                    delivery_batch_size=1,
                    delivery_runner=wait_runner,
                    processor_delivery_runner=failing_processor_runner,
                )

        self.assertEqual(
            [delivery_id for delivery_id, _request_id in processor_calls],
            [processor_ids[0], processor_ids[1], processor_ids[0]],
        )
        self.assertEqual(
            [
                request_id
                for delivery_id, request_id in processor_calls
                if delivery_id == processor_ids[0]
            ],
            [original_request_ids[processor_ids[0]]] * 2,
        )
        self.assertEqual(len(core.list_deliveries(self.db)), 1)
        self.assertEqual(len(core.list_processor_deliveries(self.db)), 2)

    def test_cycle_selects_oldest_eligible_processor_delivery_for_consumer(self) -> None:
        core.create_space(self.db, "demo")
        core.register_source(self.db, "mail", "demo", "mail")
        core.create_route(
            self.db,
            space_id="demo",
            name="triage",
            predicate={"event_type": "message.received"},
            processor="mail-triage",
        )
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer="chat:claude:session-1",
        )
        for index in range(2):
            core.emit_event(
                self.db,
                source_id="mail",
                external_id=f"message-{index}",
                event_type="message.received",
                attributes={},
            )
        deliveries = core.list_processor_deliveries(self.db)
        with self.db.transaction() as connection:
            connection.execute(
                "UPDATE processor_deliveries SET created_at=? WHERE id=?",
                ("2026-01-01T00:00:00+00:00", deliveries[0]["id"]),
            )
            connection.execute(
                "UPDATE processor_deliveries SET created_at=? WHERE id=?",
                ("2025-12-31T23:00:00+00:00", deliveries[1]["id"]),
            )
        expected = deliveries[1]["id"]
        dispatched: list[str] = []

        run_cycle(
            self.db,
            relay=self.root / "relay.py",
            cli_command=["switchboard"],
            delivery_batch_size=2,
            processor_delivery_runner=lambda _db, delivery_id, **_kwargs: (
                dispatched.append(delivery_id)
                or {"id": delivery_id, "state": "accepted"}
            ),
        )

        self.assertEqual(dispatched, [expected])

    def test_delivery_workers_do_not_block_an_unrelated_consumer(self) -> None:
        core.create_space(self.db, "demo")
        core.register_source(self.db, "mail", "demo", "mail")
        core.create_route(
            self.db,
            space_id="demo",
            name="triage",
            predicate={"event_type": "processor.message"},
            processor="mail-triage",
        )
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer="chat:claude:slow",
        )
        processor_delivery = core.emit_event(
            self.db,
            source_id="mail",
            external_id="processor-message",
            event_type="processor.message",
            attributes={},
        )
        core.create_wait(
            self.db,
            space_id="demo",
            consumer="chat:codex:healthy",
            predicate={"event_type": "wait.message"},
        )
        wait_delivery = core.emit_event(
            self.db,
            source_id="mail",
            external_id="wait-message",
            event_type="wait.message",
            attributes={},
        )["deliveries"][0]
        slow_id = core.get_processor_run(
            self.db, processor_delivery["processor_runs"][0]
        )["delivery"]["id"]
        slow_started = threading.Event()
        release_slow = threading.Event()
        healthy_completed = threading.Event()
        relay = self.root / "relay.py"
        relay.touch()

        def slow_relay(command, **_kwargs):
            slow_started.set()
            release_slow.wait(3)
            return subprocess.CompletedProcess(command, 0, stdout="accepted", stderr="")

        def slow_runner(db, delivery_id, **kwargs):
            return core.dispatch_processor_delivery(
                db, delivery_id, **kwargs, runner=slow_relay
            )

        def healthy_runner(db, delivery_id, **_kwargs):
            with db.transaction() as connection:
                connection.execute(
                    "UPDATE deliveries SET state='accepted' WHERE id=?", (delivery_id,)
                )
            healthy_completed.set()
            return {"id": delivery_id, "state": "accepted"}

        workers = DeliveryWorkers(
            self.db,
            relay=relay,
            cli_command=["switchboard"],
            concurrency=2,
            delivery_batch_size=1,
            delivery_runner=healthy_runner,
            processor_delivery_runner=slow_runner,
        )
        self.addCleanup(lambda: (release_slow.set(), workers.shutdown()))

        workers.poll()
        self.assertTrue(slow_started.wait(1))
        slow_delivery = core.get_processor_delivery(self.db, slow_id)
        self.assertEqual(slow_delivery["state"], "accepted")
        recovery_at = datetime.fromisoformat(slow_delivery["accepted_at"]) + timedelta(
            seconds=121
        )
        recovery = run_cycle(
            self.db,
            relay=None,
            cli_command=["switchboard"],
            at=recovery_at,
            process_schedules=False,
            active_delivery_ids=workers.active_delivery_ids(),
        )
        self.assertEqual(recovery["recovered_unclaimed_processor_deliveries"], 0)
        self.assertEqual(core.get_processor_delivery(self.db, slow_id)["state"], "accepted")
        workers.poll()

        self.assertTrue(healthy_completed.wait(1))
        self.assertFalse(release_slow.is_set())
        self.assertEqual(core.get_delivery(self.db, wait_delivery)["state"], "accepted")

    def test_delivery_workers_serialize_a_consumer_oldest_first(self) -> None:
        core.create_space(self.db, "demo")
        core.register_source(self.db, "mail", "demo", "mail")
        core.create_route(
            self.db,
            space_id="demo",
            name="triage",
            predicate={"event_type": "message.received"},
            processor="mail-triage",
        )
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer="chat:claude:session-1",
        )
        for index in range(2):
            core.emit_event(
                self.db,
                source_id="mail",
                external_id=f"message-{index}",
                event_type="message.received",
                attributes={},
            )
        deliveries = core.list_processor_deliveries(self.db)
        with self.db.transaction() as connection:
            connection.execute(
                "UPDATE processor_deliveries SET created_at=? WHERE id=?",
                ("2026-01-01T00:01:00+00:00", deliveries[0]["id"]),
            )
            connection.execute(
                "UPDATE processor_deliveries SET created_at=? WHERE id=?",
                ("2026-01-01T00:00:00+00:00", deliveries[1]["id"]),
            )
        oldest, newest = deliveries[1]["id"], deliveries[0]["id"]
        release_first = threading.Event()
        second_started = threading.Event()
        calls: list[str] = []
        active = 0
        max_active = 0
        guard = threading.Lock()

        def runner(db, delivery_id, **_kwargs):
            nonlocal active, max_active
            with guard:
                calls.append(delivery_id)
                active += 1
                max_active = max(max_active, active)
                call_number = len(calls)
            if call_number == 1:
                release_first.wait(3)
            else:
                second_started.set()
            with db.transaction() as connection:
                connection.execute(
                    "UPDATE processor_deliveries SET state='acknowledged' WHERE id=?",
                    (delivery_id,),
                )
            with guard:
                active -= 1
            return {"id": delivery_id, "state": "acknowledged"}

        workers = DeliveryWorkers(
            self.db,
            relay=self.root / "relay.py",
            cli_command=["switchboard"],
            concurrency=4,
            delivery_batch_size=4,
            processor_delivery_runner=runner,
        )
        self.addCleanup(lambda: (release_first.set(), workers.shutdown()))

        workers.poll()
        self.wait_until(lambda: calls == [oldest], "oldest processor wake did not start")
        workers.poll()
        workers.poll()
        self.assertEqual(calls, [oldest])

        release_first.set()
        self.wait_until(
            lambda: core.get_processor_delivery(self.db, oldest)["state"] == "acknowledged",
            "oldest processor wake did not finish",
        )
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and not second_started.is_set():
            workers.poll()
            time.sleep(0.01)
        self.assertTrue(second_started.is_set())
        self.assertEqual(calls, [oldest, newest])
        self.assertEqual(max_active, 1)

    def test_delivery_workers_retry_one_logical_wait_with_stable_request_id(self) -> None:
        core.create_space(self.db, "demo")
        core.register_source(self.db, "mail", "demo", "mail")
        core.create_wait(
            self.db,
            space_id="demo",
            consumer="chat:codex:session-1",
            predicate={"event_type": "message.received"},
        )
        delivery_id = core.emit_event(
            self.db,
            source_id="mail",
            external_id="message-1",
            event_type="message.received",
            attributes={},
        )["deliveries"][0]
        relay = self.root / "relay.py"
        relay.touch()
        request_ids: list[str] = []

        def delivery_runner(db, candidate_id, **kwargs):
            def fail(command, **_runner_kwargs):
                request_ids.append(command[command.index("--request-id") + 1])
                return subprocess.CompletedProcess(command, 1, stdout="", stderr="offline")

            return core.dispatch_delivery(db, candidate_id, **kwargs, runner=fail)

        workers = DeliveryWorkers(
            self.db,
            relay=relay,
            cli_command=["switchboard"],
            concurrency=1,
            delivery_batch_size=1,
            delivery_retry_seconds=60,
            delivery_runner=delivery_runner,
        )
        self.addCleanup(workers.shutdown)
        first_at = datetime.fromisoformat("2026-01-01T00:00:00+00:00")
        with patch("switchboard.core.now", return_value=first_at.isoformat()):
            workers.poll(first_at)
            self.wait_until(
                lambda: len(core.get_delivery(self.db, delivery_id)["attempts"]) == 1,
                "first wait attempt did not finish",
            )
            workers.poll(first_at)
        second_at = first_at + timedelta(seconds=61)
        with patch("switchboard.core.now", return_value=second_at.isoformat()):
            workers.poll(second_at)
            self.wait_until(
                lambda: len(core.get_delivery(self.db, delivery_id)["attempts"]) == 2,
                "second wait attempt did not finish",
            )

        self.assertEqual(request_ids, [request_ids[0], request_ids[0]])
        self.assertEqual(len(core.list_deliveries(self.db)), 1)
        self.assertEqual(core.get_delivery(self.db, delivery_id)["state"], "pending")

    def test_delivery_worker_cannot_start_after_shutdown_returns(self) -> None:
        core.create_space(self.db, "demo")
        core.register_source(self.db, "mail", "demo", "mail")
        core.create_wait(
            self.db,
            space_id="demo",
            consumer="chat:codex:session-1",
            predicate={"event_type": "message.received"},
        )
        delivery_id = core.emit_event(
            self.db,
            source_id="mail",
            external_id="message-1",
            event_type="message.received",
            attributes={},
        )["deliveries"][0]
        marker = self.root / "relay-started"
        relay = self.root / "relay.py"
        relay.write_text(
            "from pathlib import Path\n"
            f"Path({str(marker)!r}).write_text('started')\n"
        )
        transaction_entered = threading.Event()
        original_transaction = self.db.transaction

        @contextmanager
        def observed_transaction():
            if threading.current_thread().name.startswith("switchboard-delivery-"):
                transaction_entered.set()
            with original_transaction() as connection:
                yield connection

        self.db.transaction = observed_transaction  # type: ignore[method-assign]
        blocker = self.db.connect()
        blocker.execute("BEGIN IMMEDIATE")
        self.addCleanup(blocker.close)

        workers = DeliveryWorkers(
            self.db,
            relay=relay,
            cli_command=["switchboard"],
            concurrency=1,
            delivery_batch_size=1,
        )
        workers._shutdown_join_seconds = 0.05
        workers.poll()
        self.assertTrue(transaction_entered.wait(1))

        workers.shutdown()
        self.assertEqual(workers.active_delivery_ids(), set())
        self.assertFalse(marker.exists())
        self.assertEqual(core.get_delivery(self.db, delivery_id)["attempts"], [])

        blocker.rollback()
        self.wait_until(
            lambda: not workers._threads[0].is_alive(),
            "cancelled delivery worker did not exit after the database lock cleared",
        )
        self.assertFalse(marker.exists())
        self.assertEqual(core.get_delivery(self.db, delivery_id)["attempts"], [])

    def test_cycle_rearms_and_dispatches_an_unclaimed_accepted_wake(self) -> None:
        core.create_space(self.db, "demo")
        core.register_source(self.db, "mail", "demo", "mail")
        core.create_route(
            self.db,
            space_id="demo",
            name="triage",
            predicate={"event_type": "message.received"},
            processor="mail-triage",
        )
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer="chat:claude:session-1",
        )
        core.emit_event(
            self.db,
            source_id="mail",
            external_id="message-recover",
            event_type="message.received",
            attributes={},
        )
        delivery = core.list_processor_deliveries(self.db)[0]
        at = datetime.now().astimezone()
        old = (at - timedelta(seconds=121)).isoformat()
        with self.db.transaction() as connection:
            connection.execute(
                "UPDATE processor_deliveries SET state='accepted', accepted_at=? WHERE id=?",
                (old, delivery["id"]),
            )
        relay = self.root / "send-message.py"
        relay.touch()
        dispatched: list[str] = []

        def processor_delivery_runner(db, delivery_id, **_kwargs):
            dispatched.append(delivery_id)
            with db.transaction() as connection:
                connection.execute(
                    "UPDATE processor_deliveries SET state='accepted', accepted_at=? WHERE id=?",
                    (at.isoformat(), delivery_id),
                )
            return {"id": delivery_id, "state": "accepted"}

        result = run_cycle(
            self.db,
            relay=relay,
            cli_command=["switchboard"],
            at=at,
            processor_delivery_runner=processor_delivery_runner,
        )

        self.assertEqual(result["recovered_unclaimed_processor_deliveries"], 1)
        self.assertEqual(dispatched, [delivery["id"]])

    def test_stalled_processor_alert_and_recovery_are_each_sent_once(self) -> None:
        core.create_space(self.db, "demo")
        core.register_source(self.db, "mail", "demo", "mail")
        core.create_route(
            self.db,
            space_id="demo",
            name="triage",
            predicate={"event_type": "message.received"},
            processor="mail-triage",
        )
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer="chat:claude:session-1",
        )
        emitted = core.emit_event(
            self.db,
            source_id="mail",
            external_id="message-alert",
            event_type="message.received",
            attributes={},
        )
        delivery = core.list_processor_deliveries(self.db)[0]
        at = datetime.now().astimezone()
        old = (at - timedelta(minutes=16)).isoformat()
        with self.db.transaction() as connection:
            connection.execute(
                "UPDATE processor_deliveries SET state='accepted', accepted_at=? WHERE id=?",
                (old, delivery["id"]),
            )
        alerts: list[dict] = []

        def alert_runner(_command, payload):
            alerts.append(payload)
            return {"sent": True}

        first = run_cycle(
            self.db,
            relay=None,
            cli_command=["switchboard"],
            at=at,
            alert_command=["/alert"],
            alert_runner=alert_runner,
            accepted_retry_seconds=3600,
        )
        second = run_cycle(
            self.db,
            relay=None,
            cli_command=["switchboard"],
            at=at,
            alert_command=["/alert"],
            alert_runner=alert_runner,
            accepted_retry_seconds=3600,
        )
        core.claim_processor_run(
            self.db, emitted["processor_runs"][0], worker="chat:claude:session-1"
        )
        recovered = run_cycle(
            self.db,
            relay=None,
            cli_command=["switchboard"],
            at=at,
            alert_command=["/alert"],
            alert_runner=alert_runner,
            accepted_retry_seconds=3600,
        )

        self.assertEqual(first["alerts"][0]["state"], "notified")
        self.assertEqual(second["alerts"], [])
        self.assertEqual(recovered["alerts"][0]["state"], "recovery-notified")
        self.assertEqual([item["kind"] for item in alerts], [
            "processor-unreachable",
            "processor-recovered",
        ])

    def test_multiple_failed_deliveries_share_one_consumer_alert_episode(self) -> None:
        core.create_space(self.db, "demo")
        core.register_source(self.db, "mail", "demo", "mail")
        core.create_route(
            self.db, space_id="demo", name="triage",
            predicate={"event_type": "message.received"}, processor="mail-triage",
        )
        core.bind_processor(
            self.db, space_id="demo", processor="mail-triage",
            consumer="chat:claude:session-1",
        )
        for index in range(3):
            core.emit_event(
                self.db, source_id="mail", external_id=f"alert-{index}",
                event_type="message.received", attributes={},
            )
        at = datetime.now().astimezone()
        old = (at - timedelta(minutes=20)).isoformat()
        deliveries = core.list_processor_deliveries(self.db)
        with self.db.transaction() as connection:
            for index, delivery in enumerate(deliveries):
                request_material = f"{delivery['idempotency_key']}:{delivery['generation']}"
                import hashlib
                request_id = "msg_broker_" + hashlib.sha256(request_material.encode()).hexdigest()[:32]
                connection.execute(
                    "INSERT INTO processor_delivery_attempts("
                    "id, delivery_id, request_id, state, started_at, finished_at, detail"
                    ") VALUES(?,?,?,'failed',?,?,?)",
                    (f"attempt-{index}", delivery["id"], request_id, old, old, "offline"),
                )
        sent: list[dict] = []
        first = process_processor_alerts(
            self.db, at=at, alert_after_seconds=900, alert_command=["/alert"],
            alert_runner=lambda _command, payload: sent.append(payload) or {"sent": True},
        )
        second = process_processor_alerts(
            self.db, at=at, alert_after_seconds=900, alert_command=["/alert"],
            alert_runner=lambda _command, payload: sent.append(payload) or {"sent": True},
        )

        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])
        self.assertEqual(sent[0]["affected_delivery_count"], 3)
        self.assertEqual(len(core.list_processor_alerts(self.db, "open")), 1)

        with self.db.transaction() as connection:
            connection.execute("UPDATE processor_deliveries SET state='acknowledged'")
        recovered = process_processor_alerts(
            self.db, at=at, alert_after_seconds=900, alert_command=["/alert"],
            alert_runner=lambda _command, payload: sent.append(payload) or {"sent": True},
        )
        repeated = process_processor_alerts(
            self.db, at=at, alert_after_seconds=900, alert_command=["/alert"],
            alert_runner=lambda _command, payload: sent.append(payload) or {"sent": True},
        )
        self.assertEqual(len(recovered), 1)
        self.assertEqual(repeated, [])
        self.assertEqual([item["kind"] for item in sent], ["processor-unreachable", "processor-recovered"])

    def test_failed_alert_command_is_claimed_before_invocation_and_not_retried(self) -> None:
        core.create_space(self.db, "demo")
        core.register_source(self.db, "mail", "demo", "mail")
        core.create_route(
            self.db, space_id="demo", name="triage",
            predicate={"event_type": "message.received"}, processor="mail-triage",
        )
        core.bind_processor(
            self.db, space_id="demo", processor="mail-triage",
            consumer="chat:claude:session-1",
        )
        core.emit_event(
            self.db, source_id="mail", external_id="failed-alert",
            event_type="message.received", attributes={},
        )
        delivery = core.list_processor_deliveries(self.db)[0]
        at = datetime.now().astimezone()
        old = (at - timedelta(minutes=20)).isoformat()
        with self.db.transaction() as connection:
            connection.execute(
                "UPDATE processor_deliveries SET state='accepted', accepted_at=? WHERE id=?",
                (old, delivery["id"]),
            )
        calls = 0

        def fail(_command, _payload):
            nonlocal calls
            calls += 1
            raise ValueError("notification offline")

        first = process_processor_alerts(
            self.db, at=at, alert_after_seconds=900, alert_command=["/alert"], alert_runner=fail,
        )
        second = process_processor_alerts(
            self.db, at=at, alert_after_seconds=900, alert_command=["/alert"], alert_runner=fail,
        )
        self.assertEqual(first[0]["state"], "failed")
        self.assertEqual(second, [])
        self.assertEqual(calls, 1)
        self.assertEqual(core.list_processor_alerts(self.db, "open")[0]["notification_error"], "notification offline")

    def test_requeued_generation_ignores_previous_delivery_failure(self) -> None:
        core.create_space(self.db, "demo")
        core.register_source(self.db, "mail", "demo", "mail")
        core.create_route(
            self.db,
            space_id="demo",
            name="triage",
            predicate={"event_type": "message.received"},
            processor="mail-triage",
        )
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer="chat:claude:session-1",
        )
        emitted = core.emit_event(
            self.db,
            source_id="mail",
            external_id="message-retry",
            event_type="message.received",
            attributes={},
        )
        run_id = emitted["processor_runs"][0]
        delivery = core.list_processor_deliveries(self.db)[0]
        old = (datetime.now().astimezone() - timedelta(minutes=20)).isoformat()
        with self.db.transaction() as connection:
            connection.execute(
                "INSERT INTO processor_delivery_attempts(id, delivery_id, request_id, state, "
                "started_at, finished_at, detail) VALUES(?,?,?,?,?,?,?)",
                (
                    "pdattempt_old",
                    delivery["id"],
                    "msg_broker_previous_generation",
                    "failed",
                    old,
                    old,
                    "old failure",
                ),
            )
        core.claim_processor_run(self.db, run_id, worker="chat:claude:session-1")
        core.finish_processor_run(
            self.db,
            run_id,
            state="needs-review",
            worker="chat:claude:session-1",
        )
        core.resolve_processor_review(self.db, run_id, resolution="retry")

        current = core.get_processor_delivery(self.db, delivery["id"])
        self.assertEqual(len(current["attempts"]), 1)
        self.assertEqual(current["current_attempts"], [])
        result = run_cycle(
            self.db,
            relay=None,
            cli_command=["switchboard"],
            at=datetime.now().astimezone(),
            alert_command=["/alert"],
            alert_runner=lambda *_args: {"sent": True},
        )
        self.assertEqual(result["alerts"], [])

    def test_cycle_sends_only_one_wake_per_consumer_during_large_backlog(self) -> None:
        core.create_space(self.db, "demo")
        core.register_source(self.db, "mail", "demo", "mail")
        core.create_route(
            self.db,
            space_id="demo",
            name="triage",
            predicate={"event_type": "message.received"},
            processor="mail-triage",
        )
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer="chat:claude:session-1",
        )
        for index in range(20):
            core.emit_event(
                self.db,
                source_id="mail",
                external_id=f"message-{index}",
                event_type="message.received",
                attributes={},
            )
        dispatched: list[str] = []

        result = run_cycle(
            self.db,
            relay=self.root / "relay.py",
            cli_command=["switchboard"],
            delivery_batch_size=20,
            processor_delivery_runner=lambda _db, delivery_id, **_kwargs: (
                dispatched.append(delivery_id) or {"id": delivery_id, "state": "accepted"}
            ),
        )

        self.assertEqual(len(dispatched), 1)
        self.assertEqual(len(result["processor_deliveries"]), 1)


if __name__ == "__main__":
    unittest.main()
