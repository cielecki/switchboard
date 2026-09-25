from __future__ import annotations

import tempfile
import threading
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from switchboard import core
from switchboard.db import Database


class ProcessorDeliveryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.db = Database(self.root / "switchboard.sqlite3")
        core.create_space(self.db, "demo")
        core.register_source(self.db, "mail", "demo", "mail")
        core.create_route(
            self.db,
            space_id="demo",
            name="triage",
            predicate={"event_type": "message.received"},
            processor="mail-triage",
        )

    def emit(self, external_id: str = "message-1") -> str:
        emitted = core.emit_event(
            self.db,
            source_id="mail",
            external_id=external_id,
            event_type="message.received",
            attributes={"pointer": external_id},
        )
        return emitted["processor_runs"][0]

    def test_binding_backfills_pending_runs_and_new_runs(self) -> None:
        first = self.emit()
        binding = core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer="chat:claude:session-1",
            activate_inactive=True,
            lease_seconds=120,
            label="Inbox worker",
            url="claude://resume/session-1",
        )
        second = self.emit("message-2")

        deliveries = core.list_processor_deliveries(self.db)

        self.assertEqual({item["processor_run_id"] for item in deliveries}, {first, second})
        self.assertTrue(binding["activate_inactive"])
        self.assertEqual(binding["lease_seconds"], 120)
        self.assertEqual(binding["label"], "Inbox worker")
        self.assertEqual(binding["url"], "claude://resume/session-1")

    def test_shared_review_group_resolves_all_linked_runs(self) -> None:
        worker = "chat:claude:session-1"
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer=worker,
        )
        run_ids = [self.emit("message-1"), self.emit("message-2")]
        for run_id in run_ids:
            core.claim_processor_run(self.db, run_id, worker=worker)
            core.finish_processor_run(
                self.db,
                run_id,
                state="needs-review",
                worker=worker,
                summary="Choose one policy",
                review_key="task_shared-policy",
                review_title="Choose routing policy",
                review_url="claude://resume/decision",
            )

        groups = core.list_review_groups(self.db, state="open")
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["run_count"], 2)
        self.assertEqual(groups[0]["open_run_count"], 2)
        self.assertEqual(groups[0]["title"], "Choose routing policy")
        resolved = core.resolve_review_group(
            self.db,
            groups[0]["id"],
            resolution="retry",
            decision={"choice": "route-a"},
        )

        self.assertEqual(resolved["state"], "resolved")
        self.assertEqual(resolved["resolution"]["affected_runs"], 2)
        for run_id in run_ids:
            run = core.get_processor_run(self.db, run_id)
            self.assertEqual(run["state"], "pending")
            self.assertEqual(run["delivery"]["state"], "pending")
            self.assertEqual(run["decision"]["review"]["choice"], "route-a")

    def test_review_title_keeps_periods_in_dates(self) -> None:
        run_id = self.emit()
        core.finish_processor_run(
            self.db,
            run_id,
            state="needs-review",
            summary="Route the 14.09 recording",
            review_key="dated-review",
            review_title="Route the 14.09 recording",
        )

        group = core.list_review_groups(self.db, state="open")[0]
        self.assertEqual(group["title"], "Route the 14.09 recording")

    def test_claim_release_and_expiry_requeue_with_new_generation(self) -> None:
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer="chat:claude:session-1",
            lease_seconds=60,
        )
        run_id = self.emit()
        claimed = core.claim_processor_run(self.db, run_id, worker="chat:claude:session-1")
        self.assertEqual(claimed["state"], "running")
        self.assertEqual(claimed["attempts"][-1]["state"], "running")

        released = core.release_processor_run(
            self.db, run_id, worker="chat:claude:session-1", reason="handoff"
        )
        self.assertEqual(released["state"], "pending")
        self.assertEqual(released["delivery"]["generation"], 1)

        core.claim_processor_run(
            self.db, run_id, worker="chat:claude:session-1", lease_seconds=1
        )
        recovered = core.recover_expired_processor_attempts(
            self.db, (datetime.now(UTC) + timedelta(seconds=2)).isoformat()
        )
        self.assertEqual(recovered, 1)
        self.assertEqual(core.get_processor_run(self.db, run_id)["state"], "pending")
        self.assertEqual(core.get_processor_run(self.db, run_id)["delivery"]["generation"], 2)

    def test_claim_accepts_delivery_atomically(self) -> None:
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer="chat:claude:session-1",
        )
        run_id = self.emit()

        claimed = core.claim_processor_run(
            self.db, run_id, worker="chat:claude:session-1"
        )

        self.assertEqual(claimed["state"], "running")
        self.assertEqual(claimed["delivery"]["state"], "accepted")

        other = self.emit("message-other")
        with self.assertRaisesRegex(ValueError, "active processor run"):
            core.claim_processor_run(
                self.db, other, worker="chat:claude:session-1"
            )

    def test_claim_rejects_non_owner(self) -> None:
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer="chat:claude:session-1",
        )
        run_id = self.emit()

        with self.assertRaisesRegex(ValueError, "not bound to worker"):
            core.claim_processor_run(
                self.db, run_id, worker="chat:claude:wrong-session"
            )

    def test_claim_next_drains_oldest_one_at_a_time_after_outage(self) -> None:
        worker = "chat:claude:session-1"
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer=worker,
        )
        first = self.emit("message-1")
        second = self.emit("message-2")

        claimed = core.claim_next_processor_run(self.db, worker=worker)
        self.assertEqual(claimed["id"], first)
        self.assertEqual(claimed["delivery"]["state"], "accepted")
        with self.assertRaisesRegex(ValueError, "already has an active processor run"):
            core.claim_next_processor_run(self.db, worker=worker)

        core.finish_processor_run(self.db, first, state="completed", worker=worker)
        next_claimed = core.claim_next_processor_run(self.db, worker=worker)
        self.assertEqual(next_claimed["id"], second)

    def test_claim_next_returns_none_for_empty_consumer_queue(self) -> None:
        self.assertIsNone(
            core.claim_next_processor_run(
                self.db, worker="chat:claude:unbound-session"
            )
        )

    def test_terminal_outcome_requires_owner_and_acknowledges_delivery(self) -> None:
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer="chat:claude:session-1",
        )
        run_id = self.emit()
        core.claim_processor_run(self.db, run_id, worker="chat:claude:session-1")

        with self.assertRaisesRegex(ValueError, "active processor claim"):
            core.finish_processor_run(
                self.db,
                run_id,
                state="completed",
                worker="chat:claude:wrong-session",
            )

        completed = core.finish_processor_run(
            self.db,
            run_id,
            state="completed",
            worker="chat:claude:session-1",
            summary="Handled",
            facts={"verified": True},
            decision={"verdict": "accepted"},
            actions=[{"kind": "record", "state": "done"}],
        )
        self.assertEqual(completed["state"], "completed")
        self.assertEqual(completed["attempts"][-1]["state"], "completed")
        self.assertEqual(completed["delivery"]["state"], "acknowledged")

    def test_review_resolution_records_decision_and_can_resume(self) -> None:
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer="chat:claude:session-1",
        )
        run_id = self.emit()
        core.claim_processor_run(self.db, run_id, worker="chat:claude:session-1")
        core.finish_processor_run(
            self.db,
            run_id,
            state="needs-review",
            worker="chat:claude:session-1",
            summary="Pick destination",
            decision={"outcome": "needs-review"},
        )

        resumed = core.resolve_processor_review(
            self.db,
            run_id,
            resolution="retry",
            decision={"choice": "sales"},
            actions=[{"kind": "human-decision", "state": "recorded"}],
        )

        self.assertEqual(resumed["state"], "pending")
        self.assertEqual(resumed["decision"]["review"]["choice"], "sales")
        self.assertEqual(resumed["delivery"]["state"], "pending")
        self.assertEqual(resumed["delivery"]["generation"], 1)
        core.claim_processor_run(self.db, run_id, worker="chat:claude:session-1")
        completed = core.finish_processor_run(
            self.db,
            run_id,
            state="completed",
            worker="chat:claude:session-1",
            decision={"outcome": "filed"},
        )
        self.assertEqual(completed["decision"]["review"]["choice"], "sales")

    def test_processor_list_can_filter_and_report_space(self) -> None:
        run_id = self.emit()
        rows = core.list_processor_runs(self.db, space_id="demo")

        self.assertEqual([row["id"] for row in rows], [run_id])
        self.assertEqual(rows[0]["space_id"], "demo")
        self.assertEqual(core.list_processor_runs(self.db, space_id="other"), [])

    def test_dispatch_uses_generation_stable_request_id_and_activation(self) -> None:
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer="chat:claude:session-1",
            activate_inactive=True,
        )
        run_id = self.emit()
        delivery = core.list_processor_deliveries(self.db)[0]
        relay = self.root / "send-message.py"
        relay.touch()
        calls: list[list[str]] = []

        def runner(command, **_kwargs):
            calls.append(command)
            return SimpleNamespace(returncode=0, stdout="accepted", stderr="")

        result = core.dispatch_processor_delivery(
            self.db,
            delivery["id"],
            relay=relay,
            cli_command=["switchboard"],
            runner=runner,
        )

        self.assertEqual(result["state"], "accepted")
        self.assertIn("--activate-if-inactive", calls[0])
        message = calls[0][calls[0].index("--message") + 1]
        self.assertIn("processor claim-next", message)
        self.assertIn("--worker chat:claude:session-1", message)
        repeated = core.dispatch_processor_delivery(
            self.db,
            delivery["id"],
            relay=relay,
            cli_command=["switchboard"],
            runner=runner,
        )
        self.assertTrue(repeated["idempotent"])
        self.assertEqual(len(calls), 1)

    def codex_delivery(self) -> tuple[dict, Path]:
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer="chat:codex:thread-1",
        )
        self.emit()
        relay = self.root / "send-message.py"
        relay.touch()
        return core.list_processor_deliveries(self.db)[0], relay

    def test_codex_dispatch_always_activates_and_records_started_turn(self) -> None:
        delivery, relay = self.codex_delivery()
        calls: list[list[str]] = []
        timeouts: list[int] = []

        def runner(command, **kwargs):
            calls.append(command)
            timeouts.append(kwargs["timeout"])
            return SimpleNamespace(
                returncode=0,
                stdout=(
                    '{"status":"delivered","delivery_status":"delivered",'
                    '"activation":"started-turn"}'
                ),
                stderr="",
            )

        result = core.dispatch_processor_delivery(
            self.db, delivery["id"], relay=relay, cli_command=["switchboard"], runner=runner
        )

        self.assertEqual(result["state"], "accepted")
        self.assertEqual(result["relay_outcome"], "delivered")
        self.assertEqual(calls[0][calls[0].index("--client") + 1], "codex")
        self.assertIn("--activate-if-inactive", calls[0])
        self.assertEqual(timeouts, [2 * 30 + 25])

    def test_codex_plain_queue_is_not_delivery(self) -> None:
        delivery, relay = self.codex_delivery()

        with self.assertRaisesRegex(ValueError, r"relay exited 0 \(queued\)"):
            core.dispatch_processor_delivery(
                self.db,
                delivery["id"],
                relay=relay,
                cli_command=["switchboard"],
                runner=lambda *_args, **_kwargs: SimpleNamespace(
                    returncode=0,
                    stdout='{"status":"queued","delivery_status":"queued"}',
                    stderr="",
                ),
            )

        stored = core.get_processor_delivery(self.db, delivery["id"])
        self.assertEqual(stored["state"], "pending")
        self.assertIn("queued", stored["last_error"])

    def test_codex_loaded_busy_is_accepted_not_delivered(self) -> None:
        delivery, relay = self.codex_delivery()

        result = core.dispatch_processor_delivery(
            self.db,
            delivery["id"],
            relay=relay,
            cli_command=["switchboard"],
            runner=lambda *_args, **_kwargs: SimpleNamespace(
                returncode=0,
                stdout=(
                    '{"status":"queued","delivery_status":"queued",'
                    '"activation":"loaded-busy"}'
                ),
                stderr="",
            ),
        )

        self.assertEqual(result["state"], "accepted")
        self.assertEqual(result["relay_outcome"], "accepted")

    def test_codex_pending_activation_retries_with_same_request_id(self) -> None:
        delivery, relay = self.codex_delivery()
        calls: list[list[str]] = []

        def runner(command, **_kwargs):
            calls.append(command)
            if len(calls) == 1:
                return SimpleNamespace(
                    returncode=2,
                    stdout=(
                        '{"status":"queued","delivery_status":"queued",'
                        '"activation":"pending"}'
                    ),
                    stderr="",
                )
            return SimpleNamespace(
                returncode=0,
                stdout=(
                    '{"status":"delivered","delivery_status":"delivered",'
                    '"activation":"started-turn","idempotent_replay":true}'
                ),
                stderr="",
            )

        with self.assertRaisesRegex(ValueError, r"relay exited 2 \(failed\)"):
            core.dispatch_processor_delivery(
                self.db, delivery["id"], relay=relay, cli_command=["switchboard"], runner=runner
            )
        self.assertEqual(core.get_processor_delivery(self.db, delivery["id"])["state"], "pending")
        result = core.dispatch_processor_delivery(
            self.db, delivery["id"], relay=relay, cli_command=["switchboard"], runner=runner
        )

        self.assertEqual(result["relay_outcome"], "delivered")
        self.assertEqual(calls[0], calls[1])

    def test_relay_outcome_classifies_codex_results(self) -> None:
        cases = [
            (0, "accepted", "delivered"),
            (0, '{"delivery_status":"delivered","activation":"started-turn"}', "delivered"),
            (0, '{"delivery_status":"delivered","activation":"already-drained"}', "delivered"),
            (0, '{"status":"queued","delivery_status":"queued"}', "queued"),
            (0, '{"delivery_status":"queued","activation":"loaded-busy"}', "accepted"),
            (2, '{"delivery_status":"queued","activation":"pending"}', "failed"),
            (2, '{"delivery_status":"queued","activation":"failed"}', "failed"),
            (2, '{"delivery_status":"queued","activation":"loaded-busy"}', "failed"),
            (2, "not json", "failed"),
        ]
        for returncode, stdout, expected in cases:
            with self.subTest(stdout=stdout, returncode=returncode):
                result = SimpleNamespace(returncode=returncode, stdout=stdout, stderr="")
                self.assertEqual(core._relay_outcome(result), expected)

    def test_only_one_delivery_is_dispatched_per_consumer(self) -> None:
        worker = "chat:claude:session-1"
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer=worker,
        )
        self.emit("message-1")
        self.emit("message-2")
        deliveries = list(reversed(core.list_processor_deliveries(self.db)))
        relay = self.root / "send-message.py"
        relay.touch()
        runner = lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0, stdout="accepted", stderr=""
        )

        core.dispatch_processor_delivery(
            self.db,
            deliveries[0]["id"],
            relay=relay,
            cli_command=["switchboard"],
            runner=runner,
        )
        with self.assertRaisesRegex(ValueError, "already has in-flight work"):
            core.dispatch_processor_delivery(
                self.db,
                deliveries[1]["id"],
                relay=relay,
                cli_command=["switchboard"],
                runner=runner,
            )

        consumers = core.list_processor_consumers(self.db)
        self.assertEqual(consumers[0]["status"], "waiting-for-claim")
        self.assertEqual(consumers[0]["backlog"], 2)

    def test_concurrent_dispatch_reserves_consumer_before_calling_relay(self) -> None:
        worker = "chat:claude:session-1"
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer=worker,
        )
        self.emit("message-1")
        self.emit("message-2")
        deliveries = list(reversed(core.list_processor_deliveries(self.db)))
        relay = self.root / "send-message.py"
        relay.touch()
        first_entered_relay = threading.Event()
        release_first = threading.Event()
        relay_calls: list[str] = []
        outcomes: list[object] = []

        def runner(command, **_kwargs):
            relay_calls.append(command[command.index("--request-id") + 1])
            first_entered_relay.set()
            release_first.wait(2)
            return SimpleNamespace(returncode=0, stdout="accepted", stderr="")

        def dispatch(delivery_id: str) -> None:
            try:
                outcomes.append(
                    core.dispatch_processor_delivery(
                        self.db,
                        delivery_id,
                        relay=relay,
                        cli_command=["switchboard"],
                        runner=runner,
                    )
                )
            except Exception as exc:  # noqa: BLE001 - asserted below
                outcomes.append(exc)

        first = threading.Thread(target=dispatch, args=(deliveries[0]["id"],))
        first.start()
        self.assertTrue(first_entered_relay.wait(1))
        second = threading.Thread(target=dispatch, args=(deliveries[1]["id"],))
        second.start()
        second.join(1)
        release_first.set()
        first.join(1)

        self.assertEqual(len(relay_calls), 1)
        self.assertEqual(len(outcomes), 2)
        errors = [item for item in outcomes if isinstance(item, Exception)]
        self.assertEqual(len(errors), 1)
        self.assertIn("already has in-flight work", str(errors[0]))

    def test_upgrade_coalesces_old_accepted_wakes_into_one(self) -> None:
        worker = "chat:claude:session-1"
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer=worker,
        )
        for index in range(20):
            self.emit(f"legacy-message-{index}")
        with self.db.transaction() as connection:
            connection.execute(
                "UPDATE processor_deliveries SET state='accepted', accepted_at=created_at"
            )

        changed = core.coalesce_processor_deliveries(self.db, worker)
        states = [item["state"] for item in core.list_processor_deliveries(self.db)]

        self.assertEqual(changed, 19)
        self.assertEqual(states.count("accepted"), 1)
        self.assertEqual(states.count("pending"), 19)
        pending = core.list_processor_deliveries(self.db, "pending")
        self.assertTrue(all(item["generation"] == 1 for item in pending))

    def test_dispatch_accepts_broker_timeout_after_delivery_acceptance(self) -> None:
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer="chat:claude:session-1",
        )
        self.emit()
        delivery = core.list_processor_deliveries(self.db)[0]
        relay = self.root / "send-message.py"
        relay.touch()

        result = core.dispatch_processor_delivery(
            self.db,
            delivery["id"],
            relay=relay,
            cli_command=["switchboard"],
            runner=lambda *_args, **_kwargs: SimpleNamespace(
                returncode=2,
                stdout='{"status":"timeout","delivery_status":"accepted"}',
                stderr="",
            ),
        )

        self.assertEqual(result["state"], "accepted")
        current = core.get_processor_delivery(self.db, delivery["id"])
        self.assertEqual(current["state"], "accepted")
        self.assertEqual(current["attempts"][-1]["state"], "accepted")

    def test_unclaimed_accepted_wake_retries_with_the_same_request_id(self) -> None:
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer="chat:claude:session-1",
        )
        self.emit()
        delivery = core.list_processor_deliveries(self.db)[0]
        relay = self.root / "send-message.py"
        relay.touch()
        commands: list[list[str]] = []

        def accepted_runner(command, **_kwargs):
            commands.append(command)
            return SimpleNamespace(
                returncode=2,
                stdout=(
                    '{"status":"timeout","delivery_status":"accepted",'
                    '"receipt_status":null}'
                ),
                stderr="",
            )

        first = core.dispatch_processor_delivery(
            self.db,
            delivery["id"],
            relay=relay,
            cli_command=["switchboard"],
            runner=accepted_runner,
        )
        accepted_at = datetime.fromisoformat(
            core.get_processor_delivery(self.db, delivery["id"])["accepted_at"]
        )
        recovered = core.recover_unclaimed_processor_deliveries(
            self.db,
            (accepted_at + timedelta(seconds=121)).isoformat(),
            after_seconds=120,
        )
        current = core.get_processor_delivery(self.db, delivery["id"])
        self.assertEqual(recovered, 1)
        self.assertEqual(current["state"], "pending")
        self.assertEqual(current["generation"], 0)

        second = core.dispatch_processor_delivery(
            self.db,
            delivery["id"],
            relay=relay,
            cli_command=["switchboard"],
            runner=accepted_runner,
        )
        self.assertEqual(first["request_id"], second["request_id"])
        self.assertEqual(commands[0], commands[1])

    def test_held_receipt_remains_pending_for_retry(self) -> None:
        core.bind_processor(
            self.db,
            space_id="demo",
            processor="mail-triage",
            consumer="chat:claude:session-1",
        )
        self.emit()
        delivery = core.list_processor_deliveries(self.db)[0]
        relay = self.root / "send-message.py"
        relay.touch()

        with self.assertRaisesRegex(ValueError, "relay exited 2"):
            core.dispatch_processor_delivery(
                self.db,
                delivery["id"],
                relay=relay,
                cli_command=["switchboard"],
                runner=lambda *_args, **_kwargs: SimpleNamespace(
                    returncode=2,
                    stdout=(
                        '{"status":"held","delivery_status":"accepted",'
                        '"receipt_status":"held"}'
                    ),
                    stderr="",
                ),
            )
        current = core.get_processor_delivery(self.db, delivery["id"])
        self.assertEqual(current["state"], "pending")
        self.assertEqual(current["attempts"][-1]["state"], "failed")


if __name__ == "__main__":
    unittest.main()
