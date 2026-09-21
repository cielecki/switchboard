from __future__ import annotations

import tempfile
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
        )
        second = self.emit("message-2")

        deliveries = core.list_processor_deliveries(self.db)

        self.assertEqual({item["processor_run_id"] for item in deliveries}, {first, second})
        self.assertTrue(binding["activate_inactive"])
        self.assertEqual(binding["lease_seconds"], 120)

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
        self.assertIn(f"processor claim {run_id}", message)
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


if __name__ == "__main__":
    unittest.main()
