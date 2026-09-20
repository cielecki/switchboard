from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from switchboard import core
from switchboard.db import Database
from switchboard.supervisor import run_cycle, run_once


class SupervisorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.db = Database(self.root / "switchboard.sqlite3")
        self.status_script = self.root / "status.py"
        self.status_script.touch()

    def test_due_schedule_runs_once_and_advances(self) -> None:
        created = core.upsert_ingest_schedule(
            self.db,
            "ingest",
            status_script=self.status_script,
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
        self.assertEqual(first["schedules"][0]["state"], "completed")
        self.assertEqual(second["schedules"], [])
        schedule = core.get_schedule(self.db, "ingest")
        self.assertEqual(schedule["last_state"], "completed")
        self.assertEqual(
            datetime.fromisoformat(schedule["next_run_at"]),
            at.replace(microsecond=at.microsecond) + timedelta(seconds=60),
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
        ledger_script.touch()
        created = core.upsert_inbound_schedule(
            self.db,
            "nina-inbound",
            ledger_script=ledger_script,
            profile="nina",
            every_seconds=120,
            space_id="nina-inbound",
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


if __name__ == "__main__":
    unittest.main()
