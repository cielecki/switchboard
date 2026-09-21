from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from switchboard import core
from switchboard.adapters import (
    AdapterError,
    apply_snapshot,
    run_inbound_leads,
    run_ingest_shadow,
)
from switchboard.adapters.inbound import snapshot_from_pending
from switchboard.adapters.ingest import snapshot_from_rows
from switchboard.db import Database


class AdapterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.db = Database(self.root / "switchboard.sqlite3")

    def test_ingest_snapshot_discovers_sources_and_is_idempotent(self) -> None:
        rows = [
            {
                "id": "capture-one",
                "ingest_id": "fathom:meeting-1",
                "source": "fathom",
                "title": "Planning",
                "date": "2026-09-17T08:00:00+00:00",
                "status": "needs-routing",
                "routing_note": None,
                "attention_reason": None,
                "mean_confidence": None,
            },
            {
                "id": "capture-two",
                "ingest_id": "manual:note-1",
                "source": "manual",
                "title": "Note",
                "date": "2026-09-17T09:00:00+00:00",
                "status": "routed",
                "routing_note": "filed",
                "attention_reason": None,
                "mean_confidence": None,
            },
        ]
        first = apply_snapshot(self.db, snapshot_from_rows(rows))
        second = apply_snapshot(self.db, snapshot_from_rows(rows))

        self.assertEqual(first["created_sources"], 2)
        self.assertEqual(first["run"]["emitted_events"], 2)
        self.assertEqual(second["run"]["deduplicated_events"], 2)
        self.assertEqual({source["id"] for source in core.list_sources(self.db)}, {"ingest/fathom", "ingest/manual"})
        self.assertEqual(len(core.list_events(self.db)), 2)

    def test_ingest_shadow_refuses_stale_status(self) -> None:
        status_script = self.root / "status.py"
        status_script.touch()

        def runner(*_args, **_kwargs):
            return subprocess.CompletedProcess([], 3, stdout="[]", stderr="mirror stale")

        with self.assertRaisesRegex(AdapterError, "stale"):
            run_ingest_shadow(self.db, status_script=status_script, runner=runner)

        runs = core.list_adapter_runs(self.db)
        self.assertEqual(runs[0]["state"], "failed")
        self.assertIn("mirror stale", runs[0]["detail"])

    def test_ingest_discovery_is_bounded_and_runs_before_status(self) -> None:
        discovery_script = self.root / "watch.py"
        status_script = self.root / "status.py"
        discovery_script.touch()
        status_script.touch()
        calls: list[list[str]] = []

        def runner(command, **kwargs):
            calls.append(command)
            if command[1] == str(discovery_script.resolve()):
                self.assertEqual(command[-1], "--monitor-events")
                self.assertEqual(kwargs["env"]["MAX_POLLS"], "1")
                self.assertEqual(kwargs["env"]["STOP_AT"], "")
                return subprocess.CompletedProcess(command, 0, stdout="INGEST\t{}\n", stderr="")
            return subprocess.CompletedProcess(command, 0, stdout="[]", stderr="")

        result = run_ingest_shadow(
            self.db,
            status_script=status_script,
            discovery_script=discovery_script,
            runner=runner,
        )

        self.assertEqual(calls[0][1], str(discovery_script.resolve()))
        self.assertEqual(calls[1][1], str(status_script.resolve()))
        self.assertEqual(result["discovery"], {"enabled": True, "event_lines": 1})

    def test_inbound_snapshot_keeps_only_pointer_and_structured_state(self) -> None:
        snapshot = snapshot_from_pending(
            [
                {
                    "id": "nina:message-1",
                    "profile": "nina",
                    "internal_date": "1789902000000",
                    "sender": "Private Person <person@example.com>",
                    "subject": "Confidential opportunity",
                    "verdict": None,
                    "completed": False,
                }
            ],
            profile="nina",
            space_id="nina-inbound",
        )

        result = apply_snapshot(self.db, snapshot)
        event = result["events"][0]["event"]

        self.assertEqual(event["attributes"]["pointer"], "nina:message-1")
        self.assertNotIn("sender", event["attributes"])
        self.assertNotIn("subject", event["attributes"])
        self.assertEqual(event["event_type"], "inbound.lead.pending")

    def test_inbound_slack_snapshot_keeps_pointers_without_message_content(self) -> None:
        snapshot = snapshot_from_pending(
            [],
            profile="nina",
            space_id="nina-inbound",
            slack_lines=["MENTION\t123.4\t120.0\tU123\tprivate message text"],
        )

        result = apply_snapshot(self.db, snapshot)
        event = result["events"][0]["event"]

        self.assertEqual(event["event_type"], "inbound.slack.mention")
        self.assertEqual(event["attributes"]["pointer"], "123.4")
        self.assertEqual(event["attributes"]["thread_root"], "120.0")
        self.assertNotIn("text", event["attributes"])
        self.assertNotIn("user_id", event["attributes"])

    def test_inbound_slack_discovery_is_bounded(self) -> None:
        ledger_script = self.root / "ledger.py"
        slack_script = self.root / "watch-slack.sh"
        ledger_script.touch()
        slack_script.touch()

        def runner(command, **kwargs):
            if command[0] == "bash":
                self.assertEqual(kwargs["env"]["MAX_POLLS"], "1")
                self.assertEqual(kwargs["env"]["SET_STATUS"], "0")
                return subprocess.CompletedProcess(
                    command,
                    0,
                    stdout="MENTION\t123.4\t120.0\tU123\tprivate text\n",
                    stderr="",
                )
            return subprocess.CompletedProcess(command, 0, stdout="[]", stderr="")

        result = run_inbound_leads(
            self.db,
            ledger_script=ledger_script,
            profile="nina",
            slack_discovery_script=slack_script,
            runner=runner,
        )

        self.assertEqual(result["discovery"]["slack"]["mention_lines"], 1)
        event = result["events"][0]["event"]
        self.assertEqual(event["event_type"], "inbound.slack.mention")

    def test_slack_only_mode_does_not_read_gmail_ledger(self) -> None:
        ledger_script = self.root / "ledger.py"
        slack_script = self.root / "watch-slack.sh"
        ledger_script.touch()
        slack_script.touch()
        calls: list[list[str]] = []

        def runner(command, **_kwargs):
            calls.append(command)
            return subprocess.CompletedProcess(
                command,
                0,
                stdout="MENTION\t123.4\t120.0\tU123\tprivate text\n",
                stderr="",
            )

        run_inbound_leads(
            self.db,
            ledger_script=ledger_script,
            profile="nina",
            slack_discovery_script=slack_script,
            source_mode="slack",
            runner=runner,
        )

        self.assertEqual(calls, [["bash", str(slack_script.resolve())]])

    def test_inbound_validates_ledger_before_advancing_slack_cursor(self) -> None:
        ledger_script = self.root / "ledger.py"
        slack_script = self.root / "watch-slack.sh"
        ledger_script.touch()
        slack_script.touch()
        slack_called = False

        def runner(command, **_kwargs):
            nonlocal slack_called
            if command[0] == "bash":
                slack_called = True
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
            return subprocess.CompletedProcess(command, 0, stdout="not-json", stderr="")

        with self.assertRaises(json.JSONDecodeError):
            run_inbound_leads(
                self.db,
                ledger_script=ledger_script,
                profile="nina",
                slack_discovery_script=slack_script,
                runner=runner,
            )

        self.assertFalse(slack_called)

    def test_snapshot_rejects_event_from_undiscovered_source(self) -> None:
        snapshot = {
            "adapter": "example",
            "space": {"id": "demo"},
            "sources": [],
            "events": [
                {
                    "source_id": "missing",
                    "external_id": "1",
                    "event_type": "example.created",
                    "attributes": {},
                }
            ],
        }
        with self.assertRaisesRegex(AdapterError, "undiscovered"):
            apply_snapshot(self.db, snapshot)


if __name__ == "__main__":
    unittest.main()
