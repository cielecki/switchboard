from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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

    def test_gmail_poll_uses_one_attempt_inside_outer_deadline(self) -> None:
        ledger = self.root / "ledger.py"
        watcher = self.root / "watch.sh"
        ledger.touch()
        watcher.touch()
        for requested, outer, expected in [("", 240, 180), ("580", 240, 210),
                                            ("180", 4, 3)]:
            with self.subTest(requested=requested, outer=outer):
                def runner(command, **kwargs):
                    if command[0] == "bash":
                        env = kwargs["env"]
                        self.assertEqual(float(env["GMAIL_TIMEOUT"]), expected)
                        self.assertEqual(env["GMAIL_POLL_RETRIES"], "1")
                        self.assertEqual(env["GMAIL_RETRY_SLEEP"], "0")
                        self.assertEqual(env["MAX_POLLS"], "1")
                        self.assertLess(float(env["GMAIL_TIMEOUT"]), kwargs["timeout"])
                        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
                    return subprocess.CompletedProcess(command, 0, stdout="[]", stderr="")

                with patch.dict("os.environ", {"GMAIL_TIMEOUT": requested,
                                               "GMAIL_POLL_RETRIES": "3",
                                               "GMAIL_RETRY_SLEEP": "20"}):
                    run_inbound_leads(self.db, ledger_script=ledger, profile="fixture",
                                      discovery_script=watcher, source_mode="gmail",
                                      timeout=outer, runner=runner)

    def test_gmail_invalid_budgets_fail_without_starting_discovery(self) -> None:
        ledger = self.root / "ledger.py"
        watcher = self.root / "watch.sh"
        ledger.touch()
        watcher.touch()
        for requested, outer in [("bad", 240), ("nan", 240), ("inf", 240),
                                 ("0", 240), ("-1", 240), ("180", 0),
                                 ("180", float("nan")), ("180", float("inf"))]:
            with self.subTest(requested=requested, outer=outer):
                with patch.dict("os.environ", {"GMAIL_TIMEOUT": requested}):
                    with patch("switchboard.adapters.inbound.run_bounded") as runner:
                        with self.assertRaisesRegex(AdapterError, "finite positive"):
                            run_inbound_leads(self.db, ledger_script=ledger, profile="fixture",
                                              discovery_script=watcher, timeout=outer,
                                              runner=runner)
                        runner.assert_not_called()
                self.assertEqual(core.list_adapter_runs(self.db)[0]["state"], "failed")

    def test_failed_gmail_attempt_preserves_safe_diagnostics_then_next_poll_recovers(self) -> None:
        ledger = self.root / "ledger.py"
        watcher = self.root / "watch.sh"
        ledger.touch()
        watcher.touch()
        attempt = 0

        def runner(command, **kwargs):
            nonlocal attempt
            if command[0] == "bash":
                attempt += 1
                self.assertEqual(kwargs["env"]["GMAIL_POLL_RETRIES"], "1")
                if attempt == 1:
                    return subprocess.CompletedProcess(command, 1, stdout="private mail body",
                        stderr="TimeoutError: password=synthetic-secret\n"
                               "inbound-gmail-watch: Gmail search failed 1 times in a row; stopping")
                return subprocess.CompletedProcess(command, 0, stdout="",
                    stderr="INBOUND_GMAIL_PHASE\tsearch\t0\n"
                           "INBOUND_GMAIL_PHASE\tledger-discover\t2\n"
                           "INBOUND_GMAIL_PHASE\tcomplete\t3\npassword=synthetic-secret")
            return subprocess.CompletedProcess(command, 0, stdout='[{"id":"fixture:one",'
                '"profile":"fixture","internal_date":"1789902000000"}]', stderr="")

        with patch.dict("os.environ", {"GMAIL_TIMEOUT": "180"}):
            with self.assertRaisesRegex(AdapterError, "phase=gmail-search.*TimeoutError"):
                run_inbound_leads(self.db, ledger_script=ledger, profile="fixture",
                                  discovery_script=watcher, runner=runner)
            failed = core.list_adapter_runs(self.db)[0]
            self.assertEqual(failed["state"], "failed")
            self.assertEqual(failed["emitted_events"], 0)
            self.assertNotIn("synthetic-secret", failed["detail"])
            self.assertNotIn("private mail", failed["detail"])
            recovered = run_inbound_leads(self.db, ledger_script=ledger, profile="fixture",
                                         discovery_script=watcher, runner=runner)
        self.assertEqual(attempt, 2)
        self.assertEqual(recovered["run"]["state"], "completed")
        self.assertEqual(recovered["run"]["emitted_events"], 1)
        health = next(source for source in core.list_sources(self.db)
                      if source["id"] == "inbound/fixture")["health"]
        self.assertIn("ledger-discover@2s", health["detail"])
        self.assertNotIn("synthetic-secret", health["detail"])

    def test_gmail_outer_timeout_keeps_bytes_diagnostic_categories_only(self) -> None:
        ledger = self.root / "ledger.py"
        watcher = self.root / "watch.sh"
        ledger.touch()
        watcher.touch()

        def runner(command, **kwargs):
            raise subprocess.TimeoutExpired(command, kwargs["timeout"], output="private body",
                stderr=b"INBOUND_GMAIL_PHASE\tsearch\t0\n"
                       b"INBOUND_GMAIL_PHASE\tledger-discover\t151\n"
                       b"INBOUND_GMAIL_PHASE\tpassword=synthetic-secret\t152\n"
                       b"command timed out after 180s: --token=synthetic-secret\nTimeoutError: secret")

        with patch.dict("os.environ", {"GMAIL_TIMEOUT": "180"}):
            with self.assertRaisesRegex(AdapterError, "outcome=timeout.*search-timeout") as raised:
                run_inbound_leads(self.db, ledger_script=ledger, profile="fixture",
                                  discovery_script=watcher, runner=runner)
        self.assertNotIn("synthetic-secret", str(raised.exception))
        self.assertNotIn("private body", str(raised.exception))
        self.assertIn("phase=ledger-discover; phase_started_after=151s", str(raised.exception))
        self.assertEqual(core.list_adapter_runs(self.db)[0]["state"], "failed")

    def test_gmail_start_failure_keeps_exception_type_without_private_path(self) -> None:
        ledger = self.root / "ledger.py"
        watcher = self.root / "watch.sh"
        ledger.touch()
        watcher.touch()

        def runner(*_args, **_kwargs):
            raise PermissionError("/private/synthetic-secret/command")

        with self.assertRaisesRegex(AdapterError, "outcome=start-error.*PermissionError") as raised:
            run_inbound_leads(self.db, ledger_script=ledger, profile="fixture",
                              discovery_script=watcher, runner=runner)
        self.assertNotIn("synthetic-secret", str(raised.exception))

    def test_gmail_ledger_read_gets_only_remaining_poll_budget(self) -> None:
        ledger = self.root / "ledger.py"
        watcher = self.root / "watch.sh"
        ledger.touch()
        watcher.touch()
        calls = []

        def runner(command, **kwargs):
            calls.append(command)
            if command[0] == "bash":
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
            self.assertEqual(kwargs["timeout"], 40)
            return subprocess.CompletedProcess(command, 0, stdout="[]", stderr="")

        with patch("switchboard.adapters.inbound.time.monotonic", side_effect=[100, 300]):
            run_inbound_leads(self.db, ledger_script=ledger, profile="fixture",
                              discovery_script=watcher, timeout=240, runner=runner)
        self.assertEqual(len(calls), 2)

        calls.clear()
        with patch("switchboard.adapters.inbound.time.monotonic", side_effect=[100, 341]):
            with self.assertRaisesRegex(AdapterError, "phase=ledger-pending.*budget-exhausted"):
                run_inbound_leads(self.db, ledger_script=ledger, profile="fixture",
                                  discovery_script=watcher, timeout=240, runner=runner)
        self.assertEqual(len(calls), 1)
        self.assertEqual(core.list_adapter_runs(self.db)[0]["state"], "failed")

    def test_gmail_pending_failures_never_persist_rows_or_raw_error_text(self) -> None:
        ledger = self.root / "ledger.py"
        watcher = self.root / "watch.sh"
        ledger.touch()
        watcher.touch()
        for scenario in ["exit", "timeout", "json", "schema"]:
            with self.subTest(scenario=scenario):
                def runner(command, **kwargs):
                    if command[0] == "bash":
                        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
                    if scenario == "timeout":
                        raise subprocess.TimeoutExpired(command, kwargs["timeout"],
                            output="synthetic-private-row", stderr=b"TimeoutError: synthetic-private-row")
                    payload = '{broken-json synthetic-private-row' if scenario == "json" else (
                        '[{"id":"one","profile":"synthetic-private-row"}]')
                    return subprocess.CompletedProcess(command, 1 if scenario == "exit" else 0,
                        stdout=payload, stderr="PermissionError: synthetic-private-row")

                with self.assertRaises(AdapterError) as raised:
                    run_inbound_leads(self.db, ledger_script=ledger, profile="fixture",
                                      discovery_script=watcher, runner=runner)
                self.assertIn("phase=ledger-pending", str(raised.exception))
                self.assertNotIn("synthetic-private-row", str(raised.exception))
                run = core.list_adapter_runs(self.db)[0]
                self.assertEqual(run["state"], "failed")
                self.assertEqual(run["emitted_events"], 0)
                self.assertNotIn("synthetic-private-row", run["detail"])

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
