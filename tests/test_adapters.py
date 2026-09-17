from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

from switchboard import core
from switchboard.adapters import AdapterError, apply_snapshot, run_ingest_shadow
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
