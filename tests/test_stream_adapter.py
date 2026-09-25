from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

from switchboard.adapters.stream import run_stream_command
from switchboard.db import Database


class StreamAdapterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.db = Database(self.root / "switchboard.sqlite3")

    def script(self, lines: list[str], *, exit_code: int = 0) -> Path:
        path = self.root / "stream.py"
        path.write_text(
            "import sys\n"
            f"lines = {lines!r}\n"
            "for line in lines:\n"
            "    print(line, flush=True)\n"
            f"raise SystemExit({exit_code})\n",
            encoding="utf-8",
        )
        return path

    def snapshot(self, external_id: str, *, state: str = "ready") -> str:
        return json.dumps(
            {
                "adapter": "fixture-stream",
                "space": {"id": "fixture", "name": "Fixture"},
                "sources": [
                    {
                        "id": "fixture/socket",
                        "kind": "fixture.socket",
                        "state": state,
                        "detail": "connected",
                    }
                ],
                "events": [] if state != "ready" else [
                    {
                        "source_id": "fixture/socket",
                        "external_id": external_id,
                        "event_type": "fixture.message",
                        "attributes": {"pointer": external_id},
                    }
                ],
            }
        )

    def test_applies_each_snapshot_and_deduplicates_events(self) -> None:
        script = self.script([self.snapshot("1"), self.snapshot("1"), self.snapshot("2")])

        result = run_stream_command(self.db, command=[sys.executable, str(script)])

        self.assertEqual(result["run"]["state"], "completed")
        self.assertEqual(result["run"]["emitted_events"], 2)
        self.assertEqual(result["run"]["deduplicated_events"], 1)
        self.assertEqual(len(self.db.rows("SELECT * FROM events")), 2)
        source = self.db.row("SELECT state FROM sources WHERE id='fixture/socket'")
        self.assertEqual(source["state"], "ready")

    def test_invalid_line_fails_the_stream_run(self) -> None:
        script = self.script(["not-json"])

        with self.assertRaisesRegex(ValueError, "line 1"):
            run_stream_command(self.db, command=[sys.executable, str(script)])

        run = self.db.row("SELECT * FROM adapter_runs ORDER BY started_at DESC LIMIT 1")
        self.assertEqual(run["state"], "failed")

    def test_nonzero_exit_preserves_stderr_diagnostic(self) -> None:
        script = self.root / "failed.py"
        script.write_text("import sys\nprint('socket failed', file=sys.stderr)\nraise SystemExit(7)\n")

        with self.assertRaisesRegex(ValueError, "socket failed"):
            run_stream_command(self.db, command=[sys.executable, str(script)])


if __name__ == "__main__":
    unittest.main()
