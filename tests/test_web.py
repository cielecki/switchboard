from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

from switchboard import core
from switchboard.db import Database
from switchboard.web import handler_for


class WebTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db = Database(Path(self.directory.name) / "switchboard.sqlite3")
        core.start_adapter_run(self.db, "test-adapter")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler_for(self.db))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self._stop_server)
        host, port = self.server.server_address
        self.base_url = f"http://{host}:{port}"

    def _stop_server(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_adapter_runs_are_visible_and_web_mutation_is_rejected(self) -> None:
        with urllib.request.urlopen(f"{self.base_url}/api/adapters") as response:
            runs = json.load(response)
        self.assertEqual(runs[0]["adapter"], "test-adapter")

        request = urllib.request.Request(f"{self.base_url}/api/waits", data=b"{}", method="POST")
        with self.assertRaises(urllib.error.HTTPError) as raised:
            urllib.request.urlopen(request)
        self.assertEqual(raised.exception.code, 405)


if __name__ == "__main__":
    unittest.main()
