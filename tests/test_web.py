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
        core.create_space(self.db, "test-space", "Test space")
        core.register_source(self.db, "test-source", "test-space", "test")
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
        with urllib.request.urlopen(self.base_url) as response:
            html = response.read().decode()
        self.assertIn('<h2>Spaces</h2>', html)
        self.assertIn('<h2>Sources</h2>', html)

        with urllib.request.urlopen(f"{self.base_url}/api/spaces") as response:
            spaces = json.load(response)
        self.assertEqual(spaces[0]["id"], "test-space")

        with urllib.request.urlopen(f"{self.base_url}/api/sources") as response:
            sources = json.load(response)
        self.assertEqual(sources[0]["id"], "test-source")

        with urllib.request.urlopen(f"{self.base_url}/api/adapters") as response:
            runs = json.load(response)
        self.assertEqual(runs[0]["adapter"], "test-adapter")

        with urllib.request.urlopen(f"{self.base_url}/api/schedules") as response:
            schedules = json.load(response)
        self.assertEqual(schedules, [])

        with urllib.request.urlopen(f"{self.base_url}/api/routes") as response:
            routes = json.load(response)
        self.assertEqual(routes, [])

        with urllib.request.urlopen(f"{self.base_url}/api/processors") as response:
            processors = json.load(response)
        self.assertEqual(processors, [])

        with urllib.request.urlopen(f"{self.base_url}/api/processor-bindings") as response:
            bindings = json.load(response)
        self.assertEqual(bindings, [])

        with urllib.request.urlopen(f"{self.base_url}/api/processor-deliveries") as response:
            processor_deliveries = json.load(response)
        self.assertEqual(processor_deliveries, [])

        with urllib.request.urlopen(f"{self.base_url}/api/processor-alerts") as response:
            processor_alerts = json.load(response)
        self.assertEqual(processor_alerts, [])

        request = urllib.request.Request(f"{self.base_url}/api/waits", data=b"{}", method="POST")
        with self.assertRaises(urllib.error.HTTPError) as raised:
            urllib.request.urlopen(request)
        self.assertEqual(raised.exception.code, 405)


if __name__ == "__main__":
    unittest.main()
