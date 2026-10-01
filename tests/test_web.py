from __future__ import annotations

import json
import re
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

from switchboard import core
from switchboard.db import Database
from switchboard.web import HTML, handler_for


def contrast_ratio(foreground: str, background: str) -> float:
    def luminance(color: str) -> float:
        channels = [int(color[index : index + 2], 16) / 255 for index in (1, 3, 5)]
        linear = [
            channel / 12.92
            if channel <= 0.04045
            else ((channel + 0.055) / 1.055) ** 2.4
            for channel in channels
        ]
        return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]

    lighter, darker = sorted(
        (luminance(foreground), luminance(background)), reverse=True
    )
    return (lighter + 0.05) / (darker + 0.05)


class WebTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db = Database(Path(self.directory.name) / "switchboard.sqlite3")
        core.create_space(self.db, "test-space", "Test space")
        core.register_source(self.db, "test-source", "test-space", "timer")
        core.upsert_calendar_schedule(
            self.db,
            "daily",
            space_id="test-space",
            source_id="test-source",
            event_type="daily.due",
            local_time="07:00",
            timezone="Europe/Warsaw",
            at="2026-09-24T04:00:00+00:00",
        )
        with self.db.transaction() as connection:
            connection.execute(
                "UPDATE adapter_schedules SET last_state='failed', failure_streak=1, "
                "last_failure_at='2026-10-01T08:00:00+00:00', "
                "last_failure_detail='timer unavailable', "
                "retry_not_before='2026-10-01T08:00:30+00:00' WHERE id='daily'"
            )
            connection.execute(
                "INSERT INTO schedule_alert_episodes("
                "id,schedule_id,state,opened_at,updated_at,failure_count,"
                "last_failure_at,detail) VALUES("
                "'salert-web','daily','open','2026-10-01T08:00:00+00:00',"
                "'2026-10-01T08:00:00+00:00',1,'2026-10-01T08:00:00+00:00',"
                "'timer unavailable')"
            )
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
        self.assertIn('<h2>Needs attention</h2>', html)
        self.assertIn('<h2>Source health</h2>', html)
        self.assertIn('<summary>Technical inventory</summary>', html)
        self.assertIn('Schedule retry · ${esc(s.id)}', html)
        self.assertIn("'retry_state'", html)
        self.assertIn("'next_retry_at'", html)
        self.assertIn('.table-scroll { max-width:100%; overflow-x:auto;', html)
        self.assertIn(
            '.col-token code,.col-state code,.col-time code { white-space:nowrap;', html
        )
        self.assertIn("column==='id'||column.endsWith('_id')", html)
        self.assertIn('.col-detail code { display:block; white-space:normal;', html)
        self.assertIn(
            'return \'<div class="table-scroll" role="region" tabindex="0"><table>', html
        )
        self.assertIn("if(value===null||value===undefined||value==='')", html)
        self.assertIn('<span class="placeholder">—</span>', html)
        self.assertIn("...(r.error==null?{}:{error:r.error})", html)
        self.assertIn("JSON.stringify(runOutcome(r),null,2)", html)
        self.assertNotIn("fetch(url,{method:'POST'", html)

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
        self.assertEqual(schedules[0]["schedule_kind"], "calendar")
        self.assertEqual(schedules[0]["next_run_local"], "2026-09-24T07:00:00+02:00")
        self.assertEqual(schedules[0]["retry"]["state"], "active")
        self.assertEqual(schedules[0]["retry"]["failure_streak"], 1)
        self.assertEqual(
            schedules[0]["retry"]["next_retry_at"], "2026-10-01T08:00:30+00:00"
        )
        self.assertEqual(schedules[0]["retry"]["episode"]["state"], "open")
        self.assertIsNone(schedules[0]["last_error"])

        with urllib.request.urlopen(f"{self.base_url}/api/routes") as response:
            routes = json.load(response)
        self.assertEqual(routes, [])

        with urllib.request.urlopen(f"{self.base_url}/api/processors") as response:
            processors = json.load(response)
        self.assertEqual(processors, [])

        with urllib.request.urlopen(f"{self.base_url}/api/reviews?state=open") as response:
            reviews = json.load(response)
        self.assertEqual(reviews, [])

        with urllib.request.urlopen(f"{self.base_url}/api/processor-bindings") as response:
            bindings = json.load(response)
        self.assertEqual(bindings, [])

        with urllib.request.urlopen(f"{self.base_url}/api/processor-consumers") as response:
            consumers = json.load(response)
        self.assertEqual(consumers, [])

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

    def test_muted_and_placeholder_text_meet_normal_text_contrast(self) -> None:
        surface = re.search(
            r"\.metric,\.card,section,details \{[^}]*background:(#[0-9a-f]{6});", HTML
        )
        self.assertIsNotNone(surface)
        assert surface is not None
        for selector in ("muted", "placeholder", "empty"):
            match = re.search(rf"\.{selector} \{{ color:(#[0-9a-f]{{6}});", HTML)
            self.assertIsNotNone(match, selector)
            assert match is not None
            self.assertGreaterEqual(
                contrast_ratio(match.group(1), surface.group(1)),
                4.5,
                selector,
            )

    def test_completed_run_api_keeps_null_error_for_presentation_to_omit(self) -> None:
        core.create_route(
            self.db,
            space_id="test-space",
            name="daily processor",
            priority=10,
            predicate={"event_type": "daily.ready"},
            processor="daily-worker",
        )
        emitted = core.emit_event(
            self.db,
            source_id="test-source",
            external_id="daily-ready-1",
            event_type="daily.ready",
            attributes={},
        )
        run = core.start_processor_run(self.db, emitted["processor_runs"][0])
        core.finish_processor_run(
            self.db,
            run["id"],
            state="completed",
            summary="Daily work completed.",
        )

        with urllib.request.urlopen(f"{self.base_url}/api/processors/{run['id']}") as response:
            presented = json.load(response)

        self.assertEqual(presented["state"], "completed")
        self.assertIn("error", presented)
        self.assertIsNone(presented["error"])


if __name__ == "__main__":
    unittest.main()
