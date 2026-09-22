from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from switchboard import core
from switchboard.db import Database
from switchboard.topology import (
    apply_topology,
    export_topology,
    load_topology,
    plan_topology,
)


class TopologyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.db = Database(self.root / "switchboard.sqlite3")
        self.document = {
            "schema_version": 1,
            "owner": "example",
            "spaces": [{"id": "demo", "name": "Demo"}],
            "sources": [
                {"id": "timer/demo", "space": "demo", "kind": "timer", "config": {}}
            ],
            "routes": [
                {
                    "key": "maintenance",
                    "space": "demo",
                    "name": "Maintenance",
                    "predicate": {
                        "source_id": "timer/demo",
                        "event_type": "maintenance.due",
                    },
                    "processor": "maintenance",
                    "priority": 10,
                }
            ],
            "schedules": [
                {
                    "id": "daily",
                    "adapter": "timer",
                    "every_seconds": 86400,
                    "config": {
                        "space_id": "demo",
                        "source_id": "timer/demo",
                        "event_type": "maintenance.due",
                    },
                }
            ],
            "bindings": [
                {
                    "key": "maintenance-worker",
                    "space": "demo",
                    "processor": "maintenance",
                    "consumer": "chat:claude:session-1",
                    "lease_seconds": 900,
                }
            ],
        }

    def test_apply_is_idempotent_and_updates_only_owned_resources(self) -> None:
        first = apply_topology(self.db, self.document)
        second = apply_topology(self.db, self.document)
        self.assertEqual(first["conflicts"], 0)
        self.assertTrue(all(item["action"] == "noop" for item in second["operations"]))

        changed = json.loads(json.dumps(self.document))
        changed["routes"][0]["priority"] = 20
        plan = plan_topology(self.db, changed)
        self.assertEqual(
            [item["action"] for item in plan["operations"]].count("update"), 1
        )
        apply_topology(self.db, changed)
        self.assertEqual(core.list_routes(self.db)[0]["priority"], 20)

    def test_unmanaged_difference_is_a_conflict(self) -> None:
        core.create_space(self.db, "demo", "Other")
        plan = plan_topology(self.db, self.document)
        self.assertEqual(plan["conflicts"], 1)
        with self.assertRaisesRegex(ValueError, "unmanaged resources"):
            apply_topology(self.db, self.document)

    def test_export_redacts_paths_and_consumers(self) -> None:
        apply_topology(self.db, self.document)
        script = self.root / "status.py"
        script.touch()
        core.upsert_ingest_schedule(
            self.db,
            "ingest",
            status_script=script,
            every_seconds=60,
            space_id="demo",
        )
        exported = export_topology(self.db)
        encoded = json.dumps(exported)
        self.assertNotIn(str(script), encoded)
        self.assertNotIn("chat:claude:session-1", encoded)
        self.assertIn("${PATH_1}", encoded)
        self.assertIn("${CONSUMER_1}", encoded)

        private = json.dumps(export_topology(self.db, include_local_values=True))
        self.assertIn(str(script), private)
        self.assertIn("chat:claude:session-1", private)

    def test_loader_resolves_variables_and_rejects_missing_values(self) -> None:
        document = json.loads(json.dumps(self.document))
        document["bindings"][0]["consumer"] = "${WORKER}"
        path = self.root / "topology.json"
        path.write_text(json.dumps(document))
        loaded = load_topology(path, {"WORKER": "chat:codex:task-1"})
        self.assertEqual(loaded["bindings"][0]["consumer"], "chat:codex:task-1")
        with self.assertRaisesRegex(ValueError, "missing topology variables"):
            load_topology(path, {})

    def test_prune_disables_missing_managed_resources_but_retains_space(self) -> None:
        apply_topology(self.db, self.document)
        empty = {"schema_version": 1, "owner": "example"}
        result = apply_topology(self.db, empty, prune=True)
        actions = {
            (item["resource_type"], item["action"]) for item in result["operations"]
        }
        self.assertIn(("space", "retain"), actions)
        self.assertEqual(core.list_routes(self.db)[0]["state"], "disabled")
        self.assertFalse(core.list_schedules(self.db)[0]["enabled"])
        self.assertEqual(core.list_processor_bindings(self.db)[0]["state"], "disabled")
        self.assertEqual(core.list_sources(self.db)[0]["state"], "disabled")


if __name__ == "__main__":
    unittest.main()
