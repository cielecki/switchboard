from __future__ import annotations

import json
import tomllib
import unittest
from pathlib import Path

from switchboard import __version__


class ManifestTest(unittest.TestCase):
    def test_dual_host_manifests_are_aligned(self) -> None:
        root = Path(__file__).parents[1]
        plugin = root / "plugins" / "switchboard"
        codex = json.loads((plugin / ".codex-plugin" / "plugin.json").read_text())
        claude = json.loads((plugin / ".claude-plugin" / "plugin.json").read_text())
        codex_marketplace = json.loads(
            (root / ".agents" / "plugins" / "marketplace.json").read_text()
        )
        claude_marketplace = json.loads(
            (root / ".claude-plugin" / "marketplace.json").read_text()
        )
        pyproject = tomllib.loads((root / "pyproject.toml").read_text())
        lockfile = tomllib.loads((root / "uv.lock").read_text())
        locked_package = next(
            package
            for package in lockfile["package"]
            if package["name"] == "agent-switchboard"
        )

        self.assertEqual(codex["name"], "switchboard")
        self.assertEqual(claude["name"], "switchboard")
        self.assertEqual(pyproject["project"]["version"], __version__)
        self.assertEqual(locked_package["version"], __version__)
        self.assertEqual(codex["version"], __version__)
        self.assertEqual(claude["version"], __version__)
        self.assertEqual(claude_marketplace["metadata"]["version"], __version__)
        self.assertEqual(claude_marketplace["plugins"][0]["version"], __version__)
        self.assertEqual(
            codex_marketplace["plugins"][0]["source"]["path"], "./plugins/switchboard"
        )
        self.assertEqual(
            claude_marketplace["plugins"][0]["source"], "./plugins/switchboard"
        )
        self.assertTrue((plugin / codex["skills"]).is_dir())
        self.assertTrue((plugin / "bin" / "switchboard").is_file())

    def test_delivery_capacity_is_documented_on_operator_surfaces(self) -> None:
        root = Path(__file__).parents[1]
        paths = [
            root / "README.md",
            root / "plugins" / "switchboard" / "docs" / "operations.md",
            root / "plugins" / "switchboard" / "docs" / "supervisor.md",
        ]

        for path in paths:
            with self.subTest(path=path):
                contents = path.read_text()
                self.assertIn("--delivery-batch", contents)
                self.assertIn("--delivery-concurrency", contents)
                self.assertIn("read-only", contents)


if __name__ == "__main__":
    unittest.main()
