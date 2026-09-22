from __future__ import annotations

import json
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

        self.assertEqual(codex["name"], "switchboard")
        self.assertEqual(claude["name"], "switchboard")
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


if __name__ == "__main__":
    unittest.main()
