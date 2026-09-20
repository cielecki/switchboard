from __future__ import annotations

import plistlib
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from switchboard.db import Database
from switchboard.service import install_launch_agent, uninstall_launch_agent


class ServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.db = Database(self.root / "switchboard.sqlite3")
        self.launcher = self.root / "switchboard"
        self.launcher.touch()
        self.relay = self.root / "send-message.py"
        self.relay.touch()
        self.plist = self.root / "io.github.cielecki.switchboard.plist"
        self.commands: list[list[str]] = []

    def runner(self, command, **_kwargs):
        self.commands.append(command)
        if command[1] == "print" and not self.plist.exists():
            return subprocess.CompletedProcess(command, 113, stdout="", stderr="not found")
        return subprocess.CompletedProcess(command, 0, stdout="loaded", stderr="")

    def test_install_and_uninstall_launch_agent(self) -> None:
        with (
            patch("switchboard.service.sys.platform", "darwin"),
            patch("switchboard.service.launch_agent_path", return_value=self.plist),
        ):
            installed = install_launch_agent(
                self.db,
                cli_command=[str(self.launcher)],
                relay=self.relay,
                port=9876,
                runner=self.runner,
            )
            payload = plistlib.loads(self.plist.read_bytes())
            self.assertTrue(installed["loaded"])
            self.assertIn("supervisor", payload["ProgramArguments"])
            self.assertIn("9876", payload["ProgramArguments"])
            self.assertIn("bootstrap", [command[1] for command in self.commands])

            removed = uninstall_launch_agent(runner=self.runner)
            self.assertTrue(removed["removed"])
            self.assertFalse(self.plist.exists())


if __name__ == "__main__":
    unittest.main()
