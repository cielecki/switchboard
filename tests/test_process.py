from __future__ import annotations

import os
import subprocess
import sys
import time
import unittest

from switchboard.process import run_bounded


class ProcessTest(unittest.TestCase):
    def test_timeout_kills_descendant_process_group(self) -> None:
        command = [
            sys.executable,
            "-c",
            (
                "import subprocess,sys,time; "
                "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
                "print(child.pid, flush=True); time.sleep(60)"
            ),
        ]
        started = time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired) as raised:
            run_bounded(command, capture_output=True, text=True, timeout=0.2)
        self.assertLess(time.monotonic() - started, 3)
        child_pid = int((raised.exception.output or "").strip())
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            try:
                os.kill(child_pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.05)
        else:
            self.fail("descendant survived the parent timeout")


if __name__ == "__main__":
    unittest.main()
