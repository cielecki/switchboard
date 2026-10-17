from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
import unittest

from switchboard.process import ProcessStartCancelled, run_bounded


class ProcessTest(unittest.TestCase):
    def test_cancelled_process_does_not_spawn(self) -> None:
        cancelled = threading.Event()
        cancelled.set()

        with self.assertRaises(ProcessStartCancelled):
            run_bounded(
                [sys.executable, "-c", "raise SystemExit('must not run')"],
                cancel_event=cancelled,
            )

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

    @unittest.skipUnless(os.name == "posix", "POSIX process-group cleanup")
    def test_timeout_kills_pipe_holder_after_parent_already_exited(self) -> None:
        command = [sys.executable, "-c", (
            "import subprocess,sys; "
            "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
            "print(child.pid, flush=True)"
        )]
        started = time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired) as raised:
            run_bounded(command, capture_output=True, text=True, timeout=0.2)
        self.assertLess(time.monotonic() - started, 3)
        child_pid = int(raised.exception.output.strip())
        # A killed orphan can briefly be a zombie on Linux; it cannot hold pipes
        # or perform work, so do not confuse that with a surviving descendant.
        state = subprocess.run(["ps", "-p", str(child_pid), "-o", "stat="],
                               capture_output=True, text=True).stdout.strip()
        self.assertTrue(not state or state.startswith("Z"), state)


if __name__ == "__main__":
    unittest.main()
