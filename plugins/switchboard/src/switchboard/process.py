from __future__ import annotations

import os
import signal
import subprocess
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

_active_lock = threading.Lock()
_active_process_groups: set[int] = set()


def _terminate_process_group(process: subprocess.Popen[Any]) -> None:
    if process.poll() is not None:
        return
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        else:  # pragma: no cover - Switchboard's service host is macOS
            process.kill()
    except ProcessLookupError:
        pass


def run_bounded(
    command: Sequence[str],
    *,
    capture_output: bool = False,
    text: bool = False,
    timeout: float | None = None,
    env: dict[str, str] | None = None,
    input: str | bytes | None = None,
    check: bool = False,
) -> subprocess.CompletedProcess[Any]:
    """Run a command and terminate its whole descendant process group on timeout."""
    if input is not None and not capture_output:
        raise ValueError("input requires captured subprocess streams")
    process = subprocess.Popen(
        list(command),
        stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE if capture_output else None,
        stderr=subprocess.PIPE if capture_output else None,
        text=text,
        env=env,
        start_new_session=os.name == "posix",
    )
    with _active_lock:
        _active_process_groups.add(process.pid)
    try:
        try:
            stdout, stderr = process.communicate(input=input, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            _terminate_process_group(process)
            stdout, stderr = process.communicate()
            exc.output = stdout
            exc.stderr = stderr
            raise
    finally:
        with _active_lock:
            _active_process_groups.discard(process.pid)
    result = subprocess.CompletedProcess(list(command), process.returncode, stdout, stderr)
    if check:
        result.check_returncode()
    return result


def terminate_active_process_groups() -> None:
    """Best-effort cleanup for commands still running when the daemon stops."""
    with _active_lock:
        process_groups = list(_active_process_groups)
    for process_group in process_groups:
        try:
            if os.name == "posix":
                os.killpg(process_group, signal.SIGKILL)
            else:  # pragma: no cover - Switchboard's service host is macOS
                os.kill(process_group, signal.SIGKILL)
        except ProcessLookupError:
            pass


@contextmanager
def managed_process(
    command: Sequence[str],
    *,
    env: dict[str, str] | None = None,
) -> Iterator[subprocess.Popen[str]]:
    """Start a long-lived source process owned by the supervisor.

    Persistent stream adapters cannot use ``run_bounded`` because a healthy process is
    expected not to exit. Registering the process group here gives supervisor shutdown the
    same descendant-cleanup guarantee as bounded adapters.
    """
    process = subprocess.Popen(
        list(command),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        start_new_session=os.name == "posix",
        bufsize=1,
    )
    with _active_lock:
        _active_process_groups.add(process.pid)
    try:
        yield process
    finally:
        _terminate_process_group(process)
        process.wait()
        if process.stdout is not None:
            process.stdout.close()
        if process.stderr is not None:
            process.stderr.close()
        with _active_lock:
            _active_process_groups.discard(process.pid)
