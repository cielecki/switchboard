from __future__ import annotations

import os
import plistlib
import subprocess
import sys
from pathlib import Path
from typing import Any

from .db import Database

LABEL = "io.github.cielecki.switchboard"


def launch_agent_path() -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{LABEL}.plist"


def _domain() -> str:
    return f"gui/{os.getuid()}"


def _run(runner: Any, command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = runner(command, capture_output=True, text=True, check=False)
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise ValueError(f"{' '.join(command)} failed ({result.returncode}): {detail}")
    return result


def service_status(*, runner: Any = subprocess.run) -> dict[str, Any]:
    path = launch_agent_path()
    result = _run(runner, ["launchctl", "print", f"{_domain()}/{LABEL}"], check=False)
    return {
        "label": LABEL,
        "plist": str(path),
        "installed": path.is_file(),
        "loaded": result.returncode == 0,
        "detail": (result.stdout if result.returncode == 0 else result.stderr).strip()[-4000:],
    }


def install_launch_agent(
    db: Database,
    *,
    cli_command: list[str],
    relay: str | Path | None,
    host: str = "127.0.0.1",
    port: int = 8765,
    poll_seconds: int = 5,
    delivery_retry_seconds: int = 60,
    activate_inactive: bool = False,
    runner: Any = subprocess.run,
) -> dict[str, Any]:
    if sys.platform != "darwin":
        raise ValueError("service install currently supports macOS launchd only")
    if not cli_command or not Path(cli_command[0]).expanduser().is_file():
        raise ValueError("service install requires an absolute Switchboard launcher path")
    if relay is not None and not Path(relay).expanduser().is_file():
        raise ValueError(f"chats relay not found: {Path(relay).expanduser()}")
    if poll_seconds < 1 or delivery_retry_seconds < 1:
        raise ValueError("service intervals must be at least one second")

    state_dir = Path.home() / ".local" / "state" / "switchboard"
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    arguments = [
        *cli_command,
        "--db",
        str(db.path),
        "supervisor",
        "run",
        "--host",
        host,
        "--port",
        str(port),
        "--poll",
        str(poll_seconds),
        "--delivery-retry",
        str(delivery_retry_seconds),
    ]
    if relay is not None:
        arguments.extend(["--relay", str(Path(relay).expanduser().resolve())])
    if activate_inactive:
        arguments.append("--activate-inactive")

    payload = {
        "Label": LABEL,
        "ProgramArguments": arguments,
        "RunAtLoad": True,
        "KeepAlive": True,
        "ProcessType": "Background",
        "StandardOutPath": str(state_dir / "supervisor.stdout.log"),
        "StandardErrorPath": str(state_dir / "supervisor.stderr.log"),
    }
    path = launch_agent_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = plistlib.dumps(payload, fmt=plistlib.FMT_XML, sort_keys=True)
    path.write_bytes(encoded)
    if path.read_bytes() != encoded:
        raise ValueError(f"failed to verify launch agent: {path}")

    _run(runner, ["launchctl", "bootout", _domain(), str(path)], check=False)
    _run(runner, ["launchctl", "bootstrap", _domain(), str(path)])
    _run(runner, ["launchctl", "kickstart", "-k", f"{_domain()}/{LABEL}"])
    status = service_status(runner=runner)
    if not status["loaded"]:
        raise ValueError(f"launch agent did not load: {status['detail']}")
    return status


def uninstall_launch_agent(*, runner: Any = subprocess.run) -> dict[str, Any]:
    if sys.platform != "darwin":
        raise ValueError("service uninstall currently supports macOS launchd only")
    path = launch_agent_path()
    _run(runner, ["launchctl", "bootout", _domain(), str(path)], check=False)
    removed = path.is_file()
    if removed:
        path.unlink()
    status = service_status(runner=runner)
    if status["loaded"] or status["installed"]:
        raise ValueError("launch agent still appears installed")
    return {**status, "removed": removed}
