#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import stat
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any


def invoke(command: list[str], db: Path, *arguments: str, expect: int = 0) -> Any:
    result = subprocess.run(
        [*command, "--db", str(db), "--json", *arguments],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if result.returncode != expect:
        raise RuntimeError(
            f"command exited {result.returncode}: {' '.join(arguments)}\n"
            + (result.stderr or result.stdout)
        )
    payload = json.loads(result.stdout)
    if expect != 0:
        return payload
    if not payload.get("ok"):
        raise RuntimeError(payload.get("error", "unknown CLI error"))
    return payload["data"]


def executable(path: Path, body: str) -> None:
    path.write_text(body)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--switchboard", default="switchboard")
    args = parser.parse_args()
    command = (
        [str(Path(args.switchboard).expanduser().resolve())]
        if os.path.sep in args.switchboard
        else [args.switchboard]
    )

    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        db = root / "switchboard.sqlite3"
        topology = root / "topology.json"
        source_example = (
            Path(__file__).resolve().parents[1] / "examples" / "topology.json"
        )
        topology.write_bytes(source_example.read_bytes())
        relay = root / "offline-relay"
        executable(relay, "#!/bin/sh\nexit 1\n")
        alerts = root / "alerts.jsonl"
        alert_command = root / "capture-alert"
        executable(
            alert_command,
            "#!/usr/bin/env python3\n"
            "import os, pathlib, sys\n"
            "pathlib.Path(os.environ['SWITCHBOARD_ACCEPTANCE_ALERTS']).open('a').write(sys.stdin.read() + '\\n')\n",
        )
        environment = dict(os.environ, SWITCHBOARD_ACCEPTANCE_ALERTS=str(alerts))

        invoke(
            command,
            db,
            "topology",
            "apply",
            str(topology),
            "--var",
            "WORKER=chat:claude:acceptance",
        )
        repeated = invoke(
            command,
            db,
            "topology",
            "apply",
            str(topology),
            "--var",
            "WORKER=chat:claude:acceptance",
        )
        if any(item["action"] != "noop" for item in repeated["operations"]):
            raise RuntimeError("repeated topology apply was not a no-op")

        deliveries: list[str] = []
        for index in range(3):
            event = invoke(
                command,
                db,
                "event",
                "emit",
                "--source",
                "timer/example-daily",
                "--external-id",
                f"outage-{index}",
                "--type",
                "maintenance.due",
            )
            deliveries.extend(event["processor_runs"])
        for item in invoke(
            command, db, "processor", "delivery-list", "--state", "pending"
        ):
            invoke(
                command,
                db,
                "processor",
                "delivery-dispatch",
                item["id"],
                "--relay",
                str(relay),
                expect=2,
            )

        time.sleep(1.1)
        supervisor = [
            "supervisor",
            "once",
            "--alert-after",
            "1",
            "--alert-command-json",
            json.dumps([str(alert_command)]),
        ]
        original = os.environ.copy()
        os.environ.update(environment)
        try:
            first = invoke(command, db, *supervisor)
            restarted = invoke(command, db, *supervisor)
        finally:
            os.environ.clear()
            os.environ.update(original)
        if len(first["alerts"]) != 1 or restarted["alerts"]:
            raise RuntimeError("outage alert was not durable across supervisor restart")

        completed = 0
        while True:
            run = invoke(
                command,
                db,
                "processor",
                "claim-next",
                "--worker",
                "chat:claude:acceptance",
            )
            if run is None:
                break
            invoke(
                command,
                db,
                "processor",
                "complete",
                run["id"],
                "--worker",
                "chat:claude:acceptance",
                "--summary",
                "acceptance drain",
            )
            completed += 1
        if completed != len(deliveries):
            raise RuntimeError(
                f"drained {completed} of {len(deliveries)} processor runs"
            )

        os.environ.update(environment)
        try:
            recovered = invoke(command, db, *supervisor)
            repeated_recovery = invoke(command, db, *supervisor)
        finally:
            os.environ.clear()
            os.environ.update(original)
        kinds = [json.loads(line)["kind"] for line in alerts.read_text().splitlines()]
        if (
            len(recovered["alerts"]) != 1
            or repeated_recovery["alerts"]
            or kinds != ["processor-unreachable", "processor-recovered"]
        ):
            raise RuntimeError(f"unexpected alert episode: {kinds}")
        status = invoke(command, db, "status")
        if status["counts"]["open_processor_runs"] != 0:
            raise RuntimeError("processor backlog did not drain")
        print(
            json.dumps(
                {"ok": True, "drained": completed, "alerts": kinds}, sort_keys=True
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
