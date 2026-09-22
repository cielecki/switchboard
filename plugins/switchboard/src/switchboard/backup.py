from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

from .db import Database


def verify_backup(path: str | Path) -> dict[str, Any]:
    target = Path(path).expanduser().resolve()
    if not target.is_file():
        raise ValueError(f"backup not found: {target}")
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(f"file:{target}?mode=ro", uri=True)
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        version = connection.execute(
            "SELECT value FROM schema_meta WHERE key='schema_version'"
        ).fetchone()
    except sqlite3.Error as exc:
        raise ValueError(f"invalid Switchboard backup: {exc}") from exc
    finally:
        if connection is not None:
            connection.close()
    if integrity != "ok":
        raise ValueError(f"backup integrity check failed: {integrity}")
    if version is None:
        raise ValueError("backup has no Switchboard schema version")
    return {
        "path": str(target),
        "bytes": target.stat().st_size,
        "integrity": integrity,
        "schema_version": int(version[0]),
    }


def create_backup(db: Database, path: str | Path) -> dict[str, Any]:
    # Back up an existing database before any schema initialization or migration so this
    # command is also the safe first step of an upgrade. New databases still need a schema.
    if not db.path.exists():
        db.initialize()
    target = Path(path).expanduser().resolve()
    if target == db.path:
        raise ValueError("backup destination must differ from the active database")
    if target.exists():
        raise ValueError(f"backup destination already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        with db.session() as source, sqlite3.connect(temporary) as destination:
            source.backup(destination)
        os.chmod(temporary, 0o600)
        verification = verify_backup(temporary)
        os.replace(temporary, target)
        final = verify_backup(target)
        if final["bytes"] != verification["bytes"]:
            raise ValueError("backup changed during atomic installation")
        return final
    finally:
        if temporary.exists():
            temporary.unlink()
