from __future__ import annotations

from typing import Any

from ..db import Database
from .base import apply_snapshot


def run_timer(
    db: Database,
    *,
    space_id: str,
    source_id: str,
    event_type: str,
    scheduled_for: str,
    attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot = {
        "adapter": "timer",
        "space": {"id": space_id},
        "sources": [
            {
                "id": source_id,
                "kind": "timer",
                "state": "ready",
                "detail": "emitted by a Switchboard interval schedule",
                "config": {"adapter": "timer"},
            }
        ],
        "events": [
            {
                "source_id": source_id,
                "external_id": scheduled_for,
                "event_type": event_type,
                "occurred_at": scheduled_for,
                "attributes": {"scheduled_for": scheduled_for, **(attributes or {})},
            }
        ],
    }
    return apply_snapshot(db, snapshot)
