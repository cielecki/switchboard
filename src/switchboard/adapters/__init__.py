"""Source adapter contracts and built-in adapters."""

from .base import AdapterError, apply_snapshot, run_command_adapter
from .ingest import run_ingest_shadow

__all__ = ["AdapterError", "apply_snapshot", "run_command_adapter", "run_ingest_shadow"]
