"""
Destination bind / reconcile / empty-placeholder forensic audit (opt-in via env).

Set:
  OZLINK_DEST_BIND_TRACE=1
  OZLINK_DEST_RECONCILE_TRACE=1
  OZLINK_DEST_EMPTY_TRACE=1
  OZLINK_DEST_PLACEMENT_AUDIT=1  (existing MainWindow gate; summary truth table)
  OZLINK_DEST_EXACT_TARGET_PIPELINE_TRACE=1  (intended set → missing → ancestor → bind → post-bind; survival vs prior pass)
  OZLINK_DEST_SLOW_PASS_FORENSIC=1  (wrapper timings for authority-shell / deferred graph-id materialize paths)

Logs use message keys ``destination_forensic_*`` (routed by :mod:`ozlink_console.logger`).
"""

from __future__ import annotations

import os
import uuid
from typing import Any

from ozlink_console.logger import log_info, log_warn


def env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes")


def bind_trace_enabled() -> bool:
    return env_truthy("OZLINK_DEST_BIND_TRACE")


def reconcile_trace_enabled() -> bool:
    return env_truthy("OZLINK_DEST_RECONCILE_TRACE")


def empty_trace_enabled() -> bool:
    return env_truthy("OZLINK_DEST_EMPTY_TRACE")


def exact_target_pipeline_trace_enabled() -> bool:
    return env_truthy("OZLINK_DEST_EXACT_TARGET_PIPELINE_TRACE")


def slow_pass_forensic_enabled() -> bool:
    return env_truthy("OZLINK_DEST_SLOW_PASS_FORENSIC")


def new_trace_id() -> str:
    return str(uuid.uuid4())


def map_bind_kind_to_row_kind(bind_kind: str, *, terminal_is_file: bool) -> str:
    bk = str(bind_kind or "").strip().lower()
    if bk in ("allocation", "allocation_descendant"):
        return "allocated_file" if terminal_is_file else "allocated_folder"
    if bk == "proposed_folder":
        return "proposed_folder"
    if bk in ("planned_folder", "planned_file"):
        return bk
    if terminal_is_file:
        return "planned_file"
    return "planned_folder"


def log_bind(**payload: Any) -> None:
    log_info("destination_forensic_bind_record", **payload)


def log_bind_step(**payload: Any) -> None:
    log_info("destination_forensic_bind_step", **payload)


def log_child_lookup(**payload: Any) -> None:
    log_info("destination_forensic_child_lookup", **payload)


def log_reconcile(**payload: Any) -> None:
    log_info("destination_forensic_reconcile_record", _ozlink_stream="destination_reconcile", **payload)


def log_empty(**payload: Any) -> None:
    log_info("destination_forensic_empty_placeholder", **payload)


def log_placement_summary(**payload: Any) -> None:
    log_info("destination_forensic_placement_pass_summary", **payload)


def warn_unsafe_name_fallback_under_graph(**payload: Any) -> None:
    log_warn("destination_forensic_unsafe_name_fallback_graph_authority", **payload)
