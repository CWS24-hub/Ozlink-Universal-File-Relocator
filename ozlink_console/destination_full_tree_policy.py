"""Central policy for when the destination full-library enumerate worker may run (delta-first runtime)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ozlink_console.logger import log_info


@dataclass(frozen=True)
class DestinationFullTreeScheduleDecision:
    allowed: bool
    decision_tag: str
    payload: dict[str, Any]


def destination_graph_delta_cursor_present(graph: Any | None, drive_id: str) -> bool:
    """True when persisted delta cursor exists for this drive (bootstrap not required for delta sync)."""
    if graph is None:
        return False
    did = str(drive_id or "").strip()
    if not did:
        return False
    try:
        p = graph._drive_delta_state_path(did)  # type: ignore[attr-defined]
        if not p or not p.is_file():
            return False
        import json

        raw = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return False
        return bool(str(raw.get("delta_link") or "").strip())
    except Exception:
        return False


def should_schedule_destination_full_tree(
    *,
    reason: str,
    drive_id: str,
    force_refresh: bool = False,
    delta_failed: bool = False,
    bootstrap: bool = False,
    explicit_refresh: bool = False,
    recovery: bool = False,
    routine_followup: bool = False,
    delta_cursor_present: bool = False,
    bootstrap_complete: bool = False,
    skeleton_first_bootstrap: bool = False,
) -> DestinationFullTreeScheduleDecision:
    """
    Returns whether the full-tree worker may be scheduled.

    When Graph delta is healthy (cursor present, no failure), routine follow-ups are denied.
    """
    suffix = str(drive_id)[-16:] if len(str(drive_id)) > 16 else str(drive_id)
    base: dict[str, Any] = {
        "reason": str(reason or "")[:220],
        "selected_drive_id_suffix": suffix,
        "delta_cursor_present": bool(delta_cursor_present),
        "bootstrap_complete": bool(bootstrap_complete),
        "force_refresh": bool(force_refresh),
        "delta_failed": bool(delta_failed),
        "explicit_refresh": bool(explicit_refresh),
        "recovery": bool(recovery),
        "bootstrap": bool(bootstrap),
        "routine_followup": bool(routine_followup),
        "skeleton_first_bootstrap": bool(skeleton_first_bootstrap),
    }

    if explicit_refresh or force_refresh:
        base["decision"] = "allowed"
        log_info("destination_full_tree_schedule_decision", **base)
        log_info("destination_full_tree_schedule_allowed_explicit_refresh", **base)
        return DestinationFullTreeScheduleDecision(True, "explicit_refresh", base)

    if bootstrap:
        base["decision"] = "allowed"
        log_info("destination_full_tree_schedule_decision", **base)
        log_info("destination_full_tree_schedule_allowed_bootstrap", **base)
        return DestinationFullTreeScheduleDecision(True, "bootstrap", base)

    if recovery or delta_failed:
        base["decision"] = "allowed"
        log_info("destination_full_tree_schedule_decision", **base)
        log_info("destination_full_tree_schedule_allowed_recovery", **base)
        return DestinationFullTreeScheduleDecision(True, "recovery", base)

    if routine_followup and delta_cursor_present and not delta_failed:
        base["decision"] = "suppressed"
        log_info("destination_full_tree_schedule_decision", **base)
        if skeleton_first_bootstrap:
            log_info("destination_full_tree_schedule_suppressed_skeleton_first_bootstrap", **base)
        else:
            log_info("destination_full_tree_schedule_suppressed_delta_mode", **base)
        return DestinationFullTreeScheduleDecision(False, "suppressed_delta_mode", base)

    # Legacy / cautious default: allow when delta is not yet bootstrapped (no cursor).
    if routine_followup and not delta_cursor_present:
        base["decision"] = "allowed_fallback_no_delta_cursor"
        log_info("destination_full_tree_schedule_decision", **base)
        log_info("destination_graph_delta_full_fallback_used", **base)
        return DestinationFullTreeScheduleDecision(True, "fallback_no_delta_cursor", base)

    base["decision"] = "suppressed_unknown_intent"
    log_info("destination_full_tree_schedule_decision", **base)
    log_info("destination_full_tree_schedule_suppressed_unknown_reason", **base)
    return DestinationFullTreeScheduleDecision(False, "unknown_intent", base)


# Alias for callers that prefer private-style naming.
_should_schedule_destination_full_tree = should_schedule_destination_full_tree


def log_destination_graph_delta_lifecycle(event: str, **kwargs: Any) -> None:
    """Structured logs for Graph delta handling (branch-focused)."""
    log_info(event, **kwargs)
