"""
Hybrid destination preview (Browse/Plan/Execute — destination slice).

Authoritative spec: ``docs/architecture/browse_plan_execute_contract.md`` (hybrid + badge sections).

* Saved plan / session snapshot is shown first; live Microsoft Graph is merged on expand/refresh.
* This module is intentionally small: env gates + display badges derived from existing payload fields.
"""

from __future__ import annotations

import os
from typing import Any

from ozlink_console.sharepoint_destination_overlay_attach import destination_payload_is_planned_workspace_row


def hybrid_destination_preview_browse_first_enabled() -> bool:
    """
    When True, provisional startup skips phase-2 expand/hydrate/branch-refresh (broad rehydrate)
    until the user needs it; preview remains browse-first. Opt out with
    OZLINK_HYBRID_DESTINATION_PREVIEW=0|false|no|off.
    """
    raw = str(os.environ.get("OZLINK_HYBRID_DESTINATION_PREVIEW", "") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    return True


def _ws(pl: dict) -> str:
    return str(pl.get("workspace_row_state") or "").strip().lower()


def destination_hybrid_preview_badge_text(payload: Any) -> str:
    """
    Return a short bracketed badge for the name column, or "" when none.

    Contract mapping (first slice, derived from existing model fields only):
    * [Live + Planned] — graph_vs_planned == live_planned (set when expand merges live Graph into a planned row)
    * [Not refreshed] — cached_provisional
    * [Planned] / [Proposed] — strict planned workspace rows
    * [Live] — live Graph row without planning overlay
    """
    if not isinstance(payload, dict) or payload.get("placeholder"):
        return ""
    gvp = str(payload.get("graph_vs_planned") or "").strip().lower()
    if gvp == "live_planned":
        return "[Live + Planned]"
    ws = _ws(payload)
    if ws == "cached_provisional":
        return "[Not refreshed]"
    if destination_payload_is_planned_workspace_row(payload):
        if bool(payload.get("proposed")) or str(payload.get("node_origin") or "").strip().lower() == "proposed":
            return "[Proposed]"
        return "[Planned]"
    if ws == "live_confirmed":
        return "[Live]"
    if gvp in ("live_graph", "live"):
        return "[Live]"
    rk = str(payload.get("row_kind") or "").lower()
    if str(payload.get("id") or "").strip() and rk.startswith("live_"):
        return "[Live]"
    return ""


def destination_hybrid_name_column_text(base_label: str, payload: Any) -> str:
    """Append hybrid badge to the explorer name when browse-first is enabled and a badge applies."""
    if not hybrid_destination_preview_browse_first_enabled():
        return str(base_label or "")
    b = str(base_label or "")
    tag = destination_hybrid_preview_badge_text(payload)
    if not tag:
        return b
    return f"{b}  {tag}" if b else tag
