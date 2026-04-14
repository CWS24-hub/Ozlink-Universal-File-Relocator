"""
SharePoint destination — overlay attach and planned workspace rows.

Live folders/files are confirmed only from Microsoft Graph (root bind + per-folder loads).

**Planned workspace rows** (``verification_state == planned_only``) are user-intent scaffolding
allowed in the destination planning model under Graph authority. They are not live Graph rows
and must never satisfy :func:`destination_payload_is_live_graph_row`.

This module holds small pure predicates shared by the attach path.
"""

from __future__ import annotations

from typing import Any


def destination_payload_is_planned_workspace_row(pl: Any) -> bool:
    """True for workspace-layer planned rows (not live Graph, not loading placeholders)."""
    if not isinstance(pl, dict) or pl.get("placeholder"):
        return False
    if str(pl.get("verification_state") or "").strip() != "planned_only":
        return False
    rk = str(pl.get("row_kind") or "").strip().lower()
    return rk in {"planned_folder", "planned_file"}


def destination_payload_is_live_graph_row(pl: Any) -> bool:
    """True when ``pl`` is a non-placeholder destination row backed by a real Graph driveItem id."""
    if not isinstance(pl, dict) or pl.get("placeholder"):
        return False
    if destination_payload_is_planned_workspace_row(pl):
        return False
    if pl.get("non_graph_structural_authority"):
        return False
    if bool(pl.get("projected")):
        return False
    if str(pl.get("node_origin", "") or "").lower() == "projecteddestination":
        return False
    iid = str(pl.get("id") or "").strip()
    return bool(iid)
