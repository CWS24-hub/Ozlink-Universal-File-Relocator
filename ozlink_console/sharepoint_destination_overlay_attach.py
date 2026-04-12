"""
SharePoint destination — overlay attach without structural scaffolding.

Visible folders/files come only from Microsoft Graph (root bind + per-folder loads).
Planning overlays (proposed folders, allocations, projected descendants) attach under
**existing** Graph-backed rows via :meth:`MainWindow._ensure_destination_projection_path`
when :func:`ozlink_console.destination_authority_contract.graph_owns_visible_real_destination_structure`
is active.

This module holds small pure predicates shared by the attach path.
"""

from __future__ import annotations

from typing import Any


def destination_payload_is_live_graph_row(pl: Any) -> bool:
    """True when ``pl`` is a non-placeholder destination row backed by a real Graph driveItem id."""
    if not isinstance(pl, dict) or pl.get("placeholder"):
        return False
    if pl.get("non_graph_structural_authority"):
        return False
    if bool(pl.get("projected")):
        return False
    if str(pl.get("node_origin", "") or "").lower() == "projecteddestination":
        return False
    iid = str(pl.get("id") or "").strip()
    return bool(iid)
