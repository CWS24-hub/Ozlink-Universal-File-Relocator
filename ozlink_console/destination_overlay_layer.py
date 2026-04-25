"""
Overlay layer for SharePoint destination.

Layer 1 (live skeleton) is Graph-backed rows in the tree model.

Overlay rows use ``overlay_node_id`` (``ovl:…`` from :mod:`ozlink_console.destination_overlay_identity`);
Graph rows keep real driveItem ids in ``id`` / ``graph_item_id`` only — never synthetic ``projected::`` /
``allocated::`` / ``proposed::`` prefixes on ``id``.
"""

from __future__ import annotations

from typing import Any, Literal

OverlayKind = Literal["proposed", "allocation", "projection", "badge", "planned_workspace"]


def overlay_row_marker(kind: OverlayKind) -> dict[str, Any]:
    """Payload fields that mark a row as non-authoritative structure (for future model rows)."""
    return {
        "destination_overlay_kind": kind,
        "non_graph_structural_authority": True,
    }
