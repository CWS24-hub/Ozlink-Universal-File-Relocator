"""
Destination overlay identity (SharePoint planning rows).

Real Microsoft Graph driveItem ids must never be mimicked on overlay rows. Graph-backed rows
keep the API ``id`` (and may duplicate it in ``graph_item_id``). Non-Graph rows use
``overlay_node_id`` only — never Graph API calls with overlay ids.

The sentinel ``OVERLAY_LIB_ROOT_SEMANTIC`` is an internal topology key in the in-memory
``model_nodes`` graph (overlay bind / incremental merge). It is not a visible tree row and
is not a Graph id.
"""

from __future__ import annotations

import hashlib
from typing import Any

from ozlink_console.planned_move_graph_resolve import is_internal_proposed_destination_item_id

# Internal-only parent key for overlay topology under the QAbstractItemModel root (not "Root").
OVERLAY_LIB_ROOT_SEMANTIC = "__ozlink_dest_lib_root__"


def new_overlay_node_id(kind: str, stable_key: str) -> str:
    """Stable overlay id derived from kind + key (same key → same id within a process lifetime)."""
    raw = f"{kind}\x00{stable_key}".encode("utf-8", errors="replace")
    digest = hashlib.sha256(raw).hexdigest()[:26]
    return f"ovl:{kind}:{digest}"


def is_overlay_node_id(value: str | None) -> bool:
    return str(value or "").startswith("ovl:")


def is_legacy_synthetic_destination_row_id(value: str | None) -> bool:
    """True for old fake ``id`` values that must not be treated as Graph ids."""
    s = str(value or "").strip()
    if not s:
        return False
    low = s.lower()
    if low.startswith("projected::"):
        return True
    if low.startswith("allocated::"):
        return True
    if low.startswith("allocated-descendant::"):
        return True
    if low.startswith("proposed::"):
        return True
    return is_internal_proposed_destination_item_id(s)


def graph_item_id_field(dest_id: str | None) -> str:
    """
    Return ``dest_id`` only when it is plausibly a real Microsoft Graph driveItem id; else empty.

    Overlay ids (``ovl:``), UI placeholders (PROP-*, INLINE-PROP-*), and legacy ``::*`` synthetic
    prefixes are never treated as Graph ids. Use :func:`migrate_legacy_overlay_payload_row` on load.
    """
    s = str(dest_id or "").strip()
    if not s:
        return ""
    if is_overlay_node_id(s):
        return ""
    u = s.upper()
    if u.startswith("PROP-") or u.startswith("INLINE-PROP-"):
        return ""
    low = s.lower()
    if (
        low.startswith("projected::")
        or low.startswith("allocated::")
        or low.startswith("allocated-descendant::")
        or low.startswith("proposed::")
    ):
        return ""
    return s


def migrate_legacy_overlay_payload_row(data: dict[str, Any]) -> None:
    """
    In-place: move legacy synthetic ``id`` into ``overlay_node_id`` if missing, clear ``id``.

    Preserves real Graph ids. Safe to call on payloads deserialized from older drafts.
    """
    if not isinstance(data, dict):
        return
    rid = str(data.get("id") or "").strip()
    if not rid or not is_legacy_synthetic_destination_row_id(rid):
        return
    if not data.get("overlay_node_id"):
        if rid.lower().startswith("proposed::"):
            kind, _, rest = rid.partition("::")
            data["overlay_node_id"] = new_overlay_node_id(kind or "proposed", rest or rid)
        elif rid.lower().startswith("projected::"):
            _, _, rest = rid.partition("::")
            data["overlay_node_id"] = new_overlay_node_id("projection", rest or rid)
        elif rid.lower().startswith("allocated-descendant::"):
            _, _, rest = rid.partition("::")
            data["overlay_node_id"] = new_overlay_node_id("allocated_descendant", rest or rid)
        elif rid.lower().startswith("allocated::"):
            _, _, rest = rid.partition("::")
            data["overlay_node_id"] = new_overlay_node_id("allocation", rest or rid)
        else:
            data["overlay_node_id"] = new_overlay_node_id("legacy", rid)
    data["id"] = ""
    data.setdefault("non_graph_structural_authority", True)
