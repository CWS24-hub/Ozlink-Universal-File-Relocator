"""
SharePoint destination — overlay attach and planned workspace rows.

Live folders/files are confirmed only from Microsoft Graph (root bind + per-folder loads).

**Planned workspace rows** (``verification_state == planned_only``) are user-intent scaffolding
allowed in the destination planning model under Graph authority. They are not live Graph rows
and must never satisfy :func:`destination_payload_is_live_graph_row`.

**Workspace row state** (``workspace_row_state``) is the single authority for whether a row is
live-confirmed vs cached provisional vs planned-only. Presence of a Graph ``id`` alone must
never imply live state.

This module holds small pure predicates shared by the attach path.
"""

from __future__ import annotations

from typing import Any

# Single source of truth for destination workspace row authority (Phase 0).
WORKSPACE_ROW_STATE_LIVE_CONFIRMED = "live_confirmed"
WORKSPACE_ROW_STATE_CACHED_PROVISIONAL = "cached_provisional"
WORKSPACE_ROW_STATE_PLANNED_ONLY = "planned_only"


def destination_payload_is_planned_workspace_row(pl: Any) -> bool:
    """True for workspace-layer planned rows (not live Graph, not loading placeholders)."""
    if not isinstance(pl, dict) or pl.get("placeholder"):
        return False
    if str(pl.get("verification_state") or "").strip() != "planned_only":
        return False
    rk = str(pl.get("row_kind") or "").strip().lower()
    return rk in {"planned_folder", "planned_file"}


def destination_payload_workspace_row_state(pl: Any) -> str:
    if not isinstance(pl, dict):
        return ""
    return str(pl.get("workspace_row_state") or "").strip()


def destination_payload_is_live_graph_row(pl: Any) -> bool:
    """True only when the payload is explicitly marked live-confirmed from Graph.

    Graph ``id`` / ``drive_id`` may be present on cached provisional rows for matching; they must
    not imply live authority.
    """
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
    return destination_payload_workspace_row_state(pl) == WORKSPACE_ROW_STATE_LIVE_CONFIRMED


def destination_payload_is_reconcile_merge_target_row(pl: Any) -> bool:
    """Rows under a Graph-loaded folder that planned→live reconcile may merge into.

    This is **not** :func:`destination_payload_is_live_graph_row` (cached snapshot shells must not
    satisfy that predicate). Match targets include ``cached_provisional`` and legacy payloads that
    already carried explicit Graph markers (``row_kind`` ``live_*`` + ``verification_state``
    ``live_confirmed``) before ``workspace_row_state`` existed. Bare driveItem ``id`` alone is
    **not** sufficient.
    """
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
    st = destination_payload_workspace_row_state(pl)
    if st == WORKSPACE_ROW_STATE_LIVE_CONFIRMED:
        return True
    if st == WORKSPACE_ROW_STATE_CACHED_PROVISIONAL:
        return True
    if st:
        return False
    rk = str(pl.get("row_kind") or "").strip().lower()
    vs = str(pl.get("verification_state") or "").strip().lower()
    return rk.startswith("live_") and vs == "live_confirmed"


def destination_payload_is_structural_row_for_planned_workspace_bind(pl: Any) -> bool:
    """Existing tree row that may act as a parent anchor when binding planned workspace segments.

    Includes live-confirmed Graph rows, cached provisional shells, planned workspace rows, legacy
    explicit Graph markers, and **legacy test/library folder payloads** (``tree_role`` destination,
    folder, drive id, item id) which are not ``live_confirmed`` but are still valid attach anchors.
    This predicate must **not** be used as a substitute for :func:`destination_payload_is_live_graph_row`.
    """
    if not isinstance(pl, dict) or pl.get("placeholder"):
        return False
    if destination_payload_is_planned_workspace_row(pl):
        return True
    if destination_payload_is_live_graph_row(pl):
        return True
    if destination_payload_workspace_row_state(pl) == WORKSPACE_ROW_STATE_CACHED_PROVISIONAL:
        return True
    if destination_payload_workspace_row_state(pl):
        return False
    rk = str(pl.get("row_kind") or "").strip().lower()
    vs = str(pl.get("verification_state") or "").strip().lower()
    if rk.startswith("live_") and vs == "live_confirmed":
        return True
    if (
        str(pl.get("tree_role") or "").strip() == "destination"
        and bool(pl.get("is_folder", True))
        and str(pl.get("drive_id") or "").strip()
        and str(pl.get("id") or "").strip()
        and str(pl.get("verification_state") or "").strip() != "planned_only"
        and not bool(pl.get("projected"))
    ):
        return True
    return False


def destination_stamp_snapshot_tree_workspace_state(snapshot_list: list[Any] | None) -> None:
    """Mark every non-placeholder node in a persisted destination tree snapshot for restore.

    Planned workspace rows stay ``planned_only``; all other rows become ``cached_provisional``.
    Graph ids are preserved; they carry no live authority until upgraded.
    """
    for root in list(snapshot_list or []):
        if isinstance(root, dict):
            _destination_stamp_snapshot_branch(root)


def _destination_stamp_snapshot_branch(snap: dict[str, Any]) -> None:
    data = snap.get("data")
    if isinstance(data, dict) and not data.get("placeholder"):
        if destination_payload_is_planned_workspace_row(data):
            data["workspace_row_state"] = WORKSPACE_ROW_STATE_PLANNED_ONLY
        else:
            data["workspace_row_state"] = WORKSPACE_ROW_STATE_CACHED_PROVISIONAL
    for ch in list(snap.get("children") or []):
        if isinstance(ch, dict):
            _destination_stamp_snapshot_branch(ch)
