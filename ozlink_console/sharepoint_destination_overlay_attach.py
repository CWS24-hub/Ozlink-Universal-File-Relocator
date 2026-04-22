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

from ozlink_console.logger import log_info

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


def destination_payload_is_memory_overlay_row_for_reuse(pl: Any) -> bool:
    """Broader than :func:`destination_payload_is_planned_workspace_row` for reuse/presnapshot counts.

    Includes restored rows that may still carry cached/provisional stamping from snapshots that lost
    strict ``verification_state`` / ``row_kind`` pairing, but are clearly not live Graph rows.
    """
    if destination_payload_is_planned_workspace_row(pl):
        return True
    if not isinstance(pl, dict) or pl.get("placeholder"):
        return False
    if destination_payload_is_live_graph_row(pl):
        return False
    if bool(pl.get("workspace_planned_row")):
        return True
    if bool(pl.get("planned_allocation_descendant")):
        return True
    if bool(pl.get("planned_allocation")):
        return True
    origin = str(pl.get("node_origin", "") or "").strip().casefold()
    if origin in ("plannedallocation", "proposed", "projecteddestination"):
        return True
    ovl = str(pl.get("overlay_state", "") or "").strip().casefold()
    if ovl == "plannedallocation":
        return True
    if bool(pl.get("proposed")):
        return True
    lbl = f"{pl.get('base_display_label', '')!s} {pl.get('tree_label', '')!s}".casefold()
    if "[planned]" in lbl or "[allocated]" in lbl:
        return True
    if " planned" in f" {lbl}" or lbl.startswith("planned") or " allocated" in f" {lbl}":
        return True
    rk = str(pl.get("row_kind") or "").strip().lower()
    if rk.startswith("planned_") or rk in ("planned_folder", "planned_file"):
        if str(pl.get("verification_state") or "").strip().casefold() in ("planned_only", ""):
            return True
    ws = destination_payload_workspace_row_state(pl)
    if ws == WORKSPACE_ROW_STATE_PLANNED_ONLY:
        return True
    vs = str(pl.get("verification_state") or "").strip().casefold()
    if vs == "planned_only" and rk.startswith("planned"):
        return True
    if ws == WORKSPACE_ROW_STATE_CACHED_PROVISIONAL and (
        bool(pl.get("allocation_id") or pl.get("request_id") or pl.get("RequestId"))
        or bool(pl.get("allocation_projection_destination_path_saved"))
    ):
        return True
    return False


def destination_snapshot_rehydrate_overlay_payload(pl: dict[str, Any]) -> bool:
    """Restore strict planned-workspace markers when snapshot rows lost pairing. Returns True if mutated."""
    if not isinstance(pl, dict) or pl.get("placeholder"):
        return False
    if destination_payload_is_planned_workspace_row(pl):
        pl.setdefault("workspace_row_state", WORKSPACE_ROW_STATE_PLANNED_ONLY)
        return False
    if destination_payload_is_live_graph_row(pl):
        return False
    mutated = False
    lbl = f"{pl.get('base_display_label', '')!s} {pl.get('tree_label', '')!s}".casefold()
    rk = str(pl.get("row_kind") or "").strip().lower()
    ws0 = destination_payload_workspace_row_state(pl)
    origin_cf = str(pl.get("node_origin", "") or "").strip().casefold()
    looks_planned = bool(
        pl.get("planned_allocation_descendant")
        or pl.get("workspace_planned_row")
        or pl.get("planned_allocation")
        or rk.startswith("planned_")
        or "[planned]" in lbl
        or "[allocated]" in lbl
        or " planned" in f" {lbl}"
        or " allocated" in f" {lbl}"
        or bool(pl.get("proposed"))
        or origin_cf in ("plannedallocation", "proposed", "projecteddestination")
        or str(pl.get("overlay_state", "") or "").strip().casefold() == "plannedallocation"
        or ws0 == WORKSPACE_ROW_STATE_PLANNED_ONLY
        or (
            ws0 == WORKSPACE_ROW_STATE_CACHED_PROVISIONAL
            and (
                bool(pl.get("allocation_id") or pl.get("request_id") or pl.get("RequestId"))
                or bool(pl.get("allocation_projection_destination_path_saved"))
            )
        )
    )
    if not looks_planned:
        return False
    if str(pl.get("verification_state") or "").strip() != "planned_only":
        pl["verification_state"] = "planned_only"
        mutated = True
    if rk not in ("planned_folder", "planned_file"):
        pl["row_kind"] = "planned_folder" if bool(pl.get("is_folder", True)) else "planned_file"
        mutated = True
    pl["workspace_row_state"] = WORKSPACE_ROW_STATE_PLANNED_ONLY
    return True


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
    roots = list(snapshot_list or [])
    for root in roots:
        if isinstance(root, dict):
            _destination_stamp_snapshot_branch(root)
    if roots:
        log_info(
            "destination_snapshot_children_marked_needs_live_refresh",
            root_count=len(roots),
            note="destination_snapshot_cached_graph_children_verified_false",
        )


def _destination_stamp_snapshot_branch(snap: dict[str, Any]) -> None:
    data = snap.get("data")
    if isinstance(data, dict) and not data.get("placeholder"):
        try:
            if destination_snapshot_rehydrate_overlay_payload(data):
                log_info(
                    "destination_snapshot_overlay_metadata_rehydrated",
                    path_excerpt=str(data.get("item_path") or data.get("destination_path") or "")[:400],
                )
        except Exception:
            pass
        if destination_payload_is_planned_workspace_row(data):
            data["workspace_row_state"] = WORKSPACE_ROW_STATE_PLANNED_ONLY
        else:
            # Only log skips when the row looked like a planned overlay but could not be stamped strict.
            try:
                _lbl = f"{data.get('base_display_label', '')!s} {data.get('tree_label', '')!s}".casefold()
                _pseudo = bool(
                    data.get("planned_allocation_descendant")
                    or data.get("workspace_planned_row")
                    or "[planned]" in _lbl
                    or "[allocated]" in _lbl
                )
                if _pseudo and not destination_payload_is_live_graph_row(data):
                    log_info(
                        "destination_snapshot_overlay_metadata_rehydrate_skipped",
                        path_excerpt=str(data.get("item_path") or data.get("destination_path") or "")[:400],
                        row_kind_excerpt=str(data.get("row_kind") or "")[:40],
                        verification_state_excerpt=str(data.get("verification_state") or "")[:24],
                    )
            except Exception:
                pass
            data["workspace_row_state"] = WORKSPACE_ROW_STATE_CACHED_PROVISIONAL
        # Visual cache only — live Graph /children must still refresh expanded branches.
        if not destination_payload_is_planned_workspace_row(data):
            data["destination_snapshot_cached"] = True
            data["graph_children_verified"] = False
            data["needs_live_child_refresh"] = True
    for ch in list(snap.get("children") or []):
        if isinstance(ch, dict):
            _destination_stamp_snapshot_branch(ch)
