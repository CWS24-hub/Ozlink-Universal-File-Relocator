"""Phase 0: workspace_row_state is the sole authority for live Graph destination rows."""

from __future__ import annotations

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication

from ozlink_console.sharepoint_destination_overlay_attach import (
    WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
    WORKSPACE_ROW_STATE_LIVE_CONFIRMED,
    WORKSPACE_ROW_STATE_PLANNED_ONLY,
    destination_payload_is_live_graph_row,
    destination_payload_is_reconcile_merge_target_row,
    destination_payload_is_structural_row_for_planned_workspace_bind,
    destination_stamp_snapshot_tree_workspace_state,
)
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def test_cached_provisional_with_graph_id_is_not_live():
    pl = {
        "id": "graph-item-1",
        "drive_id": "d1",
        "is_folder": True,
        "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
        "tree_role": "destination",
    }
    assert not destination_payload_is_live_graph_row(pl)


def test_legacy_payload_without_workspace_state_is_not_live():
    """IDs alone must not imply live authority (Option A: explicit mark only)."""
    pl = {
        "id": "graph-item-2",
        "drive_id": "d1",
        "is_folder": True,
        "tree_role": "destination",
    }
    assert not destination_payload_is_live_graph_row(pl)


def test_reconcile_merge_target_accepts_cached_provisional_and_legacy_markers():
    cached = {
        "id": "a",
        "drive_id": "d",
        "is_folder": True,
        "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
    }
    assert destination_payload_is_reconcile_merge_target_row(cached)
    legacy = {
        "id": "b",
        "drive_id": "d",
        "row_kind": "live_folder",
        "verification_state": "live_confirmed",
        "is_folder": True,
    }
    assert destination_payload_is_reconcile_merge_target_row(legacy)
    assert not destination_payload_is_reconcile_merge_target_row({"id": "only", "drive_id": "d", "is_folder": True})


def test_structural_bind_anchor_accepts_legacy_destination_hub_shape():
    hub = {
        "name": "Hub",
        "id": "live-hub",
        "is_folder": True,
        "item_path": "Hub",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
    }
    assert destination_payload_is_structural_row_for_planned_workspace_bind(hub)


def test_live_confirmed_is_live_when_not_excluded():
    pl = {
        "id": "graph-item-3",
        "drive_id": "d1",
        "is_folder": True,
        "tree_role": "destination",
        "workspace_row_state": WORKSPACE_ROW_STATE_LIVE_CONFIRMED,
    }
    assert destination_payload_is_live_graph_row(pl)


def test_planned_only_row_not_live():
    planned = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "workspace_row_state": WORKSPACE_ROW_STATE_PLANNED_ONLY,
        "is_folder": True,
        "id": "",
        "placeholder": False,
    }
    assert not destination_payload_is_live_graph_row(planned)


def test_destination_stamp_snapshot_restore_marks_rows():
    snap = {
        "text": "A",
        "data": {"id": "x", "name": "A", "is_folder": True},
        "children": [
            {
                "text": "P",
                "data": {
                    "row_kind": "planned_folder",
                    "verification_state": "planned_only",
                    "is_folder": True,
                    "name": "P",
                },
                "children": [],
            }
        ],
    }
    destination_stamp_snapshot_tree_workspace_state([snap])
    assert snap["data"]["workspace_row_state"] == WORKSPACE_ROW_STATE_CACHED_PROVISIONAL
    assert snap["children"][0]["data"]["workspace_row_state"] == WORKSPACE_ROW_STATE_PLANNED_ONLY
    assert snap["data"]["id"] == "x"


def test_replace_all_children_graph_child_bind_upgrades_provisional_in_place():
    app = QApplication.instance() or QApplication([])
    model = DestinationPlanningTreeModel()
    parent_pl = {
        "name": "Root",
        "is_folder": True,
        "item_path": "Root",
        "id": "root1",
        "workspace_row_state": WORKSPACE_ROW_STATE_LIVE_CONFIRMED,
        "drive_id": "d1",
    }
    model.reset_root_payloads([parent_pl])
    root_ix = model.index(0, 0, QModelIndex())
    prov = {
        "name": "Sub",
        "is_folder": True,
        "item_path": "Root\\Sub",
        "id": "sub-graph-id",
        "drive_id": "d1",
        "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
        "overlay_keep": "from_cache",
    }
    model.replace_all_children(root_ix, [prov])
    child0 = model.index(0, 0, root_ix)
    assert (child0.data(Qt.UserRole) or {}).get("workspace_row_state") == WORKSPACE_ROW_STATE_CACHED_PROVISIONAL

    graph_row = {
        "name": "Sub",
        "is_folder": True,
        "item_path": "Root\\Sub",
        "id": "sub-graph-id",
        "drive_id": "d1",
        "workspace_row_state": WORKSPACE_ROW_STATE_LIVE_CONFIRMED,
        "web_url": "https://example/updated",
    }
    model.replace_all_children(root_ix, [graph_row], graph_child_bind=True)
    child1 = model.index(0, 0, root_ix)
    merged = child1.data(Qt.UserRole) or {}
    assert merged.get("workspace_row_state") == WORKSPACE_ROW_STATE_LIVE_CONFIRMED
    assert merged.get("overlay_keep") == "from_cache"
    assert merged.get("web_url") == "https://example/updated"
    assert model.rowCount(root_ix) == 1
    _ = app
