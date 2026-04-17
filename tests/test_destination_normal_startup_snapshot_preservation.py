"""Normal startup: preserve deep destination snapshot across Graph root bind (Option 3)."""

from __future__ import annotations

import os
from unittest.mock import MagicMock

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication, QLabel, QTreeView

from ozlink_console.main_window import MainWindow
from ozlink_console.models import SessionState
from ozlink_console.sharepoint_destination_overlay_attach import WORKSPACE_ROW_STATE_CACHED_PROVISIONAL
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _app():
    return QApplication.instance() or QApplication([])


def _graph_index_key_mw(mw):
    return MainWindow._destination_payload_index_key.__get__(mw, MainWindow)


def test_merge_root_preserves_nested_children_when_graph_updates_same_id():
    dm = DestinationPlanningTreeModel(destination_index_key_fn=lambda pl: str((pl or {}).get("item_path") or ""))
    dm.reset_root_payloads(
        [
            {
                "name": "Hub",
                "is_folder": True,
                "item_path": "Root3",
                "id": "g1",
                "workspace_row_state": "cached_provisional",
                "tree_role": "destination",
            }
        ]
    )
    hub_ix = dm.index(0, 0, QModelIndex())
    dm.append_child_payloads(
        hub_ix,
        [
            {
                "name": "Deep",
                "is_folder": True,
                "item_path": r"Root3\Deep",
                "id": "",
                "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
                "tree_role": "destination",
            }
        ],
    )
    graph_payloads = [
        {
            "name": "Hub",
            "is_folder": True,
            "item_path": "Root3",
            "id": "g1",
            "drive_id": "d1",
        }
    ]
    st = dm.merge_sharepoint_library_root_graph_children(graph_payloads)
    assert st["updated"] == 1
    assert dm.rowCount(hub_ix) == 1
    ch = dm.index(0, 0, hub_ix)
    assert (ch.data(Qt.UserRole) or {}).get("name") == "Deep"


def test_merge_rebinds_root_in_place_when_graph_id_changes_but_path_matches():
    """Graph may return a new driveItem id for the same library root — do not remove/reinsert the row (no subtree churn)."""
    dm = DestinationPlanningTreeModel(destination_index_key_fn=lambda pl: str((pl or {}).get("item_path") or ""))
    dm.reset_root_payloads(
        [
            {
                "name": "Root3",
                "is_folder": True,
                "item_path": "Root3",
                "id": "old-session-id",
                "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
                "tree_role": "destination",
            }
        ]
    )
    hub_ix = dm.index(0, 0, QModelIndex())
    dm.append_child_payloads(
        hub_ix,
        [
            {
                "name": "Nested",
                "is_folder": True,
                "item_path": r"Root3\Nested",
                "id": "",
                "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
                "tree_role": "destination",
            }
        ],
    )
    graph_payloads = [
        {
            "name": "Root3",
            "is_folder": True,
            "item_path": "Root3",
            "id": "fresh-graph-id",
            "drive_id": "d1",
        }
    ]
    st = dm.merge_sharepoint_library_root_graph_children(graph_payloads)
    assert st["updated"] == 1
    assert st["removed"] == 0
    assert st["inserted"] == 0
    assert dm.rowCount(QModelIndex()) == 1
    hub2 = dm.index(0, 0, QModelIndex())
    plh = hub2.data(Qt.UserRole) or {}
    assert plh.get("id") == "fresh-graph-id"
    assert dm.rowCount(hub2) == 1


def test_merge_preserves_root_without_graph_id_when_cached_provisional():
    dm = DestinationPlanningTreeModel(destination_index_key_fn=lambda pl: str((pl or {}).get("item_path") or ""))
    dm.reset_root_payloads(
        [
            {
                "name": "Shell",
                "is_folder": True,
                "item_path": "ShellPath",
                "id": "",
                "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
                "row_kind": "cached_provisional_shell",
                "tree_role": "destination",
            }
        ]
    )
    st = dm.merge_sharepoint_library_root_graph_children(
        [{"name": "Real", "is_folder": True, "item_path": "R", "id": "x1", "drive_id": "d1"}]
    )
    assert st["inserted"] >= 1
    assert dm.rowCount(QModelIndex()) >= 2


def test_should_run_startup_projection_with_session_snapshot_not_import_gate(monkeypatch):
    monkeypatch.setattr(
        "ozlink_console.destination_authority_contract.graph_owns_visible_real_destination_structure",
        lambda _h: True,
    )
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw.planned_moves = [{"destination_path": r"Root3\A\x.pdf", "DestinationId": "graph-id-present"}]
    mw.proposed_folders = []
    mw._planning_browse_mode = lambda _k: "sharepoint"
    mw._draft_shell_state = SessionState(DestinationTreeSnapshot=[{"data": {"name": "n"}, "children": []}])
    mw._runtime_session_tree_snapshots = {"source": [], "destination": []}
    mw._pending_session_tree_snapshots = {}
    assert MainWindow._destination_should_run_startup_projection_materialization(mw) is True


def test_apply_root_payload_uses_merge_when_provisional_flag(monkeypatch):
    monkeypatch.setattr(
        "ozlink_console.destination_authority_contract.graph_owns_visible_real_destination_structure",
        lambda _h: True,
    )
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda _k: "sharepoint"
    mw._destination_provisional_startup_applied = True
    mw.pending_root_drive_ids = {"destination": "did"}
    mw._current_selected_destination_drive_id = lambda: "did"
    mw._schedule_destination_authority_shell_watchdog = lambda *_a, **_k: None
    mw._ensure_sharepoint_destination_full_tree_worker_scheduled = lambda *_a, **_k: None
    for n in (
        "_destination_payload_from_graph_item",
        "_apply_tree_item_visual_state",
        "_destination_lifecycle_trace_TEMP",
        "_mark_destination_real_tree_snapshot_stale",
        "_destination_should_run_startup_projection_materialization",
        "_destination_run_startup_projection_materialization",
        "_get_tree_and_status",
        "_apply_root_payload_to_destination_model_view",
    ):
        setattr(mw, n, getattr(MainWindow, n).__get__(mw, MainWindow))
    mw._log_restore_phase = lambda *a, **k: None
    mw._set_tree_status_message = lambda *a, **k: None
    mw._destination_should_run_startup_projection_materialization = lambda: False
    mw._destination_run_startup_projection_materialization = lambda **k: None

    dm = DestinationPlanningTreeModel(destination_index_key_fn=_graph_index_key_mw(mw))
    dm.reset_root_payloads(
        [
            {
                "name": "Root3",
                "is_folder": True,
                "item_path": "Root3",
                "id": "rid",
                "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
                "tree_role": "destination",
            }
        ]
    )
    hub = dm.index(0, 0, QModelIndex())
    dm.append_child_payloads(
        hub,
        [
            {
                "name": "KeepMe",
                "is_folder": True,
                "item_path": r"Root3\KeepMe",
                "id": "",
                "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
                "tree_role": "destination",
            }
        ],
    )
    tw = QTreeView()
    tw.setModel(dm)
    mw.destination_planning_model = dm
    mw.destination_tree_widget = tw
    mw.destination_tree_status = QLabel()
    # Graph returns a new item id for the same root path — merge must update in place (no extra root insert).
    items = [{"name": "Root3", "is_folder": True, "id": "graph-id-after-bind", "item_path": "Root3"}]
    mw._apply_root_payload_to_destination_model_view("destination", items)
    assert dm.rowCount(hub) == 1
    pl0 = hub.data(Qt.UserRole) or {}
    assert pl0.get("workspace_row_state") == "live_confirmed"
    assert pl0.get("id") == "graph-id-after-bind"
    deep = dm.index(0, 0, hub)
    deep_pl = deep.data(Qt.UserRole) or {}
    assert deep_pl.get("name") == "KeepMe"
    assert deep_pl.get("workspace_row_state") == WORKSPACE_ROW_STATE_CACHED_PROVISIONAL
    assert getattr(mw, "_destination_require_deferred_full_materialize_once", True) is False


def test_startup_projection_visibility_pass_skips_graph_child_load_for_leaf_allocation(monkeypatch):
    monkeypatch.setattr(
        "ozlink_console.destination_authority_contract.graph_owns_visible_real_destination_structure",
        lambda _h: True,
    )
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw._destination_startup_projection_visibility_pass_active = True
    graph_load = MagicMock()
    mw._request_graph_destination_children_load = graph_load

    dm = DestinationPlanningTreeModel(destination_index_key_fn=lambda pl: str((pl or {}).get("item_path") or ""))
    dm.reset_root_payloads(
        [
            {
                "name": "Alloc",
                "is_folder": True,
                "item_path": r"lib\Alloc",
                "id": "folder-id",
                "tree_role": "destination",
                "planned_allocation": True,
                "children_loaded": False,
                "load_failed": False,
                "allocation_descendants_applied": False,
            }
        ]
    )
    ix = dm.index(0, 0, QModelIndex())
    mw.destination_planning_model = dm
    mw.destination_tree_widget = QTreeView()
    mw._find_planned_move_for_destination_node = lambda _nd: {"source": {"is_folder": False}}
    mw._invalidate_stale_destination_allocation_projection_index = lambda _i, nd, _m: dict(nd)
    mw._remove_placeholder_children = lambda _i: None
    mw._destination_remove_deferred_file_summary_children_index = lambda _i: None
    mw._tree_item_path = lambda d: str((d or {}).get("item_path") or "")
    mw._full_trace_enabled = lambda: False
    mw._destination_forensic_planned_item_materialization_logging_enabled = lambda: False
    mw._refresh_destination_item_visibility_index = lambda _i: None
    mw._apply_tree_item_visual_state = lambda *_a, **_k: None
    mw._load_destination_projected_descendants_index = MainWindow._load_destination_projected_descendants_index.__get__(
        mw, MainWindow
    )
    mw._load_destination_projected_descendants_index(ix)
    graph_load.assert_not_called()


def test_merge_enrich_only_keeps_snapshot_root_row_when_not_in_shallow_graph_list():
    """Quiet startup: shallow root listing must enrich, not prune, rows missing from Graph children."""
    dm = DestinationPlanningTreeModel(destination_index_key_fn=lambda pl: str((pl or {}).get("item_path") or ""))
    dm.reset_root_payloads(
        [
            {
                "name": "OnlyInSnapshot",
                "is_folder": True,
                "item_path": "OnlyInSnapshot",
                "id": "snap-only",
                "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
                "tree_role": "destination",
            }
        ]
    )
    st = dm.merge_sharepoint_library_root_graph_children(
        [{"name": "Other", "is_folder": True, "item_path": "Other", "id": "g-other", "drive_id": "d1"}],
        enrich_only=True,
    )
    assert st["removed"] == 0
    assert dm.rowCount(QModelIndex()) == 2


def test_destination_snapshot_nested_spec_marks_folders_children_loaded():
    """Persisted JSON often keeps ``children_loaded`` false; restore must still treat rows as materialized."""
    mw = MainWindow.__new__(MainWindow)
    snap = {
        "data": {
            "name": "Parent",
            "is_folder": True,
            "item_path": r"lib\Parent",
            "children_loaded": False,
            "id": "p1",
            "tree_role": "destination",
        },
        "children": [
            {
                "data": {
                    "name": "Child",
                    "is_folder": False,
                    "item_path": r"lib\Parent\Child",
                    "id": "c1",
                    "tree_role": "destination",
                },
                "children": [],
            }
        ],
    }
    spec = MainWindow._destination_tree_snapshot_dict_to_nested_spec(mw, snap)
    assert spec is not None
    pl, kids = spec
    assert pl.get("children_loaded") is True
    assert pl.get("load_failed") is False
    assert len(kids) == 1


def test_reset_nested_from_snapshot_folder_empty_children_is_materialized_not_lazy():
    """Folders with no snapshot children must report rowCount 0 as authoritative empty, not lazy-unloaded."""
    dm = DestinationPlanningTreeModel(destination_index_key_fn=lambda pl: str((pl or {}).get("item_path") or ""))
    snap = {
        "data": {
            "name": "EmptyFolder",
            "is_folder": True,
            "item_path": r"lib\EmptyFolder",
            "children_loaded": False,
            "id": "e1",
            "tree_role": "destination",
        },
        "children": [],
    }
    spec = MainWindow._destination_tree_snapshot_dict_to_nested_spec(MainWindow.__new__(MainWindow), snap)
    assert spec is not None
    dm.reset_nested([spec])
    ix = dm.index(0, 0, QModelIndex())
    pl = ix.data(Qt.UserRole) or {}
    assert pl.get("children_loaded") is True
    assert dm.rowCount(ix) == 0
    assert not dm.hasChildren(ix)


def test_merge_sharepoint_root_twice_is_idempotent_for_nested_snapshot():
    dm = DestinationPlanningTreeModel(destination_index_key_fn=lambda pl: str((pl or {}).get("item_path") or ""))
    dm.reset_root_payloads(
        [
            {
                "name": "R",
                "is_folder": True,
                "item_path": "R",
                "id": "r1",
                "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
                "tree_role": "destination",
            }
        ]
    )
    r_ix = dm.index(0, 0, QModelIndex())
    dm.append_child_payloads(
        r_ix,
        [
            {
                "name": "Nested",
                "is_folder": True,
                "item_path": r"R\Nested",
                "id": "",
                "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
                "tree_role": "destination",
            }
        ],
    )
    gp = [{"name": "R", "is_folder": True, "item_path": "R", "id": "r1", "drive_id": "d"}]
    dm.merge_sharepoint_library_root_graph_children(gp)
    n1 = sum(1 for _ in dm.iter_depth_first())
    dm.merge_sharepoint_library_root_graph_children(gp)
    n2 = sum(1 for _ in dm.iter_depth_first())
    assert n1 == n2 == 2
