"""Destination tree: lazy Graph child load on expand at arbitrary depth."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from PySide6.QtCore import QModelIndex, Qt

from ozlink_console.sharepoint_destination_overlay_attach import (
    WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
    WORKSPACE_ROW_STATE_LIVE_CONFIRMED,
)
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _live_folder(*, name: str, item_id: str, drive_id: str, item_path: str, children_loaded: bool = False) -> dict:
    return {
        "name": name,
        "id": item_id,
        "drive_id": drive_id,
        "library_id": drive_id,
        "is_folder": True,
        "item_path": item_path,
        "workspace_row_state": WORKSPACE_ROW_STATE_LIVE_CONFIRMED,
        "base_display_label": f"Folder: {name}",
        "tree_role": "destination",
        "children_loaded": bool(children_loaded),
        "load_failed": False,
    }


def test_substantive_destination_folder_child_row_count_excludes_loading_placeholder():
    model = DestinationPlanningTreeModel()
    parent = _live_folder(name="Finance", item_id="fin1", drive_id="drvA", item_path=r"Lib\Root3\Finance")
    model.reset_root_payloads([parent])
    fin_ix = model.index(0, 0, QModelIndex())
    model.replace_all_children(
        fin_ix,
        [
            {
                "placeholder": True,
                "placeholder_role": "loading_in_progress",
                "base_display_label": "Loading...",
                "tree_role": "destination",
            }
        ],
    )
    assert int(model.rowCount(fin_ix)) == 1
    assert model.substantive_destination_folder_child_row_count(fin_ix) == 0


def test_reconcile_nested_folder_clears_children_loaded_when_only_loading_placeholder():
    model = DestinationPlanningTreeModel()
    parent = _live_folder(
        name="Finance",
        item_id="fin1",
        drive_id="drvA",
        item_path=r"Lib\Root3\Finance",
        children_loaded=True,
    )
    model.reset_root_payloads([parent])
    fin_ix = model.index(0, 0, QModelIndex())
    model.replace_all_children(
        fin_ix,
        [
            {
                "placeholder": True,
                "placeholder_role": "loading_in_progress",
                "base_display_label": "Loading...",
                "tree_role": "destination",
            }
        ],
    )
    assert model.reconcile_folder_children_loaded_if_empty_subtree(fin_ix, reason="test") is True
    pl = fin_ix.data(Qt.UserRole) or {}
    assert pl.get("children_loaded") is False


def test_expanding_finance_schedules_child_load_via_main_window():
    from ozlink_console.main_window import MainWindow

    model = DestinationPlanningTreeModel()
    root = _live_folder(name="Root3", item_id="r1", drive_id="drvA", item_path=r"Lib\Root3", children_loaded=True)
    fin = _live_folder(
        name="Finance", item_id="fin1", drive_id="drvA", item_path=r"Lib\Root3\Finance", children_loaded=False
    )
    model.reset_root_payloads([root])
    r_ix = model.index(0, 0, QModelIndex())
    model.replace_all_children(r_ix, [fin])
    fin_ix = model.index(0, 0, r_ix)

    mw = MainWindow.__new__(MainWindow)
    mw.destination_planning_model = model
    mw.graph = MagicMock()
    mw.graph.token = "t"
    mw._resolve_tree_item_drive_id = lambda panel, pl: str(pl.get("drive_id") or "")
    mw._destination_row_is_live_graph_structure = MainWindow._destination_row_is_live_graph_structure.__get__(mw)
    mw._request_graph_destination_children_load = MagicMock(return_value=True)

    with patch(
        "ozlink_console.main_window.destination_authority_contract.graph_owns_visible_real_destination_structure",
        return_value=True,
    ):
        MainWindow._destination_expand_request_live_graph_folder_child_load(mw, fin_ix)

    mw._request_graph_destination_children_load.assert_called_once()
    kw = mw._request_graph_destination_children_load.call_args.kwargs
    assert kw.get("reason") == "user_expand"
    assert kw.get("trigger") == "tree_expanded_live_graph_folder"


def test_graph_returns_payroll_finance_payload_bind_count_matches(monkeypatch):
    """B: folder worker success inserts Graph children (model.replace_all_children)."""
    from ozlink_console.main_window import MainWindow

    model = DestinationPlanningTreeModel()
    fin = _live_folder(
        name="Finance", item_id="fin1", drive_id="drvA", item_path=r"Lib\Root3\Finance", children_loaded=False
    )
    model.reset_root_payloads([fin])
    fin_ix = model.index(0, 0, QModelIndex())

    mw = MainWindow.__new__(MainWindow)
    mw.destination_planning_model = model
    mw.destination_tree_widget = MagicMock()
    mw._memory_restore_in_progress = False
    mw.graph = MagicMock()
    mw.folder_load_workers = {
        "destination:fin1": {
            "id": 99,
            "destination_folder_parent_persistent": None,
            "item": None,
        }
    }
    mw._snapshot_branch_refresh_baseline_by_worker = {}
    mw._destination_preserved_children_by_worker = {}
    mw._count_visible_subtree_nodes_index = lambda ix: 1
    mw._count_folder_payload_nodes = lambda items: 0
    mw._destination_semantic_path = lambda nd: nd.get("item_path") or ""
    mw._destination_folder_expects_planned_workspace_underneath = lambda sp: False
    mw._destination_collect_planned_workspace_children_under_model = lambda ix: []
    mw._destination_planned_workspace_snapshot_identity_only_tree = lambda x: x
    mw._destination_register_planned_workspace_snapshot_for_parent_path = lambda *a, **k: None
    mw._destination_payload_from_graph_item = lambda c: {
        "name": c.get("name"),
        "id": c.get("id"),
        "drive_id": "drvA",
        "is_folder": True,
        "item_path": r"Lib\Root3\Finance\Payroll",
        "workspace_row_state": WORKSPACE_ROW_STATE_LIVE_CONFIRMED,
    }
    mw._apply_tree_item_visual_state = lambda *a, **k: None
    mw._destination_strip_terminal_empty_when_planned_descendants_expected = lambda *a, **k: None
    mw._destination_forensic_terminal_empty_context = lambda *a, **k: None
    mw._destination_graph_subtree_schedule_new_folders_after_bind = lambda *a, **k: None
    mw._destination_invoke_planned_workspace_reconcile_after_graph_folder_load = lambda *a, **k: None
    mw._destination_bump_interactive_expand_idle_full_tree_lockout = lambda *a, **k: None
    mw._expand_all_pending = {}
    mw._pending_snapshot_branch_refresh = {}
    mw._schedule_destination_expand_all_continue = lambda: None
    mw._continue_expand_all = lambda *a, **k: None
    mw._refresh_tree_column_width = lambda *a, **k: None
    mw._process_pending_destination_navigation = lambda *a, **k: None
    mw._schedule_workspace_ui_persist = lambda *a, **k: None
    mw._schedule_snapshot_branch_refresh = lambda *a, **k: None
    mw._schedule_progress_summary_refresh = lambda: None
    mw._mark_destination_real_tree_snapshot_stale = lambda: None
    mw._log_restore_exception = lambda *a, **k: None
    mw._destination_lifecycle_trace_TEMP = lambda *a, **k: None
    mw._dest_scroll_profiler = None

    payload = {
        "panel_key": "destination",
        "drive_id": "drvA",
        "item_id": "fin1",
        "items": [{"id": "pay1", "name": "Payroll", "is_folder": True}],
    }

    with patch(
        "ozlink_console.main_window.destination_authority_contract.graph_owns_visible_real_destination_structure",
        return_value=True,
    ):
        MainWindow.on_folder_load_success(mw, payload, 99)

    assert model.rowCount(fin_ix) == 1
    child = model.index(0, 0, fin_ix).data(Qt.UserRole) or {}
    assert child.get("name") == "Payroll"


def test_expanding_hr_second_level_schedules_child_load():
    from ozlink_console.main_window import MainWindow

    model = DestinationPlanningTreeModel()
    root = _live_folder(name="Root3", item_id="r1", drive_id="drvA", item_path=r"Lib\Root3", children_loaded=True)
    hr = _live_folder(
        name="HR", item_id="hr1", drive_id="drvA", item_path=r"Lib\Root3\HR", children_loaded=False
    )
    model.reset_root_payloads([root])
    r_ix = model.index(0, 0, QModelIndex())
    model.replace_all_children(r_ix, [hr])
    hr_ix = model.index(0, 0, r_ix)

    mw = MainWindow.__new__(MainWindow)
    mw.destination_planning_model = model
    mw.graph = MagicMock()
    mw.graph.token = "t"
    mw._resolve_tree_item_drive_id = lambda panel, pl: str(pl.get("drive_id") or "")
    mw._request_graph_destination_children_load = MagicMock(return_value=True)

    with patch(
        "ozlink_console.main_window.destination_authority_contract.graph_owns_visible_real_destination_structure",
        return_value=True,
    ):
        MainWindow._destination_expand_request_live_graph_folder_child_load(mw, hr_ix)

    mw._request_graph_destination_children_load.assert_called_once()


def test_children_loaded_true_zero_substantive_rows_resets_via_reconcile_before_request():
    from ozlink_console.main_window import MainWindow

    model = DestinationPlanningTreeModel()
    fin = _live_folder(
        name="Finance",
        item_id="fin1",
        drive_id="drvA",
        item_path=r"Lib\Root3\Finance",
        children_loaded=True,
    )
    model.reset_root_payloads([fin])
    fin_ix = model.index(0, 0, QModelIndex())
    assert (fin_ix.data(Qt.UserRole) or {}).get("children_loaded") is True

    mw = MainWindow.__new__(MainWindow)
    mw.destination_planning_model = model
    mw.graph = MagicMock()
    mw.graph.token = "t"
    mw.folder_load_workers = {}
    mw.pending_folder_loads = {"destination": set()}
    mw._resolve_tree_item_drive_id = lambda panel, pl: str(pl.get("drive_id") or "")
    mw._destination_row_is_live_graph_structure = MainWindow._destination_row_is_live_graph_structure.__get__(mw)
    mw._folder_load_worker_thread_running = lambda wk: False
    mw._destination_row_raw_path_for_path_lookup_match = lambda nd: nd.get("item_path") or ""
    mw._destination_graph_child_load_backlog_keys = set()
    mw._destination_graph_child_load_backlog_hi = []
    mw._destination_graph_child_load_backlog_lo = []
    mw._destination_start_graph_folder_load_worker_if_eligible = MagicMock(return_value=True)

    with patch(
        "ozlink_console.main_window.destination_authority_contract.graph_owns_visible_real_destination_structure",
        return_value=True,
    ):
        MainWindow._request_graph_destination_children_load(
            mw, fin_ix, reason="user_expand", trigger="test"
        )

    mw._destination_start_graph_folder_load_worker_if_eligible.assert_called_once()


def test_missing_item_id_expand_does_not_crash():
    from ozlink_console.main_window import MainWindow

    pl = _live_folder(name="X", item_id="", drive_id="d1", item_path=r"P\X")
    pl.pop("id", None)
    model = DestinationPlanningTreeModel()
    model.reset_root_payloads([pl])
    ix = model.index(0, 0, QModelIndex())

    mw = MainWindow.__new__(MainWindow)
    mw.destination_planning_model = model
    mw.graph = MagicMock()
    mw._resolve_tree_item_drive_id = lambda panel, p: str(p.get("drive_id") or "")

    with patch(
        "ozlink_console.main_window.destination_authority_contract.graph_owns_visible_real_destination_structure",
        return_value=True,
    ):
        MainWindow._destination_expand_request_live_graph_folder_child_load(mw, ix)


def test_already_loaded_folder_with_real_children_does_not_request_again():
    from ozlink_console.main_window import MainWindow

    model = DestinationPlanningTreeModel()
    fin = _live_folder(
        name="Finance",
        item_id="fin1",
        drive_id="drvA",
        item_path=r"Lib\Root3\Finance",
        children_loaded=True,
    )
    model.reset_root_payloads([fin])
    fin_ix = model.index(0, 0, QModelIndex())
    model.replace_all_children(
        fin_ix,
        [
            _live_folder(
                name="Payroll",
                item_id="p1",
                drive_id="drvA",
                item_path=r"Lib\Root3\Finance\Payroll",
                children_loaded=False,
            )
        ],
    )

    mw = MainWindow.__new__(MainWindow)
    mw.destination_planning_model = model
    mw.graph = MagicMock()
    mw._resolve_tree_item_drive_id = lambda panel, pl: str(pl.get("drive_id") or "")
    mw._request_graph_destination_children_load = MagicMock(return_value=True)

    with patch(
        "ozlink_console.main_window.destination_authority_contract.graph_owns_visible_real_destination_structure",
        return_value=True,
    ):
        MainWindow._destination_expand_request_live_graph_folder_child_load(mw, fin_ix)

    mw._request_graph_destination_children_load.assert_not_called()


def test_unresolved_destination_library_blocks_expand():
    from ozlink_console.main_window import MainWindow

    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda pk: "sharepoint"
    mw._destination_library_context_unresolved_for_graph_display = lambda: True
    payload = _live_folder(
        name="Any", item_id="a1", drive_id="d1", item_path="P", children_loaded=False
    )
    mock_index = MagicMock()
    mock_index.isValid.return_value = True
    mock_index.column.return_value = 0
    mock_index.siblingAtColumn.return_value = mock_index
    mock_index.data.side_effect = lambda role, p=payload: p if role == Qt.UserRole else None

    with patch("ozlink_console.main_window.log_info") as log:
        MainWindow._on_destination_planning_model_expanded(mw, mock_index)
    msgs = [c.args[0] for c in log.call_args_list if c.args]
    assert "destination_expand_blocked_library_unresolved" in msgs


def test_provisional_destination_snapshot_deferred_when_library_unresolved():
    """FIX1: no reset_nested while Destination Library selector is unresolved."""
    from ozlink_console.main_window import MainWindow

    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda pk: "sharepoint"
    mw._destination_tree_uses_model_view = lambda: True
    mw.destination_planning_model = DestinationPlanningTreeModel()
    mw.destination_tree_widget = MagicMock()
    mw._pending_session_tree_snapshots = {"destination": [{"name": "Root3", "semantic_path": r"Lib\Root3"}]}
    mw._runtime_session_tree_snapshots = {"source": [], "destination": []}
    mw._intended_destination_drive_id_for_snapshot_validation = lambda: "drv_snap"
    mw._destination_library_context_unresolved_for_graph_display = lambda: True
    mw.set_tree_placeholder = MagicMock()

    with patch("ozlink_console.main_window.sanitize_destination_startup_snapshot_top_level") as san:
        with patch.object(mw.destination_planning_model, "reset_nested") as rn:
            ok = MainWindow._destination_apply_provisional_session_snapshot_if_eligible(mw, phase="test")

    assert ok is False
    assert san.call_count == 0
    rn.assert_not_called()
    mw.set_tree_placeholder.assert_called()
    ph_args = mw.set_tree_placeholder.call_args[0]
    assert ph_args[0] == "destination"
    assert "destination library" in ph_args[1].lower()


def test_provisional_destination_snapshot_rejected_drive_mismatch():
    """FIX1: envelope drive id must match selected library drive id."""
    from ozlink_console.main_window import MainWindow

    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda pk: "sharepoint"
    mw._destination_tree_uses_model_view = lambda: True
    mw.destination_planning_model = DestinationPlanningTreeModel()
    mw.destination_tree_widget = MagicMock()
    mw._pending_session_tree_snapshots = {"destination": [{"name": "Root3"}]}
    mw._runtime_session_tree_snapshots = {"source": [], "destination": []}
    mw._intended_destination_drive_id_for_snapshot_validation = lambda: "drive_AAA"
    mw._destination_library_context_unresolved_for_graph_display = lambda: False
    mw._current_selected_destination_drive_id = lambda: "drive_BBB"
    mw.set_tree_placeholder = MagicMock()

    with patch("ozlink_console.main_window.sanitize_destination_startup_snapshot_top_level") as san:
        with patch.object(mw.destination_planning_model, "reset_nested") as rn:
            ok = MainWindow._destination_apply_provisional_session_snapshot_if_eligible(mw, phase="test")

    assert ok is False
    assert san.call_count == 0
    rn.assert_not_called()


def test_expand_maybe_reset_cached_provisional_finance_then_request_schedules_load():
    """FIX2: cached_provisional + children_loaded + empty substantive -> Graph child request."""
    from ozlink_console.main_window import MainWindow

    fin_pl = {
        "name": "Finance",
        "id": "fin1",
        "drive_id": "drvA",
        "library_id": "drvA",
        "is_folder": True,
        "item_path": r"Lib\Root3\Finance",
        "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
        "base_display_label": "Folder: Finance",
        "tree_role": "destination",
        "children_loaded": True,
        "load_failed": False,
        "verification_state": "live_confirmed",
        "row_kind": "live_folder",
    }
    model = DestinationPlanningTreeModel()
    model.reset_root_payloads([dict(fin_pl)])
    fin_ix = model.index(0, 0, QModelIndex())

    mw = MainWindow.__new__(MainWindow)
    mw.destination_planning_model = model
    mw._resolve_tree_item_drive_id = lambda panel, pl: str(pl.get("drive_id") or "")
    mw._destination_library_context_unresolved_for_graph_display = lambda: False
    mw._current_selected_destination_drive_id = lambda: "drvA"
    mw._request_graph_destination_children_load = MagicMock(return_value=True)
    mw.graph = MagicMock()
    mw.graph.token = "t"

    nd = dict(fin_ix.data(Qt.UserRole) or {})
    nd2 = MainWindow._destination_expand_maybe_reset_stale_graph_children_loaded(mw, fin_ix, nd)
    assert nd2.get("children_loaded") is False

    with patch(
        "ozlink_console.main_window.destination_authority_contract.graph_owns_visible_real_destination_structure",
        return_value=True,
    ):
        MainWindow._destination_expand_request_live_graph_folder_child_load(mw, fin_ix)

    mw._request_graph_destination_children_load.assert_called_once()


def test_planned_only_row_blocked_from_graph_lazy_load_reset():
    from ozlink_console.main_window import MainWindow

    pl = {
        "name": "Plan",
        "id": "",
        "drive_id": "drvA",
        "is_folder": True,
        "verification_state": "planned_only",
        "row_kind": "planned_folder",
        "workspace_row_state": "planned_only",
        "children_loaded": True,
        "base_display_label": "Folder: Plan",
        "tree_role": "destination",
    }
    model = DestinationPlanningTreeModel()
    model.reset_root_payloads([pl])
    ix = model.index(0, 0, QModelIndex())
    mw = MainWindow.__new__(MainWindow)
    mw.destination_planning_model = model
    mw._resolve_tree_item_drive_id = lambda panel, p: str(p.get("drive_id") or "")
    mw._destination_library_context_unresolved_for_graph_display = lambda: False
    mw._current_selected_destination_drive_id = lambda: "drvA"

    out = MainWindow._destination_expand_maybe_reset_stale_graph_children_loaded(mw, ix, dict(pl))
    assert out.get("children_loaded") is True
