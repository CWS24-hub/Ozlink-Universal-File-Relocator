"""Target path and planned-move identity resolution for destination UI actions."""

from __future__ import annotations

from unittest.mock import MagicMock

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtGui import QStandardItem, QStandardItemModel
from PySide6.QtWidgets import QAbstractItemView, QApplication

from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _qapp():
    return QApplication.instance() or QApplication([])


def test_destination_row_semantic_path_prefers_visible_columns_over_destination_path_field():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    node = {
        "display_path": "Root\\Finance",
        "item_path": "Root\\Finance",
        "destination_path": "Root\\Finance\\Payroll",
        "is_folder": True,
        "tree_role": "destination",
    }
    sem = mw._destination_row_semantic_path(node)
    assert "Payroll" not in sem
    assert sem == mw._canonical_destination_projection_path("Root\\Finance")


def test_destination_target_path_for_node_uses_row_semantic_path():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    node = {
        "display_path": "Root\\A\\B",
        "destination_path": "Root\\Z",
        "tree_role": "destination",
    }
    assert mw._destination_target_path_for_node(node) == mw._destination_row_semantic_path(node)


def test_find_planned_move_index_matches_allocation_path_before_node_keys():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.planned_moves = [
        {
            "destination_path": "Root\\Finance",
            "destination": {
                "display_path": "Root\\Finance",
                "item_path": "Root\\Finance",
                "tree_role": "destination",
            },
            "target_name": "Payroll",
            "source": {"name": "Payroll", "is_folder": True},
        },
    ]
    node = {
        "display_path": "Root\\Finance\\Payroll",
        "item_path": "Root\\Finance\\Payroll",
        "tree_role": "destination",
        "planned_allocation": True,
        "id": "allocated::Root\\Finance\\Payroll",
    }
    assert mw.find_planned_move_index_by_destination(node) == 0


def test_build_planned_move_record_destination_path_uses_semantic_path():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    src = {"name": "Doc", "display_path": "S\\Doc", "item_path": "S\\Doc", "id": "s1"}
    dest = {
        "name": "Nested",
        "display_path": "Root\\Finance\\Nested",
        "item_path": "Root\\Finance\\Nested",
        "destination_path": "Root\\Other",
        "id": "d1",
        "is_folder": True,
        "tree_role": "destination",
    }
    rec = mw.build_planned_move_record(src, dest)
    assert "Other" not in rec["destination_path"]
    assert rec["destination_path"] == mw._destination_row_semantic_path(dest)


def test_paste_here_pins_context_row_without_target_path_scan():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    target_ix = MagicMock()
    target_ix.isValid.return_value = True
    target_ix.column.return_value = 0
    source_ix = MagicMock()
    source_ix.isValid.return_value = True
    source_ix.column.return_value = 0
    find_calls = []

    def find_vis(path):
        find_calls.append(path)
        return source_ix

    def gdata(ix):
        if ix is target_ix:
            return {
                "is_folder": True,
                "tree_role": "destination",
                "node_origin": "Real",
                "display_path": "Root\\Finance\\Nested",
                "item_path": "Root\\Finance\\Nested",
            }
        if ix is source_ix:
            return {
                "name": "Item",
                "display_path": "Root\\Src\\Item",
                "item_path": "Root\\Src\\Item",
                "tree_role": "destination",
            }
        return None

    draft_args = {}

    def draft(s, t, **_kwargs):
        draft_args["source"] = s
        draft_args["target"] = t

    mw._destination_cut_buffer = {"path": "Root\\Src\\Item", "display_path": "Root\\Src\\Item"}
    mw._normalize_paste_destination_row_ref = lambda r: r
    mw._paste_here_destination_row_allowed = lambda pl: True
    mw.get_tree_item_node_data = gdata
    mw._destination_row_semantic_path = lambda n: str(
        n.get("item_path") or n.get("display_path") or ""
    )
    mw._destination_tree_index_if_current_matches_path = lambda p: None
    mw._find_visible_destination_item_by_path = find_vis
    mw.handle_destination_draft_move = draft
    mw._perf_explorer_log = lambda *a, **k: None

    mw.handle_paste_destination_item({"display_path": "ignored"}, paste_target_item=target_ix)

    assert draft_args.get("target") is target_ix
    assert len(find_calls) == 1


def test_paste_here_nested_semantic_path_from_captured_payload():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    nested = {
        "is_folder": True,
        "tree_role": "destination",
        "node_origin": "Real",
        "display_path": "Root\\Finance\\Employee Hours\\2025-26",
        "item_path": "Root\\Finance\\Employee Hours\\2025-26",
    }
    sem = mw._destination_row_semantic_path(nested)
    assert "Employee Hours" in sem.replace("/", "\\")
    assert "2025-26" in sem.replace("/", "\\")


def test_paste_here_row_eligibility_allows_allocated_folder_container():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    pl = {
        "is_folder": True,
        "planned_allocation": True,
        "node_origin": "PlannedAllocation",
        "display_path": "Root\\Finance\\Employee Hours",
        "item_path": "Root\\Finance\\Employee Hours",
        "tree_role": "destination",
    }
    ok, reason = mw._paste_here_row_eligibility(pl)
    assert ok
    assert reason == "allowed_allocated_folder_container"
    assert mw._paste_here_destination_row_allowed(pl) is True


def test_paste_here_row_eligibility_normal_folder():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    pl = {
        "is_folder": True,
        "node_origin": "Real",
        "display_path": "Root\\Finance\\Payroll",
        "item_path": "Root\\Finance\\Payroll",
        "tree_role": "destination",
    }
    ok, reason = mw._paste_here_row_eligibility(pl)
    assert ok
    assert reason == "allowed_normal_folder"


def test_paste_here_row_eligibility_denies_projected_descendant():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    pl = {
        "is_folder": True,
        "node_origin": "projectedallocationdescendant",
        "tree_role": "destination",
    }
    ok, reason = mw._paste_here_row_eligibility(pl)
    assert not ok
    assert "projected" in reason


def test_handle_destination_draft_move_accepts_allocated_folder_only_for_paste_kwarg():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    src_ix = MagicMock()
    tgt_ix = MagicMock()
    alloc_folder = {
        "is_folder": True,
        "planned_allocation": True,
        "node_origin": "PlannedAllocation",
        "display_path": "Root\\A\\B",
        "item_path": "Root\\A\\B",
    }
    source_node = {"name": "Item", "display_path": "Root\\S\\Item", "item_path": "Root\\S\\Item"}

    def gdata(ix):
        if ix is src_ix:
            return source_node
        if ix is tgt_ix:
            return alloc_folder
        return None

    mw.get_tree_item_node_data = gdata
    mw.node_is_proposed = lambda n: False
    moved = []

    def do_move(s, t, **kwargs):
        moved.append(True)
        return True

    mw._move_planned_destination_node = do_move
    mw.refresh_planned_moves_table = lambda: None
    mw._schedule_deferred_destination_materialization = lambda *a, **k: None
    mw._persist_planning_change = lambda *a, **k: None
    mw.planned_moves_status = MagicMock()
    mw.destination_tree_status = MagicMock()
    mw.clear_selection_details = lambda: None
    mw._perf_explorer_log = lambda *a, **k: None

    mw.handle_destination_draft_move(src_ix, tgt_ix, _paste_here_target=True)
    assert moved == [True]

    moved.clear()
    mw.handle_destination_draft_move(src_ix, tgt_ix)
    assert moved == []


def test_normalize_paste_destination_row_ref_column_zero():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._destination_tree_uses_model_view = lambda: True
    model = QStandardItemModel()
    model.setColumnCount(3)
    row0 = QStandardItem("c0")
    row1 = QStandardItem("c1")
    row2 = QStandardItem("c2")
    model.appendRow([row0, row1, row2])
    ix_col1 = model.indexFromItem(row1)
    assert ix_col1.column() == 1
    out = mw._normalize_paste_destination_row_ref(ix_col1)
    assert out.column() == 0
    assert out.row() == ix_col1.row()


def test_move_planned_from_paste_uses_lightweight_persist_when_quick_apply_succeeds():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    move = {
        "source_name": "Doc.xlsx",
        "target_name": "Doc.xlsx",
        "source_path": "Src\\Doc.xlsx",
        "destination_path": "Root\\OldParent",
        "destination_id": "d1",
        "destination_name": "OldParent",
        "destination": {
            "id": "d1",
            "name": "OldParent",
            "display_path": "Root\\OldParent",
            "item_path": "Root\\OldParent",
        },
        "source": {"name": "Doc.xlsx"},
        "status": "Draft",
    }
    mw.planned_moves = [move]
    mw._resolve_planned_move_for_destination_node = lambda node: (0, move, None)
    mw._is_move_submitted = lambda m: False
    mw._destination_row_semantic_path = lambda n: str(n.get("display_path") or n.get("item_path") or "")
    mw._find_visible_destination_item_by_path = lambda path: None
    mw._quick_remove_planned_move_from_destination_tree = lambda old: 0
    mw._reset_unresolved_allocation_queue = lambda: None
    mw._allocation_parent_candidates_for_touch_paths = lambda _t: {"Root\\NewParent"}
    mw._reapply_allocation_overlays_for_paste_touch_paths = lambda o, n, t: (1, {"Root\\NewParent": 1})
    mw._paths_equivalent = lambda a, b, role: str(a).replace("/", "\\") == str(b).replace("/", "\\")
    scheduled = []
    mw._schedule_deferred_destination_materialization = lambda r, delay_ms=180: scheduled.append((r, delay_ms))
    lightweight = []
    mw._persist_planning_change_lightweight = lambda **kw: lightweight.append(kw)
    overlay_bumps = []
    mw._bump_destination_materialized_overlay_fingerprint = lambda **kw: overlay_bumps.append(kw)

    def heavy_persist(r):
        raise AssertionError("heavy persist should not run when quick apply succeeds")

    mw._persist_planning_change = heavy_persist
    mw.refresh_planned_moves_table = lambda: None
    mw.planned_moves_status = MagicMock()
    mw.destination_tree_status = MagicMock()

    src = {"name": "Doc.xlsx", "display_path": "Root\\OldParent\\Doc.xlsx", "item_path": "Root\\OldParent\\Doc.xlsx"}
    tgt = {"name": "NewParent", "display_path": "Root\\NewParent", "item_path": "Root\\NewParent", "is_folder": True}

    assert mw._move_planned_destination_node(src, tgt, from_paste_here=True) is True
    assert not scheduled
    assert len(lightweight) == 1
    assert len(overlay_bumps) == 1
    assert overlay_bumps[0].get("phase") == "paste_touch_reapply"
    assert lightweight[0].get("planning_refresh_reason") == "planned_item_moved_paste_quick"
    assert move["destination_path"] == "Root\\NewParent"


def test_move_planned_from_paste_falls_back_finalize_when_quick_apply_zero():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    move = {
        "source_name": "Doc.xlsx",
        "target_name": "Doc.xlsx",
        "source_path": "Src\\Doc.xlsx",
        "destination_path": "Root\\OldParent",
        "destination_id": "d1",
        "destination_name": "OldParent",
        "destination": {
            "id": "d1",
            "name": "OldParent",
            "display_path": "Root\\OldParent",
            "item_path": "Root\\OldParent",
        },
        "source": {"name": "Doc.xlsx"},
        "status": "Draft",
    }
    mw.planned_moves = [move]
    mw._resolve_planned_move_for_destination_node = lambda node: (0, move, None)
    mw._is_move_submitted = lambda m: False
    mw._destination_row_semantic_path = lambda n: str(n.get("display_path") or n.get("item_path") or "")
    mw._find_visible_destination_item_by_path = lambda path: None
    mw._quick_remove_planned_move_from_destination_tree = lambda old: 0
    mw._reset_unresolved_allocation_queue = lambda: None
    mw._allocation_parent_candidates_for_touch_paths = lambda _t: set()
    mw._reapply_allocation_overlays_for_paste_touch_paths = lambda o, n, t: (0, {})
    mw._paths_equivalent = lambda a, b, role: False
    scheduled = []
    mw._schedule_deferred_destination_materialization = lambda r, delay_ms=180: scheduled.append((r, delay_ms))
    mw._persist_planning_change_lightweight = lambda: (_ for _ in ()).throw(
        AssertionError("lightweight should not run when quick apply fails")
    )
    heavy = []
    mw._persist_planning_change = lambda r: heavy.append(r)
    mw.refresh_planned_moves_table = lambda: None
    mw.planned_moves_status = MagicMock()
    mw.destination_tree_status = MagicMock()

    src = {"name": "Doc.xlsx", "display_path": "Root\\OldParent\\Doc.xlsx", "item_path": "Root\\OldParent\\Doc.xlsx"}
    tgt = {"name": "NewParent", "display_path": "Root\\NewParent", "item_path": "Root\\NewParent", "is_folder": True}

    assert mw._move_planned_destination_node(src, tgt, from_paste_here=True) is True
    assert scheduled
    assert heavy == ["planned_item_moved"]


def test_destination_tree_item_matches_move_accepts_qmodelindex():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    model = QStandardItemModel()
    it = QStandardItem("n")
    it.setData({"source_path": "S\\X"}, Qt.UserRole)
    model.appendRow(it)
    ix = model.index(0, 0)
    move = {"source_path": "S\\X"}
    assert mw._destination_tree_item_matches_move(ix, move) is True


def test_select_canonical_destination_item_prefers_current_index_when_in_candidates():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    prefer = MagicMock()
    prefer.isValid.return_value = True
    prefer.column.return_value = 0
    pl = {
        "display_path": "Root\\Finance",
        "item_path": "Root\\Finance",
        "tree_role": "destination",
        "is_folder": True,
        "planned_allocation": True,
        "node_origin": "PlannedAllocation",
    }
    prefer.data.return_value = pl
    other = MagicMock()
    other.isValid.return_value = True
    other.column.return_value = 0
    other.data.return_value = dict(pl)
    items = [other, prefer]
    chosen = mw._select_canonical_destination_item(items, prefer=prefer)
    assert chosen is prefer


def test_allocation_parent_candidates_includes_ancestors_of_touch_paths():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.unresolved_allocations_by_parent_path = {
        "Root\\Media": {"a": {}},
        "Root\\Media\\100GOPRO": {"b": {}},
    }
    c = mw._allocation_parent_candidates_for_touch_paths(["Root\\Media\\100GOPRO\\file.txt"])
    assert "Root\\Media" in c
    assert "Root\\Media\\100GOPRO" in c


def test_reapply_paste_touch_paths_calls_ensure_projection_and_apply_children():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.unresolved_allocations_by_parent_path = {"Root\\New": {}}
    mw._allocation_parent_candidates_for_touch_paths = lambda _t: {"Root\\New"}
    calls = []
    mw._ensure_destination_projection_path = lambda p: calls.append(("ensure", p)) or "stub-item"
    mw._apply_allocation_children_to_item = lambda ix: calls.append(("apply", ix)) or 1
    mw._log_restore_exception = lambda *a, **k: None
    total, per = mw._reapply_allocation_overlays_for_paste_touch_paths("", "Root\\New", "Root\\New")
    assert total == 1
    assert per.get("Root\\New") == 1
    assert ("ensure", "Root\\New") in calls
    assert ("apply", "stub-item") in calls


def test_try_skip_redundant_destination_materialize_when_fp_matches_and_async_benign():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.destination_tree_widget = MagicMock()
    mw._planning_tree_top_level_count = lambda _t: 1
    mw._destination_last_materialized_overlay_fp = "samefp"
    mw._current_destination_full_overlay_fingerprint = lambda: "samefp"
    mw._destination_future_projection_async_state = {"move_index": 0}
    mw._count_visible_destination_future_state_nodes = lambda: 3
    mw._log_restore_phase = lambda *a, **kwargs: None
    mw._set_tree_status_message = lambda *a, **k: None
    out = mw._try_skip_redundant_destination_future_model_materialize("source_folder_load_success")
    assert out == 3


def test_try_skip_redundant_destination_materialize_none_when_async_and_non_benign_reason():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.destination_tree_widget = MagicMock()
    mw._planning_tree_top_level_count = lambda _t: 1
    mw._destination_last_materialized_overlay_fp = "samefp"
    mw._current_destination_full_overlay_fingerprint = lambda: "samefp"
    mw._destination_future_projection_async_state = {"move_index": 0}
    out = mw._try_skip_redundant_destination_future_model_materialize("planned_item_moved")
    assert out is None


def test_try_skip_redundant_non_benign_never_skips_when_fp_matches_without_async():
    """Non-benign materialize must run full rebind if needed; fp match alone is not enough."""
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.destination_tree_widget = MagicMock()
    mw._planning_tree_top_level_count = lambda _t: 1
    mw._destination_last_materialized_overlay_fp = "samefp"
    mw._current_destination_full_overlay_fingerprint = lambda: "samefp"
    mw._destination_future_projection_async_state = None
    out = mw._try_skip_redundant_destination_future_model_materialize("planned_item_moved")
    assert out is None


def test_try_skip_redundant_skips_when_benign_fp_matches_without_async():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.destination_tree_widget = MagicMock()
    mw._planning_tree_top_level_count = lambda _t: 1
    mw._destination_last_materialized_overlay_fp = "x"
    mw._current_destination_full_overlay_fingerprint = lambda: "x"
    mw._destination_future_projection_async_state = None
    mw._count_visible_destination_future_state_nodes = lambda: 4
    mw._log_restore_phase = lambda *a, **kwargs: None
    mw._set_tree_status_message = lambda *a, **k: None
    assert mw._try_skip_redundant_destination_future_model_materialize("source_folder_load_success") == 4


def test_try_skip_redundant_destination_materialize_none_when_fp_mismatch():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.destination_tree_widget = MagicMock()
    mw._planning_tree_top_level_count = lambda _t: 1
    mw._destination_last_materialized_overlay_fp = "a"
    mw._current_destination_full_overlay_fingerprint = lambda: "b"
    mw._destination_future_projection_async_state = None
    out = mw._try_skip_redundant_destination_future_model_materialize("source_folder_load_success")
    assert out is None


def test_try_skip_redundant_allows_deferred_prefix_during_async():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.destination_tree_widget = MagicMock()
    mw._planning_tree_top_level_count = lambda _t: 1
    mw._destination_last_materialized_overlay_fp = "x"
    mw._current_destination_full_overlay_fingerprint = lambda: "x"
    mw._destination_future_projection_async_state = {"move_index": 0}
    mw._count_visible_destination_future_state_nodes = lambda: 1
    mw._log_restore_phase = lambda *a, **kwargs: None
    mw._set_tree_status_message = lambda *a, **k: None
    assert mw._try_skip_redundant_destination_future_model_materialize("deferred_planning_change_lightweight") == 1


def test_deferred_planning_refresh_skips_destination_for_paste_quick_reason():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    stub = MagicMock()
    stub.viewport.return_value.update = lambda: None
    mw.destination_tree_widget = stub
    mw.source_tree_widget = stub
    mw._deferred_planning_refresh_pending = True
    mw._deferred_planning_refresh_reasons = ["planned_item_moved_paste_quick"]
    mw._deferred_source_projection_paths = set()
    materialized = []
    mw._materialize_destination_future_model = lambda reason: materialized.append(reason)
    mw._schedule_source_projection_refresh_for_paths = lambda *a, **k: None
    mw.update_progress_summaries = lambda: None
    mw._set_window_title_status = lambda status_text="": None
    mw._log_restore_exception = lambda *a, **k: None
    mw._run_deferred_planning_refresh()
    assert not materialized


def test_destination_model_priority_allocated_wins_over_real():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    assert mw._destination_model_state_priority("allocated") > mw._destination_model_state_priority("real")


def test_upsert_allocated_merges_over_real_folder_for_reset_nested_payload():
    """Real snapshot row at allocation path must become allocated in model_nodes (same path)."""
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.proposed_folders = []
    mw.planned_moves = []
    mw._destination_full_tree_ready = lambda: False
    path = r"Root\Media\100GOPRO"
    parent = r"Root\Media"
    model_nodes = {}
    mw._upsert_destination_model_node(
        model_nodes,
        path,
        name="100GOPRO",
        node_state="real",
        data={
            "name": "100GOPRO",
            "is_folder": True,
            "children_loaded": True,
            "id": "graph-folder-id",
            "item_path": path,
            "display_path": path,
            "destination_path": path,
            "tree_role": "destination",
        },
        parent_semantic_path=parent,
    )
    move = {
        "source_path": r"S\100GOPRO",
        "destination_path": parent,
        "target_name": "100GOPRO",
        "source": {"name": "100GOPRO", "is_folder": True},
    }
    parent_data = model_nodes.get(parent, {}).get("data", {})
    alloc_data = mw._build_destination_allocation_node_data(move, parent_data)
    mw._upsert_destination_model_node(
        model_nodes,
        path,
        name="100GOPRO",
        node_state="allocated",
        data=alloc_data,
        parent_semantic_path=parent,
    )
    node = model_nodes[path]
    assert node["node_state"] == "allocated"
    d = node["data"]
    assert d.get("planned_allocation") is True
    assert d.get("node_origin") == "PlannedAllocation"
    assert "[Allocated]" in str(d.get("base_display_label", ""))
    assert d.get("id") == "graph-folder-id"
    assert d.get("children_loaded") is True

    nested = mw._build_destination_future_nested(model_nodes, path)
    pl, _kids = nested
    assert "[Allocated]" in str(pl.get("base_display_label", ""))
    assert pl.get("planned_allocation") is True


def _dest_key(payload: dict) -> str:
    return str(payload.get("item_path") or payload.get("display_path") or "").strip().replace("/", "\\")


def test_manual_drop_on_folder_row_commits_that_folder():
    _qapp()
    model = DestinationPlanningTreeModel(destination_index_key_fn=_dest_key)
    model.reset_root_payloads(
        [
            {
                "base_display_label": "D",
                "name": "D",
                "is_folder": True,
                "item_path": r"Root\D",
                "display_path": r"Root\D",
                "tree_role": "destination",
                "node_origin": "Real",
            }
        ]
    )
    folder_ix = model.index(0, 0, QModelIndex())
    mw = MainWindow.__new__(MainWindow)
    commit_ix, meta = mw._resolve_destination_manual_drop_folder_index(
        folder_ix, QAbstractItemView.DropIndicatorPosition.OnItem
    )
    assert commit_ix == folder_ix
    assert meta["exact_hovered_row_committed"] is True
    assert meta["indicator_mode"] == "on_folder"
    assert meta["resolved_target_path"]


def test_manual_drop_between_nested_rows_resolves_parent_folder():
    _qapp()
    model = DestinationPlanningTreeModel(destination_index_key_fn=_dest_key)
    model.reset_root_payloads(
        [
            {
                "base_display_label": "D",
                "name": "D",
                "is_folder": True,
                "item_path": r"Root\D",
                "display_path": r"Root\D",
                "tree_role": "destination",
                "node_origin": "Real",
            }
        ]
    )
    folder_ix = model.index(0, 0, QModelIndex())
    model.replace_all_children(
        folder_ix,
        [
            {
                "base_display_label": "f",
                "name": "f.txt",
                "is_folder": False,
                "item_path": r"Root\D\f.txt",
                "display_path": r"Root\D\f.txt",
                "tree_role": "destination",
                "node_origin": "Real",
            }
        ],
    )
    file_ix = model.index(0, 0, folder_ix)
    mw = MainWindow.__new__(MainWindow)
    for dip in (
        QAbstractItemView.DropIndicatorPosition.AboveItem,
        QAbstractItemView.DropIndicatorPosition.BelowItem,
    ):
        commit_ix, meta = mw._resolve_destination_manual_drop_folder_index(file_ix, dip)
        assert commit_ix == folder_ix
        assert meta["insertion_context"] is True
        assert meta["indicator_mode"] == "between_rows"
        assert meta["drop_rejected_reason"] == ""


def test_manual_drop_between_top_level_rows_rejected_without_path_fallback():
    _qapp()
    model = DestinationPlanningTreeModel(destination_index_key_fn=_dest_key)
    model.reset_root_payloads(
        [
            {
                "base_display_label": "A",
                "name": "A",
                "is_folder": True,
                "item_path": r"Root\A",
                "display_path": r"Root\A",
                "tree_role": "destination",
                "node_origin": "Real",
            },
            {
                "base_display_label": "B",
                "name": "B",
                "is_folder": True,
                "item_path": r"Root\B",
                "display_path": r"Root\B",
                "tree_role": "destination",
                "node_origin": "Real",
            },
        ]
    )
    b_ix = model.index(1, 0, QModelIndex())
    mw = MainWindow.__new__(MainWindow)
    commit_ix, meta = mw._resolve_destination_manual_drop_folder_index(
        b_ix, QAbstractItemView.DropIndicatorPosition.AboveItem
    )
    assert commit_ix is None
    assert meta["drop_rejected_reason"] == "between_rows_top_level"


def test_manual_drop_resolver_does_not_reset_model():
    _qapp()
    model = DestinationPlanningTreeModel(destination_index_key_fn=_dest_key)
    model.reset_root_payloads(
        [
            {
                "base_display_label": "D",
                "name": "D",
                "is_folder": True,
                "item_path": r"Root\D",
                "display_path": r"Root\D",
                "tree_role": "destination",
                "node_origin": "Real",
            }
        ]
    )
    folder_ix = model.index(0, 0, QModelIndex())
    resets = []
    model.modelAboutToBeReset.connect(lambda: resets.append(1))
    mw = MainWindow.__new__(MainWindow)
    mw._resolve_destination_manual_drop_folder_index(
        folder_ix, QAbstractItemView.DropIndicatorPosition.OnItem
    )
    assert model.rowCount(QModelIndex()) == 1
    assert len(resets) == 0


def test_manual_drop_rejects_allocated_folder_target():
    _qapp()
    model = DestinationPlanningTreeModel(destination_index_key_fn=_dest_key)
    model.reset_root_payloads(
        [
            {
                "base_display_label": "Alloc",
                "name": "Alloc",
                "is_folder": True,
                "item_path": r"Root\Alloc",
                "display_path": r"Root\Alloc",
                "tree_role": "destination",
                "planned_allocation": True,
                "node_origin": "PlannedAllocation",
            }
        ]
    )
    alloc_ix = model.index(0, 0, QModelIndex())
    mw = MainWindow.__new__(MainWindow)
    commit_ix, meta = mw._resolve_destination_manual_drop_folder_index(
        alloc_ix, QAbstractItemView.DropIndicatorPosition.OnItem
    )
    assert commit_ix is None
    assert meta["drop_rejected_reason"] == "target_not_eligible"
    assert not meta.get("resolved_target_path")

