"""Target path and planned-move identity resolution for destination UI actions."""

from __future__ import annotations

from unittest.mock import MagicMock

from PySide6.QtCore import Qt
from PySide6.QtGui import QStandardItem, QStandardItemModel
from PySide6.QtWidgets import QApplication

from ozlink_console.main_window import MainWindow


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
    mw._register_and_project_planned_move = lambda m, reason="": 1
    mw._apply_allocation_children_to_item = lambda ix: 0
    mw._paths_equivalent = lambda a, b, role: str(a).replace("/", "\\") == str(b).replace("/", "\\")
    scheduled = []
    mw._schedule_deferred_destination_materialization = lambda r, delay_ms=180: scheduled.append((r, delay_ms))
    lightweight = []
    mw._persist_planning_change_lightweight = lambda: lightweight.append(True)

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
    assert lightweight == [True]
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
    mw._register_and_project_planned_move = lambda m, reason="": 0
    mw._apply_allocation_children_to_item = lambda ix: 0
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

