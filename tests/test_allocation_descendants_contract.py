"""Contract tests: planned-allocation descendant projection vs explicit child rows."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication, QTreeWidgetItem

from ozlink_console.main_window import MainWindow


@pytest.fixture(scope="module")
def qapp():
    return QApplication.instance() or QApplication([])


def test_invalidate_projection_index_clears_allocation_descendants_applied_on_path_mismatch():
    mw = MainWindow.__new__(MainWindow)
    move = {
        "destination_path": "\\Root",
        "target_name": "AllocFolder",
        "source": {"is_folder": True, "name": "AllocFolder"},
    }
    nd = {
        "node_origin": "plannedallocation",
        "is_folder": True,
        "children_loaded": True,
        "allocation_descendants_applied": True,
        "allocation_projection_destination_path_saved": "\\Root\\WrongName",
    }
    bad_ix = QModelIndex()
    result = mw._invalidate_stale_destination_allocation_projection_index(bad_ix, nd, move)
    assert result.get("children_loaded") is False
    assert "allocation_descendants_applied" not in result


def test_mark_allocation_descendants_applied_updates_model_payload():
    mw = MainWindow.__new__(MainWindow)
    model = MagicMock()
    mw.destination_planning_model = model
    ix = MagicMock()
    ix.isValid.return_value = True
    ix.column.return_value = 0
    ix.siblingAtColumn.return_value = ix
    mw._mark_allocation_descendants_applied_on_allocation_folder_model_index(ix, None)
    model.update_payload_for_index.assert_called_once()
    args, _kw = model.update_payload_for_index.call_args
    assert args[0] is ix
    mut = args[1]
    p = {}
    mut(p)
    assert p.get("allocation_descendants_applied") is True


def test_apply_visible_model_branch_uses_flag_not_child_presence():
    """Explicit children must not skip descendant apply; only ``allocation_descendants_applied`` may."""
    mw = MainWindow.__new__(MainWindow)
    tree = MagicMock()
    mw.destination_tree_widget = tree
    mw._destination_tree_uses_model_view = lambda: True  # noqa: E731

    dmodel = MagicMock()
    mw.destination_planning_model = dmodel

    ix = MagicMock()
    ix.isValid.return_value = True
    ix.data.return_value = {
        "node_origin": "plannedallocation",
        "is_folder": True,
        "allocation_descendants_applied": False,
    }
    move = {"destination_path": "\\D", "target_name": "F", "source": {"is_folder": True, "name": "F"}}

    mw._build_planned_move_destination_lookup = lambda: {"alloc_by_path": {}}  # noqa: E731
    mw._destination_model_build_allocation_apply_pairs = lambda _m, _l: [(ix, move)]  # noqa: E731
    mw._destination_bind_normalized_expanded_targets = lambda _ep: {"\\Alloc"}  # noqa: E731
    mw._destination_semantic_path = lambda _nd: "\\Alloc"  # noqa: E731
    mw._destination_bind_should_apply_allocation_descendants_now = lambda _sem, _nt: True  # noqa: E731
    applied = {"n": 0}

    def _fake_apply(_parent, _mv):
        applied["n"] += 1
        return 3

    mw._apply_allocation_descendants_to_item = _fake_apply

    mw._apply_visible_destination_allocation_descendants(destination_expanded_paths={"\\Alloc"})
    assert applied["n"] == 1
    dmodel.rowCount.assert_not_called()


def test_apply_visible_model_skips_apply_when_descendants_already_applied():
    mw = MainWindow.__new__(MainWindow)
    tree = MagicMock()
    mw.destination_tree_widget = tree
    mw._destination_tree_uses_model_view = lambda: True  # noqa: E731

    dmodel = MagicMock()
    mw.destination_planning_model = dmodel

    ix = MagicMock()
    ix.isValid.return_value = True
    ix.data.return_value = {
        "node_origin": "plannedallocation",
        "is_folder": True,
        "allocation_descendants_applied": True,
        "children_loaded": False,
    }
    move = {"destination_path": "\\D", "target_name": "F", "source": {"is_folder": True, "name": "F"}}

    mw._build_planned_move_destination_lookup = lambda: {"alloc_by_path": {}}  # noqa: E731
    mw._destination_model_build_allocation_apply_pairs = lambda _m, _l: [(ix, move)]  # noqa: E731
    mw._destination_bind_normalized_expanded_targets = lambda _ep: {"\\Alloc"}  # noqa: E731
    mw._destination_semantic_path = lambda _nd: "\\Alloc"  # noqa: E731
    mw._destination_bind_should_apply_allocation_descendants_now = lambda _sem, _nt: True  # noqa: E731

    def _fail_apply(*_a, **_k):
        raise AssertionError("_apply_allocation_descendants_to_item should not run when flag is set")

    mw._apply_allocation_descendants_to_item = _fail_apply

    mw._apply_visible_destination_allocation_descendants(destination_expanded_paths={"\\Alloc"})
    dmodel.update_payload_for_index.assert_called_once()
    mut = dmodel.update_payload_for_index.call_args[0][1]
    p = {"node_origin": "plannedallocation", "is_folder": True, "allocation_descendants_applied": True}
    mut(p)
    assert p.get("children_loaded") is True


def test_invalidate_projection_widget_clears_allocation_descendants_applied_on_path_mismatch(qapp):
    mw = MainWindow.__new__(MainWindow)
    item = QTreeWidgetItem()
    move = {
        "destination_path": "\\Root",
        "target_name": "AllocFolder",
        "source": {"is_folder": True, "name": "AllocFolder"},
    }
    nd = {
        "node_origin": "plannedallocation",
        "is_folder": True,
        "children_loaded": True,
        "allocation_descendants_applied": True,
        "allocation_projection_destination_path_saved": "\\Root\\WrongName",
    }
    item.setData(0, Qt.UserRole, nd)
    out = mw._invalidate_stale_destination_allocation_projection(item, nd, move)
    assert out.get("children_loaded") is False
    assert "allocation_descendants_applied" not in out
