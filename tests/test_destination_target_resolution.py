"""Target path and planned-move identity resolution for destination UI actions."""

from __future__ import annotations

from unittest.mock import MagicMock

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

