"""Planning/execution consistency after destination-side planned or proposed-folder relocates."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from PySide6.QtWidgets import QApplication

from ozlink_console.main_window import MainWindow
from ozlink_console.transfer_manifest import _planned_move_to_step


def _qapp():
    return QApplication.instance() or QApplication([])


def test_rewrite_nonprimary_planned_moves_destination_prefix_updates_descendants():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    primary = {
        "source": {
            "name": "Dept",
            "display_path": "S\\Dept",
            "item_path": "S\\Dept",
            "is_folder": True,
        },
        "source_path": "S\\Dept",
        "destination_path": "Root\\Archive",
        "target_name": "Dept",
        "destination": {
            "display_path": "Root\\Archive\\Dept",
            "item_path": "Root\\Archive\\Dept",
        },
    }
    child = {
        "source": {
            "name": "a.txt",
            "display_path": "S\\Dept\\a.txt",
            "item_path": "S\\Dept\\a.txt",
            "is_folder": False,
        },
        "source_path": "S\\Dept\\a.txt",
        "destination_path": "Root\\HR\\Dept\\sub",
        "target_name": "a.txt",
        "destination": {
            "display_path": "Root\\HR\\Dept\\sub\\a.txt",
            "item_path": "Root\\HR\\Dept\\sub\\a.txt",
        },
    }
    mw.planned_moves = [primary, child]
    mw._is_move_submitted = lambda _m: False
    out = mw._rewrite_nonprimary_planned_moves_destination_prefix(
        "Root\\HR\\Dept", "Root\\Archive\\Dept", primary_move=primary
    )
    assert child in out
    assert child["destination_path"] == "Root\\Archive\\Dept\\sub"
    assert child["target_name"] == "a.txt"
    assert child["destination"]["item_path"] == "Root\\Archive\\Dept\\sub\\a.txt"
    step = _planned_move_to_step(0, child)
    assert step.destination_path == "Root\\Archive\\Dept\\sub"
    assert step.destination_name == "a.txt"


def test_expand_source_projection_paths_for_move_network_includes_descendant_moves():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    folder = {
        "source": {
            "name": "Dept",
            "display_path": "Lib\\Dept",
            "item_path": "Lib\\Dept",
            "is_folder": True,
        },
        "source_path": "Lib\\Dept",
        "destination_path": "D\\Dept",
        "target_name": "Dept",
    }
    nested = {
        "source": {
            "name": "f.txt",
            "display_path": "Lib\\Dept\\f.txt",
            "item_path": "Lib\\Dept\\f.txt",
        },
        "source_path": "Lib\\Dept\\f.txt",
        "destination_path": "D\\Dept",
        "target_name": "f.txt",
    }
    mw.planned_moves = [folder, nested]
    paths = mw._expand_source_projection_paths_for_move_network(folder)
    canon_dept = mw._canonical_source_projection_path("Lib\\Dept")
    canon_file = mw._canonical_source_projection_path("Lib\\Dept\\f.txt")
    assert canon_dept in paths
    assert canon_file in paths


def test_finalize_destination_move_planning_consistency_refreshes_source_projection():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    move = {
        "source_path": "S\\One",
        "source": {"display_path": "S\\One", "item_path": "S\\One", "is_folder": False},
        "destination_path": "D\\T",
        "target_name": "One",
    }
    mw.planned_moves = [move]
    mw._expand_source_projection_paths_for_move_network = MagicMock(return_value={"S\\One"})
    inv = MagicMock()
    ref = MagicMock()
    mw._invalidate_projection_lookup_caches = inv
    mw._refresh_source_projection_for_paths = ref
    mw.source_tree_widget = MagicMock()
    with patch("ozlink_console.main_window.is_dev_mode", return_value=False):
        mw._finalize_destination_move_planning_consistency(
            move,
            rewritten_related=[],
            old_destination_projection="D\\Old",
            new_destination_projection="D\\New",
            table_refreshed=True,
            incremental_lightweight=True,
            from_manual_planning_drag=False,
        )
    inv.assert_called_once_with(bump_generation=False)
    ref.assert_called_once()
    args, kwargs = ref.call_args
    assert "S\\One" in args[0]
    assert args[1] == "planned_destination_move_sync_light"


def test_finalize_destination_move_planning_consistency_does_not_call_full_persist():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    move = {
        "source_path": "S\\One",
        "source": {"display_path": "S\\One", "item_path": "S\\One"},
        "destination_path": "D\\T",
        "target_name": "One",
    }
    mw.planned_moves = [move]
    mw._expand_source_projection_paths_for_move_network = MagicMock(return_value=set())
    mw._invalidate_projection_lookup_caches = MagicMock()
    mw._refresh_source_projection_for_paths = MagicMock()
    mw.source_tree_widget = None
    mw._persist_planning_change = MagicMock()
    mw._persist_planning_change_lightweight = MagicMock()
    with patch("ozlink_console.main_window.is_dev_mode", return_value=False):
        mw._finalize_destination_move_planning_consistency(
            move,
            rewritten_related=[],
            old_destination_projection="a",
            new_destination_projection="b",
            table_refreshed=True,
            incremental_lightweight=True,
            from_manual_planning_drag=False,
        )
    mw._persist_planning_change.assert_not_called()
    mw._persist_planning_change_lightweight.assert_not_called()


def test_collect_source_projection_paths_under_destination_allocation_prefix():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    m1 = {
        "source_path": "S\\f",
        "source": {"display_path": "S\\f", "item_path": "S\\f"},
        "destination_path": "P\\Prop\\inner",
        "target_name": "f",
        "destination": {"display_path": "P\\Prop\\inner\\f", "item_path": "P\\Prop\\inner\\f"},
    }
    mw.planned_moves = [m1]
    s = mw._collect_source_projection_paths_under_destination_allocation_prefix("P\\Prop")
    canon = mw._canonical_source_projection_path("S\\f")
    assert canon in s


def test_moveaudit_logged_for_manual_drag_in_dev_mode():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    move = {
        "source_path": "S\\Item",
        "source": {"display_path": "S\\Item", "item_path": "S\\Item"},
        "destination_path": "D\\P",
        "target_name": "Item",
    }
    mw.planned_moves = [move]
    mw._expand_source_projection_paths_for_move_network = MagicMock(return_value={"S\\Item"})
    mw._invalidate_projection_lookup_caches = MagicMock()
    mw._refresh_source_projection_for_paths = MagicMock()
    mw.source_tree_widget = MagicMock()
    audits = []

    def _capture(msg, **data):
        if msg == "MOVEAUDIT":
            audits.append(data)

    with patch("ozlink_console.main_window.is_dev_mode", return_value=True):
        with patch("ozlink_console.main_window.log_info", side_effect=_capture):
            mw._finalize_destination_move_planning_consistency(
                move,
                rewritten_related=[],
                old_destination_projection="D\\Old\\Item",
                new_destination_projection="D\\New\\Item",
                table_refreshed=True,
                incremental_lightweight=True,
                from_manual_planning_drag=True,
            )
    assert len(audits) == 1
    assert audits[0].get("planned_move_map_updated") is True
    assert audits[0].get("source_traceability_updated") is True
    assert audits[0].get("planned_moves_table_updated") is True
    assert audits[0].get("execution_state_updated") is True
