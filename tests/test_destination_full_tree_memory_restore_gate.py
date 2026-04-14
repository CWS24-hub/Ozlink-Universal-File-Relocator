"""Full-tree worker must not defer on memory restore after destination Graph root bind (same drive)."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from ozlink_console.main_window import MainWindow


def _qapp():
    return QApplication.instance() or QApplication([])


def test_memory_restore_does_not_block_when_root_graph_bound_matches_drive():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._memory_restore_in_progress = True
    mw._destination_sharepoint_root_graph_bound_drive_id = "drive-abc"
    assert mw._destination_full_tree_memory_restore_may_block_worker("drive-abc") is False


def test_memory_restore_blocks_when_drive_not_yet_root_bound():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._memory_restore_in_progress = True
    mw._destination_sharepoint_root_graph_bound_drive_id = ""
    assert mw._destination_full_tree_memory_restore_may_block_worker("drive-abc") is True


def test_no_memory_restore_never_blocks_gate():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._memory_restore_in_progress = False
    mw._destination_sharepoint_root_graph_bound_drive_id = ""
    assert mw._destination_full_tree_memory_restore_may_block_worker("drive-abc") is False


def test_memory_restore_does_not_block_when_graph_owns_visible_destination_structure():
    """Graph authority mode: full-tree enumeration must not defer on session restore (live SPO wins)."""
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._memory_restore_in_progress = True
    mw._destination_sharepoint_root_graph_bound_drive_id = ""
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw.destination_planning_model = object()
    assert mw._destination_full_tree_memory_restore_may_block_worker("drive-xyz") is False
