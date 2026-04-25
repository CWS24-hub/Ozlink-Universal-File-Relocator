"""Authority-pending detection without a visible placeholder row."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from ozlink_console.main_window import MainWindow


def _qapp():
    return QApplication.instance() or QApplication([])


def test_reconcile_pending_flag_implies_authority_shell_visible_for_gates():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._destination_browse_mode = "sharepoint"
    mw.destination_planning_model = None
    mw._destination_full_library_reconcile_pending = True
    assert mw._destination_tree_shows_authority_pending_shell() is True
