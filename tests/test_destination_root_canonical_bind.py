"""Destination SharePoint planning tree: live Graph top-level rows, no synthetic visible Root wrapper."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication

from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _app():
    return QApplication.instance() or QApplication([])


def _shell_rows():
    loading = {
        "placeholder": True,
        "placeholder_role": "destination_authority_pending",
        "base_display_label": "Reconciling full library structure from Microsoft 365…",
        "tree_role": "destination",
        "non_authoritative_destination_shell": True,
    }
    finance = {
        "name": "Finance",
        "base_display_label": "Finance",
        "is_folder": True,
        "tree_role": "destination",
        "id": "id-fin",
        "item_path": "/Finance",
    }
    hr = {
        "name": "HR",
        "base_display_label": "HR",
        "is_folder": True,
        "tree_role": "destination",
        "id": "id-hr",
        "item_path": "/HR",
    }
    root_test = {
        "name": "RootTest",
        "base_display_label": "RootTest",
        "is_folder": True,
        "tree_role": "destination",
        "id": "id-rt",
        "item_path": "/RootTest",
    }
    return loading, finance, hr, root_test


def test_authority_pending_shell_detects_top_level_placeholder():
    _app()
    mw = MainWindow.__new__(MainWindow)
    loading, finance, hr, root_test = _shell_rows()
    dm = DestinationPlanningTreeModel()
    mw.destination_planning_model = dm
    dm.reset_root_payloads([loading, finance, hr, root_test])
    assert MainWindow._destination_tree_shows_authority_pending_shell(mw) is True


def test_authority_pending_shell_detects_legacy_placeholder_under_folder():
    """Older layout: single folder row with authority placeholder as child (still supported)."""
    _app()
    mw = MainWindow.__new__(MainWindow)
    dm = DestinationPlanningTreeModel()
    mw.destination_planning_model = dm
    root = QModelIndex()
    dm.reset_root_payloads(
        [{"name": "RootTest", "is_folder": True, "tree_role": "destination", "base_display_label": "RootTest"}]
    )
    root_ix = dm.index(0, 0, root)
    dm.replace_all_children(
        root_ix,
        [
            {
                "placeholder": True,
                "placeholder_role": "destination_authority_pending",
                "tree_role": "destination",
            }
        ],
    )
    assert MainWindow._destination_tree_shows_authority_pending_shell(mw) is True


def test_tree_has_bound_root_content_true_when_first_row_is_shell_placeholder():
    _app()
    mw = MainWindow.__new__(MainWindow)
    loading, finance, _, _ = _shell_rows()
    dm = DestinationPlanningTreeModel()
    mw.destination_planning_model = dm
    mw.destination_tree_widget = object()
    dm.reset_root_payloads([loading, finance])
    assert MainWindow._tree_has_bound_root_content(mw, "destination") is True


def test_top_level_real_folder_named_root_is_ordinary_row():
    """A SharePoint folder literally named Root is not special in the model shape."""
    _app()
    loading, *_rest = _shell_rows()
    root_folder = {
        "name": "Root",
        "base_display_label": "Root",
        "is_folder": True,
        "tree_role": "destination",
        "id": "id-root",
        "item_path": "/Root",
    }
    dm = DestinationPlanningTreeModel()
    dm.reset_root_payloads([loading, root_folder])
    inv = QModelIndex()
    assert dm.rowCount(inv) == 2
    row1 = dm.index(1, 0, inv).data(Qt.UserRole) or {}
    assert row1.get("name") == "Root"
    assert row1.get("base_display_label") == "Root"


def test_internal_future_model_root_key_still_used_via_destination_root_base_data():
    """Planning future-state graph keeps internal ``Root`` parent key (not visible tree bind)."""
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw.destination_planning_model = DestinationPlanningTreeModel()
    mw.destination_tree_widget = None
    data = MainWindow._destination_root_base_data(mw)
    assert data.get("name") == "Root"
    assert str(data.get("destination_path", "") or "").strip() == "Root"
