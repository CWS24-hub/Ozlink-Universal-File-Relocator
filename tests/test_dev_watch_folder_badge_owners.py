"""_dev_log_watch_folder_badge_owners must not crash on DestinationPlanningTreeView (model/view)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QTreeWidget, QTreeWidgetItem

from ozlink_console.main_window import MainWindow


def _qapp():
    return QApplication.instance() or QApplication([])


def test_dev_log_watch_folder_badge_owners_model_view_no_top_level_item_api():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    tree = MagicMock(spec=["objectName"])  # no topLevelItemCount / topLevelItem
    tree.objectName.return_value = "DestinationPlanningTreeView"
    mw.destination_tree_widget = tree
    mw._destination_tree_model_view = True
    mw._planning_tree_top_level_count = lambda _t: 1

    class _Model:
        def iter_depth_first(self):
            yield from ()

    mw.destination_planning_model = _Model()
    mw.get_tree_item_node_data = lambda _ix: {}
    mw._destination_row_semantic_path = lambda _nd: ""

    with patch("ozlink_console.main_window.is_dev_mode", return_value=True):
        mw._dev_log_watch_folder_badge_owners("test_ctx_model")
    # No AttributeError on topLevelItemCount


def test_dev_log_watch_folder_badge_owners_model_view_collects_watch_rows():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.destination_tree_widget = MagicMock()
    mw._destination_tree_model_view = True
    mw._planning_tree_top_level_count = lambda _t: 1

    ix0 = object()

    payload = {
        "name": "Employee Hours",
        "base_display_label": "Folder: Employee Hours [Allocated]",
        "planned_allocation": True,
        "display_path": "Root\\Employee Hours",
        "item_path": "Root\\Employee Hours",
        "is_folder": True,
        "tree_role": "destination",
    }

    class _Model:
        def iter_depth_first(self):
            yield ix0

    mw.destination_planning_model = _Model()
    mw.get_tree_item_node_data = lambda ix: dict(payload) if ix is ix0 else {}

    captured = []

    def _capture(msg, **data):
        if msg == "destination_allocation_folder_lifecycle" and data.get("phase") == "final_watch_folder_badge_probe":
            captured.append(data)

    with patch("ozlink_console.main_window.is_dev_mode", return_value=True):
        with patch("ozlink_console.main_window.log_info", side_effect=_capture):
            mw._dev_log_watch_folder_badge_owners("test_collect")

    assert captured
    last = captured[-1]
    assert last.get("context") == "test_collect"
    assert len(last.get("rows") or []) >= 1
    assert "Employee Hours" in (last["rows"][0].get("path") or "")


def test_dev_log_watch_folder_badge_owners_qtreewidget_still_walks_items():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    tree = QTreeWidget()
    tree.setColumnCount(1)
    root = QTreeWidgetItem(["Root"])
    tree.addTopLevelItem(root)
    child = QTreeWidgetItem(["Employee Hours"])
    child.setData(
        0,
        Qt.UserRole,
        {
            "name": "Employee Hours",
            "base_display_label": "Folder: Employee Hours",
            "planned_allocation": True,
            "display_path": "Root\\Employee Hours",
            "item_path": "Root\\Employee Hours",
            "is_folder": True,
            "tree_role": "destination",
        },
    )
    root.addChild(child)

    mw.destination_tree_widget = tree
    mw._destination_tree_model_view = False

    captured = []

    def _capture(msg, **data):
        if msg == "destination_allocation_folder_lifecycle" and data.get("phase") == "final_watch_folder_badge_probe":
            captured.append(data)

    with patch("ozlink_console.main_window.is_dev_mode", return_value=True):
        with patch("ozlink_console.main_window.log_info", side_effect=_capture):
            mw._dev_log_watch_folder_badge_owners("test_qtw")

    assert captured
    rows = captured[-1].get("rows") or []
    assert any("Employee Hours" in str(r.get("path", "")) for r in rows)


def test_dev_log_watch_folder_badge_owners_swallows_impl_exceptions():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.destination_tree_widget = MagicMock()
    mw._destination_tree_model_view = True
    mw._planning_tree_top_level_count = lambda _t: 1

    class _BadModel:
        def iter_depth_first(self):
            raise RuntimeError("simulated model failure")

    mw.destination_planning_model = _BadModel()

    warns = []

    def _warn(msg, **data):
        if data.get("phase") == "final_watch_folder_badge_probe_error":
            warns.append(data)

    with patch("ozlink_console.main_window.is_dev_mode", return_value=True):
        with patch("ozlink_console.main_window.log_warn", side_effect=_warn):
            mw._dev_log_watch_folder_badge_owners("test_exc")

    assert warns
    assert "simulated model failure" in str(warns[0].get("error", ""))
