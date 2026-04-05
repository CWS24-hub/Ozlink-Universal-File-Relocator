"""Focused tests for MainWindow._expand_source_tree_item (QTreeWidget vs QTreeView)."""

from __future__ import annotations

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtGui import QStandardItem, QStandardItemModel
from PySide6.QtWidgets import QApplication, QTreeView, QTreeWidget, QTreeWidgetItem

from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _qapp():
    return QApplication.instance() or QApplication([])


def test_expand_source_tree_item_qtreeview_valid_index():
    _qapp()
    tree = QTreeView()
    model = QStandardItemModel()
    parent_item = QStandardItem("parent")
    parent_item.appendRow(QStandardItem("child"))
    model.appendRow(parent_item)
    tree.setModel(model)
    parent_ix = model.index(0, 0, QModelIndex())
    assert not tree.isExpanded(parent_ix)

    host = type("Host", (), {})()
    host.source_tree_widget = tree
    MainWindow._expand_source_tree_item(host, parent_ix)
    assert tree.isExpanded(parent_ix)


def test_expand_source_tree_item_qtree_widget_item():
    _app = QApplication.instance() or QApplication([])
    tree = QTreeWidget()
    tree.setHeaderLabels(["name"])
    parent = QTreeWidgetItem(["parent"])
    parent.addChild(QTreeWidgetItem(["child"]))
    tree.addTopLevelItem(parent)
    assert not parent.isExpanded()

    host = type("Host", (), {})()
    host.source_tree_widget = tree
    MainWindow._expand_source_tree_item(host, parent)
    assert parent.isExpanded()


def test_expand_source_tree_item_qtreeview_with_widget_item_no_crash():
    _qapp()
    tree = QTreeView()
    model = QStandardItemModel()
    model.appendRow(QStandardItem("only"))
    tree.setModel(model)
    host = type("Host", (), {})()
    host.source_tree_widget = tree
    wrong = QTreeWidgetItem(["x"])
    MainWindow._expand_source_tree_item(host, wrong)


def test_select_source_item_by_path_qtreeview_selects_qmodelindex():
    """Regression: Go to source uses _select_source_item_by_path; source may be QTreeView + QModelIndex."""
    _qapp()
    tree = QTreeView()
    model = QStandardItemModel()
    root = QStandardItem("root")
    leaf = QStandardItem("leaf")
    root.appendRow(leaf)
    model.appendRow(root)
    tree.setModel(model)
    root_ix = model.index(0, 0, QModelIndex())
    leaf_ix = model.index(0, 0, root_ix)

    mw = MainWindow.__new__(MainWindow)
    mw.source_tree_widget = tree
    mw._canonical_source_projection_path = lambda p: str(p or "").strip()
    mw._source_parent_path = lambda p: ""
    mw._find_visible_source_item_by_path = lambda _p: leaf_ix
    changed = []

    def _on_sel(role):
        changed.append(role)

    mw.on_tree_selection_changed = _on_sel

    assert MainWindow._select_source_item_by_path(mw, "any")
    assert tree.isExpanded(root_ix)
    assert tree.currentIndex() == leaf_ix
    assert "source" in changed


def test_source_tree_row_payload_nonzero_column_uses_get_tree_item_node_data():
    """Regression: source path lookup must not read Qt.UserRole on explorer column 2."""
    _qapp()
    model = DestinationPlanningTreeModel(column_labels=["Name", "Size", "Type", "Modified"])
    payload = {
        "name": "a.txt",
        "tree_role": "source",
        "item_path": "Root\\\\a.txt",
        "base_display_label": "a.txt",
    }
    model.reset_root_payloads([payload])
    ix = model.index(0, 2, QModelIndex())
    assert ix.data(Qt.UserRole) is None
    mw = MainWindow.__new__(MainWindow)
    mw._full_trace_enabled = lambda: False
    row = MainWindow._source_tree_row_payload(mw, ix)
    assert row.get("item_path") == "Root\\\\a.txt"


def test_get_tree_item_node_data_nonzero_column_reads_column_zero_userrole():
    """Regression: context-menu hit tests use indexAt() which may be column 1–3 in explorer trees."""
    _qapp()
    model = DestinationPlanningTreeModel(column_labels=["Name", "Size", "Type", "Modified"])
    payload = {
        "name": "leaf.txt",
        "source_path": "Root\\Sub\\leaf.txt",
        "tree_role": "destination",
        "base_display_label": "leaf.txt",
    }
    model.reset_root_payloads([payload])
    root = QModelIndex()
    ix_col2 = model.index(0, 2, root)
    assert ix_col2.isValid()
    assert ix_col2.data(Qt.UserRole) is None

    mw = MainWindow.__new__(MainWindow)
    data = MainWindow.get_tree_item_node_data(mw, ix_col2)
    assert data is not None
    assert data.get("source_path") == "Root\\Sub\\leaf.txt"

