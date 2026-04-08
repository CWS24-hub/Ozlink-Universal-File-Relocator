"""QTreeView destination tree: merge duplicate rows that share the same semantic path."""

from __future__ import annotations

import pytest
from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication, QTreeWidget, QTreeWidgetItem

from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


@pytest.fixture(scope="module")
def qapp():
    return QApplication.instance() or QApplication([])


def test_reconcile_semantic_duplicates_model_merges_twin_sub_folders(qapp):
    """Two sibling folders with the same canonical path collapse into one (real + projected shell)."""
    mw = MainWindow.__new__(MainWindow)
    mw.destination_tree_widget = object()
    mw.planned_moves = []
    mw.proposed_folders = []
    mw._memory_restore_in_progress = False
    mw._suppress_selector_change_handlers = False
    mw._log_restore_phase = lambda *_a, **_k: None  # noqa: E731

    model = DestinationPlanningTreeModel(
        parent=None,
        destination_index_key_fn=mw._destination_payload_index_key,
    )
    mw.destination_planning_model = model
    mw._destination_tree_uses_model_view = lambda: True  # noqa: E731

    sub_path = r"Root\FTBMRoot\F\Sub"
    sub_projected = {
        "base_display_label": "folder: Sub",
        "is_folder": True,
        "item_path": sub_path,
        "tree_role": "destination",
        "node_origin": "projecteddestination",
    }
    sub_real = {
        "base_display_label": "folder: Sub",
        "is_folder": True,
        "item_path": sub_path,
        "tree_role": "destination",
        "node_origin": "sharepoint",
    }
    file_payload = {
        "base_display_label": "file3.docx",
        "is_folder": False,
        "item_path": r"Root\FTBMRoot\F\Sub\file3.docx",
        "tree_role": "destination",
    }
    f_path = r"Root\FTBMRoot\F"
    f_payload = {
        "base_display_label": "folder: F [Allocated]",
        "is_folder": True,
        "item_path": f_path,
        "tree_role": "destination",
        "node_origin": "plannedallocation",
    }

    model.reset_nested(
        [
            (
                f_payload,
                [
                    (sub_projected, []),
                    (sub_real, [(file_payload, [])]),
                ],
            )
        ]
    )

    f_ix = model.index(0, 0, QModelIndex())
    assert model.rowCount(f_ix) == 2

    moved = mw._reconcile_destination_semantic_duplicates("test")
    assert moved >= 0
    assert model.rowCount(f_ix) == 1
    sub_ix = model.index(0, 0, f_ix)
    pl = sub_ix.data(Qt.UserRole) or {}
    assert pl.get("is_folder") is True
    assert model.rowCount(sub_ix) == 1
    file_ix = model.index(0, 0, sub_ix)
    assert (file_ix.data(Qt.UserRole) or {}).get("item_path", "").endswith("file3.docx")


def test_reconcile_merges_same_named_sub_folders_when_paths_differ(qapp):
    """Sibling-folder pass: two ``Sub`` rows under the same parent with different item_path strings."""
    mw = MainWindow.__new__(MainWindow)
    mw.destination_tree_widget = object()
    mw.planned_moves = []
    mw.proposed_folders = []
    mw._memory_restore_in_progress = False
    mw._suppress_selector_change_handlers = False
    mw._log_restore_phase = lambda *_a, **_k: None  # noqa: E731

    model = DestinationPlanningTreeModel(
        parent=None,
        destination_index_key_fn=mw._destination_payload_index_key,
    )
    mw.destination_planning_model = model
    mw._destination_tree_uses_model_view = lambda: True  # noqa: E731

    f_path = r"Root\FTBMRoot\F"
    f_payload = {
        "base_display_label": "folder: F [Allocated]",
        "is_folder": True,
        "item_path": f_path,
        "tree_role": "destination",
        "node_origin": "plannedallocation",
        "name": "F",
    }
    sub_shell = {
        "base_display_label": "folder: Sub",
        "is_folder": True,
        "item_path": r"Root\FTBMRoot\F\Sub_wrong_suffix",
        "tree_role": "destination",
        "node_origin": "ProjectedDestination",
        "name": "Sub",
    }
    sub_real = {
        "base_display_label": "folder: Sub",
        "is_folder": True,
        "item_path": r"Root\FTBMRoot\F\Sub",
        "tree_role": "destination",
        "node_origin": "sharepoint",
        "name": "Sub",
    }
    file_payload = {
        "base_display_label": "file3.docx",
        "is_folder": False,
        "item_path": r"Root\FTBMRoot\F\Sub\file3.docx",
        "tree_role": "destination",
    }

    model.reset_nested(
        [
            (
                f_payload,
                [
                    (sub_shell, []),
                    (sub_real, [(file_payload, [])]),
                ],
            )
        ]
    )

    f_ix = model.index(0, 0, QModelIndex())
    assert model.rowCount(f_ix) == 2
    mw._reconcile_destination_semantic_duplicates("test")
    assert model.rowCount(f_ix) == 1
    sub_ix = model.index(0, 0, f_ix)
    assert model.rowCount(sub_ix) == 1


def test_sibling_dedup_key_from_folder_label_without_name_fields(qapp):
    mw = MainWindow.__new__(MainWindow)
    nd = {"is_folder": True, "base_display_label": "folder: Sub [Allocated]"}
    assert mw._destination_sibling_folder_dedup_key(nd) == "sub"


def test_collision_key_unifies_sub_and_sub_via_suffix(qapp):
    mw = MainWindow.__new__(MainWindow)
    a = {"is_folder": True, "name": "Sub"}
    b = {"is_folder": True, "name": "Sub - via F"}
    assert mw._destination_sibling_folder_collision_key(a) == "sub"
    assert mw._destination_sibling_folder_collision_key(b) == "sub"


def test_collision_key_unifies_full_path_name_with_leaf_sibling(qapp):
    """When one row's ``name`` is a full path and the other's is the leaf, sibling dedup still groups."""
    mw = MainWindow.__new__(MainWindow)
    leaf = {"is_folder": True, "name": "Sub"}
    full = {"is_folder": True, "name": r"Root\FTBMRoot\F\Sub"}
    assert mw._destination_sibling_folder_collision_key(leaf) == "sub"
    assert mw._destination_sibling_folder_collision_key(full) == "sub"


def test_reconcile_merges_sub_siblings_when_one_name_is_full_path(qapp):
    """Same-named folders under one parent: path-shaped ``name`` vs leaf ``name`` must collapse."""
    mw = MainWindow.__new__(MainWindow)
    mw.destination_tree_widget = object()
    mw.planned_moves = []
    mw.proposed_folders = []
    mw._memory_restore_in_progress = False
    mw._suppress_selector_change_handlers = False
    mw._log_restore_phase = lambda *_a, **_k: None  # noqa: E731

    model = DestinationPlanningTreeModel(
        parent=None,
        destination_index_key_fn=mw._destination_payload_index_key,
    )
    mw.destination_planning_model = model
    mw._destination_tree_uses_model_view = lambda: True  # noqa: E731

    f_path = r"Root\FTBMRoot\F"
    f_payload = {
        "base_display_label": "folder: F [Allocated]",
        "is_folder": True,
        "item_path": f_path,
        "tree_role": "destination",
        "node_origin": "plannedallocation",
        "name": "F",
    }
    sub_path_shaped = {
        "base_display_label": "folder: Sub",
        "is_folder": True,
        "item_path": r"Root\FTBMRoot\F\Sub_shell",
        "tree_role": "destination",
        "node_origin": "ProjectedDestination",
        "name": r"Root\FTBMRoot\F\Sub",
    }
    sub_leaf_named = {
        "base_display_label": "folder: Sub",
        "is_folder": True,
        "item_path": r"Root\FTBMRoot\F\Sub",
        "tree_role": "destination",
        "node_origin": "sharepoint",
        "name": "Sub",
    }
    file_payload = {
        "base_display_label": "file3.docx",
        "is_folder": False,
        "item_path": r"Root\FTBMRoot\F\Sub\file3.docx",
        "tree_role": "destination",
    }

    model.reset_nested(
        [
            (
                f_payload,
                [
                    (sub_path_shaped, []),
                    (sub_leaf_named, [(file_payload, [])]),
                ],
            )
        ]
    )

    f_ix = model.index(0, 0, QModelIndex())
    assert model.rowCount(f_ix) == 2
    mw._reconcile_destination_semantic_duplicates("test")
    assert model.rowCount(f_ix) == 1
    sub_ix = model.index(0, 0, f_ix)
    assert model.rowCount(sub_ix) == 1


def test_find_destination_child_matches_sub_via_when_path_ends_sub(qapp):
    mw = MainWindow.__new__(MainWindow)
    tree = QTreeWidget()
    mw.destination_tree_widget = tree
    parent = QTreeWidgetItem()
    parent.setData(0, Qt.UserRole, {"is_folder": True, "item_path": r"Root\F", "name": "F"})
    tree.addTopLevelItem(parent)
    existing = QTreeWidgetItem()
    existing.setData(
        0,
        Qt.UserRole,
        {
            "is_folder": True,
            "name": "Sub - via F",
            "item_path": r"Root\F\Sub - via F",
            "tree_role": "destination",
        },
    )
    parent.addChild(existing)
    found = mw._find_destination_child_by_path(parent, r"Root\F\Sub")
    assert found is existing


def test_find_destination_child_falls_back_to_folder_label_when_paths_mismatch(qapp):
    """Allocation apply must not add a second folder when an existing row only matches by label."""
    mw = MainWindow.__new__(MainWindow)
    tree = QTreeWidget()
    mw.destination_tree_widget = tree

    parent = QTreeWidgetItem()
    parent.setData(
        0,
        Qt.UserRole,
        {"is_folder": True, "item_path": r"Root\F", "tree_role": "destination", "name": "F"},
    )
    tree.addTopLevelItem(parent)

    existing_sub = QTreeWidgetItem()
    existing_sub.setData(
        0,
        Qt.UserRole,
        {
            "is_folder": True,
            "base_display_label": "folder: Sub",
            "item_path": "",
            "display_path": "",
            "tree_role": "destination",
        },
    )
    parent.addChild(existing_sub)

    found = mw._find_destination_child_by_path(parent, r"Root\F\Sub")
    assert found is existing_sub


def test_reconcile_sibling_folders_qtreewidget_path(qapp):
    """Default destination QTreeWidget: same-named folder siblings merge by display label."""
    mw = MainWindow.__new__(MainWindow)
    mw.destination_tree_widget = QTreeWidget()
    mw.planned_moves = []
    mw.proposed_folders = []
    mw._memory_restore_in_progress = False
    mw._suppress_selector_change_handlers = False
    mw._log_restore_phase = lambda *_a, **_k: None  # noqa: E731

    tree = mw.destination_tree_widget
    f_item = QTreeWidgetItem()
    f_item.setData(
        0,
        Qt.UserRole,
        {
            "is_folder": True,
            "name": "F",
            "item_path": r"Root\F",
            "tree_role": "destination",
            "node_origin": "plannedallocation",
        },
    )
    tree.addTopLevelItem(f_item)

    sub_shell = QTreeWidgetItem()
    sub_shell.setData(
        0,
        Qt.UserRole,
        {
            "is_folder": True,
            "base_display_label": "folder: Sub",
            "item_path": r"Root\F\Sub_shell_only",
            "tree_role": "destination",
            "node_origin": "ProjectedDestination",
        },
    )
    sub_real = QTreeWidgetItem()
    sub_real.setData(
        0,
        Qt.UserRole,
        {
            "is_folder": True,
            "base_display_label": "folder: Sub",
            "item_path": r"Root\F\Sub",
            "tree_role": "destination",
            "node_origin": "sharepoint",
        },
    )
    file3 = QTreeWidgetItem()
    file3.setData(
        0,
        Qt.UserRole,
        {
            "is_folder": False,
            "name": "File3.docx",
            "item_path": r"Root\F\Sub\File3.docx",
            "tree_role": "destination",
        },
    )
    f_item.addChild(sub_shell)
    sub_real.addChild(file3)
    f_item.addChild(sub_real)
    assert f_item.childCount() == 2

    mw._reconcile_destination_semantic_duplicates("test")
    assert f_item.childCount() == 1
    merged = f_item.child(0)
    assert merged.childCount() == 1
