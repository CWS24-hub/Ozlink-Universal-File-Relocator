"""Root-folder children must not duplicate as library top-level rows (future model + tree reconcile)."""

from __future__ import annotations

import pytest
from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication, QTreeWidget, QTreeWidgetItem

from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


@pytest.fixture(scope="module")
def qapp():
    return QApplication.instance() or QApplication([])


def _future_node(data, parent_semantic_path: str, node_state: str = "real") -> dict:
    return {
        "data": data,
        "parent_semantic_path": parent_semantic_path,
        "node_state": node_state,
        "children": [],
    }


def test_pack_real_flat_snapshot_entry_rejects_leaf_under_parent(qapp):
    mw = MainWindow.__new__(MainWindow)
    with pytest.raises(AssertionError, match="destination_real_snapshot_illegal_leaf_only_under_parent"):
        mw._destination_pack_real_flat_snapshot_entry(
            semantic_path="Finance",
            parent_semantic_path="Root",
            data={"id": "x"},
        )


def test_pack_real_flat_snapshot_entry_allows_library_root_row(qapp):
    mw = MainWindow.__new__(MainWindow)
    ent = mw._destination_pack_real_flat_snapshot_entry(
        semantic_path="Finance",
        parent_semantic_path="",
        data={"id": "x"},
    )
    assert ent["semantic_path"].endswith("Finance")
    assert ent["parent_semantic_path"] == ""


def test_real_snapshot_paths_prefix_leaf_with_parent_row(qapp):
    """Leaf-only item_path under folder Root must snapshot as Root\\<leaf>, not a false library-root key."""
    mw = MainWindow.__new__(MainWindow)
    sp, par = mw._destination_real_snapshot_paths_resolved(
        {"is_folder": True, "item_path": "Finance", "tree_role": "destination"},
        "Root",
    )
    assert sp.replace("/", "\\").endswith(r"Root\Finance")
    assert par.replace("/", "\\").endswith("Root")


def test_future_model_dedupe_removes_top_level_HR_when_Root_HR_exists(qapp):
    mw = MainWindow.__new__(MainWindow)
    nodes = {
        "Root": _future_node({"id": "root-id", "is_folder": True}, ""),
        "HR": _future_node({"id": "hr-id", "is_folder": True}, ""),
        r"Root\HR": _future_node({"id": "hr-id", "is_folder": True}, "Root"),
    }
    removed = mw._destination_deduplicate_root_child_top_level_leaks(nodes)
    assert removed == 1
    assert "HR" not in nodes
    assert r"Root\HR" in nodes
    top = [k for k, n in nodes.items() if not str(n.get("parent_semantic_path") or "").strip()]
    assert "HR" not in top
    assert "Root" in top


def test_future_model_dedupe_rekeys_HR_descendants_under_Root_HR(qapp):
    mw = MainWindow.__new__(MainWindow)
    nodes = {
        "Root": _future_node({"id": "root-id"}, ""),
        "HR": _future_node({"id": "hr-id"}, ""),
        r"Root\HR": _future_node({"id": "hr-id"}, "Root"),
        r"HR\Files": _future_node({"id": "f1"}, "HR"),
    }
    mw._destination_deduplicate_root_child_top_level_leaks(nodes)
    assert r"HR\Files" not in nodes
    assert r"Root\HR\Files" in nodes
    assert str(nodes[r"Root\HR\Files"].get("parent_semantic_path") or "").casefold() == r"root\hr".casefold()


def test_future_model_dedupe_skips_when_ids_conflict(qapp):
    mw = MainWindow.__new__(MainWindow)
    nodes = {
        "Root": _future_node({"id": "root-id"}, ""),
        "HR": _future_node({"id": "top-only"}, ""),
        r"Root\HR": _future_node({"id": "under-root"}, "Root"),
    }
    removed = mw._destination_deduplicate_root_child_top_level_leaks(nodes)
    assert removed == 0
    assert "HR" in nodes
    assert r"Root\HR" in nodes


def test_planning_model_reconcile_removes_top_level_HR_duplicate(qapp):
    mw = MainWindow.__new__(MainWindow)
    mw.destination_tree_widget = object()
    mw.planned_moves = []
    mw.proposed_folders = []
    mw._memory_restore_in_progress = False
    mw._suppress_selector_change_handlers = False
    mw._log_restore_phase = lambda *_a, **_k: None  # noqa: E731
    mw._refresh_destination_item_visibility_index = lambda *_a, **_k: None  # noqa: E731

    model = DestinationPlanningTreeModel(
        parent=None,
        destination_index_key_fn=mw._destination_payload_index_key,
    )
    mw.destination_planning_model = model
    mw._destination_tree_uses_model_view = lambda: True  # noqa: E731

    root_pl = {
        "is_folder": True,
        "item_path": "Root",
        "tree_role": "destination",
        "node_origin": "sharepoint",
        "id": "root-1",
        "name": "Root",
    }
    hr_under = {
        "is_folder": True,
        "item_path": r"Root\HR",
        "tree_role": "destination",
        "node_origin": "sharepoint",
        "id": "hr-1",
        "name": "HR",
    }
    proj_child = {
        "is_folder": False,
        "item_path": r"Root\HR\note.txt",
        "tree_role": "destination",
        "node_origin": "projecteddestination",
    }
    hr_top_leak = {
        "is_folder": True,
        "item_path": "HR",
        "tree_role": "destination",
        "node_origin": "projecteddestination",
        "id": "hr-1",
        "name": "HR",
        "proposed": True,
    }

    model.reset_nested(
        [
            (root_pl, [(hr_under, [(proj_child, [])])]),
            (hr_top_leak, []),
        ]
    )

    lib = QModelIndex()
    assert model.rowCount(lib) == 2
    mw._reconcile_destination_semantic_duplicates("test_root_child_leak")
    assert model.rowCount(lib) == 1
    root_ix = model.index(0, 0, lib)
    assert model.rowCount(root_ix) == 1
    hr_ix = model.index(0, 0, root_ix)
    assert (hr_ix.data(Qt.UserRole) or {}).get("item_path", "").endswith(r"Root\HR")
    assert model.rowCount(hr_ix) == 1


def test_planning_model_preserves_projection_under_root_after_top_level_leak_removed(qapp):
    """After removing illegal top-level HR, projected file stays under Root\\HR."""
    mw = MainWindow.__new__(MainWindow)
    mw.destination_tree_widget = object()
    mw.planned_moves = []
    mw.proposed_folders = []
    mw._memory_restore_in_progress = False
    mw._suppress_selector_change_handlers = False
    mw._log_restore_phase = lambda *_a, **_k: None  # noqa: E731
    mw._refresh_destination_item_visibility_index = lambda *_a, **_k: None  # noqa: E731

    model = DestinationPlanningTreeModel(
        parent=None,
        destination_index_key_fn=mw._destination_payload_index_key,
    )
    mw.destination_planning_model = model
    mw._destination_tree_uses_model_view = lambda: True  # noqa: E731

    root_pl = {
        "is_folder": True,
        "item_path": "Root",
        "tree_role": "destination",
        "node_origin": "sharepoint",
        "id": "r",
        "name": "Root",
    }
    hr_real = {
        "is_folder": True,
        "item_path": r"Root\HR",
        "tree_role": "destination",
        "node_origin": "sharepoint",
        "id": "h",
        "name": "HR",
    }
    hr_top = {
        "is_folder": True,
        "item_path": "HR",
        "tree_role": "destination",
        "node_origin": "projecteddestination",
        "id": "h",
        "planned_allocation": True,
    }
    nested_file = {
        "is_folder": False,
        "item_path": r"Root\HR\doc.docx",
        "tree_role": "destination",
        "node_origin": "projecteddestination",
    }

    model.reset_nested(
        [
            (root_pl, [(hr_real, [])]),
            (hr_top, [(nested_file, [])]),
        ]
    )

    mw._reconcile_destination_semantic_duplicates("test_projection_preserved")
    lib = QModelIndex()
    assert model.rowCount(lib) == 1
    hr_ix = model.index(0, 0, model.index(0, 0, lib))
    names = []
    for r in range(model.rowCount(hr_ix)):
        ch = model.index(r, 0, hr_ix)
        names.append((ch.data(Qt.UserRole) or {}).get("item_path", ""))
    assert any(str(p).endswith("doc.docx") for p in names)


def test_widget_reconcile_removes_top_level_HR_when_Root_HR_exists(qapp):
    mw = MainWindow.__new__(MainWindow)
    tw = QTreeWidget()
    mw.destination_tree_widget = tw
    mw.planned_moves = []
    mw.proposed_folders = []
    mw._memory_restore_in_progress = False
    mw._suppress_selector_change_handlers = False
    mw._log_restore_phase = lambda *_a, **_k: None  # noqa: E731
    mw._refresh_destination_item_visibility = lambda *_a, **_k: None  # noqa: E731
    mw._apply_tree_item_visual_state = lambda *_a, **_k: None  # noqa: E731
    mw.destination_planning_model = None
    mw._destination_tree_uses_model_view = lambda: False  # noqa: E731

    root = QTreeWidgetItem()
    root.setData(
        0,
        Qt.UserRole,
        {
            "is_folder": True,
            "item_path": "Root",
            "tree_role": "destination",
            "node_origin": "sharepoint",
            "id": "r1",
            "name": "Root",
        },
    )
    hr_under = QTreeWidgetItem()
    hr_under.setData(
        0,
        Qt.UserRole,
        {
            "is_folder": True,
            "item_path": r"Root\HR",
            "tree_role": "destination",
            "node_origin": "sharepoint",
            "id": "h1",
            "name": "HR",
        },
    )
    root.addChild(hr_under)

    hr_leak = QTreeWidgetItem()
    hr_leak.setData(
        0,
        Qt.UserRole,
        {
            "is_folder": True,
            "item_path": "HR",
            "tree_role": "destination",
            "node_origin": "projecteddestination",
            "id": "h1",
            "name": "HR",
        },
    )

    tw.addTopLevelItem(root)
    tw.addTopLevelItem(hr_leak)

    n = mw._reconcile_destination_root_child_top_level_leaks_widget("test_widget")
    assert n == 1
    assert tw.topLevelItemCount() == 1
    top0 = tw.topLevelItem(0)
    assert (top0.data(0, Qt.UserRole) or {}).get("item_path") == "Root"


def test_planning_model_reconcile_skips_when_top_and_under_root_ids_differ(qapp):
    """RECONCILE CONSTRAINT: two different Graph ids → do not remove either row."""
    mw = MainWindow.__new__(MainWindow)
    mw.destination_tree_widget = object()
    mw.planned_moves = []
    mw.proposed_folders = []
    mw._memory_restore_in_progress = False
    mw._suppress_selector_change_handlers = False
    mw._log_restore_phase = lambda *_a, **_k: None  # noqa: E731
    mw._refresh_destination_item_visibility_index = lambda *_a, **_k: None  # noqa: E731
    mw._merge_destination_projection_children_index = lambda *_a, **_k: None  # noqa: E731
    mw._destination_merge_future_overlay_into_real_index = lambda *_a, **_k: None  # noqa: E731
    mw._destination_log_root_child_top_level_screen_diagnostic = lambda *_a, **_k: None  # noqa: E731

    model = DestinationPlanningTreeModel(
        parent=None,
        destination_index_key_fn=mw._destination_payload_index_key,
    )
    mw.destination_planning_model = model
    mw._destination_tree_uses_model_view = lambda: True  # noqa: E731

    root_pl = {
        "is_folder": True,
        "item_path": "Root",
        "tree_role": "destination",
        "node_origin": "sharepoint",
        "id": "root-1",
        "name": "Root",
    }
    hr_under = {
        "is_folder": True,
        "item_path": r"Root\HR",
        "tree_role": "destination",
        "node_origin": "sharepoint",
        "id": "hr-under-id",
        "name": "HR",
    }
    hr_top = {
        "is_folder": True,
        "item_path": "HR",
        "tree_role": "destination",
        "node_origin": "sharepoint",
        "id": "hr-top-only-id",
        "name": "HR",
    }
    model.reset_nested([(root_pl, [(hr_under, [])]), (hr_top, [])])
    lib = QModelIndex()
    assert model.rowCount(lib) == 2
    removed = mw._reconcile_destination_root_child_top_level_leaks_planning_model("test_id_mismatch_skip")
    assert removed == 0
    assert model.rowCount(lib) == 2


def test_widget_reconcile_skips_when_top_and_under_root_ids_differ(qapp):
    mw = MainWindow.__new__(MainWindow)
    tw = QTreeWidget()
    mw.destination_tree_widget = tw
    mw.planned_moves = []
    mw.proposed_folders = []
    mw._memory_restore_in_progress = False
    mw._suppress_selector_change_handlers = False
    mw._log_restore_phase = lambda *_a, **_k: None  # noqa: E731
    mw._refresh_destination_item_visibility = lambda *_a, **_k: None  # noqa: E731
    mw._apply_tree_item_visual_state = lambda *_a, **_k: None  # noqa: E731
    mw._merge_destination_projection_children = lambda *_a, **_k: None  # noqa: E731
    mw._detach_destination_item = lambda *_a, **_k: None  # noqa: E731
    mw.destination_planning_model = None
    mw._destination_tree_uses_model_view = lambda: False  # noqa: E731

    root = QTreeWidgetItem()
    root.setData(
        0,
        Qt.UserRole,
        {
            "is_folder": True,
            "item_path": "Root",
            "tree_role": "destination",
            "node_origin": "sharepoint",
            "id": "r1",
            "name": "Root",
        },
    )
    hr_under = QTreeWidgetItem()
    hr_under.setData(
        0,
        Qt.UserRole,
        {
            "is_folder": True,
            "item_path": r"Root\HR",
            "tree_role": "destination",
            "node_origin": "sharepoint",
            "id": "under-hr",
            "name": "HR",
        },
    )
    root.addChild(hr_under)

    hr_leak = QTreeWidgetItem()
    hr_leak.setData(
        0,
        Qt.UserRole,
        {
            "is_folder": True,
            "item_path": "HR",
            "tree_role": "destination",
            "node_origin": "sharepoint",
            "id": "top-hr",
            "name": "HR",
        },
    )

    tw.addTopLevelItem(root)
    tw.addTopLevelItem(hr_leak)

    n = mw._reconcile_destination_root_child_top_level_leaks_widget("test_widget_id_mismatch")
    assert n == 0
    assert tw.topLevelItemCount() == 2
