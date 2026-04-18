"""Phase 1: recursive SharePoint source snapshot shell (identity-gated)."""

from __future__ import annotations

import unittest
from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication, QTreeView

from ozlink_console.main_window import MainWindow
from ozlink_console.models import SessionState
from ozlink_console.tree_models.sharepoint_source_model import SharePointSourceTreeModel


def _app():
    return QApplication.instance() or QApplication([])


class _SelectorStub:
    def __init__(self, data):
        self._data = data

    def currentData(self):
        return self._data


class _LabelStub:
    def setText(self, *_a, **_k):
        pass


def _identity_window(*, drive_id: str, site_key: str = "site-key-1", library_name: str = "Documents"):
    window = MainWindow.__new__(MainWindow)
    window._source_browse_mode = "sharepoint"
    window.planning_inputs = {
        "Source Site": _SelectorStub({"site_key": site_key, "id": site_key, "name": "Site"}),
        "Source Library": _SelectorStub({"id": drive_id, "name": library_name}),
    }
    window._draft_shell_state = SessionState(
        SourceBrowseMode="sharepoint",
        SelectedSourceSiteKey=site_key,
        SelectedSourceLibrary=library_name,
    )
    return window


def _snap_r(drive_id: str, site_key: str, path: str, is_folder: bool = True, **extra):
    d = {
        "name": path.split("\\")[-1],
        "item_path": path,
        "is_folder": is_folder,
        "drive_id": drive_id,
        "site_key": site_key,
        "tree_role": "source",
        "children_loaded": False,
    }
    d.update(extra)
    return d


class SourceSharePointSnapshotShellTests(unittest.TestCase):
    def test_model_mount_round_trip_nested_descendants(self):
        _app()
        tree = QTreeView()
        model = SharePointSourceTreeModel(
            parent=tree,
            column_labels=["Name", "Size", "Type", "Modified"],
            source_index_key_fn=lambda pl: str(pl.get("item_path", "") or "").replace("/", "\\").strip(),
        )
        drive = "drive-99"
        site = "site-99"
        snapshots = [
            {
                "text": "Folder: Root",
                "data": _snap_r(drive, site, "Root", True, id="r1", children_loaded=True),
                "children": [
                    {
                        "text": "Folder: Child",
                        "data": _snap_r(drive, site, "Root\\Child", True, id="c1", children_loaded=True),
                        "children": [
                            {
                                "text": "File: leaf.txt",
                                "data": _snap_r(drive, site, "Root\\Child\\leaf.txt", False, id="f1"),
                                "children": [],
                            }
                        ],
                    }
                ],
            }
        ]
        n = model.mount_from_session_snapshot_roots(snapshots)
        self.assertEqual(n, 3)
        inv = QModelIndex()
        self.assertEqual(model.rowCount(inv), 1)
        r0 = model.index(0, 0, inv)
        self.assertEqual(model.rowCount(r0), 1)
        c0 = model.index(0, 0, r0)
        self.assertEqual(model.rowCount(c0), 1)
        leaf = model.index(0, 0, c0)
        pl = leaf.data(Qt.UserRole) or {}
        self.assertFalse(pl.get("is_folder"))
        self.assertTrue(pl.get("source_shell_provisional"))

    def test_restore_recursive_identity_match(self):
        _app()
        w = _identity_window(drive_id="drive-A")
        tree = QTreeView()
        model = SharePointSourceTreeModel(
            parent=tree,
            column_labels=["Name", "Size", "Type", "Modified"],
            source_index_key_fn=lambda pl: str(pl.get("item_path", "") or "").replace("/", "\\").strip(),
        )
        w.source_tree_widget = tree
        w.source_sharepoint_model = model
        w.source_tree_status = _LabelStub()
        w._runtime_session_tree_snapshots = {"source": [], "destination": []}
        w._finalize_tree_snapshot_restore = MainWindow._finalize_tree_snapshot_restore.__get__(w, MainWindow)
        w._set_tree_status_message = lambda *a, **k: None
        w._count_tree_snapshot_nodes = MainWindow._count_tree_snapshot_nodes.__get__(w, MainWindow)

        snapshots = [
            {
                "text": "Folder: A",
                "data": _snap_r("drive-A", "site-key-1", "A", True, id="a1", children_loaded=True),
                "children": [
                    {
                        "text": "Folder: B",
                        "data": _snap_r("drive-A", "site-key-1", "A\\B", True, id="b1", children_loaded=True),
                        "children": [],
                    }
                ],
            }
        ]
        ok = w._restore_tree_items_snapshot("source", snapshots, "msg")
        self.assertTrue(ok)
        self.assertTrue(getattr(w, "_source_startup_snapshot_mount_seen", False))
        inv = QModelIndex()
        self.assertEqual(model.rowCount(inv), 1)
        ix = model.index(0, 0, inv)
        self.assertEqual(model.rowCount(ix), 1)

    def test_restore_skipped_drive_mismatch(self):
        _app()
        w = _identity_window(drive_id="drive-OTHER")
        tree = QTreeView()
        model = SharePointSourceTreeModel(parent=tree, source_index_key_fn=lambda pl: str(pl.get("item_path", "")))
        w.source_tree_widget = tree
        w.source_sharepoint_model = model
        w.source_tree_status = _LabelStub()
        w._runtime_session_tree_snapshots = {"source": [], "destination": []}
        w._finalize_tree_snapshot_restore = MainWindow._finalize_tree_snapshot_restore.__get__(w, MainWindow)
        w._set_tree_status_message = lambda *a, **k: None
        w._count_tree_snapshot_nodes = MainWindow._count_tree_snapshot_nodes.__get__(w, MainWindow)

        snapshots = [
            {
                "text": "Folder: A",
                "data": _snap_r("drive-X", "site-key-1", "A", True, id="a1"),
                "children": [],
            }
        ]
        ok = w._restore_tree_items_snapshot("source", snapshots, "msg")
        self.assertTrue(ok)
        self.assertFalse(getattr(w, "_source_startup_snapshot_mount_seen", False))
        self.assertEqual(model.rowCount(QModelIndex()), 0)

    def test_local_browse_mode_skips_sharepoint_shell(self):
        _app()
        w = _identity_window(drive_id="drive-A")
        w._source_browse_mode = "local"
        tree = QTreeView()
        model = SharePointSourceTreeModel(parent=tree, source_index_key_fn=lambda pl: str(pl.get("item_path", "")))
        model.reset_root_payloads([_snap_r("drive-A", "site-key-1", "Only", True, id="o1")])
        w.source_tree_widget = tree
        w.source_sharepoint_model = model
        w.source_tree_status = _LabelStub()
        w._runtime_session_tree_snapshots = {"source": [], "destination": []}
        w._finalize_tree_snapshot_restore = MainWindow._finalize_tree_snapshot_restore.__get__(w, MainWindow)
        w._set_tree_status_message = lambda *a, **k: None
        w._count_tree_snapshot_nodes = MainWindow._count_tree_snapshot_nodes.__get__(w, MainWindow)

        snapshots = [
            {
                "text": "Folder: A",
                "data": _snap_r("drive-A", "site-key-1", "A", True, id="a1"),
                "children": [{"text": "x", "data": _snap_r("drive-A", "site-key-1", "A\\x", True, id="x1"), "children": []}],
            }
        ]
        w._restore_tree_items_snapshot("source", snapshots, "msg")
        self.assertEqual(model.rowCount(QModelIndex()), 1)

    def test_force_refresh_root_replaces_instead_of_merge(self):
        _app()
        w = _identity_window(drive_id="drive-F")
        w.normalize_memory_path = MainWindow.normalize_memory_path.__get__(w, MainWindow)
        w._canonical_source_projection_path = MainWindow._canonical_source_projection_path.__get__(w, MainWindow)
        tree = QTreeView()
        model = SharePointSourceTreeModel(
            parent=tree,
            column_labels=["Name", "Size", "Type", "Modified"],
            source_index_key_fn=MainWindow._source_payload_index_key.__get__(w, MainWindow),
        )
        snapshots = [
            {
                "text": "Folder: Root",
                "data": _snap_r("drive-F", "site-key-1", "hub\\Root", True, id="root-1", children_loaded=True),
                "children": [
                    {
                        "text": "Folder: Deep",
                        "data": _snap_r("drive-F", "site-key-1", "hub\\Root\\Deep", True, id="deep-1", children_loaded=True),
                        "children": [],
                    }
                ],
            }
        ]
        model.mount_from_session_snapshot_roots(snapshots)
        w.source_tree_widget = tree
        w.source_sharepoint_model = model
        w.source_tree_status = _LabelStub()
        w.pending_root_drive_ids = {"source": "drive-F"}
        w._root_tree_bind_in_progress = False
        w._log_root_success_step = lambda *a, **k: None
        w._source_startup_snapshot_mount_seen = True
        w._source_sharepoint_root_force_replace = True
        w._set_tree_status_message = lambda *a, **k: None
        w._prewarm_source_path_lookup_cache_after_source_root_bind = lambda: None
        w._apply_tree_item_visual_state = MainWindow._apply_tree_item_visual_state.__get__(w, MainWindow)
        before = len(model.iter_depth_first())
        items = [{"name": "Root", "id": "root-1", "is_folder": True}]
        MainWindow._apply_root_payload_to_source_model_view(w, "source", items)
        after = len(model.iter_depth_first())
        self.assertLess(after, before)

    def test_graph_root_merge_preserves_descendants(self):
        _app()
        w = _identity_window(drive_id="drive-M")
        w.normalize_memory_path = MainWindow.normalize_memory_path.__get__(w, MainWindow)
        w._canonical_source_projection_path = MainWindow._canonical_source_projection_path.__get__(w, MainWindow)
        tree = QTreeView()
        model = SharePointSourceTreeModel(
            parent=tree,
            source_index_key_fn=MainWindow._source_payload_index_key.__get__(w, MainWindow),
        )
        snapshots = [
            {
                "text": "Folder: Root",
                "data": _snap_r("drive-M", "site-key-1", "hub\\Root", True, id="root-1", children_loaded=True),
                "children": [
                    {
                        "text": "Folder: Deep",
                        "data": _snap_r("drive-M", "site-key-1", "hub\\Root\\Deep", True, id="deep-1", children_loaded=True),
                        "children": [],
                    }
                ],
            }
        ]
        model.mount_from_session_snapshot_roots(snapshots)
        w.source_tree_widget = tree
        w.source_sharepoint_model = model
        w.source_tree_status = _LabelStub()
        w._root_tree_bind_in_progress = False
        w._log_root_success_step = lambda *a, **k: None
        w._source_startup_snapshot_mount_seen = True
        w._set_tree_status_message = lambda *a, **k: None
        w._prewarm_source_path_lookup_cache_after_source_root_bind = lambda: None
        w._apply_tree_item_visual_state = MainWindow._apply_tree_item_visual_state.__get__(w, MainWindow)
        before = len(model.iter_depth_first())
        items = [{"name": "Root", "id": "root-1", "is_folder": True}]
        MainWindow._apply_root_payload_to_source_model_view(w, "source", items)
        after = len(model.iter_depth_first())
        self.assertGreaterEqual(after, before)

    def test_loading_placeholder_preserves_startup_shell_same_identity(self):
        _app()
        w = _identity_window(drive_id="drive-P")
        w._planning_browse_mode = MainWindow._planning_browse_mode.__get__(w, MainWindow)
        tree = QTreeView()
        model = SharePointSourceTreeModel(
            parent=tree,
            source_index_key_fn=lambda pl: str(pl.get("item_path", "") or "").replace("/", "\\").strip(),
        )
        snapshots = [
            {
                "text": "Folder: Root",
                "data": _snap_r("drive-P", "site-key-1", "Root", True, id="r1", children_loaded=True),
                "children": [
                    {
                        "text": "File: a.txt",
                        "data": _snap_r("drive-P", "site-key-1", "Root\\a.txt", False, id="f1"),
                        "children": [],
                    }
                ],
            }
        ]
        model.mount_from_session_snapshot_roots(snapshots)
        w.source_tree_widget = tree
        w.source_sharepoint_model = model
        w.source_tree_status = _LabelStub()
        w._source_startup_snapshot_mount_seen = True
        w._source_snapshot_mount_drive_id = "drive-P"
        w.pending_root_drive_ids = {"source": "drive-P"}
        w._source_sharepoint_root_force_replace = False
        w._set_tree_status_message = lambda *a, **k: None
        before = len(model.iter_depth_first())
        MainWindow.set_tree_placeholder(w, "source", "Loading root content...")
        after = len(model.iter_depth_first())
        self.assertEqual(after, before)

    def test_loading_placeholder_force_replace_still_destructive(self):
        _app()
        w = _identity_window(drive_id="drive-F")
        w._planning_browse_mode = MainWindow._planning_browse_mode.__get__(w, MainWindow)
        tree = QTreeView()
        model = SharePointSourceTreeModel(
            parent=tree,
            source_index_key_fn=lambda pl: str(pl.get("item_path", "") or "").strip(),
        )
        model.mount_from_session_snapshot_roots(
            [
                {
                    "text": "x",
                    "data": _snap_r("drive-F", "site-key-1", "X", True, id="x1", children_loaded=True),
                    "children": [],
                }
            ]
        )
        w.source_tree_widget = tree
        w.source_sharepoint_model = model
        w.source_tree_status = _LabelStub()
        w._source_startup_snapshot_mount_seen = True
        w._source_snapshot_mount_drive_id = "drive-F"
        w.pending_root_drive_ids = {"source": "drive-F"}
        w._source_sharepoint_root_force_replace = True
        w._set_tree_status_message = lambda *a, **k: None
        MainWindow.set_tree_placeholder(w, "source", "Loading root content...")
        self.assertEqual(len(model.iter_depth_first()), 1)
        pl0 = model.index(0, 0, QModelIndex()).data(Qt.UserRole) or {}
        self.assertTrue(pl0.get("placeholder"))

    def test_loading_placeholder_drive_mismatch_destructive(self):
        _app()
        w = _identity_window(drive_id="drive-F")
        w._planning_browse_mode = MainWindow._planning_browse_mode.__get__(w, MainWindow)
        tree = QTreeView()
        model = SharePointSourceTreeModel(
            parent=tree,
            source_index_key_fn=lambda pl: str(pl.get("item_path", "") or "").strip(),
        )
        model.mount_from_session_snapshot_roots(
            [
                {
                    "text": "x",
                    "data": _snap_r("drive-F", "site-key-1", "X", True, id="x1", children_loaded=True),
                    "children": [],
                }
            ]
        )
        w.source_tree_widget = tree
        w.source_sharepoint_model = model
        w.source_tree_status = _LabelStub()
        w._source_startup_snapshot_mount_seen = True
        w._source_snapshot_mount_drive_id = "drive-F"
        w.pending_root_drive_ids = {"source": "drive-OTHER"}
        w._source_sharepoint_root_force_replace = False
        w._set_tree_status_message = lambda *a, **k: None
        MainWindow.set_tree_placeholder(w, "source", "Loading root content...")
        self.assertEqual(len(model.iter_depth_first()), 1)

    def test_guard_local_browse_mode_no_preservation(self):
        _app()
        w = _identity_window(drive_id="drive-L")
        w._source_browse_mode = "local"
        w._planning_browse_mode = MainWindow._planning_browse_mode.__get__(w, MainWindow)
        w._source_startup_snapshot_mount_seen = True
        w._source_snapshot_mount_drive_id = "drive-L"
        w.pending_root_drive_ids = {"source": "drive-L"}
        sup, info = MainWindow._source_loading_placeholder_shell_preservation_guard(w, "Loading root content...")
        self.assertFalse(sup)
        self.assertEqual(info["action_taken"], "not_sharepoint_browse_mode")

    def test_guard_site_key_mismatch_blocks_preservation(self):
        _app()
        w = _identity_window(drive_id="drive-S")
        w._planning_browse_mode = MainWindow._planning_browse_mode.__get__(w, MainWindow)
        w._source_startup_snapshot_mount_seen = True
        w._source_snapshot_mount_drive_id = "drive-S"
        w.pending_root_drive_ids = {"source": "drive-S"}
        w.planning_inputs["Source Site"] = _SelectorStub({"site_key": "other-site", "id": "other-site", "name": "Site"})
        sup, info = MainWindow._source_loading_placeholder_shell_preservation_guard(w, "Loading root content...")
        self.assertFalse(sup)
        self.assertEqual(info["action_taken"], "site_key_mismatch_session_vs_selector")

    def test_no_duplicate_path_keys_after_mount(self):
        _app()
        tree = QTreeView()
        model = SharePointSourceTreeModel(
            parent=tree,
            source_index_key_fn=lambda pl: str(pl.get("item_path", "") or "").lower(),
        )
        snapshots = [
            {
                "text": "a",
                "data": _snap_r("d1", "s1", "p\\a", True, id="1", children_loaded=True),
                "children": [],
            },
            {
                "text": "b",
                "data": _snap_r("d1", "s1", "p\\b", True, id="2", children_loaded=True),
                "children": [],
            },
        ]
        model.mount_from_session_snapshot_roots(snapshots)
        inv = QModelIndex()
        keys = []
        for r in range(model.rowCount(inv)):
            ix = model.index(r, 0, inv)
            keys.append((ix.data(Qt.UserRole) or {}).get("item_path"))
        self.assertEqual(len(keys), len(set(keys)))


if __name__ == "__main__":
    unittest.main()
