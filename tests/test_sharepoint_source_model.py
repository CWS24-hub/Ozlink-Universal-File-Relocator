import os
import unittest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication

from ozlink_console.tree_models.sharepoint_source_model import SharePointSourceTreeModel


class SharePointSourceTreeModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = QApplication.instance() or QApplication([])

    def test_reset_and_find_by_id(self):
        m = SharePointSourceTreeModel()
        m.reset_root_payloads(
            [
                {"id": "r1", "name": "Root", "is_folder": True, "drive_id": "d1", "tree_role": "source", "base_display_label": "Root"},
            ]
        )
        ix = m.find_index_by_drive_item("d1", "r1")
        self.assertTrue(ix.isValid())
        pl = ix.data(Qt.UserRole)
        self.assertEqual(pl.get("id"), "r1")

    def test_replace_children_and_empty(self):
        m = SharePointSourceTreeModel()
        m.reset_root_payloads(
            [
                {
                    "id": "p",
                    "name": "P",
                    "is_folder": True,
                    "drive_id": "d",
                    "tree_role": "source",
                    "base_display_label": "P",
                },
            ]
        )
        p = m.index(0, 0, QModelIndex())
        m.replace_all_children(
            p,
            [
                {
                    "id": "c",
                    "name": "C",
                    "is_folder": False,
                    "drive_id": "d",
                    "tree_role": "source",
                    "base_display_label": "C",
                },
            ],
        )
        self.assertEqual(m.rowCount(p), 1)
        m.replace_all_children(p, [])
        self.assertEqual(m.rowCount(p), 1)
        pl0 = m.index(0, 0, p).data(Qt.UserRole) or {}
        self.assertTrue(pl0.get("placeholder"))

    def test_canonical_path_index_lookup(self):
        def key_fn(pl):
            return str(pl.get("item_path", "") or "").strip()

        m = SharePointSourceTreeModel(source_index_key_fn=key_fn)
        m.reset_root_payloads(
            [
                {
                    "id": "root",
                    "name": "Root",
                    "is_folder": True,
                    "drive_id": "d",
                    "tree_role": "source",
                    "base_display_label": "Root",
                    "item_path": "Sites\\Lib\\Root",
                },
            ]
        )
        ix = m.find_index_for_canonical_source_path("Sites\\Lib\\Root")
        self.assertTrue(ix.isValid())
        self.assertEqual((ix.data(Qt.UserRole) or {}).get("id"), "root")

        p = m.index(0, 0, QModelIndex())
        m.replace_all_children(
            p,
            [
                {
                    "id": "c1",
                    "name": "Child",
                    "is_folder": False,
                    "drive_id": "d",
                    "tree_role": "source",
                    "base_display_label": "Child",
                    "item_path": "Sites\\Lib\\Root\\Child",
                },
            ],
        )
        cix = m.find_index_for_canonical_source_path("Sites\\Lib\\Root\\Child")
        self.assertTrue(cix.isValid())
        self.assertEqual((cix.data(Qt.UserRole) or {}).get("id"), "c1")

    def test_merge_folder_children_preserves_nested_subtree(self):
        def key_fn(pl):
            return str(pl.get("item_path", "") or "").strip()

        m = SharePointSourceTreeModel(source_index_key_fn=key_fn)
        snapshots = [
            {
                "text": "Folder: R",
                "data": {
                    "id": "root",
                    "name": "R",
                    "is_folder": True,
                    "drive_id": "d1",
                    "item_path": "R",
                    "tree_role": "source",
                    "children_loaded": True,
                },
                "children": [
                    {
                        "text": "Folder: A",
                        "data": {
                            "id": "a",
                            "name": "A",
                            "is_folder": True,
                            "drive_id": "d1",
                            "item_path": "R\\A",
                            "tree_role": "source",
                            "children_loaded": True,
                        },
                        "children": [
                            {
                                "text": "File: b",
                                "data": {
                                    "id": "b",
                                    "name": "b.txt",
                                    "is_folder": False,
                                    "drive_id": "d1",
                                    "item_path": "R\\A\\b.txt",
                                    "tree_role": "source",
                                },
                                "children": [],
                            }
                        ],
                    }
                ],
            }
        ]
        m.mount_from_session_snapshot_roots(snapshots)
        inv = QModelIndex()
        root_ix = m.index(0, 0, inv)
        before = len(m.iter_depth_first())
        m.merge_sharepoint_folder_children(
            root_ix,
            [
                {
                    "id": "a",
                    "name": "A",
                    "is_folder": True,
                    "drive_id": "d1",
                    "item_path": "R\\A",
                    "tree_role": "source",
                }
            ],
            drive_id="d1",
        )
        after = len(m.iter_depth_first())
        self.assertGreaterEqual(after, before)
        a_ix = m.index(0, 0, root_ix)
        self.assertEqual(m.rowCount(a_ix), 1)
        leaf = m.index(0, 0, a_ix)
        self.assertFalse((leaf.data(Qt.UserRole) or {}).get("is_folder"))

    def test_merge_folder_children_removes_non_provisional_missing_from_graph(self):
        m = SharePointSourceTreeModel()
        m.reset_root_payloads(
            [
                {
                    "id": "p",
                    "name": "P",
                    "is_folder": True,
                    "drive_id": "d1",
                    "tree_role": "source",
                    "base_display_label": "P",
                    "children_loaded": True,
                },
            ]
        )
        p = m.index(0, 0, QModelIndex())
        m.replace_all_children(
            p,
            [
                {
                    "id": "gone",
                    "name": "Gone",
                    "is_folder": False,
                    "drive_id": "d1",
                    "tree_role": "source",
                    "base_display_label": "Gone",
                },
            ],
        )
        self.assertEqual(m.rowCount(p), 1)
        m.merge_sharepoint_folder_children(
            p,
            [],
            drive_id="d1",
        )
        pl0 = m.index(0, 0, p).data(Qt.UserRole) or {}
        self.assertTrue(pl0.get("placeholder"))

    def test_merge_folder_children_keeps_provisional_not_in_graph(self):
        m = SharePointSourceTreeModel()
        m.reset_root_payloads(
            [
                {
                    "id": "p",
                    "name": "P",
                    "is_folder": True,
                    "drive_id": "d1",
                    "tree_role": "source",
                    "base_display_label": "P",
                    "children_loaded": True,
                },
            ]
        )
        p = m.index(0, 0, QModelIndex())
        m.replace_all_children(
            p,
            [
                {
                    "id": "snap",
                    "name": "Snap",
                    "is_folder": False,
                    "drive_id": "d1",
                    "tree_role": "source",
                    "base_display_label": "Snap",
                    "source_shell_provisional": True,
                },
            ],
        )
        m.merge_sharepoint_folder_children(
            p,
            [],
            drive_id="d1",
        )
        pl0 = m.index(0, 0, p).data(Qt.UserRole) or {}
        self.assertEqual(pl0.get("id"), "snap")
        self.assertTrue(pl0.get("source_shell_provisional"))

    def test_merge_root_path_rebind_updates_row(self):
        m = SharePointSourceTreeModel()
        m.reset_root_payloads(
            [
                {
                    "id": "old-id",
                    "name": "Renamed",
                    "is_folder": True,
                    "drive_id": "d1",
                    "item_path": "hub\\Folder",
                    "tree_role": "source",
                    "base_display_label": "OldName",
                },
            ]
        )
        m.merge_sharepoint_source_root_graph_children(
            [
                {
                    "id": "new-id",
                    "name": "Renamed",
                    "is_folder": True,
                    "drive_id": "d1",
                    "item_path": "hub\\Folder",
                    "tree_role": "source",
                }
            ],
            enrich_only=False,
            default_drive_id="d1",
        )
        ix = m.index(0, 0, QModelIndex())
        pl = ix.data(Qt.UserRole) or {}
        self.assertEqual(pl.get("id"), "new-id")

    def test_merge_folder_updates_by_path_rename(self):
        m = SharePointSourceTreeModel(source_index_key_fn=lambda pl: str(pl.get("item_path", "") or "").strip())
        m.reset_root_payloads(
            [
                {
                    "id": "p",
                    "name": "P",
                    "is_folder": True,
                    "drive_id": "d1",
                    "item_path": "P",
                    "tree_role": "source",
                    "children_loaded": True,
                },
            ]
        )
        p = m.index(0, 0, QModelIndex())
        m.replace_all_children(
            p,
            [
                {
                    "id": "c-old",
                    "name": "Child",
                    "is_folder": False,
                    "drive_id": "d1",
                    "item_path": "P\\Child",
                    "tree_role": "source",
                },
            ],
        )
        c_ix = m.index(0, 0, p)
        m.merge_sharepoint_folder_children(
            p,
            [
                {
                    "id": "c-new",
                    "name": "Child",
                    "is_folder": False,
                    "drive_id": "d1",
                    "item_path": "P\\Child",
                    "tree_role": "source",
                },
            ],
            drive_id="d1",
        )
        pl = m.index(0, 0, p).data(Qt.UserRole) or {}
        self.assertEqual(pl.get("id"), "c-new")


if __name__ == "__main__":
    unittest.main()
