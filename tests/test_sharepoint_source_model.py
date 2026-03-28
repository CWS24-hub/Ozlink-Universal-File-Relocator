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
                {"id": "r1", "name": "Root", "is_folder": True, "drive_id": "d1", "tree_role": "source", "base_display_label": "Folder: Root"},
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
                    "base_display_label": "Folder: P",
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
                    "base_display_label": "File: C",
                },
            ],
        )
        self.assertEqual(m.rowCount(p), 1)
        m.replace_all_children(p, [])
        self.assertEqual(m.rowCount(p), 1)
        pl0 = m.index(0, 0, p).data(Qt.UserRole) or {}
        self.assertTrue(pl0.get("placeholder"))


if __name__ == "__main__":
    unittest.main()
