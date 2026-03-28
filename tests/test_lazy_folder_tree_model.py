import os
import unittest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication

from ozlink_console.tree_models.lazy_folder_tree_model import LazyFolderTreeModel


class LazyFolderTreeModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._app = QApplication.instance() or QApplication([])

    def test_lazy_fetch_loads_children(self):
        calls = []

        def load(path):
            calls.append(path)
            if path == ("Root",):
                return [("Folder", "Alpha")]
            if path == ("Root", "Alpha"):
                return [("File", "readme.txt"), ("Folder", "Beta")]
            if path == ("Root", "Alpha", "Beta"):
                return []
            return []

        m = LazyFolderTreeModel(load_children=load)
        m.set_top_level([("Folder", "Root")])

        root_idx = m.index(0, 0, QModelIndex())
        self.assertEqual(m.data(root_idx, Qt.DisplayRole), "Folder: Root")
        self.assertEqual(m.rowCount(root_idx), 0)
        self.assertTrue(m.canFetchMore(root_idx))

        m.fetchMore(root_idx)
        self.assertEqual(calls, [("Root",)])
        self.assertEqual(m.rowCount(root_idx), 1)
        alpha = m.index(0, 0, root_idx)
        self.assertEqual(m.data(alpha, Qt.DisplayRole), "Folder: Alpha")
        self.assertTrue(m.canFetchMore(alpha))

        m.fetchMore(alpha)
        self.assertEqual(calls[-1], ("Root", "Alpha"))
        self.assertEqual(m.rowCount(alpha), 2)
        file_idx = m.index(0, 0, alpha)
        beta_idx = m.index(1, 0, alpha)
        self.assertEqual(m.data(file_idx, Qt.DisplayRole), "File: readme.txt")
        self.assertFalse(m.canFetchMore(file_idx))
        self.assertFalse(m.hasChildren(file_idx))
        self.assertEqual(m.data(beta_idx, Qt.DisplayRole), "Folder: Beta")

    def test_empty_folder_fetch(self):
        def load(path):
            if path == ("Root", "X"):
                return []
            if path == ("Root",):
                return [("Folder", "X")]
            return []

        m = LazyFolderTreeModel(load_children=load)
        m.set_top_level([("Folder", "Root")])

        root_idx = m.index(0, 0, QModelIndex())
        m.fetchMore(root_idx)
        x = m.index(0, 0, root_idx)
        m.fetchMore(x)
        self.assertEqual(m.rowCount(x), 0)
        self.assertFalse(m.hasChildren(x))

    def test_parent_roundtrip(self):
        m = LazyFolderTreeModel(
            load_children=lambda path: [("Folder", "Child")] if path == ("R",) else []
        )
        m.set_top_level([("Folder", "R")])
        r = m.index(0, 0, QModelIndex())
        m.fetchMore(r)
        c = m.index(0, 0, r)
        p = m.parent(c)
        self.assertEqual(p, r)


if __name__ == "__main__":
    unittest.main()
