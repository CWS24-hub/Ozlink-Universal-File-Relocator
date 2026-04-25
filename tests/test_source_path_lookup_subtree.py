"""Source visible-path lookup cache: subtree invalidation (Graph-only tree)."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex
from PySide6.QtWidgets import QApplication, QTreeView

from ozlink_console.main_window import MainWindow


def test_source_invalidate_path_lookup_subtree_clears_descendants_only():
    mw = MainWindow.__new__(MainWindow)
    mw._source_path_lookup_negative = {"/FTBMRoot", "/FTBMRoot/Documents", "/FTBMRoot/Other"}
    mw._source_path_lookup_cache = {
        "/FTBMRoot/Documents/Child": object(),
        "/FTBMRoot/Pictures": object(),
    }
    class _M:
        def structure_generation(self):
            return 42

    mw.source_sharepoint_model = _M()
    MainWindow._source_invalidate_path_lookup_subtree(mw, "/FTBMRoot/Documents", reason="unit_test")
    assert "/FTBMRoot/Documents" not in mw._source_path_lookup_negative
    assert "/FTBMRoot/Documents/Child" not in mw._source_path_lookup_cache
    assert "/FTBMRoot" in mw._source_path_lookup_negative
    assert "/FTBMRoot/Other" in mw._source_path_lookup_negative
    assert "/FTBMRoot/Pictures" in mw._source_path_lookup_cache


def test_find_visible_source_item_by_path_index_miss_does_not_negative_cache():
    """Async folder loads populate the path index later; stale negatives broke projection descendants."""
    app = QApplication.instance()
    if app is None:
        app = QApplication([])

    mw = MainWindow.__new__(MainWindow)
    mw._source_path_lookup_negative = set()
    mw._source_path_lookup_cache = {}
    mw.source_tree_widget = QTreeView()

    class _Model:
        def structure_generation(self):
            return 1

        def find_index_for_canonical_source_path(self, _path):
            return QModelIndex()

    mw.source_sharepoint_model = _Model()
    mw._canonical_source_projection_path = lambda p: str(p or "").strip().replace("/", "\\")  # type: ignore[method-assign]
    mw._destination_user_scroll_interaction_active = lambda: False  # type: ignore[method-assign]

    assert MainWindow._find_visible_source_item_by_path(mw, "FTBMRoot\\Documents\\LaterRow") is None
    assert "FTBMRoot\\Documents\\LaterRow" not in mw._source_path_lookup_negative


def test_destination_overlay_target_path_finds_model_index_without_visible_cache():
    mw = MainWindow.__new__(MainWindow)

    class _Ix:
        def __init__(self, v: bool = True):
            self._v = v

        def isValid(self):
            return self._v

    class _DM:
        def __init__(self):
            self._hit = _Ix()

        def find_indices_for_canonical_destination_path(self, ck: str):
            if ck == "Root\\T":
                return [self._hit]
            return []

        def is_index_live(self, ix):
            return True

    mw.destination_planning_model = _DM()
    mw._canonical_destination_projection_path = lambda p: str(p or "").strip()  # type: ignore[method-assign]
    mw._normalize_memory_path = lambda p: p  # type: ignore[method-assign]
    mw._destination_visible_path_lookup_canonical_keys_ex = lambda _p, n: (["Root\\T"], "")  # type: ignore[method-assign]
    out = MainWindow._destination_find_planning_index_for_overlay_target_path(mw, "Root\\T")
    assert out is not None and out.isValid()
