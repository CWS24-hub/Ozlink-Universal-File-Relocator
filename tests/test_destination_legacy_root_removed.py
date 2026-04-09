"""Destination: no synthetic logical-root ``Root`` hub; real folder named Root is structural only."""

from __future__ import annotations

import unittest

from PySide6.QtCore import QModelIndex

from ozlink_console.main_window import (
    MainWindow,
    _DESTINATION_FUTURE_MODEL_LIBRARY_PARENT_KEY,
)


class DestinationLegacyRootRemovedTests(unittest.TestCase):
    def test_library_parent_key_is_empty_not_name_root(self):
        self.assertEqual(_DESTINATION_FUTURE_MODEL_LIBRARY_PARENT_KEY, "")

    def test_normalize_parent_root_never_remapped_to_library(self):
        mw = MainWindow.__new__(MainWindow)
        # Parent "Root" is always the real folder name (no hub → library inference).
        self.assertEqual(
            MainWindow._destination_future_model_normalize_parent_key(mw, "Root", "Finance"),
            "Root",
        )
        self.assertEqual(
            MainWindow._destination_future_model_normalize_parent_key(mw, "Root", r"Root\Sub"),
            "Root",
        )

    def test_semantic_depth_one_treats_real_root_like_any_top_level(self):
        mw = MainWindow.__new__(MainWindow)
        mw._canonical_destination_projection_path = lambda p: str(p or "").strip()
        self.assertTrue(MainWindow._destination_semantic_path_is_library_depth_one(mw, "Finance"))
        self.assertTrue(MainWindow._destination_semantic_path_is_library_depth_one(mw, "Root"))
        self.assertFalse(MainWindow._destination_semantic_path_is_library_depth_one(mw, r"Root\Sub"))

    def test_model_index_library_top_level(self):
        mw = MainWindow.__new__(MainWindow)
        # Uninitialized index: not library top-level
        self.assertFalse(MainWindow._destination_model_index_is_library_top_level(mw, QModelIndex()))


if __name__ == "__main__":
    unittest.main()
