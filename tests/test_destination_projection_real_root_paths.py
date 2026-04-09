"""Destination paths: library-relative after site/library trim; real ``Root\\`` folder preserved."""

import os
import unittest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from ozlink_console.main_window import MainWindow


def _bare_mw():
    mw = MainWindow.__new__(MainWindow)
    mw.pending_root_drive_ids = {"destination": "drive-dest"}
    return mw


class DestinationProjectionRealRootPathsTests(unittest.TestCase):
    def test_real_root_folder_keeps_full_path_segments(self):
        mw = _bare_mw()
        mw._current_destination_context_segments = lambda: []
        self.assertEqual(
            MainWindow._destination_projection_segments(mw, r"Root\Finance\Invoices"),
            ["Root", "Finance", "Invoices"],
        )

    def test_real_top_level_folder_named_root_not_stripped(self):
        mw = _bare_mw()
        mw._current_destination_context_segments = lambda: []
        self.assertEqual(MainWindow._destination_projection_segments(mw, r"Root"), ["Root"])
        self.assertEqual(MainWindow._canonical_destination_projection_path(mw, r"Root"), r"Root")

    def test_canonical_path_under_real_root_folder_unchanged(self):
        mw = _bare_mw()
        mw._current_destination_context_segments = lambda: []
        self.assertEqual(
            MainWindow._canonical_destination_projection_path(mw, r"Root\Sales\Active Projects"),
            r"Root\Sales\Active Projects",
        )

    def test_plain_library_relative_path_not_prefixed_with_root(self):
        mw = _bare_mw()
        mw._current_destination_context_segments = lambda: []
        self.assertEqual(
            MainWindow._destination_projection_segments(mw, r"HR\Policies"),
            ["HR", "Policies"],
        )
        self.assertEqual(
            MainWindow._canonical_destination_projection_path(mw, r"HR\Policies"),
            r"HR\Policies",
        )

    def test_site_library_display_path_trims_to_library_children(self):
        mw = _bare_mw()
        mw._current_destination_context_segments = lambda: ["Contoso", "Documents"]
        self.assertEqual(
            MainWindow._destination_projection_segments(mw, r"Contoso\Documents\Finance\AR"),
            ["Finance", "AR"],
        )

    def test_projected_folder_inherits_destination_drive_when_parent_empty(self):
        mw = _bare_mw()
        core = MainWindow._projected_destination_folder_core_data(
            mw,
            "Proposed",
            r"Proposed\Sub",
            {},
        )
        self.assertEqual(core.get("drive_id"), "drive-dest")
        self.assertEqual(core.get("library_id"), "drive-dest")

    def test_branch_visual_context_uses_first_library_segment(self):
        mw = _bare_mw()
        mw._current_destination_context_segments = lambda: []
        node = {
            "display_path": r"Sales\East",
            "item_path": r"Sales\East",
        }
        branch, depth = MainWindow._destination_branch_visual_context(mw, node)
        self.assertEqual(branch.lower(), "sales")
        self.assertEqual(depth, 1)


if __name__ == "__main__":
    unittest.main()
