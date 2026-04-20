"""Draft shell capture: source/destination library ids and names persist; sparse restore promotion."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from ozlink_console.main_window import MainWindow
from ozlink_console.models import SessionState
from ozlink_console.planning_selector_restore import library_combo_index_for_session_restore


class DraftShellSelectorPersistenceTests(unittest.TestCase):
    def test_build_draft_shell_writes_library_id_and_name_from_combo_data(self):
        w = MainWindow.__new__(MainWindow)
        w._draft_shell_state = SessionState()
        w.active_draft_session_id = "DRAFT-SHELL-1"
        w.current_session_context = {"user_role": "user", "operator_upn": "", "tenant_domain": ""}
        w._plan_leaf_exclusions = set()
        w._needs_review_dismissed_inherited_paths = set()
        w._destination_tree_snapshot_dirty_for_persist = False
        w._destination_descendant_apply_tick_running = False
        w._destination_startup_memory_workspace_building = False
        w._planning_memory_nonempty = lambda: False
        w._destination_draft_save_destination_snapshot_override = None
        w._planning_browse_mode = lambda _p: "sharepoint"
        w.pending_root_drive_ids = {"source": "", "destination": ""}

        src_site = MagicMock()
        src_site.currentData.return_value = {
            "id": "site-src",
            "name": "SrcSite",
            "site_key": "https://x.sharepoint.com/sites/s1",
        }
        dst_site = MagicMock()
        dst_site.currentData.return_value = {
            "id": "site-dst",
            "name": "DstSite",
            "site_key": "https://x.sharepoint.com/sites/s1",
        }
        src_lib = MagicMock()
        src_lib.currentData.return_value = {"id": "b!src-drive", "name": "SrcLib"}
        src_lib.currentIndex.return_value = 0
        src_lib.count.return_value = 2
        src_lib.itemData.return_value = {"id": "b!src-drive", "name": "SrcLib"}
        dst_lib = MagicMock()
        dst_lib.currentData.return_value = {"id": "b!dst-drive", "name": "DstLib"}
        dst_lib.currentIndex.return_value = 0
        dst_lib.count.return_value = 2
        dst_lib.itemData.return_value = {"id": "b!dst-drive", "name": "DstLib"}

        w.planning_inputs = {
            "Source Site": src_site,
            "Source Library": src_lib,
            "Destination Site": dst_site,
            "Destination Library": dst_lib,
        }

        with patch("ozlink_console.main_window.log_info"):
            st = MainWindow._build_current_draft_shell_state(
                w, include_workspace_ui=False, save_reason="unit_test"
            )
        self.assertEqual(st.SelectedSourceLibrary, "SrcLib")
        self.assertEqual(st.SelectedSourceLibraryId, "b!src-drive")
        self.assertEqual(st.SelectedDestinationLibrary, "DstLib")
        self.assertEqual(st.SelectedDestinationLibraryId, "b!dst-drive")

    def test_merge_preserves_existing_library_when_restore_and_capture_blank(self):
        w = MainWindow.__new__(MainWindow)
        w._memory_restore_in_progress = True
        w._suppress_autosave = False
        w._memory_ui_rebind_in_progress = False
        w.pending_root_drive_ids = {"source": "", "destination": ""}
        existing = SessionState(
            SelectedSourceSiteKey="https://x.sharepoint.com/sites/s1",
            SelectedSourceLibraryId="b!keep-src",
            SelectedSourceLibrary="KeepSrc",
            SelectedDestinationSiteKey="https://x.sharepoint.com/sites/s1",
            SelectedDestinationLibraryId="b!keep-dst",
            SelectedDestinationLibrary="KeepDst",
        )
        w._draft_shell_state = existing

        src_site = MagicMock()
        src_site.currentData.return_value = {
            "name": "S",
            "site_key": "https://x.sharepoint.com/sites/s1",
        }
        dst_site = MagicMock()
        dst_site.currentData.return_value = {
            "name": "S",
            "site_key": "https://x.sharepoint.com/sites/s1",
        }
        src_lib = MagicMock()
        src_lib.currentData.return_value = None
        src_lib.currentIndex.return_value = -1
        src_lib.count.return_value = 0
        dst_lib = MagicMock()
        dst_lib.currentData.return_value = None
        dst_lib.currentIndex.return_value = -1
        dst_lib.count.return_value = 0

        w.planning_inputs = {
            "Source Site": src_site,
            "Source Library": src_lib,
            "Destination Site": dst_site,
            "Destination Library": dst_lib,
        }

        w.active_draft_session_id = "DRAFT-X"
        w.current_session_context = {"user_role": "user", "operator_upn": "", "tenant_domain": ""}
        w._plan_leaf_exclusions = set()
        w._needs_review_dismissed_inherited_paths = set()
        w._destination_tree_snapshot_dirty_for_persist = False
        w._destination_descendant_apply_tick_running = False
        w._destination_startup_memory_workspace_building = False
        w._planning_memory_nonempty = lambda: False
        w._destination_draft_save_destination_snapshot_override = None
        w._planning_browse_mode = lambda _p: "sharepoint"

        with patch("ozlink_console.main_window.log_info"):
            st = MainWindow._build_current_draft_shell_state(
                w, include_workspace_ui=False, save_reason="unit_restore"
            )
        self.assertEqual(st.SelectedSourceLibraryId, "b!keep-src")
        self.assertEqual(st.SelectedDestinationLibraryId, "b!keep-dst")

    def test_library_restore_roundtrip_non_empty_drive_id(self):
        rows = [
            ("Documents", {"id": "b!graph-drive", "name": "Documents"}),
        ]
        idx, tag = library_combo_index_for_session_restore(
            stored_drive_id="b!graph-drive",
            stored_display_name="Documents",
            item_rows=rows,
        )
        self.assertEqual(idx, 0)
        self.assertEqual(tag, "drive_id_match")

    def test_both_selectors_need_drive_id_for_match(self):
        src = [("L1", {"id": "d1", "name": "L1"})]
        di, t = library_combo_index_for_session_restore(
            stored_drive_id="d1",
            stored_display_name="L1",
            item_rows=src,
        )
        self.assertEqual(di, 0)
        dst = [("L2", {"id": "d2", "name": "L2"})]
        dj, t2 = library_combo_index_for_session_restore(
            stored_drive_id="d2",
            stored_display_name="L2",
            item_rows=dst,
        )
        self.assertEqual(dj, 0)
