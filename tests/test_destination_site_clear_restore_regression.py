"""Regression tests for destination site-clear vs restore (selector + planning identity)."""

import unittest
from unittest.mock import MagicMock, patch

from ozlink_console.main_window import MainWindow
from ozlink_console.models import SessionState


class DestinationSiteClearRestoreRegressionTests(unittest.TestCase):
    def test_restore_rebind_preserves_planned_and_proposed(self):
        mw = MainWindow.__new__(MainWindow)
        mw._planning_browse_mode = lambda _k: "sharepoint"
        mw._runtime_session_tree_snapshots = {"destination": []}
        mw._pending_session_tree_snapshots = {"destination": []}
        mw._draft_shell_state = SessionState(
            SelectedDestinationLibraryId="drive-1",
            SelectedDestinationLibrary="Documents",
        )
        mw.destination_planning_model = MagicMock()
        mw.pending_root_drive_ids = {"source": "", "destination": ""}
        mw.pending_root_site_ids = {"source": "", "destination": ""}
        mw.planned_moves = [{"source_path": "/a", "destination_path": "/b"}]
        mw.proposed_folders = [{"name": "x"}]
        mw.refresh_planned_moves_table = MagicMock()
        with patch("ozlink_console.main_window.log_info", lambda *a, **k: None):
            MainWindow._destination_clear_stale_snapshot_state_on_site_change(
                mw,
                selected_site={"id": "s2", "name": "S"},
                previous_site_id="s1",
                new_site_id="s2",
                reason="restore_rebind",
            )
        self.assertEqual(len(mw.planned_moves), 1)
        self.assertEqual(len(mw.proposed_folders), 1)
        self.assertEqual(mw._draft_shell_state.SelectedDestinationLibraryId, "drive-1")
        self.assertEqual(mw._draft_shell_state.SelectedDestinationLibrary, "Documents")

    def test_user_site_change_clears_planned_moves(self):
        mw = MainWindow.__new__(MainWindow)
        mw._planning_browse_mode = lambda _k: "sharepoint"
        mw._runtime_session_tree_snapshots = {"destination": []}
        mw._pending_session_tree_snapshots = {"destination": []}
        mw._draft_shell_state = SessionState()
        mw.destination_planning_model = MagicMock()
        mw.pending_root_drive_ids = {"source": "", "destination": ""}
        mw.pending_root_site_ids = {"source": "", "destination": ""}
        mw.planned_moves = [{"x": 1}]
        mw.proposed_folders = [{"y": 2}]
        mw.refresh_planned_moves_table = MagicMock()
        with patch("ozlink_console.main_window.log_info", lambda *a, **k: None):
            MainWindow._destination_clear_stale_snapshot_state_on_site_change(
                mw,
                selected_site={"id": "s2", "name": "S"},
                previous_site_id="s1",
                new_site_id="s2",
                reason="user_site_change",
            )
        self.assertEqual(mw.planned_moves, [])
        self.assertEqual(mw.proposed_folders, [])

    def test_site_clear_does_not_erase_persisted_library_fields(self):
        mw = MainWindow.__new__(MainWindow)
        mw._planning_browse_mode = lambda _k: "sharepoint"
        mw._runtime_session_tree_snapshots = {"destination": []}
        mw._pending_session_tree_snapshots = {"destination": []}
        mw._draft_shell_state = SessionState(
            SelectedDestinationLibraryId="lib-drive",
            SelectedDestinationLibrary="LibName",
        )
        mw.destination_planning_model = MagicMock()
        mw.pending_root_drive_ids = {"source": "", "destination": ""}
        mw.pending_root_site_ids = {"source": "", "destination": ""}
        mw.planned_moves = []
        mw.proposed_folders = []
        with patch("ozlink_console.main_window.log_info", lambda *a, **k: None):
            MainWindow._destination_clear_stale_snapshot_state_on_site_change(
                mw,
                selected_site={"id": "s2", "name": "S"},
                previous_site_id="s1",
                new_site_id="s2",
                reason="user_site_change",
            )
        self.assertEqual(mw._draft_shell_state.SelectedDestinationLibraryId, "lib-drive")
        self.assertEqual(mw._draft_shell_state.SelectedDestinationLibrary, "LibName")

    def test_on_library_relaxes_strict_before_planning_filter(self):
        """Strict must be off before filtering so legacy rows without DestinationDriveId are not dropped."""
        mw = MainWindow.__new__(MainWindow)
        mw._log_library_restore_step = lambda *a, **k: None
        mw._pending_login_restore_args = None
        mw._suppress_selector_change_handlers = False
        mw._planning_browse_mode = lambda _g: "sharepoint"
        mw._graph_token_ready_for_sharepoint = lambda: True
        site_sel = MagicMock()
        site_sel.currentData.return_value = {"id": "site1", "name": "S"}
        lib_sel = MagicMock()
        lib_sel.currentData.return_value = {"id": "drive-1", "name": "Documents"}
        mw.planning_inputs = {"Destination Site": site_sel, "Destination Library": lib_sel}
        mw.bottom_destination = MagicMock()
        mw.update_selector_context_labels = MagicMock()
        mw._destination_suppress_provisional_placeholder_preservation = True
        mw._destination_last_memory_site_id = ""
        order: list[str] = []

        def _clear_lib(*_a, **_k):
            mw._destination_strict_planning_identity_required = True

        def _filter(**_k):
            order.append("filter")
            strict = bool(getattr(mw, "_destination_strict_planning_identity_required", False))
            order.append(f"strict_during_filter={strict}")

        mw._destination_clear_stale_snapshot_state_on_library_change = _clear_lib
        mw._destination_filter_planning_memory_to_selected_identity = _filter
        mw.load_library_root = lambda *a, **k: None
        mw._schedule_live_root_refresh = lambda *a, **k: None
        mw._memory_restore_in_progress = False
        with patch("ozlink_console.main_window.log_info", lambda *a, **k: None):
            MainWindow.on_library_selector_changed(mw, "destination", force=True)
        self.assertIn("filter", order)
        self.assertIn("strict_during_filter=False", order)

    def test_filter_keeps_legacy_planned_row_without_drive_when_strict_false(self):
        mw = MainWindow.__new__(MainWindow)
        mw._planning_browse_mode = lambda _k: "sharepoint"
        mw.planning_inputs = {"Destination Site": MagicMock(), "Destination Library": MagicMock()}
        mw._current_selected_destination_drive_id = lambda: "d1"
        mw._current_selected_destination_site_id = lambda: "site-a"
        mw.pending_root_drive_ids = {"destination": ""}
        mw._destination_strict_planning_identity_required = False
        mw.planned_moves = [{"DestinationSiteId": "site-a", "destination_path": "/x"}]
        mw.proposed_folders = []
        with patch("ozlink_console.main_window.log_info", lambda *a, **k: None):
            skipped_m, skipped_p = MainWindow._destination_filter_planning_memory_to_selected_identity(
                mw, context="test"
            )
        self.assertEqual(skipped_m, 0)
        self.assertEqual(len(mw.planned_moves), 1)

    def test_filter_drops_explicit_drive_mismatch(self):
        mw = MainWindow.__new__(MainWindow)
        mw._planning_browse_mode = lambda _k: "sharepoint"
        mw.planning_inputs = {"Destination Site": MagicMock(), "Destination Library": MagicMock()}
        mw._current_selected_destination_drive_id = lambda: "d1"
        mw._current_selected_destination_site_id = lambda: ""
        mw.pending_root_drive_ids = {"destination": ""}
        mw._destination_strict_planning_identity_required = False
        mw.planned_moves = [{"DestinationDriveId": "other", "destination_path": "/x"}]
        mw.proposed_folders = []
        with patch("ozlink_console.main_window.log_info", lambda *a, **k: None):
            skipped_m, _ = MainWindow._destination_filter_planning_memory_to_selected_identity(mw, context="test")
        self.assertEqual(skipped_m, 1)
        self.assertEqual(mw.planned_moves, [])


if __name__ == "__main__":
    unittest.main()
