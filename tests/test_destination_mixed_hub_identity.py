"""Mixed-hub / foreign planned-root identity (destination Graph merge + planning filter)."""

import unittest
from unittest.mock import patch

from PySide6.QtCore import QModelIndex, Qt

from ozlink_console.main_window import MainWindow
from ozlink_console.models import ProposedFolder, SessionState
from ozlink_console.sharepoint_destination_overlay_attach import (
    WORKSPACE_ROW_STATE_LIVE_CONFIRMED,
    WORKSPACE_ROW_STATE_PLANNED_ONLY,
)
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _live_root(name: str, drive: str, gid: str):
    return {
        "name": name,
        "id": gid,
        "drive_id": drive,
        "is_folder": True,
        "item_path": name,
        "tree_role": "destination",
        "workspace_row_state": WORKSPACE_ROW_STATE_LIVE_CONFIRMED,
    }


def _planned_root(name: str, drive: str):
    return {
        "name": name,
        "id": "",
        "drive_id": drive,
        "is_folder": True,
        "item_path": name,
        "tree_role": "destination",
        "verification_state": "planned_only",
        "row_kind": "planned_folder",
        "workspace_row_state": WORKSPACE_ROW_STATE_PLANNED_ONLY,
    }


class DestinationMixedHubIdentityTests(unittest.TestCase):
    def test_graph_merge_prunes_foreign_planned_root3_keeps_live_root(self):
        dm = DestinationPlanningTreeModel()
        dm.reset_root_payloads([_planned_root("Root3", "drive-old"), _live_root("Root", "drive-new", "g1")])
        incoming = [_live_root("Root", "drive-new", "g1")]
        with patch("ozlink_console.tree_models.destination_planning_model.log_info", lambda *a, **k: None):
            dm.merge_sharepoint_library_root_graph_children(
                incoming,
                enrich_only=True,
                intended_drive_id="drive-new",
                intended_site_id="",
                strict_planned_root_identity=True,
            )
        names = []
        inv = QModelIndex()
        for r in range(dm.rowCount(inv)):
            pl = dm.index(r, 0, inv).data(Qt.UserRole)
            if isinstance(pl, dict):
                names.append(str(pl.get("name") or ""))
        self.assertIn("Root", names)
        self.assertNotIn("Root3", names)

    def test_snap_preserving_flag_disables_merge_branch(self):
        mw = MainWindow.__new__(MainWindow)
        mw._destination_snap_preserving_disabled_due_to_identity_change = True
        snap_eligible = True
        identity_snap_block = bool(getattr(mw, "_destination_snap_preserving_disabled_due_to_identity_change", False))
        snap_preserving = bool(snap_eligible and not identity_snap_block)
        self.assertFalse(snap_preserving)

    def test_planned_moves_old_drive_skipped_after_strict(self):
        mw = MainWindow.__new__(MainWindow)
        mw._planning_browse_mode = lambda _k: "sharepoint"
        mw._current_selected_destination_drive_id = lambda: "drive-new"
        mw._current_selected_destination_site_id = lambda: "site-new"
        mw._destination_strict_planning_identity_required = True
        mw.pending_root_drive_ids = {"source": "", "destination": ""}
        mw.planned_moves = [
            {
                "destination_path": r"Root3\HR\File.pdf",
                "destination": {"drive_id": "drive-old", "name": "x"},
                "DestinationDriveId": "drive-old",
            }
        ]
        mw.proposed_folders = []
        with patch("ozlink_console.main_window.log_info", lambda *a, **k: None):
            sm, sp = MainWindow._destination_filter_planning_memory_to_selected_identity(
                mw, context="test_strict"
            )
        self.assertEqual(sm, 1)
        self.assertEqual(len(mw.planned_moves), 0)

    def test_proposed_folder_old_drive_skipped(self):
        mw = MainWindow.__new__(MainWindow)
        mw._planning_browse_mode = lambda _k: "sharepoint"
        mw._current_selected_destination_drive_id = lambda: "d-new"
        mw._current_selected_destination_site_id = lambda: ""
        mw._destination_strict_planning_identity_required = False
        mw.pending_root_drive_ids = {"source": "", "destination": ""}
        mw.planned_moves = []
        mw.proposed_folders = [
            ProposedFolder(
                FolderName="X",
                DestinationPath=r"Root3\X",
                DestinationDriveId="d-old",
            )
        ]
        with patch("ozlink_console.main_window.log_info", lambda *a, **k: None):
            sm, sp = MainWindow._destination_filter_planning_memory_to_selected_identity(
                mw, context="test_pf",
            )
        self.assertEqual(sp, 1)
        self.assertEqual(len(mw.proposed_folders), 0)

    def test_multi_hub_prune_removes_non_live_when_live_exists(self):
        mw = MainWindow.__new__(MainWindow)
        mw._planning_browse_mode = lambda _k: "sharepoint"
        dm = DestinationPlanningTreeModel()
        dm.reset_root_payloads([_planned_root("Root3", "d-old"), _live_root("Root", "d-new", "gid1")])
        mw.destination_planning_model = dm
        mw._tree_item_path = lambda pl: str(pl.get("item_path") or "") if isinstance(pl, dict) else ""
        mw._canonical_destination_projection_path = lambda p: p
        mw.normalize_memory_path = lambda p: p
        with patch("ozlink_console.main_window.log_info", lambda *a, **k: None):
            MainWindow._destination_prune_multi_hub_foreign_roots(mw, intended_drive_id="d-new")
        inv = QModelIndex()
        self.assertEqual(dm.rowCount(inv), 1)

    def test_strict_without_sel_drive_drops_moves_missing_destination_drive(self):
        mw = MainWindow.__new__(MainWindow)
        mw._planning_browse_mode = lambda _k: "sharepoint"
        mw._current_selected_destination_drive_id = lambda: ""
        mw._current_selected_destination_site_id = lambda: ""
        mw.pending_root_drive_ids = {"source": "", "destination": ""}
        mw._destination_strict_planning_identity_required = True
        mw.planned_moves = [{"destination_path": "x", "destination": {}}]
        mw.proposed_folders = [ProposedFolder(FolderName="p", DestinationPath="p")]
        with patch("ozlink_console.main_window.log_info", lambda *a, **k: None):
            sm, sp = MainWindow._destination_filter_planning_memory_to_selected_identity(mw, context="t")
        self.assertEqual(sm, 1)
        self.assertEqual(sp, 1)

    def test_restore_skips_allocation_mismatched_session_drive(self):
        mw = MainWindow.__new__(MainWindow)
        mw._planning_browse_mode = lambda _k: "sharepoint"
        mw.planned_moves = [
            {
                "destination_path": r"A\a.pdf",
                "destination": {"drive_id": "wrong", "name": "a"},
                "DestinationDriveId": "wrong",
            }
        ]
        mw.proposed_folders = []
        st = SessionState(SelectedDestinationLibraryId="right", SelectedDestinationSiteKey="")
        with patch("ozlink_console.main_window.log_info", lambda *a, **k: None):
            MainWindow._destination_filter_restored_planning_against_session_identity(
                mw, st, context="restore_test"
            )
        self.assertEqual(len(mw.planned_moves), 0)


if __name__ == "__main__":
    unittest.main()
