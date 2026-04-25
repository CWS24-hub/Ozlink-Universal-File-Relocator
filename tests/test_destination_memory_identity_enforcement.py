"""Destination snapshot identity: selected library must win over stale snapshot identity."""

import unittest
from unittest.mock import MagicMock, patch

from ozlink_console.destination_startup_snapshot_roots import (
    DestinationStartupSnapshotRootContext,
    apply_destination_snapshot_identity_gate,
    select_validated_destination_startup_snapshot,
)
from ozlink_console.main_window import MainWindow
from ozlink_console.models import SessionState


def _dest_root(text: str, drive: str):
    return {
        "text": text,
        "data": {"name": text, "tree_role": "destination", "drive_id": drive},
        "children": [],
    }


class DestinationMemoryIdentityEnforcementTests(unittest.TestCase):
    def test_documents_envelope_rejected_when_intended_is_other_library(self):
        """Session snapshot stamped for Documents must not load when intended drive is another library."""
        ctx = DestinationStartupSnapshotRootContext(
            browse_mode="sharepoint",
            destination_drive_id="other-drive",
            source_drive_id="src",
        )
        session = [_dest_root("Root", "other-drive")]
        chosen, label, _, _, meta = select_validated_destination_startup_snapshot(
            session,
            [],
            ctx,
            session_envelope_drive_id="docs-drive",
            session_envelope_library_id="docs-drive",
            session_envelope_site_id="",
            sidecar_envelope_drive_id="",
            sidecar_envelope_library_id="",
            sidecar_envelope_site_id="",
            intended_drive_id="other-drive",
            intended_site_id="",
            legacy_library_candidates=None,
        )
        self.assertEqual(chosen, [])
        self.assertIn("SessionState", label)
        self.assertEqual(meta.get("session_identity_gate"), "rejected_envelope_mismatch")

    def test_combo_drive_id_wins_over_destination_tree_snapshot_identity_in_session_state(self):
        """Intended drive follows combo; stale snapshot envelope (Documents) is rejected when combo differs."""
        mw = MainWindow.__new__(MainWindow)
        mw._draft_shell_state = SessionState(
            DestinationTreeSnapshotIdentityDriveId="docs-drive",
            DestinationTreeSnapshotIdentityLibraryId="docs-drive",
            SelectedDestinationLibraryId="wrong-session-id",
        )
        mw._current_selected_destination_drive_id = lambda: "combo-drive"
        mw._current_selected_destination_site_id = lambda: ""
        mw._destination_startup_snapshot_root_context = lambda: DestinationStartupSnapshotRootContext(
            browse_mode="sharepoint",
            destination_drive_id="combo-drive",
            source_drive_id="src",
        )
        mw._destination_legacy_snapshot_library_candidates = lambda: []
        with patch("ozlink_console.main_window.log_info", lambda *a, **k: None):
            chosen, _label, _, _, meta = MainWindow._select_destination_tree_snapshot_for_startup(
                mw,
                [_dest_root("R", "combo-drive")],
                workspace_sidecar=None,
            )
        self.assertEqual(meta.get("session_identity_gate"), "rejected_envelope_mismatch")
        self.assertEqual(chosen, [])

    def test_matching_drive_but_mismatched_site_rejected_when_both_site_ids_present(self):
        snaps = [_dest_root("R", "d1")]
        out, tag = apply_destination_snapshot_identity_gate(
            snaps,
            intended_drive_id="d1",
            snapshot_stored_drive_id="d1",
            snapshot_stored_library_id="d1",
            source="session",
            intended_site_id="site-a",
            snapshot_stored_site_id="site-b",
        )
        self.assertEqual(out, [])
        self.assertEqual(tag, "rejected_site_mismatch")

    def test_explicit_intended_site_rejects_when_snapshot_site_missing(self):
        snaps = [_dest_root("R", "d1")]
        out, tag = apply_destination_snapshot_identity_gate(
            snaps,
            intended_drive_id="d1",
            snapshot_stored_drive_id="d1",
            snapshot_stored_library_id="d1",
            source="session",
            intended_site_id="site-a",
            snapshot_stored_site_id="",
        )
        self.assertEqual(out, [])
        self.assertEqual(tag, "rejected_snapshot_site_missing_for_explicit_intended")

    def test_site_check_skipped_when_intended_site_empty(self):
        snaps = [_dest_root("R", "d1")]
        out, tag = apply_destination_snapshot_identity_gate(
            snaps,
            intended_drive_id="d1",
            snapshot_stored_drive_id="d1",
            snapshot_stored_library_id="d1",
            source="session",
            intended_site_id="",
            snapshot_stored_site_id="site-a",
        )
        self.assertEqual(len(out), 1)
        self.assertEqual(tag, "identity_ok")

    def test_mismatched_snapshot_returns_empty_so_startup_replay_has_no_rows_to_audit(self):
        """Empty chosen snapshot implies no snapshot-bound tree for replay to treat as complete."""
        ctx = DestinationStartupSnapshotRootContext(
            browse_mode="sharepoint",
            destination_drive_id="lib-b",
            source_drive_id="src",
        )
        chosen, _, _, _, meta = select_validated_destination_startup_snapshot(
            [_dest_root("X", "lib-a")],
            [],
            ctx,
            session_envelope_drive_id="lib-a",
            session_envelope_library_id="lib-a",
            intended_drive_id="lib-b",
            legacy_library_candidates=None,
        )
        self.assertEqual(chosen, [])
        self.assertEqual(meta.get("session_identity_gate"), "rejected_envelope_mismatch")

    def test_library_change_clears_runtime_snapshot_and_flags(self):
        mw = MainWindow.__new__(MainWindow)
        mw._planning_browse_mode = lambda _k: "sharepoint"
        mw._draft_shell_state = SessionState(
            DestinationTreeSnapshotIdentityDriveId="d1",
            SelectedDestinationLibraryId="d1",
        )
        mw._runtime_session_tree_snapshots = {"source": [], "destination": [{"x": 1}]}
        mw._pending_session_tree_snapshots = {"destination": [{"y": 2}]}
        mw._destination_provisional_startup_applied = True
        mw._startup_visible_snapshot_bound = True
        mw._destination_startup_snapshot_mount_seen = True
        mw._destination_snapshot_mount_drive_id = "d1"
        mw._destination_last_bound_library_drive_id = "d1"
        mw.destination_planning_model = MagicMock()
        with patch("ozlink_console.main_window.log_info", lambda *a, **k: None):
            MainWindow._destination_clear_stale_snapshot_state_on_library_change(
                mw,
                selected_site={"id": "s1", "name": "S"},
                selected_library={"id": "d2", "name": "Other"},
            )
        self.assertEqual(mw._runtime_session_tree_snapshots["destination"], [])
        self.assertEqual(mw._pending_session_tree_snapshots["destination"], [])
        self.assertFalse(mw._destination_provisional_startup_applied)
        self.assertFalse(mw._startup_visible_snapshot_bound)
        self.assertFalse(mw._destination_startup_snapshot_mount_seen)
        self.assertEqual(mw._draft_shell_state.DestinationTreeSnapshotIdentityDriveId, "")
        mw.destination_planning_model.clear.assert_called_once()

    def test_site_change_clears_snapshots_before_library_valid_even_if_library_invalid(self):
        """Stale runtime/pending snapshots clear on site change even when library currentData is invalid."""
        mw = MainWindow.__new__(MainWindow)
        mw._planning_browse_mode = lambda _k: "sharepoint"
        mw._destination_last_memory_site_id = "site-a"
        mw._runtime_session_tree_snapshots = {"source": [], "destination": [{"k": 1}]}
        mw._pending_session_tree_snapshots = {"destination": [{"k": 2}]}
        mw.pending_root_drive_ids = {"source": "", "destination": ""}
        mw.pending_root_site_ids = {"source": "", "destination": ""}
        mw._draft_shell_state = SessionState(DestinationTreeSnapshotIdentityDriveId="d1")
        site_sel = MagicMock()
        site_sel.currentData.return_value = {"id": "site-b", "name": "B"}
        lib_sel = MagicMock()
        lib_sel.currentData.return_value = None
        mw.planning_inputs = {"Destination Site": site_sel, "Destination Library": lib_sel}
        mw._populate_library_selector_for_group = lambda _g: None
        mw._maybe_schedule_legacy_snapshot_identity_inference_retry = lambda *_a, **_k: None
        mw._pending_login_restore_args = None
        mw._suppress_selector_change_handlers = False
        mw.bottom_destination = MagicMock()
        mw.set_tree_placeholder = lambda *a, **k: None
        mw.update_selector_context_labels = lambda *a, **k: None
        with patch("ozlink_console.main_window.log_info", lambda *a, **k: None):
            MainWindow.on_site_selector_changed(mw, "destination", chain_library=True)
        self.assertEqual(mw._runtime_session_tree_snapshots["destination"], [])
        self.assertEqual(mw._pending_session_tree_snapshots["destination"], [])
        self.assertEqual(mw._destination_last_memory_site_id, "site-b")

    def test_destination_site_change_resets_snapshot_bound_flags(self):
        mw = MainWindow.__new__(MainWindow)
        mw._planning_browse_mode = lambda _k: "sharepoint"
        mw._runtime_session_tree_snapshots = {"destination": [{"x": 1}]}
        mw._pending_session_tree_snapshots = {"destination": []}
        mw._destination_provisional_startup_applied = True
        mw._startup_visible_snapshot_bound = True
        mw._destination_startup_snapshot_mount_seen = True
        mw._startup_memory_visible_tree_ready_mono = 1.0
        mw._draft_shell_state = SessionState()
        mw.destination_planning_model = MagicMock()
        mw.pending_root_drive_ids = {"source": "", "destination": "pd"}
        mw.pending_root_site_ids = {"source": "", "destination": "ps"}
        with patch("ozlink_console.main_window.log_info", lambda *a, **k: None):
            MainWindow._destination_clear_stale_snapshot_state_on_site_change(
                mw,
                selected_site={"id": "s2", "name": "S"},
                previous_site_id="s1",
                new_site_id="s2",
            )
        self.assertFalse(mw._destination_provisional_startup_applied)
        self.assertFalse(mw._startup_visible_snapshot_bound)
        self.assertFalse(mw._destination_startup_snapshot_mount_seen)
        self.assertEqual(mw._startup_memory_visible_tree_ready_mono, 0.0)
        self.assertTrue(mw._destination_suppress_provisional_placeholder_preservation)
        self.assertEqual(mw.pending_root_drive_ids["destination"], "")
        self.assertEqual(mw.pending_root_site_ids["destination"], "")

    def test_cross_site_same_library_name_snapshot_not_reused_when_site_mismatch(self):
        """Same drive id label across sites: explicit intended site without snapshot site is rejected."""
        snaps = [_dest_root("Documents", "docs-drive-1")]
        out, tag = apply_destination_snapshot_identity_gate(
            snaps,
            intended_drive_id="docs-drive-1",
            snapshot_stored_drive_id="docs-drive-1",
            snapshot_stored_library_id="docs-drive-1",
            source="session",
            intended_site_id="site-contoso",
            snapshot_stored_site_id="",
        )
        self.assertEqual(out, [])
        self.assertEqual(tag, "rejected_snapshot_site_missing_for_explicit_intended")

    def test_site_change_clears_destination_planning_model(self):
        mw = MainWindow.__new__(MainWindow)
        mw._planning_browse_mode = lambda _k: "sharepoint"
        mw._runtime_session_tree_snapshots = {"destination": []}
        mw._pending_session_tree_snapshots = {"destination": []}
        mw._draft_shell_state = SessionState()
        mw.destination_planning_model = MagicMock()
        mw.pending_root_drive_ids = {"source": "", "destination": ""}
        mw.pending_root_site_ids = {"source": "", "destination": ""}
        mw.planned_moves = []
        mw.proposed_folders = []
        with patch("ozlink_console.main_window.log_info", lambda *a, **k: None):
            MainWindow._destination_clear_stale_snapshot_state_on_site_change(
                mw,
                selected_site={"id": "s2"},
                previous_site_id="s0",
                new_site_id="s2",
            )
        mw.destination_planning_model.clear.assert_called_once()


if __name__ == "__main__":
    unittest.main()
