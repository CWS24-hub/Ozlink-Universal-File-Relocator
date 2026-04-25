from __future__ import annotations

from unittest.mock import MagicMock

from ozlink_console.destination_full_tree_policy import destination_graph_delta_cursor_present, should_schedule_destination_full_tree
from ozlink_console.models import SessionState


class TestDestinationFullTreeSkeletonPolicy:
    def test_delta_cursor_routine_followup_suppressed_skeleton_log_variant(self, tmp_path):
        graph = MagicMock()
        p = tmp_path / "delta.json"
        p.write_text('{"delta_link": "https://graph.microsoft.com/mock"}', encoding="utf-8")
        graph._drive_delta_state_path = MagicMock(return_value=p)
        did = "drive-abc"
        assert destination_graph_delta_cursor_present(graph, did) is True
        dec = should_schedule_destination_full_tree(
            reason="deferred_background",
            drive_id=did,
            routine_followup=True,
            bootstrap=False,
            delta_cursor_present=True,
            skeleton_first_bootstrap=True,
        )
        assert dec.allowed is False
        assert dec.decision_tag == "suppressed_delta_mode"

    def test_no_delta_cursor_routine_followup_allows_fallback(self):
        dec = should_schedule_destination_full_tree(
            reason="deferred_background",
            drive_id="drive-xyz",
            routine_followup=True,
            bootstrap=False,
            delta_cursor_present=False,
            skeleton_first_bootstrap=True,
        )
        assert dec.allowed is True

    def test_bootstrap_still_overrides_delta_suppression(self):
        dec = should_schedule_destination_full_tree(
            reason="explicit_bootstrap",
            drive_id="drive-abc",
            bootstrap=True,
            routine_followup=True,
            delta_cursor_present=True,
            skeleton_first_bootstrap=False,
        )
        assert dec.allowed is True


class TestPersistedDestinationLibraryId:
    def test_session_only_intended_id(self):
        from ozlink_console.main_window import MainWindow

        m = MainWindow.__new__(MainWindow)
        st = SessionState()
        st.SelectedDestinationLibraryId = "drive-expected"
        m._draft_shell_state = st
        assert m._persisted_destination_library_drive_id_from_session() == "drive-expected"

        m._draft_shell_state = None
        assert m._persisted_destination_library_drive_id_from_session() == ""
