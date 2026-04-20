"""Tests for delta-first destination scheduling guard and live-vs-memory duplicate detection."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from ozlink_console.destination_full_tree_policy import (
    destination_graph_delta_cursor_present,
    should_schedule_destination_full_tree,
)
from ozlink_console.destination_live_memory_conflicts import (
    detect_conflicts_for_live_paths,
    merge_unique,
    normalize_path_key,
)
from ozlink_console.main_window import MainWindow


class TestFullTreeScheduleGuard:
    def test_routine_followup_suppressed_when_delta_cursor_present(self):
        d = should_schedule_destination_full_tree(
            reason="post_delta",
            drive_id="drive-id-1234567890123456",
            routine_followup=True,
            delta_failed=False,
            delta_cursor_present=True,
            bootstrap_complete=True,
        )
        assert d.allowed is False
        assert d.decision_tag == "suppressed_delta_mode"

    def test_routine_followup_fallback_when_no_delta_cursor(self):
        d = should_schedule_destination_full_tree(
            reason="no_cursor",
            drive_id="drive-id-1234567890123456",
            routine_followup=True,
            delta_failed=False,
            delta_cursor_present=False,
        )
        assert d.allowed is True
        assert d.decision_tag == "fallback_no_delta_cursor"

    def test_explicit_refresh_allowed_with_cursor(self):
        d = should_schedule_destination_full_tree(
            reason="user_refresh",
            drive_id="drive-id-1234567890123456",
            explicit_refresh=True,
            routine_followup=True,
            delta_cursor_present=True,
        )
        assert d.allowed is True

    def test_bootstrap_allowed(self):
        d = should_schedule_destination_full_tree(
            reason="bootstrap",
            drive_id="drive-id-1234567890123456",
            bootstrap=True,
            routine_followup=True,
            delta_cursor_present=True,
        )
        assert d.allowed is True

    def test_delta_failed_allowed(self):
        d = should_schedule_destination_full_tree(
            reason="delta_err",
            drive_id="drive-id-1234567890123456",
            delta_failed=True,
            routine_followup=True,
            delta_cursor_present=True,
        )
        assert d.allowed is True

    def test_unknown_intent_denied(self):
        d = should_schedule_destination_full_tree(
            reason="unspecified",
            drive_id="drive-id-1234567890123456",
            routine_followup=False,
            delta_cursor_present=True,
        )
        assert d.allowed is False

    def test_delta_cursor_present_reads_persisted_file(self, tmp_path):
        g = MagicMock()

        def _p(_did):
            p = tmp_path / "state.json"
            p.write_text('{"delta_link": "https://delta.example/next"}', encoding="utf-8")
            return p

        g._drive_delta_state_path = _p
        assert destination_graph_delta_cursor_present(g, "any-drive") is True


class TestLiveVsMemoryConflicts:
    def test_proposed_folder_matches_live_folder(self):
        live = {
            normalize_path_key(r"root3\hr"): {
                "path": r"root3\hr",
                "id": "i1",
                "name": "HR",
                "type": "folder",
            }
        }
        recs = detect_conflicts_for_live_paths(
            live_path_by_key=live,
            proposed_destination_paths=[r"Root3\HR"],
            planned_destination_paths=[],
            allocation_targets=[],
        )
        assert len(recs) == 1
        assert recs[0].subtype == "live_duplicate_proposed_folder"

    def test_planned_file_matches_live_file(self):
        live = {
            normalize_path_key(r"alpha\bravo\c.txt"): {
                "path": r"alpha\bravo\c.txt",
                "id": "f1",
                "name": "c.txt",
                "type": "file",
            }
        }
        recs = detect_conflicts_for_live_paths(
            live_path_by_key=live,
            proposed_destination_paths=[],
            planned_destination_paths=[r"Alpha\Bravo\c.txt"],
            allocation_targets=[],
        )
        assert len(recs) == 1
        assert recs[0].subtype == "live_duplicate_planned_file"

    def test_unrelated_live_branch_no_conflict(self):
        live = {
            normalize_path_key(r"zzz\unrelated"): {
                "path": r"zzz\unrelated",
                "id": "z",
                "name": "u",
                "type": "folder",
            }
        }
        recs = detect_conflicts_for_live_paths(
            live_path_by_key=live,
            proposed_destination_paths=[r"Root3\HR"],
            planned_destination_paths=[r"Other\File.txt"],
            allocation_targets=[],
        )
        assert recs == []

    def test_merge_unique_dedupes_with_existing_rows(self):
        from ozlink_console.destination_live_memory_conflicts import LiveMemoryConflictRecord

        r1 = LiveMemoryConflictRecord(
            kind="live_memory_duplicate",
            subtype="live_duplicate_proposed_folder",
            planned_or_proposed_path=r"A\B",
            live_graph_path=r"A\B",
            live_item_id="i",
            live_item_name="B",
            live_item_type="folder",
        )
        existing = [r1.to_workflow_row()]
        r2 = LiveMemoryConflictRecord(
            kind="live_memory_duplicate",
            subtype="live_duplicate_proposed_folder",
            planned_or_proposed_path=r"A\B",
            live_graph_path=r"A\B",
            live_item_id="i",
            live_item_name="B",
            live_item_type="folder",
        )
        out = merge_unique([r2], existing)
        assert len(out) == 1


class TestDriveDeltaSuccessBehavior:
    def test_delta_success_does_not_schedule_full_tree(self):
        mw = MainWindow.__new__(MainWindow)
        mw._restore_abort_active = lambda: False
        mw.pending_root_drive_ids = {"destination": "d1", "source": ""}
        mw._current_selected_destination_drive_id = lambda: "d1"  # type: ignore[method-assign]
        mw.planned_moves_status = None
        mw.graph = MagicMock()
        scheduled: list[tuple] = []

        def _sched(*a, **k):
            scheduled.append((a, k))

        mw._ensure_sharepoint_destination_full_tree_worker_scheduled = _sched  # type: ignore[method-assign]
        mw._detect_runtime_live_memory_duplicate_conflicts_after_graph_delta = lambda *a, **k: None  # type: ignore
        mw._destination_try_scoped_planning_overlay_after_delta = lambda *a, **k: None  # type: ignore
        mw._apply_graph_delta_to_visible_trees = lambda *a, **k: 1  # type: ignore[method-assign]
        mw._destination_full_tree_ready = lambda: True  # type: ignore[method-assign]
        mw._destination_full_tree_completed_drive_id = "d1"
        payload = {
            "invalidated_folders": 2,
            "invalidated_entries": [
                {"drive_id": "d1", "item_id": "x"},
                {"drive_id": "d1", "item_id": "y"},
            ],
            "items_seen": 4,
            "pages": 1,
            "initial_token_run": False,
        }
        with patch(
            "ozlink_console.main_window.destination_graph_delta_cursor_present",
            return_value=True,
        ):
            MainWindow._on_drive_delta_sync_success(mw, payload, "d1")
        assert scheduled == []

    def test_delta_error_schedules_fallback(self):
        mw = MainWindow.__new__(MainWindow)
        scheduled: list[tuple] = []

        def _sched(*a, **k):
            scheduled.append((a, k))

        mw._ensure_sharepoint_destination_full_tree_worker_scheduled = _sched  # type: ignore[method-assign]
        mw.planned_moves_status = None
        with patch("ozlink_console.main_window.QTimer.singleShot") as qts:

            def _immediate(_ms, fn):
                fn()

            qts.side_effect = lambda ms, fn: _immediate(ms, fn)
            MainWindow._on_drive_delta_sync_error(mw, "bad", "d1")
        assert len(scheduled) == 1
        assert scheduled[0][1].get("delta_failed") is True
