"""Destination authority shell: full_tree_ready + trust TTL recovery (watchdog)."""

import unittest
from unittest.mock import MagicMock, patch

from ozlink_console.main_window import MainWindow


class DestinationReconcileStallRecoveryTests(unittest.TestCase):
    def test_full_tree_ready_true_when_snapshot_empty_but_walk_completed_for_drive(self):
        mw = MainWindow.__new__(MainWindow)
        mw.pending_root_drive_ids = {"source": "", "destination": ""}
        mw._current_selected_destination_drive_id = lambda: "b!drive-a"
        mw._destination_full_tree_completed_drive_id = "b!drive-a"
        mw._destination_full_tree_snapshot = []
        self.assertTrue(MainWindow._destination_full_tree_ready(mw))

    def test_full_tree_ready_false_when_selector_drive_differs(self):
        mw = MainWindow.__new__(MainWindow)
        mw.pending_root_drive_ids = {"source": "", "destination": ""}
        mw._current_selected_destination_drive_id = lambda: "b!other"
        mw._destination_full_tree_completed_drive_id = "b!drive-a"
        mw._destination_full_tree_snapshot = [{"x": 1}]
        self.assertFalse(MainWindow._destination_full_tree_ready(mw))

    def test_stall_trust_recovery_marks_trust_and_flushes(self):
        mw = MainWindow.__new__(MainWindow)
        mw._planning_browse_mode = lambda _k: "sharepoint"
        mw._destination_tree_shows_authority_pending_shell = lambda: True
        mw._destination_full_tree_completed_drive_id = "b!d1"
        mw.pending_root_drive_ids = {"source": "", "destination": "b!d1"}
        mw._current_selected_destination_drive_id = lambda: "b!d1"
        mw._destination_full_tree_snapshot = []
        mw._destination_snapshot_valid_for_drive = False
        mw._destination_snapshot_trust_drive_id = ""
        mw._destination_snapshot_trust_monotonic = 0.0

        def _mark(d, k):
            mw._destination_snapshot_valid_for_drive = True
            mw._destination_snapshot_trust_drive_id = d
            mw._destination_snapshot_trust_monotonic = __import__("time").monotonic()

        flush_calls: list[str] = []

        def _flush(reason: str):
            flush_calls.append(reason)

        mw._destination_mark_spo_snapshot_trust_valid = lambda d, k: _mark(d, k)
        mw._flush_destination_authority_shell_if_ready = _flush

        with patch("ozlink_console.main_window.log_info", lambda *a, **k: None):
            ok = MainWindow._destination_try_recover_stall_trust_and_flush(mw, "b!d1")
        self.assertTrue(ok)
        self.assertTrue(mw._destination_snapshot_valid_for_drive)
        self.assertTrue(any("stall_recovery" in r for r in flush_calls))


if __name__ == "__main__":
    unittest.main()
