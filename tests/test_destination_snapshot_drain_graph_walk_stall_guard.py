"""Snapshot-capture drain must not force-clear active graph-walk on false no-progress stalls."""

from __future__ import annotations

import unittest
from collections import deque
from unittest.mock import MagicMock, patch

from ozlink_console.main_window import MainWindow


def _minimal_graph_state():
    return {
        "graph_walk": True,
        "walk_phase": "walk",
        "added_count": 0,
        "desc_index": 0,
        "seg_index": 0,
        "overlay_count": 0,
        "pending_leaf_batches": {},
        "model": MagicMock(),
        "_snapshot_drain_graph_liveness": 0,
    }


class DestinationSnapshotDrainGraphWalkStallGuardTests(unittest.TestCase):
    def test_drain_does_not_force_clear_on_repeated_yield_while_graph_walk_active(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = False
        w._destination_snapshot_capture_drain_depth = 0
        w._destination_descendant_apply_paused_for_finalize_alloc = False
        w._destination_forensic_destination_model_counts = MainWindow._destination_forensic_destination_model_counts.__get__(
            w, MainWindow
        )
        w._snapshot_capture_shutdown_drain_deadline_s = MainWindow._snapshot_capture_shutdown_drain_deadline_s.__get__(
            w, MainWindow
        )
        w._destination_descendant_apply_queue = deque()
        st = _minimal_graph_state()
        w._destination_descendant_apply_state = st

        def _tick():
            MainWindow._destination_descendant_apply_drain_note_graph_tick_end(w, "graph_yield_waiting")

        w._run_destination_descendant_apply_tick = _tick
        w._destination_flush_pending_leaf_batches_for_snapshot_capture = lambda: None
        force_calls = {"n": 0}
        w._destination_descendant_apply_force_clear_on_stall = lambda **kwargs: force_calls.__setitem__("n", force_calls["n"] + 1)
        payloads: list[dict] = []

        def _cap(msg: str, **data):
            if msg == "destination_snapshot_capture_descendant_apply_drain":
                payloads.append(dict(data))

        os_env = {"OZLINK_SNAPSHOT_DRAIN_NO_PROGRESS_TICKS": "8"}
        with patch.dict("os.environ", os_env, clear=False):
            with patch("ozlink_console.main_window.log_info", side_effect=_cap):
                MainWindow._destination_finalize_inflight_descendant_apply_for_snapshot_capture(w)
        self.assertTrue(payloads)
        self.assertNotEqual(payloads[-1].get("exit_reason"), "no_progress_stall")
        self.assertEqual(force_calls["n"], 0)
        self.assertGreater(int(st.get("_snapshot_drain_graph_liveness") or 0), 4)

    def test_drain_does_not_force_clear_on_repeated_scroll_deferred_while_graph_walk_active(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = False
        w._destination_snapshot_capture_drain_depth = 0
        w._destination_descendant_apply_paused_for_finalize_alloc = False
        w._destination_forensic_destination_model_counts = MainWindow._destination_forensic_destination_model_counts.__get__(
            w, MainWindow
        )
        w._snapshot_capture_shutdown_drain_deadline_s = MainWindow._snapshot_capture_shutdown_drain_deadline_s.__get__(
            w, MainWindow
        )
        w._destination_descendant_apply_queue = deque()
        st = _minimal_graph_state()
        w._destination_descendant_apply_state = st

        def _tick():
            MainWindow._destination_descendant_apply_drain_note_graph_tick_end(w, "graph_scroll_deferred")

        w._run_destination_descendant_apply_tick = _tick
        w._destination_flush_pending_leaf_batches_for_snapshot_capture = lambda: None
        force_calls = {"n": 0}
        w._destination_descendant_apply_force_clear_on_stall = lambda **kwargs: force_calls.__setitem__("n", force_calls["n"] + 1)
        payloads: list[dict] = []

        def _cap(msg: str, **data):
            if msg == "destination_snapshot_capture_descendant_apply_drain":
                payloads.append(dict(data))

        with patch.dict("os.environ", {"OZLINK_SNAPSHOT_DRAIN_NO_PROGRESS_TICKS": "8"}, clear=False):
            with patch("ozlink_console.main_window.log_info", side_effect=_cap):
                MainWindow._destination_finalize_inflight_descendant_apply_for_snapshot_capture(w)
        self.assertTrue(payloads)
        self.assertNotEqual(payloads[-1].get("exit_reason"), "no_progress_stall")
        self.assertEqual(force_calls["n"], 0)

    def test_drain_does_not_force_clear_on_repeated_nested_noop_while_graph_walk_active(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = False
        w._destination_snapshot_capture_drain_depth = 0
        w._destination_descendant_apply_paused_for_finalize_alloc = False
        w._destination_forensic_destination_model_counts = MainWindow._destination_forensic_destination_model_counts.__get__(
            w, MainWindow
        )
        w._snapshot_capture_shutdown_drain_deadline_s = MainWindow._snapshot_capture_shutdown_drain_deadline_s.__get__(
            w, MainWindow
        )
        w._destination_descendant_apply_queue = deque()
        st = _minimal_graph_state()
        w._destination_descendant_apply_state = st

        def _tick():
            MainWindow._destination_descendant_apply_drain_note_graph_tick_end(
                w, "graph_nested_noop", bump_liveness=False
            )

        w._run_destination_descendant_apply_tick = _tick
        w._destination_flush_pending_leaf_batches_for_snapshot_capture = lambda: None
        force_calls = {"n": 0}
        w._destination_descendant_apply_force_clear_on_stall = lambda **kwargs: force_calls.__setitem__("n", force_calls["n"] + 1)
        payloads: list[dict] = []

        def _cap(msg: str, **data):
            if msg == "destination_snapshot_capture_descendant_apply_drain":
                payloads.append(dict(data))

        with patch.dict("os.environ", {"OZLINK_SNAPSHOT_DRAIN_NO_PROGRESS_TICKS": "8"}, clear=False):
            with patch("ozlink_console.main_window.log_info", side_effect=_cap):
                MainWindow._destination_finalize_inflight_descendant_apply_for_snapshot_capture(w)
        self.assertTrue(payloads)
        self.assertNotEqual(payloads[-1].get("exit_reason"), "no_progress_stall")
        self.assertEqual(force_calls["n"], 0)

    def test_drain_still_force_clears_true_no_progress_without_benign_graph_signals(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = False
        w._destination_snapshot_capture_drain_depth = 0
        w._destination_descendant_apply_paused_for_finalize_alloc = False
        w._destination_forensic_destination_model_counts = MainWindow._destination_forensic_destination_model_counts.__get__(
            w, MainWindow
        )
        w._snapshot_capture_shutdown_drain_deadline_s = MainWindow._snapshot_capture_shutdown_drain_deadline_s.__get__(
            w, MainWindow
        )
        w._destination_descendant_apply_queue = deque()
        w._destination_descendant_apply_state = {
            "graph_walk": True,
            "walk_phase": "walk",
            "added_count": 0,
            "desc_index": 0,
            "pending_leaf_batches": {},
        }
        w._run_destination_descendant_apply_tick = lambda: None
        w._destination_flush_pending_leaf_batches_for_snapshot_capture = lambda: None
        force_calls = {"n": 0}

        def _force(**kwargs):
            force_calls["n"] += 1

        w._destination_descendant_apply_force_clear_on_stall = _force
        payloads: list[dict] = []

        def _cap(msg: str, **data):
            if msg == "destination_snapshot_capture_descendant_apply_drain":
                payloads.append(dict(data))

        with patch.dict("os.environ", {"OZLINK_SNAPSHOT_DRAIN_NO_PROGRESS_TICKS": "8"}, clear=False):
            with patch("ozlink_console.main_window.log_info", side_effect=_cap):
                MainWindow._destination_finalize_inflight_descendant_apply_for_snapshot_capture(w)
        self.assertTrue(payloads)
        self.assertEqual(payloads[-1].get("exit_reason"), "no_progress_stall")
        self.assertEqual(force_calls["n"], 1)

    def test_fingerprint_reflects_liveness_and_last_slice_kind(self):
        w = MainWindow.__new__(MainWindow)
        w._destination_snapshot_capture_drain_active = True
        w._destination_descendant_apply_queue = deque()
        st = _minimal_graph_state()
        w._destination_descendant_apply_state = st
        fp0 = MainWindow._destination_snapshot_capture_drain_progress_fingerprint(w)
        MainWindow._destination_descendant_apply_drain_note_graph_tick_end(w, "graph_yield_waiting")
        fp1 = MainWindow._destination_snapshot_capture_drain_progress_fingerprint(w)
        self.assertEqual(fp0[9], "")
        self.assertEqual(fp0[10], 0)
        self.assertEqual(fp1[9], "graph_yield_waiting")
        self.assertEqual(fp1[10], 1)
        self.assertNotEqual(fp0, fp1)
