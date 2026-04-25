"""Policy: cold-start planned allocation descendant replay is deferred in graph overlay mode unless user-initiated."""

from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from ozlink_console import destination_authority_contract
from ozlink_console.main_window import MainWindow


def _overlay_mw():
    mw = MainWindow.__new__(MainWindow)
    mw._destination_graph_overlay_mode_log_once = True
    return mw


class DestinationStartupReplayPolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Default policy is ON; keep explicit 1 for tests that also assert log/env wiring.
        os.environ["OZLINK_DESTINATION_GRAPH_OVERLAY_MODE"] = "1"

    @classmethod
    def tearDownClass(cls):
        os.environ.pop("OZLINK_DESTINATION_GRAPH_OVERLAY_MODE", None)

    def test_memory_overlay_on_by_default_unless_env_overrides(self):
        mw = MainWindow.__new__(MainWindow)
        mw._destination_graph_overlay_mode_log_once = True
        try:
            os.environ.pop("OZLINK_DESTINATION_GRAPH_OVERLAY_MODE", None)
        except Exception:
            pass
        self.assertTrue(bool(MainWindow._destination_memory_overlay_mode_enabled(mw)))
        os.environ["OZLINK_DESTINATION_GRAPH_OVERLAY_MODE"] = "0"
        self.assertFalse(bool(MainWindow._destination_memory_overlay_mode_enabled(mw)))
        os.environ.pop("OZLINK_DESTINATION_GRAPH_OVERLAY_MODE", None)

    def test_cold_replay_blocks_without_user_initiated(self):
        mw = _overlay_mw()
        with patch.object(destination_authority_contract, "graph_owns_visible_real_destination_structure", return_value=True):
            self.assertTrue(
                MainWindow._destination_should_block_cold_planned_descendant_replay(
                    mw,
                    enqueue_reason="eager_bind_allocation_descendants",
                    collect_reason="eager_bind_allocation_descendants",
                    user_initiated=False,
                )
            )
            self.assertFalse(
                MainWindow._destination_should_block_cold_planned_descendant_replay(
                    mw,
                    enqueue_reason="eager_bind_allocation_descendants",
                    collect_reason="eager_bind_allocation_descendants",
                    user_initiated=True,
                )
            )

    def test_cold_bypass_does_not_block_user_initiated(self):
        mw = _overlay_mw()
        with patch.object(destination_authority_contract, "graph_owns_visible_real_destination_structure", return_value=True):
            self.assertFalse(
                MainWindow._destination_should_block_cold_planned_descendant_replay(
                    mw,
                    enqueue_reason="load_projected_descendants_startup_visibility_bypass",
                    collect_reason="load_projected_descendants_startup_visibility_bypass",
                    user_initiated=True,
                )
            )

    def test_snapshot_inconsistent_applied_count(self):
        roots = [
            {
                "data": {
                    "is_folder": True,
                    "allocation_descendants_applied": True,
                    "placeholder": False,
                },
                "children": [],
            }
        ]
        mw = MainWindow.__new__(MainWindow)
        n = MainWindow._destination_snapshot_has_applied_planned_allocations_with_empty_subtrees(
            mw, roots, log_event=False
        )
        self.assertEqual(n, 1)

    def test_rich_baseline_thin_persist_risk(self):
        mw = MainWindow.__new__(MainWindow)
        with patch.object(MainWindow, "_destination_rich_reference_snapshot_node_count_baseline", return_value=300):
            with patch.object(
                MainWindow, "_destination_memory_overlay_mode_enabled", return_value=True
            ):
                self.assertTrue(
                    bool(MainWindow._destination_thin_planned_memory_over_rich_reference(mw, 95))
                )
                self.assertFalse(
                    bool(MainWindow._destination_thin_planned_memory_over_rich_reference(mw, 300))
                )

    def test_startup_replay_summary_resets_phase(self):
        mw = MainWindow.__new__(MainWindow)
        mw._destination_startup_phase_active = True
        mw._destination_startup_replay_enqueued_count = 0
        mw._destination_startup_replay_blocked_count = 1
        MainWindow._destination_on_startup_replay_guard_idle_ready(mw, reason="test")
        self.assertFalse(bool(getattr(mw, "_destination_startup_phase_active", True)))

    def test_classify_thin_empty_applied(self):
        mw = MainWindow.__new__(MainWindow)
        m = {
            "total_nodes": 200,
            "planned_nodes": 1,
            "allocation_nodes": 0,
            "allocation_descendant_stamped": 0,
            "applied_but_empty_count": 1,
        }
        t, r = MainWindow._destination_classify_thin_active_snapshot(mw, m, ref_baseline=400)
        self.assertTrue(t)
        self.assertEqual(r, "applied_empty")


if __name__ == "__main__":
    unittest.main()
