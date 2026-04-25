"""Memory-backed destination allocation: rehydrate-from-memory before replay; block non-explicit child replay."""

from __future__ import annotations

import os
import time
import unittest
from unittest.mock import MagicMock, patch

from PySide6.QtCore import Qt

from ozlink_console import destination_authority_contract
from ozlink_console.main_window import MainWindow


def _alloc_move():
    return {
        "source": {"is_folder": True, "item_path": "S:\\A", "display_path": "S:\\A"},
        "source_path": "S:\\A",
        "destination_path": "D:\\B",
    }


class MemoryBackedBranchPolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["OZLINK_DESTINATION_GRAPH_OVERLAY_MODE"] = "1"

    @classmethod
    def tearDownClass(cls):
        pass

    def test_user_expand_tag_not_added_when_explicit_repair(self):
        mw = MainWindow.__new__(MainWindow)
        b = "load_projected_descendants_children_loaded_graph_auth"
        t = MainWindow._destination_tag_user_expand_enqueue_reason(
            mw, b, user_initiated=True, explicit_repair_authorized=True
        )
        self.assertEqual(t, b)

    def test_user_expand_tag_for_non_explicit_user_gesture(self):
        mw = MainWindow.__new__(MainWindow)
        t = MainWindow._destination_tag_user_expand_enqueue_reason(
            mw, "load_projected_descendants_children_loaded_graph_auth",
            user_initiated=True,
            explicit_repair_authorized=False,
        )
        self.assertIn("deferred_user_expand", t)
        self.assertIn("load_projected_descendants", t)

    def test_deny_reason_matcher_catches_bypass_substrings(self):
        mw = MainWindow.__new__(MainWindow)
        self.assertTrue(
            MainWindow._destination_enqueue_reason_hits_memory_backed_replay_deny(
                mw, "load_projected_descendants_startup_visibility_bypass"
            )
        )
        self.assertTrue(
            MainWindow._destination_enqueue_reason_hits_memory_backed_replay_deny(
                mw, "load_projected_descendants_foo_deferred_user_expand"
            )
        )

    @patch("ozlink_console.main_window.log_info", lambda *a, **k: None)
    def test_policy_proceeds_when_explicit_repair_even_if_memory_backed_deny(self):
        mw = MainWindow.__new__(MainWindow)
        mw._destination_invariant_repair_cooldown_s = 0.0
        pl = {
            "is_folder": True,
            "planned_allocation": True,
            "item_path": "D:\\P\\A",
            "display_path": "D:\\P\\A",
        }
        ix = MagicMock()
        ix.isValid = MagicMock(return_value=True)
        ix.column = MagicMock(return_value=0)
        ix.siblingAtColumn = MagicMock(return_value=ix)
        ix.data = MagicMock(return_value=pl)
        dm = MagicMock()
        dm.is_index_live = MagicMock(return_value=True)
        mw.destination_planning_model = dm
        move = _alloc_move()
        with (
            patch.object(MainWindow, "_destination_memory_overlay_mode_enabled", return_value=True),  # type: ignore[misc]  # noqa: E501
            patch.object(destination_authority_contract, "graph_owns_visible_real_destination_structure", return_value=True),  # noqa: E501
            patch.object(MainWindow, "node_is_planned_allocation", return_value=True),  # type: ignore[misc]  # noqa: E501
            patch.object(MainWindow, "_tree_item_path", return_value="D:\\P\\A"),  # type: ignore[misc]  # noqa: E501
            patch.object(
                MainWindow, "_canonical_planned_memory_path_for_graph_match", return_value="D:\\P\\A"  # type: ignore[misc]  # noqa: E501
            ),
            patch.object(
                MainWindow, "_destination_assess_memory_backed_branch", return_value={  # type: ignore[misc]  # noqa: E501
                    "candid_memory_backing": True,
                    "memory_backed_branch": True,
                    "visible_underrepresents_memory": True,
                    "snapshot_source": "workspace_snapshot",
                    "stored_children_count": 3,
                    "stored_descendant_count": 10,
                    "visible_child_count": 0,
                    "visible_descendant_count": 0,
                    "allocation_descendants_applied": True,
                }
            ),
            patch.object(
                MainWindow, "_destination_rehydrate_visible_branch_from_memory_snapshot",  # type: ignore[misc]  # noqa: E501
                return_value={"inserted_rows": 0, "outcome": "no_op"},
            ),
            patch.object(
                MainWindow, "_destination_planned_replay_deferred_marker", MagicMock()  # type: ignore[misc]  # noqa: E501
            ),
        ):
            out = MainWindow._destination_pre_descendant_replay_memory_branch_policy(
                mw, ix, move, enqueue_reason="load_projected_descendants_startup_visibility_bypass",
                user_initiated=True, idle_bounded=False, explicit_repair_authorized=True,
            )
        self.assertEqual(out, "proceed")

    @patch("ozlink_console.main_window.log_info", lambda *a, **k: None)
    def test_invariant_cooldown_skips_second_fingerprint_unchanged(self):
        mw = MainWindow.__new__(MainWindow)
        mw._destination_invariant_repair_cooldown_s = 4.0
        mw._destination_invariant_repair_path_cooldown_mono = {"p": 0.0}
        mw._destination_invariant_repair_path_last_fingerprint = {"p": "1|0|0|0"}

        with patch.object(time, "monotonic", return_value=0.0):
            self.assertTrue(
                MainWindow._destination_invariant_repair_should_skip_cooldown(mw, "p", "1|0|0|0")
            )

    def test_nested_spec_node_count(self):
        mw = MainWindow.__new__(MainWindow)
        s = ({"a": 1}, [({"b": 1}, None), ({"c": 1}, [({"d": 1}, None)])])  # type: ignore[assignment, misc]  # noqa: E501
        n = MainWindow._count_planned_snapshot_nested_spec_nodes(mw, s)
        self.assertEqual(n, 4)
