"""Focused tests for Graph destination startup phase, expand routing, replay gate, and persistence hooks."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtGui import QStandardItem, QStandardItemModel

from ozlink_console import main_window
from ozlink_console.main_window import MainWindow


class _GraphSessionHost:
    def __init__(self) -> None:
        self._application_shutting_down = False
        self._destination_startup_phase_active = True
        self._destination_expand_shell_ready = False
        self._destination_expand_invocation = ""
        self._destination_graph_startup_phase_completion_key = ""
        self._destination_post_shell_memory_rehydrate_done_key = "did1|sid"
        self._destination_snapshot_overlay_classification_startup_complete = True
        self._destination_startup_replay_enqueued_count = 0
        self._destination_startup_replay_blocked_count = 0
        self._destination_expand_user_deferred_queue = []
        self.active_draft_session_id = "sid"
        self.destination_planning_model = QStandardItemModel()
        # one root row
        self.destination_planning_model.appendRow(QStandardItem("r"))

    def _destination_post_shell_library_identity(self) -> str:
        return "did1|sid"

    def _current_selected_destination_drive_id(self) -> str:
        return "did1"


class GraphStartupLifecycleTests(unittest.TestCase):
    def test_graph_startup_marks_complete_after_post_shell_tracks(self) -> None:
        h = _GraphSessionHost()
        with patch.object(main_window.destination_authority_contract, "graph_owns_visible_real_destination_structure", return_value=True):
            MainWindow._destination_mark_graph_startup_phase_complete(
                h, reason="test", drive_id="did1", pending_deferred_count=0
            )
        self.assertFalse(h._destination_startup_phase_active)
        self.assertTrue(h._destination_expand_shell_ready)
        self.assertEqual(h._destination_graph_startup_phase_completion_key, "did1|sid")

    def test_source_collect_permitted_in_startup(self) -> None:
        mw = MagicMock()
        mw._destination_startup_phase_active = True
        ok, r = MainWindow._destination_source_descendant_collect_permitted(
            mw, expand_auth_class="real_user_expand", explicit_repair_authorized=False
        )
        self.assertTrue(ok)
        self.assertIn("startup", r)

    def test_source_collect_blocked_post_startup_without_explicit(self) -> None:
        mw = MagicMock()
        mw._destination_startup_phase_active = False
        ok, r = MainWindow._destination_source_descendant_collect_permitted(
            mw, expand_auth_class="real_user_expand", explicit_repair_authorized=False
        )
        self.assertFalse(ok)

    def test_replay_enqueue_blocked_user_initiated_non_explicit(self) -> None:
        """Hard gate: user-initiated replay must not queue without explicit repair when auth is not explicit_user_repair."""
        mw = MagicMock()
        mw._destination_memory_overlay_mode_enabled = MagicMock(return_value=False)
        ix = QModelIndex()
        with patch.object(MainWindow, "_destination_infer_expand_auth_class", return_value="real_user_expand"):
            with patch.object(
                MainWindow, "_destination_descendant_snapshot_reuse_assess",
                return_value={"outcome": "rejected", "expected_descendant_count": 1, "overlay_descendant_count": 0},
            ):
                with patch.object(
                    MainWindow, "_log_destination_descendant_replay_reuse_gate_result", MagicMock()
                ):
                    with patch.object(
                        main_window.destination_authority_contract, "graph_owns_visible_real_destination_structure", return_value=True
                    ):
                        with patch.object(MainWindow, "_find_destination_allocation_descendant_parent_index", return_value=(ix, "")):
                            r = MainWindow._enqueue_destination_descendant_apply_to_model(
                                mw,
                                ix,
                                {"source_path": "a", "source": {"is_folder": True}, "destination_path": "b"},
                                enqueue_reason="test",
                                user_initiated=True,
                                expand_auth_class="real_user_expand",
                            )
        self.assertFalse(r)

    def test_draft_defer_flag(self) -> None:
        mw = MagicMock()
        mw._destination_startup_phase_active = True
        with patch.object(main_window.destination_authority_contract, "graph_owns_visible_real_destination_structure", return_value=True):
            d = MainWindow._destination_shell_persist_heavy_deferred(mw)
        self.assertTrue(d)


if __name__ == "__main__":
    unittest.main()
