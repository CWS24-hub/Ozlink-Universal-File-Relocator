"""Helpers and constants for destination tree contract (read invariant / reconcile safety)."""

from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication

from ozlink_console.destination_tree_contract import (
    DESTINATION_REFRESH_PHASE_LOAD,
    DESTINATION_REFRESH_PHASE_READ,
    DESTINATION_STRUCTURAL_LOADING_STATUS_MESSAGE,
    EMPTY_LIBRARY_MESSAGE,
    destination_overlay_only_visible_structure_contract,
    destination_structure_child_ref_resolved,
    new_destination_refresh_correlation_id,
)
from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _qapp():
    return QApplication.instance() or QApplication([])


class DestinationTreeContractHelpersTests(unittest.TestCase):
    def test_empty_library_message_wording(self):
        self.assertIn("empty", EMPTY_LIBRARY_MESSAGE.casefold())
        self.assertIn("propose", EMPTY_LIBRARY_MESSAGE.casefold())

    def test_new_destination_refresh_correlation_id_unique(self):
        a = new_destination_refresh_correlation_id()
        b = new_destination_refresh_correlation_id()
        self.assertNotEqual(a, b)
        self.assertEqual(len(a), 36)

    def test_destination_overlay_only_visible_structure_contract_helper(self):
        self.assertFalse(
            destination_overlay_only_visible_structure_contract(lazy_mode=False, uses_planning_model_view=True)
        )
        self.assertFalse(
            destination_overlay_only_visible_structure_contract(lazy_mode=True, uses_planning_model_view=False)
        )
        self.assertTrue(
            destination_overlay_only_visible_structure_contract(lazy_mode=True, uses_planning_model_view=True)
        )

    def test_destination_refresh_phase_read_constant(self):
        self.assertEqual(DESTINATION_REFRESH_PHASE_READ, "read")

    def test_structural_loading_status_message(self):
        self.assertIn("loading", DESTINATION_STRUCTURAL_LOADING_STATUS_MESSAGE.casefold())

    def test_structural_commit_snapshot_top_level_empty(self):
        mw = MainWindow.__new__(MainWindow)
        mw._set_tree_status_message = MagicMock()
        mw._destination_refresh_pipeline_log = MagicMock()
        model = DestinationPlanningTreeModel(destination_index_key_fn=lambda pl: str(pl.get("item_path") or ""))
        tree = MagicMock()
        MainWindow._destination_structural_commit_snapshot_top_level(
            mw, model, tree, [], reason="unit_empty", panel_key="destination"
        )
        self.assertEqual(model.rowCount(QModelIndex()), 1)
        pl = model.index(0, 0, QModelIndex()).data(Qt.UserRole) or {}
        self.assertTrue(pl.get("placeholder"))
        self.assertEqual(mw._destination_refresh_pipeline_log.call_count, 2)

    def test_structural_commit_snapshot_top_level_rows(self):
        mw = MainWindow.__new__(MainWindow)
        mw._set_tree_status_message = MagicMock()
        mw._destination_refresh_pipeline_log = MagicMock()
        mw._destination_audit_top_level_rows_for_synthetic_root = lambda m: None  # type: ignore[method-assign]
        model = DestinationPlanningTreeModel(destination_index_key_fn=lambda pl: str(pl.get("item_path") or ""))
        tree = MagicMock()
        rows = [
            {
                "base_display_label": "Folder: X",
                "name": "X",
                "is_folder": True,
                "item_path": "/X",
                "tree_role": "destination",
            }
        ]
        MainWindow._destination_structural_commit_snapshot_top_level(
            mw,
            model,
            tree,
            rows,
            reason="unit_rows",
            loading_status_message="Loading snapshot…",
            use_perf_timer=False,
        )
        self.assertEqual(model.rowCount(QModelIndex()), 1)
        mw._set_tree_status_message.assert_called()
        self.assertEqual(mw._destination_refresh_pipeline_log.call_count, 2)

    def test_destination_chunked_bind_pipeline_log_fields(self):
        mw = MainWindow.__new__(MainWindow)
        st = {
            "chunk_bind_correlation_id": "chunk-corr-1",
            "chunk_bind_parent_correlation_id": "parent-corr-1",
            "gen": 7,
        }
        with patch("ozlink_console.main_window.log_info") as li:
            MainWindow._destination_chunked_bind_pipeline_log(
                mw,
                st,
                pipeline_phase="load",
                chunked_bind_phase="scheduled",
                step="unit",
                extra_kw=1,
            )
        li.assert_called_once()
        args, kwargs = li.call_args
        self.assertEqual(args[0], "destination_refresh_pipeline")
        self.assertEqual(kwargs.get("correlation_id"), "chunk-corr-1")
        self.assertEqual(kwargs.get("parent_refresh_correlation_id"), "parent-corr-1")
        self.assertEqual(kwargs.get("bind_kind"), "chunked_async")
        self.assertEqual(kwargs.get("bind_generation"), 7)
        self.assertEqual(kwargs.get("step"), "unit")

    def test_destination_enter_structural_loading_shell_delegates(self):
        mw = MainWindow.__new__(MainWindow)
        mw.set_tree_placeholder = MagicMock()
        mw._destination_refresh_pipeline_log = MagicMock()
        MainWindow._destination_enter_structural_loading_shell(mw, reason="unit_test")
        mw.set_tree_placeholder.assert_called_once_with(
            "destination", DESTINATION_STRUCTURAL_LOADING_STATUS_MESSAGE
        )
        ca = mw._destination_refresh_pipeline_log.call_args
        self.assertEqual(ca[0][0], DESTINATION_REFRESH_PHASE_LOAD)
        self.assertEqual(ca[1].get("step"), "awaiting_graph_skeleton")
        self.assertEqual(ca[1].get("reason"), "unit_test")

    def test_destination_refresh_from_graph_correlation_destination_only(self):
        mw = MainWindow.__new__(MainWindow)
        called = []

        def _stub_apply(pk, items):
            called.append((pk, items))

        mw._apply_root_payload_to_tree = _stub_apply  # type: ignore[method-assign]
        MainWindow._destination_refresh_from_graph(mw, "destination", [{"name": "A"}], reason="unit")
        self.assertIsNotNone(getattr(mw, "_destination_refresh_correlation_id", None))
        self.assertEqual(called, [("destination", [{"name": "A"}])])

        mw2 = MainWindow.__new__(MainWindow)
        mw2._apply_root_payload_to_tree = _stub_apply  # type: ignore[method-assign]
        MainWindow._destination_refresh_from_graph(mw2, "source", [], reason="unit")
        self.assertIsNone(getattr(mw2, "_destination_refresh_correlation_id", None))

    def test_orchestrate_root_tree_bind_from_graph_matches_alias(self):
        """Canonical orchestrator and _destination_refresh_from_graph apply the same steps."""
        calls: list[tuple] = []

        def _stub_apply(pk, items):
            calls.append(("apply", pk, list(items)))

        mw = MainWindow.__new__(MainWindow)
        mw._apply_root_payload_to_tree = _stub_apply  # type: ignore[method-assign]
        MainWindow._orchestrate_root_tree_bind_from_graph(mw, "destination", [{"x": 1}], reason="r1")
        MainWindow._destination_refresh_from_graph(mw, "destination", [{"x": 2}], reason="r2")
        self.assertEqual(
            calls,
            [
                ("apply", "destination", [{"x": 1}]),
                ("apply", "destination", [{"x": 2}]),
            ],
        )

    def test_destination_correlation_preserved_loading_shell_then_graph_begin(self):
        mw = MainWindow.__new__(MainWindow)
        mw.set_tree_placeholder = MagicMock()
        mw._destination_refresh_pipeline_log = MagicMock()
        MainWindow._destination_enter_structural_loading_shell(mw, reason="unit_shell")
        cid = getattr(mw, "_destination_refresh_correlation_id", None)
        self.assertIsNotNone(cid)
        MainWindow._destination_refresh_begin_graph_root(mw, item_count=2, reason="unit_read")
        self.assertEqual(getattr(mw, "_destination_refresh_correlation_id", None), cid)

    def test_destination_enter_structural_loading_shell_new_correlation_each_cycle(self):
        mw = MainWindow.__new__(MainWindow)
        mw.set_tree_placeholder = MagicMock()
        mw._destination_refresh_pipeline_log = MagicMock()
        MainWindow._destination_enter_structural_loading_shell(mw, reason="a")
        first = mw._destination_refresh_correlation_id
        MainWindow._destination_enter_structural_loading_shell(mw, reason="b")
        self.assertNotEqual(mw._destination_refresh_correlation_id, first)

    def test_destination_structural_snapshot_commit_sets_fresh_correlation(self):
        mw = MainWindow.__new__(MainWindow)
        mw._destination_refresh_correlation_id = "00000000-0000-0000-0000-000000000099"
        mw._set_tree_status_message = MagicMock()
        mw._destination_refresh_pipeline_log = MagicMock()
        mw._destination_audit_top_level_rows_for_synthetic_root = lambda _m: None  # type: ignore[method-assign]
        model = DestinationPlanningTreeModel(destination_index_key_fn=lambda pl: str(pl.get("item_path") or ""))
        tree = MagicMock()
        MainWindow._destination_structural_commit_snapshot_top_level(
            mw, model, tree, [], reason="unit_snap", panel_key="destination"
        )
        self.assertNotEqual(
            getattr(mw, "_destination_refresh_correlation_id", None),
            "00000000-0000-0000-0000-000000000099",
        )

    def test_graph_skeleton_top_level_row_count_matches_items(self):
        _qapp()
        mw = MainWindow.__new__(MainWindow)
        mw.pending_root_drive_ids = {"destination": "drive-d"}
        model = DestinationPlanningTreeModel(destination_index_key_fn=lambda pl: str(pl.get("item_path") or ""))
        graph_items = [
            {"name": "Alpha", "is_folder": True, "id": "a1", "item_path": "/Alpha", "drive_id": "drive-d"},
            {"name": "Beta.txt", "is_folder": False, "id": "b1", "item_path": "/Beta.txt", "drive_id": "drive-d"},
        ]
        sorted_items = sorted(
            graph_items, key=lambda v: (not v.get("is_folder", False), str(v.get("name") or "").lower())
        )
        payloads = [MainWindow._destination_payload_from_graph_item(mw, it) for it in sorted_items]
        model.reset_root_payloads(payloads)
        lib = QModelIndex()
        self.assertEqual(model.rowCount(lib), 2)
        for row in range(2):
            pl = model.index(row, 0, lib).data(Qt.UserRole) or {}
            self.assertFalse(pl.get("placeholder"))

    def test_destination_structure_child_ref_resolved_none(self):
        self.assertFalse(destination_structure_child_ref_resolved(None, None))

    def test_destination_structure_child_ref_resolved_invalid_index(self):
        self.assertFalse(destination_structure_child_ref_resolved(QModelIndex(), None))

    def test_destination_structure_child_ref_resolved_live_index(self):
        model = DestinationPlanningTreeModel()
        model.reset_root_payloads(
            [
                {
                    "base_display_label": "A",
                    "name": "A",
                    "is_folder": True,
                    "item_path": "/A",
                    "tree_role": "destination",
                }
            ]
        )
        ix = model.index(0, 0, QModelIndex())
        self.assertTrue(ix.isValid())
        self.assertTrue(destination_structure_child_ref_resolved(ix, model))


if __name__ == "__main__":
    unittest.main()
