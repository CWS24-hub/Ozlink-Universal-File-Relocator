"""post-shell rehydrate hook, startup eligibility, and shutdown subtree preservation."""

from __future__ import annotations

import os
import unittest
import unittest.mock
from unittest.mock import patch

from ozlink_console import destination_authority_contract
from ozlink_console.main_window import MainWindow

from tests.test_destination_branch_memory_preservation import _node


class PostShellRehydrateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["OZLINK_DESTINATION_GRAPH_OVERLAY_MODE"] = "1"

    @patch("ozlink_console.main_window.log_info", lambda *a, **k: None)
    def test_overlay_graph_session_schedules_post_shell_rehydrate(self):
        """A: with Graph authority, schedule runs once; completion path is not idle_replay-only."""
        m = MainWindow.__new__(MainWindow)
        m._application_shutting_down = False  # type: ignore[attr-defined]
        m._destination_post_shell_memory_rehydrate_done_key = ""  # type: ignore[attr-defined]
        m._destination_post_shell_rich_rehydrate_scan_ran = False  # type: ignore[attr-defined]
        m._destination_post_shell_memory_rehydrate_scheduled = False  # type: ignore[attr-defined]
        m._destination_snapshot_overlay_classification_startup_complete = True  # type: ignore[attr-defined]
        m.active_draft_session_id = "s1"  # type: ignore[attr-defined]
        m._current_selected_destination_drive_id = lambda: "d1"  # type: ignore[assignment, misc]
        dm = unittest.mock.MagicMock()
        dm.rowCount = unittest.mock.MagicMock(return_value=1)
        m.destination_planning_model = dm  # type: ignore[attr-defined]

        def _instant_invoke(_n: str, f):
            f()

        with (
            patch.object(
                destination_authority_contract, "graph_owns_visible_real_destination_structure", return_value=True
            ),
            patch("ozlink_console.main_window.QTimer") as pqt,
            patch.object(m, "_safe_invoke", new=_instant_invoke),  # type: ignore[assignment, misc]  # noqa: E501
            patch.object(
                MainWindow, "_destination_run_post_shell_memory_rehydrate_once", autospec=True
            ) as p_run,
        ):
            pqt.singleShot = lambda _ms, fn: fn()
            MainWindow._destination_schedule_post_shell_memory_rehydrate_if_ready(  # type: ignore[misc]  # noqa: E501
                m, reason="test_overlay"
            )
        p_run.assert_called_once()

    @patch("ozlink_console.main_window.log_info", lambda *a, **k: None)
    def test_pictures_class_eligible_without_user_opened_in_startup_shell(self):
        """B: rich memory, thin visible, in startup visible shell path set -> eligible."""
        m = MainWindow.__new__(MainWindow)
        m._destination_planned_user_opened_paths_cf = set()  # type: ignore[attr-defined]
        m._destination_visible_startup_shell_paths_cf = {r"r\pictures".casefold()}  # type: ignore[attr-defined]
        with (
            patch.object(MainWindow, "_collect_selected_tree_path", return_value=None),
            patch.object(MainWindow, "_materialize_cached_destination_lookup_norm", return_value=None),
            patch.object(
                MainWindow, "_destination_path_in_expanded_or_selected_startup_shell", return_value=False
            ),
        ):
            el = MainWindow._destination_memory_rehydrate_eligibility_checked(  # type: ignore[misc]  # noqa: E501
                m,
                canonical_path=r"R\Pictures",
                stored_descendants=120,
                visible_descendant_count=0,
                shell_paths_cf={r"r\pictures".casefold()},
                model_underrepresents_memory=True,
            )
        self.assertTrue(el.get("in_startup_visible_shell_context"))
        self.assertTrue(el.get("eligible"))

    @patch("ozlink_console.main_window.log_info", lambda *a, **k: None)
    def test_contractor_and_pictures_same_rehydrate_entrypoint(self):
        """C: entrypoint is the shared rehydrate runner (not idle-only)."""
        m = MainWindow.__new__(MainWindow)
        m._application_shutting_down = False  # type: ignore[attr-defined]
        with (
            patch.object(MainWindow, "_destination_post_shell_library_identity", return_value="a|b"),
            patch.object(MainWindow, "_destination_run_post_shell_memory_rehydrate_once") as p_run,
        ):
            m._destination_post_shell_memory_rehydrate_done_key = "a|b"  # type: ignore[attr-defined]
            m._destination_post_shell_rich_rehydrate_scan_ran = True  # type: ignore[attr-defined]
            MainWindow._destination_post_shell_rich_memory_rehydrate_scan(m)  # type: ignore[misc]  # noqa: E501
            p_run.assert_not_called()
            m._destination_post_shell_memory_rehydrate_done_key = ""  # type: ignore[attr-defined]
            m._destination_post_shell_rich_rehydrate_scan_ran = False  # type: ignore[attr-defined]
            MainWindow._destination_post_shell_rich_memory_rehydrate_scan(m)  # type: ignore[misc]  # noqa: E501
        self.assertEqual(p_run.call_count, 1)

    @patch("ozlink_console.main_window.log_info", lambda *a, **k: None)
    def test_shutdown_uses_index_and_replaces_subtree(self):
        """D/E: shutdown phase builds path index; thin root replaced without deep child storm."""
        mw = MainWindow.__new__(MainWindow)
        current = _node(r"R\Folder", 0, alloc_applied=True)
        ex = _node(r"R\Folder", 2, alloc_applied=False)
        nodes = [current]
        cands: list = [("existing_session", [ex]), ("runtime_session", []), ("prior_workspace", [])]
        with (
            patch.object(
                MainWindow, "_destination_selected_destination_library_id_key", return_value="libx"  # type: ignore[misc]  # noqa: E501
            ),
            patch.object(
                MainWindow, "_destination_extract_branch_snapshot_path", side_effect=lambda d: d.get("item_path", "")  # type: ignore[misc]  # noqa: E501
            ),
            patch.object(
                MainWindow, "_destination_snapshot_data_planned_allocationish", return_value=True
            ),
        ):
            MainWindow._destination_merge_preserved_richer_planned_subtrees_in_place(  # type: ignore[misc]  # noqa: E501
                mw, nodes, cands, lib_key="libx", phase="shutdown_prepare"
            )
        d = (nodes[0] or {}).get("data")
        self.assertIsInstance(d, dict)
        self.assertGreaterEqual(len((nodes[0] or {}).get("children") or []), 2)
        self.assertTrue(getattr(mw, "_destination_shutdown_preservation_index_built", False))
        self.assertIsInstance(getattr(mw, "_destination_shutdown_richest_planned_path_index", None), dict)


@patch("ozlink_console.main_window.log_info", lambda *a, **k: None)
def test_post_shell_memory_scan_body_is_bounded_without_model():
    m = MainWindow.__new__(MainWindow)
    m._application_shutting_down = False  # type: ignore[attr-defined]
    with (
        patch.object(
            MainWindow, "_destination_eager_destination_tree_snapshot_roots",
            return_value=([_node(r"R\X", 4, alloc_applied=True)], "src"),  # type: ignore[misc]  # noqa: E501
        ),
    ):
        rh, sk = MainWindow._destination_post_shell_memory_rehydrate_scan_body(m, set())  # type: ignore[misc]  # noqa: E501
    assert (rh, sk) == (0, 0)


if __name__ == "__main__":
    unittest.main()
