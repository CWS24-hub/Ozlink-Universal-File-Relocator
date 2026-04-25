"""Branch-level rich-vs-thin snapshot preservation, startup gating, and rehydrate source selection."""

from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

from ozlink_console.main_window import MainWindow


def _node(path: str, n_children: int, *, alloc_applied: bool = True, place: str = "planned", lib: str = "libx") -> dict:
    ch = [
        {
            "data": {
                "is_folder": True,
                "placeholder": False,
                "row_kind": place,
                "item_path": f"{path}\\c{i}",
                "display_path": f"{path}\\c{i}",
                "ozlink_dest_library_id": lib,
            },
            "children": [],
        }
        for i in range(n_children)
    ]
    return {
        "data": {
            "is_folder": True,
            "placeholder": False,
            "row_kind": place,
            "item_path": path,
            "display_path": path,
            "ozlink_dest_library_id": lib,
            "allocation_descendants_applied": bool(alloc_applied) and n_children == 0,
        },
        "children": ch,
    }


class BranchMemoryPreservationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["OZLINK_DESTINATION_GRAPH_OVERLAY_MODE"] = "1"

    def _stub_main_window(self):
        mw = MainWindow.__new__(MainWindow)
        mw._destination_invariant_repair_known_thin_branch_paths = set()
        mw._destination_planned_user_opened_paths_cf = set()
        return mw

    @patch("ozlink_console.main_window.log_info", lambda *a, **k: None)
    def test_merge_replaces_thinner_planned_with_richer_existing_session(self):
        mw = self._stub_main_window()
        current = _node(r"R\Folder", 0, alloc_applied=True)
        ex = _node(r"R\Folder", 2, alloc_applied=False)
        nodes = [current]
        cands: list = [("existing_session", [ex]), ("runtime_session", []), ("prior_workspace", [])]

        with (
            patch.object(
                MainWindow, "_destination_selected_destination_library_id_key", return_value="libx"  # type: ignore[misc]
            ),
            patch.object(MainWindow, "_destination_extract_branch_snapshot_path", side_effect=lambda d: d.get("item_path", "")),  # type: ignore[misc]  # noqa: E501
            patch.object(
                MainWindow, "_destination_snapshot_data_planned_allocationish", return_value=True  # type: ignore[misc]  # noqa: E501
            ),
        ):
            MainWindow._destination_merge_preserved_richer_planned_subtrees_in_place(  # type: ignore[misc]  # noqa: E501
                mw, nodes, cands, lib_key="libx", phase="test"
            )
        d = (nodes[0] or {}).get("data")
        self.assertIsInstance(d, dict)
        self.assertGreaterEqual(len((nodes[0] or {}).get("children") or []), 2, "richer session branch should replace")

    @patch("ozlink_console.main_window.log_info", lambda *a, **k: None)
    def test_startup_gating_blocks_invisible_path(self):
        mw = self._stub_main_window()
        mw._destination_startup_phase_active = True  # type: ignore[attr-defined]
        with (
            patch.object(
                MainWindow, "_collect_selected_tree_path", return_value="R\\A"  # type: ignore[misc]
            ),
            patch.object(MainWindow, "_materialize_cached_destination_lookup_norm", return_value="R\\A",),  # type: ignore[misc]  # noqa: E501
            patch.object(MainWindow, "_destination_expanded_paths_for_planning_bind", return_value=set(),),  # type: ignore[misc]  # noqa: E501
        ):
            self.assertFalse(
                MainWindow._destination_should_run_branch_local_startup_work(  # type: ignore[misc]  # noqa: E501
                    mw,
                    canonical_path=r"r\deep\branch\only",
                    work_kind="t",
                    explicit_repair=False,
                )
            )

    @patch("ozlink_console.main_window.log_info", lambda *a, **k: None)
    def test_startup_allows_outside_startup_phase(self):
        mw = self._stub_main_window()
        mw._destination_startup_phase_active = False  # type: ignore[attr-defined]
        self.assertTrue(
            MainWindow._destination_should_run_branch_local_startup_work(  # type: ignore[misc]  # noqa: E501
                mw,
                canonical_path=r"r\any\path",
                work_kind="t",
                explicit_repair=False,
            )
        )

    def test_map_rehydrate_source(self):
        mw = MainWindow.__new__(MainWindow)
        self.assertEqual(
            MainWindow._destination_map_rehydrate_log_source(mw, "active_workspace"),  # type: ignore[misc]  # noqa: E501
            "active_snapshot",
        )
        self.assertEqual(
            MainWindow._destination_map_rehydrate_log_source(mw, "backup/foo/session",),  # type: ignore[misc]  # noqa: E501
            "history_candidate",
        )


@patch("ozlink_console.main_window.log_info", lambda *a, **k: None)
def test_invariant_repair_gated_during_startup_without_visible_path():
    mw = MainWindow.__new__(MainWindow)
    dm = MagicMock()
    ix = MagicMock()
    ix.isValid = MagicMock(return_value=True)
    ix.column = MagicMock(return_value=0)
    ix.siblingAtColumn = MagicMock(return_value=ix)
    ix.data = MagicMock(
        return_value={"is_folder": True, "item_path": "R\\Deep", "row_kind": "plannedAllocation"}
    )
    with (
        patch.object(MainWindow, "destination_planning_model", create=True, return_value=dm),  # type: ignore[misc]  # noqa: E501
        patch.object(MainWindow, "destination_tree_widget", create=True, return_value=MagicMock()),  # type: ignore[misc]  # noqa: E501
        patch.object(MainWindow, "_destination_startup_phase_active", True, create=True),  # type: ignore[misc]  # noqa: E501
        patch.object(
            MainWindow, "_destination_should_run_branch_local_startup_work",  # type: ignore[misc]
            return_value=False,
        ),
    ):
        MainWindow._apply_overlay_projection_invariant_repair_to_index(  # type: ignore[misc]  # noqa: E501
            mw, ix, {"source": {"is_folder": True}}
        )


def test_rehydrate_prefers_richer_history_substantive_children():
    """When active snapshot is empty for the branch, bounded history can supply richer node."""
    with patch("ozlink_console.main_window.log_info", lambda *a, **k: None):
        mw = MainWindow.__new__(MainWindow)
        hnode = _node("R\\X", 3, alloc_applied=False)
        with (
            patch.object(
                MainWindow, "_destination_eager_destination_tree_snapshot_roots",  # type: ignore[misc]
                return_value=([_node("R\\X", 0, alloc_applied=True)], "test_src"),
            ),
            patch.object(mw, "memory_manager", None, create=True),  # type: ignore[misc]  # noqa: E501
            patch.object(
                MainWindow,  # type: ignore[misc]
                "_destination_bounded_history_snapshot_source_lists",
                return_value=[("quarantine/1.json", [hnode])],
            ),
        ):
            best, lbl, dc, *rest = MainWindow._destination_select_richer_snapshot_node_for_rehydrate(  # type: ignore[misc]  # noqa: E501
                mw, "R\\X", min_desc=0
            )
    assert best is not None
    assert "quarantine" in (lbl or "").lower() or dc > 0


if __name__ == "__main__":
    unittest.main()
