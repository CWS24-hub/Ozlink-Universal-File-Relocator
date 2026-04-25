"""
Stress harness: measured destination_materialize_profile phases (synthetic tree).

This is not a substitute for a real SharePoint slow session, but it produces *actual* timings/counters
from this codebase for one heavy shape (many expanded allocation folders + hydrate).
"""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication, QTreeView

from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _app():
    return QApplication.instance() or QApplication([])


def _stress_mw(monkeypatch, *, n: int = 120):
    monkeypatch.setenv("OZLINK_DEST_MATERIALIZE_PROFILE", "1")
    monkeypatch.setattr(
        "ozlink_console.destination_authority_contract.graph_owns_visible_real_destination_structure",
        lambda _h: True,
    )
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw._memory_restore_in_progress = False
    mw._suppress_selector_change_handlers = False
    mw.isVisible = lambda: False
    mw._log_restore_phase = lambda *a, **k: None
    dm = DestinationPlanningTreeModel(
        destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow)
    )
    tw = QTreeView()
    tw.setModel(dm)
    mw.destination_planning_model = dm
    mw.destination_tree_widget = tw
    for name in (
        "normalize_memory_path",
        "_canonical_destination_projection_path",
        "_canonical_planned_memory_path_for_graph_match",
        "_path_segments",
        "_tree_item_path",
        "_destination_parent_match_details",
        "_destination_row_raw_path_for_path_lookup_match",
        "_destination_semantic_path",
        "_destination_payload_index_key",
        "_destination_model_index_user_role_dict",
        "_refresh_destination_item_visibility_index",
        "_tree_name_column_label",
        "_apply_tree_item_visual_state",
        "_find_destination_child_by_path",
        "_destination_path_effective_expanded_for_hydrate",
    ):
        setattr(mw, name, getattr(MainWindow, name).__get__(mw, MainWindow))
    mw._find_visible_destination_item_by_path = MainWindow._find_visible_destination_item_by_path.__get__(mw, MainWindow)
    mw._find_visible_destination_item_by_path_impl = MainWindow._find_visible_destination_item_by_path_impl.__get__(
        mw, MainWindow
    )
    mw._materialize_cached_destination_lookup_norm = MainWindow._materialize_cached_destination_lookup_norm.__get__(
        mw, MainWindow
    )
    mw._destination_materialize_profile_start_cycle = MainWindow._destination_materialize_profile_start_cycle.__get__(
        mw, MainWindow
    )
    mw._destination_materialize_profile_finish_cycle = MainWindow._destination_materialize_profile_finish_cycle.__get__(
        mw, MainWindow
    )
    mw._destination_materialize_profile_span = MainWindow._destination_materialize_profile_span.__get__(mw, MainWindow)
    mw._destination_materialize_profile_record = MainWindow._destination_materialize_profile_record.__get__(mw, MainWindow)
    mw._destination_materialize_profile_snapshot_ranked = MainWindow._destination_materialize_profile_snapshot_ranked.__get__(
        mw, MainWindow
    )
    mw._destination_overlay_materialize_pass_begin = MainWindow._destination_overlay_materialize_pass_begin.__get__(
        mw, MainWindow
    )
    mw._destination_overlay_materialize_pass_end = MainWindow._destination_overlay_materialize_pass_end.__get__(
        mw, MainWindow
    )

    hub = {
        "name": "Hub",
        "id": "live-hub",
        "is_folder": True,
        "item_path": "Hub",
        "destination_path": "Hub",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, hub)
    dm.reset_root_payloads([hub])
    hub_ix = dm.index(0, 0, QModelIndex())
    children = []
    for i in range(n):
        children.append(
            {
                "name": f"A{i}",
                "is_folder": True,
                "item_path": f"Hub\\A{i}",
                "destination_path": f"Hub\\A{i}",
                "tree_role": "destination",
                "children_loaded": False,
                "drive_id": "d1",
                "library_id": "d1",
                "planned_allocation": True,
                "node_origin": "PlannedAllocation",
                "id": "",
                "graph_item_id": "",
            }
        )
    MainWindow._apply_tree_item_visual_state(mw, None, children[0])
    dm.append_child_payloads(hub_ix, children)

    monkeypatch.setattr(mw, "_load_destination_projected_descendants_index", lambda ix, **_: None)
    monkeypatch.setattr(mw, "_destination_path_effective_expanded_for_hydrate", lambda tree, ix, p: True)
    monkeypatch.setattr(mw, "node_is_planned_allocation", lambda nd: bool(nd.get("planned_allocation")))

    return mw


def test_measured_top_phases_hydrate_heavy(monkeypatch):
    """Synthetic: duplicate expanded paths → hydrate dedupe + profile shows dominant phases."""
    n = 80
    mw = _stress_mw(monkeypatch, n=n)
    # Triple each path → without dedupe would thrash the same row 3×
    expanded = []
    for i in range(n):
        p = f"Hub\\A{i}"
        expanded.extend([p, p, p])

    mw._destination_materialize_profile_start_cycle()
    with mw._destination_materialize_profile_span("hydrate_destination_allocations_for_expanded_paths"):
        MainWindow._hydrate_destination_allocations_for_expanded_paths_model(mw, expanded)
    snap = mw._destination_materialize_profile_snapshot_ranked()
    mw._destination_materialize_profile_finish_cycle("stress_hydrate_only")
    assert snap, "expected profile rows"
    top = snap[0]
    assert top["phase"] == "hydrate_destination_allocations_for_expanded_paths"
    assert top["call_count"] == 1
    # find_visible aggregated inside hydrate span
    names = {r["phase"] for r in snap}
    assert "find_visible_destination_item_by_path" in names


def test_hydrate_dedupes_identical_canonical_paths(monkeypatch):
    n = 20
    mw = _stress_mw(monkeypatch, n=n)
    calls: list[int] = []

    def _counting_find(path):
        calls.append(1)
        return MainWindow._find_visible_destination_item_by_path_impl(mw, path)

    mw._find_visible_destination_item_by_path_impl = _counting_find
    mw._destination_overlay_materialize_pass_begin()
    expanded = []
    for i in range(n):
        p = f"Hub\\A{i}"
        expanded.extend([p, p, p])
    MainWindow._hydrate_destination_allocations_for_expanded_paths_model(mw, expanded)
    mw._destination_overlay_materialize_pass_end()
    assert len(calls) == n
