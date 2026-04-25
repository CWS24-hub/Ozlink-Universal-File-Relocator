#!/usr/bin/env python3
"""Print measured top phases from synthetic destination hydrate stress (offline).

Reproduce locally::

    set OZLINK_DEST_MATERIALIZE_PROFILE=1
    python scripts/destination_materialize_profile_sample.py

Your real SharePoint session will differ; use the same env on the app and capture
``destination_materialize_profile`` from logs for authoritative ranking.
"""

from __future__ import annotations

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
os.environ["OZLINK_DEST_MATERIALIZE_PROFILE"] = "1"

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication, QTreeView

from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def main() -> None:
    _ = QApplication.instance() or QApplication([])
    n = int(os.environ.get("OZLINK_PROFILE_STRESS_N", "200"))
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
    mw._destination_materialize_profile_snapshot_ranked = MainWindow._destination_materialize_profile_snapshot_ranked.__get__(
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
    dm.append_child_payloads(hub_ix, children)

    def _noop(ix):
        return None

    mw._load_destination_projected_descendants_index = _noop
    mw._destination_path_effective_expanded_for_hydrate = lambda tree, ix, p: True
    mw.node_is_planned_allocation = lambda nd: bool(nd.get("planned_allocation"))

    import ozlink_console.destination_authority_contract as dac

    real_graph = dac.graph_owns_visible_real_destination_structure

    def _graph_on(_h):
        return True

    try:
        dac.graph_owns_visible_real_destination_structure = _graph_on
        expanded = []
        for i in range(n):
            p = f"Hub\\A{i}"
            expanded.extend([p, p, p])

        mw._destination_materialize_profile_start_cycle()
        with mw._destination_materialize_profile_span("hydrate_destination_allocations_for_expanded_paths"):
            MainWindow._hydrate_destination_allocations_for_expanded_paths_model(mw, expanded)
        with mw._destination_materialize_profile_span("hydrate_destination_prefix_chain_for_path"):
            MainWindow._hydrate_destination_prefix_chain_for_path_model(mw, f"Hub\\A{n - 1}\\x")
        ranked = mw._destination_materialize_profile_snapshot_ranked()
        top3 = ranked[:3]
        print(json.dumps({"synthetic_stress_n": n, "top_3_measured": top3}, indent=2))
        mw._destination_materialize_profile_finish_cycle("scripts/destination_materialize_profile_sample.py")
    finally:
        dac.graph_owns_visible_real_destination_structure = real_graph


if __name__ == "__main__":
    main()
