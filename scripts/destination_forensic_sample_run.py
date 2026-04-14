"""Run minimal SharePoint bind + reconcile with forensic env flags; prints JSON log lines to stdout."""

from __future__ import annotations

import json
import logging
import os
import sys

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
os.environ["OZLINK_DEST_BIND_TRACE"] = "1"
os.environ["OZLINK_DEST_RECONCILE_TRACE"] = "1"
os.environ["OZLINK_DEST_EMPTY_TRACE"] = "1"
os.environ["OZLINK_DEST_PLACEMENT_AUDIT"] = "1"

from PySide6.QtCore import QModelIndex, Qt  # noqa: E402
from PySide6.QtWidgets import QApplication  # noqa: E402

from ozlink_console.logger import JsonLineFormatter, get_logger  # noqa: E402
from ozlink_console.main_window import MainWindow  # noqa: E402
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel  # noqa: E402


def main() -> None:
    log = get_logger()
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(JsonLineFormatter())
    log.addHandler(sh)

    app = QApplication.instance() or QApplication([])
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw.destination_planning_model = DestinationPlanningTreeModel(
        destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow)
    )
    for name in (
        "normalize_memory_path",
        "_canonical_destination_projection_path",
        "_path_segments",
        "_tree_item_path",
        "_destination_parent_match_details",
        "_destination_row_raw_path_for_path_lookup_match",
        "_destination_semantic_path",
        "_destination_model_index_user_role_dict",
        "_proposed_parent_path",
        "_allocation_parent_path",
        "_allocation_projection_path",
        "_canonical_destination_path_with_visible_library_anchor",
    ):
        setattr(mw, name, getattr(MainWindow, name).__get__(mw, MainWindow))
    mw._destination_visible_library_anchor_canonical_path = lambda: ""
    mw.unresolved_proposed_by_parent_path = {}
    mw.unresolved_allocations_by_parent_path = {}
    mw.proposed_folders = []
    mw.planned_moves = [
        {
            "destination_path": "Hub\\Alpha",
            "target_name": "file.txt",
            "source": {"name": "src.txt", "is_folder": False},
        }
    ]
    mw._destination_has_future_descendants = lambda _nd: False
    mw._log_restore_phase = lambda *a, **k: None
    mw._refresh_destination_item_visibility_index = lambda *a, **k: None
    mw._tree_name_column_label = MainWindow._tree_name_column_label.__get__(mw, MainWindow)
    mw._apply_tree_item_visual_state = lambda *a, **k: MainWindow._apply_tree_item_visual_state(mw, *a, **k)

    dm = mw.destination_planning_model
    hub = {
        "name": "Hub",
        "id": "live-hub",
        "is_folder": True,
        "item_path": "Hub",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, hub)
    dm.reset_root_payloads([hub])
    hub_ix = dm.index(0, 0, QModelIndex())
    dm.replace_all_children(hub_ix, [])
    print("--- terminal_empty on Hub (empty trace) ---", file=sys.stderr)
    mw._destination_forensic_terminal_empty_context(
        hub_ix, graph_payload_child_count=0, phase="script_smoke"
    )
    print("--- strip terminal_empty when planned descendant expected ---", file=sys.stderr)
    MainWindow._destination_strip_terminal_empty_when_planned_descendants_expected(
        mw, hub_ix, folder_semantic_path="Hub"
    )

    dm.replace_all_children(hub_ix, [])
    target = "Hub\\Finance\\Payroll"
    print("--- bind planned chain ---", file=sys.stderr)
    leaf = MainWindow._sharepoint_bind_planned_segment_chain(
        mw,
        hub_ix,
        ["Finance", "Payroll"],
        bind_kind="proposed_folder",
        bind_context_excerpt="forensic_smoke",
        expected_parent_canonical="Hub",
        projection_target_canonical=target,
        terminal_is_file=False,
    )
    assert leaf is not None and leaf.isValid()

    live_child = {
        "name": "Payroll",
        "id": "live-pay",
        "is_folder": True,
        "item_path": "Hub\\Finance\\Payroll",
        "tree_role": "destination",
        "row_kind": "live_folder",
        "verification_state": "live_confirmed",
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, live_child)
    fin_ix = dm.index(0, 0, hub_ix)
    dm.replace_all_children(fin_ix, [live_child])
    snap = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "name": "Payroll",
        "item_path": "Hub\\Finance\\Payroll",
        "destination_path": "Hub\\Finance\\Payroll",
        "planning_uuid": "pu-forensic",
        "placeholder": False,
        "is_folder": True,
    }
    print("--- reconcile planned to live ---", file=sys.stderr)
    MainWindow._destination_reconcile_planned_rows_after_graph_folder_load(mw, fin_ix, [snap], allow_reappend=True)

    print("--- placement audit pass ---", file=sys.stderr)
    MainWindow._destination_audit_overlay_placement_after_pass(mw, "forensic_script")

    child_ix = dm.index(0, 0, fin_ix)
    pl = child_ix.data(Qt.UserRole) or {}
    print("--- reconcile result verification_state ---", json.dumps(pl.get("verification_state")), file=sys.stderr)
    _ = app


if __name__ == "__main__":
    main()
