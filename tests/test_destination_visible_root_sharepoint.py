"""SharePoint: semantic ``Root`` must never appear as a visible destination row."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from ozlink_console.destination_path_bridge import is_internal_planning_root_semantic_path
from ozlink_console.destination_semantic_index import compute_incremental_merge_entry_roots
from ozlink_console.main_window import MainWindow


def _qapp():
    return QApplication.instance() or QApplication([])


def test_incremental_merge_entry_roots_drop_semantic_root_under_graph_authority():
    """If semantic Root is an entry root, production filters it before append_nested_child."""
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    model_nodes = {
        "Root": {
            "parent_semantic_path": "",
            "node_state": "projected",
            "data": {"name": "Root"},
            "children": ["Root\\Finance"],
        },
        "Root\\Finance": {
            "parent_semantic_path": "Root",
            "node_state": "proposed",
            "data": {"name": "Finance"},
            "children": [],
        },
    }
    new_paths = {"Root", "Root\\Finance"}
    entry = compute_incremental_merge_entry_roots(model_nodes, new_paths)
    assert "Root" in entry
    filtered = [
        er
        for er in entry
        if not is_internal_planning_root_semantic_path(mw.normalize_memory_path(str(er)))
    ]
    assert "Root" not in filtered
