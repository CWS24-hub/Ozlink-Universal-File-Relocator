"""Tests for SharePoint destination: Graph-owned real rows vs overlay-only future-model bind."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex

from ozlink_console.main_window import MainWindow


def _qapp():
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication([])


def test_resolve_overlay_parent_root_is_model_root_index():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.normalize_memory_path = MainWindow.normalize_memory_path.__get__(mw, MainWindow)
    ix = mw._destination_resolve_overlay_incremental_parent_item("Root")
    assert isinstance(ix, QModelIndex)
    assert not ix.isValid()


def test_resolve_overlay_parent_empty_is_model_root_index():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.normalize_memory_path = MainWindow.normalize_memory_path.__get__(mw, MainWindow)
    ix = mw._destination_resolve_overlay_incremental_parent_item("")
    assert isinstance(ix, QModelIndex)
    assert not ix.isValid()


def test_overlay_only_bind_replays_proposed_allocation_and_descendants():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    calls: list[str] = []

    def _replay_prop(*_a, **_k):
        calls.append("proposed")
        return 3

    def _replay_alloc(*_a, **_k):
        calls.append("allocation")
        return 4

    def _apply_desc(**_kw):
        calls.append("descendants")
        return 2

    mw._replay_unresolved_proposed_overlay = _replay_prop
    mw._replay_unresolved_allocation_overlay = _replay_alloc
    mw._apply_visible_destination_allocation_descendants = _apply_desc
    mw._destination_expanded_paths_for_planning_bind = lambda: []
    mw._flush_pending_destination_library_root_if_any = lambda **_k: None
    mw._log_restore_phase = lambda *_a, **_k: None
    mw._log_restore_exception = lambda *_a, **_k: None

    model = {"root_path": "Root", "nodes": {"a": {"node_state": "proposed"}}}
    vf, da = MainWindow._bind_destination_sharepoint_overlays_only(mw, model)
    assert vf == 7
    assert da == 2
    assert calls == ["proposed", "allocation", "descendants"]


def test_restore_handoff_skips_non_future_payload_under_graph_authority():
    """Restore must not append visible real rows from preserved specs — only overlay/future-state rows."""
    _qapp()
    from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel

    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw._memory_restore_in_progress = False
    mw._log_restore_phase = lambda *_a, **_k: None
    mw._destination_lifecycle_trace_TEMP = lambda *a, **k: None
    mw._refresh_destination_item_visibility_index = lambda *a, **k: None

    dm = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
    mw.destination_planning_model = dm
    parent_ix = QModelIndex()
    dm.append_child_payloads(
        parent_ix,
        [
            {
                "name": "Parent",
                "is_folder": True,
                "tree_role": "destination",
                "item_path": "Root\\Parent",
                "destination_path": "Root\\Parent",
                "base_display_label": "folder: Parent",
            }
        ],
    )
    pix = dm.index(0, 0, parent_ix)
    real_like = {
        "name": "Child",
        "is_folder": True,
        "tree_role": "destination",
        "item_path": "Root\\Parent\\Child",
        "destination_path": "Root\\Parent\\Child",
        "base_display_label": "folder: Child",
    }
    moved = MainWindow._restore_destination_future_state_children_model(mw, pix, [(real_like, [])])
    assert moved == 0
    assert dm.rowCount(pix) == 0

    proposed = {
        **real_like,
        "proposed": True,
        "base_display_label": "(Proposed) Child",
    }
    moved2 = MainWindow._restore_destination_future_state_children_model(mw, pix, [(proposed, [])])
    assert moved2 == 1
    assert dm.rowCount(pix) == 1
