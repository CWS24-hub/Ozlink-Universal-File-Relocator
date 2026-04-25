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


def test_graph_authority_fingerprint_skip_blocked_when_planning_memory_but_no_visible_overlay_evidence(monkeypatch):
    from ozlink_console import destination_authority_contract as dac

    monkeypatch.setattr(dac, "graph_owns_visible_real_destination_structure", lambda _host: True)
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._destination_suppress_steady_materialize_skip_once = False
    mw._destination_restore_completed_once = True
    mw.destination_tree_widget = type("T", (), {"topLevelItemCount": lambda self: 1})()
    mw._planning_tree_top_level_count = lambda tree: MainWindow._planning_tree_top_level_count(mw, tree)
    mw._destination_should_block_idle_full_tree_materialize = lambda: False
    mw._destination_materialize_reason_may_skip_without_interrupting_async = lambda _r: True
    mw._destination_future_projection_async_state = None
    mw._current_destination_full_overlay_fingerprint = lambda self, force_refresh_snapshot=True: "fp1"
    mw._destination_last_materialized_overlay_fp = "fp1"
    mw._count_visible_destination_future_state_nodes = lambda: 0
    mw._unresolved_proposed_queue_size = lambda: 0
    mw._unresolved_allocation_queue_size = lambda: 0
    mw.planned_moves = [object()]
    mw.proposed_folders = []
    mw._log_restore_phase = lambda *a, **k: None
    mw._set_tree_status_message = lambda *a, **k: None
    assert MainWindow._try_skip_redundant_destination_future_model_materialize(mw, "steady_tick") is None


def test_ensure_sharepoint_graph_walk_reanchors_under_visible_hub(monkeypatch):
    """Hub-less planning paths must walk Root3\\Finance\\… so Graph loads queue from the real library hub."""
    from ozlink_console import destination_authority_contract as dac

    monkeypatch.setattr(dac, "graph_owns_visible_real_destination_structure", lambda _host: True)
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.destination_planning_model = object()
    mw._destination_visible_library_anchor_canonical_path = lambda: "Root3"
    mw._canonical_destination_projection_path = MainWindow._canonical_destination_projection_path.__get__(mw, MainWindow)
    mw.normalize_memory_path = MainWindow.normalize_memory_path.__get__(mw, MainWindow)
    seen: list[str] = []

    def _fc(_parent_ix, _path, **_kw):
        return None

    def _fv(path):
        seen.append(str(path))
        return None

    mw._find_destination_child_by_path = _fc
    mw._find_visible_destination_item_by_path = _fv
    mw._destination_row_is_live_graph_structure = lambda _pl: True
    mw._destination_model_index_user_role_dict = lambda _ix: {}
    mw._request_graph_destination_children_load = lambda *_a, **_k: None
    mw._log_restore_phase = lambda *_a, **_k: None

    MainWindow._ensure_destination_projection_path_sharepoint_graph_only(mw, "Finance\\Sub")
    # Without hub re-anchor the first lookup would be ``Finance`` (wrong vs Graph index); with anchor it is the visible hub.
    assert seen and seen[0] == "Root3"
    assert "Finance" not in seen


def test_proactive_overlay_chain_calls_graph_ensure(monkeypatch):
    from ozlink_console import destination_authority_contract as dac

    monkeypatch.setattr(dac, "graph_owns_visible_real_destination_structure", lambda _host: True)
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.destination_tree_widget = type("T", (), {"topLevelItemCount": lambda self: 1})()
    mw._planning_tree_top_level_count = lambda tree: MainWindow._planning_tree_top_level_count(mw, tree)
    mw.unresolved_proposed_by_parent_path = {"Root\\Target": {"a": object()}}
    mw.unresolved_allocations_by_parent_path = {}
    mw.proposed_folders = []
    mw.planned_moves = []
    mw._destination_overlay_proactive_chain_seen = set()
    mw._canonical_destination_projection_path = MainWindow._canonical_destination_projection_path.__get__(mw, MainWindow)
    mw.normalize_memory_path = MainWindow.normalize_memory_path.__get__(mw, MainWindow)
    mw._proposed_parent_path = lambda _pf: "Root\\Target"
    mw._allocation_parent_path = lambda _mv: ""
    calls: list[str] = []
    mw._ensure_destination_projection_path_sharepoint_graph_only = lambda p: calls.append(p) or None
    mw._log_restore_phase = lambda *a, **k: None
    n = MainWindow._schedule_proactive_graph_parent_chains_for_unresolved_overlays(mw, reason="unit")
    assert n == 1
    assert calls == ["Target"]


def test_visible_path_lookup_keys_include_suffix_under_visible_hub(monkeypatch):
    """Index buckets may be ``Finance`` (trimmed item_path) while memory uses ``Root3\\Finance``."""
    from ozlink_console import destination_authority_contract as dac

    monkeypatch.setattr(dac, "graph_owns_visible_real_destination_structure", lambda _host: True)
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw.normalize_memory_path = MainWindow.normalize_memory_path.__get__(mw, MainWindow)
    mw._canonical_destination_projection_path = MainWindow._canonical_destination_projection_path.__get__(mw, MainWindow)
    mw._path_segments = MainWindow._path_segments.__get__(mw, MainWindow)
    mw._destination_visible_library_anchor_canonical_path = lambda: "Root3"
    keys, _ = MainWindow._destination_visible_path_lookup_canonical_keys_ex(mw, "Root3\\Finance", "Root3\\Finance")
    assert "Root3\\Finance" in keys
    assert "Finance" in keys


def test_visible_path_lookup_keys_prepend_hub_when_normalized_is_library_trimmed(monkeypatch):
    """Memory restore uses ``Finance\\Follow up`` while path index buckets stayed ``Root3\\Finance\\Follow up``."""
    from ozlink_console import destination_authority_contract as dac

    monkeypatch.setattr(dac, "graph_owns_visible_real_destination_structure", lambda _host: True)
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw.normalize_memory_path = MainWindow.normalize_memory_path.__get__(mw, MainWindow)
    mw._canonical_destination_projection_path = MainWindow._canonical_destination_projection_path.__get__(mw, MainWindow)
    mw._path_segments = MainWindow._path_segments.__get__(mw, MainWindow)
    mw._destination_visible_library_anchor_canonical_path = lambda: "Root3"
    keys, _ = MainWindow._destination_visible_path_lookup_canonical_keys_ex(
        mw, "Finance\\Follow up", "Finance\\Follow up"
    )
    assert "Finance\\Follow up" in keys
    assert "Root3\\Finance\\Follow up" in keys


def test_visible_path_lookup_canonical_keys_include_reanchor_under_single_library_hub():
    _qapp()
    from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel

    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw.normalize_memory_path = MainWindow.normalize_memory_path.__get__(mw, MainWindow)
    mw._canonical_destination_projection_path = MainWindow._canonical_destination_projection_path.__get__(mw, MainWindow)
    mw._destination_reanchor_sharepoint_projection_path = MainWindow._destination_reanchor_sharepoint_projection_path.__get__(
        mw, MainWindow
    )
    mw._destination_visible_library_anchor_canonical_path = lambda: "LibHub"
    dm = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
    mw.destination_planning_model = dm
    parent_ix = QModelIndex()
    dm.append_child_payloads(
        parent_ix,
        [
            {
                "name": "LibHub",
                "is_folder": True,
                "tree_role": "destination",
                "item_path": "Root\\LibHub",
                "destination_path": "Root\\LibHub",
                "base_display_label": "folder: LibHub",
            }
        ],
    )
    keys = MainWindow._destination_visible_path_lookup_canonical_keys(mw, "Root\\Dept", "Dept")
    assert keys[0] == "LibHub\\Dept"
    assert "Dept" in keys


def test_find_visible_destination_item_matches_row_via_reanchored_canonical_key():
    _qapp()
    from PySide6.QtWidgets import QTreeView
    from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel

    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw.normalize_memory_path = MainWindow.normalize_memory_path.__get__(mw, MainWindow)
    mw._canonical_destination_projection_path = MainWindow._canonical_destination_projection_path.__get__(mw, MainWindow)
    mw._destination_parent_match_details = MainWindow._destination_parent_match_details.__get__(mw, MainWindow)
    mw._destination_semantic_path = MainWindow._destination_semantic_path.__get__(mw, MainWindow)
    mw._tree_item_path = MainWindow._tree_item_path.__get__(mw, MainWindow)
    mw._path_segments = MainWindow._path_segments.__get__(mw, MainWindow)
    mw._destination_resolution_rank = MainWindow._destination_resolution_rank.__get__(mw, MainWindow)
    mw._root_tree_bind_in_progress = False
    mw._destination_path_lookup_cache = {}
    mw._log_restore_phase = lambda *a, **k: None
    mw._unresolved_proposed_queue_size = lambda: 0
    mw._unresolved_allocation_queue_size = lambda: 0
    dm = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
    tw = QTreeView()
    tw.setModel(dm)
    mw.destination_tree_widget = tw
    mw.destination_planning_model = dm
    parent_ix = QModelIndex()
    dm.append_child_payloads(
        parent_ix,
        [
            {
                "name": "LibHub",
                "is_folder": True,
                "tree_role": "destination",
                "item_path": "Root\\LibHub",
                "destination_path": "Root\\LibHub",
                "base_display_label": "folder: LibHub",
                "id": "hub1",
                "library_id": "drv",
            }
        ],
    )
    hub_ix = dm.index(0, 0, parent_ix)
    dm.append_child_payloads(
        hub_ix,
        [
            {
                "name": "Dept",
                "is_folder": True,
                "tree_role": "destination",
                "item_path": "Root\\LibHub\\Dept",
                "destination_path": "Root\\LibHub\\Dept",
                "base_display_label": "folder: Dept",
                "id": "dept1",
                "library_id": "drv",
            }
        ],
    )
    dept_ix = dm.index(0, 0, hub_ix)
    found = MainWindow._find_visible_destination_item_by_path(mw, "Root\\Dept")
    assert found is not None
    assert found == dept_ix


def test_visible_library_anchor_single_hub_when_authority_placeholder_sibling_at_root():
    """rowCount > 1 with only one real folder hub must still supply re-anchor (not require rc == 1)."""
    _qapp()
    from PySide6.QtWidgets import QTreeView

    from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel

    loading = {
        "placeholder": True,
        "placeholder_role": "destination_authority_pending",
        "base_display_label": "Reconciling…",
        "tree_role": "destination",
        "non_authoritative_destination_shell": True,
    }
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw.normalize_memory_path = MainWindow.normalize_memory_path.__get__(mw, MainWindow)
    mw._canonical_destination_projection_path = MainWindow._canonical_destination_projection_path.__get__(mw, MainWindow)
    mw._tree_item_path = MainWindow._tree_item_path.__get__(mw, MainWindow)
    dm = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
    tw = QTreeView()
    tw.setModel(dm)
    mw.destination_tree_widget = tw
    mw.destination_planning_model = dm
    dm.reset_root_payloads(
        [
            loading,
            {
                "name": "LibHub",
                "is_folder": True,
                "tree_role": "destination",
                "item_path": "Root\\LibHub",
                "destination_path": "Root\\LibHub",
                "base_display_label": "folder: LibHub",
                "id": "hub1",
                "library_id": "drv",
            },
        ]
    )
    assert dm.rowCount(QModelIndex()) == 2
    anchor = MainWindow._destination_visible_library_anchor_canonical_path(mw)
    assert anchor.lower().endswith("libhub")


def test_find_visible_reanchors_when_authority_placeholder_sibling_at_root():
    """Without this, persisted Root\\.. paths miss the path index when a shell row shares the root."""
    _qapp()
    from PySide6.QtWidgets import QTreeView

    from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel

    loading = {
        "placeholder": True,
        "placeholder_role": "destination_authority_pending",
        "base_display_label": "Reconciling…",
        "tree_role": "destination",
        "non_authoritative_destination_shell": True,
    }
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw.normalize_memory_path = MainWindow.normalize_memory_path.__get__(mw, MainWindow)
    mw._canonical_destination_projection_path = MainWindow._canonical_destination_projection_path.__get__(mw, MainWindow)
    mw._destination_parent_match_details = MainWindow._destination_parent_match_details.__get__(mw, MainWindow)
    mw._destination_semantic_path = MainWindow._destination_semantic_path.__get__(mw, MainWindow)
    mw._tree_item_path = MainWindow._tree_item_path.__get__(mw, MainWindow)
    mw._path_segments = MainWindow._path_segments.__get__(mw, MainWindow)
    mw._destination_resolution_rank = MainWindow._destination_resolution_rank.__get__(mw, MainWindow)
    mw._root_tree_bind_in_progress = False
    mw._destination_path_lookup_cache = {}
    mw._log_restore_phase = lambda *a, **k: None
    mw._unresolved_proposed_queue_size = lambda: 0
    mw._unresolved_allocation_queue_size = lambda: 0
    dm = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
    tw = QTreeView()
    tw.setModel(dm)
    mw.destination_tree_widget = tw
    mw.destination_planning_model = dm
    dm.reset_root_payloads(
        [
            loading,
            {
                "name": "LibHub",
                "is_folder": True,
                "tree_role": "destination",
                "item_path": "Root\\LibHub",
                "destination_path": "Root\\LibHub",
                "base_display_label": "folder: LibHub",
                "id": "hub1",
                "library_id": "drv",
            },
        ]
    )
    hub_ix = dm.index(1, 0, QModelIndex())
    dm.append_child_payloads(
        hub_ix,
        [
            {
                "name": "Dept",
                "is_folder": True,
                "tree_role": "destination",
                "item_path": "Root\\LibHub\\Dept",
                "destination_path": "Root\\LibHub\\Dept",
                "base_display_label": "folder: Dept",
                "id": "dept1",
                "library_id": "drv",
            }
        ],
    )
    dept_ix = dm.index(0, 0, hub_ix)
    found = MainWindow._find_visible_destination_item_by_path(mw, "Root\\Dept")
    assert found is not None
    assert found == dept_ix


def test_graph_authority_payload_index_prefers_item_path_over_human_display_path():
    """Graph display_path often embeds site/library text; index keys must match drive-relative paths."""
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw.destination_planning_model = object()
    mw.normalize_memory_path = MainWindow.normalize_memory_path.__get__(mw, MainWindow)
    mw._canonical_destination_projection_path = MainWindow._canonical_destination_projection_path.__get__(mw, MainWindow)
    pl = {
        "name": "Follow Up",
        "is_folder": True,
        "tree_role": "destination",
        "display_path": "Contoso / Documents/Root3/Sales/Follow Up",
        "item_path": "/Root3/Sales/Follow Up",
        "id": "x1",
    }
    key = MainWindow._destination_payload_index_key(mw, pl)
    assert "Contoso" not in (key or "")
    assert "Root3" in (key or "")
    assert "Sales" in (key or "")
