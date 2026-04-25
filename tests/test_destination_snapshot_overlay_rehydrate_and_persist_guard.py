"""Overlay rehydration, reuse predicates, and startup snapshot persist-downgrade guards."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ozlink_console.main_window import MainWindow
from ozlink_console.sharepoint_destination_overlay_attach import (
    destination_payload_is_live_graph_row,
    destination_payload_is_memory_overlay_row_for_reuse,
    destination_payload_is_planned_workspace_row,
    destination_snapshot_rehydrate_overlay_payload,
)


def test_rehydrate_restores_strict_planned_workspace_pairing():
    pl = {
        "planned_allocation_descendant": True,
        "is_folder": True,
        "verification_state": "",
        "row_kind": "cached_provisional_shell",
        "base_display_label": "x",
    }
    assert destination_snapshot_rehydrate_overlay_payload(pl)
    assert destination_payload_is_planned_workspace_row(pl)


def test_memory_overlay_predicate_accepts_restored_allocation_descendant():
    pl = {
        "planned_allocation_descendant": True,
        "is_folder": False,
        "verification_state": "",
        "row_kind": "generic",
        "base_display_label": "file [Allocated]",
    }
    assert destination_payload_is_memory_overlay_row_for_reuse(pl)
    assert not destination_payload_is_live_graph_row(pl)


def test_live_graph_row_not_memory_overlay():
    pl = {
        "id": "g1",
        "drive_id": "d1",
        "tree_role": "destination",
        "is_folder": True,
        "workspace_row_state": "live_confirmed",
        "verification_state": "live_confirmed",
        "row_kind": "live_folder",
    }
    assert destination_payload_is_live_graph_row(pl)
    assert not destination_payload_is_memory_overlay_row_for_reuse(pl)


def test_cached_graph_merge_target_without_planning_is_not_memory_overlay():
    pl = {
        "workspace_row_state": "cached_provisional",
        "row_kind": "live_folder",
        "verification_state": "live_confirmed",
        "id": "graph1",
        "drive_id": "d1",
        "is_folder": True,
        "tree_role": "destination",
    }
    assert not destination_payload_is_memory_overlay_row_for_reuse(pl)


def test_legacy_folder_row_kind_planned_only_cached_provisional_is_strict_planned_workspace():
    """Snapshots may keep structural ``folder`` row_kind while marking verification planned_only."""
    pl = {
        "tree_role": "destination",
        "is_folder": True,
        "workspace_row_state": "cached_provisional",
        "verification_state": "planned_only",
        "row_kind": "folder",
        "item_path": r"Root3\HR\Example",
    }
    assert destination_payload_is_planned_workspace_row(pl)
    assert destination_payload_is_memory_overlay_row_for_reuse(pl)


def test_planned_workspace_row_accepts_empty_verification_when_workspace_state_planned_only():
    pl = {
        "workspace_row_state": "planned_only",
        "verification_state": "",
        "row_kind": "planned_folder",
        "is_folder": True,
        "tree_role": "destination",
    }
    assert destination_payload_is_planned_workspace_row(pl)


def test_thin_persist_blocked_against_rich_sidecar_until_unlock():
    mw = MainWindow.__new__(MainWindow)
    mw._application_shutting_down = False
    mw._destination_workspace_sidecar_destination_node_count_at_startup = 570
    mw._destination_final_startup_destination_snapshot_node_count = 570
    mw._destination_destination_snapshot_persist_startup_unlocked = False
    blocked, reason = mw._destination_should_block_thin_destination_snapshot_over_rich_sidecar(0)
    assert blocked is True
    assert reason
    blocked69, _ = mw._destination_should_block_thin_destination_snapshot_over_rich_sidecar(69)
    assert blocked69 is True
    mw._destination_destination_snapshot_persist_startup_unlocked = True
    blocked_after, _ = mw._destination_should_block_thin_destination_snapshot_over_rich_sidecar(69)
    assert blocked_after is False


def test_memory_overlay_predicate_proposed_and_planned_allocation_origin():
    pl = {
        "tree_role": "destination",
        "is_folder": True,
        "proposed": True,
        "workspace_row_state": "cached_provisional",
        "verification_state": "",
        "row_kind": "cached_provisional_shell",
    }
    assert destination_payload_is_memory_overlay_row_for_reuse(pl)
    pl2 = {
        "tree_role": "destination",
        "is_folder": True,
        "planned_allocation": True,
        "workspace_row_state": "cached_provisional",
        "id": "x",
        "drive_id": "d",
    }
    assert destination_payload_is_memory_overlay_row_for_reuse(pl2)


def test_rehydrate_from_workspace_row_state_planned_only():
    pl = {
        "workspace_row_state": "planned_only",
        "row_kind": "cached_provisional_shell",
        "verification_state": "",
        "is_folder": True,
        "base_display_label": "Folder",
    }
    assert destination_snapshot_rehydrate_overlay_payload(pl)
    assert destination_payload_is_planned_workspace_row(pl)


def test_overlay_pipeline_finishes_and_sets_persist_ready_for_provisional():
    mw = MainWindow.__new__(MainWindow)

    class _M:
        def iter_depth_first(self):
            return []

    mw.destination_planning_model = _M()
    mw._destination_loaded_snapshot_overlay_classification_audit = MagicMock(return_value={})
    mw._destination_loaded_snapshot_allocation_subtree_audit = MagicMock()
    mw._destination_rehydrate_overlay_payloads_in_destination_model = MagicMock(return_value=0)
    mw._destination_startup_snapshot_preservation_applied = False
    mw._destination_run_post_snapshot_bind_overlay_pipeline(reason="provisional_startup_reset_nested")
    assert mw._destination_snapshot_overlay_classification_startup_complete is True
    assert mw._destination_startup_overlay_snapshot_persist_ready is True


def test_overlay_pipeline_pre_graph_sets_persist_ready_false_until_preservation():
    mw = MainWindow.__new__(MainWindow)

    class _M:
        def iter_depth_first(self):
            return []

    mw.destination_planning_model = _M()
    mw._destination_loaded_snapshot_overlay_classification_audit = MagicMock(return_value={})
    mw._destination_loaded_snapshot_allocation_subtree_audit = MagicMock()
    mw._destination_rehydrate_overlay_payloads_in_destination_model = MagicMock(return_value=0)
    mw._destination_startup_snapshot_preservation_applied = False
    mw._destination_run_post_snapshot_bind_overlay_pipeline(reason="pre_graph_session_bind_reset_nested")
    assert mw._destination_startup_overlay_snapshot_persist_ready is False
    mw._destination_startup_snapshot_preservation_applied = True
    mw._destination_run_post_snapshot_bind_overlay_pipeline(reason="pre_graph_session_bind_reset_nested")
    assert mw._destination_startup_overlay_snapshot_persist_ready is True


def test_thin_persist_blocked_when_overlay_persist_gate_not_ready():
    mw = MainWindow.__new__(MainWindow)
    mw._application_shutting_down = False
    mw._destination_workspace_sidecar_destination_node_count_at_startup = 500
    mw._destination_final_startup_destination_snapshot_node_count = 500
    mw._destination_last_overlay_audit_total_rows = 500
    mw._destination_destination_snapshot_persist_startup_unlocked = False
    mw._destination_startup_snapshot_mount_seen = True
    mw._destination_startup_overlay_snapshot_persist_ready = False
    blocked, reason = mw._destination_should_block_thin_destination_snapshot_over_rich_sidecar(50)
    assert blocked is True
    assert "persist_gate" in reason or "not_ready" in reason


def test_thin_persist_blocked_rich_sidecar_3000_vs_thin_model_309():
    mw = MainWindow.__new__(MainWindow)
    mw._application_shutting_down = False
    mw._destination_workspace_sidecar_destination_node_count_at_startup = 3000
    mw._destination_final_startup_destination_snapshot_node_count = 3000
    mw._destination_last_overlay_audit_total_rows = 3000
    mw._destination_destination_snapshot_persist_startup_unlocked = False
    mw._destination_startup_snapshot_mount_seen = True
    mw._destination_startup_overlay_snapshot_persist_ready = True
    blocked, reason = mw._destination_should_block_thin_destination_snapshot_over_rich_sidecar(309)
    assert blocked is True
    assert "thin_capture" in reason or "baseline" in reason


def test_thin_persist_not_blocked_when_baseline_small():
    mw = MainWindow.__new__(MainWindow)
    mw._application_shutting_down = False
    mw._destination_workspace_sidecar_destination_node_count_at_startup = 0
    mw._destination_final_startup_destination_snapshot_node_count = 10
    mw._destination_destination_snapshot_persist_startup_unlocked = False
    blocked, _ = mw._destination_should_block_thin_destination_snapshot_over_rich_sidecar(0)
    assert blocked is False


def test_strict_planned_workspace_row_counts_for_memory_overlay_reuse():
    """FIX 1A/C: strict planned rows are memory-overlay-eligible for reuse/presnapshot (not live)."""
    pl = {
        "tree_role": "destination",
        "is_folder": True,
        "workspace_row_state": "planned_only",
        "verification_state": "planned_only",
        "row_kind": "planned_folder",
        "item_path": r"Root3\HR\Employee Files\Contractor Resumes\file.pdf",
    }
    assert destination_payload_is_planned_workspace_row(pl)
    assert destination_payload_is_memory_overlay_row_for_reuse(pl)
    assert not destination_payload_is_live_graph_row(pl)


def test_placeholder_not_memory_overlay_for_reuse():
    """FIX 1D."""
    assert not destination_payload_is_memory_overlay_row_for_reuse({"placeholder": True, "verification_state": "planned_only"})


def _presnapshot_mw_stub():
    mw = MainWindow.__new__(MainWindow)
    mw._destination_semantic_path = lambda pl: str((pl or {}).get("item_path") or "")
    mw._canonical_planned_memory_path_for_graph_match = lambda p: str(p or "").replace("/", "\\").strip()
    mw._destination_row_raw_path_for_path_lookup_match = lambda _pl: ""
    mw._tree_item_path = lambda pl: str((pl or {}).get("item_path") or "")
    return mw


def test_graph_bind_presnapshot_classify_planned_direct_child_included():
    """FIX 2A: strict planned child under Graph parent path is included."""
    mw = _presnapshot_mw_stub()
    parent_cf = mw._canonical_planned_memory_path_for_graph_match(r"Root3\Sales")
    pl = {
        "item_path": r"Root3\Sales\Pictures",
        "is_folder": True,
        "verification_state": "planned_only",
        "row_kind": "planned_folder",
        "workspace_row_state": "planned_only",
    }
    ok, reason = mw._destination_graph_bind_presnapshot_classify_row(pl, parent_path_cf=parent_cf)
    assert ok is True
    assert reason == ""


def test_graph_bind_presnapshot_classify_proposed_child_included():
    """FIX 2B: proposed / planning-touch child included."""
    mw = _presnapshot_mw_stub()
    parent_cf = mw._canonical_planned_memory_path_for_graph_match(r"Root3\Management")
    pl = {
        "item_path": r"Root3\Management\Follow up",
        "is_folder": True,
        "workspace_row_state": "cached_provisional",
        "verification_state": "",
        "row_kind": "cached_provisional_shell",
        "proposed": True,
        "request_id": "req-proposed-followup",
    }
    ok, reason = mw._destination_graph_bind_presnapshot_classify_row(pl, parent_path_cf=parent_cf)
    assert ok is True
    assert reason == ""


def test_graph_bind_presnapshot_classify_live_graph_child_excluded():
    """FIX 2C."""
    mw = _presnapshot_mw_stub()
    parent_cf = mw._canonical_planned_memory_path_for_graph_match(r"Root3\Sales")
    pl = {
        "item_path": r"Root3\Sales\LiveChild",
        "is_folder": True,
        "workspace_row_state": "live_confirmed",
        "verification_state": "live_confirmed",
        "row_kind": "live_folder",
        "drive_id": "d1",
        "id": "x1",
    }
    ok, reason = mw._destination_graph_bind_presnapshot_classify_row(pl, parent_path_cf=parent_cf)
    assert ok is False
    assert reason == "live_graph_row"


def test_graph_bind_presnapshot_classify_placeholder_excluded():
    """FIX 2D."""
    mw = _presnapshot_mw_stub()
    parent_cf = mw._canonical_planned_memory_path_for_graph_match(r"Root3\Sales")
    pl = {"placeholder": True, "item_path": r"Root3\Sales\Loading"}
    ok, reason = mw._destination_graph_bind_presnapshot_classify_row(pl, parent_path_cf=parent_cf)
    assert ok is False
    assert reason == "placeholder"
