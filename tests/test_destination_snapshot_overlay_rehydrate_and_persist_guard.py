"""Overlay rehydration, reuse predicates, and startup snapshot persist-downgrade guards."""

from __future__ import annotations

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


def test_thin_persist_not_blocked_when_baseline_small():
    mw = MainWindow.__new__(MainWindow)
    mw._application_shutting_down = False
    mw._destination_workspace_sidecar_destination_node_count_at_startup = 0
    mw._destination_final_startup_destination_snapshot_node_count = 10
    mw._destination_destination_snapshot_persist_startup_unlocked = False
    blocked, _ = mw._destination_should_block_thin_destination_snapshot_over_rich_sidecar(0)
    assert blocked is False
