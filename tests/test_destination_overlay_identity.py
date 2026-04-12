"""Tests for destination overlay identity (ovl:* ids, legacy migration)."""

from __future__ import annotations

from ozlink_console.destination_overlay_identity import (
    graph_item_id_field,
    is_legacy_synthetic_destination_row_id,
    is_overlay_node_id,
    migrate_legacy_overlay_payload_row,
    new_overlay_node_id,
)


def test_new_overlay_node_id_stable():
    a = new_overlay_node_id("allocation", "Root\\HR\\Payroll")
    b = new_overlay_node_id("allocation", "Root\\HR\\Payroll")
    assert a == b
    assert is_overlay_node_id(a)


def test_graph_item_id_rejects_prop_and_legacy_synthetic():
    assert graph_item_id_field("PROP-123") == ""
    assert graph_item_id_field("INLINE-PROP-9") == ""
    assert graph_item_id_field("allocated::Root\\x") == ""
    assert graph_item_id_field("real-graph-id-abc") == "real-graph-id-abc"


def test_migrate_legacy_allocated_payload():
    row = {"id": "allocated::Root\\Finance\\Payroll", "tree_role": "destination"}
    migrate_legacy_overlay_payload_row(row)
    assert row["id"] == ""
    assert row.get("overlay_node_id", "").startswith("ovl:allocation:")
    assert row.get("non_graph_structural_authority") is True


def test_is_legacy_synthetic_destination_row_id():
    assert is_legacy_synthetic_destination_row_id("projected::Root\\A")
    assert is_legacy_synthetic_destination_row_id("proposed::Root\\B")
    assert not is_legacy_synthetic_destination_row_id("01ABCDEF")
