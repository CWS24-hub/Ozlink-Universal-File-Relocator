"""Unit tests for destination future-model overlay cache fingerprints."""

from __future__ import annotations

from ozlink_console.destination_projection_cache import (
    aggregate_moves_signature,
    full_overlay_fingerprint,
    move_list_signatures,
    proposed_folders_signature,
    real_snapshot_signature,
    stable_move_signature,
)


def test_stable_move_signature_non_dict():
    assert stable_move_signature(None) == ("", "", "", "", "", False, "")
    assert stable_move_signature("x") == ("", "", "", "", "", False, "")


def test_move_list_signatures_strict_extension_prefix():
    m1 = {"request_id": "a", "source_path": "s", "destination_path": "d"}
    m2 = {**m1, "target_name": "t"}
    s1 = move_list_signatures([m1])
    s2 = move_list_signatures([m1, m2])
    assert len(s2) > len(s1)
    assert s2[: len(s1)] == s1


def test_aggregate_moves_signature_order_sensitive():
    a = {"request_id": "1"}
    b = {"request_id": "2"}
    assert aggregate_moves_signature([a, b]) != aggregate_moves_signature([b, a])


def test_proposed_folders_signature_stable():
    class PF:
        DestinationPath = "\\A"
        FolderName = "F"
        ParentPath = "\\"
        Status = "draft"

    s1 = proposed_folders_signature([PF()])
    s2 = proposed_folders_signature([PF()])
    assert s1 == s2


def test_real_snapshot_signature_includes_drive_and_full_tree_n():
    snap = [{"semantic_path": "\\B"}, {"semantic_path": "\\A"}]
    s1 = real_snapshot_signature(snap, drive_id="d1", full_tree_entry_count=0)
    s2 = real_snapshot_signature(snap, drive_id="d2", full_tree_entry_count=0)
    s3 = real_snapshot_signature(snap, drive_id="d1", full_tree_entry_count=5)
    assert s1 != s2
    assert s1 != s3


def test_full_overlay_fingerprint_skip_flag():
    fp = full_overlay_fingerprint(
        moves_sig="m",
        proposed_sig="p",
        snapshot_sig="s",
        skip_allocation_descendants=False,
    )
    fp2 = full_overlay_fingerprint(
        moves_sig="m",
        proposed_sig="p",
        snapshot_sig="s",
        skip_allocation_descendants=True,
    )
    assert fp != fp2
