"""Tests for destination anchor missing Needs Review row (session flag → workflow row)."""

from __future__ import annotations

from ozlink_console.models import SessionState
from ozlink_console.planning_destination_anchor_review import (
    REVIEW_TYPE_DESTINATION_ANCHOR_MISSING,
    build_destination_anchor_missing_needs_review_row,
    review_signature_for_destination_anchor_row,
)


def test_a_unresolved_flag_creates_needs_review_item() -> None:
    s = SessionState()
    s.DestinationAnchorLiveUnresolved = True
    s.DestinationAnchorDisplayPath = r"Site\Lib\Visible\Anchor"
    s.DestinationAnchorItemId = "item-long-id-value-xx"
    s.DestinationAnchorDriveId = "b!drive-long-value-yy"
    row = build_destination_anchor_missing_needs_review_row(s, planned_moves_count=0, proposed_folders_count=0)
    assert row is not None
    assert row["review_type"] == REVIEW_TYPE_DESTINATION_ANCHOR_MISSING
    assert "needs review" in (row.get("item_name") or "").lower()


def test_b_row_contains_display_path_and_id_suffix() -> None:
    s = SessionState()
    s.DestinationAnchorLiveUnresolved = True
    s.DestinationAnchorDisplayPath = r"Contoso\Docs\AnchorFolder"
    s.DestinationAnchorItemId = "0123456789ABCDEF"
    s.DestinationAnchorDriveId = "b!ZZZZZZZZZZZZZZ"
    row = build_destination_anchor_missing_needs_review_row(s, planned_moves_count=2, proposed_folders_count=1)
    assert row is not None
    assert row["source_path"] == r"Contoso\Docs\AnchorFolder"
    assert row.get("anchor_item_id_suffix") == "456789ABCDEF"
    assert row.get("destination_drive_id_suffix") == "ZZZZZZZZZZZZ"


def test_c_rename_same_item_id_no_row_when_flag_cleared() -> None:
    s = SessionState()
    s.DestinationAnchorLiveUnresolved = False
    s.DestinationAnchorItemId = "same-id-still-valid"
    s.DestinationAnchorDisplayPath = r"x\y\renamed-only"
    row = build_destination_anchor_missing_needs_review_row(s, planned_moves_count=5, proposed_folders_count=0)
    assert row is None


def test_d_builder_does_not_mutate_planning_session_fields() -> None:
    """Review row builder must not mutate SessionState or plan rows (imports add nothing to memory)."""
    s = SessionState()
    s.DestinationAnchorLiveUnresolved = True
    s.DestinationAnchorDisplayPath = "a\\b"
    before = (s.DestinationAnchorDisplayPath, s.DestinationAnchorLiveUnresolved, s.DraftId)
    _ = build_destination_anchor_missing_needs_review_row(s, planned_moves_count=3, proposed_folders_count=2)
    after = (s.DestinationAnchorDisplayPath, s.DestinationAnchorLiveUnresolved, s.DraftId)
    assert before == after


def test_e_signature_tracks_planning_counts_for_increments_and_updates() -> None:
    s1 = review_signature_for_destination_anchor_row(
        anchor_path="p",
        item_id="i",
        drive_id="d",
        planned_moves_count=1,
        proposed_folders_count=0,
    )
    s2 = review_signature_for_destination_anchor_row(
        anchor_path="p",
        item_id="i",
        drive_id="d",
        planned_moves_count=2,
        proposed_folders_count=0,
    )
    assert s1 != s2
