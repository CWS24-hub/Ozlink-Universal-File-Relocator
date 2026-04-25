from __future__ import annotations

from ozlink_console.models import SessionState
from ozlink_console.planning_selector_restore import library_combo_index_for_session_restore


def test_drive_id_match_ignores_display_name_mismatch():
    rows = [
        ("Renamed Label", {"id": "drive-AAA", "name": "Renamed Label"}),
        ("Other", {"id": "drive-BBB", "name": "Other"}),
    ]
    idx, tag = library_combo_index_for_session_restore(
        stored_drive_id="drive-BBB",
        stored_display_name="Old Name",
        item_rows=rows,
    )
    assert (idx, tag) == (1, "drive_id_match")


def test_stored_drive_id_no_fallback_on_miss():
    rows = [("Lib", {"id": "x", "name": "Lib"})]
    idx, tag = library_combo_index_for_session_restore(
        stored_drive_id="missing-id",
        stored_display_name="Lib",
        item_rows=rows,
    )
    assert (idx, tag) == (-1, "unmatched_drive_id")


def test_legacy_name_match_when_no_drive_id():
    rows = [
        ("Alpha", {"id": "d1", "name": "Alpha"}),
        ("Beta", {"id": "d2", "name": "Beta"}),
    ]
    idx, tag = library_combo_index_for_session_restore(
        stored_drive_id="",
        stored_display_name="Beta",
        item_rows=rows,
    )
    assert (idx, tag) == (1, "legacy_name_only")


def test_legacy_name_miss_does_not_pick_index_zero():
    rows = [("Only", {"id": "d1", "name": "Only"})]
    idx, tag = library_combo_index_for_session_restore(
        stored_drive_id="",
        stored_display_name="Nope",
        item_rows=rows,
    )
    assert (idx, tag) == (-1, "unmatched_name")


def test_no_session_hint_both_empty():
    rows = [("Only", {"id": "d1", "name": "Only"})]
    idx, tag = library_combo_index_for_session_restore(
        stored_drive_id="",
        stored_display_name="",
        item_rows=rows,
    )
    assert (idx, tag) == (-1, "no_session_hint")


def test_session_state_from_dict_defaults_new_library_id_fields():
    s = SessionState.from_dict({"DraftId": "x"})
    assert getattr(s, "SelectedSourceLibraryId", None) == ""
    assert getattr(s, "SelectedDestinationLibraryId", None) == ""
