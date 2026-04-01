"""Unit tests for path → drive-relative parsing and source refresh used by Graph backfill."""

from ozlink_console.planned_move_graph_resolve import (
    allocation_path_to_drive_relative,
    drive_relative_path_candidates,
    refresh_planned_move_source_from_graph,
    resolve_item_by_path_candidates,
)


def test_allocation_path_strips_library_prefix():
    rel = allocation_path_to_drive_relative(
        "HR / Files to be Migrated / FTBMRoot / A / file.txt",
        library_name="Files to be Migrated",
        site_name="HR",
    )
    assert rel == "FTBMRoot/A/file.txt"


def test_allocation_path_root_only():
    rel = allocation_path_to_drive_relative(
        "HR / Files to be Migrated / FTBMRoot",
        library_name="Files to be Migrated",
        site_name="HR",
    )
    assert rel == "FTBMRoot"


def test_allocation_path_without_site_prefix():
    rel = allocation_path_to_drive_relative(
        "Files to be Migrated / FTBMRoot / X",
        library_name="Files to be Migrated",
        site_name="HR",
    )
    assert rel == "FTBMRoot/X"


def test_allocation_path_strips_repeated_site_library_prefix():
    rel = allocation_path_to_drive_relative(
        "HR / Files to be Migrated / HR / Files to be Migrated / FTBMRoot / X",
        library_name="Files to be Migrated",
        site_name="HR",
    )
    assert rel == "FTBMRoot/X"


def test_drive_relative_path_candidates_include_fallbacks():
    c = drive_relative_path_candidates(
        r"HR\Files to be Migrated\FTBMRoot\Y",
        library_name="Files to be Migrated",
        site_name="HR",
    )
    assert "FTBMRoot/Y" in c
    assert c[0] == "FTBMRoot/Y"


def test_resolve_item_by_path_candidates_second_path_wins():
    attempts: list[str] = []

    def getter(drive: str, rel: str):
        attempts.append(rel)
        if rel == "B/leaf":
            return {"id": "ok"}
        return None

    item, idx = resolve_item_by_path_candidates(
        getter,
        "d1",
        ["wrong/path", "B/leaf"],
        phase="test",
        log_context={"move_index": 3},
    )
    assert item and item.get("id") == "ok"
    assert idx == 1
    assert attempts == ["wrong/path", "B/leaf"]


def test_refresh_source_updates_path_after_sharepoint_rename():
    raw = {
        "name": "RenamedFolder",
        "folder": {},
        "parentReference": {
            "driveId": "drive-1",
            "path": "/drives/drive-1/root:/FTBMRoot",
        },
    }

    def get_raw(_drive, _item):
        return raw

    move = {
        "source_id": "item-abc",
        "source_name": "OldFolder",
        "source_path": "HR / Lib / FTBMRoot/OldFolder",
        "source": {
            "id": "item-abc",
            "drive_id": "drive-1",
            "name": "OldFolder",
            "display_path": "HR / Lib / FTBMRoot/OldFolder",
            "item_path": "/FTBMRoot/OldFolder",
            "is_folder": True,
        },
    }
    changed = refresh_planned_move_source_from_graph(
        move,
        get_raw_item=get_raw,
        source_drive_id="drive-1",
        source_library_name="Lib",
        source_site_name="HR",
    )
    assert changed is True
    assert move["source_name"] == "RenamedFolder"
    assert "RenamedFolder" in move["source_path"]
    assert move["source"]["item_path"] == "/FTBMRoot/RenamedFolder"


def test_refresh_source_noop_when_graph_matches_stored():
    raw = {
        "name": "Same",
        "folder": {},
        "parentReference": {"driveId": "d1", "path": "/drives/d1/root:/FTBMRoot"},
    }

    display = "HR / Lib / FTBMRoot/Same"  # extra spaces; refresh compares library-relative paths, not this string
    move = {
        "source_id": "x",
        "source_name": "Same",
        "source_path": display,
        "source": {
            "id": "x",
            "drive_id": "d1",
            "name": "Same",
            "display_path": display,
            "item_path": "/FTBMRoot/Same",
            "is_folder": True,
        },
    }
    assert (
        refresh_planned_move_source_from_graph(
            move,
            get_raw_item=lambda d, i: raw,
            source_drive_id="d1",
            source_library_name="Lib",
            source_site_name="HR",
        )
        is False
    )
