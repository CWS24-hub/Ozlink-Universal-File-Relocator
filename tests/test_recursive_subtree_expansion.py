"""Unit tests for Graph recursive subtree expansion helpers (Option B, in-memory bundle injection)."""

from __future__ import annotations

from ozlink_console.draft_snapshot.recursive_subtree_expansion import (
    compose_path_under_folder,
    deterministic_recursive_mapping_id,
    file_level_allocation_method,
    graph_path_is_strict_descendant_file,
    memory_canonical_to_graph_path,
    normalize_graph_path,
    planned_move_folder_template_for_browser,
    relative_suffix_under_folder,
    synthetic_template_for_browser_run,
)


def test_deterministic_mapping_id_stable():
    a = deterministic_recursive_mapping_id("d1", "i1")
    b = deterministic_recursive_mapping_id("d1", "i1")
    c = deterministic_recursive_mapping_id("d1", "i2")
    assert a == b
    assert a.startswith("recsub-")
    assert a != c


def test_normalize_graph_path():
    assert normalize_graph_path("") == "/"
    assert normalize_graph_path("/a//b") == "/a/b"
    assert normalize_graph_path("a\\b") == "/a/b"


def test_memory_canonical_to_graph_path():
    assert memory_canonical_to_graph_path(r"Team\Data") == "/Team/Data"


def test_strict_descendant():
    assert graph_path_is_strict_descendant_file("/A", "/A/f.txt")
    assert not graph_path_is_strict_descendant_file("/A", "/A")
    assert not graph_path_is_strict_descendant_file("/A", "/B/f.txt")
    assert not graph_path_is_strict_descendant_file("/A/B", "/A/C/x.txt")


def test_relative_suffix():
    assert relative_suffix_under_folder("/A/B", "/A/B/x.txt") == "x.txt"
    assert relative_suffix_under_folder("/A/B", "/A/B/sub/y.txt") == "sub\\y.txt"
    assert relative_suffix_under_folder("/A/B", "/A/C/x.txt") is None


def test_compose_path_under_folder():
    assert compose_path_under_folder(r"Root\Lib\Box", "a\\b.txt") == r"Root\Lib\Box\a\b.txt"


def test_file_level_allocation_method_strips_recursive():
    assert "recursive" not in file_level_allocation_method("Move Recursive").lower()
    assert file_level_allocation_method("") == "move"


def test_synthetic_template_for_browser_run():
    t = synthetic_template_for_browser_run()
    assert t.get("status") == "Draft"
    assert "requested_by" in t


def test_planned_move_folder_template_for_browser():
    m = synthetic_template_for_browser_run(requested_by="x")
    tpl = planned_move_folder_template_for_browser(
        meta=m,
        dest_folder_name="Dest",
        dest_drive_id="d1",
        dest_folder_item_id="i1",
        dest_folder_path=r"Root\Dest",
    )
    assert tpl["destination"]["name"] == "Dest"
    assert tpl["destination"]["drive_id"] == "d1"
