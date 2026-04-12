"""Unit tests for graph-relative destination path bridge (no Qt)."""

from __future__ import annotations

from ozlink_console.destination_path_bridge import (
    canonical_planning_path_from_library_relative_segments,
    is_internal_planning_root_semantic_path,
    library_relative_segments_from_planning_path,
    planning_path_under_anchor,
    remap_root_suffix_under_anchor,
    remap_under_visible_library_anchor,
    strip_legacy_internal_root_prefix,
)


def test_strip_legacy_root_prefix():
    assert strip_legacy_internal_root_prefix("Root\\A\\B") == "A\\B"
    assert strip_legacy_internal_root_prefix("root\\a") == "a"
    assert strip_legacy_internal_root_prefix("Root3\\Finance") == "Root3\\Finance"


def test_library_relative_strips_root():
    assert library_relative_segments_from_planning_path("Root\\A\\B") == ["A", "B"]
    assert library_relative_segments_from_planning_path("root\\a") == ["a"]


def test_canonical_from_segments():
    assert canonical_planning_path_from_library_relative_segments(["X"]) == "X"
    assert canonical_planning_path_from_library_relative_segments([]) == ""


def test_under_anchor():
    assert planning_path_under_anchor("Lib\\A", "Lib") is True
    assert planning_path_under_anchor("Other", "Lib") is False


def test_reanchor_prepends_hub():
    assert remap_under_visible_library_anchor("Finance", "RootTest2") == "RootTest2\\Finance"


def test_reanchor_idempotent_when_already_under_anchor():
    p = "RootTest2\\Finance"
    assert remap_under_visible_library_anchor(p, "RootTest2") == p


def test_remap_root_suffix_legacy_strips_then_reanchors():
    assert remap_root_suffix_under_anchor("Root\\Finance", "Root\\RootTest2") == "RootTest2\\Finance"


def test_remap_root_suffix_no_op_when_path_never_used_legacy_root():
    assert remap_root_suffix_under_anchor("Other\\Finance", "Root\\RootTest2") == "Other\\Finance"


def test_is_internal_planning_root_semantic_path():
    assert is_internal_planning_root_semantic_path("Root") is True
    assert is_internal_planning_root_semantic_path("") is True
    assert is_internal_planning_root_semantic_path("Root\\A") is False
    assert is_internal_planning_root_semantic_path("Hub") is False
