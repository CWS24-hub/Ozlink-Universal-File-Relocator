"""Unit tests for planning path ↔ library-relative bridge (no Qt)."""

from __future__ import annotations

from ozlink_console.destination_path_bridge import (
    canonical_planning_path_from_library_relative_segments,
    is_internal_planning_root_semantic_path,
    library_relative_segments_from_planning_path,
    planning_path_under_anchor,
    remap_root_suffix_under_anchor,
)


def test_library_relative_strips_root():
    assert library_relative_segments_from_planning_path("Root\\A\\B") == ["A", "B"]
    assert library_relative_segments_from_planning_path("root\\a") == ["a"]


def test_canonical_from_segments():
    assert canonical_planning_path_from_library_relative_segments(["X"]) == "Root\\X"
    assert canonical_planning_path_from_library_relative_segments([]) == "Root"


def test_under_anchor():
    assert planning_path_under_anchor("Root\\Lib\\A", "Root\\Lib") is True
    assert planning_path_under_anchor("Root\\Other", "Root\\Lib") is False


def test_reanchor_single_segment_suffix():
    assert remap_root_suffix_under_anchor("Root\\Finance", "Root\\RootTest2") == "Root\\RootTest2\\Finance"


def test_reanchor_idempotent_when_already_under_anchor():
    p = "Root\\RootTest2\\Finance"
    assert remap_root_suffix_under_anchor(p, "Root\\RootTest2") == p


def test_is_internal_planning_root_semantic_path():
    assert is_internal_planning_root_semantic_path("Root") is True
    assert is_internal_planning_root_semantic_path("") is True
    assert is_internal_planning_root_semantic_path("Root\\A") is False
