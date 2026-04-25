"""Regression: pilot / manifest identity for Graph folder-copy rows (no duplicate path tail)."""

from __future__ import annotations

from ozlink_console.paths import manifest_folder_copy_logical_path, normalize_manifest_path


def test_manifest_folder_copy_logical_path_avoids_duplicate_leaf():
    assert manifest_folder_copy_logical_path(r"Root\Personal", "Personal") == r"Root\Personal"
    assert manifest_folder_copy_logical_path("Root/Personal", "personal") == r"Root\Personal"


def test_manifest_folder_copy_logical_path_parent_plus_distinct_leaf():
    assert manifest_folder_copy_logical_path("Root", "Personal") == r"Root\Personal"
    assert manifest_folder_copy_logical_path(r"Root\Personal", "100GOPRO") == r"Root\Personal\100GOPRO"


def test_manifest_folder_copy_logical_path_empty_destination():
    assert manifest_folder_copy_logical_path("", "Personal") == ""
    assert manifest_folder_copy_logical_path("   ", "Personal") == ""
    assert manifest_folder_copy_logical_path("", "") == ""


def test_normalize_manifest_path_collapses_slashes_and_spaces():
    assert normalize_manifest_path("  a / b\\\\c  ") == r"a\b\c"
