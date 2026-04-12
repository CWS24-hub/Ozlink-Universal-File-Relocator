"""Unit tests for SharePoint single-folder anchor inference and projection path reanchoring."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from ozlink_console.main_window import MainWindow


def _qapp():
    return QApplication.instance() or QApplication([])


def _real_folder_node(name: str) -> dict:
    return {
        "parent_semantic_path": "Root",
        "node_state": "real",
        "data": {"name": name, "is_folder": True},
    }


def _projected_folder_node(name: str) -> dict:
    return {
        "parent_semantic_path": "Root",
        "node_state": "projected",
        "data": {"name": name, "is_folder": True},
    }


def test_infer_anchor_empty_and_no_single_folder():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    assert mw._destination_infer_single_library_folder_anchor_path({}) == ""
    assert mw._destination_infer_single_library_folder_anchor_path({"Root": {}}) == ""


def test_infer_anchor_one_real_root_child_folder():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    nodes = {
        "Root": {"parent_semantic_path": "", "node_state": "real", "data": {"name": "Root", "is_folder": True}},
        "Root\\RootTest2": _real_folder_node("RootTest2"),
    }
    assert mw._destination_infer_single_library_folder_anchor_path(nodes) == "Root\\RootTest2"


def test_infer_anchor_two_real_root_folders_returns_empty():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    nodes = {
        "Root": {"parent_semantic_path": "", "node_state": "real", "data": {"name": "Root", "is_folder": True}},
        "Root\\A": _real_folder_node("A"),
        "Root\\B": _real_folder_node("B"),
    }
    assert mw._destination_infer_single_library_folder_anchor_path(nodes) == ""


def test_infer_anchor_ignores_projected_sibling_and_non_folders():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    nodes = {
        "Root": {"parent_semantic_path": "", "node_state": "real", "data": {"name": "Root", "is_folder": True}},
        "Root\\RootTest2": _real_folder_node("RootTest2"),
        "Root\\Finance": _projected_folder_node("Finance"),
        "Root\\readme.txt": {
            "parent_semantic_path": "Root",
            "node_state": "real",
            "data": {"name": "readme.txt", "is_folder": False},
        },
    }
    assert mw._destination_infer_single_library_folder_anchor_path(nodes) == "Root\\RootTest2"


def test_reanchor_prefixes_under_anchor():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    anchor = "Root\\RootTest2"
    assert mw._destination_reanchor_sharepoint_projection_path("", anchor) == ""
    assert mw._destination_reanchor_sharepoint_projection_path("Root\\Finance", anchor) == "Root\\RootTest2\\Finance"
    assert mw._destination_reanchor_sharepoint_projection_path("Root\\RootTest2\\Finance", anchor) == "Root\\RootTest2\\Finance"
    assert mw._destination_reanchor_sharepoint_projection_path("Root", anchor) == "Root"


def test_reanchor_no_op_for_non_root_paths():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    anchor = "Root\\RootTest2"
    assert mw._destination_reanchor_sharepoint_projection_path("Other\\Finance", anchor) == "Other\\Finance"


def test_visible_top_level_paths_promotes_root_children_for_sharepoint():
    """SharePoint planning hides the internal semantic Root row; library children become top-level rows."""
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    nodes = {
        "Root": {"parent_semantic_path": "", "children": ["Root\\Zeta", "Root\\Alpha"]},
        "Root\\Alpha": {"parent_semantic_path": "Root"},
        "Root\\Zeta": {"parent_semantic_path": "Root"},
    }
    out = mw._destination_future_model_visible_top_level_paths(nodes, ["Root"])
    assert out == ["Root\\Alpha", "Root\\Zeta"]


def test_visible_top_level_paths_local_mode_keeps_root_row():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._destination_browse_mode = "local"
    nodes = {
        "Root": {"parent_semantic_path": "", "children": ["Root\\Alpha"]},
        "Root\\Alpha": {"parent_semantic_path": "Root"},
    }
    assert mw._destination_future_model_visible_top_level_paths(nodes, ["Root"]) == ["Root"]


def test_visible_top_level_paths_sharepoint_never_includes_semantic_root_even_with_sibling_top_level():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    nodes = {
        "Root": {"parent_semantic_path": "", "children": []},
        "Other": {"parent_semantic_path": ""},
    }
    out = mw._destination_future_model_visible_top_level_paths(nodes, ["Root", "Other"])
    norms = {mw.normalize_memory_path(x) for x in out}
    assert mw.normalize_memory_path("Root") not in norms
    assert "Other" in out


def test_visible_top_level_paths_sharepoint_merges_root_children_without_visible_root_row():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    nodes = {
        "Root": {"parent_semantic_path": "", "children": ["Root\\RootTest2", "Root\\Zeta"]},
        "Root\\RootTest2": {"parent_semantic_path": "Root"},
        "Root\\Zeta": {"parent_semantic_path": "Root"},
        "Other": {"parent_semantic_path": ""},
    }
    out = mw._destination_future_model_visible_top_level_paths(nodes, ["Root", "Other"])
    norms = {mw.normalize_memory_path(x) for x in out}
    assert mw.normalize_memory_path("Root") not in norms
    assert "Other" in out
    assert mw.normalize_memory_path("Root\\RootTest2") in norms
    assert mw.normalize_memory_path("Root\\Zeta") in norms
