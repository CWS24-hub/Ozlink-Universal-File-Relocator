"""Planning: leaf exclusions vs inherited folder moves (precedence: direct > exclusion > inherited)."""

from __future__ import annotations

import inspect

from ozlink_console.models import SessionState


def test_session_state_plan_leaf_exclusions_defaults_and_from_dict():
    s = SessionState.from_dict({})
    assert s.PlanLeafExclusions == []
    s2 = SessionState.from_dict({"PlanLeafExclusions": [r"lib\a\file.txt"]})
    assert s2.PlanLeafExclusions == [r"lib\a\file.txt"]
    s3 = SessionState.from_dict({"PlanLeafExclusions": None})
    assert s3.PlanLeafExclusions == []


def test_context_menus_define_exclude_include_actions():
    from ozlink_console.main_window import MainWindow

    src = inspect.getsource(MainWindow.show_source_context_menu)
    assert "Exclude from Plan" in src and "Include in Plan" in src
    assert "handle_exclude_leaf_from_plan" in src
    dst = inspect.getsource(MainWindow.show_destination_context_menu)
    assert "Exclude from Plan" in dst and "Include in Plan" in dst


def test_find_inherited_raw_still_finds_when_excluded_but_wrapped_returns_none():
    from unittest.mock import MagicMock

    from ozlink_console.main_window import MainWindow

    mw = MainWindow.__new__(MainWindow)
    mw.planned_moves = [
        {
            "source_path": "p\\folder",
            "source": {
                "display_path": "p\\folder",
                "item_path": "p\\folder",
                "is_folder": True,
                "tree_role": "source",
            },
            "destination_path": "d\\dest",
        }
    ]
    mw._plan_leaf_exclusions = {"p\\folder\\leaf.txt"}
    mw._canonical_source_projection_path = lambda x: str(x or "").replace("/", "\\")
    mw._path_is_descendant = MainWindow._path_is_descendant.__get__(mw, MainWindow)

    raw = MainWindow._find_inherited_planned_move_for_source_path_raw(mw, "p\\folder\\leaf.txt")
    assert raw is not None
    inh = MainWindow._find_inherited_planned_move_for_source_path(mw, "p\\folder\\leaf.txt")
    assert inh is None


def test_find_planned_move_for_destination_respects_exclusion_without_exact_override():
    from ozlink_console.main_window import MainWindow

    mw = MainWindow.__new__(MainWindow)
    mw.planned_moves = [
        {
            "source_path": "p\\folder",
            "source": {
                "display_path": "p\\folder",
                "item_path": "p\\folder",
                "is_folder": True,
                "tree_role": "source",
            },
            "destination_path": "d\\dest",
        }
    ]
    mw._plan_leaf_exclusions = {"p\\folder\\x.bin"}
    mw._canonical_destination_projection_path = lambda x: str(x or "").replace("/", "\\")
    mw._canonical_source_projection_path = lambda x: str(x or "").replace("/", "\\")
    mw._allocation_projection_path = MainWindow._allocation_projection_path.__get__(mw, MainWindow)
    mw._paths_equivalent = MainWindow._paths_equivalent.__get__(mw, MainWindow)
    mw._path_is_descendant = MainWindow._path_is_descendant.__get__(mw, MainWindow)

    node = {
        "is_folder": False,
        "source_path": "p\\folder\\x.bin",
        "display_path": "d\\dest\\x.bin",
        "item_path": "d\\dest\\x.bin",
        "node_origin": "projectedallocationdescendant",
    }
    assert MainWindow._find_planned_move_for_destination_node(mw, node) is None
