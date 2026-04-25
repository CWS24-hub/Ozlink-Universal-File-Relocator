"""Regression tests for restore-session narrow destination real-snapshot overlay scope."""

from __future__ import annotations

from ozlink_console.main_window import MainWindow
from ozlink_console.models import ProposedFolder


def test_planning_relevant_snapshot_includes_descendants_under_prefix():
    mw = MainWindow.__new__(MainWindow)
    relevant = {"Root", "Root\\Lib\\Sub"}
    assert MainWindow._is_destination_real_snapshot_node_planning_relevant(
        mw, {"semantic_path": "Root\\Lib\\Sub\\Deep\\File.txt"}, relevant
    )
    assert not MainWindow._is_destination_real_snapshot_node_planning_relevant(
        mw, {"semantic_path": "Root\\OtherLib\\X"}, relevant
    )


def test_collect_planning_relevant_semantic_paths_unbound():
    pf = ProposedFolder(
        FolderName="ProposedFolder",
        DestinationPath=r"Root\Lib\ProposedFolder",
        ParentPath="",
        Status="draft",
    )

    mw = MainWindow.__new__(MainWindow)
    mw.proposed_folders = [pf]
    mw.planned_moves = [
        {
            "destination_path": r"Root\Lib\AllocParent",
            "target_name": "Moved.txt",
            "source": {},
        }
    ]
    paths = MainWindow._collect_destination_planning_relevant_semantic_paths(mw)
    assert "Root" in paths
    assert any("ProposedFolder" in p for p in paths)
    assert any("AllocParent" in p for p in paths)
    assert any("Moved.txt" in p for p in paths)
