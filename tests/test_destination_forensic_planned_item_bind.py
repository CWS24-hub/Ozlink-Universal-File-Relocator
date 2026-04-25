"""Forensic logging for planned/proposed bind: enable with OZLINK_DESTINATION_FORENSIC_PLANNED_ITEM_BIND=1."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication, QTreeView

from ozlink_console import main_window as mw_mod
from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _app():
    return QApplication.instance() or QApplication([])


@pytest.fixture
def forensic_log_capture(monkeypatch):
    events: list[tuple[str, dict]] = []

    def _cap(msg: str, **data):
        events.append((msg, dict(data)))

    monkeypatch.setattr(mw_mod, "log_info", _cap)
    return events


def _minimal_graph_mw(monkeypatch):
    monkeypatch.setattr(
        "ozlink_console.destination_authority_contract.graph_owns_visible_real_destination_structure",
        lambda _h: True,
    )
    _app()
    win = MainWindow.__new__(MainWindow)
    win._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    dm = DestinationPlanningTreeModel(
        destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(win, MainWindow)
    )
    win.destination_planning_model = dm
    tw = QTreeView()
    tw.setModel(dm)
    win.destination_tree_widget = tw
    win.unresolved_proposed_by_parent_path = {}
    win.unresolved_allocations_by_parent_path = {}
    win.proposed_folders = []
    root = {
        "name": "Contractor Resumes",
        "id": "",
        "is_folder": True,
        "item_path": r"Root3\HR\Employee Files\Contractor Resumes",
        "destination_path": r"Root3\HR\Employee Files\Contractor Resumes",
        "tree_role": "destination",
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "workspace_planned_row": True,
        "children_loaded": False,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(win, None, root)
    dm.reset_root_payloads([root])
    ix = dm.index(0, 0, QModelIndex())
    win.planned_moves = [
        {
            "destination_path": r"Root3\HR\Employee Files\Contractor Resumes\Report.pdf",
            "source_path": r"S\Report.pdf",
            "source": {"is_folder": False, "name": "Report.pdf"},
            "request_id": "f1",
        }
    ]
    for name in (
        "normalize_memory_path",
        "_canonical_destination_projection_path",
        "_path_segments",
        "_tree_item_path",
        "_destination_parent_match_details",
        "_paths_equivalent",
        "_get_unresolved_candidates_for_parent",
        "_proposed_parent_path",
        "_proposed_destination_path",
        "_proposed_folder_key",
        "_materialize_planned_workspace_proposed_descendants_fixpoint",
        "_apply_proposed_children_to_model_index",
    ):
        setattr(win, name, getattr(MainWindow, name).__get__(win, MainWindow))
    return win, dm, ix


def test_forensic_logs_planned_files_without_proposed_queue(monkeypatch, forensic_log_capture):
    """Branch Root3\\HR\\Employee Files\\Contractor Resumes [Planned] + planned_move file: gap is explicit."""
    monkeypatch.setenv("OZLINK_DESTINATION_FORENSIC_PLANNED_ITEM_BIND", "1")
    win, _dm, ix = _minimal_graph_mw(monkeypatch)
    MainWindow._apply_proposed_children_to_model_index(win, ix)
    msgs = [m for m, _d in forensic_log_capture]
    assert "destination_forensic_proposed_bind_enter" in msgs
    assert "destination_forensic_proposed_bind_skip" in msgs
    skip = next(d for m, d in forensic_log_capture if m == "destination_forensic_proposed_bind_skip")
    assert skip.get("skip_reason") == "parent_has_planned_move_files_not_in_proposed_folder_queue"
    assert "Report.pdf" in str(skip.get("planned_file_paths_in_planning_state") or "")
    enter = next(d for m, d in forensic_log_capture if m == "destination_forensic_proposed_bind_enter")
    assert int(enter.get("planned_move_file_destinations_under_parent_count") or 0) >= 1


def test_forensic_planned_file_paths_helper_matches_move(monkeypatch):
    win, _dm, _ix = _minimal_graph_mw(monkeypatch)
    parent = r"Root3\HR\Employee Files\Contractor Resumes"
    files = MainWindow._forensic_planned_file_destination_paths_under_folder(win, parent)
    assert any(str(p).endswith("Report.pdf") for p in files)


def test_forensic_planned_file_helper_anchors_relative_destination_under_graph_hub(monkeypatch):
    """Persisted ``destination_path`` may omit the visible library hub (e.g. HR\\... vs Root3\\HR\\...)."""
    win, _dm, _ix = _minimal_graph_mw(monkeypatch)
    win._destination_visible_library_anchor_canonical_path = lambda: "Root3"
    win.planned_moves = [
        {
            "destination_path": r"HR\Employee Files\Contractor Resumes\Report.pdf",
            "source_path": r"S\Report.pdf",
            "source": {"is_folder": False, "name": "Report.pdf"},
            "request_id": "f1",
        }
    ]
    parent = r"Root3\HR\Employee Files\Contractor Resumes"
    files = MainWindow._forensic_planned_file_destination_paths_under_folder(win, parent)
    assert any(str(p).endswith("Report.pdf") for p in files)
