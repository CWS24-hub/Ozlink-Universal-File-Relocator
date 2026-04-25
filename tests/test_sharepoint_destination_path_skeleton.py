"""SharePoint destination path model: live Graph skeleton is the single structural coordinate system."""

from __future__ import annotations

import os
from unittest.mock import MagicMock

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from ozlink_console.main_window import MainWindow, destination_projection_segments_explicit
from ozlink_console.planned_move_graph_resolve import (
    allocation_path_to_drive_relative,
    drive_relative_path_candidates,
    enrich_single_planned_move,
)


def test_graph_skeleton_segment_root3_preserved_in_projection_segments():
    assert destination_projection_segments_explicit(r"Root3\Finance\Payroll", []) == [
        "Root3",
        "Finance",
        "Payroll",
    ]
    assert destination_projection_segments_explicit(r"Root3\Sales\Pictures", []) == [
        "Root3",
        "Sales",
        "Pictures",
    ]
    assert destination_projection_segments_explicit(r"Root3\Projects\Completed Projects", []) == [
        "Root3",
        "Projects",
        "Completed Projects",
    ]


def test_legacy_internal_root_stripped_not_real_root3():
    assert destination_projection_segments_explicit(r"Root\Finance\Payroll", []) == [
        "Finance",
        "Payroll",
    ]


def test_inner_segment_named_root_not_sliced_as_legacy_root():
    assert destination_projection_segments_explicit(r"Root3\root\SubFolder", []) == [
        "Root3",
        "root",
        "SubFolder",
    ]


def test_allocation_path_preserves_root3_without_anchor():
    rel = allocation_path_to_drive_relative(r"Root3\Finance\Payroll")
    assert rel == "Root3/Finance/Payroll"


def test_allocation_path_prepends_visible_hub_when_omitted():
    rel = allocation_path_to_drive_relative(
        r"Finance\Payroll",
        visible_library_anchor="Root3",
    )
    assert rel == "Root3/Finance/Payroll"


def test_allocation_path_idempotent_when_already_under_hub():
    rel = allocation_path_to_drive_relative(
        r"Root3\Finance\Payroll",
        visible_library_anchor="Root3",
    )
    assert rel == "Root3/Finance/Payroll"


def test_drive_relative_restrict_single_candidate_is_anchored():
    c = drive_relative_path_candidates(
        r"Finance\Payroll",
        visible_library_anchor="Root3",
        restrict_to_live_graph_skeleton=True,
    )
    assert c == ["Root3/Finance/Payroll"]


def test_drive_relative_missing_parent_path_not_degraded_when_restricted():
    """With skeleton restriction, we never emit a library-root ``Finance/...`` alternate."""
    c = drive_relative_path_candidates(
        r"Root3\Finance\Payroll",
        visible_library_anchor="Root3",
        restrict_to_live_graph_skeleton=True,
    )
    assert c == ["Root3/Finance/Payroll"]
    assert "Finance/Payroll" not in c


def test_root3_finance_payroll_never_emits_finance_payroll_candidate_under_skeleton():
    """Graph-authority-style restriction: no de-anchored Finance/Payroll alternate."""
    c = drive_relative_path_candidates(
        r"Root3\Finance\Payroll",
        visible_library_anchor="Root3",
        restrict_to_live_graph_skeleton=True,
    )
    assert c == ["Root3/Finance/Payroll"]
    for alt in c:
        assert not alt.startswith("Finance/")
        assert "Finance/Payroll" != alt


def test_enrich_single_planned_move_resolves_via_hub_anchored_parent_only():
    """Graph parent lookup uses ``Root3/Finance``, not library-root ``Finance``."""

    def get_item_by_path(drive: str, rel: str):
        r = str(rel or "").replace("\\", "/").strip("/")
        if drive == "d-dst" and r == "Root3/Finance":
            return {"id": "PARENT-UNDER-HUB"}
        if drive == "d-dst" and r == "Finance":
            return {"id": "WRONG-ROOT"}
        return None

    def get_root_item(drive: str):
        return {"id": "ROOT"} if drive == "d-dst" else None

    move = {
        "source_path": "S/a.jpg",
        "destination_path": r"Finance\file.pdf",
        "source_id": "SRC-1",
        "destination_id": "",
        "source": {"id": "SRC-1", "drive_id": "d-src"},
        "destination": {"id": "", "drive_id": "d-dst", "name": ""},
    }
    enrich_single_planned_move(
        move,
        get_item_by_path=get_item_by_path,
        get_root_item=get_root_item,
        source_drive_id="d-src",
        source_library_name="Lib",
        dest_drive_id="d-dst",
        dest_library_name="Lib",
        visible_library_anchor_destination="Root3",
        sharepoint_graph_authority_destination=True,
    )
    assert move["destination_id"] == "PARENT-UNDER-HUB"


def test_graph_resolve_ensure_drive_folder_path_does_not_call_graph_mkdir():
    """Planning helper must not create folders under the library root (execution phase only)."""
    win = MainWindow.__new__(MainWindow)
    graph = MagicMock()
    graph.get_drive_root_item = MagicMock()
    graph.create_child_folder = MagicMock()
    graph.get_drive_item_by_path = MagicMock()
    ok = win._graph_resolve_ensure_drive_folder_path(
        graph,
        "drive-1",
        "Finance/Payroll",
        dest_library_name="Documents",
        dest_site_name="",
        mkdir_session_verified_prefixes=set(),
    )
    assert ok is False
    graph.create_child_folder.assert_not_called()
    graph.get_drive_root_item.assert_not_called()
    graph.get_drive_item_by_path.assert_not_called()


def test_enrich_graph_authority_without_anchor_does_not_query_library_root_parent():
    attempted: list[str] = []

    def get_item_by_path(drive: str, rel: str):
        attempted.append(str(rel).replace("\\", "/").strip("/"))
        return {"id": "SHOULD-NOT-USE"}

    def get_root_item(drive: str):
        return {"id": "ROOT"}

    move = {
        "source_path": "S/a.jpg",
        "destination_path": r"Finance\file.pdf",
        "source_id": "SRC-1",
        "destination_id": "",
        "source": {"id": "SRC-1", "drive_id": "d-src"},
        "destination": {"id": "", "drive_id": "d-dst", "name": ""},
    }
    enrich_single_planned_move(
        move,
        get_item_by_path=get_item_by_path,
        get_root_item=get_root_item,
        source_drive_id="d-src",
        source_library_name="Lib",
        dest_drive_id="d-dst",
        dest_library_name="Lib",
        visible_library_anchor_destination="",
        sharepoint_graph_authority_destination=True,
    )
    assert str(move.get("destination_id") or "").strip() == ""
    assert attempted == []


def test_reconcile_graph_enforcement_runtime_always_disabled():
    win = MainWindow.__new__(MainWindow)
    assert win._destination_reconcile_graph_enforcement_runtime_enabled() is False


def test_reconcile_graph_authoritative_enforcement_is_noop():
    from PySide6.QtCore import QModelIndex

    win = MainWindow.__new__(MainWindow)
    out = win._destination_reconcile_graph_authoritative_enforcement_for_path(
        enforce_full_canon="Root3/Finance/Payroll",
        snap={
            "is_folder": True,
            "row_kind": "planned_file",
            "planning_uuid": "u1",
        },
        parent_col0=QModelIndex(),
    )
    assert out == {"corrections": 0, "verified": False, "error": ""}
