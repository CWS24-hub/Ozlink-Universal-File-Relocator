"""File vs folder for planned moves when ``source.is_folder`` is missing (extension fallback)."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication

from ozlink_console.destination_overlay_layer import overlay_row_marker
from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel
from ozlink_console.tree_models.explorer_columns import planned_leaf_filename_implies_document_file


def _app():
    return QApplication.instance() or QApplication([])


def test_planned_leaf_filename_implies_document_file_docx():
    assert planned_leaf_filename_implies_document_file("Contractor bank.docx")
    assert not planned_leaf_filename_implies_document_file("Employee Files")


def _stub_mw(monkeypatch):
    monkeypatch.setattr(
        "ozlink_console.destination_authority_contract.graph_owns_visible_real_destination_structure",
        lambda _h: True,
    )
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw.planned_moves = []
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw.destination_planning_model = DestinationPlanningTreeModel(
        destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow)
    )
    for n in (
        "normalize_memory_path",
        "_canonical_destination_projection_path",
        "_canonical_planned_memory_path_for_graph_match",
        "_canonical_destination_path_with_visible_library_anchor",
        "_path_segments",
        "_tree_item_path",
        "_destination_parent_match_details",
        "_destination_row_raw_path_for_path_lookup_match",
        "_destination_semantic_path",
        "_destination_resolution_rank",
        "_select_canonical_destination_item",
        "_destination_sibling_folder_dedup_key",
        "_destination_sibling_folder_collision_key",
        "_destination_find_child_by_last_segment_folder_name",
        "_destination_has_future_descendants",
        "_destination_payload_index_key",
        "_destination_model_index_user_role_dict",
        "_refresh_destination_item_visibility_index",
        "_tree_name_column_label",
        "_apply_tree_item_visual_state",
    ):
        setattr(mw, n, getattr(MainWindow, n).__get__(mw, MainWindow))
    mw._find_destination_child_by_path = MainWindow._find_destination_child_by_path.__get__(mw, MainWindow)
    mw._memory_restore_in_progress = False
    mw._suppress_selector_change_handlers = False
    mw.isVisible = lambda: False
    mw._log_restore_phase = lambda *a, **k: None
    return mw


def test_forensic_planned_file_includes_move_without_is_folder_docx(monkeypatch):
    mw = _stub_mw(monkeypatch)
    mw.planned_moves = [
        {
            "destination_path": r"Hub\HR\Contractor bank.docx",
            "target_name": "Contractor bank.docx",
            "source": {"name": "Contractor bank.docx"},
        }
    ]
    paths = mw._forensic_planned_file_destination_paths_under_folder(r"Hub\HR")
    assert any("Contractor bank.docx" in p for p in paths)


def test_forensic_planned_folder_excludes_ambiguous_docx_move(monkeypatch):
    mw = _stub_mw(monkeypatch)
    mw.planned_moves = [
        {
            "destination_path": r"Hub\HR\Contractor bank.docx",
            "target_name": "Contractor bank.docx",
            "source": {"name": "Contractor bank.docx"},
        }
    ]
    paths = mw._forensic_planned_folder_move_destination_paths_under_folder(r"Hub\HR")
    assert not any("Contractor bank.docx" in p for p in paths)


def test_exact_target_enforcement_terminal_is_file_without_is_folder(monkeypatch):
    mw = _stub_mw(monkeypatch)
    mw.planned_moves = [
        {
            "destination_path": r"Hub\HR\Contractor bank.docx",
            "target_name": "Contractor bank.docx",
            "source": {"name": "Contractor bank.docx"},
        }
    ]
    ic = mw._canonical_planned_memory_path_for_graph_match(r"Hub\HR\Contractor bank.docx")
    assert mw._destination_enforcement_terminal_is_file_for_intended_path(ic)
    m = mw._destination_enforcement_terminal_is_file_lookup_map()
    assert m.get(ic.casefold()) is True


def test_enforcement_terminal_is_file_lookup_map_matches_linear_scan(monkeypatch):
    mw = _stub_mw(monkeypatch)
    mw.planned_moves = [
        {"destination_path": r"A\F.txt", "target_name": "F.txt", "source": {"name": "F.txt"}},
        {"destination_path": r"A\Sub", "target_name": "Sub", "source": {"name": "Sub"}},
    ]
    m = mw._destination_enforcement_terminal_is_file_lookup_map()
    for move in mw.planned_moves:
        proj = mw._allocation_projection_path(move)
        c = mw._canonical_destination_projection_path(proj) or mw.normalize_memory_path(proj)
        if not c:
            continue
        assert m.get(c.casefold()) == mw._destination_enforcement_terminal_is_file_for_intended_path(c)


def test_bind_reclassifies_existing_planned_folder_to_file_on_terminal_hit(monkeypatch):
    mw = _stub_mw(monkeypatch)
    dm = mw.destination_planning_model
    hub = {
        "name": "Hub",
        "id": "live-hub",
        "is_folder": True,
        "item_path": "Hub",
        "destination_path": "Hub",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, hub)
    dm.reset_root_payloads([hub])
    hub_ix = dm.index(0, 0, QModelIndex())
    hr = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "workspace_planned_row": True,
        "planning_uuid": "hr-u1",
        "id": "",
        "graph_item_id": "",
        "name": "HR",
        "item_path": "Hub\\HR",
        "destination_path": "Hub\\HR",
        "tree_role": "destination",
        "is_folder": True,
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
        **overlay_row_marker("planned_workspace"),
    }
    MainWindow._apply_tree_item_visual_state(mw, None, hr)
    dm.append_child_payloads(hub_ix, [hr])
    hr_ix = dm.index(0, 0, hub_ix)
    wrong_leaf = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "workspace_planned_row": True,
        "planning_uuid": "leaf-u1",
        "id": "",
        "graph_item_id": "",
        "name": "Contractor bank.docx",
        "item_path": "Hub\\HR\\Contractor bank.docx",
        "destination_path": "Hub\\HR\\Contractor bank.docx",
        "tree_role": "destination",
        "is_folder": True,
        "children_loaded": False,
        "drive_id": "d1",
        "library_id": "d1",
        **overlay_row_marker("planned_workspace"),
    }
    MainWindow._apply_tree_item_visual_state(mw, None, wrong_leaf)
    dm.append_child_payloads(hr_ix, [wrong_leaf])
    leaf = MainWindow._sharepoint_bind_planned_segment_chain(
        mw,
        hr_ix,
        ["Contractor bank.docx"],
        bind_kind="exact_target_enforcement",
        bind_context_excerpt="test",
        projection_target_canonical="Hub\\HR\\Contractor bank.docx",
        terminal_is_file=True,
    )
    assert leaf is not None and leaf.isValid()
    pl = dict(leaf.data(Qt.UserRole) or {})
    assert pl.get("row_kind") == "planned_file"
    assert pl.get("is_folder") is False
    assert pl.get("planning_uuid") == "leaf-u1"
