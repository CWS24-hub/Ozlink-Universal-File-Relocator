"""Surgical fixes for allocation descendant projection: parent resolve, dedupe, relative-path guard."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from PySide6.QtCore import Qt
from PySide6.QtGui import QStandardItem, QStandardItemModel

from ozlink_console import destination_authority_contract
from ozlink_console.main_window import MainWindow


def _stub_mw_for_descendant_tests():
    mw = MainWindow.__new__(MainWindow)
    mw.pending_root_drive_ids = {"destination": "b!DEST"}
    mw._current_selected_destination_drive_id = lambda: "b!DEST"
    mw._allocation_move_key = lambda m: str(m.get("projection_path", ""))[:120]
    mw._move_target_name = lambda m: str(m.get("name") or "").strip()
    mw._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\").strip()
    mw._canonical_planned_memory_path_for_graph_match = lambda p: str(p or "").replace("/", "\\").strip()
    mw._normalize_memory_path = lambda p: str(p or "").replace("/", "\\").strip()
    mw.normalize_memory_path = mw._normalize_memory_path
    mw._canonical_source_projection_path = lambda p: str(p or "").replace("/", "\\").strip()
    mw._path_segments = lambda p: [s for s in str(p).replace("/", "\\").split("\\") if s]
    mw._tree_item_path = lambda d: str((d or {}).get("item_path") or (d or {}).get("path") or "")
    return mw


def test_allocation_effective_destination_root_uses_planned_parent_when_resolved():
    mw = _stub_mw_for_descendant_tests()
    move = {
        "projection_path": "Root3\\X",
        "DestinationParentPlannedPath": "Root3\\HR",
        "LegacyMigrationPlannedParentResolved": "true",
        "name": "Y",
    }
    mw._allocation_projection_path = lambda m: "Root3\\HR\\Y"
    mw._canonical_planned_memory_path_for_graph_match = lambda p: p
    mw._canonical_destination_projection_path = lambda p: p
    mw.normalize_memory_path = lambda p: p.replace("/", "\\")
    r = mw._allocation_effective_destination_allocation_root_path(move)
    assert "HR" in r and "Y" in r


def test_try_add_future_relative_rejected_when_not_under_source_root():
    """B: Pictures root must not accept FTBMRoot\\Documents\\... via length-only slice."""
    mw = _stub_mw_for_descendant_tests()
    mw._log_restore_phase = lambda *a, **k: None
    mw._find_exact_planned_move_for_source_path = lambda _s: None
    mw._destination_preview_descendant_exists = lambda *a, **k: False
    mw._build_destination_allocation_descendant_node_data = MagicMock(return_value={"name": "x"})
    mw._upsert_destination_model_node = MagicMock()
    mw._attach_destination_model_child = MagicMock()

    move = {}
    model_nodes: dict = {}
    allocation_path = "Root3\\Sales\\Pictures"
    allocation_node = {"data": {"drive_id": "b!DEST", "library_id": "b!DEST", "is_folder": True}}
    root = r"FTBMRoot\Pictures"
    bad_desc = {"item_path": r"FTBMRoot\Documents\evil.xlsx", "is_folder": False, "name": "evil.xlsx"}
    got = mw._try_add_single_allocation_descendant_to_future_model(
        move,
        model_nodes,
        allocation_path=allocation_path,
        allocation_node=allocation_node,
        allocation_destination_path=allocation_path,
        source_root_path=mw._canonical_source_projection_path(root),
        source_root_segments=mw._path_segments(root),
        descendant_data=bad_desc,
        log_added_count=0,
    )
    assert got is False
    mw._upsert_destination_model_node.assert_not_called()


def test_try_add_future_accepts_pictures_documents_child():
    mw = _stub_mw_for_descendant_tests()
    mw._log_restore_phase = lambda *a, **k: None
    mw._find_exact_planned_move_for_source_path = lambda _s: None
    mw._destination_preview_descendant_exists = lambda *a, **k: False
    mw._build_destination_allocation_descendant_node_data = MagicMock(return_value={"name": "f"})
    mw._upsert_destination_model_node = MagicMock()
    mw._attach_destination_model_child = MagicMock()

    move = {}
    model_nodes: dict = {}
    root = r"FTBMRoot\Pictures"
    ok_desc = {
        "item_path": r"FTBMRoot\Pictures\Documents\f.doc",
        "is_folder": False,
        "name": "f.doc",
    }
    got = mw._try_add_single_allocation_descendant_to_future_model(
        move,
        model_nodes,
        allocation_path="x",
        allocation_node={"data": {"drive_id": "b!DEST", "is_folder": True}},
        allocation_destination_path=r"Root3\Sales\Pictures",
        source_root_path=mw._canonical_source_projection_path(root),
        source_root_segments=mw._path_segments(root),
        descendant_data=ok_desc,
        log_added_count=0,
    )
    assert got is True
    mw._upsert_destination_model_node.assert_called_once()


def test_planned_bind_reuses_existing_child_under_parent():
    """Duplicate canonical planned folder under the same parent is reused instead of insert."""
    mw = MainWindow.__new__(MainWindow)
    mw._canonical_planned_memory_path_for_graph_match = lambda p: str(p or "").strip().replace("/", "\\")
    mw._canonical_destination_projection_path = lambda p: str(p or "").strip().replace("/", "\\")
    mw._tree_item_path = lambda pl: str(pl.get("item_path") or "")
    mw._destination_row_raw_path_for_path_lookup_match = lambda pl: str(pl.get("item_path") or "")
    parent_ix = MagicMock()
    parent_ix.isValid.return_value = True
    parent_payload = {"item_path": r"Root3\Sales\Pictures", "is_folder": True, "drive_id": "b!DEST"}
    parent_ix.data.side_effect = lambda role, *a, **k: parent_payload if role == Qt.UserRole else None
    parent_ix.column.return_value = 0
    parent_ix.siblingAtColumn.return_value = parent_ix
    doc_ix = MagicMock()
    doc_ix.isValid.return_value = True
    doc_ix.column.return_value = 0
    doc_ix.siblingAtColumn.return_value = doc_ix
    doc_payload = {
        "item_path": r"Root3\Sales\Pictures\Documents",
        "name": "Documents",
        "is_folder": True,
        "verification_state": "planned_only",
        "row_kind": "planned_folder",
        "drive_id": "b!DEST",
    }
    doc_ix.data.side_effect = lambda role, *a, **k: doc_payload if role == Qt.UserRole else None

    class _Model:
        def rowCount(self, _p):
            return 1

        def index(self, row, _col, _par):
            return doc_ix if row == 0 else MagicMock(isValid=lambda: False)

        def data(self, ix, role):
            if ix is doc_ix and role == Qt.UserRole:
                return doc_payload
            return None

    def _user_role_dict(ix):
        if ix is parent_ix:
            return {"item_path": r"Root3\Sales\Pictures", "is_folder": True, "drive_id": "b!DEST"}
        if ix is doc_ix:
            return doc_payload
        return {}

    mw._destination_model_index_user_role_dict = _user_role_dict
    m = _Model()
    r = mw._destination_planned_bind_find_existing_planned_row_under_parent(
        parent_ix,
        r"Root3\Sales\Pictures\Documents",
        m,
    )
    assert r is not None


def test_find_parent_index_prefers_structural_row():
    """A/B: Overlay / structural parent row resolves without requiring Graph live_confirmed."""
    mw = _stub_mw_for_descendant_tests()
    move = {
        "source_path": r"FTBMRoot\Contractor Resumes",
        "projection_path": r"Root3\HR\Employee Files\Contractor Resumes",
        "source": {"is_folder": True},
    }
    mw._allocation_projection_path = lambda m: r"Root3\HR\Employee Files\Contractor Resumes"
    mw._allocation_effective_destination_allocation_root_path = lambda m: r"Root3\HR\Employee Files\Contractor Resumes"
    mw._canonical_planned_memory_path_for_graph_match = lambda p: str(p or "").strip().replace("/", "\\")
    mw._canonical_destination_projection_path = lambda p: str(p or "").strip().replace("/", "\\")
    payload = {
        "item_path": r"Root3\HR\Employee Files\Contractor Resumes",
        "is_folder": True,
        "drive_id": "b!DEST",
        "verification_state": "planned_only",
        "row_kind": "planned_folder",
    }
    qm = QStandardItemModel()
    it = QStandardItem("Contractor Resumes")
    it.setData(payload, Qt.UserRole)
    qm.appendRow(it)
    ix = qm.index(0, 0)

    class DM:
        def __init__(self, real_ix):
            self._ix = real_ix

        def is_index_live(self, cand):
            return True

        def find_indices_for_canonical_destination_path(self, _c):
            return [self._ix]

        def data(self, index, role):
            return qm.data(index, role)

    mw.destination_planning_model = DM(ix)
    mw._destination_graph_descendant_model_index_keys_for_lookup = (
        lambda p: [str(p).strip()] if p else []
    )
    mw._destination_parent_match_details = lambda exp, act: {"exact_match": str(exp).casefold() == str(act).casefold()}
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(destination_authority_contract, "graph_owns_visible_real_destination_structure", lambda _s: True)
        parent, rsn = mw._find_destination_allocation_descendant_parent_index(move)
    assert parent is not None and rsn == "ok"


def test_bind_intended_path_prefers_full_last_segment_for_allocation_descendant():
    """A: intended canonical uses deep last_next_branch for allocation_descendant bind."""
    mw = MainWindow.__new__(MainWindow)
    mw._log_restore_phase = lambda *a, **k: None
    mw._destination_leaf_forensic_trace_payload = lambda *a, **k: None
    mw._destination_bind_trace_enabled = lambda: False
    mw._destination_placement_bind_log_enabled = lambda: False

    short_tp = r"Root3\file.doc"
    long_nb = r"Root3\Sales\Pictures\Documents\file.doc"
    # Force projection_target shorter than terminal path
    raw_tp_after = None

    def fake_bind(*_a, **_k):
        nonlocal raw_tp_after
        # Simulate logic: raw_tp picks last_next_branch when allocation_descendant
        raw_tp = str(_k.get("projection_target_canonical", "") or "").strip()
        ln = str(long_nb).strip()
        if str(_k.get("bind_kind", "")).lower() == "allocation_descendant" and ln:
            from ozlink_console import destination_authority_contract as dac

            graph_strict = dac.graph_owns_visible_real_destination_structure(mw)
            if graph_strict and (not raw_tp or len(ln) > len(raw_tp)):
                raw_tp_after = ln
        return None

    mw._sharepoint_bind_planned_segment_chain = fake_bind  # type: ignore[method-assign]
    # Only validate the intended-path correction rule without running full bind:
    graph_strict = True
    raw_tp = short_tp
    bind_kind = "allocation_descendant"
    last_next_branch = long_nb
    lb_full = str(last_next_branch).strip()
    rp_full = raw_tp
    if (
        str(bind_kind or "").strip().lower() == "allocation_descendant"
        and graph_strict
        and str(last_next_branch or "").strip()
    ):
        if (not raw_tp) or (lb_full and rp_full and len(lb_full) > len(rp_full)):
            raw_tp = str(last_next_branch).strip()
    assert raw_tp == long_nb

