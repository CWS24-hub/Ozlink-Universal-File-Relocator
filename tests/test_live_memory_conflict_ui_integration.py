"""Deeper integration-style tests for live_duplicate_proposed_folder (no live Graph)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ozlink_console.destination_live_memory_conflicts import REVIEW_TYPE_LIVE_MEMORY_DUPLICATE
from ozlink_console.main_window import MainWindow
from ozlink_console.models import ProposedFolder


def _conflict_row():
    return {
        "review_type": REVIEW_TYPE_LIVE_MEMORY_DUPLICATE,
        "live_memory_subtype": "live_duplicate_proposed_folder",
        "source_path": r"Root3\HR",
        "reason": "conflict",
        "item_name": "x",
        "action": r"Root3\HR",
        "live_graph_path": r"Root3\HR",
        "live_item_id": "lid",
    }


def test_a_needs_review_includes_live_memory_duplicate_row() -> None:
    mw = MainWindow.__new__(MainWindow)
    mw._draft_shell_state = None
    mw._iter_visible_source_nodes = lambda: []
    mw.normalize_memory_path = lambda p: str(p or "").replace("/", "\\")  # type: ignore[method-assign]
    mw._runtime_live_memory_conflict_rows = [_conflict_row()]
    mw.planned_moves = []
    mw.proposed_folders = []
    mw._migration_import_needs_review_rows = []
    st = MainWindow._compute_planning_workflow_state(mw)
    rows = st.get("needs_review_rows") or []
    assert any(
        isinstance(r, dict) and str(r.get("review_type") or "") == REVIEW_TYPE_LIVE_MEMORY_DUPLICATE for r in rows
    )


def test_b_activate_opens_dialog(monkeypatch: pytest.MonkeyPatch) -> None:
    mw = MainWindow.__new__(MainWindow)
    opened = {"n": 0}

    def _show(_self, _row):
        opened["n"] += 1

    monkeypatch.setattr(MainWindow, "_show_live_duplicate_proposed_folder_resolution_dialog", _show)
    MainWindow._activate_workflow_source_row(mw, _conflict_row())
    assert opened["n"] == 1


def test_c_accept_updates_audit_and_removes_conflict(monkeypatch: pytest.MonkeyPatch) -> None:
    mw = MainWindow.__new__(MainWindow)
    mw._application_shutting_down = False
    mw.memory_manager = MagicMock()
    mw.graph = MagicMock()
    mw.proposed_folders = [ProposedFolder(FolderName="HR", DestinationPath=r"Root3\HR")]
    row = _conflict_row()
    mw._runtime_live_memory_conflict_rows = [dict(row)]
    mw._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\")  # type: ignore[method-assign]
    mw._refresh_workflow_state_on_demand = lambda: None  # type: ignore[method-assign]
    mw._refresh_proposed_folders_table = lambda: None  # type: ignore[method-assign]
    mw._on_destination_state_mutation = lambda *a, **k: None  # type: ignore[method-assign]
    mw._notify_planning_mutation_destination_snapshot_dirty = MagicMock()  # type: ignore[method-assign]
    MainWindow._resolve_live_duplicate_proposed_folder_accept_existing(mw, row)
    assert mw._runtime_live_memory_conflict_rows == []
    pf = mw.proposed_folders[0]
    assert str(getattr(pf, "LiveMemoryDuplicateResolution", "") or "").strip()


def test_d_remove_when_no_dependents(monkeypatch: pytest.MonkeyPatch) -> None:
    mw = MainWindow.__new__(MainWindow)
    mw._application_shutting_down = False
    mw.memory_manager = MagicMock()
    mw.proposed_folders = [ProposedFolder(FolderName="HR", DestinationPath=r"Root3\HR")]
    mw.planned_moves = []
    row = _conflict_row()
    mw._runtime_live_memory_conflict_rows = [dict(row)]
    mw._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\")  # type: ignore[method-assign]
    mw._planned_moves_dependent_on_proposed_destination = lambda _dp: []  # type: ignore[method-assign]
    mw._refresh_workflow_state_on_demand = lambda: None  # type: ignore[method-assign]
    mw._refresh_proposed_folders_table = lambda: None  # type: ignore[method-assign]
    mw._on_destination_state_mutation = lambda *a, **k: None  # type: ignore[method-assign]
    mw._notify_planning_mutation_destination_snapshot_dirty = MagicMock()  # type: ignore[method-assign]
    MainWindow._resolve_live_duplicate_proposed_folder_remove_proposed(mw, row)
    assert mw.proposed_folders == []
    assert mw._runtime_live_memory_conflict_rows == []


def test_e_remove_blocks_when_dependents(monkeypatch: pytest.MonkeyPatch) -> None:
    mw = MainWindow.__new__(MainWindow)
    mw._application_shutting_down = False
    mw.memory_manager = MagicMock()
    mw.proposed_folders = [ProposedFolder(FolderName="HR", DestinationPath=r"Root3\HR")]
    row = _conflict_row()
    mw._runtime_live_memory_conflict_rows = [dict(row)]
    mw._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\")  # type: ignore[method-assign]
    mw._planned_moves_dependent_on_proposed_destination = lambda _dp: [{"destination_path": r"x"}]  # type: ignore[method-assign]
    monkeypatch.setattr("ozlink_console.main_window.QMessageBox.warning", lambda *_a, **_k: None)
    ok = MainWindow._resolve_live_duplicate_proposed_folder_remove_proposed(mw, row)
    assert ok is False
    assert len(mw.proposed_folders) == 1


def test_f_rename_runs_rewrite_path(monkeypatch: pytest.MonkeyPatch) -> None:
    mw = MainWindow.__new__(MainWindow)
    mw._application_shutting_down = False
    mw.memory_manager = MagicMock()
    mw.graph = MagicMock()
    mw.proposed_folders = [ProposedFolder(FolderName="HR", DestinationPath=r"Root3\HR")]
    mw.planned_moves = []
    mw.destination_tree_status = MagicMock()
    row = _conflict_row()
    mw._runtime_live_memory_conflict_rows = [dict(row)]
    mw._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\")  # type: ignore[method-assign]
    mw.normalize_memory_path = lambda p: str(p or "").replace("/", "\\")  # type: ignore[method-assign]
    mw._destination_parent_path = lambda p: "Root3" if "HR" in str(p) else ""  # type: ignore[method-assign]
    mw._find_visible_destination_item_by_path = lambda _p: None  # type: ignore[method-assign]
    mw._path_segments = MainWindow._path_segments.__get__(mw, MainWindow)  # type: ignore[method-assign]
    mw._refresh_workflow_state_on_demand = lambda: None  # type: ignore[method-assign]
    mw._refresh_proposed_folders_table = lambda: None  # type: ignore[method-assign]
    mw._refresh_planning_derived_state = lambda *_a: None  # type: ignore[method-assign]
    mw._on_destination_state_mutation = lambda *a, **k: None  # type: ignore[method-assign]
    mw._notify_planning_mutation_destination_snapshot_dirty = MagicMock()  # type: ignore[method-assign]
    before = str(mw.proposed_folders[0].DestinationPath or "").casefold()
    monkeypatch.setattr(
        "ozlink_console.main_window.QInputDialog.getText",
        lambda *a, **k: ("HR2", True),
    )
    MainWindow._live_duplicate_proposed_folder_run_rename(mw, row)
    after = str(mw.proposed_folders[0].DestinationPath or "").casefold()
    assert after != before
    assert mw._runtime_live_memory_conflict_rows == []


def test_h_conflict_persists_until_action() -> None:
    mw = MainWindow.__new__(MainWindow)
    mw._draft_shell_state = None
    mw._iter_visible_source_nodes = lambda: []
    mw.normalize_memory_path = lambda p: str(p or "").replace("/", "\\")  # type: ignore[method-assign]
    mw._runtime_live_memory_conflict_rows = [_conflict_row()]
    mw.planned_moves = []
    mw.proposed_folders = [ProposedFolder(FolderName="HR", DestinationPath=r"Root3\HR")]
    mw._migration_import_needs_review_rows = []
    st = MainWindow._compute_planning_workflow_state(mw)
    assert st["needs_review_count"] >= 1


def test_i_readiness_needs_attention_while_unresolved_conflict() -> None:
    mw = MainWindow.__new__(MainWindow)
    mw._cached_duplicate_destination_groups = {}
    mw._workflow_needs_review_rows = [_conflict_row()]
    mw.planned_moves = []
    mw.proposed_folders = []
    mw._needs_review_dismissed_inherited_paths = set()
    mw._plan_leaf_exclusions = []
    MainWindow._refresh_planning_derived_state(mw, "test")
    st = getattr(mw, "_planning_derived_state", None)
    assert st is not None
    assert st.readiness_state == "needs_attention"


def test_j_readiness_updates_after_conflict_cleared(monkeypatch: pytest.MonkeyPatch) -> None:
    mw = MainWindow.__new__(MainWindow)
    mw._cached_duplicate_destination_groups = {}
    mw.planned_moves = []
    mw.proposed_folders = []
    mw._needs_review_dismissed_inherited_paths = set()
    mw._plan_leaf_exclusions = []
    mw._workflow_needs_review_rows = [_conflict_row()]
    MainWindow._refresh_planning_derived_state(mw, "a")
    assert getattr(mw, "_planning_derived_state").readiness_state == "needs_attention"
    mw._workflow_needs_review_rows = []
    MainWindow._refresh_planning_derived_state(mw, "b")
    assert getattr(mw, "_planning_derived_state").readiness_state == "ready"


def test_k_retarget_begins_only_for_dependent_file_moves(monkeypatch: pytest.MonkeyPatch) -> None:
    mw = MainWindow.__new__(MainWindow)
    mw._live_memory_retarget_pending_indices = []
    mw._live_memory_retarget_tree_hook = []
    monkeypatch.setattr("ozlink_console.main_window.QMessageBox.information", lambda *_a, **_k: None)
    log: list[str] = []

    def _log(msg: str, **_kw) -> None:
        log.append(msg)

    monkeypatch.setattr("ozlink_console.main_window.log_info", _log)
    m0 = {"source_path": "a", "destination_path": r"Root3\HR\f.txt", "source": {"is_folder": False}}
    m1 = {"source_path": "b", "destination_path": r"Root9\Other\f.txt", "source": {"is_folder": False}}
    mw.planned_moves = [m0, m1]
    mw.proposed_folders = [ProposedFolder(FolderName="HR", DestinationPath=r"Root3\HR")]
    mw.destination_tree_widget = None
    mw.normalize_memory_path = lambda p: str(p or "").replace("/", "\\")  # type: ignore[method-assign]
    mw._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\")  # type: ignore[method-assign]

    def _deps(_dp):
        return [m0]

    mw._planned_moves_dependent_on_proposed_destination = _deps  # type: ignore[method-assign]
    MainWindow._live_duplicate_proposed_folder_begin_retarget(mw, _conflict_row())
    MainWindow._end_live_memory_duplicate_retarget_session(mw, cancelled=True)
    assert "destination_live_memory_duplicate_retarget_started" in log
