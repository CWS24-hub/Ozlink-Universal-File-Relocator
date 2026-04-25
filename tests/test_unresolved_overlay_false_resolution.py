"""False-resolution guards for unresolved proposed/allocation overlay queues."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex

from ozlink_console.main_window import MainWindow
from ozlink_console.models import ProposedFolder
from ozlink_console.unresolved_overlay_queue import UnresolvedQueueRemovalReason


def test_mark_proposed_without_verified_outcome_does_not_pop_queue():
    mw = MainWindow.__new__(MainWindow)
    mw.unresolved_proposed_by_parent_path = {}
    pf = ProposedFolder(FolderName="Child", DestinationPath="Hub\\Child", ParentPath="Hub")
    k = MainWindow._proposed_folder_key(mw, pf)
    pp = MainWindow._proposed_parent_path(mw, pf)
    mw.unresolved_proposed_by_parent_path = {pp: {k: pf}}
    ok = mw._mark_proposed_folder_resolved(
        pf,
        reason=UnresolvedQueueRemovalReason.bound_planned_chain_visible,
        outcome_ix=QModelIndex(),
        expected_target_canonical="Hub\\Child",
        verify_visible_outcome=True,
    )
    assert ok is False
    assert mw._unresolved_proposed_queue_size() == 1


def test_mark_allocation_invalid_index_does_not_pop_when_verify_on():
    mw = MainWindow.__new__(MainWindow)
    move = {"destination_path": "Hub\\f.txt", "source_name": "f.txt", "request_id": "r1"}
    pp = MainWindow._allocation_parent_path(mw, move)
    mw.unresolved_allocations_by_parent_path = {pp: {MainWindow._allocation_move_key(mw, move): move}}
    ok = mw._mark_allocation_resolved(
        move,
        reason=UnresolvedQueueRemovalReason.attached_to_existing_live_row,
        outcome_ix=QModelIndex(),
        expected_target_canonical="Hub\\f.txt",
        verify_visible_outcome=True,
    )
    assert ok is False
    assert mw._unresolved_allocation_queue_size() == 1


def test_user_retarget_marks_allocation_without_graph_verify():
    mw = MainWindow.__new__(MainWindow)
    move = {"destination_path": "Parent\\leaf", "target_name": "leaf", "request_id": "z"}
    pp = MainWindow._allocation_parent_path(mw, move)
    mk = MainWindow._allocation_move_key(mw, move)
    mw.unresolved_allocations_by_parent_path = {pp: {mk: move}}
    ok = mw._mark_allocation_resolved(
        move,
        reason=UnresolvedQueueRemovalReason.user_planned_move_retargeted,
        verify_visible_outcome=False,
    )
    assert ok is True
    assert mw._unresolved_allocation_queue_size() == 0


def test_fingerprint_skip_blocked_when_planning_targets_missing_visible_evidence():
    class Host:
        destination_planning_model = object()

        def __init__(self) -> None:
            self.destination_tree_widget = object()
            self.planned_moves = [{"destination_path": "Hub", "source_name": "n"}]
            self.proposed_folders = []
            self._destination_last_materialized_overlay_fp = "fp"

        def _planning_browse_mode(self, panel_key: str) -> str:
            return "sharepoint" if panel_key == "destination" else "local"

        def _planning_tree_top_level_count(self, _tree) -> int:
            return 1

        def _destination_should_block_idle_full_tree_materialize(self) -> bool:
            return False

        def _current_destination_full_overlay_fingerprint(self, *, force_refresh_snapshot: bool = True) -> str:
            return "fp"

        def _unresolved_proposed_queue_size(self) -> int:
            return 0

        def _unresolved_allocation_queue_size(self) -> int:
            return 0

        def _count_visible_destination_future_state_nodes(self) -> int:
            return 19

        def _destination_unresolved_overlay_parent_visibility_diag(self) -> dict[str, int]:
            return {
                "allocation_parent_visible": 0,
                "allocation_parent_not_visible": 0,
                "proposed_parent_visible": 0,
                "proposed_parent_not_visible": 0,
            }

        def _destination_planning_overlay_targets_missing_visible_evidence(self) -> list[str]:
            return ["Hub\\n"]

        def _log_restore_phase(self, *_a, **_k) -> None:
            return None

        def _set_tree_status_message(self, *_a, **_k) -> None:
            return None

        _destination_materialize_reason_may_skip_without_interrupting_async = (
            MainWindow._destination_materialize_reason_may_skip_without_interrupting_async
        )

    host = Host()
    out = MainWindow._try_skip_redundant_destination_future_model_materialize(host, "deferred_test_reason")
    assert out is None
