"""Overlay fingerprint / steady-state skips must not strand unresolved proposed/allocation replay."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from ozlink_console.main_window import MainWindow


def test_steady_state_not_redundant_when_unresolved_allocation_queued():
    mw = MainWindow.__new__(MainWindow)
    mw._destination_suppress_steady_materialize_skip_once = False
    mw._destination_restore_completed_once = True
    mw.unresolved_allocations_by_parent_path = {"Hub": {"m1": {"request_id": "1"}}}
    mw.unresolved_proposed_by_parent_path = {}
    mw._restore_destination_overlay_pending = True
    mw._current_destination_full_overlay_fingerprint = lambda **k: "same"  # type: ignore[method-assign]
    mw._destination_last_materialized_overlay_fp = "same"
    assert MainWindow._destination_steady_state_full_materialize_redundant(mw) is False


def test_steady_state_not_redundant_when_restore_overlay_pending_flag_set():
    mw = MainWindow.__new__(MainWindow)
    mw._destination_restore_completed_once = True
    mw.unresolved_allocations_by_parent_path = {}
    mw.unresolved_proposed_by_parent_path = {}
    mw._restore_destination_overlay_pending = True
    mw._current_destination_full_overlay_fingerprint = lambda **k: "same"  # type: ignore[method-assign]
    mw._destination_last_materialized_overlay_fp = "same"
    assert MainWindow._destination_steady_state_full_materialize_redundant(mw) is False


def test_steady_state_redundant_when_queues_clear_and_fp_matches():
    mw = MainWindow.__new__(MainWindow)
    mw._destination_restore_completed_once = True
    mw.unresolved_allocations_by_parent_path = {}
    mw.unresolved_proposed_by_parent_path = {}
    mw._restore_destination_overlay_pending = False
    mw._current_destination_full_overlay_fingerprint = lambda **k: "fp"  # type: ignore[method-assign]
    mw._destination_last_materialized_overlay_fp = "fp"
    assert MainWindow._destination_steady_state_full_materialize_redundant(mw) is True


class _TreeStub:
    pass


def test_fingerprint_skip_suppressed_when_unresolved_backlog_even_if_local_destination():
    """Non-SharePoint: same fingerprint must not skip materialize while replay queue is non-empty."""

    class Host:
        def __init__(self) -> None:
            self.destination_tree_widget = _TreeStub()
            self.planned_moves = [{"x": 1}]
            self.proposed_folders = []
            self._destination_last_materialized_overlay_fp = "fp"
            self.logged: list[tuple[str, dict]] = []

        def _planning_browse_mode(self, _key: str) -> str:
            return "local"

        def _planning_tree_top_level_count(self, _tree) -> int:
            return 1

        def _destination_should_block_idle_full_tree_materialize(self) -> bool:
            return False

        def _current_destination_full_overlay_fingerprint(self, *, force_refresh_snapshot: bool = True) -> str:
            return "fp"

        def _unresolved_proposed_queue_size(self) -> int:
            return 0

        def _unresolved_allocation_queue_size(self) -> int:
            return 5

        def _count_visible_destination_future_state_nodes(self) -> int:
            return 19

        def _destination_unresolved_overlay_parent_visibility_diag(self) -> dict[str, int]:
            return {
                "allocation_parent_visible": 2,
                "allocation_parent_not_visible": 3,
                "proposed_parent_visible": 0,
                "proposed_parent_not_visible": 0,
            }

        def _log_restore_phase(self, phase: str, **data) -> None:
            self.logged.append((phase, dict(data)))

        def _set_tree_status_message(self, *_a, **_k) -> None:
            return None

        _destination_materialize_reason_may_skip_without_interrupting_async = (
            MainWindow._destination_materialize_reason_may_skip_without_interrupting_async
        )

    host = Host()
    out = MainWindow._try_skip_redundant_destination_future_model_materialize(host, "deferred_test_reason")
    assert out is None
    decisions = [d for p, d in host.logged if p == "destination_overlay_skip_decision"]
    assert decisions
    assert decisions[-1].get("final_decision") == "apply"
    assert decisions[-1].get("skip_blocker") == "unresolved_overlay_backlog"
    assert decisions[-1].get("unresolved_parent_visibility", {}).get("allocation_parent_not_visible") == 3
