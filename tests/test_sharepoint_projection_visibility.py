"""Projection/overlay visibility ordering and Graph-authority guards (narrow)."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from ozlink_console.main_window import MainWindow


def test_apply_destination_planning_overlays_body_runs_proactive_before_try_skip():
    """Proactive parent-chain scheduling must not be skipped when redundant-skip returns early."""
    order: list[str] = []

    class Host:
        _destination_future_model_last_blocked_source_restore = False
        _destination_suppress_steady_materialize_skip_once = False
        _destination_incremental_merge_in_progress = False

        def _schedule_proactive_graph_parent_chains_for_unresolved_overlays(self, *, reason: str) -> int:
            order.append("proactive")
            return 0

        def _try_skip_redundant_destination_future_model_materialize(self, reason):
            order.append("try_skip")
            return 0

    h = Host()
    out = MainWindow._apply_destination_planning_overlays_body(h, "idle_destination_materialize")
    assert out == 0
    assert order == ["proactive", "try_skip"]


def test_drfws_deferred_schedule_short_circuit_still_invokes_proactive(monkeypatch):
    from unittest.mock import MagicMock

    from ozlink_console import destination_authority_contract as dac

    monkeypatch.setattr(dac, "graph_owns_visible_real_destination_structure", lambda _h: False)
    calls: list[str] = []

    class Win:
        planned_moves = []
        proposed_folders = []
        _destination_chunked_bind_state = None
        _destination_future_projection_async_state = None
        _destination_idle_materialize_timer = None
        _destination_idle_materialize_pending_reason = ""
        _destination_drfws_affected_paths = set()

        def _should_park_deferred_materialize_for_full_tree(self, _r):
            return False

        def _try_skip_redundant_destination_future_model_materialize(self, _r):
            return 1

        def _schedule_proactive_graph_parent_chains_for_unresolved_overlays(self, *, reason: str) -> int:
            calls.append(reason)
            return 0

        def _destination_drfws_pending_work_signature(self):
            return ""

    w = Win()
    w._destination_idle_materialize_timer = MagicMock()
    w._destination_idle_materialize_timer.isActive.return_value = False

    MainWindow._schedule_deferred_destination_materialization(
        w, "deferred_reconcile_folder_worker_success", delay_ms=180
    )
    assert calls == ["deferred_reconcile_folder_worker_success"]
