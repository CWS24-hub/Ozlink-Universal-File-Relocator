"""Regression: startup memory-truth must not block ``startup_memory_visible_tree_ready`` on heavy replay."""

from __future__ import annotations

import contextlib
import time
from unittest.mock import MagicMock, patch

from ozlink_console.main_window import MainWindow


def _minimal_mw_for_memory_truth_body():
    mw = MainWindow.__new__(MainWindow)
    mw._startup_memory_planned_attach_skipped_log = []
    mw.planned_moves = [
        {
            "source_path": "s1",
            "destination_path": "Root3\\Deep\\A\\Leaf",
            "destination": {"id": "d1", "name": "Leaf"},
        }
        for _ in range(24)
    ]
    mw.proposed_folders = [{"DestinationPath": f"Root3\\Deep\\PF{i}", "FolderName": f"PF{i}"} for i in range(8)]
    mw._reset_unresolved_proposed_queue = lambda: None
    mw._reset_unresolved_allocation_queue = lambda: None
    mw._destination_prune_invalid_unresolved_replay_parent_paths = lambda **k: None
    mw._cancel_destination_future_async_projection = lambda *a, **k: None
    mw._destination_startup_memory_expanded_paths_for_bind = lambda: {
        f"Root3\\Seg{j}" for j in range(12)
    }
    mw._destination_planning_overlay_replay_persisted_only = MagicMock(return_value=42)
    mw._startup_memory_mark_saved_descendant_expand_affordances = MagicMock()
    mw._startup_memory_truth_deferred_finish = MagicMock()
    mw._mark_destination_tree_snapshot_dirty_after_injection = MagicMock()
    mw._log_restore_exception = lambda *a, **k: None
    mw._safe_invoke = lambda _n, fn: fn()
    mw._destination_startup_memory_workspace_building = False
    return mw


@contextlib.contextmanager
def _memory_truth_timer_queue():
    """Queue ``QTimer.singleShot`` callbacks; call ``drain()`` to run them FIFO (nested posts supported)."""
    q: list[tuple[int, object]] = []

    def fake_single_shot(ms, fn):
        q.append((int(ms), fn))

    def drain() -> None:
        safety = 0
        while q and safety < 64:
            _ms, fn = q.pop(0)
            fn()
            safety += 1

    with patch("ozlink_console.main_window.QTimer.singleShot", side_effect=fake_single_shot):
        yield drain


def test_visible_tree_ready_before_background_replay_and_refine():
    """Critical path logs ``startup_memory_visible_tree_ready`` before replay and refine hooks."""
    mw = _minimal_mw_for_memory_truth_body()
    seq: list[str] = []

    def capture(msg: str, **_kwargs):
        if isinstance(msg, str) and (
            msg.startswith("startup_memory_")
            or msg.startswith("destination_phase_timing")
            or msg.startswith("destination_")
        ):
            seq.append(msg)

    replay_calls = [0]

    def counting_replay(*_a, **_k):
        replay_calls[0] += 1
        return 42

    mw._destination_planning_overlay_replay_persisted_only = counting_replay

    with patch("ozlink_console.main_window.log_info", side_effect=capture):
        with patch("ozlink_console.main_window.QApplication.processEvents", lambda *_a, **_k: None):
            with _memory_truth_timer_queue() as drain:
                MainWindow._apply_destination_planning_overlays_body_memory_truth_startup(
                    mw,
                    "startup_planned_workspace_memory_truth",
                )
                assert "startup_memory_visible_tree_ready" in seq
                assert replay_calls[0] == 0
                assert "startup_memory_background_refine_begin" not in seq
                drain()
    assert seq.index("startup_memory_visible_tree_ready") < seq.index("startup_memory_background_refine_begin")
    assert replay_calls[0] == 1
    mw._startup_memory_truth_deferred_finish.assert_called_once()


def test_log_tokens_no_materialize_before_visible():
    """Guards against re-adding destination materialize calls before ``startup_memory_visible_tree_ready``."""
    mw = _minimal_mw_for_memory_truth_body()
    msgs: list[str] = []

    def capture(msg: str, **_kwargs):
        msgs.append(str(msg))

    with patch("ozlink_console.main_window.log_info", side_effect=capture):
        with patch("ozlink_console.main_window.QApplication.processEvents", lambda *_a, **_k: None):
            with _memory_truth_timer_queue() as drain:
                MainWindow._apply_destination_planning_overlays_body_memory_truth_startup(
                    mw,
                    "startup_planned_workspace_memory_truth",
                )
                vis = msgs.index("startup_memory_visible_tree_ready")
                for i in range(vis):
                    assert "materialize_destination_future_model" not in msgs[i]
                    assert "destination_future_model_materialize" not in msgs[i]
                drain()


def test_sync_critical_path_completes_under_threshold_ms():
    """The synchronous body (through ``visible_tree_ready``) must stay sub-second; heavy work is deferred."""
    mw = _minimal_mw_for_memory_truth_body()

    def slow_replay(*_a, **_k):
        time.sleep(0.2)
        return 1

    mw._destination_planning_overlay_replay_persisted_only = slow_replay

    with _memory_truth_timer_queue() as drain:
        t0 = time.perf_counter()
        with patch("ozlink_console.main_window.QApplication.processEvents", lambda *_a, **_k: None):
            MainWindow._apply_destination_planning_overlays_body_memory_truth_startup(
                mw,
                "startup_planned_workspace_memory_truth",
            )
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        assert elapsed_ms < 500.0
        drain()


def test_bounded_replay_invocations_before_after_ready():
    """Replay runs at most once per startup pass; not on the synchronous pre-ready path."""
    mw = _minimal_mw_for_memory_truth_body()
    n = [0]

    def count(*_a, **_k):
        n[0] += 1
        return 1

    mw._destination_planning_overlay_replay_persisted_only = count

    with patch("ozlink_console.main_window.QApplication.processEvents", lambda *_a, **_k: None):
        with _memory_truth_timer_queue() as drain:
            MainWindow._apply_destination_planning_overlays_body_memory_truth_startup(
                mw,
                "startup_planned_workspace_memory_truth",
            )
            assert n[0] == 0
            drain()
    assert n[0] == 1


def test_background_refine_after_visible_tree_ready_log_order():
    """``startup_memory_background_refine_begin`` must follow ``startup_memory_visible_tree_ready`` in log order."""
    mw = _minimal_mw_for_memory_truth_body()
    ordered: list[str] = []

    def capture(msg: str, **_kwargs):
        if msg in (
            "startup_memory_visible_tree_ready",
            "startup_memory_background_refine_begin",
        ):
            ordered.append(msg)

    with patch("ozlink_console.main_window.log_info", side_effect=capture):
        with patch("ozlink_console.main_window.QApplication.processEvents", lambda *_a, **_k: None):
            with _memory_truth_timer_queue() as drain:
                MainWindow._apply_destination_planning_overlays_body_memory_truth_startup(
                    mw,
                    "startup_planned_workspace_memory_truth",
                )
                assert ordered == ["startup_memory_visible_tree_ready"]
                drain()
    assert ordered == [
        "startup_memory_visible_tree_ready",
        "startup_memory_background_refine_begin",
    ]


def test_post_startup_workspace_building_cleared_and_idle_hooks():
    """After deferred chain, workspace-building clears; deferred finish and snapshot hook run once (settle)."""
    mw = _minimal_mw_for_memory_truth_body()

    def real_deferred(_ctx, _reason, _n, _exp_len):
        pass

    mw._startup_memory_truth_deferred_finish = real_deferred

    with patch("ozlink_console.main_window.QApplication.processEvents", lambda *_a, **_k: None):
        with _memory_truth_timer_queue() as drain:
            MainWindow._apply_destination_planning_overlays_body_memory_truth_startup(
                mw,
                "startup_planned_workspace_memory_truth",
            )
            assert mw._destination_startup_memory_workspace_building is True
            drain()
    assert mw._destination_startup_memory_workspace_building is False
    mw._mark_destination_tree_snapshot_dirty_after_injection.assert_called_once()


def test_outer_overlay_returns_before_replay_runs():
    """``_apply_destination_planning_overlays`` for memory-truth returns before replay executes."""
    mw = _minimal_mw_for_memory_truth_body()
    phases: list[str] = []

    def outer_body(self, reason, **kwargs):
        phases.append("body_enter")
        MainWindow._apply_destination_planning_overlays_body_memory_truth_startup(self, reason, **kwargs)
        phases.append("body_exit")

    def tag_replay(*_a, **_k):
        phases.append("replay")
        return 0

    mw._destination_planning_overlay_replay_persisted_only = tag_replay

    with patch.object(MainWindow, "_apply_destination_planning_overlays_body", outer_body):
        with patch("ozlink_console.main_window._shutdown_mutation_skip_for_host", lambda *_a, **_k: False):
            with patch("ozlink_console.main_window.QApplication.processEvents", lambda *_a, **_k: None):
                with patch("ozlink_console.main_window.is_dev_mode", lambda: False):
                    with _memory_truth_timer_queue() as drain:
                        MainWindow._apply_destination_planning_overlays(
                            mw,
                            "startup_planned_workspace_memory_truth",
                            allow_defer=True,
                            prefer_chunked_projection=False,
                        )
                        assert phases == ["body_enter", "body_exit"]
                        assert "replay" not in phases
                        drain()
    assert "replay" in phases
