"""Regression: startup memory-truth shows the full persisted workspace via bounded foreground minimal replay;
full persisted replay (descendants apply, etc.) stays deferred until after grace."""

from __future__ import annotations

import contextlib
import os
import time
from unittest.mock import MagicMock, patch

from ozlink_console import destination_authority_contract
from ozlink_console.main_window import MainWindow
from ozlink_console.models import ProposedFolder


def _minimal_mw_for_memory_truth_body():
    mw = MainWindow.__new__(MainWindow)
    mw.unresolved_proposed_by_parent_path = {}
    mw.unresolved_allocations_by_parent_path = {}
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
    mw._count_destination_model_non_placeholder_nodes = lambda: 120
    mw._destination_user_scroll_interaction_active = lambda: False
    mw._startup_memory_full_workspace_audit_run = lambda: {
        "expected_total_persisted_planned_rows": 24,
        "expected_total_persisted_proposed_folders": 8,
        "present_visible_planned_rows": 24,
        "present_visible_proposed_folders": 8,
        "missing_visible_planned_rows": 0,
        "missing_visible_proposed_folders": 0,
        "missing_planned_paths": [],
        "missing_proposed_paths": [],
        "visible_planned_enumeration_count": 32,
    }
    mw._replay_unresolved_proposed_overlay = lambda *a, **k: 0
    mw._replay_unresolved_allocation_overlay = lambda *a, **k: 0
    mw._count_visible_destination_future_state_nodes = lambda: 0
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
    """``startup_memory_visible_tree_ready`` precedes replay and ``startup_background_refine_begin``."""
    mw = _minimal_mw_for_memory_truth_body()
    seq: list[str] = []

    def capture(msg: str, **_kwargs):
        if isinstance(msg, str) and (
            msg.startswith("startup_memory_")
            or msg.startswith("startup_background_")
            or msg.startswith("destination_phase_timing")
            or msg.startswith("destination_")
        ):
            seq.append(msg)

    replay_calls = [0]

    def counting_replay(*_a, **_k):
        replay_calls[0] += 1
        return 42

    mw._destination_planning_overlay_replay_persisted_only = counting_replay

    with patch.dict(os.environ, {"OZLINK_STARTUP_BACKGROUND_REFINE_GRACE_MS": "120"}, clear=False):
        with patch("ozlink_console.main_window.log_info", side_effect=capture):
            with patch("ozlink_console.main_window.QApplication.processEvents", lambda *_a, **_k: None):
                with _memory_truth_timer_queue() as drain:
                    MainWindow._apply_destination_planning_overlays_body_memory_truth_startup(
                        mw,
                        "startup_planned_workspace_memory_truth",
                    )
                    assert "startup_memory_visible_tree_ready" in seq
                    assert replay_calls[0] == 0
                    assert "startup_background_refine_begin" not in seq
                    drain()
    assert seq.index("startup_memory_visible_tree_ready") < seq.index("startup_memory_replay_deferred_to_background")
    assert seq.index("startup_memory_visible_tree_ready") < seq.index("startup_background_refine_begin")
    assert replay_calls[0] == 1
    mw._startup_memory_truth_deferred_finish.assert_called_once()


def test_replay_not_on_foreground_startup_path():
    """Persisted overlay replay must not run during the synchronous memory presentation body."""
    mw = _minimal_mw_for_memory_truth_body()
    replay_calls = [0]

    def counting_replay(*_a, **_k):
        replay_calls[0] += 1
        return 1

    mw._destination_planning_overlay_replay_persisted_only = counting_replay

    with patch.dict(os.environ, {"OZLINK_STARTUP_BACKGROUND_REFINE_GRACE_MS": "120"}, clear=False):
        with patch("ozlink_console.main_window.QApplication.processEvents", lambda *_a, **_k: None):
            with _memory_truth_timer_queue() as drain:
                MainWindow._apply_destination_planning_overlays_body_memory_truth_startup(
                    mw,
                    "startup_planned_workspace_memory_truth",
                )
                assert replay_calls[0] == 0
                drain()
    assert replay_calls[0] >= 1


def test_log_tokens_no_materialize_before_visible():
    """Guards against re-adding destination materialize calls before ``startup_memory_visible_tree_ready``."""
    mw = _minimal_mw_for_memory_truth_body()
    msgs: list[str] = []

    def capture(msg: str, **_kwargs):
        msgs.append(str(msg))

    with patch.dict(os.environ, {"OZLINK_STARTUP_BACKGROUND_REFINE_GRACE_MS": "120"}, clear=False):
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
    """The synchronous body (through ``visible_tree_ready``) must stay sub-second; deferred refine follows timers."""
    mw = _minimal_mw_for_memory_truth_body()

    def slow_replay(*_a, **_k):
        time.sleep(0.2)
        return 1

    mw._destination_planning_overlay_replay_persisted_only = slow_replay

    with patch.dict(os.environ, {"OZLINK_STARTUP_BACKGROUND_REFINE_GRACE_MS": "120"}, clear=False):
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


def test_bounded_replay_invocations_background_only():
    """Replay runs once in the deferred tail after grace, not on the foreground presentation path."""
    mw = _minimal_mw_for_memory_truth_body()
    n = [0]

    def count(*_a, **_k):
        n[0] += 1
        return 1

    mw._destination_planning_overlay_replay_persisted_only = count

    with patch.dict(os.environ, {"OZLINK_STARTUP_BACKGROUND_REFINE_GRACE_MS": "120"}, clear=False):
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
    """``startup_background_refine_begin`` follows ``startup_memory_visible_tree_ready`` after grace drain."""
    mw = _minimal_mw_for_memory_truth_body()
    ordered: list[str] = []

    def capture(msg: str, **_kwargs):
        if msg in (
            "startup_memory_visible_tree_ready",
            "startup_background_refine_begin",
        ):
            ordered.append(msg)

    with patch.dict(os.environ, {"OZLINK_STARTUP_BACKGROUND_REFINE_GRACE_MS": "120"}, clear=False):
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
        "startup_background_refine_begin",
    ]


def test_post_startup_workspace_building_cleared_before_deferred_refine():
    """Workspace is usable after foreground presentation; snapshot dirty follows deferred tail."""
    mw = _minimal_mw_for_memory_truth_body()

    def real_deferred(_ctx, _reason, _n, _exp_len):
        pass

    mw._startup_memory_truth_deferred_finish = real_deferred

    with patch.dict(os.environ, {"OZLINK_STARTUP_BACKGROUND_REFINE_GRACE_MS": "120"}, clear=False):
        with patch("ozlink_console.main_window.QApplication.processEvents", lambda *_a, **_k: None):
            with _memory_truth_timer_queue() as drain:
                MainWindow._apply_destination_planning_overlays_body_memory_truth_startup(
                    mw,
                    "startup_planned_workspace_memory_truth",
                )
                assert mw._destination_startup_memory_workspace_building is False
                assert mw._destination_startup_memory_phase == "memory_presented"
                drain()
    mw._mark_destination_tree_snapshot_dirty_after_injection.assert_called_once()


def test_outer_overlay_returns_before_background_replay():
    """Memory-truth body returns before persisted replay; replay runs in deferred tail after grace."""
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

    with patch.dict(os.environ, {"OZLINK_STARTUP_BACKGROUND_REFINE_GRACE_MS": "120"}, clear=False):
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
                            drain()
                            assert phases == ["body_enter", "body_exit", "replay"]


def test_graph_descendant_paused_logs_and_resumes_on_scroll_idle():
    """Graph descendant apply defers to scroll idle instead of immediate timer(0) reschedule."""
    mw = MainWindow.__new__(MainWindow)
    mw._application_shutting_down = False
    mw._destination_descendant_apply_paused_for_finalize_alloc = False
    mw._destination_descendant_apply_tick_running = False
    mw._destination_descendant_apply_state = {"graph_walk": True}
    mw._destination_descendant_apply_queue = None
    mw._destination_descendant_apply_inline_drain = False
    mw._destination_planning_model = MagicMock()
    mw._destination_planning_model.begin_coalesce_destination_structure_signal = None
    mw._destination_planning_model.end_coalesce_destination_structure_signal = None
    calls = []

    def scroll_on():
        calls.append("scroll_active")
        return True

    mw._destination_user_scroll_interaction_active = scroll_on
    mw._destination_descendant_apply_drain_note_graph_tick_end = lambda *a, **k: None
    mw._destination_descendant_apply_drain_finalize_graph_walk_tick_slice = lambda *a, **k: None
    idle_started = []

    class _T:
        def start(self, ms):
            idle_started.append(ms)

    mw._destination_tree_scroll_idle_timer = _T()
    paused_msgs: list[str] = []

    def cap_info(msg, **_k):
        if msg == "background_graph_descendant_paused_for_interaction":
            paused_msgs.append(msg)

    with patch("ozlink_console.main_window.log_info", side_effect=cap_info):
        MainWindow._run_destination_descendant_apply_tick_body(mw)
    assert paused_msgs == ["background_graph_descendant_paused_for_interaction"]
    assert mw._destination_descendant_apply_deferred_for_scroll_resume is True
    assert idle_started and idle_started[0] >= 120


def test_snapshot_drain_deferred_when_scroll_active():
    mw = MainWindow.__new__(MainWindow)
    mw._application_shutting_down = False
    mw._destination_snapshot_capture_drain_depth = 0
    mw._destination_descendant_apply_paused_for_finalize_alloc = False
    mw._destination_descendant_apply_tick_running = False
    mw._destination_user_scroll_interaction_active = lambda: True
    logged = []

    def cap(msg, **_k):
        if msg == "background_snapshot_drain_paused_for_interaction":
            logged.append(msg)

    idle_ms = []

    class _T:
        def start(self, ms):
            idle_ms.append(ms)

    mw._destination_tree_scroll_idle_timer = _T()
    with patch("ozlink_console.main_window.log_info", side_effect=cap):
        MainWindow._destination_finalize_inflight_descendant_apply_for_snapshot_capture(mw)
    assert logged == ["background_snapshot_drain_paused_for_interaction"]
    assert mw._destination_snapshot_drain_deferred_for_scroll is True
    assert idle_ms[0] >= 120


def test_scroll_idle_resumes_descendant_and_clears_flag():
    mw = MainWindow.__new__(MainWindow)
    mw._destination_descendant_apply_deferred_for_scroll_resume = True
    mw._destination_snapshot_drain_deferred_for_scroll = False
    sched = []

    def sched_tick():
        sched.append("tick")

    mw._schedule_destination_descendant_apply_tick = sched_tick
    mw._destination_reconcile_pended_after_scroll = None
    mw._destination_global_planned_reconcile_pended_after_scroll = None
    mw._destination_materialize_pended_for_scroll_reason = ""
    mw._destination_materialize_pended_for_scroll_kwargs = {}
    mw._destination_indicator_refresh_deferred_for_scroll = False
    MainWindow._destination_on_destination_tree_scroll_idle(mw)
    assert mw._destination_descendant_apply_deferred_for_scroll_resume is False
    assert sched == ["tick"]


def test_minimal_replay_complete_log_before_visible_tree_ready():
    """Persisted workspace audit is clean before ``startup_memory_visible_tree_ready`` when replay converges."""
    mw = _minimal_mw_for_memory_truth_body()
    audit_calls = [0]

    def audit():
        audit_calls[0] += 1
        if audit_calls[0] == 1:
            return {
                "present_visible_planned_rows": 0,
                "missing_visible_planned_rows": 2,
                "missing_visible_proposed_folders": 0,
                "missing_planned_paths": ["Root3\\Deep\\A\\Miss"],
                "missing_proposed_paths": [],
                "expected_total_persisted_planned_rows": 24,
                "expected_total_persisted_proposed_folders": 8,
                "present_visible_proposed_folders": 0,
                "visible_planned_enumeration_count": 0,
            }
        return {
            "present_visible_planned_rows": 24,
            "missing_visible_planned_rows": 0,
            "missing_visible_proposed_folders": 0,
            "missing_planned_paths": [],
            "missing_proposed_paths": [],
            "expected_total_persisted_planned_rows": 24,
            "expected_total_persisted_proposed_folders": 8,
            "present_visible_proposed_folders": 8,
            "visible_planned_enumeration_count": 32,
        }

    mw._startup_memory_full_workspace_audit_run = audit
    _qs = {"qp": 1, "qa": 1}

    def _q_prop():
        return int(_qs["qp"])

    def _q_alloc():
        return int(_qs["qa"])

    mw._unresolved_proposed_queue_size = _q_prop
    mw._unresolved_allocation_queue_size = _q_alloc

    def _drain_replay_prop(*_a, **_k):
        _qs["qp"] = 0
        _qs["qa"] = 0
        return 1

    mw._replay_unresolved_proposed_overlay = _drain_replay_prop
    mw._replay_unresolved_allocation_overlay = lambda *_a, **_k: 0
    seq: list[str] = []

    def capture(msg: str, **_kwargs):
        if isinstance(msg, str):
            seq.append(msg)

    with patch.dict(os.environ, {"OZLINK_STARTUP_BACKGROUND_REFINE_GRACE_MS": "120"}, clear=False):
        with patch("ozlink_console.main_window.log_info", side_effect=capture):
            with patch("ozlink_console.main_window.QApplication.processEvents", lambda *_a, **_k: None):
                with _memory_truth_timer_queue() as drain:
                    MainWindow._apply_destination_planning_overlays_body_memory_truth_startup(
                        mw,
                        "startup_planned_workspace_memory_truth",
                    )
                    assert seq.index("startup_memory_minimal_replay_complete") < seq.index(
                        "startup_memory_visible_tree_ready"
                    )
                    drain()


def test_foreground_no_allocation_descendants_apply_before_visible_tree_ready():
    """Heavy allocation-descendants apply is not invoked during minimal foreground replay."""
    mw = _minimal_mw_for_memory_truth_body()
    mw._apply_visible_destination_allocation_descendants = MagicMock(return_value=0)
    msgs: list[str] = []

    def capture(msg: str, **_kwargs):
        msgs.append(str(msg))

    with patch.dict(os.environ, {"OZLINK_STARTUP_BACKGROUND_REFINE_GRACE_MS": "120"}, clear=False):
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
                    assert mw._apply_visible_destination_allocation_descendants.call_count == 0
                    drain()


def test_heavy_persisted_replay_blocked_while_minimal_replay_flag_active():
    """Guard B: full persisted replay cannot run during the minimal-replay critical section."""
    mw = MainWindow.__new__(MainWindow)
    mw._startup_memory_minimal_replay_active = True
    logged: list[str] = []

    def cap(msg: str, **_k):
        logged.append(str(msg))

    with patch("ozlink_console.main_window.log_info", side_effect=cap):
        n = MainWindow._destination_planning_overlay_replay_persisted_only(mw, "ctx")
    assert n == 0
    assert "startup_memory_minimal_replay_heavy_path_blocked" in logged


def test_minimal_replay_runs_until_max_rounds_when_queues_still_nonempty():
    """Zero overlay progress does not end minimal replay early; max_rounds caps the loop."""
    mw = _minimal_mw_for_memory_truth_body()
    mw._startup_memory_full_workspace_audit_run = lambda: {
        "present_visible_planned_rows": 0,
        "missing_visible_planned_rows": 5,
        "missing_visible_proposed_folders": 1,
        "missing_planned_paths": ["a"],
        "missing_proposed_paths": ["b"],
        "expected_total_persisted_planned_rows": 24,
        "expected_total_persisted_proposed_folders": 8,
        "present_visible_proposed_folders": 0,
        "visible_planned_enumeration_count": 0,
    }
    mw._unresolved_proposed_queue_size = lambda: 1
    mw._unresolved_allocation_queue_size = lambda: 1
    prop_calls = [0]
    alloc_calls = [0]

    _st = {"qp": 8, "qa": 8}

    def c_prop(*_a, **_k):
        prop_calls[0] += 1
        _st["qp"] = max(0, int(_st["qp"]) - 1)
        return 0

    def c_alloc(*_a, **_k):
        alloc_calls[0] += 1
        _st["qa"] = max(0, int(_st["qa"]) - 1)
        return 0

    mw._unresolved_proposed_queue_size = lambda: int(_st["qp"])
    mw._unresolved_allocation_queue_size = lambda: int(_st["qa"])
    mw._replay_unresolved_proposed_overlay = c_prop
    mw._replay_unresolved_allocation_overlay = c_alloc

    with patch.dict(
        os.environ,
        {"OZLINK_STARTUP_BACKGROUND_REFINE_GRACE_MS": "120", "OZLINK_STARTUP_MINIMAL_REPLAY_MAX_ROUNDS": "8"},
        clear=False,
    ):
        with patch("ozlink_console.main_window.QApplication.processEvents", lambda *_a, **_k: None):
            with _memory_truth_timer_queue() as drain:
                MainWindow._apply_destination_planning_overlays_body_memory_truth_startup(
                    mw,
                    "startup_planned_workspace_memory_truth",
                )
                drain()
    assert prop_calls[0] == 8
    assert alloc_calls[0] == 8


def test_minimal_replay_exits_when_unresolved_queues_empty():
    """When both unresolved queues are drained, minimal replay stops even if audit still shows gaps."""
    mw = _minimal_mw_for_memory_truth_body()
    mw._startup_memory_full_workspace_audit_run = lambda: {
        "present_visible_planned_rows": 0,
        "missing_visible_planned_rows": 3,
        "missing_visible_proposed_folders": 0,
        "missing_planned_paths": ["x"],
        "missing_proposed_paths": [],
        "expected_total_persisted_planned_rows": 24,
        "expected_total_persisted_proposed_folders": 8,
        "present_visible_proposed_folders": 0,
        "visible_planned_enumeration_count": 0,
    }
    mw._unresolved_proposed_queue_size = lambda: 0
    mw._unresolved_allocation_queue_size = lambda: 0
    prop_calls = [0]
    alloc_calls = [0]

    def _no_prop(*_a, **_k):
        prop_calls[0] += 1
        return 0

    def _no_alloc(*_a, **_k):
        alloc_calls[0] += 1
        return 0

    mw._replay_unresolved_proposed_overlay = _no_prop
    mw._replay_unresolved_allocation_overlay = _no_alloc
    logged: list[tuple[str, dict]] = []

    def cap(msg: str, **kwargs):
        logged.append((msg, kwargs))

    with patch.dict(os.environ, {"OZLINK_STARTUP_BACKGROUND_REFINE_GRACE_MS": "120"}, clear=False):
        with patch("ozlink_console.main_window.log_info", side_effect=cap):
            with patch("ozlink_console.main_window.QApplication.processEvents", lambda *_a, **_k: None):
                with _memory_truth_timer_queue() as drain:
                    MainWindow._apply_destination_planning_overlays_body_memory_truth_startup(
                        mw,
                        "startup_planned_workspace_memory_truth",
                    )
                    drain()
    assert prop_calls[0] == 0
    assert alloc_calls[0] == 0
    exit_msgs = [k for m, k in logged if m == "startup_memory_minimal_replay_exit_reason"]
    assert any(k.get("reason") == "queues_empty" for k in exit_msgs)


def test_workspace_audit_missing_zero_before_visible_tree_ready_when_converged():
    """``startup_memory_visible_tree_ready`` receives zero missing counts after minimal replay completes the audit."""
    mw = _minimal_mw_for_memory_truth_body()
    audit_calls = [0]

    def audit():
        audit_calls[0] += 1
        if audit_calls[0] == 1:
            return {
                "present_visible_planned_rows": 0,
                "missing_visible_planned_rows": 1,
                "missing_visible_proposed_folders": 0,
                "missing_planned_paths": ["Root3\\Deep\\Chain\\Leaf"],
                "missing_proposed_paths": [],
                "expected_total_persisted_planned_rows": 24,
                "expected_total_persisted_proposed_folders": 8,
                "present_visible_proposed_folders": 0,
                "visible_planned_enumeration_count": 0,
            }
        return {
            "present_visible_planned_rows": 24,
            "missing_visible_planned_rows": 0,
            "missing_visible_proposed_folders": 0,
            "missing_planned_paths": [],
            "missing_proposed_paths": [],
            "expected_total_persisted_planned_rows": 24,
            "expected_total_persisted_proposed_folders": 8,
            "present_visible_proposed_folders": 8,
            "visible_planned_enumeration_count": 32,
        }

    mw._startup_memory_full_workspace_audit_run = audit
    _qs = {"qp": 1, "qa": 1}

    def _drain_prop(*_a, **_k):
        _qs["qp"] = 0
        _qs["qa"] = 0
        return 1

    mw._unresolved_proposed_queue_size = lambda: int(_qs["qp"])
    mw._unresolved_allocation_queue_size = lambda: int(_qs["qa"])
    mw._replay_unresolved_proposed_overlay = _drain_prop
    mw._replay_unresolved_allocation_overlay = lambda *_a, **_k: 0
    vis_kw: dict = {}

    def cap(msg: str, **kwargs):
        if msg == "startup_memory_visible_tree_ready":
            vis_kw.clear()
            vis_kw.update(kwargs)

    with patch.dict(os.environ, {"OZLINK_STARTUP_BACKGROUND_REFINE_GRACE_MS": "120"}, clear=False):
        with patch("ozlink_console.main_window.log_info", side_effect=cap):
            with patch("ozlink_console.main_window.QApplication.processEvents", lambda *_a, **_k: None):
                with _memory_truth_timer_queue() as drain:
                    MainWindow._apply_destination_planning_overlays_body_memory_truth_startup(
                        mw,
                        "startup_planned_workspace_memory_truth",
                    )
                    drain()
    assert vis_kw.get("missing_visible_planned_rows") == 0
    assert vis_kw.get("missing_visible_proposed_folders") == 0
    assert vis_kw.get("persisted_workspace_audit_complete") is True


def test_minimal_replay_seeds_queues_for_deep_persisted_paths_before_visible_ready():
    """When audit lists missing planned rows, seeding re-queues persisted moves so replay can finish with zero missing."""
    mw = _minimal_mw_for_memory_truth_body()
    mw._reset_unresolved_proposed_queue = lambda: None
    mw._reset_unresolved_allocation_queue = lambda: None
    deep = "Root3\\Deep\\A\\Leaf"
    mw.planned_moves = [
        {
            "source_path": "s1",
            "destination_path": deep,
            "destination": {"id": "d1", "name": "Leaf"},
        }
    ]
    mw.proposed_folders = []
    audit_calls = [0]

    def audit():
        audit_calls[0] += 1
        if audit_calls[0] == 1:
            return {
                "present_visible_planned_rows": 0,
                "missing_visible_planned_rows": 1,
                "missing_visible_proposed_folders": 0,
                "missing_planned_paths": [deep],
                "missing_proposed_paths": [],
                "expected_total_persisted_planned_rows": 1,
                "expected_total_persisted_proposed_folders": 0,
                "present_visible_proposed_folders": 0,
                "visible_planned_enumeration_count": 0,
            }
        return {
            "present_visible_planned_rows": 1,
            "missing_visible_planned_rows": 0,
            "missing_visible_proposed_folders": 0,
            "missing_planned_paths": [],
            "missing_proposed_paths": [],
            "expected_total_persisted_planned_rows": 1,
            "expected_total_persisted_proposed_folders": 0,
            "present_visible_proposed_folders": 0,
            "visible_planned_enumeration_count": 1,
        }

    mw._startup_memory_full_workspace_audit_run = audit
    mw._startup_memory_audit_canonical_destination_for_planned_move = (
        lambda _m: deep if isinstance(_m, dict) and _m.get("source_path") == "s1" else ""
    )
    mw._allocation_parent_path = lambda _m: "Root3\\Deep\\A" if isinstance(_m, dict) else ""
    mw._allocation_projection_path = lambda _m: deep if isinstance(_m, dict) else ""
    mw._destination_unresolved_replay_parent_path_structurally_invalid = lambda *_a, **_k: (False, "")
    mw._destination_invalid_unresolved_parent_previously_pruned = lambda *_a, **_k: False
    mw._find_visible_destination_item_by_path = lambda *_a, **_k: None
    vis_kw: dict = {}
    seeded: list[dict] = []

    def capture(msg: str, **kwargs):
        if msg == "startup_memory_visible_tree_ready":
            vis_kw.update(kwargs)
        if msg == "startup_memory_minimal_replay_seed_paths":
            seeded.append(dict(kwargs))

    with patch.object(destination_authority_contract, "graph_owns_visible_real_destination_structure", return_value=False):
        with patch.dict(os.environ, {"OZLINK_STARTUP_BACKGROUND_REFINE_GRACE_MS": "120"}, clear=False):
            with patch("ozlink_console.main_window.log_info", side_effect=capture):
                with patch("ozlink_console.main_window.QApplication.processEvents", lambda *_a, **_k: None):
                    with _memory_truth_timer_queue() as drain:
                        MainWindow._apply_destination_planning_overlays_body_memory_truth_startup(
                            mw,
                            "startup_planned_workspace_memory_truth",
                        )
                        drain()
    assert vis_kw.get("missing_visible_planned_rows") == 0
    assert vis_kw.get("persisted_workspace_audit_complete") is True
    assert seeded and int(seeded[0].get("paths_seeded", 0) or 0) >= 1


def test_startup_unbounded_replay_applies_many_parents_per_overlay_pass():
    """With ``_startup_memory_replay_unbounded``, unresolved proposed replay is not limited to one parent per call."""
    mw = MainWindow.__new__(MainWindow)
    mw._memory_restore_in_progress = False
    mw._suppress_selector_change_handlers = False
    mw._log_restore_phase = lambda *a, **k: None
    mw._schedule_destination_restore_materialization_queue = lambda *a, **k: None
    mw._schedule_unresolved_replay_drain_after_budget_suppression = lambda *a, **k: None
    mw._restore_queue_tick_delay_ms = 80
    mw.destination_tree_widget = object()
    mw.unresolved_proposed_by_parent_path = {}
    mw.unresolved_allocations_by_parent_path = {}
    for i in range(12):
        pp = f"Root\\P{i}"
        pf = ProposedFolder(FolderName=f"Folder{i}", DestinationPath="", ParentPath=pp)
        mw.unresolved_proposed_by_parent_path[pp] = {f"sk{i}": pf}

    apply_ops: list[int] = []

    def apply_children(_parent_item):
        apply_ops.append(1)
        return 1

    mw._apply_proposed_children_to_item = apply_children
    ix = MagicMock()
    ix.isValid = lambda: True
    mw._ensure_destination_projection_path = lambda _p: ix
    mw._destination_user_scroll_interaction_active = lambda: False
    mw._count_visible_destination_future_state_nodes = lambda: 0

    with patch.object(MainWindow, "_planning_tree_top_level_count", lambda _self, _t: 3):
        with patch.dict(os.environ, {"OZLINK_RESTORE_REPLAY_PARENT_BUDGET": "1"}, clear=False):
            mw._startup_memory_replay_unbounded = True
            n = MainWindow._replay_unresolved_proposed_overlay(mw, "z:startup_memory_minimal_replay", "")
    assert n == 12
    assert len(apply_ops) == 12

    mw2 = MainWindow.__new__(MainWindow)
    mw2._memory_restore_in_progress = False
    mw2._suppress_selector_change_handlers = False
    mw2._log_restore_phase = lambda *a, **k: None
    mw2._schedule_destination_restore_materialization_queue = lambda *a, **k: None
    mw2._schedule_unresolved_replay_drain_after_budget_suppression = lambda *a, **k: None
    mw2._restore_queue_tick_delay_ms = 80
    mw2.destination_tree_widget = object()
    mw2.unresolved_proposed_by_parent_path = dict(mw.unresolved_proposed_by_parent_path)
    mw2.unresolved_allocations_by_parent_path = {}
    apply_b: list[int] = []

    def apply_children_b(_parent_item):
        apply_b.append(1)
        return 1

    mw2._apply_proposed_children_to_item = apply_children_b
    mw2._ensure_destination_projection_path = lambda _p: ix
    mw2._destination_user_scroll_interaction_active = lambda: False
    mw2._count_visible_destination_future_state_nodes = lambda: 0

    with patch.object(MainWindow, "_planning_tree_top_level_count", lambda _self, _t: 3):
        with patch.dict(os.environ, {"OZLINK_RESTORE_REPLAY_PARENT_BUDGET": "1"}, clear=False):
            mw2._startup_memory_replay_unbounded = False
            MainWindow._replay_unresolved_proposed_overlay(mw2, "z:startup_memory_minimal_replay", "")
    assert len(apply_b) == 1


def test_minimal_replay_logs_progress_each_round_when_replay_runs():
    """Progress logs appear for each round; exit_reason max_rounds when cap hit with work remaining."""
    mw = _minimal_mw_for_memory_truth_body()
    mw._startup_memory_full_workspace_audit_run = lambda: {
        "present_visible_planned_rows": 0,
        "missing_visible_planned_rows": 2,
        "missing_visible_proposed_folders": 0,
        "missing_planned_paths": ["a"],
        "missing_proposed_paths": [],
        "expected_total_persisted_planned_rows": 24,
        "expected_total_persisted_proposed_folders": 8,
        "present_visible_proposed_folders": 0,
        "visible_planned_enumeration_count": 0,
    }
    _st = {"qp": 10, "qa": 10}

    def _dec_prop(*_a, **_k):
        _st["qp"] = max(0, int(_st["qp"]) - 1)
        return 0

    def _dec_alloc(*_a, **_k):
        _st["qa"] = max(0, int(_st["qa"]) - 1)
        return 0

    mw._unresolved_proposed_queue_size = lambda: int(_st["qp"])
    mw._unresolved_allocation_queue_size = lambda: int(_st["qa"])
    mw._replay_unresolved_proposed_overlay = _dec_prop
    mw._replay_unresolved_allocation_overlay = _dec_alloc
    progress: list[str] = []
    exits: list[str] = []

    def cap(msg: str, **kwargs):
        if msg == "startup_memory_minimal_replay_progress":
            progress.append(msg)
        if msg == "startup_memory_minimal_replay_exit_reason":
            exits.append(str(kwargs.get("reason", "")))

    with patch.dict(
        os.environ,
        {"OZLINK_STARTUP_BACKGROUND_REFINE_GRACE_MS": "120", "OZLINK_STARTUP_MINIMAL_REPLAY_MAX_ROUNDS": "4"},
        clear=False,
    ):
        with patch("ozlink_console.main_window.log_info", side_effect=cap):
            with patch("ozlink_console.main_window.QApplication.processEvents", lambda *_a, **_k: None):
                with _memory_truth_timer_queue() as drain:
                    MainWindow._apply_destination_planning_overlays_body_memory_truth_startup(
                        mw,
                        "startup_planned_workspace_memory_truth",
                    )
                    drain()
    assert len(progress) == 4
    assert exits == ["max_rounds"]
