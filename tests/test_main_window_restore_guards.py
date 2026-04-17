from __future__ import annotations

from contextlib import nullcontext

from ozlink_console.main_window import MainWindow


class _NoTopLevelTree:
    """Simulates model-view tree without QTreeWidget APIs."""

    def isExpanded(self, _item):
        return True


class _AbortHost:
    def __init__(self) -> None:
        self._restore_abort_mode = False
        self._restore_abort_reason = ""
        self._restore_destination_overlay_pending = True
        self._restore_finalization_deferred_active = True
        self._restore_finalization_deferred_reason = "x"
        self._destination_restore_materialization_queue = [1]
        self._destination_restore_materialization_seen = {1}
        self._source_restore_materialization_queue = [1]
        self._source_restore_materialization_seen = {1}
        self._source_projection_refresh_paths = {"x"}
        self._deferred_planning_refresh_pending = True
        self._deferred_planning_refresh_reasons = ["x"]
        self._deferred_source_projection_paths = {"x"}
        self._memory_restore_in_progress = True
        self._memory_restore_background_trees = True
        self._suppress_autosave = False
        self.logged = []

    def _update_source_count_labels(self, loaded_items) -> None:
        """Stub: MainWindow._enter_restore_abort_mode refreshes count labels after full-count cleanup."""

    def _restore_abort_active(self) -> bool:
        return bool(self._restore_abort_mode)

    def _log_restore_phase(self, phase, **data):
        self.logged.append((phase, dict(data)))


def test_panel_loaded_branch_state_uses_planning_model_without_top_level_api():
    host = type("Host", (), {})()
    host.source_tree_widget = _NoTopLevelTree()
    host.destination_tree_widget = _NoTopLevelTree()
    host._panel_loaded_branch_state_planning_model = lambda _tree: (True, True)
    result = MainWindow._panel_loaded_branch_state(host, "destination")
    assert result == (True, True)


def test_enter_restore_abort_mode_logs_without_phase_argument_collision():
    host = _AbortHost()
    MainWindow._enter_restore_abort_mode(host, "boom", phase="root_bind")
    assert host._restore_abort_mode is True
    assert host.logged, "expected restore abort log entry"
    phase_name, payload = host.logged[-1]
    assert phase_name == "restore_abort_mode_entered"
    assert payload.get("source_phase") == "root_bind"


class _MaterializeSkipHost:
    """Minimal host for _apply_destination_planning_overlays overlay-pass tests."""

    def __init__(self) -> None:
        self._destination_future_model_last_blocked_source_restore = False
        self._destination_future_projection_async_state = None
        self._destination_chunked_bind_state = None
        self._destination_future_bind_sync_active = False
        self._destination_incremental_merge_in_progress = False
        self._destination_root_prime_pending = False
        self._destination_snapshot_chunked_restore_active = False
        self._destination_trust_gate_pending_materialize = None
        self.pending_root_drive_ids = {"source": "", "destination": ""}
        self.proposed_folders = []
        self.planned_moves = []
        self.cancel_projection_calls: list[str] = []
        self.logged: list[tuple[str, dict]] = []

    def _planning_browse_mode(self, _key: str) -> str:
        return "local"

    def _destination_steady_state_full_materialize_redundant(self) -> bool:
        return False

    def _destination_lifecycle_trace_TEMP(self, *args, **kwargs) -> None:
        return None

    def _startup_post_snapshot_trace_event(self, *_a, **_k) -> None:
        return None

    def _destination_materialize_reason_should_queue_until_async_finishes(self, _reason: str) -> bool:
        return False

    def _destination_full_tree_ready(self) -> bool:
        return True

    def _destination_sharepoint_planning_destination_active(self) -> bool:
        return False

    def _destination_future_model_blocked_by_source_restore(self, _reason) -> bool:
        return False

    def _destination_snapshot_spo_trust_valid(self, _did: str) -> bool:
        return True

    def _current_selected_destination_drive_id(self) -> str:
        return ""

    def _should_defer_destination_materialization(self, _reason) -> bool:
        return False

    def _destination_expanded_paths_for_planning_bind(self):
        return []

    def _destination_selected_path_for_planning_bind(self):
        return ""

    def _replay_unresolved_proposed_overlay(self, *_a, **_k) -> int:
        return 0

    def _replay_unresolved_allocation_overlay(self, *_a, **_k) -> int:
        return 0

    def _apply_visible_destination_allocation_descendants(self, **_k) -> int:
        return 0

    def _hydrate_destination_allocations_for_expanded_paths_model(self, *_a, **_k) -> None:
        return None

    def _hydrate_destination_prefix_chain_for_path_model(self, *_a, **_k) -> None:
        return None

    def _schedule_refresh_destination_tree_indicators(self) -> None:
        return None

    def _restore_expanded_tree_paths(self, *_a, **_k) -> None:
        return None

    def _restore_selected_tree_path(self, *_a, **_k) -> None:
        return None

    def _refresh_expand_all_button_for_panel(self, *_a, **_k) -> None:
        return None

    def _reconcile_destination_semantic_duplicates(self, *_a, **_k) -> None:
        return None

    def _destination_on_authoritative_tree_bind_committed(self, *_a, **_k) -> None:
        return None

    def _dev_log_watch_folder_badge_owners(self, *_a, **_k) -> None:
        return None

    def _current_destination_full_overlay_fingerprint(self) -> str:
        return "fp"

    def _bump_destination_materialized_overlay_fingerprint(self, **_k) -> None:
        return None

    def _set_tree_status_message(self, *_a, **_k) -> None:
        return None

    def _try_skip_redundant_destination_future_model_materialize(self, reason):
        return MainWindow._try_skip_redundant_destination_future_model_materialize(self, reason)

    def _log_restore_phase(self, phase, **data):
        self.logged.append((phase, dict(data)))

    def _log_restore_exception(self, *_a, **_k) -> None:
        """Body uses this in ``except`` paths; real window logs — stub stays quiet."""

    def _destination_audit_overlay_placement_after_pass(self, *_a) -> None:
        return None

    def _destination_materialize_profile_start_cycle(self) -> None:
        return None

    def _destination_materialize_profile_finish_cycle(self, _reason: str) -> None:
        return None

    def _destination_materialize_profile_span(self, _phase: str):
        return nullcontext()

    def _destination_reconcile_all_planned_parents_after_graph_update(self) -> None:
        return None

    def _cancel_destination_future_async_projection(self, reason=""):
        self.cancel_projection_calls.append(str(reason or ""))

    def _on_destination_state_mutation(self, *_a, **_k) -> None:
        """Real :class:`MainWindow` always has this; stubs must not break overlay batch ``finally``."""

    def _schedule_proactive_graph_parent_chains_for_unresolved_overlays(self, *, reason: str) -> int:
        return 0

    def _apply_destination_planning_overlays_body(
        self,
        reason,
        *,
        allow_defer=True,
        prefer_chunked_projection=False,
        narrow_restore_real_snapshot=False,
        force_authoritative_bind=False,
    ):
        return MainWindow._apply_destination_planning_overlays_body(
            self,
            reason,
            allow_defer=allow_defer,
            prefer_chunked_projection=prefer_chunked_projection,
            narrow_restore_real_snapshot=narrow_restore_real_snapshot,
            force_authoritative_bind=force_authoritative_bind,
        )


def test_materialize_cancels_async_projection_on_folder_worker_success():
    """Overlay pass cancels in-flight async projection instead of skipping (no future-model merge pipeline)."""
    host = _MaterializeSkipHost()
    host._destination_future_projection_async_state = {"reason": "folder_worker_success"}
    host.destination_tree_widget = None
    out = MainWindow._apply_destination_planning_overlays(host, "folder_worker_success", allow_defer=False)
    assert out == 0
    assert host.cancel_projection_calls == ["folder_worker_success"]
    assert any(p == "destination_planning_overlay_pass" for p, _d in host.logged)


def test_materialize_runs_overlay_pass_while_chunked_bind_state_stale():
    """Chunked destination bind is removed; a non-None legacy field must not block overlay refresh."""
    host = _MaterializeSkipHost()
    host._destination_chunked_bind_state = {"phase": "legacy_cleared_elsewhere"}
    host.destination_tree_widget = None
    out = MainWindow._apply_destination_planning_overlays(host, "folder_worker_success", allow_defer=False)
    assert out == 0
    assert host.cancel_projection_calls == ["folder_worker_success"]
    assert any(p == "destination_planning_overlay_pass" for p, _d in host.logged)


def test_schedule_post_login_phase4_skips_when_restore_not_in_progress():
    class Host:
        def __init__(self) -> None:
            self.calls: list[str] = []
            self._memory_restore_in_progress = False
            self._restore_destination_overlay_pending = True
            self._restore_abort_mode = False

        def _restore_abort_active(self) -> bool:
            return bool(self._restore_abort_mode)

        def _safe_invoke(self, name, fn) -> None:
            self.calls.append(str(name))

    host = Host()
    MainWindow._schedule_post_login_restore_phase4_if_pending(host)
    assert host.calls == []


def test_schedule_post_login_phase4_skips_when_abort_active():
    class Host:
        def __init__(self) -> None:
            self.calls: list[str] = []
            self._memory_restore_in_progress = True
            self._restore_destination_overlay_pending = True
            self._restore_abort_mode = True

        def _restore_abort_active(self) -> bool:
            return bool(self._restore_abort_mode)

        def _safe_invoke(self, name, fn) -> None:
            self.calls.append(str(name))

    host = Host()
    MainWindow._schedule_post_login_restore_phase4_if_pending(host)
    assert host.calls == []


def test_reset_draft_async_busy_false_on_idle_host():
    host = type(
        "Host",
        (),
        {
            "_memory_ui_rebind_in_progress": False,
            "_root_tree_bind_in_progress": False,
            "root_load_workers": {},
            "folder_load_workers": {},
            "pending_folder_loads": {"source": set(), "destination": set()},
            "_destination_restore_materialization_queue": [],
            "_source_restore_materialization_queue": [],
            "_destination_chunked_bind_state": None,
            "_destination_future_bind_sync_active": False,
            "_destination_future_projection_async_state": None,
            "_destination_snapshot_chunked_restore_active": False,
            "_expand_all_pending": {"source": False, "destination": False},
            "_lazy_destination_projection_pending_reason": "",
        },
    )()
    assert MainWindow._memory_restore_async_busy_for_reset_draft(host) is False


class _RunningWorker:
    def isRunning(self):
        return True


def test_reset_draft_async_busy_false_when_root_entry_but_worker_not_running():
    class _StoppedWorker:
        def isRunning(self):
            return False

    host = type(
        "Host",
        (),
        {
            "_memory_ui_rebind_in_progress": False,
            "_root_tree_bind_in_progress": False,
            "root_load_workers": {"destination": {"id": "w1", "worker": _StoppedWorker()}},
            "folder_load_workers": {},
            "pending_folder_loads": {"source": set(), "destination": set()},
            "_destination_restore_materialization_queue": [],
            "_source_restore_materialization_queue": [],
            "_destination_chunked_bind_state": None,
            "_destination_future_bind_sync_active": False,
            "_destination_future_projection_async_state": None,
            "_destination_snapshot_chunked_restore_active": False,
            "_expand_all_pending": {"source": False, "destination": False},
            "_lazy_destination_projection_pending_reason": "",
        },
    )()
    assert MainWindow._memory_restore_async_busy_for_reset_draft(host) is False


def test_reset_draft_async_busy_true_when_destination_root_worker_running():
    host = type(
        "Host",
        (),
        {
            "_memory_ui_rebind_in_progress": False,
            "_root_tree_bind_in_progress": False,
            "root_load_workers": {"destination": {"id": "w1", "worker": _RunningWorker()}},
            "folder_load_workers": {},
            "pending_folder_loads": {"source": set(), "destination": set()},
            "_destination_restore_materialization_queue": [],
            "_source_restore_materialization_queue": [],
            "_destination_chunked_bind_state": None,
            "_destination_future_bind_sync_active": False,
            "_destination_future_projection_async_state": None,
            "_destination_snapshot_chunked_restore_active": False,
            "_expand_all_pending": {"source": False, "destination": False},
            "_lazy_destination_projection_pending_reason": "",
        },
    )()
    assert MainWindow._memory_restore_async_busy_for_reset_draft(host) is True


def test_schedule_post_login_phase4_invokes_safe_invoke_when_pending():
    class Host:
        def __init__(self) -> None:
            self.calls: list[str] = []
            self._memory_restore_in_progress = True
            self._restore_destination_overlay_pending = True
            self._restore_abort_mode = False

        def _restore_abort_active(self) -> bool:
            return bool(self._restore_abort_mode)

        def _import_ok_trace(self, *args, **kwargs) -> None:
            return None

        def _import_ok_timing(self, *args, **kwargs) -> None:
            return None

        def _run_after_import_success_dialog_idle(self, callback_name, fn, *, delay_ms=50) -> None:
            self._safe_invoke(callback_name, fn)

        def _post_login_restore_phase4(self) -> None:
            pass

        def _safe_invoke(self, name, fn) -> None:
            self.calls.append(str(name))

    host = Host()
    MainWindow._schedule_post_login_restore_phase4_if_pending(host)
    assert host.calls == ["phase4_destination_overlay_after_destination_ui"]


def test_materialize_runs_overlay_pass_while_bind_sync_flag_stale():
    """Sync future-model bind was removed; legacy bind-sync flag must not block overlay refresh."""
    host = _MaterializeSkipHost()
    host._destination_future_bind_sync_active = True
    host.destination_tree_widget = None
    out = MainWindow._apply_destination_planning_overlays(host, "folder_worker_success", allow_defer=False)
    assert out == 0
    assert host.cancel_projection_calls == ["folder_worker_success"]
    assert any(p == "destination_planning_overlay_pass" for p, _d in host.logged)
