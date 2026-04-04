from __future__ import annotations

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
    """Minimal host for _materialize_destination_future_model early-exit tests."""

    def __init__(self) -> None:
        self._destination_future_model_last_blocked_source_restore = False
        self._destination_future_projection_async_state = None
        self._destination_chunked_bind_state = None
        self._destination_future_bind_sync_active = False
        self.cancel_projection_calls: list[str] = []
        self.logged: list[tuple[str, dict]] = []

    def _log_restore_phase(self, phase, **data):
        self.logged.append((phase, dict(data)))

    def _cancel_destination_future_async_projection(self, reason=""):
        self.cancel_projection_calls.append(str(reason or ""))

    def _materialize_destination_future_model_body(
        self, reason, *, allow_defer=True, prefer_chunked_projection=False, narrow_restore_real_snapshot=False
    ):
        return MainWindow._materialize_destination_future_model_body(
            self,
            reason,
            allow_defer=allow_defer,
            prefer_chunked_projection=prefer_chunked_projection,
            narrow_restore_real_snapshot=narrow_restore_real_snapshot,
        )


def test_materialize_skips_folder_worker_success_while_projection_merge_in_progress():
    host = _MaterializeSkipHost()
    host._destination_future_projection_async_state = {"reason": "folder_worker_success"}
    out = MainWindow._materialize_destination_future_model(host, "folder_worker_success")
    assert out == 0
    assert host.cancel_projection_calls == []
    assert any(
        p == "destination_future_model_materialize_skipped"
        and d.get("skip_reason") == "projection_merge_in_progress"
        for p, d in host.logged
    )


def test_materialize_skips_folder_worker_success_while_chunked_bind_in_progress():
    host = _MaterializeSkipHost()
    host._destination_chunked_bind_state = {"phase": "bind"}
    out = MainWindow._materialize_destination_future_model(host, "folder_worker_success")
    assert out == 0
    assert host.cancel_projection_calls == []
    assert any(
        p == "destination_future_model_materialize_skipped"
        and d.get("skip_reason") == "chunked_bind_in_progress"
        for p, d in host.logged
    )


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

        def _post_login_restore_phase4(self) -> None:
            pass

        def _safe_invoke(self, name, fn) -> None:
            self.calls.append(str(name))

    host = Host()
    MainWindow._schedule_post_login_restore_phase4_if_pending(host)
    assert host.calls == ["phase4_destination_overlay_after_destination_ui"]


def test_materialize_skips_folder_worker_success_while_bind_sync_active():
    host = _MaterializeSkipHost()
    host._destination_future_bind_sync_active = True
    out = MainWindow._materialize_destination_future_model(host, "folder_worker_success")
    assert out == 0
    assert host.cancel_projection_calls == []
    assert any(
        p == "destination_future_model_materialize_skipped"
        and d.get("skip_reason") == "bind_sync_in_progress"
        for p, d in host.logged
    )
