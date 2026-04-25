from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import ozlink_console.main_window as mw
from ozlink_console.main_window import MainWindow


class _Timer:
    def __init__(self) -> None:
        self.starts: list[int | None] = []

    def start(self, ms: int = 0) -> None:
        self.starts.append(int(ms) if ms is not None else 0)

    def isActive(self) -> bool:  # noqa: D401
        return False

    def stop(self) -> None:
        pass


class _RehydrateHost:
    """Minimal host for post-shell rehydrate tick / finish (no Qt model)."""

    def __init__(self) -> None:
        self._application_shutting_down = False
        self._destination_post_shell_memory_rehydrate_running = True
        self._destination_post_shell_rehydrate_max_branches_per_tick = 1
        self._destination_post_shell_rehydrate_max_ms_per_tick = 2000
        self._destination_post_shell_rehydrate_max_rows_per_tick = 400
        self._destination_post_shell_rehydrate_total_ticks = 0
        self._destination_post_shell_rehydrate_chunk_timer = _Timer()
        self._destination_post_shell_rich_rehydrate_scan_ran = False
        self._recovery_checkpoint_startup_work_gate_resolved = True
        self.finish_calls: list[tuple] = []
        self._destination_post_shell_memory_rehydrate_chunk_state: dict | None = None

    def _destination_post_shell_rehydrate_recovery_checkpoint_would_block(self) -> bool:
        return False

    def _destination_post_shell_rehydrate_process_single_path(
        self, pth: str, *, shell_cf, t_src, child_index_start: int = 0, max_row_nodes=None
    ) -> tuple[dict, bool]:
        if not hasattr(self, "_ps_calls"):
            self._ps_calls: list[str] = []
        self._ps_calls.append(str(pth))
        return ({"inserted_rows": 1, "upgraded_rows": 0, "partial_pending": False}, False)

    def _destination_finish_post_shell_memory_rehydrate(
        self,
        reason: str,
        library_identity: str,
        n_rehydrated: int,
        n_scanned: int,
        n_sk: int,
        t_wall0: float,
        last_tick_ms: float,
        from_exception: bool,
    ) -> None:
        self.finish_calls.append(
            (reason, library_identity, n_rehydrated, n_scanned, n_sk, t_wall0, last_tick_ms, from_exception)
        )
        self._destination_post_shell_memory_rehydrate_running = False
        self._destination_post_shell_memory_rehydrate_chunk_state = None


def test_post_shell_rehydrate_tick_takes_two_ticks_before_finish(monkeypatch) -> None:
    host = _RehydrateHost()
    t0 = time.perf_counter()
    host._destination_post_shell_memory_rehydrate_chunk_state = {
        "library_identity": "K",
        "reason": "test",
        "paths": ["/a", "/b"],
        "path_index": 0,
        "n_sk": 1,
        "n_rh": 0,
        "n_rows_tick": 0,
        "t_src": "snap",
        "shell_cf": set(),
        "t_wall0": t0,
        "total_ticks": 0,
        "total_elapsed_ms": 0.0,
        "partial": None,
    }
    log_events: list = []
    monkeypatch.setattr(mw, "log_info", lambda name, **kw: log_events.append((name, kw)) or None)

    MainWindow._destination_run_post_shell_memory_rehydrate_tick(host)  # type: ignore[misc]
    assert len(getattr(host, "_ps_calls", [])) == 1
    assert not host.finish_calls, "one branch per tick => not done"

    MainWindow._destination_run_post_shell_memory_rehydrate_tick(host)  # type: ignore[misc]
    assert len(host._ps_calls) == 2
    assert len(host.finish_calls) == 1, "second tick completes rehydration"


def test_post_shell_rehydrate_does_not_duplicate_path_when_partial_pends() -> None:
    """Second tick resumes the same path when partial_pending is set."""

    class H(_RehydrateHost):
        def _destination_post_shell_rehydrate_process_single_path(
            self, pth, *, shell_cf, t_src, child_index_start: int = 0, max_row_nodes=None
        ) -> tuple[dict, bool]:
            if not hasattr(self, "n"):
                self.n = 0
            self.n += 1
            if self.n == 1:
                return (
                    {
                        "inserted_rows": 0,
                        "upgraded_rows": 0,
                        "partial_pending": True,
                        "next_child_index": 3,
                    },
                    True,
                )
            return ({"inserted_rows": 2, "upgraded_rows": 0, "partial_pending": False}, False)

    host = H()
    host._destination_post_shell_memory_rehydrate_chunk_state = {
        "library_identity": "K",
        "reason": "test",
        "paths": ["/a"],
        "path_index": 0,
        "n_sk": 0,
        "n_rh": 0,
        "t_src": "",
        "shell_cf": set(),
        "t_wall0": time.perf_counter(),
        "total_ticks": 0,
        "partial": None,
    }
    MainWindow._destination_run_post_shell_memory_rehydrate_tick(host)  # type: ignore[misc]
    assert host.n == 1
    assert (host._destination_post_shell_memory_rehydrate_chunk_state or {}).get("partial")
    MainWindow._destination_run_post_shell_memory_rehydrate_tick(host)  # type: ignore[misc]
    assert host.n == 2
    assert host.finish_calls


def test_chunked_startup_shell_work_active() -> None:
    h1 = _RehydrateHost()
    h1._destination_post_shell_memory_rehydrate_running = True
    assert MainWindow._destination_chunked_startup_shell_work_active(h1) is True  # type: ignore[misc]
    h2 = _RehydrateHost()
    h2._destination_post_shell_memory_rehydrate_running = False
    h2._destination_planning_overlay_gui_chunk_state = {"cold_start_materialize": True}
    h2._destination_startup_phase_active = True
    assert MainWindow._destination_chunked_startup_shell_work_active(h2) is True  # type: ignore[misc]
    h3 = _RehydrateHost()
    h3._destination_post_shell_memory_rehydrate_running = False
    h3._destination_planning_overlay_gui_chunk_state = {"cold_start_materialize": True}
    h3._destination_startup_phase_active = False
    assert MainWindow._destination_chunked_startup_shell_work_active(h3) is False  # type: ignore[misc]


def test_gui_chunk_tick_deferred_when_recovery_gate(monkeypatch) -> None:
    st = {
        "gen": 1,
        "phase": 0,
        "reason": "r",
        "allow_defer": True,
        "prefer_chunked_projection": False,
        "narrow_restore_real_snapshot": False,
        "cold_start_materialize": True,
        "chunk_seq": 0,
        "t_pass0": time.perf_counter(),
    }
    host = MagicMock()
    host._application_shutting_down = False
    host._destination_planning_overlay_gui_chunk_state = st
    host._destination_planning_overlay_gui_chunk_gen = 1
    t = _Timer()
    host._destination_planning_overlay_gui_chunk_timer = t
    log: list = []
    monkeypatch.setattr(mw, "log_info", lambda n, **k: log.append((n, k)))
    with patch.object(
        MainWindow, "_destination_post_shell_rehydrate_recovery_checkpoint_would_block", return_value=True
    ):
        with patch("ozlink_console.main_window._shutdown_mutation_skip_for_host", return_value=False):
            MainWindow._destination_planning_overlay_body_gui_chunk_tick(host)  # type: ignore[misc]
    assert t.starts, "deferred: timer re-armed"
    assert any(x[0] == "destination_startup_work_deferred_for_recovery_prompt" for x in log)
    assert st["chunk_seq"] == 0, "no work slice while gate blocks"


@patch("ozlink_console.main_window.destination_authority_contract")
def test_maybe_finish_blocks_while_cold_chunk_running(mock_dac) -> None:
    mock_dac.graph_owns_visible_real_destination_structure.return_value = True
    mark = MagicMock()
    host = SimpleNamespace(
        _application_shutting_down=False,
        _destination_post_shell_memory_rehydrate_done_key="K",
        _destination_post_shell_memory_rehydrate_running=False,
        _destination_graph_startup_phase_completion_key="",
        _destination_startup_phase_active=True,
        _destination_expand_user_deferred_queue=[],
        _destination_graph_startup_deferred_queue_suppressed_for_phase_completion=False,
        _destination_planning_overlay_gui_chunk_state={"cold_start_materialize": True},
        _destination_post_shell_library_identity=lambda: "K",
        _current_selected_destination_drive_id=lambda: "D",
        _destination_mark_startup_phase_complete=mark,
    )
    host._destination_graph_startup_shell_bound_for_phase_completion = lambda: True
    MainWindow._destination_maybe_finish_graph_startup_phase(  # type: ignore[misc]
        host, reason="t"
    )
    mark.assert_not_called()


@patch("ozlink_console.main_window.destination_authority_contract")
def test_maybe_finish_completes_when_cold_done(mock_dac) -> None:
    mock_dac.graph_owns_visible_real_destination_structure.return_value = True
    mark = MagicMock()
    host = SimpleNamespace(
        _application_shutting_down=False,
        _destination_post_shell_memory_rehydrate_done_key="K",
        _destination_post_shell_memory_rehydrate_running=False,
        _destination_graph_startup_phase_completion_key="",
        _destination_startup_phase_active=True,
        _destination_expand_user_deferred_queue=[],
        _destination_graph_startup_deferred_queue_suppressed_for_phase_completion=False,
        _destination_planning_overlay_gui_chunk_state=None,
        _destination_post_shell_library_identity=lambda: "K",
        _current_selected_destination_drive_id=lambda: "D",
        _destination_mark_startup_phase_complete=mark,
    )
    host._destination_graph_startup_shell_bound_for_phase_completion = lambda: True
    MainWindow._destination_maybe_finish_graph_startup_phase(  # type: ignore[misc]
        host, reason="t"
    )
    mark.assert_called_once()
