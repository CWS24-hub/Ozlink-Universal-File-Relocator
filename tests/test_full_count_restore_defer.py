"""Full library count is skipped during memory restore and flushed after finalize."""

from __future__ import annotations

from ozlink_console.main_window import MainWindow


def test_schedule_full_count_skips_while_memory_restore_in_progress():
    mw = MainWindow.__new__(MainWindow)
    mw._memory_restore_in_progress = True
    mw._memory_restore_background_trees = False
    mw.planned_moves = []
    mw._expand_all_pending = {"source": False, "destination": False}
    mw._source_restore_materialization_queue = []
    started: list[str] = []

    def _fake_start(drive_id: str):
        started.append(drive_id)

    mw.start_full_count_worker = _fake_start  # type: ignore[method-assign]
    mw._update_source_count_labels = lambda *_a, **_k: None  # type: ignore[method-assign]
    mw.count_tree_items = lambda *_a, **_k: 0  # type: ignore[method-assign]
    mw.source_tree_widget = None

    MainWindow._schedule_full_count_with_restore_backoff(mw, "drive-a")
    assert started == []
    assert mw._full_count_pending_drive_id_after_restore == "drive-a"


def test_schedule_full_count_skips_while_restore_background_trees():
    mw = MainWindow.__new__(MainWindow)
    mw._memory_restore_in_progress = False
    mw._memory_restore_background_trees = True
    mw.planned_moves = []
    mw._expand_all_pending = {"source": False, "destination": False}
    mw._source_restore_materialization_queue = []
    started: list[str] = []

    mw.start_full_count_worker = lambda d: started.append(d)  # type: ignore[method-assign]
    mw._update_source_count_labels = lambda *_a, **_k: None  # type: ignore[method-assign]
    mw.count_tree_items = lambda *_a, **_k: 0  # type: ignore[method-assign]
    mw.source_tree_widget = None

    MainWindow._schedule_full_count_with_restore_backoff(mw, "drive-b")
    assert started == []
    assert mw._full_count_pending_drive_id_after_restore == "drive-b"


def test_start_full_count_worker_noop_while_restore_active():
    mw = MainWindow.__new__(MainWindow)
    mw._memory_restore_in_progress = True
    mw._memory_restore_background_trees = False
    mw.full_count_worker = None
    mw._full_count_sequence = 0
    mw._active_full_count_worker_id = 0
    mw._retired_full_count_workers = {}
    mw.graph = None  # would break if worker started
    mw._update_source_count_labels = lambda *_a, **_k: None  # type: ignore[method-assign]
    mw.count_tree_items = lambda *_a, **_k: 3  # type: ignore[method-assign]
    mw.source_tree_widget = object()

    MainWindow.start_full_count_worker(mw, "drive-c")
    assert mw.full_count_worker is None
    assert mw._full_count_pending_drive_id_after_restore == "drive-c"


def test_graph_count_shows_deferred_when_pending_during_restore():
    mw = MainWindow.__new__(MainWindow)
    mw._memory_restore_in_progress = True
    mw._full_count_pending_drive_id_after_restore = "d1"
    mw._current_selected_source_drive_id = lambda: "d1"  # type: ignore[method-assign]
    mw.full_source_file_count = None
    mw._full_count_completed_drive_id = ""
    mw._full_count_error_message = ""
    mw._full_count_requested_drive_id = ""
    mw._sharepoint_lazy_mode = False
    assert MainWindow._graph_file_count_display(mw) == "Deferred"
    assert MainWindow._graph_folder_count_display(mw) == "Deferred"


def test_flush_pending_schedules_when_drive_still_selected():
    mw = MainWindow.__new__(MainWindow)
    mw._full_count_pending_drive_id_after_restore = "d1"
    mw._memory_restore_in_progress = False
    mw._memory_restore_background_trees = False
    scheduled: list[str] = []

    mw._current_selected_source_drive_id = lambda: "d1"  # type: ignore[method-assign]
    mw._schedule_full_count_with_restore_backoff = lambda d: scheduled.append(d)  # type: ignore[method-assign]

    MainWindow._flush_pending_source_full_count_after_memory_restore(mw)
    assert scheduled == ["d1"]
    assert mw._full_count_pending_drive_id_after_restore == ""


def test_flush_pending_clears_when_library_changed():
    mw = MainWindow.__new__(MainWindow)
    mw._full_count_pending_drive_id_after_restore = "old"
    mw._current_selected_source_drive_id = lambda: "new"  # type: ignore[method-assign]
    mw._update_source_count_labels = lambda *_a, **_k: None  # type: ignore[method-assign]
    mw.count_tree_items = lambda *_a, **_k: 0  # type: ignore[method-assign]
    mw.source_tree_widget = None
    scheduled: list[str] = []
    mw._schedule_full_count_with_restore_backoff = lambda d: scheduled.append(d)  # type: ignore[method-assign]

    MainWindow._flush_pending_source_full_count_after_memory_restore(mw)
    assert scheduled == []
    assert mw._full_count_pending_drive_id_after_restore == ""


def test_enter_restore_abort_mode_clears_pending_full_count():
    mw = MainWindow.__new__(MainWindow)
    mw._restore_abort_mode = False
    mw._restore_abort_reason = ""
    mw._restore_destination_overlay_pending = False
    mw._restore_finalization_deferred_active = False
    mw._restore_finalization_deferred_reason = ""
    mw._destination_restore_materialization_queue = []
    mw._destination_restore_materialization_seen = set()
    mw._source_restore_materialization_queue = []
    mw._source_restore_materialization_seen = set()
    mw._source_projection_refresh_paths = set()
    mw._deferred_planning_refresh_pending = False
    mw._deferred_planning_refresh_reasons = []
    mw._deferred_source_projection_paths = set()
    mw._memory_restore_in_progress = True
    mw._memory_restore_background_trees = True
    mw._full_count_pending_drive_id_after_restore = "pending-drive"
    mw._suppress_selector_change_handlers = False
    mw.source_tree_widget = None
    mw.count_tree_items = lambda *_a, **_k: 0  # type: ignore[method-assign]
    updated: list[int] = []

    def _capture_labels(loaded: int):
        updated.append(int(loaded))

    mw._update_source_count_labels = _capture_labels  # type: ignore[method-assign]
    mw.isVisible = lambda: False  # type: ignore[method-assign]

    MainWindow._enter_restore_abort_mode(mw, "unit_test", phase="test_full_count_restore_defer")
    assert mw._full_count_pending_drive_id_after_restore == ""
    assert updated == [0]
    assert mw._restore_abort_mode is True
