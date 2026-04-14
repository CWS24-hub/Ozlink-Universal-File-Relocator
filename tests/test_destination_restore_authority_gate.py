"""Stale _memory_restore_in_progress must not block destination reconcile under live Graph authority."""

from __future__ import annotations

import os
from unittest.mock import MagicMock

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


def _minimal_mw_graph_destination_authoritative():
    from ozlink_console.main_window import MainWindow

    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw.destination_planning_model = object()
    mw.unresolved_proposed_by_parent_path = {}
    mw.unresolved_allocations_by_parent_path = {}
    mw._restore_destination_overlay_pending = False
    mw._destination_restore_materialization_queue = []
    mw.root_load_workers = {}
    mw.pending_folder_loads = {"source": set(), "destination": set()}
    mw._destination_root_prime_pending = False
    mw._memory_restore_background_trees = False
    mw._destination_tree_shows_authority_pending_shell = lambda: False
    mw._destination_authority_pending_shell = False
    mw._destination_non_authoritative_shell_active = False
    tw = MagicMock()
    tw.topLevelItemCount.return_value = 1
    mw.destination_tree_widget = tw
    mw.loaded_root_request_signatures = {"destination": "sig"}
    mw.active_root_request_signatures = {"destination": "sig"}
    return mw


def test_graph_authority_supersedes_memory_restore_gate_predicate():
    from ozlink_console.main_window import MainWindow

    mw = _minimal_mw_graph_destination_authoritative()
    assert MainWindow._destination_graph_authority_supersedes_memory_restore_gate(mw) is True
    mw._destination_tree_shows_authority_pending_shell = lambda: True
    # Root bind is authoritative: stale authority-pending shell must not block supersede.
    assert MainWindow._destination_graph_authority_supersedes_memory_restore_gate(mw) is True


def test_authority_supersedes_true_when_root_authoritative_despite_pending_shell_core():
    from ozlink_console.main_window import MainWindow

    mw = _minimal_mw_graph_destination_authoritative()
    mw._destination_tree_shows_authority_pending_shell = lambda: True
    ok, reason = MainWindow._destination_graph_authority_supersedes_memory_restore_gate_core(mw)
    assert ok is True
    assert reason == ""


def test_authority_supersedes_false_when_pending_shell_and_root_not_authoritative():
    from ozlink_console.main_window import MainWindow

    mw = _minimal_mw_graph_destination_authoritative()
    mw.loaded_root_request_signatures = {}
    mw._destination_tree_shows_authority_pending_shell = lambda: True
    ok, reason = MainWindow._destination_graph_authority_supersedes_memory_restore_gate_core(mw)
    assert ok is False
    assert reason == "authority_pending_shell_visible"


def test_authority_supersedes_true_when_root_authoritative_despite_non_authoritative_shell_flag():
    from ozlink_console.main_window import MainWindow

    mw = _minimal_mw_graph_destination_authoritative()
    mw._destination_non_authoritative_shell_active = True
    ok, reason = MainWindow._destination_graph_authority_supersedes_memory_restore_gate_core(mw)
    assert ok is True
    assert reason == ""


def test_reconcile_not_deferred_when_memory_restore_but_graph_authority():
    from ozlink_console.main_window import MainWindow

    mw = _minimal_mw_graph_destination_authoritative()
    mw._memory_restore_in_progress = True
    mw.unresolved_allocations_by_parent_path = {"Hub": {"a": {"destination_path": "Hub\\X"}}}
    assert MainWindow._unresolved_allocation_queue_size(mw) > 0
    assert MainWindow._should_defer_destination_duplicate_reconcile(mw, "folder_worker_success") is False


def test_reconcile_still_deferred_when_memory_restore_and_not_authoritative():
    from ozlink_console.main_window import MainWindow

    mw = _minimal_mw_graph_destination_authoritative()
    mw._memory_restore_in_progress = True
    mw.loaded_root_request_signatures = {}
    mw.active_root_request_signatures = {}
    mw.unresolved_allocations_by_parent_path = {"Hub": {"a": {}}}
    assert MainWindow._should_defer_destination_duplicate_reconcile(mw, "folder_worker_success") is True


def test_full_tree_not_blocked_by_memory_when_graph_authority():
    from ozlink_console.main_window import MainWindow

    mw = _minimal_mw_graph_destination_authoritative()
    mw._memory_restore_in_progress = True
    mw._destination_sharepoint_root_graph_bound_drive_id = "d1"
    assert MainWindow._destination_full_tree_memory_restore_may_block_worker(mw, "d1") is False


def test_stale_memory_restore_cleared_when_authority_helpers_run():
    from ozlink_console.main_window import MainWindow

    mw = _minimal_mw_graph_destination_authoritative()
    mw._memory_restore_in_progress = True
    mw._finalize_memory_restore_if_ready = MagicMock(return_value=False)
    MainWindow._destination_try_clear_stale_foreground_memory_restore_gate(mw, "test_clear")
    assert mw._memory_restore_in_progress is False
    mw._finalize_memory_restore_if_ready.assert_called_once()


def test_force_clear_shell_clears_latches_when_root_authoritative():
    from unittest.mock import patch

    from ozlink_console.main_window import MainWindow

    mw = _minimal_mw_graph_destination_authoritative()
    mw._destination_authority_pending_shell = True
    mw._destination_non_authoritative_shell_active = True
    mw._destination_full_library_reconcile_pending = True
    with patch.object(MainWindow, "_apply_destination_planning_overlays", return_value=0):
        MainWindow._destination_force_clear_authority_shell_state_after_authoritative_bind(mw, "unit_test")
    assert mw._destination_authority_pending_shell is False
    assert mw._destination_non_authoritative_shell_active is False
    assert mw._destination_full_library_reconcile_pending is False


def test_maybe_deferred_runs_reconcile_under_authority_despite_memory_flag():
    from unittest.mock import patch

    from ozlink_console.main_window import MainWindow

    mw = _minimal_mw_graph_destination_authoritative()
    mw._memory_restore_in_progress = True
    mw._destination_semantic_reconcile_guard_depth = 0
    mw._destination_sharepoint_planning_destination_active = lambda: True
    mw._destination_full_tree_ready = lambda: True
    with patch.object(MainWindow, "_reconcile_destination_semantic_duplicates", return_value=3) as rec:
        n = MainWindow._reconcile_destination_semantic_duplicates_maybe_deferred(mw, "folder_worker_success")
    assert n == 3
    rec.assert_called_once()
