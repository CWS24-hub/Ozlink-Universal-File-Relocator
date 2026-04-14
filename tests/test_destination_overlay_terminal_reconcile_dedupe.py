from __future__ import annotations

"""Global planned-parent reconcile must run once per overlay stack when authority shell nests materialize."""

from ozlink_console.main_window import MainWindow
from tests.test_main_window_restore_guards import _MaterializeSkipHost


def test_single_overlay_pass_runs_global_planned_reconcile_once():
    host = _MaterializeSkipHost()
    host.destination_tree_widget = None
    host.reconcile_count = 0

    def _rec():
        host.reconcile_count += 1

    host._destination_reconcile_all_planned_parents_after_graph_update = _rec
    MainWindow._apply_destination_planning_overlays(host, "folder_worker_success", allow_defer=False)
    assert host.reconcile_count == 1


def test_authority_shell_nested_overlay_does_not_triple_global_planned_reconcile():
    """Regression: outer overlay + force_clear nested apply used to reconcile three times."""
    host = _MaterializeSkipHost()
    host.destination_tree_widget = None
    host.reconcile_count = 0

    def _rec():
        host.reconcile_count += 1

    host._destination_reconcile_all_planned_parents_after_graph_update = _rec
    host._planning_browse_mode = lambda key: "sharepoint" if key == "destination" else "local"
    host._destination_sharepoint_planning_destination_active = lambda: True
    host._destination_graph_enumeration_ignores_memory_restore_gate = lambda: True
    host._destination_root_bind_is_authoritative = lambda: True
    host._destination_authority_pending_shell = True
    host._destination_tree_shows_authority_pending_shell = lambda: True
    host._destination_model_has_authority_pending_placeholder_rows = lambda: False
    host._destination_clear_restore_blockers_after_authoritative_bind = lambda *_a, **_k: None
    host._destination_resume_snapshot_branch_refresh_after_authority = lambda: None
    host._destination_try_clear_stale_foreground_memory_restore_gate = lambda *_a, **_k: None

    host._destination_on_authoritative_tree_bind_committed = (
        lambda bk: MainWindow._destination_on_authoritative_tree_bind_committed(host, bk)
    )

    MainWindow._apply_destination_planning_overlays(host, "folder_worker_success", allow_defer=False)
    assert host.reconcile_count == 1
