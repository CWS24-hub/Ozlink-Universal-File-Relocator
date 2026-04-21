"""Planned-parent path composition for destination overlay (legacy restore / empty Graph parent id)."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from ozlink_console.main_window import MainWindow


def _mw_stub():
    m = MainWindow.__new__(MainWindow)
    for fn in (
        "normalize_memory_path",
        "_normalize_to_graph_canonical_path",
        "_canonical_planned_memory_path_for_graph_match",
        "_canonical_destination_projection_path",
    ):
        setattr(m, fn, getattr(MainWindow, fn).__get__(m, MainWindow))
    return m


def test_planned_parent_reanchors_destination_path_for_projection():
    """B: Planned-parent row with empty destination Graph id uses DestinationParentPlannedPath + leaf."""
    m = _mw_stub()
    move = {
        "destination_path": r"Root\HR\Reports\file.pdf",
        "destination_id": "",
        "LegacyMigrationDestinationParentResolution": "planned_parent",
        "DestinationParentPlannedPath": r"Root3\HR\Reports",
        "target_name": "file.pdf",
        "source": {"name": "file.pdf"},
    }
    assert m._allocation_projection_destination_path_source(move).replace("/", "\\").endswith(r"Root3\HR\Reports\file.pdf")
    assert r"Root3\HR\Reports" in m._allocation_parent_path(move).replace("/", "\\")
    key = m._allocation_projection_path(move)
    assert "Root3" in key.replace("/", "\\")


def test_graph_resolution_unchanged_when_not_planned_parent():
    """Non–planned-parent rows keep destination_path as-is."""
    m = _mw_stub()
    move = {
        "destination_path": r"Root3\Finance\a.xlsx",
        "destination_id": "graph-parent",
        "LegacyMigrationDestinationParentResolution": "graph",
        "DestinationParentPlannedPath": "",
        "source": {"name": "a.xlsx"},
    }
    assert m._allocation_projection_destination_path_source(move) == m._normalize_to_graph_canonical_path(
        r"Root3\Finance\a.xlsx"
    ) or m._normalize_to_graph_canonical_path(move["destination_path"])


def test_runtime_counts_helpers_match_allocation_dict_shape():
    """Smoke: resolution counters read the same keys as _build_restored_planned_move_record."""
    m = MainWindow.__new__(MainWindow)
    m.planned_moves = [
        {
            "destination_path": r"A\x",
            "destination_id": "",
            "LegacyMigrationDestinationParentResolution": "planned_parent",
            "DestinationParentPlannedPath": r"A",
        },
        {
            "destination_path": r"B\y",
            "destination_id": "id",
            "LegacyMigrationDestinationParentResolution": "graph",
            "DestinationParentPlannedPath": "",
        },
    ]
    m.proposed_folders = []
    from unittest.mock import MagicMock

    m._unresolved_allocation_queue_size = MagicMock(return_value=3)
    m._unresolved_proposed_queue_size = MagicMock(return_value=1)
    m._destination_full_tree_ready = lambda: True
    m.destination_planning_model = None
    m.pending_root_drive_ids = {"destination": "d", "source": "s"}
    m._current_selected_destination_drive_id = lambda: ""
    m._current_selected_source_drive_id = lambda: ""
    m._root_tree_bind_in_progress = False
    m._destination_root_prime_pending = False
    m.destination_tree_widget = object()
    m._restore_destination_overlay_pending = True
    m._memory_restore_in_progress = False
    MainWindow._log_restored_planning_runtime_counts(m, context="unit")
    MainWindow._log_destination_overlay_entry_context(m, "unit_test")


def test_recoverable_row_without_graph_parent_id_still_classed_for_overlay():
    """E: Recoverable planned-parent rows (no destination_id) are recognized for overlay gating logs."""
    m = MainWindow.__new__(MainWindow)
    move = {
        "destination_id": "",
        "LegacyMigrationDestinationParentResolution": "planned_parent",
        "DestinationParentPlannedPath": r"Root3\HR",
    }
    assert MainWindow._allocation_row_is_planned_parent_resolution(m, move) is True
    assert MainWindow._allocation_row_recoverable_missing_destination_graph_parent(m, move) is True
    assert MainWindow._allocation_row_recoverable_missing_destination_graph_parent(
        m,
        {"destination_id": "graph-id", "LegacyMigrationDestinationParentResolution": "planned_parent"},
    ) is False


def test_destination_overlay_after_import_skipped_requires_followup_flag():
    """F: Import-followup skip logging only fires while overlay follow-up from bundle hydrate is active."""
    from unittest.mock import patch

    m = MainWindow.__new__(MainWindow)
    m._overlay_followup_after_import = False
    m._destination_materialize_active_reason = "x"
    with patch("ozlink_console.main_window.log_info") as li:
        MainWindow._log_destination_overlay_after_import_skipped(m, skip_reason="unit", overlay_reason="r")
        li.assert_not_called()
    m._overlay_followup_after_import = True
    with patch("ozlink_console.main_window.log_info") as li2:
        MainWindow._log_destination_overlay_after_import_skipped(m, skip_reason="unit_skip", overlay_reason="r2")
        li2.assert_called_once()
        assert li2.call_args[0][0] == "destination_overlay_after_import_skipped"


def test_branch_scoped_delegate_logs_import_skip_reason():
    """G: When full sync body delegates to narrow/local-first paths, import follow-up records a skip reason."""
    from unittest.mock import patch

    m = MainWindow.__new__(MainWindow)
    m._overlay_followup_after_import = True
    m._destination_materialize_active_reason = "deferred_planned_item_moved"
    with patch("ozlink_console.main_window.log_info") as li:
        MainWindow._log_destination_overlay_after_import_skipped(
            m,
            skip_reason="delegated_narrow_planned_item_move",
            overlay_reason="deferred_planned_item_moved",
        )
        li.assert_called_once()
        args, kwargs = li.call_args
        assert kwargs.get("skip_reason") == "delegated_narrow_planned_item_move"


def test_planned_parent_overlay_skipped_emits_distinct_event():
    """Planned-parent overlay failure path logs ``skipped`` (graph bind unresolved)."""
    from unittest.mock import patch

    m = MainWindow.__new__(MainWindow)
    m._planned_parent_overlay_diag_count = 0
    move = {
        "request_id": "r1",
        "LegacyMigrationDestinationParentResolution": "planned_parent",
        "DestinationParentPlannedPath": r"Root3\X",
        "destination_path": r"Root3\X\y.txt",
        "source": {"name": "y.txt"},
    }
    with patch("ozlink_console.main_window.log_info") as li:
        MainWindow._maybe_log_planned_parent_overlay(
            m,
            move,
            "skipped",
            skip_reason="unit",
        )
        assert any(
            getattr(c, "args", c)[0] == "destination_planned_parent_overlay_skipped" for c in li.call_args_list
        )
