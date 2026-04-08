"""Planning/execution consistency after destination-side planned or proposed-folder relocates."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from PySide6.QtCore import QModelIndex
from PySide6.QtWidgets import QApplication

from ozlink_console.main_window import MainWindow
from ozlink_console.transfer_manifest import _planned_move_to_step


def _qapp():
    return QApplication.instance() or QApplication([])


def test_rewrite_nonprimary_planned_moves_destination_prefix_updates_descendants():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    primary = {
        "source": {
            "name": "Dept",
            "display_path": "S\\Dept",
            "item_path": "S\\Dept",
            "is_folder": True,
        },
        "source_path": "S\\Dept",
        "destination_path": "Root\\Archive",
        "target_name": "Dept",
        "destination": {
            "display_path": "Root\\Archive\\Dept",
            "item_path": "Root\\Archive\\Dept",
        },
    }
    child = {
        "source": {
            "name": "a.txt",
            "display_path": "S\\Dept\\a.txt",
            "item_path": "S\\Dept\\a.txt",
            "is_folder": False,
        },
        "source_path": "S\\Dept\\a.txt",
        "destination_path": "Root\\HR\\Dept\\sub",
        "target_name": "a.txt",
        "destination": {
            "display_path": "Root\\HR\\Dept\\sub\\a.txt",
            "item_path": "Root\\HR\\Dept\\sub\\a.txt",
        },
    }
    mw.planned_moves = [primary, child]
    mw._is_move_submitted = lambda _m: False
    out = mw._rewrite_nonprimary_planned_moves_destination_prefix(
        "Root\\HR\\Dept", "Root\\Archive\\Dept", primary_move=primary
    )
    assert child in out
    assert child["destination_path"] == "Root\\Archive\\Dept\\sub"
    assert child["target_name"] == "a.txt"
    assert child["destination"]["item_path"] == "Root\\Archive\\Dept\\sub\\a.txt"
    step = _planned_move_to_step(0, child)
    assert step.destination_path == "Root\\Archive\\Dept\\sub"
    assert step.destination_name == "a.txt"


def test_expand_source_projection_paths_for_move_network_includes_descendant_moves():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    folder = {
        "source": {
            "name": "Dept",
            "display_path": "Lib\\Dept",
            "item_path": "Lib\\Dept",
            "is_folder": True,
        },
        "source_path": "Lib\\Dept",
        "destination_path": "D\\Dept",
        "target_name": "Dept",
    }
    nested = {
        "source": {
            "name": "f.txt",
            "display_path": "Lib\\Dept\\f.txt",
            "item_path": "Lib\\Dept\\f.txt",
        },
        "source_path": "Lib\\Dept\\f.txt",
        "destination_path": "D\\Dept",
        "target_name": "f.txt",
    }
    mw.planned_moves = [folder, nested]
    paths = mw._expand_source_projection_paths_for_move_network(folder)
    canon_dept = mw._canonical_source_projection_path("Lib\\Dept")
    canon_file = mw._canonical_source_projection_path("Lib\\Dept\\f.txt")
    assert canon_dept in paths
    assert canon_file in paths


def test_finalize_destination_move_planning_consistency_refreshes_source_projection():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    move = {
        "source_path": "S\\One",
        "source": {"display_path": "S\\One", "item_path": "S\\One", "is_folder": False},
        "destination_path": "D\\T",
        "target_name": "One",
    }
    mw.planned_moves = [move]
    mw._expand_source_projection_paths_for_move_network = MagicMock(return_value={"S\\One"})
    inv = MagicMock()
    ref = MagicMock()
    mw._invalidate_projection_lookup_caches = inv
    mw._refresh_source_projection_for_paths = ref
    mw.source_tree_widget = MagicMock()
    with patch("ozlink_console.main_window.is_dev_mode", return_value=False):
        mw._finalize_destination_move_planning_consistency(
            move,
            rewritten_related=[],
            old_destination_projection="D\\Old",
            new_destination_projection="D\\New",
            table_refreshed=True,
            incremental_lightweight=True,
            move_origin="other",
        )
    inv.assert_called_once_with(bump_generation=False)
    ref.assert_called_once()
    args, kwargs = ref.call_args
    assert "S\\One" in args[0]
    assert args[1] == "planned_destination_move_sync_light"


def test_finalize_destination_move_planning_consistency_does_not_call_full_persist():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    move = {
        "source_path": "S\\One",
        "source": {"display_path": "S\\One", "item_path": "S\\One"},
        "destination_path": "D\\T",
        "target_name": "One",
    }
    mw.planned_moves = [move]
    mw._expand_source_projection_paths_for_move_network = MagicMock(return_value=set())
    mw._invalidate_projection_lookup_caches = MagicMock()
    mw._refresh_source_projection_for_paths = MagicMock()
    mw.source_tree_widget = None
    mw._persist_planning_change = MagicMock()
    mw._persist_planning_change_lightweight = MagicMock()
    with patch("ozlink_console.main_window.is_dev_mode", return_value=False):
        mw._finalize_destination_move_planning_consistency(
            move,
            rewritten_related=[],
            old_destination_projection="a",
            new_destination_projection="b",
            table_refreshed=True,
            incremental_lightweight=True,
            move_origin="other",
        )
    mw._persist_planning_change.assert_not_called()
    mw._persist_planning_change_lightweight.assert_not_called()


def test_collect_source_projection_paths_under_destination_allocation_prefix():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    m1 = {
        "source_path": "S\\f",
        "source": {"display_path": "S\\f", "item_path": "S\\f"},
        "destination_path": "P\\Prop\\inner",
        "target_name": "f",
        "destination": {"display_path": "P\\Prop\\inner\\f", "item_path": "P\\Prop\\inner\\f"},
    }
    mw.planned_moves = [m1]
    s = mw._collect_source_projection_paths_under_destination_allocation_prefix("P\\Prop")
    canon = mw._canonical_source_projection_path("S\\f")
    assert canon in s


def test_moveaudit_logged_for_manual_drag_in_dev_mode():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    move = {
        "source_path": "S\\Item",
        "source": {"display_path": "S\\Item", "item_path": "S\\Item"},
        "destination_path": "D\\P",
        "target_name": "Item",
    }
    mw.planned_moves = [move]
    mw._expand_source_projection_paths_for_move_network = MagicMock(return_value={"S\\Item"})
    mw._invalidate_projection_lookup_caches = MagicMock()
    mw._refresh_source_projection_for_paths = MagicMock()
    mw.source_tree_widget = MagicMock()
    audits = []

    def _capture(msg, **data):
        if msg == "MOVEAUDIT":
            audits.append(data)

    with patch("ozlink_console.main_window.is_dev_mode", return_value=True):
        with patch("ozlink_console.main_window.log_info", side_effect=_capture):
            mw._finalize_destination_move_planning_consistency(
                move,
                rewritten_related=[],
                old_destination_projection="D\\Old\\Item",
                new_destination_projection="D\\New\\Item",
                table_refreshed=True,
                incremental_lightweight=True,
                move_origin="manual_drag",
            )
    assert len(audits) == 1
    assert audits[0].get("move_origin") == "manual_drag"
    assert audits[0].get("planned_move_map_updated") is True
    assert audits[0].get("source_traceability_updated") is True
    assert audits[0].get("planned_moves_table_updated") is True
    assert audits[0].get("execution_state_updated") is True


def test_moveaudit_logged_for_paste_here_in_dev_mode():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    move = {
        "source_path": "S\\Item",
        "source": {"display_path": "S\\Item", "item_path": "S\\Item"},
        "destination_path": "D\\P",
        "target_name": "Item",
    }
    mw.planned_moves = [move]
    mw._expand_source_projection_paths_for_move_network = MagicMock(return_value={"S\\Item"})
    mw._invalidate_projection_lookup_caches = MagicMock()
    mw._refresh_source_projection_for_paths = MagicMock()
    mw.source_tree_widget = MagicMock()
    audits = []

    def _capture(msg, **data):
        if msg == "MOVEAUDIT":
            audits.append(data)

    with patch("ozlink_console.main_window.is_dev_mode", return_value=True):
        with patch("ozlink_console.main_window.log_info", side_effect=_capture):
            mw._finalize_destination_move_planning_consistency(
                move,
                rewritten_related=[],
                old_destination_projection="D\\Old",
                new_destination_projection="D\\New",
                table_refreshed=True,
                incremental_lightweight=True,
                move_origin="paste_here",
            )
    assert len(audits) == 1
    assert audits[0].get("move_origin") == "paste_here"


def test_move_planned_destination_node_passes_move_origin_paste_and_manual():
    """Drag and paste both use the same finalize contract with distinct move_origin."""
    _qapp()
    from ozlink_console.main_window import MainWindow as MW

    def _run(with_paste: bool, with_manual: bool, expected_origin: str):
        mw = MW.__new__(MW)
        move = {
            "source_name": "Doc.xlsx",
            "target_name": "Doc.xlsx",
            "source_path": "Src\\Doc.xlsx",
            "destination_path": "Root\\OldParent",
            "destination_id": "d1",
            "destination_name": "OldParent",
            "destination": {
                "id": "d1",
                "name": "OldParent",
                "display_path": "Root\\OldParent",
                "item_path": "Root\\OldParent",
            },
            "source": {"name": "Doc.xlsx"},
            "status": "Draft",
        }
        mw.planned_moves = [move]
        mw._resolve_planned_move_for_destination_node = lambda node: (0, move, None)
        mw._is_move_submitted = lambda m: False
        mw._destination_row_semantic_path = lambda n: str(n.get("display_path") or n.get("item_path") or "")
        mw._find_visible_destination_item_by_path = lambda path: None
        mw._quick_remove_planned_move_from_destination_tree = lambda old: 0
        mw._reset_unresolved_allocation_queue = lambda: None
        mw._allocation_parent_candidates_for_touch_paths = lambda _t: {"Root\\NewParent"}
        mw._reapply_allocation_overlays_for_paste_touch_paths = lambda o, n, t: (1, {"Root\\NewParent": 1})
        mw._paths_equivalent = lambda a, b, role: str(a).replace("/", "\\") == str(b).replace("/", "\\")
        mw._schedule_deferred_destination_materialization = lambda *a, **k: None
        mw._persist_planning_change_lightweight = lambda **kw: None
        mw._bump_destination_materialized_overlay_fingerprint = lambda **kw: None
        mw._persist_planning_change = MagicMock()
        mw.refresh_planned_moves_table = lambda: None
        mw.planned_moves_status = MagicMock()
        mw.destination_tree_status = MagicMock()
        calls = []

        def _capture_finalize(primary_move, **kw):
            calls.append({"primary_move": primary_move, **kw})
            return frozenset(), True

        mw._finalize_destination_move_planning_consistency = _capture_finalize
        src = {"name": "Doc.xlsx", "display_path": "Root\\OldParent\\Doc.xlsx", "item_path": "Root\\OldParent\\Doc.xlsx"}
        tgt = {"name": "NewParent", "display_path": "Root\\NewParent", "item_path": "Root\\NewParent", "is_folder": True}
        mw._move_planned_destination_node(
            src, tgt, from_paste_here=with_paste, from_manual_planning_drag=with_manual
        )
        assert len(calls) == 1
        assert calls[0]["move_origin"] == expected_origin

    _run(True, False, "paste_here")
    _run(False, True, "manual_drag")


def test_finalize_returns_normalized_paths_and_did_sync_flag():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    move = {
        "source_path": "S\\One",
        "source": {"display_path": "S\\One", "item_path": "S\\One", "is_folder": False},
        "destination_path": "D\\T",
        "target_name": "One",
    }
    mw.planned_moves = [move]
    mw._expand_source_projection_paths_for_move_network = MagicMock(return_value={"S\\One"})
    mw._invalidate_projection_lookup_caches = MagicMock()
    mw._refresh_source_projection_for_paths = MagicMock()
    mw.source_tree_widget = MagicMock()
    canon = mw._canonical_source_projection_path("S\\One")
    with patch("ozlink_console.main_window.is_dev_mode", return_value=False):
        paths, did_sync = mw._finalize_destination_move_planning_consistency(
            move,
            rewritten_related=[],
            old_destination_projection="a",
            new_destination_projection="b",
            table_refreshed=True,
            incremental_lightweight=True,
            move_origin="manual_drag",
        )
    assert canon in paths
    assert did_sync is True
    mw.source_tree_widget = None
    with patch("ozlink_console.main_window.is_dev_mode", return_value=False):
        paths2, did_sync2 = mw._finalize_destination_move_planning_consistency(
            move,
            rewritten_related=[],
            old_destination_projection="a",
            new_destination_projection="b",
            table_refreshed=True,
            incremental_lightweight=True,
            move_origin="manual_drag",
        )
    assert canon in paths2
    assert did_sync2 is False


def test_persist_lightweight_deferred_paths_override_skips_full_collect():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.planned_moves = []
    mw._save_draft_shell = lambda force=True: None
    mw._rebuild_submission_visual_cache = lambda: None
    mw.update_progress_summaries = lambda: None
    mw._set_window_title_status = lambda *a, **k: None
    collect = MagicMock(side_effect=AssertionError("collect should not run"))
    mw._collect_current_source_projection_paths = collect
    q = MagicMock()
    mw._queue_deferred_planning_refresh = q
    with patch("ozlink_console.main_window.is_dev_mode", return_value=False):
        mw._persist_planning_change_lightweight(
            planning_refresh_reason="planned_item_moved_manual_drag",
            deferred_source_projection_paths=frozenset(["S\\Only"]),
        )
    collect.assert_not_called()
    q.assert_called_once()
    assert q.call_args.kwargs["source_projection_paths"] == {"S\\Only"}


def test_map_visible_source_items_uses_single_bulk_walk_for_multiple_index_misses():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    payloads = [
        {"display_path": "S\\a", "item_path": "S\\a", "tree_role": "source"},
        {"display_path": "S\\b", "item_path": "S\\b", "tree_role": "source"},
    ]
    idxs = []
    for i in range(len(payloads)):
        m_ix = MagicMock()
        m_ix.isValid.return_value = True
        m_ix._row_i = i
        idxs.append(m_ix)
    walk_calls = {"n": 0}

    def fake_iter_depth_first():
        walk_calls["n"] += 1
        yield from idxs

    model = MagicMock()
    model.find_index_for_canonical_source_path = MagicMock(return_value=QModelIndex())
    model.iter_depth_first = fake_iter_depth_first

    mw.source_tree_widget = MagicMock()
    mw.source_sharepoint_model = model
    mw._source_tree_uses_model_view = lambda: True
    mw.get_tree_item_node_data = lambda ix: payloads[getattr(ix, "_row_i", 0)]
    mw._canonical_source_projection_path = lambda p: str(p or "").replace("/", "\\") if p else ""
    mw._tree_item_path = lambda d: d.get("item_path", "") or ""
    mw._source_lookup_cache_get = lambda _p: None
    mw._source_lookup_cache_put = lambda *_a, **_k: None

    want = {mw._canonical_source_projection_path("S\\a"), mw._canonical_source_projection_path("S\\b")}
    out, stats = mw._map_visible_source_items_by_canonical_paths(want)
    assert walk_calls["n"] == 1
    assert stats["bulk_walk_used"] == 1
    assert stats["bulk_scan_nodes"] == 2
    assert len(out) == 2


def test_minimal_descendant_cover_paths_collapses_nested():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._path_segments = MainWindow._path_segments.__get__(mw, MainWindow)
    paths = {"R\\A", "R\\A\\B", "R\\A\\C"}
    cov = mw._minimal_descendant_cover_paths(paths)
    assert set(cov) == {"R\\A"}


def test_minimal_descendant_cover_paths_keeps_disjoint():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._path_segments = MainWindow._path_segments.__get__(mw, MainWindow)
    paths = {"R\\A", "R\\B", "R\\B\\X"}
    cov = set(mw._minimal_descendant_cover_paths(paths))
    assert cov == {"R\\A", "R\\B"}


def test_evaluate_source_relationship_direct_suffix_reflects_destination_name():
    """Relationship text is derived from current planned_moves (execution-safe display)."""
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    file_node = {
        "name": "Doc.txt",
        "display_path": "Lib\\Doc.txt",
        "item_path": "Lib\\Doc.txt",
        "id": "f1",
        "tree_role": "source",
    }
    mv = {
        "source_name": "Doc.txt",
        "destination_name": "OldDest",
        "target_name": "Doc.txt",
        "source_path": "Lib\\Doc.txt",
        "source": dict(file_node),
        "destination_path": "D\\T",
        "status": "Draft",
    }
    mw.planned_moves = [mv]
    r1 = mw._evaluate_source_relationship(file_node)
    assert "OldDest" in r1.get("suffix", "")
    mv["destination_name"] = "NewDest"
    r2 = mw._evaluate_source_relationship(file_node)
    assert "NewDest" in r2.get("suffix", "")
    assert "OldDest" not in r2.get("suffix", "")
