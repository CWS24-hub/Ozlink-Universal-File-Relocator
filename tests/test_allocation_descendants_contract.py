"""Contract tests: planned-allocation descendant projection vs explicit child rows."""

from __future__ import annotations

from unittest.mock import MagicMock

from PySide6.QtCore import QModelIndex

from ozlink_console.main_window import MainWindow


def test_invalidate_projection_index_clears_allocation_descendants_applied_on_path_mismatch():
    mw = MainWindow.__new__(MainWindow)
    move = {
        "destination_path": "\\Root",
        "target_name": "AllocFolder",
        "source": {"is_folder": True, "name": "AllocFolder"},
    }
    nd = {
        "node_origin": "plannedallocation",
        "is_folder": True,
        "children_loaded": True,
        "allocation_descendants_applied": True,
        "allocation_projection_destination_path_saved": "\\Root\\WrongName",
        "allocation_projection_resume_source_token": "abc",
        "allocation_projection_resume_desc_index": 3,
        "allocation_projection_resume_descendants_total": 10,
    }
    bad_ix = QModelIndex()
    result = mw._invalidate_stale_destination_allocation_projection_index(bad_ix, nd, move)
    assert result.get("children_loaded") is False
    assert "allocation_descendants_applied" not in result
    assert "allocation_projection_resume_source_token" not in result


def test_mark_allocation_descendants_applied_updates_model_payload():
    mw = MainWindow.__new__(MainWindow)
    model = MagicMock()
    mw.destination_planning_model = model
    ix = MagicMock()
    ix.isValid.return_value = True
    ix.column.return_value = 0
    ix.siblingAtColumn.return_value = ix
    mw._mark_allocation_descendants_applied_on_allocation_folder_model_index(ix, None)
    model.update_payload_for_index.assert_called_once()
    args, _kw = model.update_payload_for_index.call_args
    assert args[0] is ix
    mut = args[1]
    p = {}
    mut(p)
    assert p.get("allocation_descendants_applied") is True


def test_apply_visible_model_branch_uses_flag_not_child_presence():
    """Explicit children must not skip descendant apply; only ``allocation_descendants_applied`` may."""
    mw = MainWindow.__new__(MainWindow)
    tree = MagicMock()
    mw.destination_tree_widget = tree
    mw._destination_descendant_apply_state = None
    mw._destination_descendant_apply_queue = []

    dmodel = MagicMock()
    mw.destination_planning_model = dmodel

    ix = MagicMock()
    ix.isValid.return_value = True
    ix.data.return_value = {
        "node_origin": "plannedallocation",
        "is_folder": True,
        "allocation_descendants_applied": False,
    }
    move = {"destination_path": "\\D", "target_name": "F", "source": {"is_folder": True, "name": "F"}}

    _find_pass = {"n": 0}

    def _find(_p):
        _find_pass["n"] += 1
        return [ix] if _find_pass["n"] == 1 else []

    dmodel.find_indices_for_canonical_destination_path = _find
    dmodel.is_index_live = lambda _i: True

    mw._build_planned_move_destination_lookup = lambda: {"alloc_by_path": {}}  # noqa: E731
    mw._destination_model_build_allocation_apply_pairs = lambda _m, _l: [("\\D", move)]  # noqa: E731
    mw._destination_bind_normalized_expanded_targets = lambda _ep: {"\\Alloc"}  # noqa: E731
    mw._destination_semantic_path = lambda _nd: "\\Alloc"  # noqa: E731
    mw._destination_bind_allocation_descendants_eager_active = lambda: True  # noqa: E731
    mw._destination_bind_should_apply_allocation_descendants_now = lambda _sem, _nt: True  # noqa: E731
    applied = {"n": 0}

    def _fake_apply(_parent, _mv, **_kw):
        applied["n"] += 1
        return 3

    mw._apply_allocation_descendants_to_model_index = _fake_apply

    mw._apply_visible_destination_allocation_descendants(destination_expanded_paths={"\\Alloc"})
    assert applied["n"] == 1
    dmodel.rowCount.assert_not_called()


def test_apply_visible_model_skips_apply_when_descendants_already_applied():
    mw = MainWindow.__new__(MainWindow)
    tree = MagicMock()
    mw.destination_tree_widget = tree
    mw._destination_descendant_apply_state = None
    mw._destination_descendant_apply_queue = []

    dmodel = MagicMock()
    mw.destination_planning_model = dmodel

    ix = MagicMock()
    ix.isValid.return_value = True
    ix.data.return_value = {
        "node_origin": "plannedallocation",
        "is_folder": True,
        "allocation_descendants_applied": True,
        "children_loaded": False,
    }
    move = {"destination_path": "\\D", "target_name": "F", "source": {"is_folder": True, "name": "F"}}

    _find_pass_b = {"n": 0}

    def _find_b(_p):
        _find_pass_b["n"] += 1
        return [ix] if _find_pass_b["n"] == 1 else []

    dmodel.find_indices_for_canonical_destination_path = _find_b
    dmodel.is_index_live = lambda _i: True

    mw._build_planned_move_destination_lookup = lambda: {"alloc_by_path": {}}  # noqa: E731
    mw._destination_model_build_allocation_apply_pairs = lambda _m, _l: [("\\D", move)]  # noqa: E731
    mw._destination_bind_normalized_expanded_targets = lambda _ep: {"\\Alloc"}  # noqa: E731
    mw._destination_semantic_path = lambda _nd: "\\Alloc"  # noqa: E731
    mw._destination_bind_allocation_descendants_eager_active = lambda: True  # noqa: E731
    mw._destination_bind_should_apply_allocation_descendants_now = lambda _sem, _nt: True  # noqa: E731

    def _fail_apply(*_a, **_k):
        raise AssertionError("_apply_allocation_descendants_to_model_index should not run when flag is set")

    mw._apply_allocation_descendants_to_model_index = _fail_apply

    mw._apply_visible_destination_allocation_descendants(destination_expanded_paths={"\\Alloc"})
    dmodel.update_payload_for_index.assert_called_once()
    mut = dmodel.update_payload_for_index.call_args[0][1]
    p = {"node_origin": "plannedallocation", "is_folder": True, "allocation_descendants_applied": True}
    mut(p)
    assert p.get("children_loaded") is True


def test_allocation_apply_one_index_per_resolve_second_pass_after_first_removed_from_resolve():
    """After first mutation, re-resolve must not reuse a stale sibling QModelIndex from the prior list."""
    mw = MainWindow.__new__(MainWindow)
    dmodel = MagicMock()
    ix1, ix2 = MagicMock(), MagicMock()
    ix1.isValid.return_value = True
    ix2.isValid.return_value = True
    phase = {"n": 0}

    def _find(_p):
        if phase["n"] == 0:
            return [ix1, ix2]
        if phase["n"] == 1:
            return [ix2]
        return []

    dmodel.find_indices_for_canonical_destination_path = _find
    dmodel.is_index_live = lambda _i: True
    applied = []

    def _pred(_ix, nd):
        return nd.get("work") is True

    ix1.data.return_value = {"work": True}
    ix2.data.return_value = {"work": True}

    def _apply(ix, _nd):
        applied.append(ix)
        if ix is ix1:
            phase["n"] = 1
        elif ix is ix2:
            phase["n"] = 2

    n = mw._destination_allocation_apply_canonical_path_one_index_per_resolve(
        dmodel,
        "\\Canon",
        predicate=_pred,
        apply_once=_apply,
    )
    assert n == 2
    assert applied == [ix1, ix2]


def test_allocation_apply_one_index_per_resolve_read_only_scan_skips_completed_first_row():
    """Fresh list each round: first row may no longer qualify; scan picks the next qualifying index."""
    mw = MainWindow.__new__(MainWindow)
    dmodel = MagicMock()
    ix1, ix2 = MagicMock(), MagicMock()
    ix1.isValid.return_value = True
    ix2.isValid.return_value = True
    dmodel.find_indices_for_canonical_destination_path = lambda _p: [ix1, ix2]
    dmodel.is_index_live = lambda _i: True
    nd1 = {"work": True, "row": 1}
    nd2 = {"work": True, "row": 2}
    ix1.data.return_value = nd1
    ix2.data.return_value = nd2
    applied = []

    def _pred(_ix, nd):
        return nd.get("work") is True

    def _apply(ix, nd):
        applied.append(ix)
        if nd.get("row") == 1:
            nd1["work"] = False
            ix1.data.return_value = dict(nd1)
        elif nd.get("row") == 2:
            nd2["work"] = False
            ix2.data.return_value = dict(nd2)

    n = mw._destination_allocation_apply_canonical_path_one_index_per_resolve(
        dmodel,
        "\\Canon",
        predicate=_pred,
        apply_once=_apply,
    )
    assert n == 2
    assert applied == [ix1, ix2]
