"""Reactive overlay invariant: correctness immediately on mutation, not only via QTimer."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt

from ozlink_console.main_window import MainWindow
from test_destination_source_descendant_projection import (
    _base_destination_root,
    _dest_child_names,
    _mw_graph_source_dest,
)


def _email_projection_fixture(monkeypatch):
    """Source subtree Mgmt\\Email\\Sub and a planned_folder row bound to that source."""
    mw, sm, dm = _mw_graph_source_dest(monkeypatch)
    mw._disable_overlay_invariant_timer_for_test = True

    fld = {
        "name": "Email",
        "is_folder": True,
        "item_path": "Mgmt\\Email",
        "drive_id": "sd1",
        "id": "e1",
        "children_loaded": True,
        "tree_role": "source",
    }
    subf = {
        "name": "Sub",
        "is_folder": True,
        "item_path": "Mgmt\\Email\\Sub",
        "drive_id": "sd1",
        "id": "e2",
        "children_loaded": True,
        "tree_role": "source",
    }
    sm.reset_root_payloads([fld])
    fld_ix = sm.index(0, 0, QModelIndex())
    sm.replace_all_children(fld_ix, [subf])

    root3 = dict(_base_destination_root())
    MainWindow._apply_tree_item_visual_state(mw, None, root3)
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())
    target = {
        "name": "Email attachments",
        "item_path": "Root3\\Management\\Email attachments",
        "destination_path": "Root3\\Management\\Email attachments",
        "tree_role": "destination",
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "workspace_planned_row": True,
        "is_folder": True,
        "children_loaded": False,
        "id": "",
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, target)
    dm.append_child_payloads(r_ix, [target])
    tgt_ix = dm.index(0, 0, r_ix)

    move = {
        "source_path": r"Mgmt\Email",
        "destination_path": r"Root3\Management\Email attachments",
        "target_name": "Email attachments",
        "source": {"is_folder": True, "name": "Email", "drive_id": "sd1", "id": "e1"},
        "request_id": "reactive-1",
    }
    return mw, dm, tgt_ix, move


def test_reactive_invariant_timers_disabled_no_manual_enforce(monkeypatch):
    """After planned_moves assignment, tree is correct with QTimer scheduling disabled."""
    mw, dm, tgt_ix, move = _email_projection_fixture(monkeypatch)
    mw.planned_moves = [move]
    assert "Sub" in _dest_child_names(dm, tgt_ix)
    assert MainWindow._enforce_destination_overlay_source_projection_invariant(mw, "post") == 0


def test_reactive_invariant_second_assign_re_runs_enforce(monkeypatch):
    """Replacing planned_moves via setter still leaves the tree correct with timers off (no stale window)."""
    mw, dm, tgt_ix, move = _email_projection_fixture(monkeypatch)
    mw.planned_moves = [move]
    assert "Sub" in _dest_child_names(dm, tgt_ix)

    mw.planned_moves = [dict(move, request_id="reactive-2")]
    assert "Sub" in _dest_child_names(dm, tgt_ix)
    assert MainWindow._enforce_destination_overlay_source_projection_invariant(mw, "tail") == 0


def test_reactive_invariant_assign_vs_extend_equivalent(monkeypatch):
    """bulk assign vs clear+extend both yield immediate projection."""
    mw, dm, tgt_ix, move = _email_projection_fixture(monkeypatch)

    mw.planned_moves = [dict(move)]
    names_assign = _dest_child_names(dm, tgt_ix)

    mw.planned_moves.clear()
    assert _dest_child_names(dm, tgt_ix) == []
    mw.planned_moves.extend([dict(move)])
    names_extend = _dest_child_names(dm, tgt_ix)

    assert names_assign == names_extend == ["Sub"]


def _two_branch_projection_fixture(monkeypatch):
    """Two folder moves: Email→Email attachments, Docs→Docs vault; distinct source subtrees."""
    mw, sm, dm = _mw_graph_source_dest(monkeypatch)
    mw._disable_overlay_invariant_timer_for_test = True

    email = {
        "name": "Email",
        "is_folder": True,
        "item_path": "Mgmt\\Email",
        "drive_id": "sd1",
        "id": "e1",
        "children_loaded": True,
        "tree_role": "source",
    }
    sub_e = {
        "name": "SubE",
        "is_folder": True,
        "item_path": "Mgmt\\Email\\SubE",
        "drive_id": "sd1",
        "id": "e1a",
        "children_loaded": True,
        "tree_role": "source",
    }
    docs = {
        "name": "Docs",
        "is_folder": True,
        "item_path": "Mgmt\\Docs",
        "drive_id": "sd1",
        "id": "d1",
        "children_loaded": True,
        "tree_role": "source",
    }
    sub_d = {
        "name": "SubD",
        "is_folder": True,
        "item_path": "Mgmt\\Docs\\SubD",
        "drive_id": "sd1",
        "id": "d1a",
        "children_loaded": True,
        "tree_role": "source",
    }
    sm.reset_root_payloads([email, docs])
    email_ix = sm.index(0, 0, QModelIndex())
    docs_ix = sm.index(1, 0, QModelIndex())
    sm.replace_all_children(email_ix, [sub_e])
    sm.replace_all_children(docs_ix, [sub_d])

    root3 = dict(_base_destination_root())
    MainWindow._apply_tree_item_visual_state(mw, None, root3)
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())

    def _target(name, leaf_segment):
        p = f"Root3\\Management\\{leaf_segment}"
        pl = {
            "name": name,
            "item_path": p,
            "destination_path": p,
            "tree_role": "destination",
            "row_kind": "planned_folder",
            "verification_state": "planned_only",
            "workspace_planned_row": True,
            "is_folder": True,
            "children_loaded": False,
            "id": "",
            "drive_id": "d1",
            "library_id": "d1",
        }
        MainWindow._apply_tree_item_visual_state(mw, None, pl)
        return pl

    t_email = _target("Email attachments", "Email attachments")
    t_docs = _target("Docs vault", "Docs vault")
    dm.append_child_payloads(r_ix, [t_email, t_docs])
    tgt_email_ix = dm.index(0, 0, r_ix)
    tgt_docs_ix = dm.index(1, 0, r_ix)

    move_email = {
        "source_path": r"Mgmt\Email",
        "destination_path": r"Root3\Management\Email attachments",
        "target_name": "Email attachments",
        "source": {"is_folder": True, "name": "Email", "drive_id": "sd1", "id": "e1"},
        "request_id": "m-email",
    }
    move_docs = {
        "source_path": r"Mgmt\Docs",
        "destination_path": r"Root3\Management\Docs vault",
        "target_name": "Docs vault",
        "source": {"is_folder": True, "name": "Docs", "drive_id": "sd1", "id": "d1"},
        "request_id": "m-docs",
    }
    return mw, sm, dm, tgt_email_ix, tgt_docs_ix, move_email, move_docs


def test_reactive_teardown_planned_moves_clear_no_timer(monkeypatch):
    mw, dm, tgt_ix, move = _email_projection_fixture(monkeypatch)
    mw.planned_moves = [move]
    assert "Sub" in _dest_child_names(dm, tgt_ix)
    mw.planned_moves.clear()
    assert _dest_child_names(dm, tgt_ix) == []


def test_reactive_teardown_remove_single_move_preserves_sibling_branch(monkeypatch):
    mw, sm, dm, ix_e, ix_d, m_e, m_d = _two_branch_projection_fixture(monkeypatch)
    mw.planned_moves = [m_e, m_d]
    assert "SubE" in _dest_child_names(dm, ix_e)
    assert "SubD" in _dest_child_names(dm, ix_d)

    mw.planned_moves = [m_d]
    assert _dest_child_names(dm, ix_e) == []
    assert "SubD" in _dest_child_names(dm, ix_d)


def test_reactive_teardown_file_move_source_not_folder_backed(monkeypatch):
    mw, dm, tgt_ix, move = _email_projection_fixture(monkeypatch)
    mw.planned_moves = [move]
    assert "Sub" in _dest_child_names(dm, tgt_ix)

    file_move = dict(move)
    file_move["source"] = {"is_folder": False, "name": "Email", "drive_id": "sd1", "id": "e1"}
    mw.planned_moves = [file_move]
    assert _dest_child_names(dm, tgt_ix) == []


def test_reactive_teardown_source_unavailable_invokes_dispatcher(monkeypatch):
    """When source rows disappear, the reactive dispatcher must drop unjustified projection (same entry as other mutations)."""
    mw, dm, tgt_ix, move = _email_projection_fixture(monkeypatch)
    sm = mw.source_sharepoint_model
    mw.planned_moves = [move]
    assert "Sub" in _dest_child_names(dm, tgt_ix)
    sm.reset_root_payloads([])
    mw._on_destination_state_mutation("test_source_tree_cleared", None)
    assert _dest_child_names(dm, tgt_ix) == []
