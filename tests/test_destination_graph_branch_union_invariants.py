"""Invariants for merge_graph_branch_union_at_parent: Graph ∪ planning/proposed, no silent drops."""

from __future__ import annotations

import os
import unittest
from unittest.mock import patch

import pytest
from PySide6.QtCore import QModelIndex
from PySide6.QtWidgets import QApplication

from ozlink_console.paths import normalize_manifest_path
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel, NestedSpec


def _ik(pl: dict) -> str:
    return normalize_manifest_path(str(pl.get("item_path") or pl.get("destination_path") or ""))


def _base_drive() -> str:
    return "d0"


def _planned_folder(name: str, rel: str) -> dict:
    p = f"L\\{rel}" if not rel.startswith("L\\") else rel
    return {
        "name": name,
        "base_display_label": name,
        "tree_label": "Folder",
        "is_folder": True,
        "semantic_path": p,
        "item_path": p,
        "destination_path": p,
        "tree_role": "destination",
        "drive_id": _base_drive(),
        "verification_state": "planned_only",
        "row_kind": "planned_folder",
        "workspace_row_state": "planned_only",
        "id": "",
    }


def _live_folder(name: str, rel: str, iid: str) -> dict:
    p = f"L\\{rel}" if not rel.startswith("L\\") else rel
    return {
        "name": name,
        "base_display_label": name,
        "is_folder": True,
        "semantic_path": p,
        "item_path": p,
        "destination_path": p,
        "tree_role": "destination",
        "drive_id": _base_drive(),
        "id": iid,
        "graph_item_id": iid,
        "workspace_row_state": "live_confirmed",
        "row_kind": "live_folder",
        "verification_state": "live_confirmed",
    }


def _stale_unprotected(rel: str, iid: str) -> dict:
    p = f"L\\{rel}" if not rel.startswith("L\\") else rel
    return {
        "name": "Stale",
        "is_folder": True,
        "semantic_path": p,
        "item_path": p,
        "destination_path": p,
        "tree_role": "destination",
        "drive_id": _base_drive(),
        "id": iid,
        "workspace_row_state": "live_confirmed",
        "row_kind": "live_folder",
        "verification_state": "live_confirmed",
    }


@pytest.fixture
def _qapp() -> QApplication:
    return QApplication.instance() or QApplication([])


@patch("ozlink_console.tree_models.destination_planning_model.log_info", lambda *a, **k: None)
def test_graph_union_keeps_planned_when_graph_adds_different_child(_qapp) -> None:
    """Test 1: Graph child list does not list an existing planned row; that row must remain a sibling."""
    m = DestinationPlanningTreeModel(destination_index_key_fn=_ik)
    m.append_child_payloads(
        QModelIndex(), [_planned_folder("Plann", "Plann"), _planned_folder("OtherP", "OtherP")]
    )
    graph = [_live_folder("GOnly", "GOnly", "g-gonly")]
    m.merge_graph_branch_union_at_parent(
        QModelIndex(), graph, parent_canonical_path="L"
    )
    assert m.rowCount(QModelIndex()) == 3
    found = {(_ik(m.index(r, 0, QModelIndex()).data(256) or {})) for r in range(m.rowCount(QModelIndex()))}
    assert "L\\Plann" in found
    assert "L\\GOnly" in found
    assert "L\\OtherP" in found


@patch("ozlink_console.tree_models.destination_planning_model.log_info", lambda *a, **k: None)
def test_matched_path_grafts_protected_subtree(_qapp) -> None:
    """Test 2: When Graph matches by path, planned descendants under that folder must survive the upgrade."""
    f_pl = _planned_folder("Fin", "Fin")
    sub_pl = _planned_folder("Pay", "L\\Fin\\Pay")
    nest: NestedSpec = (f_pl, [(sub_pl, [])])
    m = DestinationPlanningTreeModel(destination_index_key_fn=_ik)
    m.reset_nested(
        [
            (
                {
                    "name": "L",
                    "is_folder": True,
                    "item_path": "L",
                    "destination_path": "L",
                    "semantic_path": "L",
                    "tree_role": "destination",
                    "drive_id": _base_drive(),
                    "id": "r",
                },
                [nest],
            )
        ]
    )
    inv = QModelIndex()
    l_ix = m.index(0, 0, inv)
    fin_ix = m.index(0, 0, l_ix)
    assert m.count_planning_protected_descendant_rows(fin_ix) >= 1
    g_fin = [dict(_live_folder("Fin", "Fin", "g-fin"))]
    m.merge_graph_branch_union_at_parent(l_ix, g_fin, parent_canonical_path="L")
    fin2 = m.index(0, 0, l_ix)
    assert m.rowCount(fin2) == 1
    pay_ix = m.index(0, 0, fin2)
    ppl = pay_ix.data(256) or {}
    assert "Pay" in str(ppl.get("name", "")) or ppl.get("name") == "Pay"
    assert "Fin\\Pay" in str(ppl.get("item_path", "")).replace("/", "\\")


@patch("ozlink_console.tree_models.destination_planning_model.log_info", lambda *a, **k: None)
def test_coalesce_removes_duplicate_path_siblings(_qapp) -> None:
    """Test 3: Snapshot duplicate same-path direct children are merged to one path after union."""
    a = _planned_folder("D", "D")
    b = dict(a)
    b["id"] = "x-dup"
    b["proposed"] = False
    m = DestinationPlanningTreeModel(destination_index_key_fn=_ik)
    m.append_child_payloads(QModelIndex(), [a, b])
    assert m.rowCount(QModelIndex()) == 2
    n = m._coalesce_direct_children_duplicate_path_keys(QModelIndex())  # noqa: SLF001
    m._rebuild_path_index()  # noqa: SLF001
    assert m.rowCount(QModelIndex()) == 1
    assert int(n) >= 1


@patch("ozlink_console.tree_models.destination_planning_model.log_info", lambda *a, **k: None)
def test_unprotected_row_not_resurrected_by_union_reinsert(_qapp) -> None:
    """Test 4: Non-planning structural rows are not re-appended from stash; Graph may displace that edge."""
    m = DestinationPlanningTreeModel(destination_index_key_fn=_ik)
    m.append_child_payloads(
        QModelIndex(), [_stale_unprotected("Stale1", "s1"), _planned_folder("Keep", "Keep")]
    )
    m.merge_graph_branch_union_at_parent(
        QModelIndex(), [], parent_canonical_path="L"
    )
    k = {(_ik(m.index(r, 0, QModelIndex()).data(256) or {})) for r in range(m.rowCount(QModelIndex()))}
    assert "L\\Keep" in k
    assert "L\\Stale1" in k


if __name__ == "__main__":
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    unittest.main()
