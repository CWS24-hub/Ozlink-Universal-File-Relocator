from __future__ import annotations

from PySide6.QtCore import QModelIndex, Qt

from ozlink_console.sharepoint_destination_overlay_attach import WORKSPACE_ROW_STATE_LIVE_CONFIRMED
from ozlink_console.tree_models.destination_planning_model import (
    DESTINATION_PLAN_LEAF_EXCLUSION_PAINT_ROLE,
    DestinationPlanningTreeModel,
)


def test_destination_model_reset_and_payload_access():
    model = DestinationPlanningTreeModel()
    model.reset_root_payloads(
        [
            {
                "base_display_label": "Folder: Personal",
                "name": "Personal",
                "is_folder": True,
                "item_path": r"Root\Personal",
            }
        ]
    )
    assert model.rowCount(QModelIndex()) == 1
    ix = model.index(0, 0, QModelIndex())
    assert ix.isValid()
    payload = ix.data(Qt.UserRole) or {}
    assert payload.get("name") == "Personal"
    assert model.hasChildren(ix) is True


def test_destination_model_replace_children_and_clear():
    model = DestinationPlanningTreeModel()
    model.reset_root_payloads(
        [
            {
                "base_display_label": "Folder: Root",
                "name": "Root",
                "is_folder": True,
                "item_path": "Root",
            }
        ]
    )
    root_ix = model.index(0, 0, QModelIndex())
    model.replace_all_children(
        root_ix,
        [
            {
                "base_display_label": "File: x.txt",
                "name": "x.txt",
                "is_folder": False,
                "item_path": r"Root\x.txt",
            }
        ],
    )
    assert model.rowCount(root_ix) == 1
    child_ix = model.index(0, 0, root_ix)
    assert (child_ix.data(Qt.UserRole) or {}).get("name") == "x.txt"
    model.clear()
    assert model.rowCount(QModelIndex()) == 0


def test_rowcount_safe_when_internal_pointer_not_node():
    """Regression: stale/corrupt indexes must not assume internalPointer is _Node (avoids list._children)."""
    model = DestinationPlanningTreeModel()
    model.reset_root_payloads(
        [
            {
                "base_display_label": "Folder: R",
                "name": "R",
                "is_folder": True,
                "item_path": "R",
            }
        ]
    )
    root_ix = model.index(0, 0, QModelIndex())
    folder_node = root_ix.internalPointer()
    folder_node._children = [[]]
    assert model.rowCount(root_ix) == 1
    assert model.index(0, 0, root_ix).isValid() is False


def test_append_child_payloads_invalid_parent_is_top_level():
    model = DestinationPlanningTreeModel()
    model.append_child_payloads(
        QModelIndex(),
        [
            {
                "base_display_label": "A",
                "name": "A",
                "is_folder": True,
                "item_path": "A",
            }
        ],
    )
    assert model.rowCount(QModelIndex()) == 1


def test_destination_structure_changed_signal_on_mutations():
    model = DestinationPlanningTreeModel()
    hits = []

    model.destination_structure_changed.connect(lambda: hits.append(1))

    model.reset_root_payloads(
        [
            {
                "base_display_label": "Folder: Root",
                "name": "Root",
                "is_folder": True,
                "item_path": "Root",
            }
        ]
    )
    assert sum(hits) >= 1
    before = sum(hits)
    root_ix = model.index(0, 0, QModelIndex())
    model.replace_all_children(
        root_ix,
        [
            {
                "base_display_label": "File: a.txt",
                "name": "a.txt",
                "is_folder": False,
                "item_path": r"Root\a.txt",
            }
        ],
    )
    assert sum(hits) > before


def test_destination_structure_changed_coalesced_emits_once_per_batch():
    model = DestinationPlanningTreeModel()
    hits = []
    model.destination_structure_changed.connect(lambda: hits.append(1))
    root_ix = model.index(0, 0, QModelIndex())
    model.reset_root_payloads(
        [
            {
                "base_display_label": "Folder: Root",
                "name": "Root",
                "is_folder": True,
                "item_path": "Root",
            }
        ]
    )
    hits.clear()
    model.begin_coalesce_destination_structure_signal()
    try:
        model.replace_all_children(
            root_ix,
            [
                {
                    "base_display_label": "File: a.txt",
                    "name": "a.txt",
                    "is_folder": False,
                    "item_path": r"Root\a.txt",
                }
            ],
        )
        model.append_child_payloads(
            root_ix,
            [
                {
                    "base_display_label": "File: b.txt",
                    "name": "b.txt",
                    "is_folder": False,
                    "item_path": r"Root\b.txt",
                }
            ],
        )
    finally:
        model.end_coalesce_destination_structure_signal()
    assert len(hits) == 1


def test_destination_path_index_multi_candidate():
    def key_fn(pl):
        return str(pl.get("item_path", "") or "").strip()

    model = DestinationPlanningTreeModel(destination_index_key_fn=key_fn)
    model.reset_root_payloads(
        [
            {
                "base_display_label": "Folder: Root",
                "name": "Root",
                "is_folder": True,
                "item_path": "Root",
            }
        ]
    )
    ixs = model.find_indices_for_canonical_destination_path("Root")
    assert len(ixs) == 1
    root_ix = model.index(0, 0, QModelIndex())
    model.replace_all_children(
        root_ix,
        [
            {
                "base_display_label": "File: a.txt",
                "name": "a.txt",
                "is_folder": False,
                "item_path": r"Root\a.txt",
            }
        ],
    )
    ixs2 = model.find_indices_for_canonical_destination_path(r"Root\a.txt")
    assert len(ixs2) == 1


def test_find_indices_resolves_casefold_via_side_map_not_full_bucket_scan():
    def key_fn(pl):
        return str(pl.get("item_path", "") or "").strip()

    model = DestinationPlanningTreeModel(destination_index_key_fn=key_fn)
    model.reset_root_payloads(
        [
            {
                "base_display_label": "Folder: Lib",
                "name": "Lib",
                "is_folder": True,
                "item_path": r"Site\Lib",
            }
        ]
    )
    lib_ix = model.index(0, 0, QModelIndex())
    model.append_child_payloads(
        lib_ix,
        [
            {
                "base_display_label": "File: Doc",
                "name": "Contractor bank.docx",
                "is_folder": False,
                "item_path": r"Site\Lib\HR\Contractor bank.docx",
            }
        ],
    )
    mixed = model.find_indices_for_canonical_destination_path(r"site\lib\hr\contractor bank.docx")
    assert len(mixed) == 1
    assert (mixed[0].data(Qt.UserRole) or {}).get("name") == "Contractor bank.docx"


def test_plan_leaf_exclusion_paint_role_matches_contract():
    """Delegate probes DESTINATION_PLAN_LEAF_EXCLUSION_PAINT_ROLE instead of full UserRole for exclusion."""
    model = DestinationPlanningTreeModel()
    model.reset_root_payloads(
        [
            {
                "base_display_label": "File: a.txt",
                "name": "a.txt",
                "is_folder": False,
                "item_path": r"Root\a.txt",
                "source_path": r"Src\a.txt",
            }
        ]
    )
    ix = model.index(0, 0, QModelIndex())
    assert ix.data(DESTINATION_PLAN_LEAF_EXCLUSION_PAINT_ROLE) is False
    model.set_plan_leaf_exclusion_paint_contract(frozenset({"canon_a"}), lambda s: "canon_a" if s else "")
    assert ix.data(DESTINATION_PLAN_LEAF_EXCLUSION_PAINT_ROLE) is True
    model.set_plan_leaf_exclusion_paint_contract(frozenset({"other"}), lambda s: "canon_a" if s else "")
    assert ix.data(DESTINATION_PLAN_LEAF_EXCLUSION_PAINT_ROLE) is False


def test_reconcile_clears_children_loaded_when_live_folder_has_empty_subtree():
    """Stale snapshot may set children_loaded=True with no model rows under the folder."""
    model = DestinationPlanningTreeModel()
    model.reset_root_payloads(
        [
            {
                "base_display_label": "Folder: Root3",
                "name": "Root3",
                "is_folder": True,
                "id": "graph-item-root3",
                "drive_id": "b!drive-test",
                "workspace_row_state": WORKSPACE_ROW_STATE_LIVE_CONFIRMED,
                "children_loaded": True,
                "load_failed": False,
                "item_path": "Root3",
                "tree_role": "destination",
            }
        ]
    )
    n = model.reconcile_top_level_live_graph_children_loaded_when_subtree_empty(reason="unit_test")
    assert n == 1
    pl = model.index(0, 0, QModelIndex()).data(Qt.UserRole) or {}
    assert pl.get("children_loaded") is False


def test_reconcile_preserves_children_loaded_when_subtree_has_rows():
    model = DestinationPlanningTreeModel()
    model.reset_root_payloads(
        [
            {
                "base_display_label": "Folder: Root3",
                "name": "Root3",
                "is_folder": True,
                "id": "graph-item-root3",
                "drive_id": "b!drive-test",
                "workspace_row_state": WORKSPACE_ROW_STATE_LIVE_CONFIRMED,
                "children_loaded": True,
                "load_failed": False,
                "item_path": "Root3",
                "tree_role": "destination",
            }
        ]
    )
    top = model.index(0, 0, QModelIndex())
    model.replace_all_children(
        top,
        [
            {
                "base_display_label": "Folder: Child",
                "name": "Child",
                "is_folder": True,
                "id": "graph-item-child",
                "drive_id": "b!drive-test",
                "workspace_row_state": WORKSPACE_ROW_STATE_LIVE_CONFIRMED,
                "children_loaded": False,
                "item_path": r"Root3\Child",
                "tree_role": "destination",
            }
        ],
    )
    n = model.reconcile_top_level_live_graph_children_loaded_when_subtree_empty(reason="unit_test")
    assert n == 0
    pl = top.data(Qt.UserRole) or {}
    assert pl.get("children_loaded") is True
