from __future__ import annotations

from PySide6.QtCore import QModelIndex, Qt

from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


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
