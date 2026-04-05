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
