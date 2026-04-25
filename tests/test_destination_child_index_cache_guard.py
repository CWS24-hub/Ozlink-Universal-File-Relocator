"""Guards for destination child path cache: generation invalidation and persistent indices."""

from __future__ import annotations

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication

from ozlink_console.main_window import MainWindow
from ozlink_console.sharepoint_destination_overlay_attach import WORKSPACE_ROW_STATE_LIVE_CONFIRMED
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _qapp():
    return QApplication.instance() or QApplication([])


def test_child_map_cache_clears_when_model_structure_generation_changes():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._destination_tree_model_view = True
    model = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
    mw.destination_planning_model = model

    model.reset_root_payloads(
        [
            {
                "base_display_label": "Folder: Root",
                "name": "Root",
                "is_folder": True,
                "item_path": "Root",
                "tree_role": "destination",
            }
        ]
    )
    root_ix = model.index(0, 0, QModelIndex())
    child_pl = {
        "base_display_label": "Folder: Alpha",
        "name": "Alpha",
        "is_folder": True,
        "item_path": r"Root\Alpha",
        "destination_path": r"Root\Alpha",
        "tree_role": "destination",
    }
    model.append_child_payloads(root_ix, [child_pl])
    sem = mw._destination_semantic_path(child_pl)
    assert sem

    cache = {}
    first = mw._find_destination_child_by_path_index_cached(root_ix, r"Root\Alpha", cache)
    assert first is not None
    assert mw._destination_semantic_path(first.data(Qt.UserRole) or {}) == sem
    assert "__dm_struct_gen__" in cache
    gen_after_first = model.structure_generation()

    model.append_child_payloads(
        root_ix,
        [
            {
                "base_display_label": "Folder: Beta",
                "name": "Beta",
                "is_folder": True,
                "item_path": r"Root\Beta",
                "destination_path": r"Root\Beta",
                "tree_role": "destination",
            }
        ],
    )
    assert model.structure_generation() > gen_after_first

    second = mw._find_destination_child_by_path_index_cached(root_ix, r"Root\Alpha", cache)
    assert second is not None
    assert mw._destination_semantic_path(second.data(Qt.UserRole) or {}) == sem


def test_child_map_cache_rejects_non_index_cached_payload():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    model = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
    mw.destination_planning_model = model
    model.reset_root_payloads(
        [
            {
                "base_display_label": "Folder: Root",
                "name": "Root",
                "is_folder": True,
                "item_path": "Root",
                "tree_role": "destination",
            }
        ]
    )
    root_ix = model.index(0, 0, QModelIndex())
    model.append_child_payloads(
        root_ix,
        [
            {
                "base_display_label": "Folder: Gamma",
                "name": "Gamma",
                "is_folder": True,
                "item_path": r"Root\Gamma",
                "destination_path": r"Root\Gamma",
                "tree_role": "destination",
            }
        ],
    )
    gamma_ix = model.index(0, 0, root_ix)
    sem = mw._destination_semantic_path(gamma_ix.data(Qt.UserRole) or {})
    cache = {}
    mw._destination_child_map_cache_sync_generation(cache, model)
    ck = mw._destination_child_map_cache_key_index(root_ix)
    cache[ck] = {sem: "not_an_index"}
    resolved = mw._find_destination_child_by_path_index_cached(root_ix, r"Root\Gamma", cache)
    assert resolved is not None
    assert resolved == gamma_ix


def test_child_map_cache_hits_live_file_using_row_semantic_when_display_path_skews(monkeypatch):
    """Regression: child map used display-first semantic keys; live rows match allocation paths on item_path."""
    monkeypatch.setattr(
        "ozlink_console.destination_authority_contract.graph_owns_visible_real_destination_structure",
        lambda _h: True,
    )
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._destination_tree_model_view = True
    model = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
    mw.destination_planning_model = model
    model.reset_root_payloads(
        [
            {
                "base_display_label": "Folder: Root",
                "name": "Root",
                "is_folder": True,
                "item_path": "Root",
                "tree_role": "destination",
            }
        ]
    )
    root_ix = model.index(0, 0, QModelIndex())
    folder_pl = {
        "base_display_label": "Folder: Hub",
        "name": "Hub",
        "is_folder": True,
        "item_path": r"Root\Hub",
        "destination_path": r"Root\Hub",
        "tree_role": "destination",
    }
    model.append_child_payloads(root_ix, [folder_pl])
    hub_ix = model.index(0, 0, root_ix)
    file_pl = {
        "base_display_label": "photo.jpg",
        "name": "photo.jpg",
        "is_folder": False,
        "item_path": r"Root\Hub\photo.jpg",
        "display_path": r"Contoso\Sales Library\Root\Hub\photo.jpg",
        "destination_path": r"Root\Hub\photo.jpg",
        "workspace_row_state": WORKSPACE_ROW_STATE_LIVE_CONFIRMED,
        "tree_role": "destination",
        "id": "file-id-1",
        "drive_id": "d1",
    }
    model.append_child_payloads(hub_ix, [file_pl])
    row_key = mw._destination_row_semantic_path(file_pl)
    leg_key = mw._destination_semantic_path(file_pl)
    m = mw._destination_children_semantic_map_index(hub_ix)
    assert row_key
    assert m.get(row_key) is not None
    if leg_key and leg_key.casefold() != row_key.casefold():
        assert m.get(leg_key) is not None
    cache = {}
    hit = mw._find_destination_child_by_path_index_cached(hub_ix, row_key, cache)
    assert hit is not None
    assert (hit.data(Qt.UserRole) or {}).get("name") == "photo.jpg"
