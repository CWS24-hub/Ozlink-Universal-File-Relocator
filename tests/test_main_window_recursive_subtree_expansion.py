"""MainWindow recursive subtree seed expansion (Graph stub)."""

from __future__ import annotations

from ozlink_console.main_window import MainWindow


def _folder_move(row: int, *, source_path: str, dest_path: str, drive: str = "drv", fid: str = "fold1") -> dict:
    return {
        "request_id": f"f{row}",
        "source_path": source_path,
        "destination_path": dest_path,
        "allocation_method": "Move Recursive",
        "status": "Draft",
        "requested_by": "t",
        "requested_date": "d",
        "source": {
            "id": fid,
            "drive_id": drive,
            "is_folder": True,
            "name": "F",
            "item_path": "/Box",
        },
        "destination": {"drive_id": "dd", "name": "D"},
        "destination_id": "dp1",
    }


class _Graph:
    def __init__(self, files: list[dict]) -> None:
        self._files = files
        self.token = "x"

    def list_drive_folder_descendant_files_normalized(self, drive_id, item_id, **kwargs):
        return list(self._files)


def test_expand_recursive_union_and_deterministic_order():
    mw = MainWindow.__new__(MainWindow)
    # Align with library-relative anchor ``/Box`` used in Graph ``item_path`` values below.
    mw._canonical_source_projection_path = lambda p: "Box" if "Box" in str(p) else str(p or "").strip()
    mw._canonical_destination_projection_path = lambda p: str(p or "").strip()
    files = [
        {"id": "fa", "name": "a.txt", "item_path": "/Box/a.txt", "drive_id": "drv"},
        {"id": "fb", "name": "b.txt", "item_path": "/Box/sub/b.txt", "drive_id": "drv"},
    ]
    mw.graph = _Graph(files)
    moves = [
        _folder_move(0, source_path=r"Root\Lib\Box", dest_path=r"D\Box", fid="fold1"),
    ]
    mw.planned_moves = moves
    seeds, allocs, pms, n, err = mw._expand_snapshot_scoped_recursive_subtree([0])
    assert err == ""
    assert n == 2
    assert len(seeds) == 2
    assert seeds == sorted(seeds)
    assert len(allocs) == len(pms) == 2
    assert all(str(x["RequestId"]).startswith("recsub-") for x in allocs)
    # Overlap: same Graph file id should not duplicate
    seeds2, _, _, n2, err2 = mw._expand_snapshot_scoped_recursive_subtree([0, 0])
    assert err2 == ""
    assert n2 == 2
    assert len(seeds2) == 2


def test_expand_browsed_recursive_subtree():
    mw = MainWindow.__new__(MainWindow)
    mw._canonical_source_projection_path = lambda p: "Box" if "Box" in str(p) else str(p or "").strip()
    mw._canonical_destination_projection_path = lambda p: "DBox" if "DBox" in str(p) else str(p or "").strip()
    mw._tree_item_path = lambda n: n.get("_path", "")
    files = [
        {"id": "fa", "name": "a.txt", "item_path": "/Box/a.txt", "drive_id": "drv"},
    ]

    class _Graph:
        token = "t"

        def list_drive_folder_descendant_files_normalized(self, drive_id, item_id, **kwargs):
            return list(files)

    mw.graph = _Graph()
    src = {
        "is_folder": True,
        "id": "fold1",
        "drive_id": "drv",
        "library_id": "drv",
        "_path": r"Root\Lib\Box",
        "item_path": "/Box",
    }
    dst = {
        "is_folder": True,
        "id": "droot",
        "drive_id": "dd",
        "library_id": "dd",
        "name": "DBox",
        "_path": r"Root\Dest\DBox",
    }
    mw._resolve_tree_item_drive_id = lambda panel, n: str(n.get("drive_id") or "")
    seeds, allocs, moves, n, err = mw._expand_browsed_recursive_subtree(src, dst, allocation_method="copy")
    assert err == ""
    assert n == 1
    assert len(seeds) == 1
    assert allocs[0]["AllocationMethod"] == "copy"
    assert "recsub-" in allocs[0]["RequestId"]
    assert allocs[0]["DestinationParentItemId"] == "droot"
    assert moves[0]["destination_id"] == "droot"
    # Source folder leaf "Box" is mirrored under destination root "DBox" (not flattened).
    assert str(allocs[0].get("RequestedDestinationPath", "")).replace("/", "\\") == r"DBox\Box\a.txt"
    assert str(moves[0].get("destination_path", "")).replace("/", "\\") == r"DBox\Box\a.txt"


def test_expand_browsed_recursive_mirrors_source_folder_name_under_parent_destination():
    """
    REGRESSION LOCK (browsed recursive): source folder leaf must mirror under parent destination — no flattening.

    Covers: recursive subtree destination leaf preservation; allocated child segment in synthetic rows.
    """
    mw = MainWindow.__new__(MainWindow)
    mw._canonical_source_projection_path = lambda p: str(p or "").replace("/", "\\").strip()
    mw._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\").strip()
    mw._tree_item_path = lambda n: n.get("_path", "")

    class _Graph:
        token = "t"

        def list_drive_folder_descendant_files_normalized(self, drive_id, item_id, **kwargs):
            return [
                {
                    "id": "gopr",
                    "name": "GOPR0307.JPG",
                    "item_path": "/Root/Library/100GOPRO/GOPR0307.JPG",
                    "drive_id": "drv",
                },
                {
                    "id": "deep",
                    "name": "x.txt",
                    "item_path": "/Root/Library/100GOPRO/sub/x.txt",
                    "drive_id": "drv",
                },
            ]

    mw.graph = _Graph()
    mw._resolve_tree_item_drive_id = lambda panel, n: str(n.get("drive_id") or "")
    src = {
        "is_folder": True,
        "id": "fold100",
        "drive_id": "drv",
        "library_id": "drv",
        "name": "100GOPRO",
        "_path": r"Root\Library\100GOPRO",
        "item_path": "/Root/Library/100GOPRO",
    }
    dst = {
        "is_folder": True,
        "id": "personal",
        "drive_id": "dd",
        "library_id": "dd",
        "name": "Personal",
        "_path": r"Root\Personal",
    }
    seeds, allocs, _, n, err = mw._expand_browsed_recursive_subtree(src, dst, allocation_method="copy")
    assert err == ""
    assert n == 2
    paths = {str(a.get("RequestedDestinationPath", "")).replace("/", "\\") for a in allocs}
    assert r"Root\Personal\100GOPRO\GOPR0307.JPG" in paths
    assert r"Root\Personal\100GOPRO\sub\x.txt" in paths


def test_expand_browsed_recursive_no_duplicate_when_destination_is_source_leaf_folder():
    """Selecting the destination folder that already matches the source leaf avoids double segment."""
    mw = MainWindow.__new__(MainWindow)
    mw._canonical_source_projection_path = lambda p: str(p or "").replace("/", "\\").strip()
    mw._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\").strip()
    mw._tree_item_path = lambda n: n.get("_path", "")

    class _Graph:
        token = "t"

        def list_drive_folder_descendant_files_normalized(self, drive_id, item_id, **kwargs):
            return [{"id": "g", "name": "a.txt", "item_path": "/Root/Source/100GOPRO/a.txt", "drive_id": "drv"}]

    mw.graph = _Graph()
    mw._resolve_tree_item_drive_id = lambda panel, n: str(n.get("drive_id") or "")
    src = {
        "is_folder": True,
        "id": "s1",
        "drive_id": "drv",
        "library_id": "drv",
        "name": "100GOPRO",
        "_path": r"Root\Source\100GOPRO",
        "item_path": "/Root/Source/100GOPRO",
    }
    dst = {
        "is_folder": True,
        "id": "d100",
        "drive_id": "dd",
        "library_id": "dd",
        "name": "100GOPRO",
        "_path": r"Root\Personal\100GOPRO",
    }
    _, allocs, _, n, err = mw._expand_browsed_recursive_subtree(src, dst, allocation_method="copy")
    assert err == "" and n == 1
    assert str(allocs[0].get("RequestedDestinationPath", "")).replace("/", "\\") == r"Root\Personal\100GOPRO\a.txt"


def test_browsed_recursive_preflight_dependency_closure_defers_missing_dest_folder_id():
    mw = MainWindow.__new__(MainWindow)
    step = {
        "source_path": r"Box\a.txt",
        "destination_path": r"DBox\a.txt",
        "source_drive_id": "s",
        "source_item_id": "file1",
        "destination_drive_id": "d",
        "destination_item_id": "",
    }
    opts_dep = {"snapshot_browsed_recursive": True, "snapshot_scoped_mode": "dependency_closure"}
    opts_strict = {"snapshot_browsed_recursive": True, "snapshot_scoped_mode": "strict"}
    assert mw._browsed_recursive_transfer_preflight_deferred_ready(step, opts_dep)
    assert not mw._browsed_recursive_transfer_preflight_deferred_ready(step, opts_strict)
    assert MainWindow._is_transfer_step_graph_ready_for_preflight(mw, step, opts_dep)
    assert not MainWindow._is_transfer_step_graph_ready_for_preflight(mw, step, opts_strict)


def test_prepare_browsed_context_includes_allocation_method():
    mw = MainWindow.__new__(MainWindow)
    mw._canonical_source_projection_path = lambda p: "Box" if "Box" in str(p) else str(p or "").strip()
    mw._canonical_destination_projection_path = lambda p: "DBox" if "DBox" in str(p) else str(p or "").strip()
    mw._tree_item_path = lambda n: n.get("_path", "")
    files = [{"id": "fa", "name": "a.txt", "item_path": "/Box/a.txt", "drive_id": "drv"}]

    class _Graph:
        token = "t"

        def list_drive_folder_descendant_files_normalized(self, drive_id, item_id, **kwargs):
            return list(files)

    mw.graph = _Graph()
    src = {
        "is_folder": True,
        "id": "fold1",
        "drive_id": "drv",
        "library_id": "drv",
        "_path": r"Root\Lib\Box",
        "item_path": "/Box",
    }
    dst = {
        "is_folder": True,
        "id": "droot",
        "drive_id": "dd",
        "library_id": "dd",
        "name": "DBox",
        "_path": r"Root\Dest\DBox",
    }
    mw._resolve_tree_item_drive_id = lambda panel, n: str(n.get("drive_id") or "")
    ctx, err = mw._prepare_browsed_recursive_execution_context(
        source_data=src,
        dest_data=dst,
        scoped_mode="strict",
        allocation_method="Copy",
    )
    assert err == ""
    assert ctx is not None
    assert ctx.get("allocation_method") == "copy"


def test_prepare_browsed_context_requires_destination():
    mw = MainWindow.__new__(MainWindow)
    mw._tree_item_path = lambda n: n.get("p", "")
    mw._canonical_source_projection_path = lambda p: str(p or "")
    mw._canonical_destination_projection_path = lambda p: str(p or "")
    src = {"is_folder": True, "id": "1", "drive_id": "d", "library_id": "d", "p": "/src"}
    ctx, err = mw._prepare_browsed_recursive_execution_context(
        source_data=src,
        dest_data={},
        scoped_mode="strict",
        allocation_method="move",
    )
    assert ctx is None
    assert "destination" in err.lower()


def test_expand_recursive_requires_graph_ids():
    mw = MainWindow.__new__(MainWindow)
    mw._canonical_source_projection_path = lambda p: str(p or "").strip()
    mw.graph = _Graph([])
    mw.planned_moves = [
        {
            "request_id": "x",
            "source_path": "p",
            "destination_path": "d",
            "source": {"id": "", "drive_id": "", "is_folder": True},
            "destination": {},
        }
    ]
    _, _, _, _, err = mw._expand_snapshot_scoped_recursive_subtree([0])
    assert "Graph" in err
