"""Draft-first scoped snapshot: expand folder selections to file mapping ids via planned_moves."""

from __future__ import annotations

from types import SimpleNamespace

from ozlink_console.main_window import MainWindow


def _mw():
    mw = MainWindow.__new__(MainWindow)
    mw._canonical_source_projection_path = lambda p: str(p or "").strip().replace("/", "\\")
    return mw


def _move_file(idx: int, source_path: str, *, request_id: str = "") -> dict:
    return {
        "request_id": request_id,
        "source_path": source_path,
        "source": {"is_folder": False, "name": source_path.rsplit("\\", 1)[-1]},
    }


def _move_folder(idx: int, source_path: str, *, request_id: str = "") -> dict:
    return {
        "request_id": request_id,
        "source_path": source_path,
        "source": {"is_folder": True, "name": source_path.rsplit("\\", 1)[-1]},
    }


def test_planned_move_is_file_allocation():
    assert MainWindow._planned_move_is_file_allocation(_move_file(0, "a"))
    assert not MainWindow._planned_move_is_file_allocation(_move_folder(0, "a"))


def test_file_row_selected_yields_single_seed():
    mw = _mw()
    mw.planned_moves = [_move_file(0, r"Root\Lib\a.txt", request_id="r1")]
    out = mw._derive_snapshot_scoped_file_seed_mapping_ids([0])
    assert out == ["r1"]


def test_folder_row_collects_descendant_file_seeds():
    mw = _mw()
    mw.planned_moves = [
        _move_folder(0, r"Root\Lib\100GOPRO", request_id="folder"),
        _move_file(1, r"Root\Lib\100GOPRO\clip.mp4", request_id=""),
        _move_file(2, r"Root\Lib\100GOPRO\sub\other.mp4", request_id="deep"),
    ]
    out = mw._derive_snapshot_scoped_file_seed_mapping_ids([0])
    assert set(out) == {"alloc-1", "deep"}


def test_folder_selection_excludes_sibling_branch_files():
    mw = _mw()
    mw.planned_moves = [
        _move_folder(0, r"Root\Lib\100GOPRO"),
        _move_file(1, r"Root\Lib\100GOPRO\a.txt"),
        _move_file(2, r"Root\Lib\OtherFolder\b.txt"),
    ]
    out = mw._derive_snapshot_scoped_file_seed_mapping_ids([0])
    assert out == ["alloc-1"]


def test_union_multiple_selections():
    mw = _mw()
    mw.planned_moves = [
        _move_folder(0, r"Root\Lib\A"),
        _move_file(1, r"Root\Lib\A\x.txt"),
        _move_file(2, r"Root\Lib\B\y.txt"),
    ]
    out = mw._derive_snapshot_scoped_file_seed_mapping_ids([0, 2])
    assert set(out) == {"alloc-1", "alloc-2"}


def test_folder_only_no_descendants_returns_empty():
    mw = _mw()
    mw.planned_moves = [
        _move_folder(0, r"Root\Lib\EmptyFolder"),
        _move_file(1, r"Root\Lib\Elsewhere\z.txt"),
    ]
    assert mw._derive_snapshot_scoped_file_seed_mapping_ids([0]) == []


def test_collect_snapshot_scoped_matches_derive():
    mw = _mw()
    mw.planned_moves = [
        _move_folder(0, r"Root\Lib\Box"),
        _move_file(1, r"Root\Lib\Box\f.txt"),
    ]

    class _Rg:
        def topRow(self):
            return 0

        def bottomRow(self):
            return 0

    mw.planned_moves_table = SimpleNamespace(selectedRanges=lambda: [_Rg()])
    assert mw._collect_snapshot_scoped_request_ids_from_planned_moves_selection() == ["alloc-1"]
