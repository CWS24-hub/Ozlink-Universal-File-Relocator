"""Pilot subtree injection must not treat parent Personal as matching child Personal\\100GOPRO."""

from __future__ import annotations

from ozlink_console.main_window import MainWindow


def test_pilot_allocation_destinations_exact_match_requires_full_path():
    mw = MainWindow.__new__(MainWindow)
    mw._pilot_norm_destination_identity = lambda p: str(p or "").strip().replace("/", "\\")

    assert MainWindow._pilot_allocation_destinations_exact_match(mw, r"Root\Personal", r"Root\Personal\100GOPRO") is False
    assert MainWindow._pilot_allocation_destinations_exact_match(mw, r"Root\Personal\100GOPRO", r"Root\Personal\100GOPRO") is True
    assert MainWindow._pilot_allocation_destinations_exact_match(mw, r"Root\Personal", r"Root\Personal") is True


def test_inject_skips_parent_folder_move_when_allocated_is_deeper_child():
    """Loose path equivalence would pick the Personal move; strict match does not."""
    mw = MainWindow.__new__(MainWindow)
    mw.planned_moves = [
        {
            "source": {"is_folder": True, "id": "src-personal", "drive_id": "sd", "name": "Personal"},
            "destination_path": r"Z:\lib\Root",
            "destination_name": "Personal",
            "destination": {"drive_id": "dd", "id": "parent"},
            "source_path": r"Z:\src\Personal",
            "source_name": "Personal",
            "request_id": "r1",
            "status": "Draft",
        }
    ]

    def _folder_identity(move):
        if move is mw.planned_moves[0]:
            return r"Root\Personal"
        return ""

    mw._pilot_planned_move_folder_identity = _folder_identity
    mw._pilot_norm_destination_identity = lambda p: str(p or "").strip().replace("/", "\\")
    mw._source_parent_path = lambda p: p  # unused when no file move

    run_manifest = {"transfer_steps": []}
    indices, uids, injected, exact = MainWindow._inject_pilot_allocated_subtree_step(
        mw,
        run_manifest,
        allocated_folder_identity=r"Root\Personal\100GOPRO",
        selected_step_indices=[],
    )
    assert injected is False
    assert indices == []
    assert uids == []
    assert exact is False


def test_inject_succeeds_when_planned_folder_move_matches_allocated_exactly():
    mw = MainWindow.__new__(MainWindow)
    mw.planned_moves = [
        {
            "source": {"is_folder": True, "id": "g", "drive_id": "sd", "name": "100GOPRO"},
            "destination_path": r"Root\Personal",
            "destination_name": "100GOPRO",
            "destination": {"drive_id": "dd", "id": "dp"},
            "source_path": r"S:\100GOPRO",
            "source_name": "100GOPRO",
            "request_id": "r1",
            "status": "Draft",
        }
    ]

    def _fid(m):
        return r"Root\Personal\100GOPRO"

    mw._pilot_planned_move_folder_identity = _fid
    mw._pilot_norm_destination_identity = lambda p: str(p or "").strip().replace("/", "\\")
    mw._pilot_allocation_destinations_exact_match = lambda a, b: MainWindow._pilot_allocation_destinations_exact_match(mw, a, b)

    run_manifest = {"transfer_steps": [{"index": 0, "is_source_folder": True}]}
    indices, uids, injected, exact = MainWindow._inject_pilot_allocated_subtree_step(
        mw,
        run_manifest,
        allocated_folder_identity=r"Root\Personal\100GOPRO",
        selected_step_indices=[],
    )
    assert injected is True
    assert indices == [1]
    assert uids and uids[0].startswith("SCOPE-SYN::")


def test_inject_does_not_fallback_to_manifest_folder_template():
    """Exact manifest folder-copy row must not seed SCOPE-SYN without a matching planned move."""
    mw = MainWindow.__new__(MainWindow)
    mw.planned_moves = []
    run_manifest = {
        "transfer_steps": [
            {
                "index": 2,
                "operation": "copy",
                "is_source_folder": True,
                "destination_path": r"Root\Personal",
                "destination_name": "100GOPRO",
                "source_path": r"S:\src\100GOPRO",
                "source_name": "100GOPRO",
                "source_drive_id": "sd",
                "source_item_id": "si",
                "destination_drive_id": "dd",
                "destination_item_id": "dp",
                "request_id": "r9",
                "status": "Draft",
            }
        ]
    }
    indices, uids, injected, exact = MainWindow._inject_pilot_allocated_subtree_step(
        mw,
        run_manifest,
        allocated_folder_identity=r"Root\Personal\100GOPRO",
        selected_step_indices=[],
    )
    assert injected is False
    assert indices == []
    assert len(run_manifest["transfer_steps"]) == 1


