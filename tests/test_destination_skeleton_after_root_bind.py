from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ozlink_console.main_window import MainWindow


@pytest.fixture
def mw() -> MainWindow:
    m = MainWindow.__new__(MainWindow)
    m._planning_browse_mode = lambda side: "sharepoint"
    m._destination_library_context_unresolved_for_graph_display = lambda: False
    m.pending_root_drive_ids = {"destination": "drive-BBB"}
    m.root_load_workers = {"destination": {"id": 42}}
    m.destination_planning_model = MagicMock()

    def _row_payload(r: int) -> dict:
        if r == 0:
            return {
                "is_folder": True,
                "children_loaded": False,
                "load_failed": False,
                "placeholder": False,
            }
        return {}

    def _uc(idx):
        return 1 if not idx.isValid() else 0

    def _index(r, c, parent):
        im = MagicMock()
        im.isValid.return_value = r >= 0 and c >= 0 and not parent.isValid()
        return im

    m.destination_planning_model.rowCount = _uc
    m.destination_planning_model.index = _index

    def _user_role(ix):
        return _row_payload(0)

    m._destination_model_index_user_role_dict = lambda ix: _user_role(ix)
    m._destination_row_is_live_graph_structure = lambda pl: True
    m._request_graph_destination_children_load = MagicMock(return_value=True)
    return m


def test_skeleton_schedules_when_root_worker_registry_cleared_but_pending_matches(mw: MainWindow):
    """Simulates finished hook clearing root_load_workers before a deferred timer; pending drive still authorizes."""
    mw.root_load_workers = {}
    mw._destination_schedule_skeleton_first_level_graph_child_loads(
        "drive-BBB",
        99,
        worker_tag="test_post_cleanup",
    )
    assert mw._request_graph_destination_children_load.called


def test_skeleton_skips_when_registry_cleared_and_pending_mismatches(mw: MainWindow):
    mw.root_load_workers = {}
    mw.pending_root_drive_ids = {"destination": "drive-OTHER"}
    mw._destination_schedule_skeleton_first_level_graph_child_loads(
        "drive-BBB",
        99,
        worker_tag="test_mismatch",
    )
    mw._request_graph_destination_children_load.assert_not_called()


def test_skeleton_wrap_logs_entry_and_exit(mw: MainWindow):
    mw._destination_graph_skeleton_schedule_after_live_root_bind(
        "drive-BBB",
        42,
        worker_tag="post_root_sync",
        schedule_phase="unit_test",
    )
    assert mw._request_graph_destination_children_load.called
