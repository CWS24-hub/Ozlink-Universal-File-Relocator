"""Runtime verification (CI) for descendant resume + structure-signal coalescing — no architecture changes."""

from __future__ import annotations

import statistics
from unittest.mock import MagicMock, patch

import pytest
from PySide6.QtCore import QModelIndex

from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _alloc_folder_payload():
    return {
        "node_origin": "plannedallocation",
        "planned_allocation": True,
        "is_folder": True,
        "name": "Alloc",
        "base_display_label": "Alloc",
        "item_path": r"General\Projects\AllocFolder",
        "destination_path": r"General\Projects\AllocFolder",
    }


def _move_dict():
    return {
        "source_path": r"SourceLib\FolderA",
        "destination_path": r"General\Projects\AllocFolder",
        "source": {
            "is_folder": True,
            "name": "FolderA",
            "drive_id": "drive1",
            "id": "src-item-1",
            "item_path": r"SourceLib\FolderA",
            "display_path": r"SourceLib\FolderA",
        },
    }


def _ten_dummy_descendants():
    out = []
    for i in range(10):
        out.append(
            {
                "name": f"f{i}.txt",
                "is_folder": False,
                "item_path": rf"SourceLib\FolderA\sub\f{i}.txt",
            }
        )
    return out


@pytest.fixture
def mw_graph_auth():
    mw = MainWindow.__new__(MainWindow)
    with patch(
        "ozlink_console.main_window.destination_authority_contract.graph_owns_visible_real_destination_structure",
        return_value=True,
    ):
        yield mw


class TestDescendantResumeAcrossRestart:
    def test_mid_injection_resume_seeds_desc_index_and_logs(
        self, mw_graph_auth, monkeypatch
    ):
        """Simulates snapshot + reopen: persisted resume fields seed nonzero desc_index; log line emitted."""
        mw = mw_graph_auth
        model = DestinationPlanningTreeModel()
        mw.destination_planning_model = model
        pl = _alloc_folder_payload()
        model.reset_root_payloads([pl])
        ix = model.index(0, 0, QModelIndex())
        move = _move_dict()
        src_root = dict(move["source"])

        captured: list[tuple[str, dict]] = []

        def _capture(msg: str, **data):
            captured.append((msg, dict(data)))

        monkeypatch.setattr("ozlink_console.main_window.log_info", _capture)

        def _canon_dest_anchor(p: str) -> str:
            return p or r"General\Projects\AllocFolder"

        # Token must use the same path helpers as _build_destination_graph_allocation_descendant_apply_state
        monkeypatch.setattr(mw, "_canonical_destination_path_with_visible_library_anchor", _canon_dest_anchor)
        monkeypatch.setattr(mw, "_allocation_projection_path", lambda m: m.get("destination_path") or "")
        token = mw._destination_allocation_projection_resume_token(move, src_root)
        assert len(token) == 32

        monkeypatch.setattr(
            mw,
            "_collect_source_descendants_for_projection",
            lambda *_a, **_k: _ten_dummy_descendants(),
        )
        monkeypatch.setattr(mw, "_sort_descendants_for_allocation_apply", lambda d: d)
        monkeypatch.setattr(mw, "_find_source_item_for_planned_move", lambda *_a, **_k: None)

        def _stamp_resume(p, t=token):
            p["allocation_projection_resume_source_token"] = t
            p["allocation_projection_resume_desc_index"] = 7
            p["allocation_projection_resume_descendants_total"] = 10

        model.update_payload_for_index(ix, _stamp_resume)

        st = mw._build_destination_graph_allocation_descendant_apply_state(ix, move, None)
        assert st is not None
        assert st["desc_index"] == 7
        assert len(st["descendants"]) == 10

        seeded_msgs = [c for c in captured if c[0] == "allocation_projection_resume_desc_index_seeded"]
        assert len(seeded_msgs) == 1
        fields = seeded_msgs[0][1]
        assert fields.get("desc_index") == 7
        assert fields.get("descendants_total") == 10
        assert fields.get("token_suffix") == token[-8:]

    def test_flush_before_capture_writes_resume_fields(self, mw_graph_auth):
        mw = mw_graph_auth
        model = MagicMock()
        model2 = DestinationPlanningTreeModel()
        model2.reset_root_payloads([_alloc_folder_payload()])
        ix = model2.index(0, 0, QModelIndex())
        mw.destination_planning_model = model
        move = _move_dict()
        descendants = _ten_dummy_descendants()
        mw._destination_descendant_apply_state = {
            "graph_walk": True,
            "parent_ix": ix,
            "move": move,
            "descendants": descendants,
            "desc_index": 4,
        }
        with patch.object(mw, "_find_source_item_for_planned_move", return_value=None):
            mw._destination_flush_descendant_apply_resume_to_model_payloads()

        model.update_payload_for_index.assert_called_once()
        mut = model.update_payload_for_index.call_args[0][1]
        p: dict = {}
        mut(p)
        assert p["allocation_projection_resume_desc_index"] == 4
        assert p["allocation_projection_resume_descendants_total"] == 10
        assert p["allocation_projection_resume_source_token"]

    def test_completed_job_removes_resume_keys(self, mw_graph_auth):
        mw = mw_graph_auth
        model = MagicMock()
        ix = MagicMock()
        ix.isValid.return_value = True
        ix.column.return_value = 0
        ix.siblingAtColumn.return_value = ix
        mw.destination_planning_model = model
        mw._mark_allocation_descendants_applied_on_allocation_folder_model_index(ix, _move_dict())
        mut = model.update_payload_for_index.call_args[0][1]
        p = {
            "allocation_descendants_applied": False,
            "allocation_projection_resume_source_token": "x",
            "allocation_projection_resume_desc_index": 3,
            "allocation_projection_resume_descendants_total": 10,
        }
        mut(p)
        assert p.get("allocation_projection_resume_source_token") is None
        assert "allocation_projection_resume_desc_index" not in p
        assert p.get("allocation_descendants_applied") is True

    def test_stale_resume_rejected_when_invalidate_clears_keys(self, mw_graph_auth):
        mw = mw_graph_auth
        bad_ix = QModelIndex()
        move = _move_dict()
        nd = {
            "node_origin": "plannedallocation",
            "is_folder": True,
            "children_loaded": True,
            "allocation_projection_destination_path_saved": r"General\Projects\Wrong",
            "allocation_projection_resume_source_token": "tok",
            "allocation_projection_resume_desc_index": 5,
            "allocation_projection_resume_descendants_total": 99,
        }
        result = mw._invalidate_stale_destination_allocation_projection_index(bad_ix, nd, move)
        assert "allocation_projection_resume_source_token" not in result


class TestStructureChangedCoalescing:
    def test_uncoalesced_mutations_match_signal_count(self):
        """Baseline: each structural mutation emits destination_structure_changed (custom signal)."""
        model = DestinationPlanningTreeModel()
        hits: list[int] = []
        model.destination_structure_changed.connect(lambda: hits.append(1))
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
        hits.clear()
        root_ix = model.index(0, 0, QModelIndex())
        for i in range(8):
            model.append_child_payloads(
                root_ix,
                [
                    {
                        "base_display_label": f"x{i}",
                        "name": f"x{i}.txt",
                        "is_folder": False,
                        "item_path": rf"R\x{i}.txt",
                    }
                ],
            )
        assert len(hits) == 8

    def test_coalesced_block_emits_one_custom_structure_signal(self):
        """With coalescing (same pattern as one descendant-apply tick), one emit for many mutations."""
        model = DestinationPlanningTreeModel()
        hits: list[int] = []
        model.destination_structure_changed.connect(lambda: hits.append(1))
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
        hits.clear()
        root_ix = model.index(0, 0, QModelIndex())
        model.begin_coalesce_destination_structure_signal()
        try:
            for i in range(8):
                model.append_child_payloads(
                    root_ix,
                    [
                        {
                            "base_display_label": f"x{i}",
                            "name": f"x{i}.txt",
                            "is_folder": False,
                            "item_path": rf"R\x{i}.txt",
                        }
                    ],
                )
        finally:
            model.end_coalesce_destination_structure_signal()
        assert len(hits) == 1


class TestInjectTickProfile:
    def test_destination_inject_tick_profile_logged_when_env_set(self, mw_graph_auth, monkeypatch):
        monkeypatch.setenv("OZLINK_DEST_INJECT_PROFILE", "1")
        mw = mw_graph_auth
        mw.destination_planning_model = DestinationPlanningTreeModel()
        mw._destination_descendant_apply_queue = None
        mw._destination_descendant_apply_state = None
        mw._destination_descendant_apply_inline_drain = False
        captured: list[tuple[str, dict]] = []

        def _cap(msg: str, **data):
            captured.append((msg, dict(data)))

        monkeypatch.setattr("ozlink_console.main_window.log_info", _cap)
        mw._run_destination_descendant_apply_tick_body()
        prof = [c for c in captured if c[0] == "destination_inject_tick_profile"]
        assert len(prof) == 1
        row = prof[0][1]
        assert "wall_ms" in row
        assert "graph_ops" in row
        assert "max_graph_ops" in row

    def test_inject_profile_aggregate_empty_queue_ticks(self, mw_graph_auth, monkeypatch):
        """Measured summary for idle ticks (empty queue): wall_ms distribution for profiling harness."""
        monkeypatch.setenv("OZLINK_DEST_INJECT_PROFILE", "1")
        mw = mw_graph_auth
        mw.destination_planning_model = DestinationPlanningTreeModel()
        mw._destination_descendant_apply_queue = None
        mw._destination_descendant_apply_state = None
        mw._destination_descendant_apply_inline_drain = False
        rows: list[dict] = []

        def _cap(msg: str, **data):
            if msg == "destination_inject_tick_profile":
                rows.append(dict(data))

        monkeypatch.setattr("ozlink_console.main_window.log_info", _cap)
        n = 40
        for _ in range(n):
            mw._run_destination_descendant_apply_tick_body()
        assert len(rows) == n
        walls = sorted(float(r["wall_ms"]) for r in rows)
        avg = statistics.mean(walls)
        p95 = walls[int(0.95 * (len(walls) - 1))]
        ops = [int(r["graph_ops"]) for r in rows]
        assert all(o == 0 for o in ops)
        # Publish for humans reading pytest output (values are environment-dependent)
        print(
            "inject_profile_aggregate_empty_queue_ticks",
            f"n_ticks={n}",
            f"avg_wall_ms={avg:.4f}",
            f"p95_wall_ms={p95:.4f}",
            f"avg_graph_ops={statistics.mean(ops):.4f}",
        )
