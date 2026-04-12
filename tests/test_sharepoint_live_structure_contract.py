"""Contract: SharePoint real tree rows come from live Graph; overlays do not filter real imports."""

from __future__ import annotations

import os
from types import MethodType

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QEventLoop, QModelIndex, QTimer
from PySide6.QtWidgets import QApplication

from ozlink_console.main_window import (
    MainWindow,
    destination_library_structural_fingerprint_from_items,
)
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _app():
    return QApplication.instance() or QApplication([])


def test_destination_real_tree_snapshot_pinned_without_future_model_build():
    """Narrow-restore / digest paths pin ``_destination_real_tree_snapshot`` without a future-model graph."""
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw.planned_moves = []
    mw.proposed_folders = []
    mw.pending_root_drive_ids = {"source": "", "destination": ""}
    mw._destination_full_tree_snapshot = []
    mw._destination_full_tree_completed_drive_id = ""
    mw._log_restore_phase = lambda *a, **k: None  # type: ignore[method-assign]

    fake_snap = [
        {
            "semantic_path": "Root\\Zeta",
            "parent_semantic_path": "Root",
            "data": {"name": "Zeta", "is_folder": True, "item_path": "Root/Zeta"},
            "children": [],
        },
    ]

    def _pin_snapshot(*_a, **_k):
        mw._destination_real_tree_snapshot = list(fake_snap)

    mw._refresh_destination_real_tree_snapshot = _pin_snapshot  # type: ignore[method-assign]
    mw._refresh_destination_real_tree_snapshot()
    assert mw._destination_real_tree_snapshot
    assert mw._destination_real_tree_snapshot[0]["semantic_path"] == "Root\\Zeta"


def test_remove_placeholder_children_normalizes_nonzero_column_parent():
    """beginRemoveRows must use column-0 parents; rowCount() is 0 for column > 0."""
    _app()
    m = DestinationPlanningTreeModel()
    root = QModelIndex()
    m.append_child_payloads(
        root,
        [{"name": "Folder", "is_folder": True, "tree_role": "destination"}],
    )
    parent0 = m.index(0, 0, root)
    m.append_child_payloads(
        parent0,
        [
            {
                "placeholder": True,
                "placeholder_role": "projection_pending",
                "tree_role": "destination",
            }
        ],
    )
    assert m.rowCount(parent0) == 1
    parent_bad_col = m.index(0, 2, root)
    assert parent_bad_col.column() > 0
    m.remove_placeholder_children(parent_bad_col)
    assert m.rowCount(parent0) == 0


class _SharePointAuthoritativeMaterializeHost:
    """Minimal shell for :meth:`MainWindow._apply_destination_planning_overlays_body` SharePoint gate + overlay pass."""

    _destination_sharepoint_planning_destination_active = MainWindow._destination_sharepoint_planning_destination_active

    def __init__(self) -> None:
        self.logged: list[tuple[str, dict]] = []
        self.destination_planning_model = object()
        self.pending_folder_loads = {"source": set(), "destination": set()}
        self.pending_root_drive_ids = {"source": "", "destination": ""}
        self._destination_full_tree_worker = None
        self._destination_full_tree_requested_drive_id = ""
        self._destination_full_tree_completed_drive_id = ""
        self._destination_future_model_pending_after_source_restore = False
        self._expand_all_pending = {"source": False, "destination": False}

    def _planning_browse_mode(self, key: str) -> str:
        return "graph" if key == "destination" else "local"

    def _destination_full_tree_ready(self) -> bool:
        return False

    def _destination_lifecycle_trace_TEMP(self, *args, **kwargs) -> None:
        return None

    def _try_skip_redundant_destination_future_model_materialize(self, _reason):
        return None

    def _cancel_destination_future_async_projection(self, *_a, **_k) -> None:
        return None

    def _destination_materialize_reason_should_queue_until_async_finishes(self, _reason: str) -> bool:
        return False

    def _merge_pending_materialize_after_async(self, a: str, b: str) -> str:
        return f"{a}|{b}".strip("|")

    def _park_deferred_destination_materialize_for_full_tree_wait(self, reason: str) -> str:
        return f"parked:{reason}"

    def _destination_future_model_blocked_by_source_restore(self, _reason) -> bool:
        return False

    def _should_defer_destination_materialization(self, _reason) -> bool:
        return False

    def _count_expandable_tree_nodes(self, _panel: str) -> int:
        return 0

    def _schedule_deferred_destination_materialization(self, _reason) -> None:
        return None

    def _set_tree_status_message(self, *_a, **_k) -> None:
        return None

    def _current_selected_destination_drive_id(self) -> str:
        return ""

    def _destination_snapshot_spo_trust_valid(self, _did: str) -> bool:
        return True

    def _reconcile_destination_semantic_duplicates(self, *_a, **_k) -> None:
        return None

    def _hydrate_destination_allocations_for_expanded_paths_model(self, *_a, **_k) -> None:
        return None

    def _hydrate_destination_prefix_chain_for_path_model(self, *_a, **_k) -> None:
        return None

    def _destination_on_authoritative_tree_bind_committed(self, *_a, **_k) -> None:
        return None

    def _dev_log_watch_folder_badge_owners(self, *_a, **_k) -> None:
        return None

    def _destination_expanded_paths_for_planning_bind(self):
        return []

    def _destination_selected_path_for_planning_bind(self):
        return ""

    def _replay_unresolved_proposed_overlay(self, *_a, **_k) -> int:
        return 0

    def _replay_unresolved_allocation_overlay(self, *_a, **_k) -> int:
        return 0

    def _apply_visible_destination_allocation_descendants(self, **_k) -> int:
        return 0

    def _schedule_refresh_destination_tree_indicators(self) -> None:
        return None

    def _restore_expanded_tree_paths(self, *_a, **_k) -> None:
        return None

    def _restore_selected_tree_path(self, *_a, **_k) -> None:
        return None

    def _refresh_expand_all_button_for_panel(self, *_a, **_k) -> None:
        return None

    def _current_destination_full_overlay_fingerprint(self):
        return ""

    def _bump_destination_materialized_overlay_fingerprint(self, **_k) -> None:
        return None

    def _log_restore_phase(self, phase, **data):
        self.logged.append((phase, dict(data)))


def test_sharepoint_materialize_bypasses_full_tree_gate_when_graph_authority_active():
    """Graph-owned SharePoint structure: full-tree readiness must not block overlay materialize."""
    host = _SharePointAuthoritativeMaterializeHost()
    out = MainWindow._apply_destination_planning_overlays_body(
        host,
        "idle_tick",
        allow_defer=False,
        prefer_chunked_projection=True,
    )
    assert out == 0
    assert any(
        p == "destination_future_model_materialize_full_tree_gate_bypass_sharepoint_live_graph_authority"
        and d.get("full_tree_worker_running") is False
        for p, d in host.logged
    )
    assert any(p == "destination_planning_overlay_pass" for p, _d in host.logged)


def test_ensure_full_tree_not_blocked_by_memory_restore_when_authority_shell_active():
    """Session memory-restore flag must not defer the full-library walk while the authority shell waits."""
    mw = MainWindow.__new__(MainWindow)
    mw._log_restore_phase = lambda *a, **k: None  # type: ignore[method-assign]
    mw.pending_root_drive_ids = {"source": "", "destination": "d-mem"}
    mw._destination_non_authoritative_shell_active = True
    mw._memory_restore_in_progress = True
    mw._destination_expand_burst_ctx = None
    mw._destination_future_bind_sync_active = False
    mw._destination_full_tree_worker = None
    mw._destination_full_tree_snapshot = []
    mw._destination_full_tree_completed_drive_id = ""
    started: list[str] = []
    mw.start_destination_full_tree_worker = lambda d: started.append(str(d))  # type: ignore[method-assign]
    MainWindow._ensure_sharepoint_destination_full_tree_worker_scheduled(mw, "d-mem")
    assert started == ["d-mem"]


def test_ensure_full_tree_not_blocked_by_expand_burst_when_authority_shell_active():
    """Destination restore expand-burst must not defer the full-library walk while the shell waits."""
    mw = MainWindow.__new__(MainWindow)
    mw._log_restore_phase = lambda *a, **k: None  # type: ignore[method-assign]
    mw.pending_root_drive_ids = {"source": "", "destination": "d-burst"}
    mw._destination_non_authoritative_shell_active = True
    mw._destination_expand_burst_ctx = {"run_id": 1}
    mw._destination_future_bind_sync_active = False
    mw._memory_restore_in_progress = False
    mw._destination_full_tree_worker = None
    mw._destination_full_tree_snapshot = []
    mw._destination_full_tree_completed_drive_id = ""
    started: list[str] = []
    mw.start_destination_full_tree_worker = lambda d: started.append(str(d))  # type: ignore[method-assign]
    MainWindow._ensure_sharepoint_destination_full_tree_worker_scheduled(mw, "d-burst")
    assert started == ["d-burst"]


def test_ensure_full_tree_when_shell_active_bypasses_ready_short_circuit():
    """Stale in-memory full-tree must not block scheduling while the authority shell is up."""
    mw = MainWindow.__new__(MainWindow)
    mw._log_restore_phase = lambda *a, **k: None  # type: ignore[method-assign]
    mw.pending_root_drive_ids = {"source": "", "destination": "d-shell"}
    mw._destination_full_tree_snapshot = [{"semantic_path": "Root\\A"}]
    mw._destination_full_tree_completed_drive_id = "d-shell"
    mw._destination_non_authoritative_shell_active = True
    mw._destination_future_bind_sync_active = False
    mw._memory_restore_in_progress = False
    mw._destination_expand_burst_ctx = None
    mw._destination_full_tree_worker = None
    started: list[str] = []
    mw.start_destination_full_tree_worker = lambda d: started.append(str(d))  # type: ignore[method-assign]
    MainWindow._ensure_sharepoint_destination_full_tree_worker_scheduled(mw, "d-shell")
    assert started == ["d-shell"]


def test_ensure_skips_when_snapshot_ready_and_authority_shell_cleared():
    mw = MainWindow.__new__(MainWindow)
    mw._log_restore_phase = lambda *a, **k: None  # type: ignore[method-assign]
    mw._destination_full_tree_snapshot = [{"semantic_path": "x"}]
    mw._destination_full_tree_completed_drive_id = "d1"
    mw.pending_root_drive_ids = {"source": "", "destination": "d1"}
    mw._current_selected_destination_drive_id = lambda: ""  # type: ignore[method-assign]
    mw._destination_non_authoritative_shell_active = False
    mw.destination_planning_model = None
    mw._destination_future_bind_sync_active = False
    mw._memory_restore_in_progress = False
    mw._destination_expand_burst_ctx = None
    mw._destination_full_tree_worker = None
    started: list[str] = []
    mw.start_destination_full_tree_worker = lambda d: started.append(str(d))  # type: ignore[method-assign]
    assert MainWindow._destination_full_tree_ready(mw) is True
    MainWindow._ensure_sharepoint_destination_full_tree_worker_scheduled(mw, "d1")
    assert started == []


def test_prepare_live_sharepoint_root_invalidates_full_tree_for_same_drive():
    """Fresh root Graph load must drop completed full-tree state so a new walk is not skipped."""
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw._log_restore_phase = lambda *a, **k: None  # type: ignore[method-assign]
    mw._planning_browse_mode = lambda k: "graph" if k == "destination" else "local"

    class _G:
        def invalidate_drive_root_children_cache(self, d: str) -> None:
            self.last = d

    mw.graph = _G()
    mw._destination_restore_materialization_queue = []
    mw._destination_restore_materialization_seen = set()
    mw._pending_snapshot_branch_refresh = {"destination": set()}
    mw._destination_full_tree_snapshot = [{"semantic_path": "Root\\A"}]
    mw._destination_full_tree_completed_drive_id = "lib-x"
    mw._destination_authoritative_recursive_item_total = 99
    mw._destination_authoritative_structure_fp = "fp"
    mw._destination_real_tree_snapshot_stale = False
    mw._mark_destination_real_tree_snapshot_stale = MethodType(
        MainWindow._mark_destination_real_tree_snapshot_stale, mw
    )
    mw._sync_restore_destination_overlay_pending_from_unresolved_queues = lambda: None  # type: ignore[method-assign]

    MainWindow._destination_prepare_live_sharepoint_root_load(mw, "lib-x")
    assert mw.graph.last == "lib-x"
    assert mw._destination_full_tree_snapshot == []
    assert mw._destination_full_tree_completed_drive_id == ""
    assert mw._destination_authoritative_recursive_item_total == 0


def test_invalidate_destination_full_tree_clears_snapshot_for_matching_drive():
    """force_refresh must allow a new live walk; completed snapshot for that drive is dropped."""
    mw = MainWindow.__new__(MainWindow)
    mw._log_restore_phase = lambda *a, **k: None  # type: ignore[method-assign]
    mw._destination_full_tree_snapshot = [{"semantic_path": "Root\\A"}]
    mw._destination_full_tree_completed_drive_id = "drive-1"
    mw._destination_authoritative_recursive_item_total = 42
    mw._destination_real_tree_snapshot_stale = False
    MainWindow._invalidate_destination_full_tree_for_live_reconcile(mw, "drive-1")
    assert mw._destination_full_tree_snapshot == []
    assert mw._destination_full_tree_completed_drive_id == ""
    assert mw._destination_authoritative_recursive_item_total == 0
    assert getattr(mw, "_destination_authoritative_structure_fp", "") == ""
    assert mw._destination_real_tree_snapshot_stale is True


def test_expand_gesture_blocked_for_sharepoint_until_full_tree_ready():
    mw = MainWindow.__new__(MainWindow)
    mw.destination_planning_model = object()
    mw.pending_folder_loads = {"destination": set()}
    mw.pending_root_drive_ids = {"destination": ""}
    mw._planning_browse_mode = lambda key: "graph" if key == "destination" else "local"
    mw._destination_chunked_bind_state = None
    mw._destination_future_bind_sync_active = False
    mw._destination_incremental_merge_in_progress = False
    mw._destination_incremental_merge_session = None
    mw._destination_future_projection_async_state = None
    mw._destination_full_tree_ready = lambda: False
    assert MainWindow._destination_pipeline_blocks_user_expand_gesture(mw) is True
    mw._destination_full_tree_ready = lambda: True
    mw._destination_snapshot_light_validation_worker = None
    assert MainWindow._destination_pipeline_blocks_user_expand_gesture(mw) is False


def test_root_children_fingerprint_stable_under_child_order():
    mw = MainWindow.__new__(MainWindow)
    mw.normalize_memory_path = MethodType(MainWindow.normalize_memory_path, mw)
    mw._destination_full_tree_snapshot = [
        {"parent_semantic_path": "Root", "data": {"id": "b"}},
        {"parent_semantic_path": "Root", "data": {"id": "a"}},
    ]
    fp, n = MainWindow._destination_root_children_fingerprint_from_full_snapshot(mw)
    assert n == 2
    mw2 = MainWindow.__new__(MainWindow)
    mw2.normalize_memory_path = MethodType(MainWindow.normalize_memory_path, mw2)
    mw2._destination_full_tree_snapshot = [
        {"parent_semantic_path": "Root", "data": {"id": "a"}},
        {"parent_semantic_path": "Root", "data": {"id": "b"}},
    ]
    fp2, _n2 = MainWindow._destination_root_children_fingerprint_from_full_snapshot(mw2)
    assert fp == fp2


class _SPOTrustGateMaterializeHost(_SharePointAuthoritativeMaterializeHost):
    """Full-tree snapshot exists but SharePoint trust flag is false — materialize must defer."""

    _destination_snapshot_spo_trust_valid = MainWindow._destination_snapshot_spo_trust_valid

    def __init__(self) -> None:
        super().__init__()
        self.pending_root_drive_ids = {"source": "", "destination": "drive-x"}
        self._destination_snapshot_valid_for_drive = False
        self._destination_snapshot_trust_drive_id = ""
        self._destination_snapshot_trust_monotonic = 0.0
        self._destination_snapshot_light_validation_worker = None
        self._destination_full_tree_worker = None
        self.light_validation_drive: str | None = None

    def _destination_full_tree_ready(self) -> bool:
        return True

    def _current_selected_destination_drive_id(self) -> str:
        return "drive-x"

    def _destination_begin_spo_snapshot_light_validation(self, drive_id: str) -> None:
        self.light_validation_drive = str(drive_id or "")


def test_sharepoint_materialize_gate_schedules_full_tree_when_no_worker():
    """When full tree is not ready, materialize must kick _ensure so drive context is not lost."""

    class _GateHost(_SharePointAuthoritativeMaterializeHost):
        def __init__(self) -> None:
            super().__init__()
            self.pending_root_drive_ids = {"source": "", "destination": "drive-sched"}
            self.scheduled_drives: list[str] = []

        def _ensure_sharepoint_destination_full_tree_worker_scheduled(self, drive_id: str) -> None:
            self.scheduled_drives.append(str(drive_id or ""))

    host = _GateHost()
    MainWindow._apply_destination_planning_overlays_body(
        host,
        "idle_tick",
        allow_defer=False,
        prefer_chunked_projection=True,
    )
    assert "drive-sched" in host.scheduled_drives
    assert any(
        p == "destination_future_model_materialize_full_tree_gate_bypass_sharepoint_live_graph_authority"
        and d.get("pending_or_selected_drive_id") == "drive-sched"
        for p, d in host.logged
    )


def test_full_tree_ready_true_when_completed_snapshot_and_no_conflicting_selector():
    mw = MainWindow.__new__(MainWindow)
    mw._destination_full_tree_snapshot = [{"semantic_path": "Root\\A"}]
    mw._destination_full_tree_completed_drive_id = "drive-z"
    mw.pending_root_drive_ids = {"destination": ""}
    mw._current_selected_destination_drive_id = lambda: ""  # type: ignore[method-assign]
    assert MainWindow._destination_full_tree_ready(mw) is True


def test_full_tree_ready_false_when_selector_library_differs_from_completed_snapshot():
    mw = MainWindow.__new__(MainWindow)
    mw._destination_full_tree_snapshot = [{"semantic_path": "Root\\A"}]
    mw._destination_full_tree_completed_drive_id = "drive-z"
    mw.pending_root_drive_ids = {"destination": ""}
    mw._current_selected_destination_drive_id = lambda: "other-drive"  # type: ignore[method-assign]
    assert MainWindow._destination_full_tree_ready(mw) is False


def test_destination_structure_fingerprint_changes_on_rename_same_count():
    """Count-preserving rename must change the structural digest (path+id rows)."""
    ctx: list[str] = []
    a = [
        {"id": "1", "item_path": "/x", "display_path": "Root\\FolderA\\f.txt"},
        {"id": "2", "item_path": "/y", "display_path": "Root\\FolderB\\g.txt"},
    ]
    b = [
        {"id": "1", "item_path": "/x", "display_path": "Root\\FolderRenamed\\f.txt"},
        {"id": "2", "item_path": "/y", "display_path": "Root\\FolderB\\g.txt"},
    ]
    fa, na = destination_library_structural_fingerprint_from_items(a, context_segments=ctx)
    fb, nb = destination_library_structural_fingerprint_from_items(b, context_segments=ctx)
    assert na == nb == 2
    assert fa != fb


def test_light_validation_structure_mismatch_schedules_full_walk():
    mw = MainWindow.__new__(MainWindow)
    mw._log_restore_phase = lambda *a, **k: None  # type: ignore[method-assign]
    mw.normalize_memory_path = MethodType(MainWindow.normalize_memory_path, mw)
    mw._destination_full_tree_completed_drive_id = "d1"
    mw._destination_full_tree_snapshot = [
        {
            "semantic_path": "Root\\A",
            "parent_semantic_path": "Root",
            "data": {"id": "id-a"},
            "children": [],
        },
    ]
    mw._destination_authoritative_recursive_item_total = 1
    mw._destination_authoritative_structure_fp = "aaa"
    calls: list[str] = []

    def _invalidate(_self, d):
        calls.append(f"invalidate:{d}")

    def _start(_self, d):
        calls.append(f"walk:{d}")

    mw._invalidate_destination_full_tree_for_live_reconcile = MethodType(  # type: ignore[method-assign]
        _invalidate, mw
    )
    mw.start_destination_full_tree_worker = MethodType(_start, mw)  # type: ignore[method-assign]
    rf, _rn = MainWindow._destination_root_children_fingerprint_from_full_snapshot(mw)
    MainWindow._on_destination_spo_snapshot_light_validation_success(
        mw,
        {
            "drive_id": "d1",
            "root_fingerprint": rf,
            "root_child_count": 1,
            "recursive_item_total_live": 1,
            "structure_fingerprint": "bbb",
        },
    )
    _loop = QEventLoop()
    QTimer.singleShot(0, _loop.quit)
    _loop.exec()
    QApplication.processEvents()
    assert any(c.startswith("invalidate:") for c in calls)
    assert any(c.startswith("walk:") for c in calls)


def test_full_tree_success_flushes_non_auth_shell_via_non_deferring_materialize():
    mw = MainWindow.__new__(MainWindow)
    mw._log_restore_phase = lambda *a, **k: None  # type: ignore[method-assign]
    mw.normalize_memory_path = MethodType(MainWindow.normalize_memory_path, mw)
    mw._path_segments = MethodType(MainWindow._path_segments, mw)
    mw._destination_non_authoritative_shell_active = True
    mw._destination_expand_all_after_full_tree = False
    mw._active_destination_full_tree_worker_id = 1
    mw._destination_full_tree_sequence = 1
    mw._destination_full_tree_requested_drive_id = "d1"
    recorded: list[tuple[str, bool]] = []

    def _mat(_self, reason, *, allow_defer=True, prefer_chunked_projection=False, narrow_restore_real_snapshot=False):
        recorded.append((str(reason), bool(allow_defer)))
        return 1

    def _canon(_self, path):
        p = str(path or "").strip()
        return p if p.startswith("Root") else MainWindow._canonical_destination_projection_path(_self, path)

    mw._apply_destination_planning_overlays = MethodType(_mat, mw)  # type: ignore[method-assign]
    mw._canonical_destination_projection_path = MethodType(_canon, mw)  # type: ignore[method-assign]
    mw._mark_destination_real_tree_snapshot_stale = lambda: None  # type: ignore[method-assign]
    mw._destination_mark_spo_snapshot_trust_valid = lambda *a, **k: None  # type: ignore[method-assign]
    mw._destination_flush_trust_gate_pending_materialize = lambda: None  # type: ignore[method-assign]
    mw._flush_parked_destination_materialize_after_full_tree_completion = lambda: None  # type: ignore[method-assign]
    mw._destination_full_tree_context = lambda: {}  # type: ignore[method-assign]
    payload = {
        "drive_id": "d1",
        "items": [{"id": "n1", "display_path": "Root\\Z", "item_path": "Root/Z"}],
    }
    MainWindow.on_destination_full_tree_success(mw, payload, 1)
    assert ("destination_full_tree_after_non_auth_shell", False) in recorded


def test_snapshot_branch_refresh_noops_while_destination_authority_shell_visible():
    """Saved-branch restore must not spin against placeholder rows; wait for authoritative bind."""
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw._log_restore_phase = lambda *a, **k: None  # type: ignore[method-assign]
    mw._destination_lifecycle_trace_TEMP = lambda *a, **k: None  # type: ignore[method-assign]
    mw._planning_browse_mode = lambda k: "graph" if k == "destination" else "local"
    mw.destination_planning_model = DestinationPlanningTreeModel()
    mw._pending_snapshot_branch_refresh = {"source": set(), "destination": {"Root\\X"}}
    mw.destination_tree_widget = object()
    dm = mw.destination_planning_model
    dm.reset_root_payloads(
        [
            {
                "placeholder": True,
                "placeholder_role": "destination_authority_pending",
                "tree_role": "destination",
                "base_display_label": "Reconciling…",
            },
            {"name": "X", "is_folder": True, "tree_role": "destination", "base_display_label": "X"},
        ]
    )
    MainWindow._process_snapshot_branch_refresh(mw, "destination")
    assert mw._pending_snapshot_branch_refresh["destination"] == {"Root\\X"}


def test_live_refresh_blocked_for_restore_ux_false_while_authority_shell():
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "graph" if k == "destination" else "local"
    mw.destination_planning_model = DestinationPlanningTreeModel()
    dm = mw.destination_planning_model
    dm.reset_root_payloads(
        [
            {
                "placeholder": True,
                "placeholder_role": "destination_authority_pending",
                "tree_role": "destination",
                "base_display_label": "Reconciling…",
            },
            {"name": "X", "is_folder": True, "tree_role": "destination"},
        ]
    )
    mw._memory_restore_in_progress = True
    assert MainWindow._destination_live_refresh_blocked_for_restore_ux(mw) is False


def test_authoritative_preview_idle_false_while_shell_placeholder_visible():
    mw = MainWindow.__new__(MainWindow)
    mw._log_restore_phase = lambda *a, **k: None  # type: ignore[method-assign]
    mw.pending_folder_loads = {"destination": set()}
    mw._planning_browse_mode = lambda key: "graph" if key == "destination" else "local"
    mw.destination_planning_model = DestinationPlanningTreeModel()
    mw._destination_full_tree_snapshot = [{"semantic_path": "Root\\X"}]
    mw._destination_full_tree_completed_drive_id = "d9"
    mw.pending_root_drive_ids = {"destination": "d9"}
    mw._destination_snapshot_valid_for_drive = True
    mw._destination_snapshot_trust_drive_id = "d9"
    mw._destination_snapshot_trust_monotonic = __import__("time").monotonic()
    dm = mw.destination_planning_model
    dm.reset_root_payloads(
        [
            {
                "placeholder": True,
                "placeholder_role": "destination_authority_pending",
                "tree_role": "destination",
                "base_display_label": "Reconciling…",
            },
            {"name": "X", "is_folder": True, "tree_role": "destination"},
        ]
    )
    assert MainWindow._destination_authoritative_preview_pipeline_idle(mw) is False


def test_destination_prepare_live_sharepoint_invalidates_graph_root_cache():
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw._log_restore_phase = lambda *a, **k: None  # type: ignore[method-assign]
    mw._destination_full_tree_completed_drive_id = ""
    mw._destination_full_tree_snapshot = []
    mw._destination_authoritative_recursive_item_total = 0
    mw._destination_authoritative_structure_fp = ""
    mw._destination_snapshot_valid_for_drive = False
    mw._destination_snapshot_trust_drive_id = ""
    mw._destination_snapshot_trust_monotonic = 0.0
    invalidated: list[str] = []

    class _G:
        def invalidate_drive_root_children_cache(self, did):
            invalidated.append(str(did))

    mw.graph = _G()
    mw._destination_restore_materialization_queue = ["p"]
    mw._destination_restore_materialization_seen = {"s"}
    mw._pending_snapshot_branch_refresh = {"source": {"a"}, "destination": {"old"}}
    mw._mark_destination_real_tree_snapshot_stale = lambda: invalidated.append("stale")  # type: ignore[method-assign]
    mw._sync_restore_destination_overlay_pending_from_unresolved_queues = (  # type: ignore[method-assign]
        lambda: invalidated.append("sync")
    )
    MainWindow._destination_prepare_live_sharepoint_root_load(mw, "b!driveid123")
    assert "b!driveid123" in invalidated
    assert "stale" in invalidated
    assert "sync" in invalidated
    assert mw._destination_restore_materialization_queue == []
    assert mw._destination_restore_materialization_seen == set()
    assert mw._pending_snapshot_branch_refresh["destination"] == set()


def test_sharepoint_materialize_deferred_when_snapshot_trust_invalid():
    host = _SPOTrustGateMaterializeHost()
    out = MainWindow._apply_destination_planning_overlays_body(
        host,
        "idle_tick",
        allow_defer=False,
        prefer_chunked_projection=True,
    )
    assert out == 0
    assert host.light_validation_drive == "drive-x"
    assert any(
        p == "destination_future_model_materialize_deferred" and d.get("waiting_for_snapshot_trust") is True
        for p, d in host.logged
    )
