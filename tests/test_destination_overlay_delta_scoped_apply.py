"""Scoped overlay after delta applies narrow materialize (not global overlay)."""

from __future__ import annotations

from ozlink_console.destination_live_memory_conflicts import normalize_path_key
from ozlink_console.main_window import MainWindow


def test_scoped_overlay_calls_materialize_with_scope_not_global_overlay() -> None:
    mw = MainWindow.__new__(MainWindow)
    called = []

    def _mat_scoped(*_a, **kwargs):
        called.append(("scoped", kwargs))
        return 3

    def _global_overlay(*_a, **_k):
        called.append(("global",))

    mw._destination_memory_paths_for_live_conflict_detection = lambda: (
        [r"Root3\HR"],
        [],
        [],
    )  # type: ignore[method-assign]
    mw._destination_drfws_affected_paths = set()
    mw._destination_delta_entry_item_ids = lambda _d, _e: ["i1"]  # type: ignore[method-assign]
    mw._destination_live_path_meta_for_delta_item_ids = lambda _d, _ids: {  # type: ignore[method-assign]
        normalize_path_key(r"Root3\HR"): {"path": r"Root3\HR", "id": "i1", "name": "HR", "type": "folder"}
    }
    mw._destination_materialize_with_optional_overlay_scope = _mat_scoped  # type: ignore[method-assign]
    mw._apply_destination_planning_overlays = _global_overlay  # type: ignore[method-assign]

    MainWindow._destination_try_scoped_planning_overlay_after_delta(mw, "drive-1", [{"drive_id": "drive-1", "item_id": "i1"}])

    assert not any(k == "global" for k, _ in called)
    assert any(k == "scoped" and kw.get("scope_paths") for k, kw in called)


def test_scoped_overlay_no_hit_does_not_call_materialize() -> None:
    mw = MainWindow.__new__(MainWindow)

    def _boom(**_k):
        raise AssertionError("should not run")

    mw._destination_memory_paths_for_live_conflict_detection = lambda: (  # type: ignore[method-assign]
        [r"Root9\Zzz"],
        [],
        [],
    )
    mw._destination_delta_entry_item_ids = lambda _d, _e: ["i1"]  # type: ignore[method-assign]
    mw._destination_live_path_meta_for_delta_item_ids = lambda _d, _ids: {  # type: ignore[method-assign]
        normalize_path_key(r"Root3\HR"): {"path": r"Root3\HR"}
    }
    mw._destination_materialize_with_optional_overlay_scope = _boom  # type: ignore[method-assign]
    mw._destination_drfws_affected_paths = set()
    MainWindow._destination_try_scoped_planning_overlay_after_delta(mw, "drive-1", [{"drive_id": "drive-1", "item_id": "i1"}])
