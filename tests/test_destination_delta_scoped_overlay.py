"""Branch-scoped delta overlay path computation."""

from __future__ import annotations

from ozlink_console.destination_delta_scoped_overlay import build_delta_overlay_scope_paths
from ozlink_console.destination_live_memory_conflicts import normalize_path_key


def _norm(p: str) -> str:
    return str(p or "").strip().replace("/", "\\")


def test_a_scope_includes_hr_branch_not_finance() -> None:
    live = {
        normalize_path_key(r"root3\hr"): {
            "path": r"Root3\HR",
        }
    }
    scope = build_delta_overlay_scope_paths(
        live_meta_by_key=live,
        proposed_paths=[r"Root3\HR", r"Root3\Finance\A"],
        planned_paths=[],
        allocation_paths=[],
        normalize=_norm,
    )
    assert any(normalize_path_key(p) == normalize_path_key(r"Root3\HR") for p in scope)
    finance_key = normalize_path_key(r"Root3\Finance\A")
    assert not any(normalize_path_key(p) == finance_key for p in scope)


def test_f_proposed_under_branch_included() -> None:
    live = {normalize_path_key(r"root3\hr"): {"path": r"Root3\HR"}}
    scope = build_delta_overlay_scope_paths(
        live_meta_by_key=live,
        proposed_paths=[r"Root3\HR\Onboarding"],
        planned_paths=[],
        allocation_paths=[],
        normalize=_norm,
    )
    assert any("Onboarding" in p for p in scope)


def test_g_allocation_path_under_branch() -> None:
    live = {normalize_path_key(r"root3\hr"): {"path": r"Root3\HR"}}
    scope = build_delta_overlay_scope_paths(
        live_meta_by_key=live,
        proposed_paths=[],
        planned_paths=[],
        allocation_paths=[r"Root3\HR\file.txt"],
        normalize=_norm,
    )
    assert any("file.txt" in p for p in scope)


def test_h_empty_live_yields_empty_scope() -> None:
    scope = build_delta_overlay_scope_paths(
        live_meta_by_key={},
        proposed_paths=[r"Root3\HR"],
        planned_paths=[],
        allocation_paths=[],
        normalize=_norm,
    )
    assert scope == set()
