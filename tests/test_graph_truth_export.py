"""destination_graph_truth_export: comparison and path prefix helpers (no live Graph)."""

from __future__ import annotations

from ozlink_console.destination_graph_truth_export import (
    build_graph_truth_comparison_records,
    path_matches_canonical_prefix,
)


def test_path_matches_canonical_prefix():
    assert path_matches_canonical_prefix(r"Root3\Finance", r"Root3\Finance")
    assert path_matches_canonical_prefix(r"Root3\Finance\Payroll", r"Root3\Finance")
    assert not path_matches_canonical_prefix(r"Root3\HR", r"Root3\Finance")
    assert path_matches_canonical_prefix("", "")


def test_comparison_graph_exists_but_app_shows_planned():
    app = [
        {
            "canonical_path": r"Root3\Finance\Payroll",
            "name": "Payroll",
            "verification_state": "planned_only",
            "row_kind": "planned_folder",
            "graph_item_id": "",
            "visible_in_tree": True,
            "is_folder": True,
        }
    ]
    live = [
        {
            "canonical_path": r"Root3\Finance\Payroll",
            "graph_item_id": "live-id-1",
            "is_folder": True,
            "name": "Payroll",
        }
    ]
    comp = build_graph_truth_comparison_records(app, live)
    assert len(comp) == 1
    assert "graph_exists_but_app_shows_planned" in comp[0]["classifications"]
    det = comp[0].get("graph_exists_but_app_shows_planned_detail") or {}
    assert det.get("live_graph_item_id") == "live-id-1"


def test_comparison_graph_and_app_live_match():
    app = [
        {
            "canonical_path": r"Root3\A",
            "verification_state": "verified",
            "row_kind": "folder",
            "graph_item_id": "same",
            "visible_in_tree": True,
            "is_folder": True,
        }
    ]
    live = [{"canonical_path": r"Root3\A", "graph_item_id": "same", "is_folder": True}]
    comp = build_graph_truth_comparison_records(app, live)
    assert "graph_and_app_live_match" in comp[0]["classifications"]


def test_comparison_duplicate_same_path_in_app():
    app = [
        {
            "canonical_path": r"Root3\X",
            "graph_item_id": "a",
            "verification_state": "verified",
            "visible_in_tree": True,
            "is_folder": True,
        },
        {
            "canonical_path": r"Root3\X",
            "graph_item_id": "b",
            "verification_state": "verified",
            "visible_in_tree": True,
            "is_folder": True,
        },
    ]
    live = [{"canonical_path": r"Root3\X", "graph_item_id": "a", "is_folder": True}]
    comp = build_graph_truth_comparison_records(app, live)
    assert "duplicate_same_path_in_app" in comp[0]["classifications"]
