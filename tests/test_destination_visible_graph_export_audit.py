"""Path-set compare for visible (UI) vs Graph (list_drive) debug exports + layer-union invariants (spec)."""

from __future__ import annotations

import pytest

from ozlink_console.destination_visible_graph_export_audit import (
    build_visible_vs_graph_path_diff_audit,
    load_destination_export_json,
    normalize_export_path,
)

_FIXTURE_AQUATIC = {
    "visible": {
        "summary": {
            "export_kind": "visible_destination_planning_model_ui",
            "total_visible_rows": 347,
            "count_graph_vs_planned": {"live_graph": 0, "planned": 345, "other": 2},
        },
        "rows_flat_depth_first": [
            {
                "canonical_path": "Root3",
                "parent_canonical_path": "",
                "graph_vs_planned": "planned",
                "leaf_name": "Root3",
            },
            {
                "canonical_path": "Root3\\Sales",
                "parent_canonical_path": "Root3",
                "graph_vs_planned": "planned",
                "leaf_name": "Sales",
            },
            {
                "canonical_path": "Root3\\Sales\\Pictures",
                "parent_canonical_path": "Root3\\Sales",
                "graph_vs_planned": "planned",
                "leaf_name": "Pictures",
            },
            {
                "canonical_path": "Root3\\Management\\Email attachments",
                "parent_canonical_path": "Root3\\Management",
                "graph_vs_planned": "planned",
            },
        ],
    },
    "graph": {
        "summary": {
            "export_kind": "graph_live_list_drive_all_items_normalized",
            "total_nodes": 93,
        },
        "rows_flat_depth_first": [
            {"canonical_path": "Root3", "parent_canonical_path": "", "name": "Root3"},
            {"canonical_path": "Root3\\IT", "parent_canonical_path": "Root3", "name": "IT", "is_folder": True},
            {
                "canonical_path": "Root3\\IT\\Contracts",
                "parent_canonical_path": "Root3\\IT",
                "name": "Contracts",
                "is_folder": True,
            },
            {
                "canonical_path": "Root3\\Marketing",
                "parent_canonical_path": "Root3",
                "name": "Marketing",
                "is_folder": True,
            },
            {
                "canonical_path": "Root3\\Sales",
                "parent_canonical_path": "Root3",
                "name": "Sales",
                "is_folder": True,
            },
            {
                "canonical_path": "Root3\\Sales\\Pictures",
                "parent_canonical_path": "Root3\\Sales",
                "name": "Pictures",
            },
        ],
    },
}


def test_normalize_export_path_casefold() -> None:
    assert normalize_export_path("Root3/IT") == normalize_export_path("root3\\it")


def test_path_diff_marks_graph_only_folders() -> None:
    a = build_visible_vs_graph_path_diff_audit(_FIXTURE_AQUATIC["visible"], _FIXTURE_AQUATIC["graph"])
    assert a["graph_missing_from_visible_count"] > 0
    assert a["common_path_count"] > 0
    miss = a["missing_graph_sample"]
    s_it = "root3\\it"
    s_mk = "root3\\marketing"
    have_it = s_it in {normalize_export_path(m) for m in miss} or s_it in miss
    have_m = s_mk in {normalize_export_path(m) for m in miss} or s_mk in miss
    # IT not in visible fixture -> must appear in missing-from-visible set
    assert have_it
    assert a["audit_path_flags"]["graph_path_root3_it_missing_from_visible"] is True
    assert a["audit_path_flags"]["graph_path_root3_marketing_missing_from_visible"] is True
    g_root3 = a["root3_top_level_names_graph"]
    v_root3 = a["root3_top_level_names_visible"]
    assert "IT" in g_root3
    assert "IT" not in v_root3
    assert "Sales" in v_root3
    g_only = a.get("root3_top_level_names_graph_only")
    assert isinstance(g_only, list) and "IT" in g_only


def test_planned_path_can_exist_in_graph_too() -> None:
    """Same path may exist in both: Graph and planned overlay; visible should be mergeable, not either-or in design."""
    a = build_visible_vs_graph_path_diff_audit(_FIXTURE_AQUATIC["visible"], _FIXTURE_AQUATIC["graph"])
    p_pic = "root3\\sales\\pictures"  # after normalize, folder names casefold
    v_doc = _FIXTURE_AQUATIC["visible"]
    g_doc = _FIXTURE_AQUATIC["graph"]
    v_paths = {line["canonical_path"].casefold() for line in v_doc["rows_flat_depth_first"] if line.get("canonical_path")}
    g_paths = {line["canonical_path"].casefold() for line in g_doc["rows_flat_depth_first"] if line.get("canonical_path")}
    for k in v_paths:
        if "pictures" in k and "sales" in k:
            p_pic = k
            break
    assert p_pic in v_paths
    assert p_pic in g_paths
    m_pic = [x for x in a["common_path_state_mismatch_sample"] if "pictures" in str(x.get("path", "")).casefold()]
    # Visible marks planned, not live_graph, so a mismatch is expected
    assert len(m_pic) >= 1
    assert m_pic[0]["visible_graph_vs_planned"] == "planned"


def test_layer_union_spec_both_retain_merged_paths() -> None:
    """E/D: future merge must allow Graph-only and planned-only branches under the same library root."""
    a = _FIXTURE_AQUATIC
    g_paths = {r["canonical_path"] for r in a["graph"]["rows_flat_depth_first"]}
    v_paths = {r["canonical_path"] for r in a["visible"]["rows_flat_depth_first"]}
    assert r"Root3\Sales\Pictures" in v_paths
    assert r"Root3\IT" in g_paths
    assert r"Root3\IT" not in v_paths


def test_load_nonexistent(tmp_path) -> None:
    p = tmp_path / "missing.json"
    with pytest.raises((FileNotFoundError, OSError)):
        load_destination_export_json(p)
