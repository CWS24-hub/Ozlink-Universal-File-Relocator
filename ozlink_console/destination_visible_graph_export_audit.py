"""
Offline and in-session compare of *visible* destination model exports vs *Graph* list exports.

Used for debugging layer union issues (memory/planned vs live Graph) without calling Graph again.
See log event ``destination_visible_vs_graph_path_diff_audit`` for the emitted summary.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple


def normalize_export_path(s: str) -> str:
    v = str(s or "").strip().replace("/", "\\")
    v = v.strip("\\")
    return v.casefold()


def load_destination_export_json(path: Path) -> dict[str, Any]:
    p = Path(path)
    with open(p, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        return {}
    return data


def _rows_flat(export_doc: dict[str, Any]) -> list[dict[str, Any]]:
    for k in ("rows_flat_depth_first", "rows_flat", "rows"):
        v = export_doc.get(k)
        if isinstance(v, list):
            return [r for r in v if isinstance(r, dict)]
    return []


def _visible_path_to_state(export_doc: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for r in _rows_flat(export_doc):
        raw = str(r.get("canonical_path") or r.get("path") or "").strip()
        if not raw:
            continue
        k = normalize_export_path(raw)
        if not k:
            continue
        lvp = str(r.get("graph_vs_planned") or r.get("lvp") or "other")
        if k not in out:
            out[k] = lvp
    return out


def _graph_path_set(export_doc: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for r in _rows_flat(export_doc):
        raw = str(r.get("canonical_path") or r.get("path") or "").strip()
        if not raw:
            continue
        k = normalize_export_path(raw)
        if k:
            out.add(k)
    return out


def _children_under_parent_name(flat: list[dict[str, Any]], parent: str) -> set[str]:
    pcf = (parent or "").strip().casefold()
    if not pcf:
        return set()
    out: set[str] = set()
    for r in flat:
        par = str(r.get("parent_canonical_path") or r.get("parent") or "").strip()
        if normalize_export_path(par) != pcf:
            continue
        ch = str(r.get("leaf_name") or r.get("name") or r.get("display_name") or "").strip()
        if ch:
            out.add(ch)
        cp = str(r.get("canonical_path") or "").strip()
        if cp and "\\" in cp:
            seg = cp.rsplit("\\", 1)[-1].strip()
            if seg:
                out.add(seg)
    return out


def _sample_keys(keys: set[str] | set, limit: int = 24) -> list[str]:
    return sorted({str(s) for s in keys if s}, key=str)[: max(0, int(limit))]


def build_visible_vs_graph_path_diff_audit(visible: dict[str, Any], graph: dict[str, Any]) -> dict[str, Any]:
    v_paths = set(_visible_path_to_state(visible).keys())
    g_paths = _graph_path_set(graph)
    vmap = _visible_path_to_state(visible)

    missing_in_visible = g_paths - v_paths
    missing_in_graph = v_paths - g_paths
    common = v_paths & g_paths

    mismatch: list[dict[str, str]] = []
    for c in common:
        lvp = (vmap.get(c) or "").strip() or "other"
        if lvp != "live_graph":
            mismatch.append({"path": c, "visible_graph_vs_planned": lvp})

    mist_sample = [m for m in mismatch[:80]]

    g_flat = _rows_flat(graph)
    v_flat = _rows_flat(visible)
    g_root3 = _children_under_parent_name(g_flat, "Root3")
    v_root3 = _children_under_parent_name(v_flat, "Root3")

    p_it = normalize_export_path(r"Root3\IT")
    p_mk = normalize_export_path(r"Root3\Marketing")
    p_fin = normalize_export_path(r"Root3\Finance")
    m_it = p_it in missing_in_visible
    m_mk = p_mk in missing_in_visible
    m_fn = p_fin in common and (vmap.get(p_fin) or "") != "live_graph"

    return {
        "graph_total": int(len(g_paths)),
        "visible_total": int(len(v_paths)),
        "graph_missing_from_visible_count": int(len(missing_in_visible)),
        "visible_missing_from_graph_count": int(len(missing_in_graph)),
        "common_path_count": int(len(common)),
        "common_path_state_mismatch_count": int(len(mismatch)),
        "missing_graph_sample": _sample_keys(missing_in_visible, 32),
        "visible_only_sample": _sample_keys(missing_in_graph, 32),
        "common_path_state_mismatch_sample": mist_sample,
        "root3_top_level_names_graph_only": sorted(g_root3 - v_root3)[:32],
        "root3_top_level_names_visible_only": sorted(v_root3 - g_root3)[:32],
        "root3_top_level_names_graph": sorted(g_root3)[:64],
        "root3_top_level_names_visible": sorted(v_root3)[:64],
        "audit_path_flags": {
            "graph_path_root3_it_missing_from_visible": m_it,
            "graph_path_root3_marketing_missing_from_visible": m_mk,
            "path_root3_finance_common_but_not_live_graph": m_fn,
        },
    }


def main() -> None:
    import sys

    if len(sys.argv) < 3:
        print(
            "usage: python -m ozlink_console.destination_visible_graph_export_audit <visible.json> <graph.json>",
            file=sys.stderr,
        )
        raise SystemExit(2)
    v = load_destination_export_json(Path(sys.argv[1]))
    g = load_destination_export_json(Path(sys.argv[2]))
    out = build_visible_vs_graph_path_diff_audit(v, g)
    print(json.dumps(out, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
