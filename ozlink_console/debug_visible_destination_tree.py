"""
Temporary debug: export the destination QTreeView model as it is *right now* in memory (UI model only).

No Graph calls, no persisted snapshot rebuild, no overlay store — only
:class:`~ozlink_console.tree_models.destination_planning_model.DestinationPlanningTreeModel` rowCount/index walk.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple

from PySide6.QtCore import QModelIndex, Qt

from ozlink_console.logger import log_info
from ozlink_console.sharepoint_destination_overlay_attach import (
    destination_payload_is_live_graph_row,
    destination_payload_is_memory_overlay_row_for_reuse,
    destination_payload_is_planned_workspace_row,
    destination_payload_workspace_row_state,
)


def _live_vs_planned_label(pl: dict) -> str:
    if not isinstance(pl, dict):
        return "other"
    if pl.get("placeholder"):
        return "placeholder"
    if destination_payload_is_planned_workspace_row(pl):
        return "planned"
    if destination_payload_is_live_graph_row(pl):
        return "live_graph"
    if destination_payload_is_memory_overlay_row_for_reuse(pl):
        return "memory_or_overlay"
    w = str(destination_payload_workspace_row_state(pl) or "").strip()
    if w == "cached_provisional":
        return "cached_provisional"
    return w or "other"


def _row_dict_for_export(host: Any, col0: QModelIndex) -> Dict[str, Any]:
    pl = host._destination_model_index_user_role_dict(col0) if col0.isValid() else {}
    if not isinstance(pl, dict):
        pl = {}
    display = str(col0.data(Qt.DisplayRole) or "") if col0.isValid() else ""
    leaf = str(pl.get("name") or pl.get("base_display_label") or display or "").strip()
    raw_path = str(host._tree_item_path(pl) or "").strip()
    try:
        canon = str(
            host._canonical_planned_memory_path_for_graph_match(
                host.normalize_memory_path(str(raw_path or pl.get("item_path") or pl.get("destination_path") or ""))
            )
            or raw_path
            or ""
        ).strip()
    except Exception:
        canon = raw_path
    pcan = ""
    if col0.isValid():
        try:
            p = col0.parent()
        except Exception:
            p = QModelIndex()
        if p.isValid():
            try:
                ppl = host._destination_model_index_user_role_dict(p)
                if isinstance(ppl, dict):
                    praw = str(host._tree_item_path(ppl) or "").strip()
                    pcan = str(
                        host._canonical_planned_memory_path_for_graph_match(
                            host.normalize_memory_path(
                                str(praw or ppl.get("item_path") or ppl.get("destination_path") or "")
                            )
                        )
                        or praw
                        or ""
                    ).strip()
            except Exception:
                pcan = ""
    lvp = _live_vs_planned_label(pl)
    return {
        "depth": int(_depth_of_index(col0)),
        "display_text": display[:2000] if display else leaf,
        "leaf_name": leaf,
        "canonical_path": canon,
        "parent_canonical_path": pcan,
        "row_kind": str(pl.get("row_kind") or "")[:200],
        "verification_state": str(pl.get("verification_state") or "")[:200],
        "graph_item_id": str(pl.get("id") or "")[:200],
        "graph_vs_planned": lvp,
        "workspace_row_state": str(destination_payload_workspace_row_state(pl) or "")[:200],
        "placeholder": bool(pl.get("placeholder")),
        "drive_id_suffix": str((pl.get("drive_id") or pl.get("library_id") or "") or "")[-16:],
    }


def _depth_of_index(col0: QModelIndex) -> int:
    d = 0
    w = col0
    while w is not None and w.isValid():
        try:
            p = w.parent()
        except Exception:
            return d
        if not p.isValid():
            return d
        d += 1
        w = p
    return d


def _iter_visible_rows_in_order(model: Any) -> List[QModelIndex]:
    if model is None or not hasattr(model, "rowCount"):
        return []
    if hasattr(model, "iter_depth_first"):
        try:
            return list(model.iter_depth_first())  # type: ignore[no-untyped-call]
        except Exception:
            pass
    out: list[QModelIndex] = []

    def walk(parent: QModelIndex) -> None:
        try:
            n = int(model.rowCount(parent))
        except Exception:
            return
        for r in range(n):
            try:
                ix = model.index(r, 0, parent)
            except Exception:
                continue
            if not ix.isValid():
                continue
            out.append(ix)
            walk(ix)

    walk(QModelIndex())
    return out


def _build_nested(
    model: Any, host: Any, parent: QModelIndex
) -> List[Dict[str, Any]]:
    out: list[dict] = []
    if model is None:
        return out
    try:
        n = int(model.rowCount(parent))
    except Exception:
        return out
    for r in range(n):
        try:
            col0 = model.index(r, 0, parent)
        except Exception:
            continue
        if not col0.isValid():
            continue
        rowd = _row_dict_for_export(host, col0)
        node = {
            "row": rowd,
            "children": _build_nested(model, host, col0),
        }
        out.append(node)
    return out


def _format_txt_line(d: dict, depth: int) -> str:
    sp = "  " * int(depth)
    dpy = d.get("display_text") or d.get("leaf_name") or "?"
    canon = d.get("canonical_path") or ""
    pk = d.get("row_kind") or "?"
    gid = d.get("graph_item_id") or ""
    lvp = d.get("graph_vs_planned") or "?"
    vs = d.get("verification_state") or ""
    pcan = d.get("parent_canonical_path") or ""
    vs_part = f" verification={vs}" if str(vs).strip() else ""
    gid_part = f" graph_id=…{gid}" if len(str(gid)) > 8 else f" graph_id={gid or '—'}"
    return f"{sp}[{depth}] {dpy!s} | canonical={canon!s} | row_kind={pk!s} | {lvp!s}{vs_part} | p={pcan!s} |{gid_part}"


def export_visible_destination_tree(
    host: Any,
    *,
    out_dir: Path,
    file_stem: str,
) -> Tuple[Path, Path, Dict[str, Any]]:
    """Walk ``host.destination_planning_model``; write ``<file_stem>``.txt / .json. Returns (txt, json, summary)."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = str(file_stem or "visible__unknown__unknown__export").strip().strip(".")
    txt_path = out_dir / f"{stem}.txt"
    json_path = out_dir / f"{stem}.json"

    model = getattr(host, "destination_planning_model", None)
    summary: Dict[str, Any] = {
        "file_stem": stem,
        "export_kind": "visible_destination_planning_model_ui",
        "model_class": str(type(model).__name__) if model is not None else "None",
        "top_level_row_count": 0,
        "duplicate_top_level_names": {},
        "count_by_row_kind": {},
        "count_graph_vs_planned": {"live_graph": 0, "planned": 0, "other": 0},
        "more_than_one_root3_top_level": False,
        "total_visible_rows": 0,
    }

    if model is None:
        summary["error"] = "destination_planning_model is None"
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("destination_planning_model is None; nothing to export.\n")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump({"summary": summary, "tree": []}, f, indent=2, ensure_ascii=False)
        return txt_path, json_path, summary

    # --- summary from top level
    inv = QModelIndex()
    top_names: List[str] = []
    name_cf_counts: Counter[str] = Counter()
    root3_like = 0
    try:
        tr = int(model.rowCount(inv))
    except Exception:
        tr = 0
    summary["top_level_row_count"] = int(tr)
    for r in range(min(int(tr), 10_000)):
        try:
            tix = model.index(r, 0, inv)
        except Exception:
            continue
        if not tix.isValid():
            continue
        pl = host._destination_model_index_user_role_dict(tix) if tix.isValid() else {}
        if not isinstance(pl, dict) or pl.get("placeholder"):
            continue
        nm = str(pl.get("name") or pl.get("base_display_label") or "")[:200].strip()
        if nm:
            top_names.append(nm)
            name_cf_counts[nm.casefold()] += 1
        if str(pl.get("name") or "").strip().casefold() == "root3":
            root3_like += 1
    dups = {k: c for k, c in name_cf_counts.items() if c > 1}
    summary["duplicate_top_level_names"] = dups
    summary["more_than_one_root3_top_level"] = bool(root3_like > 1)

    rows: List[Dict[str, Any]] = []
    idx_list = _iter_visible_rows_in_order(model)
    rk_counts: Counter[str] = Counter()
    lvp_bucket = {"live_graph": 0, "planned": 0, "other": 0}

    for col0 in idx_list:
        d = _row_dict_for_export(host, col0)
        rows.append(d)
        rkk = (d.get("row_kind") or "empty").strip() or "empty"
        rk_counts[rkk] += 1
        lv = d.get("graph_vs_planned") or "other"
        if lv in ("live_graph",):
            lvp_bucket["live_graph"] += 1
        elif lv in ("planned",):
            lvp_bucket["planned"] += 1
        else:
            lvp_bucket["other"] += 1
    summary["count_by_row_kind"] = dict(sorted(rk_counts.items(), key=lambda x: (-x[1], x[0])))
    summary["count_graph_vs_planned"] = lvp_bucket
    summary["total_visible_rows"] = len(rows)

    lines: List[str] = [
        "visible destination tree (DestinationPlanningTreeModel, current UI only)",
        f"rows={summary['total_visible_rows']} top_level={summary['top_level_row_count']}",
        f"summary: row_kinds={summary['count_by_row_kind']} live_vs_planned={lvp_bucket}",
        f"duplicate top-level (casefold): {dups!r} root3_top>1={summary['more_than_one_root3_top_level']!r}",
        "",
    ]
    for d in rows:
        lines.append(_format_txt_line(d, int(d.get("depth") or 0)))

    tree = _build_nested(model, host, inv)
    payload: Dict[str, Any] = {
        "summary": summary,
        "tree": tree,
        "rows_flat_depth_first": rows,
    }

    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    log_info(
        "debug_visible_destination_tree_exported",
        txt=str(txt_path)[:500],
        json=str(json_path)[:500],
        n_rows=int(len(rows)),
    )
    return txt_path, json_path, summary
