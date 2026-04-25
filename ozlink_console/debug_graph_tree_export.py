"""
Debug: export live Microsoft Graph **read-only** tree for the selected destination drive.

Uses :meth:`ozlink_console.graph.GraphClient.list_drive_all_items_normalized` (same as full-library
enumeration). No UI model, no snapshots, no overlay store — **Graph only** (GETs via existing client).
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

from ozlink_console.logger import log_info


def _raw_graph_path_to_segments(raw: str) -> str:
    s = str(raw or "").strip()
    if s.startswith("/"):
        s = s[1:]
    return s.replace("/", "\\").strip()


def _parent_of_segments_path(canonical: str) -> str:
    c = str(canonical or "").strip().strip("\\")
    if not c:
        return ""
    parts = [p for p in c.split("\\") if p]
    if len(parts) <= 1:
        return ""
    return "\\".join(parts[:-1])


def _depth_from_canonical(canonical: str) -> int:
    parts = [p for p in str(canonical or "").split("\\") if p]
    return max(0, len(parts) - 1)


def _sort_children_key(node: Dict[str, Any]) -> str:
    r = (node.get("row") or {}) if isinstance(node.get("row"), dict) else {}
    return str(r.get("name") or r.get("display_name") or "").casefold()


def _build_nested_forest(
    flat_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not flat_rows:
        return []
    by_path: Dict[str, Dict[str, Any]] = {}
    for r in flat_rows:
        k = str(r.get("canonical_path") or "").strip()
        if not k:
            continue
        if k in by_path:
            continue
        by_path[k] = {"row": r, "children": []}
    roots: List[Dict[str, Any]] = []
    for r in flat_rows:
        k = str(r.get("canonical_path") or "").strip()
        p = str(r.get("parent_canonical_path") or "").strip()
        if k not in by_path:
            continue
        node = by_path[k]
        if not p or p not in by_path:
            roots.append(node)
        else:
            by_path[p]["children"].append(node)

    def _sort_ch(n: Dict[str, Any]) -> None:
        ch = n.get("children")
        if not isinstance(ch, list):
            return
        ch.sort(key=_sort_children_key)
        for c in ch:
            if isinstance(c, dict):
                _sort_ch(c)

    for rt in roots:
        _sort_ch(rt)
    roots.sort(key=_sort_children_key)
    return roots


def export_graph_destination_tree(
    host: Any,
    *,
    out_dir: Path,
    file_stem: str,
) -> Tuple[Path, Path, Dict[str, Any]]:
    t0 = time.perf_counter()
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = str(file_stem or "graph__unknown__unknown__export").strip().strip(".")
    txt_path = out_dir / f"{stem}.txt"
    json_path = out_dir / f"{stem}.json"

    summary: Dict[str, Any] = {
        "file_stem": stem,
        "export_kind": "graph_live_list_drive_all_items_normalized",
        "total_nodes": 0,
        "folder_count": 0,
        "file_count": 0,
        "max_depth": 0,
        "root_name": "",
        "duration_ms": 0.0,
    }

    graph = getattr(host, "graph", None)
    if graph is None or not getattr(graph, "token", None):
        err = "graph client or token missing"
        summary["error"] = err
        txt_path.write_text(f"graph export failed: {err}\n", encoding="utf-8")
        json_path.write_text(json.dumps({"summary": summary, "tree": [], "rows_flat_depth_first": []}, indent=2), encoding="utf-8")
        summary["duration_ms"] = round((time.perf_counter() - t0) * 1000.0, 2)
        _log_done(txt_path, json_path, 0, summary["duration_ms"])
        return txt_path, json_path, summary

    drive_id = ""
    if hasattr(host, "_current_selected_destination_drive_id"):
        try:
            drive_id = str(host._current_selected_destination_drive_id() or "").strip()  # type: ignore[union-attr, unused-ignore]
        except Exception:
            drive_id = ""
    if not drive_id:
        drive_id = str((getattr(host, "pending_root_drive_ids", None) or {}).get("destination") or "").strip()
    if not drive_id:
        err = "no destination drive id (select destination library / pending root)"
        summary["error"] = err
        txt_path.write_text(f"graph export failed: {err}\n", encoding="utf-8")
        json_path.write_text(json.dumps({"summary": summary, "tree": [], "rows_flat_depth_first": []}, indent=2), encoding="utf-8")
        summary["duration_ms"] = round((time.perf_counter() - t0) * 1000.0, 2)
        _log_done(txt_path, json_path, 0, summary["duration_ms"])
        return txt_path, json_path, summary

    ctx: Dict[str, Any] = {}
    try:
        if hasattr(host, "_destination_full_tree_context"):
            ctx = host._destination_full_tree_context() or {}
    except Exception:
        ctx = {}
    if not isinstance(ctx, dict):
        ctx = {}
    site_id = str(ctx.get("site_id") or "")
    site_name = str(ctx.get("site_name") or "")
    lib_id = str(ctx.get("library_id") or "") or drive_id
    lib_name = str(ctx.get("library_name") or "")

    try:
        items = graph.list_drive_all_items_normalized(  # type: ignore[union-attr, unused-ignore]
            drive_id,
            site_id=site_id,
            site_name=site_name,
            library_id=lib_id,
            library_name=lib_name,
            tree_role="destination",
        )
    except Exception as exc:
        summary["error"] = str(exc)[:500]
        txt_path.write_text(f"graph list_drive_all_items_normalized failed: {exc!s}\n", encoding="utf-8")
        json_path.write_text(json.dumps({"summary": summary, "tree": [], "rows_flat_depth_first": []}, indent=2), encoding="utf-8")
        summary["duration_ms"] = round((time.perf_counter() - t0) * 1000.0, 2)
        _log_done(txt_path, json_path, 0, summary["duration_ms"])
        return txt_path, json_path, summary

    flat: List[Dict[str, Any]] = []
    max_d = 0
    n_folder = 0
    n_file = 0
    root_guess = ""

    for it in list(items or []):
        if not isinstance(it, dict) or not it.get("id"):
            continue
        ip = _raw_graph_path_to_segments(str(it.get("item_path") or ""))
        try:
            norm = str(host.normalize_memory_path(str(ip)))  # type: ignore[union-attr, unused-ignore]
            m = str(host._canonical_planned_memory_path_for_graph_match(norm) or norm).strip()  # type: ignore[union-attr, unused-ignore]
        except Exception:
            m = str(ip).replace("/", "\\").strip()
        canon = m
        pcanon = _parent_of_segments_path(canon)
        dep = _depth_from_canonical(canon)
        max_d = max(max_d, dep)
        isf = bool(it.get("is_folder"))
        if isf:
            n_folder += 1
        else:
            n_file += 1
        if not root_guess and (ip or canon):
            s = str(canon or ip)
            parts = [p for p in s.replace("/", "\\").split("\\") if p]
            if parts:
                root_guess = parts[0]
        name = str(it.get("name") or "").strip() or "?"
        rec = {
            "depth": dep,
            "name": name,
            "display_name": name,
            "canonical_path": canon,
            "parent_canonical_path": pcanon,
            "graph_item_id": str(it.get("id") or ""),
            "is_folder": isf,
            "item_path_graph": str(it.get("item_path") or ""),
            "drive_id_suffix": str((it.get("drive_id") or drive_id) or "")[-16:],
            "source": "graph_list_drive_all_items_normalized",
        }
        flat.append(rec)

    # rows_flat: same order as Graph walk (list_drive_all_items)
    tree = _build_nested_forest(flat)
    summary["total_nodes"] = len(flat)
    summary["folder_count"] = n_folder
    summary["file_count"] = n_file
    summary["max_depth"] = int(max_d)
    summary["root_name"] = root_guess
    summary["destination_drive_id_suffix"] = str(drive_id)[-16:] if len(drive_id) > 16 else str(drive_id)
    duration_ms = round((time.perf_counter() - t0) * 1000.0, 2)
    summary["duration_ms"] = duration_ms

    lines: List[str] = [
        "graph destination tree (Graph API read-only, list_drive_all_items_normalized)",
        f"total_nodes={summary['total_nodes']} folders={n_folder} files={n_file} max_depth={max_d} root_guess={root_guess!r}",
        f"duration_ms={duration_ms}",
        "",
    ]
    for r in flat:
        sp = "  " * int(r.get("depth") or 0)
        n = r.get("name") or "?"
        kind = "folder" if r.get("is_folder") else "file"
        gids = str(r.get("graph_item_id") or "")
        cp = str(r.get("canonical_path") or "")
        d = int(r.get("depth") or 0)
        if len(gids) > 12:
            lines.append(f"{sp}[{d}] {n} | path={cp} | kind={kind} | id=…{gids[-12:]}")
        else:
            lines.append(f"{sp}[{d}] {n} | path={cp} | kind={kind} | id={gids}")

    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    out_payload = {
        "summary": summary,
        "tree": tree,
        "rows_flat_depth_first": flat,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out_payload, f, indent=2, ensure_ascii=False)
    _log_done(txt_path, json_path, len(flat), duration_ms)
    return txt_path, json_path, summary


def _log_done(txt_path: Path, json_path: Path, n: int, duration_ms: float) -> None:
    log_info(
        "debug_graph_destination_tree_exported",
        txt_path=str(txt_path)[:500],
        json_path=str(json_path)[:500],
        total_nodes=int(n),
        duration_ms=float(round(duration_ms, 2)),
    )
