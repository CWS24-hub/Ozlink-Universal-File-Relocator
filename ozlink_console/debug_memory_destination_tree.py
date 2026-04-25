"""
Debug: export **raw** in-memory destination tree snapshot (session / runtime buffers).

Same source order as :meth:`MainWindow._destination_apply_provisional_session_snapshot_if_eligible`:
``_pending_session_tree_snapshots['destination']`` or ``_runtime_session_tree_snapshots['destination']``.

Read-only: no UI model walk, no Graph, no overlay-store transforms, no projection/injection.
"""

from __future__ import annotations

import json
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple

from ozlink_console.logger import log_info


def _memory_snapshot_destination_roots(host: Any) -> tuple[List[dict], str]:
    """
    Return (roots, source_label) matching provisional snapshot startup selection.
    """
    pending = getattr(host, "_pending_session_tree_snapshots", None) or {}
    if isinstance(pending, dict):
        pdest = list(pending.get("destination") or [])
        if pdest:
            return [x for x in pdest if isinstance(x, dict)], "pending_session_tree_snapshots.destination"
    rs = getattr(host, "_runtime_session_tree_snapshots", None) or {}
    if isinstance(rs, dict):
        rdest = list(rs.get("destination") or [])
        if rdest:
            return [x for x in rdest if isinstance(x, dict)], "runtime_session_tree_snapshots.destination"
    return [], "empty"


def _parent_for_path(canonical: str) -> str:
    c = str(canonical or "").strip().strip("\\")
    if not c:
        return ""
    parts = [p for p in c.replace("/", "\\").split("\\") if p]
    if len(parts) <= 1:
        return ""
    return "\\".join(parts[:-1])


def _path_depth(canonical: str) -> int:
    parts = [p for p in str(canonical or "").replace("/", "\\").split("\\") if p]
    return max(0, len(parts) - 1)


def _row_from_snapshot_node(
    node: dict,
    *,
    parent_canonical_default: str,
    depth: int,
) -> Dict[str, Any]:
    data = node.get("data") if isinstance(node.get("data"), dict) else {}
    sp = str(node.get("semantic_path") or "").strip()
    if not sp:
        sp = str(
            data.get("semantic_path")
            or data.get("item_path")
            or data.get("destination_path")
            or data.get("display_path")
            or ""
        ).strip()
    psp = str(node.get("parent_semantic_path") or "").strip()
    if not psp:
        psp = str(parent_canonical_default or "").strip() or _parent_for_path(sp)
    ch = list(node.get("children") or [])
    nch = len([c for c in ch if isinstance(c, dict)])
    # Materialization hints from stored flags only (no inference).
    ch_loaded = data.get("children_loaded")
    is_folder = bool(data.get("is_folder"))
    partial = None
    if is_folder:
        if ch_loaded is True and nch == 0:
            partial = "folder_marked_children_loaded_empty_stored_children"
        elif ch_loaded is not True and nch == 0:
            partial = "folder_stored_children_empty_not_loaded"
        else:
            partial = "folder_with_stored_children"
    else:
        partial = "file"
    return {
        "depth": int(depth),
        "canonical_path": sp,
        "parent_canonical_path": psp,
        "row_kind": str(data.get("row_kind") or ""),
        "verification_state": str(data.get("verification_state") or ""),
        "proposed": bool(data.get("proposed")),
        "placeholder": bool(data.get("placeholder")),
        "workspace_row_state": str(data.get("workspace_row_state") or ""),
        "is_folder": is_folder,
        "graph_item_id": str(data.get("id") or "")[:500],
        "children_loaded": ch_loaded,
        "graph_children_verified": data.get("graph_children_verified"),
        "needs_live_child_refresh": data.get("needs_live_child_refresh"),
        "destination_snapshot_cached": data.get("destination_snapshot_cached"),
        "stored_children_count": nch,
        "materialization_note": partial,
        "memory_payload": dict(data),
        "snapshot_node_keys": sorted(str(k) for k in node.keys()) if isinstance(node, dict) else [],
    }


def _walk_preorder(
    node: dict,
    parent_canon: str,
    depth: int,
    flat_out: List[Dict[str, Any]],
) -> Dict[str, Any]:
    row = _row_from_snapshot_node(node, parent_canonical_default=parent_canon, depth=depth)
    flat_out.append(row)
    canon = str(row.get("canonical_path") or "")
    kids_in: List[dict] = [c for c in (node.get("children") or []) if isinstance(c, dict)]
    child_trees: List[Dict[str, Any]] = []
    for c in kids_in:
        child_trees.append(_walk_preorder(c, canon, depth + 1, flat_out))
    return {"row": row, "children": child_trees}


def export_memory_destination_tree(
    host: Any,
    *,
    out_dir: Path,
    file_stem: str,
) -> Tuple[Path, Path, Dict[str, Any]]:
    t0 = time.perf_counter()
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = str(file_stem or "memory__unknown__unknown__export").strip().strip(".")
    txt_path = out_dir / f"{stem}.txt"
    json_path = out_dir / f"{stem}.json"

    roots, source = _memory_snapshot_destination_roots(host)
    summary: Dict[str, Any] = {
        "file_stem": stem,
        "export_kind": "memory_session_snapshot_destination",
        "snapshot_source": source,
        "total_nodes": 0,
        "top_level_count": len(roots),
        "count_by_type": {},
        "has_root3": False,
        "duration_ms": 0.0,
    }

    flat: List[Dict[str, Any]] = []
    tree_nested: List[Dict[str, Any]] = []
    for r in roots:
        tree_nested.append(_walk_preorder(r, "", 0, flat))

    summary["total_nodes"] = len(flat)
    rk = Counter(
        (str(x.get("row_kind") or "").strip() or "<empty>") for x in flat
    )
    wss = Counter(
        (str(x.get("workspace_row_state") or "").strip() or "<empty>") for x in flat
    )
    summary["count_by_type"] = {
        "by_row_kind": dict(sorted(rk.items(), key=lambda x: (-x[1], x[0]))),
        "by_workspace_row_state": dict(sorted(wss.items(), key=lambda x: (-x[1], x[0]))),
    }
    for x in flat:
        p = str(x.get("canonical_path") or "")
        if any(s.casefold() == "root3" for s in p.replace("/", "\\").split("\\") if s):
            summary["has_root3"] = True
            break

    duration_ms = round((time.perf_counter() - t0) * 1000.0, 2)
    summary["duration_ms"] = duration_ms

    lines: List[str] = [
        "memory destination tree (raw session/runtime snapshot, no UI model / no Graph)",
        f"snapshot_source={source} total_nodes={summary['total_nodes']} top_level={summary['top_level_count']}",
        f"has_root3={summary['has_root3']} duration_ms={duration_ms}",
        "",
    ]
    for d in flat:
        sp = "  " * int(d.get("depth") or 0)
        name = ""
        pl = d.get("memory_payload")
        if isinstance(pl, dict):
            name = str(pl.get("name") or pl.get("base_display_label") or "")[:200]
        cp = d.get("canonical_path") or ""
        pk = d.get("row_kind") or "?"
        wst = d.get("workspace_row_state") or "?"
        vs = d.get("verification_state") or ""
        pcan = d.get("parent_canonical_path") or ""
        pr = d.get("proposed")
        ph = d.get("placeholder")
        nch = d.get("stored_children_count")
        vs_part = f" verification={vs}" if str(vs).strip() else ""
        lines.append(
            f"{sp}[{d.get('depth', 0)}] {name!s} | canonical={cp!s} | row_kind={pk!s} | "
            f"ws={wst!s}{vs_part} | p={pcan!s} | proposed={pr!s} placeholder={ph!s} | stored_children={nch!s}"
        )

    payload: Dict[str, Any] = {
        "summary": summary,
        "tree": tree_nested,
        "rows_flat_depth_first": flat,
    }
    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False, default=str)
    log_info(
        "debug_memory_destination_tree_exported",
        txt_path=str(txt_path)[:500],
        json_path=str(json_path)[:500],
        total_nodes=int(len(flat)),
        duration_ms=float(duration_ms),
        snapshot_source=str(source)[:200],
    )
    return txt_path, json_path, summary
