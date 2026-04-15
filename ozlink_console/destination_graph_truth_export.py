"""
Forensic export: app destination tree vs live Microsoft Graph enumeration.

Export formats (JSONL, one JSON object per line):
  - App audit: keys documented in :func:`collect_app_destination_audit_rows`.
  - Live Graph: keys documented in :func:`live_graph_rows_to_export_records`.
  - Comparison: keys documented in :func:`build_graph_truth_comparison_records`.

CLI: ``python scripts/graph_truth_compare.py --app X.jsonl --live Y.jsonl --out Z.jsonl``
"""

from __future__ import annotations

import csv
import json
import uuid
from collections import defaultdict
from typing import Any, Callable, Protocol

from PySide6.QtCore import QModelIndex, QThread, Signal
from PySide6.QtWidgets import QAbstractItemView, QTreeView

from ozlink_console.logger import log_warn
from ozlink_console.sharepoint_destination_overlay_attach import (
    destination_payload_is_live_graph_row,
    destination_payload_is_planned_workspace_row,
)


class _GraphTruthHost(Protocol):
    destination_planning_model: Any
    destination_tree_widget: QTreeView
    normalize_memory_path: Callable[..., str]
    _canonical_planned_memory_path_for_graph_match: Callable[..., str]
    _destination_model_index_user_role_dict: Callable[..., dict]
    _destination_row_raw_path_for_path_lookup_match: Callable[..., str]
    _planning_browse_mode: Callable[[str], str]


def _norm_key(path: str) -> str:
    return str(path or "").strip().casefold()


def path_matches_canonical_prefix(canonical_path: str, prefix_canonical: str) -> bool:
    """True if ``canonical_path`` is exactly ``prefix_canonical`` or a descendant under it."""
    c = _norm_key(canonical_path)
    p = _norm_key(prefix_canonical)
    if not p:
        return True
    if c == p:
        return True
    return c.startswith(p + "\\")


def graph_item_path_to_raw_windows_path(item_path: str) -> str:
    """Graph ``item_path`` uses '/'; planning uses '\\'."""
    s = str(item_path or "").strip().replace("/", "\\")
    while s.startswith("\\"):
        s = s[1:]
    return s


def collect_app_destination_audit_rows(host: _GraphTruthHost) -> list[dict[str, Any]]:
    """Walk the destination planning model; one record per non-placeholder row."""
    model = getattr(host, "destination_planning_model", None)
    tree = getattr(host, "destination_tree_widget", None)
    if model is None:
        return []

    def _visible(ix: QModelIndex) -> bool:
        if tree is None or not isinstance(tree, QAbstractItemView):
            return True
        if not ix.isValid():
            return True
        cur = ix
        while cur.isValid():
            par = cur.parent()
            if par.isValid() and not tree.isExpanded(par):
                return False
            cur = par
        try:
            if tree.isRowHidden(ix.row(), ix.parent()):
                return False
        except Exception:
            pass
        return True

    def _depth(ix: QModelIndex) -> int:
        d = 0
        cur = ix
        while cur.isValid():
            d += 1
            cur = cur.parent()
        return max(0, d - 1)

    raw_rows: list[dict[str, Any]] = []
    try:
        for ix in model.iter_depth_first():
            if not ix.isValid():
                continue
            pl = host._destination_model_index_user_role_dict(ix)
            if not isinstance(pl, dict) or pl.get("placeholder"):
                continue
            raw = host._destination_row_raw_path_for_path_lookup_match(pl)
            canon = str(host._canonical_planned_memory_path_for_graph_match(str(raw).strip()) or "").strip()
            name = str(pl.get("name") or pl.get("real_name") or "")[:500]
            parent_ix = ix.parent()
            parent_pl = (
                host._destination_model_index_user_role_dict(parent_ix) if parent_ix.isValid() else {}
            )
            parent_gid = ""
            if isinstance(parent_pl, dict):
                parent_gid = str(parent_pl.get("id") or parent_pl.get("graph_item_id") or "").strip()
            graph_id = str(pl.get("id") or pl.get("graph_item_id") or "").strip()
            col0 = ix.siblingAtColumn(0) if ix.column() != 0 else ix
            child_count = 0
            child_visible = 0
            try:
                for r in range(model.rowCount(col0)):
                    cix = model.index(r, 0, col0)
                    if not cix.isValid():
                        continue
                    child_count += 1
                    if _visible(cix):
                        child_visible += 1
            except Exception:
                pass

            raw_rows.append(
                {
                    "canonical_path": canon,
                    "name": name,
                    "depth": _depth(ix),
                    "is_folder": bool(pl.get("is_folder")),
                    "verification_state": str(pl.get("verification_state") or ""),
                    "row_kind": str(pl.get("row_kind") or ""),
                    "graph_item_id": graph_id,
                    "graph_parent_id": parent_gid,
                    "workspace_planned_row": bool(pl.get("workspace_planned_row")),
                    "destination_overlay_kind": str(pl.get("destination_overlay_kind") or ""),
                    "node_origin": str(pl.get("node_origin") or ""),
                    "projected": bool(pl.get("projected")),
                    "visible_in_tree": _visible(ix),
                    "child_count_visible": int(child_visible),
                    "child_count_model": int(child_count),
                    "_is_live_graph_predicate": destination_payload_is_live_graph_row(pl),
                    "_is_planned_workspace_predicate": destination_payload_is_planned_workspace_row(pl),
                }
            )
    except Exception:
        return raw_rows

    by_path: dict[str, list[int]] = defaultdict(list)
    for i, row in enumerate(raw_rows):
        by_path[_norm_key(row.get("canonical_path", "") or "")].append(i)

    group_ids: dict[str, str] = {}
    for key, idxs in by_path.items():
        if not key:
            continue
        gid = str(uuid.uuid4())
        if len(idxs) > 1:
            for _ in idxs:
                group_ids[key] = gid

    out: list[dict[str, Any]] = []
    for row in raw_rows:
        key = _norm_key(row.get("canonical_path", "") or "")
        dup_n = len(by_path.get(key, []))
        dup_gid = group_ids.get(key, "") if dup_n > 1 else ""
        live_at_path = any(
            raw_rows[j].get("_is_live_graph_predicate") for j in by_path.get(key, [])
        )
        equiv_id = ""
        if live_at_path:
            for j in by_path.get(key, []):
                rj = raw_rows[j]
                if rj.get("_is_live_graph_predicate"):
                    equiv_id = str(rj.get("graph_item_id") or "")
                    break
        rec = dict(row)
        rec.pop("_is_live_graph_predicate", None)
        rec.pop("_is_planned_workspace_predicate", None)
        rec["has_graph_backed_equivalent_same_path"] = bool(live_at_path)
        rec["equivalent_graph_item_id"] = equiv_id
        rec["duplicate_path_group_id"] = dup_gid
        rec["duplicate_count_for_path"] = int(dup_n) if key else 0
        out.append(rec)
    return out


def add_live_canonical_paths(
    rows: list[dict[str, Any]],
    canonicalize: Callable[[str], str],
) -> list[dict[str, Any]]:
    """Mutate copies: set ``canonical_path`` from Graph ``item_path``."""
    out = []
    for item in rows:
        d = dict(item)
        raw_p = graph_item_path_to_raw_windows_path(str(d.get("item_path", "") or ""))
        d["canonical_path"] = str(canonicalize(raw_p) or "").strip()
        out.append(d)
    return out


def live_graph_rows_to_export_records(normalized_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flatten normalized Graph items to export rows (before host canonicalize pass)."""
    out: list[dict[str, Any]] = []
    for it in normalized_items:
        if not isinstance(it, dict):
            continue
        out.append(
            {
                "item_path": str(it.get("item_path", "") or ""),
                "name": str(it.get("name", "") or ""),
                "is_folder": bool(it.get("is_folder")),
                "graph_item_id": str(it.get("id", "") or ""),
                "graph_parent_id": str(it.get("parent_item_id", "") or ""),
                "child_count_graph": int(it.get("child_count") or 0),
                "web_url": str(it.get("web_url", "") or ""),
                "drive_id": str(it.get("drive_id", "") or ""),
            }
        )
    return out


def index_live_rows_by_canonical_path(live_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """First row wins per case-insensitive canonical path."""
    m: dict[str, dict[str, Any]] = {}
    for r in live_rows:
        k = _norm_key(str(r.get("canonical_path") or ""))
        if k and k not in m:
            m[k] = r
    return m


def log_graph_truth_violations(
    app_rows: list[dict[str, Any]],
    live_by_path: dict[str, dict[str, Any]],
) -> int:
    """Log ``destination_graph_truth_violation`` for planned visible rows whose path exists live on Graph."""
    n = 0
    for row in app_rows:
        if str(row.get("verification_state") or "").strip() != "planned_only":
            continue
        if not row.get("visible_in_tree", True):
            continue
        path = str(row.get("canonical_path") or "").strip()
        key = _norm_key(path)
        if not key:
            continue
        live = live_by_path.get(key)
        if not live:
            continue
        n += 1
        log_warn(
            "destination_graph_truth_violation",
            canonical_path=path[:500],
            app_verification_state=str(row.get("verification_state") or "")[:80],
            app_row_kind=str(row.get("row_kind") or "")[:80],
            app_graph_item_id=str(row.get("graph_item_id") or "")[:120],
            live_graph_item_id=str(live.get("graph_item_id") or "")[:120],
            live_is_folder=bool(live.get("is_folder")),
            app_is_folder=bool(row.get("is_folder")),
        )
    return n


def build_graph_truth_comparison_records(
    app_rows: list[dict[str, Any]],
    live_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Join by ``canonical_path`` (case-insensitive)."""
    apps_by: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in app_rows:
        k = _norm_key(str(r.get("canonical_path") or ""))
        if k:
            apps_by[k].append(r)

    live_by: dict[str, dict[str, Any]] = {}
    dup_live: dict[str, int] = defaultdict(int)
    for r in live_rows:
        k = _norm_key(str(r.get("canonical_path") or ""))
        if not k:
            continue
        dup_live[k] += 1
        if k not in live_by:
            live_by[k] = r

    all_keys = sorted(set(apps_by.keys()) | set(live_by.keys()) | set(dup_live.keys()))
    records: list[dict[str, Any]] = []

    for k in all_keys:
        apps = apps_by.get(k, [])
        live = live_by.get(k)
        classes: list[str] = []

        if len(apps) > 1:
            classes.append("duplicate_same_path_in_app")

        app_planned = [
            a
            for a in apps
            if str(a.get("verification_state") or "").strip() == "planned_only"
            and a.get("visible_in_tree", True)
        ]

        if live and dup_live.get(k, 0) > 1:
            classes.append("duplicate_same_path_in_live_export")

        if live and not apps:
            classes.append("graph_exists_but_missing_in_app")
        elif live and apps:
            live_id = str(live.get("graph_item_id") or "").strip()
            app_ids = [str(a.get("graph_item_id") or "").strip() for a in apps]
            app_ids_n = [x for x in app_ids if x]
            match_id = bool(live_id and live_id in app_ids_n)
            if match_id:
                classes.append("graph_and_app_live_match")
            if match_id and app_planned:
                classes.append("graph_live_and_planned_duplicate_same_path")
            elif live_id and not match_id and app_planned:
                classes.append("graph_exists_but_app_shows_planned")
            elif live_id and not match_id and app_ids_n:
                classes.append("app_shows_graph_id_mismatch_vs_live_export")

        if apps and not live:
            if any(str(a.get("graph_item_id") or "").strip() for a in apps):
                classes.append("app_shows_graph_but_not_in_graph_export")

        if live and apps:
            for a in apps:
                if bool(a.get("is_folder")) != bool(live.get("is_folder")):
                    classes.append("type_mismatch")
                    break

        classes = list(dict.fromkeys(classes))

        graph_planned_detail = {}
        if "graph_exists_but_app_shows_planned" in classes and app_planned:
            ap0 = app_planned[0]
            graph_planned_detail = {
                "app_verification_state": ap0.get("verification_state"),
                "app_row_kind": ap0.get("row_kind"),
                "app_graph_item_id": ap0.get("graph_item_id"),
                "live_graph_item_id": live.get("graph_item_id") if live else "",
            }

        display_path = ""
        if apps:
            display_path = str(apps[0].get("canonical_path") or "")
        elif live:
            display_path = str(live.get("canonical_path") or "")

        records.append(
            {
                "canonical_path": display_path,
                "path_key": k,
                "classifications": classes,
                "graph_exists_but_app_shows_planned_detail": graph_planned_detail,
                "live_row": live,
                "app_rows": apps,
            }
        )

    return records


def write_jsonl(path: str, rows: list[dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_csv(path: str, rows: list[dict[str, Any]], flatten: bool = False) -> None:
    if not rows:
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write("")
        return
    if not flatten:
        fieldnames = sorted({k for r in rows for k in r.keys()})
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in fieldnames})
        return
    # Flatten comparison nested dicts — minimal
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write("path_key,classifications,live_id,app_count\n")
        for r in rows:
            f.write(
                f'{r.get("path_key","")!s},"{",".join(r.get("classifications") or [])}",'
                f'{str((r.get("live_row") or {}).get("graph_item_id",""))!s},{len(r.get("app_rows") or [])}\n'
            )


class GraphLiveEnumerateWorker(QThread):
    """Background Graph walk; emits normalized item dicts (serializable)."""

    finished_ok = Signal(list)
    failed = Signal(str)

    def __init__(
        self,
        graph: Any,
        drive_id: str,
        *,
        mode: str,
        subtree_item_id: str = "",
        subtree_parent_item_path: str = "",
        context: dict[str, Any] | None = None,
        parent=None,
    ):
        super().__init__(parent)
        self._graph = graph
        self._drive_id = str(drive_id or "").strip()
        self._mode = str(mode or "").strip().lower()
        self._subtree_item_id = str(subtree_item_id or "").strip()
        self._subtree_parent_item_path = str(subtree_parent_item_path or "").strip()
        self._ctx = dict(context or {})

    def run(self):
        try:
            if not self._drive_id or self._graph is None:
                self.failed.emit("missing_drive_or_graph")
                return
            site_id = str(self._ctx.get("site_id") or "")
            site_name = str(self._ctx.get("site_name") or "")
            library_id = str(self._ctx.get("library_id") or "")
            library_name = str(self._ctx.get("library_name") or "")
            tree_role = str(self._ctx.get("tree_role") or "destination")

            if self._mode == "full_library":
                items = self._graph.list_drive_all_items_normalized(
                    self._drive_id,
                    site_id=site_id,
                    site_name=site_name,
                    library_id=library_id or self._drive_id,
                    library_name=library_name,
                    tree_role=tree_role,
                )
            elif self._mode == "subtree":
                items = self._graph.list_drive_subtree_items_normalized(
                    self._drive_id,
                    self._subtree_item_id,
                    site_id=site_id,
                    site_name=site_name,
                    library_id=library_id or self._drive_id,
                    library_name=library_name,
                    tree_role=tree_role,
                    parent_item_path=self._subtree_parent_item_path or "/",
                )
            else:
                self.failed.emit(f"unknown_mode:{self._mode}")
                return
            self.finished_ok.emit(items)
        except Exception as exc:
            self.failed.emit(str(exc)[:2000])


def comparison_summary_text(comparison: list[dict[str, Any]]) -> str:
    counts: dict[str, int] = defaultdict(int)
    for rec in comparison:
        for c in rec.get("classifications") or []:
            counts[c] += 1
    lines = ["Graph truth comparison summary (path-key counts):"]
    for k in sorted(counts.keys()):
        lines.append(f"  {k}: {counts[k]}")
    return "\n".join(lines)
