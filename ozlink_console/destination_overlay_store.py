"""
Off-model destination memory for graph-overlay startup mode (OZLINK_DESTINATION_GRAPH_OVERLAY_MODE).

Graph supplies visible structure; overlay records hold planned / cached / proposed metadata that
attaches to existing graph rows when their canonical parent path is visible.
"""

from __future__ import annotations

import os
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from ozlink_console.logger import log_info
from ozlink_console.sharepoint_destination_overlay_attach import (
    destination_payload_is_live_graph_row,
    destination_payload_is_memory_overlay_row_for_reuse,
    destination_payload_is_planned_workspace_row,
)


@dataclass
class DestinationOverlayRecord:
    canonical_path: str
    canonical_parent_path: str
    leaf_name: str
    overlay_kind: str
    payload: Dict[str, Any]
    attached: bool = False
    waiting_for_graph_parent: bool = True


@dataclass
class DestinationOverlayStore:
    by_path: Dict[str, DestinationOverlayRecord] = field(default_factory=dict)
    pending_by_parent: Dict[str, List[DestinationOverlayRecord]] = field(default_factory=lambda: defaultdict(list))

    def _path_key(self, path: str) -> str:
        return str(path or "").strip().casefold()

    def requeue_under_parent(self, rec: DestinationOverlayRecord) -> None:
        rec.waiting_for_graph_parent = True
        pk = self._path_key(rec.canonical_parent_path)
        self.pending_by_parent[pk].append(rec)

    def take_pending_for_parent(self, parent_canonical_path: str) -> List[DestinationOverlayRecord]:
        pk = self._path_key(parent_canonical_path)
        out = list(self.pending_by_parent.get(pk) or [])
        if out:
            self.pending_by_parent[pk] = []
        return out

    def has_overlay_path_prefix(self, prefix: str) -> bool:
        pcf = self._path_key(prefix)
        if not pcf:
            return bool(self.by_path)
        for k in self.by_path.keys():
            if k == pcf or k.startswith(pcf + "\\") or pcf.startswith(k + "\\"):
                return True
        for pk in self.pending_by_parent.keys():
            if pk and (pk == pcf or pk.startswith(pcf + "\\") or pcf.startswith(pk + "\\")):
                return True
        return False

    def register_record(self, rec: DestinationOverlayRecord) -> None:
        pk_path = self._path_key(rec.canonical_path)
        if not pk_path:
            return
        if pk_path in self.by_path:
            return
        self.by_path[pk_path] = rec
        self.requeue_under_parent(rec)


def _parent_dir(normalized: str) -> str:
    p = (normalized or "").replace("/", "\\").strip()
    if not p:
        return ""
    d, _f = os.path.split(p.rstrip("\\"))
    d = d.strip()
    return d


def _leaf_name_of(normalized: str) -> str:
    p = (normalized or "").replace("/", "\\").strip()
    if not p:
        return ""
    _d, f = os.path.split(p.rstrip("\\"))
    return f or p


def _infer_overlay_kind(pl: dict[str, Any], *, is_proposed: bool) -> str:
    if not isinstance(pl, dict):
        return "planned_folder"
    if is_proposed or bool(pl.get("proposed")):
        return "proposed_folder" if pl.get("is_folder", True) else "proposed_folder"
    rk = str(pl.get("row_kind") or "").lower()
    st = str(pl.get("Status") or pl.get("status") or "").lower()
    if "recover" in st or str(pl.get("row_kind") or "") == "recovered":
        return "recovered"
    if "alloc" in st or "allocated" in f"{pl.get('base_display_label', '')!s} ".lower():
        if not pl.get("is_folder", True) or "file" in rk:
            return "planned_file"
        return "allocated_folder"
    if "planned" in rk and "file" in rk:
        return "planned_file"
    if "planned" in rk or destination_payload_is_planned_workspace_row(pl):
        if pl.get("is_folder", True) is False:
            return "planned_file"
        return "planned_folder"
    return "planned_folder"


def _walk_snapshot_subtree(
    node: dict,
    parent_norm: str,
    normalize: Callable[[str], str],
) -> List[DestinationOverlayRecord]:
    out: list[DestinationOverlayRecord] = []
    if not isinstance(node, dict):
        return out
    d = node.get("data")
    this_path = str(parent_norm or "")
    if isinstance(d, dict) and not d.get("placeholder"):
        rec = _payload_from_snapshot_node(d, parent_norm=parent_norm, normalize=normalize)
        if rec is not None:
            out.append(rec)
        raw = str(d.get("item_path") or d.get("destination_path") or d.get("display_path") or "").strip()
        if raw:
            this_path = normalize(raw)
    for ch in list(node.get("children") or []):
        if isinstance(ch, dict):
            out.extend(_walk_snapshot_subtree(ch, this_path, normalize))
    return out


def _payload_from_snapshot_node(
    data: dict[str, Any], *, parent_norm: str, normalize: Callable[[str], str]
) -> Optional[DestinationOverlayRecord]:
    if not isinstance(data, dict) or data.get("placeholder"):
        return None
    if destination_payload_is_live_graph_row(data):
        return None
    if not destination_payload_is_memory_overlay_row_for_reuse(data):
        return None
    raw = (
        str(
            data.get("item_path")
            or data.get("destination_path")
            or data.get("display_path")
            or data.get("semantic_path")
            or ""
        )
        .strip()
    )
    npath = normalize(str(raw or "")).strip()
    if not npath:
        return None
    pnorm = (normalize(parent_norm).strip() if parent_norm else "") or _parent_dir(npath)
    return DestinationOverlayRecord(
        canonical_path=npath,
        canonical_parent_path=pnorm,
        leaf_name=_leaf_name_of(npath) or str(data.get("name") or "").strip(),
        overlay_kind=_infer_overlay_kind(data, is_proposed=bool(data.get("proposed"))),
        payload=dict(data),
    )


def build_destination_overlay_store_from_session_snapshots(
    snapshots: list, *, normalize: Callable[[str], str]
) -> DestinationOverlayStore:
    store = DestinationOverlayStore()
    for snap in list(snapshots or []):
        if not isinstance(snap, dict):
            continue
        for rec in _walk_snapshot_subtree(snap, "", normalize):
            store.register_record(rec)
    return store


def merge_proposed_folders_into_overlay_store(
    store: DestinationOverlayStore, proposed_folders: list | None, *, normalize: Callable[[str], str]
) -> int:
    n = 0
    for pf in list(proposed_folders or []):
        dest = str(getattr(pf, "DestinationPath", None) or getattr(pf, "destination_path", "") or "").strip()
        parent = str(getattr(pf, "ParentPath", None) or getattr(pf, "parent_path", "") or "").strip()
        if not dest:
            continue
        norm = normalize(dest)
        leaf = str(getattr(pf, "FolderName", None) or _leaf_name_of(norm) or "").strip()
        pl: dict[str, Any] = {
            "is_folder": True,
            "name": leaf,
            "proposed": True,
            "verification_state": "planned_only",
            "row_kind": "proposed_folder",
        }
        rec = DestinationOverlayRecord(
            canonical_path=norm,
            canonical_parent_path=normalize(parent) if parent else _parent_dir(norm),
            leaf_name=leaf,
            overlay_kind="proposed_folder",
            payload=pl,
        )
        store.register_record(rec)
        n += 1
    return n


def merge_planned_allocations_into_overlay_store(
    store: DestinationOverlayStore, planned_rows: list | None, *, normalize: Callable[[str], str]
) -> int:
    n = 0
    for row in list(planned_rows or []):
        dest = ""
        if isinstance(row, dict):
            dest = str(row.get("destination_path") or row.get("DestinationPath") or row.get("RequestedDestinationPath") or "")
        if hasattr(row, "RequestedDestinationPath"):
            dest = str(getattr(row, "RequestedDestinationPath", "") or dest or "")
        if not str(dest or "").strip():
            continue
        norm = normalize(str(dest).strip())
        pl: dict[str, Any] = {"is_folder": True, "row_kind": "allocated_folder", "source": "planning_state"}
        if isinstance(row, dict):
            pl.update({k: v for k, v in row.items() if isinstance(v, (str, int, float, bool, type(None))) and len(str(k)) < 80})
        if hasattr(row, "to_dict") and callable(row.to_dict):
            try:
                d = row.to_dict()
                if isinstance(d, dict):
                    pl["planning"] = d
            except Exception:
                pass
        is_folder = True
        if "file" in str((row if isinstance(row, dict) else {}).get("source_type", "")).lower():
            is_folder = "folder" in str(
                (row if isinstance(row, dict) else {}).get("source_type", "")
            ).lower() or is_folder
        if isinstance(row, dict) and row.get("is_folder") is not None:
            is_folder = bool(row.get("is_folder"))
        pl["is_folder"] = is_folder
        kind = "planned_file" if not is_folder else "allocated_folder"
        rec = DestinationOverlayRecord(
            canonical_path=norm,
            canonical_parent_path=_parent_dir(norm),
            leaf_name=_leaf_name_of(norm),
            overlay_kind=kind,
            payload=pl,
        )
        store.register_record(rec)
        n += 1
    return n


def load_full_destination_overlay_store(
    *,
    session_snapshots: list | None,
    proposed_folders: list | None,
    planned_moves: list | None,
    normalize: Callable[[str], str],
) -> DestinationOverlayStore:
    s = build_destination_overlay_store_from_session_snapshots(
        list(session_snapshots or []), normalize=normalize
    )
    n_pf = merge_proposed_folders_into_overlay_store(s, proposed_folders, normalize=normalize)
    n_pl = merge_planned_allocations_into_overlay_store(s, planned_moves, normalize=normalize)
    n = int(len(s.by_path))
    log_info(
        "destination_overlay_store_loaded",
        count=int(n),
        proposed_merged=int(n_pf),
        planned_merged=int(n_pl),
    )
    return s
