"""Destination visible-library anchor: Graph item id is authority; path/name is mutable display state."""

from __future__ import annotations

from enum import Enum
from typing import Any

from ozlink_console.paths import normalize_manifest_path


class AnchorDeltaKind(str, Enum):
    OK = "ok"
    RENAMED = "renamed"
    MISSING = "missing"


def _norm_seg(s: str) -> str:
    return normalize_manifest_path(str(s or "")).strip("\\/")


def rebase_path_under_anchor_prefix(path: str, old_anchor: str, new_anchor: str) -> str:
    """If ``path`` is the anchor or under ``old_anchor``, rewrite to use ``new_anchor`` (display-only rename)."""
    p = normalize_manifest_path(str(path or ""))
    o = _norm_seg(old_anchor)
    n = _norm_seg(new_anchor)
    if not o:
        return path
    if p.casefold() == o.casefold():
        return n
    i = len(o)
    if len(p) > i and p[i] == "\\" and p[:i].casefold() == o.casefold():
        tail = p[i + 1 :]
        return n + ("\\" + tail if tail else "")
    return path


def rebase_allocation_and_proposed_paths_for_anchor_rename(
    allocations: list[dict[str, Any]],
    proposed: list[dict[str, Any]],
    *,
    old_anchor_path: str,
    new_anchor_path: str,
) -> tuple[int, list[dict[str, Any]], list[dict[str, Any]]]:
    """Return updated copies of row dicts and a count of path fields updated."""
    keys_alloc = ("RequestedDestinationPath", "DestinationParentPlannedPath")
    keys_prop = ("DestinationPath", "ParentPath", "DestinationParentPlannedPath")

    def _reb_row(d: dict[str, Any], keys: tuple[str, ...]) -> tuple[dict[str, Any], int]:
        out = dict(d)
        nchg = 0
        for k in keys:
            if k not in out:
                continue
            before = str(out.get(k) or "")
            after = rebase_path_under_anchor_prefix(before, old_anchor_path, new_anchor_path)
            if after != before:
                out[k] = after
                nchg += 1
        return out, nchg

    out_a: list[dict[str, Any]] = []
    out_p: list[dict[str, Any]] = []
    total = 0
    for row in allocations:
        if isinstance(row, dict):
            nr, c = _reb_row(row, keys_alloc)
            total += c
            out_a.append(nr)
        else:
            out_a.append(row)
    for row in proposed:
        if isinstance(row, dict):
            nr, c = _reb_row(row, keys_prop)
            total += c
            out_p.append(nr)
        else:
            out_p.append(row)
    return total, out_a, out_p


def classify_anchor_tree_observation(
    *,
    stored_item_id: str,
    stored_display_path: str,
    live_item_id: str | None,
    live_canonical_path: str | None,
) -> AnchorDeltaKind:
    """Compare persisted anchor Graph identity (item id authority) to a live observation."""
    sid = (stored_item_id or "").strip()
    if not sid:
        return AnchorDeltaKind.MISSING
    lid = (live_item_id or "").strip()
    if not lid:
        return AnchorDeltaKind.MISSING
    if sid.casefold() != lid.casefold():
        # Same display name at a path is not authority — a different Graph item id is unrelated memory.
        return AnchorDeltaKind.MISSING
    old_p = _norm_seg(stored_display_path)
    new_p = _norm_seg(live_canonical_path or "")
    if new_p and old_p and new_p.casefold() != old_p.casefold():
        return AnchorDeltaKind.RENAMED
    return AnchorDeltaKind.OK
