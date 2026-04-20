"""
Index migrated proposed-folder paths for allocation destination parent fallback.

Paths are normalized (backslashes), lookups are case-insensitive.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ozlink_console.paths import normalize_manifest_path


def _parts(path: str) -> list[str]:
    return [x for x in normalize_manifest_path(str(path or "").strip()).split("\\") if x]


def _join(parts: list[str]) -> str:
    return normalize_manifest_path("\\".join(parts))


def _all_prefix_paths(path: str) -> list[str]:
    """Root3\\A\\B -> [Root3, Root3\\A, Root3\\A\\B]."""
    ps = _parts(path)
    if not ps:
        return []
    out: list[str] = []
    for i in range(1, len(ps) + 1):
        out.append(_join(ps[:i]))
    return out


def align_legacy_top_segment_to_anchor(
    path: str,
    visible_anchor: str,
    *,
    live_top_level_names_cf: frozenset[str] | None = None,
) -> str:
    """
    Map legacy internal ``Root\\`` to the visible library anchor top (e.g. Root3) when they differ.

    When the live Graph library has a real top-level folder named ``Root``, do not remap — the path may
    target that business folder instead of the legacy internal hub segment.
    """
    ps = _parts(path)
    if not ps:
        return normalize_manifest_path(path)
    ap = _parts(visible_anchor)
    if not ap:
        return normalize_manifest_path(path)
    tops = live_top_level_names_cf or frozenset()
    if ps[0].casefold() == "root" and "root" in tops:
        return normalize_manifest_path(path)
    if ps[0].casefold() == "root" and ap[0].casefold() != ps[0].casefold():
        ps[0] = ap[0]
        return _join(ps)
    return normalize_manifest_path(path)


@dataclass(frozen=True)
class PlannedParentLookup:
    """Outcome of matching an allocation destination parent against the planned index."""

    kind: str  # exact | ancestor | missing_descendant | none
    matched_prefix: str = ""


def lookup_planned_parent(parent_path_norm: str, index_cf: set[str]) -> PlannedParentLookup:
    """
    ``index_cf`` holds normalize_manifest_path(v).casefold() for every indexed folder path.
    """
    p = normalize_manifest_path(str(parent_path_norm or "").strip())
    if not p:
        return PlannedParentLookup("none")
    pcf = p.casefold()
    if pcf in index_cf:
        return PlannedParentLookup("exact", p)

    parts = _parts(p)
    if not parts:
        return PlannedParentLookup("none")

    longest_q = ""
    longest_len = 0
    for depth in range(len(parts) - 1, 0, -1):
        cand = _join(parts[:depth])
        cc = cand.casefold()
        if cc in index_cf and len(parts[:depth]) > longest_len:
            longest_q = cand
            longest_len = depth
            break
    if not longest_q:
        return PlannedParentLookup("none")

    if longest_q.casefold() == pcf:
        return PlannedParentLookup("exact", p)

    # Next folder along P after longest_q
    q_parts = _parts(longest_q)
    if len(parts) <= len(q_parts):
        return PlannedParentLookup("ancestor", longest_q)
    next_depth = len(q_parts) + 1
    next_path = _join(parts[:next_depth])
    if next_path.casefold() in index_cf:
        return PlannedParentLookup("ancestor", longest_q)

    return PlannedParentLookup("missing_descendant", longest_q)


def build_planned_parent_path_index(
    migrated_proposed: list[dict[str, Any]],
    *,
    scaffold_row_paths: list[str] | None = None,
    anchor: str = "",
    live_top_level_names_cf: frozenset[str] | None = None,
) -> tuple[set[str], int, int]:
    """
    Build casefold path set for folder paths implied by proposed rows.

    Paths are passed through :func:`align_legacy_top_segment_to_anchor` so legacy ``Root\\\\`` prefixes match
    aligned allocation paths when Graph re-anchor for proposed rows did not complete.

    Returns (index_casefold, scaffold_row_count, indexed_path_count).
    """
    index_cf: set[str] = set()
    scaffold_ct = 0

    def _norm_segment(p: str) -> str:
        p = normalize_manifest_path(str(p or "").strip())
        if not p or not anchor:
            return p
        return align_legacy_top_segment_to_anchor(p, anchor, live_top_level_names_cf=live_top_level_names_cf)

    for r in migrated_proposed or []:
        if not isinstance(r, dict):
            continue
        if bool(r.get("LegacyMigrationPlannedScaffoldOnly")):
            scaffold_ct += 1

        pp = _norm_segment(str(r.get("ParentPath") or "").strip())
        fn = str(r.get("FolderName") or "").strip()
        dp = _norm_segment(str(r.get("DestinationPath") or "").strip())

        paths_to_add: list[str] = []
        if fn and pp:
            paths_to_add.append(normalize_manifest_path(f"{pp}\\{fn}"))
        elif fn and not pp:
            paths_to_add.append(_norm_segment(fn))
        if dp:
            paths_to_add.append(dp)
        if pp:
            paths_to_add.append(pp)

        for raw in paths_to_add:
            raw = _norm_segment(raw) if raw else raw
            for pref in _all_prefix_paths(raw):
                index_cf.add(pref.casefold())

    for extra in scaffold_row_paths or []:
        ex = _norm_segment(str(extra or "").strip())
        if ex:
            for pref in _all_prefix_paths(ex):
                index_cf.add(pref.casefold())

    indexed_path_count = len(index_cf)
    return index_cf, scaffold_ct, indexed_path_count
