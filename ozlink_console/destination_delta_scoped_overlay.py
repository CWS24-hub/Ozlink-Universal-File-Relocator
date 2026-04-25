"""Compute branch-scoped overlay path sets for Graph delta success (planning overlay bind scope only)."""

from __future__ import annotations

from typing import Callable, Iterable


def _canonical_segments(path: str, normalize: Callable[[str], str]) -> list[str]:
    p = normalize(str(path or ""))
    if not p:
        return []
    return [s for s in p.replace("/", "\\").split("\\") if s]


def ancestor_paths_for_canonical(path: str, normalize: Callable[[str], str]) -> list[str]:
    """Return path and every strict parent prefix (library-relative canonical)."""
    segs = _canonical_segments(path, normalize)
    if not segs:
        return []
    out: list[str] = []
    for i in range(1, len(segs) + 1):
        out.append("\\".join(segs[:i]))
    return out


def path_on_delta_branch(memory_path: str, delta_canonical: str) -> bool:
    """True when memory_path is the delta folder or a strict descendant (same touched branch only)."""
    m1 = str(memory_path or "").replace("/", "\\").strip().casefold()
    d1 = str(delta_canonical or "").replace("/", "\\").strip().casefold()
    if not m1 or not d1:
        return False
    if m1 == d1:
        return True
    return m1.startswith(d1 + "\\")


def build_delta_overlay_scope_paths(
    *,
    live_meta_by_key: dict[str, dict],
    proposed_paths: Iterable[str],
    planned_paths: Iterable[str],
    allocation_paths: Iterable[str],
    normalize: Callable[[str], str],
) -> set[str]:
    """
    Build canonical path set for overlay bind scope: delta anchors + ancestors +
    any proposed/planned/allocation path on the same branch as an anchor.
    """
    seeds: set[str] = set()
    for meta in live_meta_by_key.values():
        if not isinstance(meta, dict):
            continue
        p = str(meta.get("path") or "").strip()
        if not p:
            continue
        c = normalize(p) if normalize else p.replace("/", "\\")
        for ax in ancestor_paths_for_canonical(c, normalize):
            cc = normalize(ax) if normalize else ax
            if cc:
                seeds.add(cc)

    scope: set[str] = set(seeds)
    delta_roots: list[str] = []
    for meta in live_meta_by_key.values():
        if not isinstance(meta, dict):
            continue
        p = str(meta.get("path") or "").strip()
        if not p:
            continue
        delta_roots.append(str(normalize(p) if normalize else p.replace("/", "\\")).strip())

    memory_lists = (proposed_paths, planned_paths, allocation_paths)
    for lst in memory_lists:
        for raw in lst:
            m = normalize(str(raw or "").strip()) if normalize else str(raw or "").strip()
            if not m:
                continue
            for d in delta_roots:
                if path_on_delta_branch(m, d):
                    scope.add(m)
                    for ax in ancestor_paths_for_canonical(m, normalize):
                        cc = normalize(ax) if normalize else ax
                        if cc:
                            scope.add(cc)
                    break

    return {x for x in scope if str(x).strip()}
