# -*- coding: utf-8 -*-
"""Expand planned moves into Graph-eligible copy steps (pure; not wired to runner).

Clean folder moves become a single folder step. Dirty folder moves are expanded into
clean subfolder folder steps and file steps for overrides and uncovered inherited files,
omitting excluded leaf paths. Under a dirty parent, dirty subfolders are expanded first
(deepest first); clean subfolders are emitted shallow-first with at most one Graph folder
copy per ancestor chain (descendant planned folder rows are marked handled, not emitted).
Does not mutate inputs.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from ozlink_console.graph_folder_execution_safety import compute_graph_unsafe_folder_step_indices
from ozlink_console.plan_execution_duplicate_validation import (
    duplicate_collision_report_from_transfer_steps,
    format_execution_duplicate_message,
)

MoveDict = dict[str, Any]


@dataclass(frozen=True)
class ExpandedGraphStep:
    """One Graph copy step derived from a planned move index."""

    kind: Literal["folder", "file"]
    planned_move_index: int
    source_path: str
    destination_path: str
    destination_name: str
    source_name: str
    is_source_folder: bool


def _is_folder_move(m: MoveDict) -> bool:
    if not isinstance(m, dict):
        return False
    src = m.get("source") or {}
    return bool(src.get("is_folder", False))


def _destination_leaf(m: MoveDict) -> str:
    src = m.get("source") or {}
    dest = m.get("destination") or {}
    return str(
        m.get("target_name", "")
        or m.get("destination_name", "")
        or m.get("source_name", "")
        or dest.get("name", "")
        or src.get("name", "")
        or ""
    ).strip()


def _expanded_from_move(index: int, m: MoveDict) -> ExpandedGraphStep:
    src = m.get("source") or {}
    is_folder = bool(src.get("is_folder", False))
    leaf = _destination_leaf(m)
    return ExpandedGraphStep(
        kind="folder" if is_folder else "file",
        planned_move_index=index,
        source_path=str(m.get("source_path", "") or ""),
        destination_path=str(m.get("destination_path", "") or ""),
        destination_name=leaf,
        source_name=str(m.get("source_name", "") or src.get("name", "") or ""),
        is_source_folder=is_folder,
    )


def _path_depth(path: str) -> int:
    p = str(path or "").strip().replace("/", "\\").rstrip("\\")
    if not p:
        return 0
    return len([x for x in p.split("\\") if x])


def _mark_indices_under_emitted_clean_folder(
    planned_moves: list[MoveDict],
    root_idx: int,
    root_src: str,
    handled: set[int],
    *,
    canonical_source: Callable[[MoveDict], str],
    path_is_descendant: Callable[[str, str], bool],
) -> None:
    """Mark root_idx and every planned move whose source is strictly under root_src."""
    handled.add(root_idx)
    for k in range(len(planned_moves)):
        if k == root_idx or k in handled:
            continue
        ck = canonical_source(planned_moves[k])
        if ck and path_is_descendant(ck, root_src):
            handled.add(k)


def _outermost_dirty_folder_roots(
    dirty_folders: set[int],
    planned_moves: list[MoveDict],
    *,
    canonical_source: Callable[[MoveDict], str],
    path_is_descendant: Callable[[str, str], bool],
) -> list[int]:
    """Dirty folder indices that are not strictly under another dirty folder's source."""
    roots: list[int] = []
    for i in dirty_folders:
        if i < 0 or i >= len(planned_moves):
            continue
        Fi = canonical_source(planned_moves[i])
        if not Fi:
            continue
        is_inner = False
        for j in dirty_folders:
            if j == i or j < 0 or j >= len(planned_moves):
                continue
            Fj = canonical_source(planned_moves[j])
            if not Fj:
                continue
            if path_is_descendant(Fi, Fj):
                is_inner = True
                break
        if not is_inner:
            roots.append(i)
    return sorted(roots)


def _expand_dirty_folder(
    i: int,
    planned_moves: list[MoveDict],
    plan_leaf_exclusions: set[str] | frozenset[str] | None,
    dirty_folders: set[int],
    handled: set[int],
    emitted_clean_folder_roots: list[str],
    *,
    canonical_source: Callable[[MoveDict], str],
    path_is_descendant: Callable[[str, str], bool],
    paths_equivalent: Callable[[str, str, str], bool],
) -> list[ExpandedGraphStep]:
    """Expand one dirty folder move; marks handled indices for this subtree."""
    out: list[ExpandedGraphStep] = []
    F = planned_moves[i]
    F_src = canonical_source(F)
    if not F_src:
        handled.add(i)
        return out

    handled.add(i)

    subfolders = [
        j
        for j in range(len(planned_moves))
        if j != i
        and j not in handled
        and _is_folder_move(planned_moves[j])
        and path_is_descendant(canonical_source(planned_moves[j]), F_src)
    ]

    # Pass 1: expand dirty subfolders deepest-first so nested dirty runs before a shallow clean emit.
    dirty_subs = [j for j in subfolders if j in dirty_folders]
    dirty_subs.sort(
        key=lambda j: (-_path_depth(canonical_source(planned_moves[j])), canonical_source(planned_moves[j]).lower())
    )
    for j in dirty_subs:
        if j in handled:
            continue
        out.extend(
            _expand_dirty_folder(
                j,
                planned_moves,
                plan_leaf_exclusions,
                dirty_folders,
                handled,
                emitted_clean_folder_roots,
                canonical_source=canonical_source,
                path_is_descendant=path_is_descendant,
                paths_equivalent=paths_equivalent,
            )
        )

    # Pass 2: emit clean subfolders shallow-first; one Graph folder copy covers all descendant planned rows.
    clean_subs = [j for j in subfolders if j not in dirty_folders]
    clean_subs.sort(
        key=lambda j: (_path_depth(canonical_source(planned_moves[j])), canonical_source(planned_moves[j]).lower())
    )
    for j in clean_subs:
        if j in handled:
            continue
        fj_src = canonical_source(planned_moves[j])
        if not fj_src:
            handled.add(j)
            continue
        if emitted_clean_folder_roots and any(
            path_is_descendant(fj_src, r) for r in emitted_clean_folder_roots
        ):
            _mark_indices_under_emitted_clean_folder(
                planned_moves,
                j,
                fj_src,
                handled,
                canonical_source=canonical_source,
                path_is_descendant=path_is_descendant,
            )
            continue
        out.append(_expanded_from_move(j, planned_moves[j]))
        emitted_clean_folder_roots.append(fj_src)
        _mark_indices_under_emitted_clean_folder(
            planned_moves,
            j,
            fj_src,
            handled,
            canonical_source=canonical_source,
            path_is_descendant=path_is_descendant,
        )

    for k in range(len(planned_moves)):
        if k in handled:
            continue
        if _is_folder_move(planned_moves[k]):
            continue
        fm = planned_moves[k]
        fs = canonical_source(fm)
        if not fs or not path_is_descendant(fs, F_src):
            continue
        if _is_excluded_leaf(fs, plan_leaf_exclusions, paths_equivalent):
            handled.add(k)
            continue
        out.append(_expanded_from_move(k, fm))
        handled.add(k)

    return out


def _is_excluded_leaf(
    file_canonical: str,
    plan_leaf_exclusions: set[str] | frozenset[str] | None,
    paths_equivalent: Callable[[str, str, str], bool],
) -> bool:
    fc = str(file_canonical or "").strip()
    if not fc:
        return False
    for ex in plan_leaf_exclusions or set():
        es = str(ex).strip()
        if not es:
            continue
        if paths_equivalent(fc, es, "source"):
            return True
    return False


def _assert_no_duplicate_steps(steps: list[ExpandedGraphStep]) -> None:
    step_dicts: list[dict[str, Any]] = []
    for st in steps:
        step_dicts.append(
            {
                "operation": "copy",
                "source_path": st.source_path,
                "destination_path": st.destination_path,
                "destination_name": st.destination_name,
                "source_name": st.source_name,
                "is_source_folder": st.is_source_folder,
            }
        )
    ds, dd = duplicate_collision_report_from_transfer_steps(step_dicts)
    if ds or dd:
        raise ValueError(format_execution_duplicate_message(ds, dd))


def _sort_expanded_steps(steps: list[ExpandedGraphStep]) -> list[ExpandedGraphStep]:
    """Folders first (shallow source path before deeper), then files."""

    def sort_key(st: ExpandedGraphStep) -> tuple[int, int, str]:
        depth = _path_depth(st.source_path)
        if st.kind == "folder":
            return (0, depth, st.source_path.lower())
        return (1, depth, st.source_path.lower())

    return sorted(steps, key=sort_key)


def expand_graph_transfer_steps(
    planned_moves: list[MoveDict],
    plan_leaf_exclusions: set[str] | frozenset[str] | None,
    *,
    canonical_source: Callable[[MoveDict], str],
    path_is_descendant: Callable[[str, str], bool],
    allocation_projection_path: Callable[[MoveDict], str],
    normalize_memory_path: Callable[[str], str],
    canonical_destination_projection_path: Callable[[str], str],
    paths_equivalent: Callable[[str, str, str], bool],
) -> list[ExpandedGraphStep]:
    """
    Produce an ordered list of Graph copy steps for the given plan.

    * Clean folder moves appear as a single folder step.
    * Dirty folder moves are replaced by expanded folder/file steps (no single step for F).
    * Excluded leaf paths emit no step; planned rows for excluded files are consumed (omitted).
    * Duplicate source or duplicate file destination keys raise ValueError.

    Ordering: folder steps before file steps; within each group, ascending by source path depth
    then lexicographic source path (deterministic).
    """
    moves = list(planned_moves or [])
    dirty_folders = set(
        compute_graph_unsafe_folder_step_indices(
            moves,
            plan_leaf_exclusions,
            canonical_source=canonical_source,
            path_is_descendant=path_is_descendant,
            allocation_projection_path=allocation_projection_path,
            normalize_memory_path=normalize_memory_path,
            canonical_destination_projection_path=canonical_destination_projection_path,
            paths_equivalent=paths_equivalent,
        )
    )

    handled: set[int] = set()
    emitted_clean_folder_roots: list[str] = []
    result: list[ExpandedGraphStep] = []

    roots = _outermost_dirty_folder_roots(
        dirty_folders,
        moves,
        canonical_source=canonical_source,
        path_is_descendant=path_is_descendant,
    )
    for root_i in roots:
        if root_i in handled:
            continue
        result.extend(
            _expand_dirty_folder(
                root_i,
                moves,
                plan_leaf_exclusions,
                dirty_folders,
                handled,
                emitted_clean_folder_roots,
                canonical_source=canonical_source,
                path_is_descendant=path_is_descendant,
                paths_equivalent=paths_equivalent,
            )
        )

    for i in range(len(moves)):
        if i in handled:
            continue
        m = moves[i]
        if i in dirty_folders:
            continue
        result.append(_expanded_from_move(i, m))
        handled.add(i)

    ordered = _sort_expanded_steps(result)
    _assert_no_duplicate_steps(ordered)
    return ordered
