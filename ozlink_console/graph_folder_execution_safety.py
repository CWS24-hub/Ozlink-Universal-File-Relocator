# -*- coding: utf-8 -*-
"""Detect whether a planned folder move is unsafe for a single Microsoft Graph folder copy.

Graph copies one folder subtree; it cannot honor per-file overrides or plan leaf exclusions
inside that folder. "Dirty" folder moves must not run as Graph folder steps.
"""

from __future__ import annotations

GRAPH_DIRTY_FOLDER_COPY_BLOCKED = (
    "Graph folder copy blocked: this folder step conflicts with per-file destinations or "
    "excluded files under the same folder. Copy this folder using local paths on this PC, "
    "or adjust the plan so Graph does not need a single folder copy for those cases."
)

from collections.abc import Callable
from typing import Any

MoveDict = dict[str, Any]


def _relative_path_under_folder(file_canonical: str, folder_canonical: str) -> str:
    """Path relative to folder root (no leading slash), or "" if not a strict descendant."""
    f = str(file_canonical or "").strip().replace("/", "\\").rstrip("\\")
    p = str(folder_canonical or "").strip().replace("/", "\\").rstrip("\\")
    if not f or not p:
        return ""
    fl = f.lower()
    pl = p.lower()
    if fl == pl or not fl.startswith(pl + "\\"):
        return ""
    return f[len(p) + 1 :]


def _inherit_file_projection_under_folder_move(
    file_move: MoveDict,
    folder_move: MoveDict,
    *,
    canonical_source: Callable[[MoveDict], str],
    allocation_projection_path: Callable[[MoveDict], str],
    normalize_memory_path: Callable[[str], str],
    canonical_destination_projection_path: Callable[[str], str],
) -> str:
    """Destination projection for a file if only the folder_move applied (naive path join)."""
    fs = canonical_source(file_move)
    ps = canonical_source(folder_move)
    rel = _relative_path_under_folder(fs, ps)
    if not rel:
        return ""
    fd = allocation_projection_path(folder_move)
    if not fd:
        return ""
    combined = normalize_memory_path(f"{fd.rstrip('\\')}\\{rel}")
    return canonical_destination_projection_path(combined) or combined


def folder_planned_move_is_graph_dirty(
    folder_move: MoveDict,
    planned_moves: list[MoveDict],
    plan_leaf_exclusions: set[str] | frozenset[str] | None,
    *,
    canonical_source: Callable[[MoveDict], str],
    path_is_descendant: Callable[[str, str], bool],
    allocation_projection_path: Callable[[MoveDict], str],
    normalize_memory_path: Callable[[str], str],
    canonical_destination_projection_path: Callable[[str], str],
    paths_equivalent: Callable[[str, str, str], bool],
) -> bool:
    """
    True if this folder move cannot be executed as a single Graph folder copy without
    contradicting exclusions or per-file destination overrides under the same folder.
    """
    if not isinstance(folder_move, dict):
        return False
    src_folder = folder_move.get("source") or {}
    if not bool(src_folder.get("is_folder", False)):
        return False

    F = canonical_source(folder_move)
    if not F:
        return False

    for e in plan_leaf_exclusions or set():
        es = str(e).strip()
        if not es:
            continue
        if path_is_descendant(es, F):
            return True

    for m2 in planned_moves or []:
        if not isinstance(m2, dict) or m2 is folder_move:
            continue
        s2 = m2.get("source") or {}
        if bool(s2.get("is_folder", False)):
            continue
        p = canonical_source(m2)
        if not p or not path_is_descendant(p, F):
            continue

        inherited = _inherit_file_projection_under_folder_move(
            m2,
            folder_move,
            canonical_source=canonical_source,
            allocation_projection_path=allocation_projection_path,
            normalize_memory_path=normalize_memory_path,
            canonical_destination_projection_path=canonical_destination_projection_path,
        )
        direct = allocation_projection_path(m2)
        if not inherited or not direct:
            continue
        if not paths_equivalent(inherited, direct, "destination"):
            return True

    return False


def compute_graph_unsafe_folder_step_indices(
    planned_moves: list[MoveDict],
    plan_leaf_exclusions: set[str] | frozenset[str] | None,
    *,
    canonical_source: Callable[[MoveDict], str],
    path_is_descendant: Callable[[str, str], bool],
    allocation_projection_path: Callable[[MoveDict], str],
    normalize_memory_path: Callable[[str], str],
    canonical_destination_projection_path: Callable[[str], str],
    paths_equivalent: Callable[[str, str, str], bool],
) -> list[int]:
    """Indices of folder moves that must not run as Graph folder copies (sorted)."""
    indices: list[int] = []
    for i, m in enumerate(planned_moves or []):
        if not isinstance(m, dict):
            continue
        s = m.get("source") or {}
        if not bool(s.get("is_folder", False)):
            continue
        if folder_planned_move_is_graph_dirty(
            m,
            planned_moves,
            plan_leaf_exclusions,
            canonical_source=canonical_source,
            path_is_descendant=path_is_descendant,
            allocation_projection_path=allocation_projection_path,
            normalize_memory_path=normalize_memory_path,
            canonical_destination_projection_path=canonical_destination_projection_path,
            paths_equivalent=paths_equivalent,
        ):
            indices.append(i)
    return sorted(indices)


def runner_should_block_graph_folder_copy_fallback(
    manifest: dict[str, Any],
    step_index: int,
    step: dict[str, Any],
    plan_leaf_exclusions: list[str],
) -> bool:
    """
    Conservative detection when manifest has no embedded graph_unsafe_folder_step_indices
    (older exports). Blocks Graph folder copy if exclusions or file steps under the folder exist.
    """
    if not bool(step.get("is_source_folder", False)):
        return False
    folder_src = str(step.get("source_path", "") or "").strip().replace("/", "\\").rstrip("\\")
    if not folder_src:
        return True
    fl = folder_src.lower()

    for ex in plan_leaf_exclusions or []:
        exs = str(ex).strip().replace("/", "\\").rstrip("\\")
        if not exs:
            continue
        el = exs.lower()
        if el != fl and el.startswith(fl + "\\"):
            return True

    for st in manifest.get("transfer_steps") or []:
        if int(st.get("index", -1)) == int(step_index):
            continue
        if str(st.get("operation", "copy")).lower() != "copy":
            continue
        if bool(st.get("is_source_folder", False)):
            continue
        sp = str(st.get("source_path", "") or "").strip().replace("/", "\\").rstrip("\\")
        if not sp:
            continue
        sl = sp.lower()
        if sl != fl and sl.startswith(fl + "\\"):
            return True

    return False
