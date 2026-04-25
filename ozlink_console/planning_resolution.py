"""Canonical planning precedence for execution and manifest (shared with MainWindow logic).

Precedence (same as planning/UI):
1. Direct exact move (source path matches a planned move row exactly)
2. Leaf exclusion (path is in plan_leaf_exclusions)
3. Inherited ancestor move (nearest enclosing folder move)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal

MoveDict = dict

CanonicalizeMoveSource = Callable[[MoveDict], str]
PathIsDescendant = Callable[[str, str], bool]


def normalize_canonical_compare_key(path: str) -> str:
    """Lowercase backslash-normalized key for set lookups (Windows-friendly)."""
    return str(path or "").strip().replace("/", "\\").rstrip("\\").lower()


def path_is_descendant_canonical_paths(child: str, parent: str) -> bool:
    """True when child is strictly under parent (both canonical projection-style paths)."""
    c = normalize_canonical_compare_key(child)
    p = normalize_canonical_compare_key(parent)
    if not c or not p or c == p:
        return False
    return c.startswith(p + "\\")


def find_exact_planned_move_for_source_path(
    canonical_source_path: str,
    planned_moves: list[MoveDict],
    canonicalize_move_source: CanonicalizeMoveSource,
) -> MoveDict | None:
    if not canonical_source_path:
        return None
    for move in planned_moves or []:
        move_source_path = canonicalize_move_source(move)
        if move_source_path and move_source_path == canonical_source_path:
            return move
    return None


def find_inherited_planned_move_for_source_path_raw(
    canonical_source_path: str,
    planned_moves: list[MoveDict],
    canonicalize_move_source: CanonicalizeMoveSource,
    path_is_descendant: PathIsDescendant,
) -> MoveDict | None:
    """Nearest ancestor folder move for a source path, ignoring leaf exclusions."""
    if not canonical_source_path:
        return None
    inherited_move: MoveDict | None = None
    inherited_path_length = -1
    for move in planned_moves or []:
        move_source_path = canonicalize_move_source(move)
        if not move_source_path or move_source_path == canonical_source_path:
            continue
        if path_is_descendant(canonical_source_path, move_source_path):
            if len(move_source_path) > inherited_path_length:
                inherited_move = move
                inherited_path_length = len(move_source_path)
    return inherited_move


def _exclusion_set_normalized(plan_leaf_exclusions: set[str] | frozenset[str] | None) -> set[str]:
    return {normalize_canonical_compare_key(x) for x in (plan_leaf_exclusions or set()) if str(x).strip()}


@dataclass(frozen=True)
class EffectivePlanningResolution:
    kind: Literal["direct", "excluded", "inherited", "none"]
    direct_move: MoveDict | None = None
    inherited_move: MoveDict | None = None


def resolve_effective_planning(
    canonical_source_path: str,
    planned_moves: list[MoveDict],
    plan_leaf_exclusions: set[str] | frozenset[str] | None,
    *,
    canonicalize_move_source: CanonicalizeMoveSource,
    path_is_descendant: PathIsDescendant,
) -> EffectivePlanningResolution:
    """
    Apply direct > exclusion > inherited for one canonical source path.

    ``canonical_source_path`` must use the same canonicalization as planning (e.g. MainWindow._canonical_source_projection_path).
    """
    if not canonical_source_path:
        return EffectivePlanningResolution("none", None, None)

    direct = find_exact_planned_move_for_source_path(
        canonical_source_path, planned_moves, canonicalize_move_source
    )
    if direct is not None:
        return EffectivePlanningResolution("direct", direct, None)

    excl = _exclusion_set_normalized(plan_leaf_exclusions)
    if normalize_canonical_compare_key(canonical_source_path) in excl:
        return EffectivePlanningResolution("excluded", None, None)

    inherited = find_inherited_planned_move_for_source_path_raw(
        canonical_source_path, planned_moves, canonicalize_move_source, path_is_descendant
    )
    if inherited is not None:
        return EffectivePlanningResolution("inherited", None, inherited)
    return EffectivePlanningResolution("none", None, None)


def local_absolute_path_matches_canonical_projection(absolute_path: str, canonical_exclusion: str) -> bool:
    """
    Best-effort match between a local absolute path and a library-relative canonical key.

    Used when execution has full paths but plan_leaf_exclusions uses projection paths.
    """
    abs_n = normalize_canonical_compare_key(absolute_path)
    exc_n = normalize_canonical_compare_key(canonical_exclusion)
    if not abs_n or not exc_n:
        return False
    if abs_n == exc_n:
        return True
    return abs_n.endswith("\\" + exc_n)
