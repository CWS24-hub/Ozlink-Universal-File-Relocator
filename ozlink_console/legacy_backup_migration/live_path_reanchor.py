"""
Semantic re-anchoring of legacy destination paths against the live Graph library skeleton.

Uses live folder names from the destination drive (never a hardcoded Root → Root3 map).
Legacy memory is planning overlay only; the Graph skeleton is structural authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ozlink_console.destination_path_bridge import LEGACY_INTERNAL_ROOT_SEGMENT, strip_legacy_internal_root_prefix
from ozlink_console.legacy_backup_migration.types import (
    ANCHOR_CLASS_FOREIGN_ROOT_BLOCKED,
    ANCHOR_CLASS_LIVE_MATCHED,
    ANCHOR_CLASS_REANCHORED_TO_LIVE_GRAPH,
    ANCHOR_CLASS_UNRESOLVED_AMBIGUOUS_ANCHOR,
)
from ozlink_console.logger import log_info
from ozlink_console.paths import normalize_manifest_path

# First-segment names that denote **library/app namespace tokens** (not business folders named Root).
# Do **not** include ``LEGACY_INTERNAL_ROOT_SEGMENT`` (``Root``): after library-container strip, a leading
# ``Root\\`` segment is usually a legacy business hub folder; re-anchor tries ``Root3\\…`` etc. Treating
# ``root`` as a foreign namespace here caused allocation-only ``foreign_root_blocked`` when Graph had
# no match yet—same paths worked for proposed when shallow (e.g. ``Root\\Finance``).
# ``foreign_root_blocked`` from re-anchor applies only to unresolvable **documents**/wrapper-like tokens.
_LEGACY_REANCHOR_FOREIGN_NAMESPACE_MARKERS_CF = frozenset(
    {
        "documents",
        "library documents",
    }
)


@dataclass(frozen=True)
class LiveReanchorOutcome:
    """Result of one destination path normalization attempt."""

    path_manifest: str
    kind: str  # unchanged | live_exists | reanchored | ambiguous | failed | foreign_root_blocked
    before_manifest: str = ""
    live_top_used: str = ""
    ignored_legacy_internal_root_segment: bool = False


def live_destination_top_level_folder_names(graph: Any, destination_drive_id: str) -> list[str]:
    """Collect folder names at the document library root (structural authority)."""
    drive_id = str(destination_drive_id or "").strip()
    if not drive_id:
        return []
    fn = getattr(graph, "list_drive_root_children", None)
    if not callable(fn):
        return []
    try:
        raw = fn(drive_id) or []
    except Exception:
        return []
    names: list[str] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        # Graph returns ``folder: {}`` for folders; do not treat empty dict as falsy.
        if "folder" not in item:
            continue
        nm = str(item.get("name") or "").strip()
        if nm:
            names.append(nm)
    return names


# Result of ``list_drive_root_children`` for the selected destination drive: no API, empty root, or non-empty.
GRAPH_ROOT_NO_LISTING = "no_graph_root_listing"
GRAPH_ROOT_EMPTY = "empty_graph_root"
GRAPH_ROOT_NON_EMPTY = "non_empty_graph_root"


def probe_live_destination_graph_root(
    graph: Any, destination_drive_id: str
) -> tuple[list[str], str]:
    """
    Probe the live Graph document-library root listing.

    Returns ``(folder_names, kind)`` where ``kind`` is one of:
    - ``GRAPH_ROOT_NO_LISTING``: graph missing list API, exception, or no drive id → caller may fall back.
    - ``GRAPH_ROOT_EMPTY``: API succeeded and returned no folder children → no live anchor strip/re-map.
    - ``GRAPH_ROOT_NON_EMPTY``: at least one folder child exists → Graph skeleton applies.
    """
    drive_id = str(destination_drive_id or "").strip()
    if not drive_id or graph is None:
        return [], GRAPH_ROOT_NO_LISTING
    fn = getattr(graph, "list_drive_root_children", None)
    if not callable(fn):
        return [], GRAPH_ROOT_NO_LISTING
    try:
        raw = fn(drive_id) or []
    except Exception:
        return [], GRAPH_ROOT_NO_LISTING
    names: list[str] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        if "folder" not in item:
            continue
        nm = str(item.get("name") or "").strip()
        if nm:
            names.append(nm)
    if not names:
        return [], GRAPH_ROOT_EMPTY
    return names, GRAPH_ROOT_NON_EMPTY


def legacy_anchor_classification_from_reanchor(ra: LiveReanchorOutcome) -> str:
    """Map a live re-anchor outcome to a stable migration anchor classification string."""
    k = (ra.kind or "").strip()
    if k == "foreign_root_blocked":
        return ANCHOR_CLASS_FOREIGN_ROOT_BLOCKED
    if k in ("ambiguous", "failed"):
        return ANCHOR_CLASS_UNRESOLVED_AMBIGUOUS_ANCHOR
    if k == "reanchored":
        return ANCHOR_CLASS_REANCHORED_TO_LIVE_GRAPH
    if k in ("unchanged", "live_exists"):
        return ANCHOR_CLASS_LIVE_MATCHED
    return ANCHOR_CLASS_UNRESOLVED_AMBIGUOUS_ANCHOR


def _graph_get_by_manifest_path(graph: Any, drive_id: str, manifest_path: str) -> dict | None:
    """Resolve graph-relative manifest path (Backslash) using existing Graph helpers."""
    if not getattr(graph, "get_drive_item_by_path", None):
        return None
    rel = str(manifest_path or "").replace("\\", "/").strip("/")
    if not rel:
        return None
    try:
        return graph.get_drive_item_by_path(str(drive_id), rel)
    except Exception:
        return None


def reanchor_manifest_destination_path_against_live_skeleton(
    manifest_path: str,
    *,
    destination_drive_id: str,
    graph: Any,
    live_top_level_names: list[str],
    row_index: int,
    row_kind: str,  # allocation | proposed_dest | proposed_parent
) -> LiveReanchorOutcome:
    """
    Match legacy paths to the live library skeleton by Graph proof only (no hardcoded hub rename).

    - If the path resolves as-is on Graph, keep it.
    - If the first segment is already a live top-level name (e.g. business folder ``Root``), do not
      substitute another anchor — keep the path string for downstream Graph resolution.
    - If the first segment is not live, attempt ``<liveTop>/<all segments except the first>`` for
      each top-level folder name; require a unique resolving path.
    - Single-segment paths that are not live top names are tried as ``<liveTop>/<segment>``.
    """
    before = normalize_manifest_path(str(manifest_path or "").strip())
    if not before:
        return LiveReanchorOutcome(path_manifest="", kind="unchanged")

    drive_id = str(destination_drive_id or "").strip()
    if not drive_id or not graph or not live_top_level_names:
        return LiveReanchorOutcome(path_manifest=before, kind="unchanged")

    get_item: Callable[[str], dict | None] = lambda rel: _graph_get_by_manifest_path(graph, drive_id, rel)
    parts = [p for p in before.split("\\") if p]
    if not parts:
        return LiveReanchorOutcome(path_manifest=before, kind="unchanged")

    live_map_cf: dict[str, str] = {}
    for nm in live_top_level_names:
        cf = nm.casefold()
        if cf not in live_map_cf:
            live_map_cf[cf] = nm

    full_rel = "/".join(parts)
    if get_item(full_rel):
        log_info(
            "legacy_backup_migration_legacy_root_detected",
            row_index=row_index,
            kind=row_kind,
            detail="live_path_resolves_as_is",
            segment=parts[0][:120],
        )
        return LiveReanchorOutcome(
            path_manifest=before,
            kind="live_exists",
            before_manifest=before,
            live_top_used=live_map_cf.get(parts[0].casefold(), parts[0]),
        )

    first_cf = parts[0].casefold()
    if first_cf in live_map_cf:
        log_info(
            "legacy_backup_migration_legacy_root_detected",
            row_index=row_index,
            kind=row_kind,
            detail="first_segment_matches_live_top_not_resolved_yet",
            segment=parts[0][:120],
        )
        return LiveReanchorOutcome(
            path_manifest=before,
            kind="unchanged",
            before_manifest=before,
            live_top_used=live_map_cf[first_cf],
        )

    ignore_root = first_cf == LEGACY_INTERNAL_ROOT_SEGMENT.casefold() and first_cf not in live_map_cf
    if ignore_root:
        log_info(
            "legacy_backup_migration_synthetic_root_stripped",
            row_index=row_index,
            kind=row_kind,
            before_excerpt=before[:220],
            after_excerpt="/".join(parts[1:])[:220] if len(parts) > 1 else "",
        )

    # One segment that is not a live top name — e.g. legacy ``HR`` intended as ``Root3\\HR``.
    if len(parts) == 1:
        singles: list[str] = []
        leaf = parts[0]
        for top in live_top_level_names:
            rel = f"{top}/{leaf}"
            it = get_item(rel)
            if it and isinstance(it, dict):
                singles.append(top)
        if len(singles) == 1:
            top_used = singles[0]
            canon = normalize_manifest_path(f"{top_used}\\{leaf}")
            log_info(
                "legacy_backup_migration_path_reanchored",
                row_index=row_index,
                kind=row_kind,
                before_excerpt=before[:220],
                after_excerpt=canon[:220],
                live_top=top_used[:120],
            )
            return LiveReanchorOutcome(
                path_manifest=canon,
                kind="reanchored",
                before_manifest=before,
                live_top_used=top_used,
                ignored_legacy_internal_root_segment=ignore_root,
            )
        if len(singles) > 1:
            log_info(
                "legacy_backup_migration_path_reanchor_ambiguous",
                row_index=row_index,
                kind=row_kind,
                candidate_count=len(singles),
                before_excerpt=before[:220],
            )
            return LiveReanchorOutcome(path_manifest=before, kind="ambiguous", before_manifest=before)
        log_info(
            "legacy_backup_migration_path_reanchor_failed",
            row_index=row_index,
            kind=row_kind,
            reason="single_segment_no_live_parent",
            before_excerpt=before[:220],
        )
        if first_cf in _LEGACY_REANCHOR_FOREIGN_NAMESPACE_MARKERS_CF:
            log_info(
                "legacy_backup_migration_foreign_root_blocked",
                row_index=row_index,
                kind=row_kind,
                legacy_top_segment=parts[0][:120],
            )
            return LiveReanchorOutcome(path_manifest=before, kind="foreign_root_blocked", before_manifest=before)
        return LiveReanchorOutcome(path_manifest=before, kind="failed", before_manifest=before)

    remainder = parts[1:]
    log_info(
        "legacy_backup_migration_legacy_root_detected",
        row_index=row_index,
        kind=row_kind,
        legacy_top_segment=parts[0][:120],
        live_top_level_count=len(live_top_level_names),
    )

    candidates: list[str] = []
    for top in live_top_level_names:
        cand_parts = [top] + remainder
        rel = "/".join(cand_parts)
        it = get_item(rel)
        if it and isinstance(it, dict):
            candidates.append(top)

    if len(candidates) == 1:
        top_used = candidates[0]
        canon = normalize_manifest_path("\\".join([top_used] + remainder))
        log_info(
            "legacy_backup_migration_path_reanchored",
            row_index=row_index,
            kind=row_kind,
            before_excerpt=before[:220],
            after_excerpt=canon[:220],
            live_top=top_used[:120],
        )
        return LiveReanchorOutcome(
            path_manifest=canon,
            kind="reanchored",
            before_manifest=before,
            live_top_used=top_used,
            ignored_legacy_internal_root_segment=ignore_root,
        )

    if len(candidates) > 1:
        log_info(
            "legacy_backup_migration_path_reanchor_ambiguous",
            row_index=row_index,
            kind=row_kind,
            candidate_count=len(candidates),
            before_excerpt=before[:220],
        )
        return LiveReanchorOutcome(path_manifest=before, kind="ambiguous", before_manifest=before)

    log_info(
        "legacy_backup_migration_path_reanchor_failed",
        row_index=row_index,
        kind=row_kind,
        reason="no_live_suffix_match",
        before_excerpt=before[:220],
    )

    if first_cf in _LEGACY_REANCHOR_FOREIGN_NAMESPACE_MARKERS_CF:
        log_info(
            "legacy_backup_migration_foreign_root_blocked",
            row_index=row_index,
            kind=row_kind,
            legacy_top_segment=parts[0][:120],
        )
        return LiveReanchorOutcome(path_manifest=before, kind="foreign_root_blocked", before_manifest=before)

    return LiveReanchorOutcome(path_manifest=before, kind="failed", before_manifest=before)


def strip_internal_root_when_not_live_top_name(
    manifest_path: str,
    *,
    live_top_level_names: list[str],
    row_index: int,
    row_kind: str,
) -> tuple[str, bool]:
    """
    Strip leading legacy internal ``Root\\`` only when the first segment is not a live
    library top-level folder name (so a real business folder named Root is preserved).
    Emits ``legacy_backup_migration_synthetic_root_stripped`` when the path mutates.
    """
    raw = normalize_manifest_path(str(manifest_path or "").strip())
    if not raw:
        return "", False
    parts = [p for p in raw.split("\\") if p]
    live_cf = {str(x).casefold() for x in live_top_level_names if str(x).strip()}
    if parts and parts[0].casefold() in live_cf:
        return raw, False
    stripped = strip_legacy_internal_root_prefix(raw)
    if raw != stripped:
        log_info(
            "legacy_backup_migration_synthetic_root_stripped",
            row_index=row_index,
            kind=row_kind,
            before_excerpt=raw[:220],
            after_excerpt=stripped[:220],
        )
        return stripped, True
    return raw, False


def strip_internal_root_with_log_legacy_only(
    manifest_path: str,
    *,
    row_index: int,
    row_kind: str,
) -> tuple[str, bool]:
    """Strip internal ``Root\\`` when live skeleton is unavailable (same log semantics)."""
    raw = normalize_manifest_path(str(manifest_path or "").strip())
    stripped = strip_legacy_internal_root_prefix(raw)
    if raw != stripped:
        log_info(
            "legacy_backup_migration_synthetic_root_stripped",
            row_index=row_index,
            kind=row_kind,
            before_excerpt=raw[:220],
            after_excerpt=stripped[:220],
        )
        return stripped, True
    return raw, False
