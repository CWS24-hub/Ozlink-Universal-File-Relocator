"""
Destination planning paths for SharePoint / Graph: **graph-relative** canonical form.

Canonical keys and lookups use the same path shape as visible Graph-backed rows, for example
``Root3\\Finance\\Payroll`` — **not** a leading internal ``Root\\`` segment.

Legacy persisted values may still start with ``Root\\`` (old internal namespace). Functions here
strip that prefix when parsing; they never add it when building canonical paths.

The sentinel :data:`OVERLAY_LIB_ROOT_SEMANTIC` in ``destination_overlay_identity`` is the only
internal topology key for overlay wiring (not a visible folder and not the string ``Root``).
"""

from __future__ import annotations

from ozlink_console.paths import normalize_manifest_path

# First path segment to strip when normalizing legacy persisted paths (historical namespace).
LEGACY_INTERNAL_ROOT_SEGMENT = "Root"


def strip_legacy_internal_root_prefix(path: str) -> str:
    """Remove leading legacy ``Root\\`` segments; return normalized graph-relative path (may be empty)."""
    norm = normalize_manifest_path(str(path or "").strip())
    if not norm:
        return ""
    parts = [p for p in norm.split("\\") if p]
    while parts and parts[0].lower() == LEGACY_INTERNAL_ROOT_SEGMENT.lower():
        parts.pop(0)
    if not parts:
        return ""
    return normalize_manifest_path("\\".join(parts))


def is_internal_planning_root_semantic_path(normalized_planning_path: str) -> bool:
    """
    True when, after stripping legacy ``Root\\`` prefixes, no path segments remain.

    Used to drop obsolete semantic ``Root`` entry roots from packaged maps — not to assert that
    ``Root`` is a valid canonical key.
    """
    return strip_legacy_internal_root_prefix(normalized_planning_path) == ""


def library_relative_segments_from_planning_path(planning_path: str) -> list[str]:
    """Split a planning path and strip legacy ``Root\\`` prefix segments; remaining segments are canonical."""
    s = strip_legacy_internal_root_prefix(planning_path)
    if not s:
        return []
    return [p for p in s.split("\\") if p]


def canonical_planning_path_from_library_relative_segments(segments: list[str]) -> str:
    """Join segments into a normalized graph-relative canonical path (empty list → empty string)."""
    clean = [str(s).strip() for s in segments if str(s).strip()]
    if not clean:
        return ""
    return normalize_manifest_path("\\".join(clean))


def planning_path_under_anchor(planning_path: str, anchor_path: str) -> bool:
    """True if ``planning_path`` is ``anchor_path`` or a descendant (normalized, graph-relative)."""
    p = normalize_manifest_path(planning_path)
    a = normalize_manifest_path(anchor_path)
    if not p or not a:
        return False
    if p.lower() == a.lower():
        return True
    return p.lower().startswith(a.lower() + "\\")


def remap_under_visible_library_anchor(path: str, anchor_path: str) -> str:
    """
    If ``path`` is not already under ``anchor_path``, prepend ``anchor_path``.

    Both arguments should be graph-relative (legacy ``Root\\`` stripped by callers if needed).
    Used when persisted paths omitted the visible library hub segment (e.g. ``Finance`` → ``Root3\\Finance``).
    """
    p = normalize_manifest_path(str(path or "").strip())
    an = normalize_manifest_path(str(anchor_path or "").strip())
    if not p:
        return an
    if not an:
        return p
    pl = p.lower()
    al = an.lower()
    if pl == al or pl.startswith(al + "\\"):
        return p
    p_segs = [x for x in p.split("\\") if x]
    an_segs = [x for x in an.split("\\") if x]
    if p_segs and an_segs and p_segs[0].lower() == an_segs[0].lower():
        return p
    return normalize_manifest_path(an + "\\" + p)


def remap_root_suffix_under_anchor(planning_path: str, anchor_path: str) -> str:
    """
    If ``planning_path`` used a legacy leading ``Root\\`` segment, strip it and prepend ``anchor_path``
    when needed (:func:`remap_under_visible_library_anchor`). Paths that never used that prefix are
    returned graph-relative via :func:`strip_legacy_internal_root_prefix` only (no hub injection).
    """
    raw_p = normalize_manifest_path(str(planning_path or "").strip())
    p_strip = strip_legacy_internal_root_prefix(raw_p)
    a_strip = strip_legacy_internal_root_prefix(str(anchor_path or "").strip())
    if not raw_p:
        return ""
    had_legacy_root = raw_p.lower() == LEGACY_INTERNAL_ROOT_SEGMENT.lower() or raw_p.lower().startswith(
        LEGACY_INTERNAL_ROOT_SEGMENT.lower() + "\\"
    )
    if not had_legacy_root:
        return p_strip
    return remap_under_visible_library_anchor(p_strip, a_strip)
