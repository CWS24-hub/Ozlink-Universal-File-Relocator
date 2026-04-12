"""
Translation between internal planning paths (``Root\\...``) and library-relative structure.

Real SharePoint rows are identified in the UI by Graph ``id`` / ``drive_id``; planning memory and
execution still use canonical ``Root\\``-prefixed paths. This module keeps those conversions explicit
and testable without importing MainWindow.

The planning segment ``Root`` is an **invisible namespace** for SharePoint: it must not appear as a
visible folder row; live library folders (e.g. ``RootTest2``) sit at the model/view root.
"""

from __future__ import annotations

from ozlink_console.paths import normalize_manifest_path

PLANNING_ROOT = "Root"


def is_internal_planning_root_semantic_path(normalized_planning_path: str) -> bool:
    """
    True when the path is the internal planning root only (``Root`` or equivalent empty library path).

    Used to attach overlay rows under the **invisible** QAbstractItemModel root — never a visible
    ``Root`` tree row.
    """
    norm = normalize_manifest_path(normalized_planning_path)
    if not norm:
        return True
    parts = [p for p in norm.split("\\") if p]
    return len(parts) == 1 and parts[0].lower() == PLANNING_ROOT.lower()


def library_relative_segments_from_planning_path(planning_path: str) -> list[str]:
    """Strip the leading ``Root`` segment; return folder/file segments under the document library."""
    norm = normalize_manifest_path(planning_path)
    if not norm:
        return []
    parts = [p for p in norm.split("\\") if p]
    if not parts:
        return []
    if parts[0].lower() == PLANNING_ROOT.lower():
        return parts[1:]
    return parts


def canonical_planning_path_from_library_relative_segments(segments: list[str]) -> str:
    """Build ``Root\\Seg1\\Seg2`` from library-relative segments (no leading Root)."""
    clean = [str(s).strip() for s in segments if str(s).strip()]
    if not clean:
        return PLANNING_ROOT
    return normalize_manifest_path(f"{PLANNING_ROOT}\\" + "\\".join(clean))


def planning_path_under_anchor(planning_path: str, anchor_path: str) -> bool:
    """True if ``planning_path`` is ``anchor_path`` or a descendant (normalized)."""
    p = normalize_manifest_path(planning_path)
    a = normalize_manifest_path(anchor_path)
    if not p or not a:
        return False
    if p.lower() == a.lower():
        return True
    return p.lower().startswith(a.lower() + "\\")


def remap_root_suffix_under_anchor(planning_path: str, anchor_path: str) -> str:
    """
    If ``planning_path`` is ``Root\\X\\Rest`` and ``anchor_path`` is ``Root\\LibFolder``, return
    ``Root\\LibFolder\\X\\Rest``. Used when persisted paths omit a single default library folder.
    Mirrors :meth:`MainWindow._destination_reanchor_sharepoint_projection_path` without Qt.
    """
    p = normalize_manifest_path(planning_path)
    an = normalize_manifest_path(anchor_path)
    if not p or not an:
        return p
    if p.lower().startswith(an.lower() + "\\") or p.lower() == an.lower():
        return p
    rk = normalize_manifest_path(PLANNING_ROOT)
    if p.lower() == rk.lower():
        return p
    if not p.lower().startswith(rk.lower() + "\\"):
        return p
    parts = [x for x in p.split("\\") if x]
    if not parts or parts[0].lower() != rk.lower():
        return p
    suffix_parts = parts[1:]
    if not suffix_parts:
        return p
    return normalize_manifest_path(an + "\\" + "\\".join(suffix_parts))
