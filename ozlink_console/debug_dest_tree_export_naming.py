"""Paired file stems for destination tree debug exports (visible UI vs Graph), same timestamp."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Final

# Pairing contract: one timestamp string shared by visible + graph exports
_TS_RE: Final[str] = r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$"


def dest_tree_export_timestamp_str(
    when: datetime | None = None,
) -> str:
    """``YYYY-MM-DD_HH-MM-SS`` in local time (naive *when* is treated as local)."""
    dt = when or datetime.now()
    if dt.tzinfo is None:
        dt = dt.astimezone()
    else:
        dt = dt.astimezone()
    return dt.strftime("%Y-%m-%d_%H-%M-%S")


def dest_tree_export_sanitize_label(label: str) -> str:
    """
    For filename segments: replace spaces with underscores, drop Windows-forbidden
    path chars, trim; empty becomes ``unknown``.
    """
    s = str(label or "").strip()
    s = s.replace(" ", "_")
    for ch in "<>:\"/\\|?*":
        s = s.replace(ch, "_")
    s = re.sub(r"_{2,}", "_", s).strip("._")
    if not s:
        return "unknown"
    if len(s) > 200:
        s = s[:200]
    return s


def dest_tree_paired_file_stems(
    site_display_name: str, library_name: str, ts: str
) -> tuple[str, str]:
    """
    Stems (no extension) for paired exports, e.g.
    ``visible__MySite__Documents__2026-04-21_14-30-00`` and
    ``graph__MySite__Documents__2026-04-21_14-30-00``.
    """
    a, b, _ = dest_tree_triple_file_stems(site_display_name, library_name, ts)
    return (a, b)


def dest_tree_triple_file_stems(
    site_display_name: str, library_name: str, ts: str
) -> tuple[str, str, str]:
    """
    Stems (no extension) for visible, graph, and memory (session snapshot) exports with one timestamp, e.g.
    ``visible__...``, ``graph__...``, ``memory__...``.
    """
    if not re.match(_TS_RE, str(ts or "").strip()):
        ts = dest_tree_export_timestamp_str()
    site = dest_tree_export_sanitize_label(site_display_name)
    lib = dest_tree_export_sanitize_label(library_name)
    t = str(ts).strip()
    v = f"visible__{site}__{lib}__{t}"
    g = f"graph__{site}__{lib}__{t}"
    m = f"memory__{site}__{lib}__{t}"
    return (v, g, m)
