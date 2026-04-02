"""Planner-expanded recursive folder scope: Graph subtree → deterministic file mapping ids (Option B).

Kept separate from draft-first folder expansion (``planned_moves`` descendants only).
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any


def synthetic_template_for_browser_run(*, requested_by: str = "browse_recursive_run") -> dict[str, Any]:
    """Minimal ``planned_move``-like metadata for Graph browser–driven synthetic allocations."""
    return {
        "requested_by": str(requested_by or "browse_recursive_run"),
        "requested_date": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "status": "Draft",
    }


def planned_move_folder_template_for_browser(
    *,
    meta: dict[str, Any],
    dest_folder_name: str,
    dest_drive_id: str,
    dest_folder_item_id: str,
    dest_folder_path: str,
) -> dict[str, Any]:
    """``folder_template`` for :func:`planned_move_dict_for_expanded_file` when roots come from trees, not drafts."""
    return {
        **meta,
        "destination": {
            "name": str(dest_folder_name or ""),
            "drive_id": str(dest_drive_id or ""),
            "id": str(dest_folder_item_id or ""),
            "item_path": str(dest_folder_path or ""),
            "is_folder": True,
            "tree_role": "destination",
        },
    }


def deterministic_recursive_mapping_id(source_drive_id: str, source_item_id: str) -> str:
    """Stable mapping_id for an expanded file row: ``recsub-`` + SHA-256 hex (drive + Graph item id)."""
    base = f"{str(source_drive_id or '').strip()}|{str(source_item_id or '').strip()}"
    digest = hashlib.sha256(base.encode("utf-8")).hexdigest()
    return f"recsub-{digest}"


def normalize_graph_path(path: str) -> str:
    """Leading ``/``, forward slashes, no duplicate slashes, non-empty → at least ``/``."""
    raw = str(path or "").strip().replace("\\", "/")
    if not raw or raw == "/":
        return "/"
    parts = [p for p in raw.split("/") if p]
    return "/" + "/".join(parts)


def memory_canonical_to_graph_path(canonical_relative_path: str) -> str:
    """Map library-relative memory path segments to Graph ``item_path`` style."""
    p = str(canonical_relative_path or "").strip().replace("\\", "/").strip("/")
    if not p:
        return "/"
    return normalize_graph_path("/" + p)


def graph_path_is_strict_descendant_file(folder_path: str, file_path: str) -> bool:
    """True when ``file_path`` is strictly under ``folder_path`` (excludes siblings and the folder itself)."""
    fo = normalize_graph_path(folder_path)
    fi = normalize_graph_path(file_path)
    if fi == fo:
        return False
    return fi.lower().startswith(fo.lower() + "/")


def relative_suffix_under_folder(folder_graph_path: str, file_graph_path: str) -> str | None:
    """
    Return library-relative suffix using backslashes (memory style), or None if not a strict descendant.
    Example: folder ``/A/B``, file ``/A/B/c/d.txt`` → ``c\\d.txt``.
    """
    fo = normalize_graph_path(folder_graph_path)
    fi = normalize_graph_path(file_graph_path)
    if not graph_path_is_strict_descendant_file(fo, fi):
        return None
    suf = fi[len(fo) :].lstrip("/")
    if not suf:
        return None
    return suf.replace("/", "\\")


def file_level_allocation_method(folder_method: str) -> str:
    """Drop *recursive* from folder allocation wording for per-file synthetic rows."""
    m = re.sub(r"recursive", "", str(folder_method or ""), flags=re.IGNORECASE)
    m = " ".join(m.split())
    return m if m else "move"


def compose_path_under_folder(folder_base_path: str, relative_suffix_backslash: str) -> str:
    base = str(folder_base_path or "").rstrip("\\/")
    rel = str(relative_suffix_backslash or "").strip("\\/")
    if not rel:
        return base
    if not base:
        return rel.replace("/", "\\")
    return f"{base}\\{rel.replace('/', '\\')}"


def allocation_row_dict_for_expanded_file(
    *,
    mapping_id: str,
    file_name: str,
    source_path: str,
    destination_path: str,
    source_drive_id: str,
    source_item_id: str,
    destination_drive_id: str,
    destination_parent_item_id: str,
    allocation_method: str,
    template_move: dict[str, Any],
) -> dict[str, Any]:
    """JSON object shape for ``Draft-AllocationQueue.json`` (``AllocationRow`` fields)."""
    return {
        "RequestId": str(mapping_id),
        "SourceItemName": str(file_name or ""),
        "SourcePath": str(source_path or ""),
        "SourceType": "File",
        "RequestedDestinationPath": str(destination_path or ""),
        "AllocationMethod": str(allocation_method or ""),
        "RequestedBy": str(template_move.get("requested_by") or ""),
        "RequestedDate": str(template_move.get("requested_date") or ""),
        "Status": str(template_move.get("status") or "Pending"),
        "SourceDriveId": str(source_drive_id or ""),
        "SourceItemId": str(source_item_id or ""),
        "DestinationDriveId": str(destination_drive_id or ""),
        "DestinationParentItemId": str(destination_parent_item_id or ""),
    }


def planned_move_dict_for_expanded_file(
    *,
    mapping_id: str,
    file_name: str,
    source_path: str,
    destination_path: str,
    source_drive_id: str,
    source_item_id: str,
    destination_drive_id: str,
    destination_parent_item_id: str,
    allocation_method: str,
    folder_template: dict[str, Any],
) -> dict[str, Any]:
    """Shape compatible with ``transfer_manifest._planned_move_to_step`` / runtime planned move."""
    dest = folder_template.get("destination") if isinstance(folder_template.get("destination"), dict) else {}
    return {
        "request_id": str(mapping_id),
        "source_path": str(source_path or ""),
        "destination_path": str(destination_path or ""),
        "source_name": str(file_name or ""),
        "destination_name": str(file_name or ""),
        "allocation_method": str(allocation_method or ""),
        "status": str(folder_template.get("status") or "Draft"),
        "requested_by": str(folder_template.get("requested_by") or ""),
        "requested_date": str(folder_template.get("requested_date") or ""),
        "source": {
            "id": str(source_item_id or ""),
            "name": str(file_name or ""),
            "drive_id": str(source_drive_id or ""),
            "is_folder": False,
            "item_path": str(source_path or ""),
            "tree_role": "source",
        },
        "destination": {
            "id": str(destination_parent_item_id or ""),
            "name": str(dest.get("name") or ""),
            "drive_id": str(destination_drive_id or ""),
            "is_folder": True,
            "item_path": str(destination_path or ""),
            "tree_role": "destination",
        },
        "destination_id": str(destination_parent_item_id or ""),
    }
