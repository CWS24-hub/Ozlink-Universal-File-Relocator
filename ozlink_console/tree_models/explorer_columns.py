"""Shared Explorer-style columns matching QFileSystemModel: Name, Size, Type, Date modified."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from PySide6.QtCore import QDateTime, QLocale, Qt
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import QFileIconProvider

# Same order as Qt's QFileSystemModel detail columns (local This PC / network tree).
EXPLORER_COLUMN_LABELS = ("Name", "Size", "Type", "Date modified")
EXPLORER_COLUMN_COUNT = 4

_EXT_LABELS: Dict[str, str] = {
    ".txt": "Text Document",
    ".pdf": "PDF Document",
    ".doc": "Microsoft Word Document",
    ".docx": "Microsoft Word Document",
    ".xls": "Microsoft Excel Worksheet",
    ".xlsx": "Microsoft Excel Worksheet",
    ".ppt": "Microsoft PowerPoint Presentation",
    ".pptx": "Microsoft PowerPoint Presentation",
    ".png": "PNG Image",
    ".jpg": "JPEG Image",
    ".jpeg": "JPEG Image",
    ".gif": "GIF Image",
    ".zip": "Compressed (zipped) Folder",
    ".json": "JSON Source File",
    ".xml": "XML Document",
    ".csv": "Microsoft Excel Comma Separated Values File",
    ".md": "Markdown Source File",
}

_icon_provider: Optional[QFileIconProvider] = None
_folder_icon: Optional[QIcon] = None
_file_icon: Optional[QIcon] = None


def _explorer_folder_file_icons() -> Tuple[QIcon, QIcon]:
    global _icon_provider, _folder_icon, _file_icon
    if _folder_icon is None or _file_icon is None:
        _icon_provider = QFileIconProvider()
        _folder_icon = _icon_provider.icon(QFileIconProvider.IconType.Folder)
        _file_icon = _icon_provider.icon(QFileIconProvider.IconType.File)
    return _folder_icon, _file_icon


def explorer_icon_for_node(node_data: Dict[str, Any]) -> QIcon:
    """Same style as QFileSystemModel name column (system folder / generic file icon)."""
    if not node_data or node_data.get("placeholder"):
        return QIcon()
    folder_ic, file_ic = _explorer_folder_file_icons()
    if node_data.get("is_folder"):
        return folder_ic
    return file_ic


def planned_leaf_filename_implies_document_file(name: str) -> bool:
    """True when the leaf name has a known document/media extension (planned bind fallback).

    Used when ``move['source'].get('is_folder')`` is absent so folder moves without extensions
    stay folder-shaped, while ``Contractor bank.docx``-style leaves still bind as ``planned_file``.

    ``.zip`` is excluded: Explorer labels it as a compressed folder and real folder names can end in ``.zip``.
    """
    ext = Path(str(name or "").strip()).suffix.lower()
    if not ext or ext == ".zip":
        return False
    return ext in _EXT_LABELS


def _type_from_filename(name: str) -> str:
    ext = Path(name or "").suffix.lower()
    if ext in _EXT_LABELS:
        return _EXT_LABELS[ext]
    if ext:
        return f"{ext[1:].upper()} File"
    return "File"


def explorer_size_label(node_data: Dict[str, Any]) -> str:
    if not node_data or node_data.get("placeholder"):
        return ""
    if node_data.get("is_folder"):
        return ""
    size = node_data.get("size")
    if isinstance(size, int) and size > 0:
        return QLocale.system().formattedDataSize(size)
    return ""


def _explorer_type_label_cache_tag(node_data: Dict[str, Any]) -> str:
    raw = node_data.get("raw") if isinstance(node_data.get("raw"), dict) else {}
    mime = ""
    if isinstance(raw, dict):
        file_meta = raw.get("file") if isinstance(raw.get("file"), dict) else {}
        mime = str(file_meta.get("mimeType") or "").strip().lower()
    parts = (
        str(node_data.get("placeholder") or ""),
        str(node_data.get("is_folder") or ""),
        str(node_data.get("node_origin") or ""),
        str(node_data.get("verification_state") or ""),
        str(node_data.get("row_kind") or ""),
        str(node_data.get("planned_allocation") or ""),
        str(node_data.get("proposed") or ""),
        str(node_data.get("name") or ""),
        mime,
    )
    return "|".join(parts)


def explorer_type_label(node_data: Dict[str, Any]) -> str:
    if not node_data or node_data.get("placeholder"):
        return ""
    tag = _explorer_type_label_cache_tag(node_data)
    if node_data.get("_explorer_type_lbl_tag") == tag and "_explorer_type_lbl_val" in node_data:
        return str(node_data.get("_explorer_type_lbl_val") or "")
    origin = str(node_data.get("node_origin", "")).lower()
    if origin == "localfilesystem":
        out = "File folder" if node_data.get("is_folder") else _type_from_filename(str(node_data.get("name", "")))
    elif node_data.get("proposed") or origin == "proposed":
        out = "Proposed folder"
    else:
        vs_planned = str(node_data.get("verification_state", "")).strip() == "planned_only"
        rk = str(node_data.get("row_kind", "")).strip().lower()
        if (
            vs_planned
            and rk in {"planned_folder", "planned_file"}
            and not node_data.get("planned_allocation")
            and origin != "plannedallocation"
        ):
            if rk == "planned_file":
                out = "Planned file"
            else:
                out = "Planned folder"
        elif node_data.get("planned_allocation") or origin == "plannedallocation":
            out = "Allocated folder" if node_data.get("is_folder") else "Allocated file"
        elif origin == "projecteddestination":
            out = "File folder" if node_data.get("is_folder") else _type_from_filename(str(node_data.get("name", "")))
        elif origin == "projectedallocationdescendant":
            out = "File folder" if node_data.get("is_folder") else _type_from_filename(str(node_data.get("name", "")))
        elif node_data.get("is_folder"):
            out = "File folder"
        else:
            raw = node_data.get("raw") if isinstance(node_data.get("raw"), dict) else {}
            file_meta = raw.get("file") if isinstance(raw.get("file"), dict) else {}
            mime = str(file_meta.get("mimeType") or "").strip().lower()
            if "pdf" in mime:
                out = "PDF Document"
            elif "wordprocessingml" in mime or mime == "application/msword":
                out = "Microsoft Word Document"
            elif "spreadsheetml" in mime or mime == "application/vnd.ms-excel":
                out = "Microsoft Excel Worksheet"
            elif "presentationml" in mime or mime == "application/vnd.ms-powerpoint":
                out = "Microsoft PowerPoint Presentation"
            elif mime.startswith("image/"):
                out = "Image"
            elif mime.startswith("text/"):
                out = "Text Document"
            else:
                out = _type_from_filename(str(node_data.get("name", "")))
    node_data["_explorer_type_lbl_tag"] = tag
    node_data["_explorer_type_lbl_val"] = out
    return out


def explorer_date_label(node_data: Dict[str, Any]) -> str:
    if not node_data or node_data.get("placeholder"):
        return ""
    raw = node_data.get("raw") if isinstance(node_data.get("raw"), dict) else {}
    iso = raw.get("lastModifiedDateTime") or node_data.get("last_modified")
    tag = str(iso or "")
    if node_data.get("_explorer_date_lbl_tag") == tag and "_explorer_date_lbl_val" in node_data:
        return str(node_data.get("_explorer_date_lbl_val") or "")
    if not iso:
        node_data["_explorer_date_lbl_tag"] = tag
        node_data["_explorer_date_lbl_val"] = "—"
        return "—"
    iso_str = str(iso).strip()
    qdt = QDateTime.fromString(iso_str, Qt.DateFormat.ISODateWithMs)
    if not qdt.isValid():
        qdt = QDateTime.fromString(iso_str, Qt.DateFormat.ISODate)
    if qdt.isValid():
        out = QLocale.system().toString(qdt.toLocalTime(), QLocale.FormatType.ShortFormat)
        node_data["_explorer_date_lbl_tag"] = tag
        node_data["_explorer_date_lbl_val"] = out
        return out
    try:
        s = str(iso).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        qdt2 = QDateTime.fromSecsSinceEpoch(int(dt.timestamp()))
        out = QLocale.system().toString(qdt2, QLocale.FormatType.ShortFormat)
        node_data["_explorer_date_lbl_tag"] = tag
        node_data["_explorer_date_lbl_val"] = out
        return out
    except Exception:
        out = str(iso)[:22]
        node_data["_explorer_date_lbl_tag"] = tag
        node_data["_explorer_date_lbl_val"] = out
        return out
