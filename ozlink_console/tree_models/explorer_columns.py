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


def explorer_type_label(node_data: Dict[str, Any]) -> str:
    if not node_data or node_data.get("placeholder"):
        return ""
    origin = str(node_data.get("node_origin", "")).lower()
    if origin == "localfilesystem":
        if node_data.get("is_folder"):
            return "File folder"
        return _type_from_filename(str(node_data.get("name", "")))
    if node_data.get("proposed") or origin == "proposed":
        return "Proposed folder"
    if node_data.get("planned_allocation") or origin == "plannedallocation":
        if node_data.get("is_folder"):
            return "File folder"
        return _type_from_filename(str(node_data.get("name", "")))
    if origin == "projecteddestination":
        return "File folder"
    if node_data.get("is_folder"):
        return "File folder"
    raw = node_data.get("raw") if isinstance(node_data.get("raw"), dict) else {}
    file_meta = raw.get("file") if isinstance(raw.get("file"), dict) else {}
    mime = str(file_meta.get("mimeType") or "").strip().lower()
    if "pdf" in mime:
        return "PDF Document"
    if "wordprocessingml" in mime or mime == "application/msword":
        return "Microsoft Word Document"
    if "spreadsheetml" in mime or mime == "application/vnd.ms-excel":
        return "Microsoft Excel Worksheet"
    if "presentationml" in mime or mime == "application/vnd.ms-powerpoint":
        return "Microsoft PowerPoint Presentation"
    if mime.startswith("image/"):
        return "Image"
    if mime.startswith("text/"):
        return "Text Document"
    return _type_from_filename(str(node_data.get("name", "")))


def explorer_date_label(node_data: Dict[str, Any]) -> str:
    if not node_data or node_data.get("placeholder"):
        return ""
    raw = node_data.get("raw") if isinstance(node_data.get("raw"), dict) else {}
    iso = raw.get("lastModifiedDateTime") or node_data.get("last_modified")
    if not iso:
        return "—"
    iso_str = str(iso).strip()
    qdt = QDateTime.fromString(iso_str, Qt.DateFormat.ISODateWithMs)
    if not qdt.isValid():
        qdt = QDateTime.fromString(iso_str, Qt.DateFormat.ISODate)
    if qdt.isValid():
        return QLocale.system().toString(qdt.toLocalTime(), QLocale.FormatType.ShortFormat)
    try:
        s = str(iso).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        qdt2 = QDateTime.fromSecsSinceEpoch(int(dt.timestamp()))
        return QLocale.system().toString(qdt2, QLocale.FormatType.ShortFormat)
    except Exception:
        return str(iso)[:22]
