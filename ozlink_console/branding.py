"""Ozlink IT branding: application icon via Qt resources only (no filesystem paths).

Resource tree is compiled into :mod:`ozlink_console.ozlink_resources_rc` from
``resources/ozlink_resources.qrc``. Regenerate raster assets with
``python scripts/generate_branding_assets.py`` after editing SVGs.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from PySide6.QtGui import QIcon
    from PySide6.QtWidgets import QApplication, QWidget


def _register_qt_resources() -> None:
    """Import side effect: registers :/branding/* with Qt resource system."""
    import ozlink_console.ozlink_resources_rc  # noqa: F401


def branding_resource(path: str) -> str:
    """Return a Qt resource path under ``:/branding/`` (leading slash optional)."""
    p = str(path or "").strip().lstrip("/")
    return f":/branding/{p}" if p else ":/branding/"


def application_icon() -> "QIcon":
    """Primary window/taskbar icon: scalable SVG + raster fallbacks for engines without SVG."""
    from PySide6.QtGui import QIcon, QPixmap

    _register_qt_resources()
    icon = QIcon()
    # Order: explicit PNGs for crisp fixed sizes, SVG + ICO for scalable / shell integration.
    for size in (16, 24, 32, 48, 64, 128, 256):
        p = branding_resource(f"ozlink_app_icon_{size}.png")
        pm = QPixmap(p)
        if not pm.isNull():
            icon.addPixmap(pm)
    svg = branding_resource("ozlink_app_icon.svg")
    icon.addFile(svg)
    ico = branding_resource("ozlink_favicon.ico")
    icon.addFile(ico)
    if icon.isNull():
        return QIcon(svg)
    return icon


def apply_application_branding(app: "QApplication") -> None:
    """Set default window icon for the process (taskbar, new windows, dialogs)."""
    ic = application_icon()
    if not ic.isNull():
        app.setWindowIcon(ic)


def apply_window_icon(widget: "QWidget") -> None:
    """Apply branding to a top-level window (frameless title bar may not inherit QApplication icon)."""
    ic = application_icon()
    if not ic.isNull():
        widget.setWindowIcon(ic)
