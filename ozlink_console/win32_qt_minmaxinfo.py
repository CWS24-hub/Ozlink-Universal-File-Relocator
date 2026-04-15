"""
Windows: cap WM_GETMINMAXINFO ``ptMinTrackSize`` to the monitor work area.

Qt can compute a minimum track size larger than the shell work rect on mixed-DPI and
multi-monitor setups. A process-wide :class:`QAbstractNativeEventFilter` ensures every
top-level HWND path receives the clamp (not only :meth:`QWidget.nativeEvent` on the
main window).
"""

from __future__ import annotations

import sys
from typing import Optional, Tuple

WM_GETMINMAXINFO = 0x0024
GA_ROOT = 2

if sys.platform.startswith("win"):
    import ctypes
    from ctypes import wintypes

    class _WIN32_RECT(ctypes.Structure):
        _fields_ = (
            ("left", ctypes.c_long),
            ("top", ctypes.c_long),
            ("right", ctypes.c_long),
            ("bottom", ctypes.c_long),
        )

    class _WIN32_MONITORINFO(ctypes.Structure):
        _fields_ = (
            ("cbSize", wintypes.DWORD),
            ("rcMonitor", _WIN32_RECT),
            ("rcWork", _WIN32_RECT),
            ("dwFlags", wintypes.DWORD),
        )

    class _WIN32_POINT(ctypes.Structure):
        _fields_ = (("x", ctypes.c_long), ("y", ctypes.c_long))

    class _WIN32_MINMAXINFO(ctypes.Structure):
        _fields_ = (
            ("ptReserved", _WIN32_POINT),
            ("ptMaxSize", _WIN32_POINT),
            ("ptMaxPosition", _WIN32_POINT),
            ("ptMaxTrackSize", _WIN32_POINT),
            ("ptMinTrackSize", _WIN32_POINT),
        )

    def win32_monitor_rc_work_phys(hwnd: int) -> Optional[Tuple[int, int]]:
        """Monitor work area size in pixels (same space as ``MINMAXINFO.ptMinTrackSize``).

        Prefer ``MonitorFromPoint`` on the window center so mixed-DPI / multi-monitor layouts
        match the monitor that actually contains the frame (``MonitorFromWindow`` alone can
        disagree for Qt wrapper HWNDs).
        """
        if hwnd == 0:
            return None
        MONITOR_DEFAULTTONEAREST = 2

        def _work_from_hmon(hmon) -> Optional[Tuple[int, int]]:
            if not hmon:
                return None
            mi = _WIN32_MONITORINFO()
            mi.cbSize = ctypes.sizeof(_WIN32_MONITORINFO)
            if not ctypes.windll.user32.GetMonitorInfoW(hmon, ctypes.byref(mi)):
                return None
            w = int(mi.rcWork.right - mi.rcWork.left)
            h = int(mi.rcWork.bottom - mi.rcWork.top)
            if w <= 0 or h <= 0:
                return None
            return w, h

        rect = _WIN32_RECT()
        if ctypes.windll.user32.GetWindowRect(int(hwnd), ctypes.byref(rect)):
            cx = (int(rect.left) + int(rect.right)) // 2
            cy = (int(rect.top) + int(rect.bottom)) // 2
            pt = wintypes.POINT(cx, cy)
            hmon_pt = ctypes.windll.user32.MonitorFromPoint(pt, MONITOR_DEFAULTTONEAREST)
            wh = _work_from_hmon(hmon_pt)
            if wh is not None:
                return wh

        hmon = ctypes.windll.user32.MonitorFromWindow(int(hwnd), MONITOR_DEFAULTTONEAREST)
        return _work_from_hmon(hmon)

    def win32_clamp_minmaxinfo_pt_min_track(hwnd: int, lparam: int) -> None:
        """Cap ``ptMinTrackSize`` so it never exceeds the monitor work area."""
        if not lparam:
            return
        wh = win32_monitor_rc_work_phys(hwnd)
        if wh is None:
            return
        work_w, work_h = wh
        try:
            mmi = ctypes.cast(int(lparam), ctypes.POINTER(_WIN32_MINMAXINFO)).contents
        except (TypeError, ValueError, OSError):
            return
        cap_w = max(320, work_w)
        cap_h = max(240, work_h)
        if mmi.ptMinTrackSize.x > cap_w:
            mmi.ptMinTrackSize.x = cap_w
        if mmi.ptMinTrackSize.y > cap_h:
            mmi.ptMinTrackSize.y = cap_h

    def _win32_root_hwnd(hwnd: int) -> int:
        if hwnd == 0:
            return 0
        try:
            root = ctypes.windll.user32.GetAncestor(int(hwnd), GA_ROOT)
            return int(root) if root else int(hwnd)
        except Exception:
            return int(hwnd)

    def _clamp_lparam_for_hwnds(lparam: int, hwnds: list[int]) -> None:
        seen: set[int] = set()
        for h in hwnds:
            hi = int(h or 0)
            if hi == 0 or hi in seen:
                continue
            seen.add(hi)
            try:
                win32_clamp_minmaxinfo_pt_min_track(hi, lparam)
            except (TypeError, ValueError, OSError, RuntimeError, AttributeError):
                pass

    def qt_native_clamp_getminmaxinfo(event_type, message, *, extra_hwnds: tuple[int, ...] = ()) -> bool:
        """If ``event_type``/``message`` is WM_GETMINMAXINFO, clamp in place; return True if so."""
        from PySide6.QtCore import QByteArray

        et = event_type
        if isinstance(et, QByteArray):
            et = bytes(et)
        elif not isinstance(et, (bytes, bytearray)):
            return False
        if et != b"windows_generic_MSG":
            return False
        try:
            msg = wintypes.MSG.from_address(int(message))
        except (TypeError, ValueError, OSError):
            return False
        if msg.message != WM_GETMINMAXINFO:
            return False
        lp = int(getattr(msg, "lParam", 0) or 0)
        if lp == 0:
            return True
        _hwnd = getattr(msg, "hWnd", None)
        if _hwnd is None:
            _hwnd = getattr(msg, "hwnd", None)
        hi = int(_hwnd) if _hwnd is not None else 0
        hwnds: list[int] = []
        if hi != 0:
            hwnds.append(hi)
            root = _win32_root_hwnd(hi)
            if root not in (0, hi):
                hwnds.append(root)
        for ex in extra_hwnds:
            ei = int(ex or 0)
            if ei != 0 and ei not in hwnds:
                hwnds.append(ei)
        _clamp_lparam_for_hwnds(lp, hwnds)
        return True

else:

    def win32_monitor_rc_work_phys(_hwnd: int) -> Optional[Tuple[int, int]]:
        return None

    def win32_clamp_minmaxinfo_pt_min_track(_hwnd: int, _lparam: int) -> None:
        return None

    def qt_native_clamp_getminmaxinfo(_event_type, _message, *, extra_hwnds: tuple[int, ...] = ()) -> bool:
        return False


def install_win32_getminmaxinfo_native_filter(app) -> None:
    """Register a process-wide filter so every HWND path gets MINMAXINFO clamping."""
    if not sys.platform.startswith("win"):
        return
    try:
        from PySide6.QtGui import QAbstractNativeEventFilter
    except ImportError:
        return

    class _OzlinkGetMinMaxInfoFilter(QAbstractNativeEventFilter):
        def nativeEventFilter(self, event_type, message):
            try:
                qt_native_clamp_getminmaxinfo(event_type, message, extra_hwnds=())
            except Exception:
                pass
            return False, 0

    filt = _OzlinkGetMinMaxInfoFilter()
    app.installNativeEventFilter(filt)
    setattr(app, "_ozlink_getminmaxinfo_native_filter", filt)
