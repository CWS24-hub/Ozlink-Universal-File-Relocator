"""Full process respawn for developer iteration (no in-process reload)."""

from __future__ import annotations

import os
import subprocess
import sys

from PySide6.QtWidgets import QApplication

from ozlink_console.dev_mode import is_dev_mode


def build_respawn_command() -> list[str]:
    """Build argv for ``subprocess.Popen`` to rerun the same entry mode.

    Non-frozen: ``[sys.executable] + sys.argv`` (interpreter + script + args).

    Frozen (e.g. PyInstaller): ``sys.executable`` is the bundle; avoid duplicating
    ``argv[0]`` when it already matches the executable path.
    """
    exe = sys.executable
    argv = list(sys.argv)
    if getattr(sys, "frozen", False):
        if argv:
            try:
                a0 = os.path.normcase(os.path.abspath(argv[0]))
                ex = os.path.normcase(os.path.abspath(exe))
                if a0 == ex:
                    return [exe] + argv[1:]
            except OSError:
                pass
        return [exe] + argv[1:] if len(argv) > 1 else [exe]
    return [exe] + argv


def respawn_and_exit() -> None:
    """Start a new process with the same command line, then terminate this one."""
    if not is_dev_mode():
        return
    cmd = build_respawn_command()
    popen_kw: dict = {}
    if sys.platform == "win32":
        popen_kw["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        popen_kw["close_fds"] = True
        popen_kw["start_new_session"] = True
    subprocess.Popen(cmd, **popen_kw)
    app = QApplication.instance()
    if app is not None:
        app.quit()
    sys.exit(0)
