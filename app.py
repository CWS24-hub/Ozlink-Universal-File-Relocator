import atexit
import os
import signal
import sys
import threading
import faulthandler

from PySide6.QtCore import QtMsgType, QThreadPool, QTimer, qInstallMessageHandler
from PySide6.QtWidgets import QApplication

from ozlink_console.logger import (
    flush_logger,
    get_crash_binary_log_file,
    get_crash_log_path,
    init_session_logging,
    log_error,
    log_info,
    log_session_diagnostics_initialized,
    log_trace,
    qt_threadpool_snapshot,
    thread_inventory_snapshot,
)
from ozlink_console.paths import ensure_app_storage_directories
from ozlink_console.dev_mode import apply_cli_dev_flag
from ozlink_console.main_window import MainWindow


_CRASH_FILE_HANDLE = None


def _log_shutdown_thread_inventory(phase: str, **extra: object) -> None:
    """Emit JSON-safe thread list for diagnosing python.exe lingering after UI shutdown."""
    try:
        inv = thread_inventory_snapshot()
        qt_snap = qt_threadpool_snapshot()
        log_info(
            "shutdown_trace",
            event="thread_inventory",
            phase=str(phase)[:120],
            **inv,
            **qt_snap,
            **{k: v for k, v in extra.items() if v is not None},
        )
    except Exception as exc:
        log_info("shutdown_trace", event="thread_inventory_failed", phase=str(phase)[:120], error=str(exc)[:240])
    try:
        flush_logger()
    except Exception:
        pass


def _atexit_process_marker() -> None:
    """Last Python-level hook before interpreter teardown (if the process actually exits)."""
    try:
        _log_shutdown_thread_inventory("atexit")
        log_info("shutdown_trace", event="process_termination_marker", note="after_atexit_python_shutdown_pending")
        flush_logger()
    except Exception as exc:
        try:
            log_error("atexit_shutdown_marker_failed.", error=str(exc)[:500])
            flush_logger()
        except Exception:
            pass


def _drain_global_qthreadpool_after_exec(*, phase: str) -> None:
    """Second-chance pool drain after app.exec() returns (runnables may still be queued)."""
    try:
        tp = QThreadPool.globalInstance()
        log_info(
            "shutdown_trace",
            event="qthreadpool_post_app_exec_enter",
            phase=str(phase)[:80],
            **qt_threadpool_snapshot(),
        )
        tp.waitForDone(12000)
        try:
            tp.clear()
        except Exception:
            pass
        tp.waitForDone(4000)
        log_info(
            "shutdown_trace",
            event="qthreadpool_post_app_exec_exit",
            phase=str(phase)[:80],
            **qt_threadpool_snapshot(),
        )
    except Exception as exc:
        log_info("shutdown_trace", event="qthreadpool_post_app_exec_failed", error=str(exc)[:240])


def _install_native_crash_capture():
    global _CRASH_FILE_HANDLE

    crash_path = get_crash_log_path()
    _CRASH_FILE_HANDLE = get_crash_binary_log_file()
    banner = f"[native-crash] crash log started path={crash_path}\n"
    _CRASH_FILE_HANDLE.write(banner.encode("utf-8", errors="replace"))
    _CRASH_FILE_HANDLE.flush()

    faulthandler.enable(file=_CRASH_FILE_HANDLE, all_threads=True)

    def _qt_message_handler(msg_type, context, message):
        type_map = {
            QtMsgType.QtDebugMsg: "debug",
            QtMsgType.QtInfoMsg: "info",
            QtMsgType.QtWarningMsg: "warning",
            QtMsgType.QtCriticalMsg: "critical",
            QtMsgType.QtFatalMsg: "fatal",
        }
        file_name = getattr(context, "file", "") or ""
        line_number = getattr(context, "line", 0) or 0
        function_name = getattr(context, "function", "") or ""
        formatted = (
            f"[qt-message] type={type_map.get(msg_type, str(msg_type))} "
            f"file={file_name} line={line_number} function={function_name} message={message}\n"
        )
        _CRASH_FILE_HANDLE.write(formatted.encode("utf-8", errors="replace"))
        _CRASH_FILE_HANDLE.flush()
        log_info(
            "Qt message captured.",
            qt_type=type_map.get(msg_type, str(msg_type)),
            qt_file=file_name,
            qt_line=line_number,
            qt_function=function_name,
            qt_message=message,
        )

    qInstallMessageHandler(_qt_message_handler)
    log_info("Native crash capture enabled.", crash_log_path=str(crash_path))
    log_session_diagnostics_initialized()


def _install_global_exception_hooks():
    def _sys_excepthook(exc_type, exc_value, exc_traceback):
        log_error(
            "Unhandled process exception.",
            exception_type=getattr(exc_type, "__name__", str(exc_type)),
            error=str(exc_value),
            traceback="".join(__import__("traceback").format_exception(exc_type, exc_value, exc_traceback)),
        )
        sys.__excepthook__(exc_type, exc_value, exc_traceback)

    sys.excepthook = _sys_excepthook

    if hasattr(threading, "excepthook"):
        def _threading_excepthook(args):
            log_error(
                "Unhandled thread exception.",
                thread_name=getattr(args.thread, "name", ""),
                exception_type=getattr(args.exc_type, "__name__", str(args.exc_type)),
                error=str(args.exc_value),
                traceback="".join(__import__("traceback").format_exception(args.exc_type, args.exc_value, args.exc_traceback)),
            )
            threading.__excepthook__(args)

        threading.excepthook = _threading_excepthook


def run_app():
    apply_cli_dev_flag(sys.argv)
    # Full UI/graph trace off by default (large logs and JSON serialization can stall the UI thread).
    # Set OZLINK_FULL_TRACE=1 before launch when diagnosing projection/restore issues.
    os.environ.setdefault("OZLINK_FULL_TRACE", "0")
    ensure_app_storage_directories()
    init_session_logging()
    atexit.register(_atexit_process_marker)
    _install_global_exception_hooks()
    _install_native_crash_capture()
    app = QApplication(sys.argv)
    app.setApplicationName("Ozlink IT – SharePoint File Relocation Console")
    app.setOrganizationName("Ozlink IT")
    log_trace("app", "run_app_start", argv_excerpt=" ".join(sys.argv[:8])[:400])

    def _on_about_to_quit():
        log_info("QApplication aboutToQuit emitted.")
        log_trace("app", "about_to_quit")
        log_info(
            "shutdown_trace",
            event="aboutToQuit",
            note="qt_emits_before_event_loop_returns_from_exec",
            **thread_inventory_snapshot(),
            **qt_threadpool_snapshot(),
        )
        _log_shutdown_thread_inventory("aboutToQuit")

    app.aboutToQuit.connect(_on_about_to_quit)

    window = MainWindow()
    log_trace("app", "main_window_constructed")
    # Console Ctrl+C: avoid KeyboardInterrupt inside Qt/model code (e.g. overlay invariant DFS).
    try:

        def _sigint_graceful_quit(_signum, _frame):
            try:
                window._application_shutting_down = True
            except Exception:
                pass
            try:
                QTimer.singleShot(0, app.quit)
            except Exception:
                pass

        signal.signal(signal.SIGINT, _sigint_graceful_quit)
    except (ValueError, OSError):
        pass
    window.show()
    log_trace("app", "main_window_shown")

    exit_code = int(app.exec())
    _drain_global_qthreadpool_after_exec(phase="post_app_exec")
    log_info(
        "shutdown_trace",
        event="app_exec_returned",
        exit_code=exit_code,
        note="qt_event_loop_exited",
        **thread_inventory_snapshot(),
        **qt_threadpool_snapshot(),
    )
    _log_shutdown_thread_inventory("shutdown_end_post_app_exec")
    flush_logger()
    return exit_code


if __name__ == "__main__":
    _ec = run_app()
    log_info("shutdown_trace", event="main_after_run_app", exit_code=int(_ec), note="before_sys_exit")
    _log_shutdown_thread_inventory("main_after_run_app")
    flush_logger()
    sys.exit(_ec)
