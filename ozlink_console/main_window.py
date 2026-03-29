import sys
import os
import json
import hashlib
import subprocess
import time
from collections import Counter, deque
import ctypes
import traceback
import re
import io
import math
import zipfile
from datetime import datetime, timezone
from pathlib import Path
import xml.etree.ElementTree as ET

from PySide6.QtWidgets import (
    QMainWindow, QWidget, QHBoxLayout, QVBoxLayout, QGridLayout,
    QPushButton, QLabel, QStackedWidget, QMessageBox, QFrame,
    QLineEdit, QSizePolicy, QComboBox, QTreeWidget, QTreeWidgetItem,
    QAbstractItemView, QTableWidget, QTableWidgetItem, QHeaderView,
    QTabWidget, QMenu, QInputDialog, QTextEdit, QStyledItemDelegate,
    QStyle, QStyleOptionViewItem, QApplication, QFileDialog, QDialog,
    QCheckBox,
    QDialogButtonBox, QFormLayout, QTreeView,
)
from PySide6.QtCore import (
    Qt,
    QThread,
    Signal,
    QTimer,
    QRect,
    QSettings,
    QUrl,
    QPoint,
    QEventLoop,
    QElapsedTimer,
    QModelIndex,
    QPersistentModelIndex,
)
from PySide6.QtGui import QGuiApplication, QDesktopServices, QPainter, QColor, QPolygon, QCursor, QBrush

from ozlink_console.graph import GraphClient
from ozlink_console.tree_models.sharepoint_source_model import SharePointSourceTreeModel
from ozlink_console.logger import log_error, log_info, log_trace, log_warn
from ozlink_console.memory import MemoryManager
from ozlink_console.models import AllocationRow, ProposedFolder, SessionState, SubmissionBatch
from ozlink_console.requests_store import RequestStore
from ozlink_console.transfer_manifest import build_simulation_manifest, write_manifest_json
from ozlink_console.transfer_job_runner import (
    load_manifest_json,
    validate_manifest,
    manifest_execution_summary,
)


class LoginWorker(QThread):
    success = Signal(dict)
    error = Signal(str)

    def __init__(self, graph):
        super().__init__()
        self.graph = graph

    def run(self):
        log_trace("worker", "LoginWorker_start")
        try:
            self.graph.acquire_token()
            session_context = self.graph.build_session_context()
            log_trace("worker", "LoginWorker_success", user_role=session_context.get("user_role", ""))
            self.success.emit({
                "profile": session_context.get("profile", {}),
                "session_context": session_context,
            })
        except Exception as e:
            log_trace("worker", "LoginWorker_error", error_excerpt=str(e)[:500])
            self.error.emit(str(e))


class DiscoverSitesWorker(QThread):
    success = Signal(dict)
    error = Signal(str)

    def __init__(self, graph):
        super().__init__()
        self.graph = graph

    def run(self):
        log_trace("worker", "DiscoverSitesWorker_start")
        try:
            discovered_sites = self.graph.discover_sites_with_libraries()
            log_trace("worker", "DiscoverSitesWorker_success", site_count=len(discovered_sites or []))
            self.success.emit({"discovered_sites": discovered_sites})
        except Exception as e:
            log_trace("worker", "DiscoverSitesWorker_error", error_excerpt=str(e)[:500])
            self.error.emit(str(e))


class DeviceFlowWorker(QThread):
    success = Signal(dict)
    error = Signal(str)

    def __init__(self, graph):
        super().__init__()
        self.graph = graph

    def run(self):
        log_trace("worker", "DeviceFlowWorker_start")
        try:
            flow = self.graph.connect_device_flow()
            log_trace("worker", "DeviceFlowWorker_success", has_code=bool(flow.get("code")))
            self.success.emit(flow)
        except Exception as e:
            log_trace("worker", "DeviceFlowWorker_error", error_excerpt=str(e)[:500])
            self.error.emit(str(e))


class RootLoadWorker(QThread):
    success = Signal(dict)
    error = Signal(dict)

    def __init__(self, graph, panel_key, drive_id, context):
        super().__init__()
        self.graph = graph
        self.panel_key = panel_key
        self.drive_id = drive_id
        self.context = context

    def run(self):
        log_trace(
            "worker",
            "RootLoadWorker_start",
            panel_key=self.panel_key,
            drive_id_suffix=str(self.drive_id)[-16:],
            cache_only=bool((self.context or {}).get("cache_only")),
        )
        try:
            items = self.graph.list_drive_root_items_normalized(self.drive_id, **self.context)
            log_trace(
                "worker",
                "RootLoadWorker_success",
                panel_key=self.panel_key,
                item_count=len(items or []),
            )
            self.success.emit({
                "panel_key": self.panel_key,
                "drive_id": self.drive_id,
                "items": items,
            })
        except Exception as e:
            log_trace("worker", "RootLoadWorker_error", panel_key=self.panel_key, error_excerpt=str(e)[:500])
            self.error.emit({
                "panel_key": self.panel_key,
                "drive_id": self.drive_id,
                "error": str(e),
            })


class FolderLoadWorker(QThread):
    success = Signal(dict)
    error = Signal(dict)

    def __init__(self, graph, panel_key, drive_id, item_id, context):
        super().__init__()
        self.graph = graph
        self.panel_key = panel_key
        self.drive_id = drive_id
        self.item_id = item_id
        self.context = context

    def run(self):
        log_trace(
            "worker",
            "FolderLoadWorker_start",
            panel_key=self.panel_key,
            drive_id_suffix=str(self.drive_id)[-16:],
            item_id_suffix=str(self.item_id)[-16:],
            cache_only=bool((self.context or {}).get("cache_only")),
        )
        try:
            items = self.graph.list_drive_item_children_normalized(self.drive_id, self.item_id, **self.context)
            log_trace(
                "worker",
                "FolderLoadWorker_success",
                panel_key=self.panel_key,
                item_count=len(items or []),
            )
            self.success.emit({
                "panel_key": self.panel_key,
                "drive_id": self.drive_id,
                "item_id": self.item_id,
                "items": items,
            })
        except Exception as e:
            log_trace("worker", "FolderLoadWorker_error", panel_key=self.panel_key, error_excerpt=str(e)[:500])
            self.error.emit({
                "panel_key": self.panel_key,
                "drive_id": self.drive_id,
                "item_id": self.item_id,
                "error": str(e),
            })


class DestinationPlanningTreeWidget(QTreeWidget):
    proposedBranchMoveRequested = Signal(object, object)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._dragged_item = None

    def startDrag(self, supported_actions):
        self._dragged_item = self.currentItem()
        try:
            super().startDrag(supported_actions)
        finally:
            self._dragged_item = None

    def dropEvent(self, event):
        source_item = self._dragged_item
        target_item = self.itemAt(event.position().toPoint())
        if source_item is None or target_item is None or source_item is target_item:
            event.ignore()
            return
        self.proposedBranchMoveRequested.emit(source_item, target_item)
        event.setDropAction(Qt.MoveAction)
        event.accept()
        self._dragged_item = None


class FullCountWorker(QThread):
    success = Signal(dict)
    error = Signal(dict)

    def __init__(self, graph, drive_id):
        super().__init__()
        self.graph = graph
        self.drive_id = drive_id

    def run(self):
        log_trace("worker", "FullCountWorker_start", drive_id_suffix=str(self.drive_id)[-16:])
        try:
            file_count, folder_count = self.graph.count_drive_items_recursive_split(self.drive_id)
            total_count = file_count + folder_count
            log_trace(
                "worker",
                "FullCountWorker_success",
                total_count=total_count,
                file_count=file_count,
                folder_count=folder_count,
            )
            self.success.emit({
                "drive_id": self.drive_id,
                "total_count": total_count,
                "file_count": file_count,
                "folder_count": folder_count,
            })
        except Exception as e:
            log_trace("worker", "FullCountWorker_error", error_excerpt=str(e)[:500])
            self.error.emit({
                "drive_id": self.drive_id,
                "error": str(e),
            })


class FilePreviewWorker(QThread):
    success = Signal(dict)
    error = Signal(dict)

    def __init__(self, graph, drive_id, item_id, *, item_name="", max_bytes=262144):
        super().__init__()
        self.graph = graph
        self.drive_id = drive_id
        self.item_id = item_id
        self.item_name = item_name
        self.max_bytes = max_bytes

    def run(self):
        log_trace(
            "worker",
            "FilePreviewWorker_start",
            drive_id_suffix=str(self.drive_id)[-16:],
            item_id_suffix=str(self.item_id)[-16:],
            max_bytes=self.max_bytes,
        )
        try:
            content = self.graph.download_drive_item_content(
                self.drive_id,
                self.item_id,
                max_bytes=self.max_bytes,
            )
            log_trace("worker", "FilePreviewWorker_success", bytes_len=len(content or b""))
            self.success.emit({
                "drive_id": self.drive_id,
                "item_id": self.item_id,
                "item_name": self.item_name,
                "content": content,
            })
        except Exception as e:
            log_trace("worker", "FilePreviewWorker_error", error_excerpt=str(e)[:500])
            self.error.emit({
                "drive_id": self.drive_id,
                "item_id": self.item_id,
                "item_name": self.item_name,
                "error": str(e),
            })


class DestinationFullTreeWorker(QThread):
    success = Signal(dict)
    error = Signal(dict)

    def __init__(self, graph, drive_id, context):
        super().__init__()
        self.graph = graph
        self.drive_id = drive_id
        self.context = context

    def run(self):
        log_trace("worker", "DestinationFullTreeWorker_start", drive_id_suffix=str(self.drive_id)[-16:])
        try:
            items = self.graph.list_drive_all_items_normalized(self.drive_id, **self.context)
            log_trace("worker", "DestinationFullTreeWorker_success", item_count=len(items or []))
            self.success.emit({
                "drive_id": self.drive_id,
                "items": items,
            })
        except Exception as e:
            log_trace("worker", "DestinationFullTreeWorker_error", error_excerpt=str(e)[:500])
            self.error.emit({
                "drive_id": self.drive_id,
                "error": str(e),
            })


class CacheRefreshWorker(QThread):
    success = Signal(dict)
    error = Signal(str)

    def __init__(self, graph, drive_ids):
        super().__init__()
        self.graph = graph
        self.drive_ids = [str(drive_id or "").strip() for drive_id in drive_ids if str(drive_id or "").strip()]

    def run(self):
        log_trace("worker", "CacheRefreshWorker_start", drive_id_count=len(self.drive_ids))
        try:
            for drive_id in self.drive_ids:
                self.graph.clear_drive_children_cache(drive_id)
            log_trace("worker", "CacheRefreshWorker_success", drive_ids_cleared=len(self.drive_ids))
            self.success.emit({"drive_ids": list(self.drive_ids)})
        except Exception as e:
            log_trace("worker", "CacheRefreshWorker_error", error_excerpt=str(e)[:500])
            self.error.emit(str(e))


class DriveDeltaSyncWorker(QThread):
    success = Signal(dict)
    error = Signal(str)

    def __init__(self, graph, drive_id: str, *, allow_initial_bootstrap: bool = True):
        super().__init__()
        self.graph = graph
        self.drive_id = str(drive_id or "").strip()
        self.allow_initial_bootstrap = bool(allow_initial_bootstrap)

    def run(self):
        log_trace("worker", "DriveDeltaSyncWorker_start", drive_id_suffix=self.drive_id[-16:])
        try:
            result = self.graph.sync_drive_children_delta(
                self.drive_id,
                allow_initial_bootstrap=self.allow_initial_bootstrap,
            )
            log_trace(
                "worker",
                "DriveDeltaSyncWorker_complete",
                skipped=bool(result.get("skipped")),
                ok=bool(result.get("ok", True)),
                pages=int(result.get("pages") or 0),
                invalidated_folders=int(result.get("invalidated_folders") or 0),
                initial_token_run=bool(result.get("initial_token_run")),
            )
            if result.get("ok", True):
                self.success.emit(result)
            else:
                self.error.emit(str(result.get("error") or result.get("reason") or "delta_sync_failed"))
        except Exception as e:
            log_trace("worker", "DriveDeltaSyncWorker_error", error_excerpt=str(e)[:500])
            self.error.emit(str(e))


class SessionKeepAliveWorker(QThread):
    success = Signal(dict)
    error = Signal(str)

    def __init__(self, graph):
        super().__init__()
        self.graph = graph

    def run(self):
        log_trace("worker", "SessionKeepAliveWorker_tick")
        try:
            refreshed = self.graph.refresh_access_token_silently(force_refresh=False)
            log_trace("worker", "SessionKeepAliveWorker_result", refreshed=bool(refreshed))
            self.success.emit({"refreshed": bool(refreshed)})
        except Exception as e:
            log_trace("worker", "SessionKeepAliveWorker_error", error_excerpt=str(e)[:500])
            self.error.emit(str(e))


class DeviceFlowPromptDialog(QDialog):
    cancel_requested = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Microsoft 365 Sign-In")
        self.setModal(False)
        self.setMinimumWidth(560)
        self.setObjectName("DeviceFlowPromptDialog")
        self._wait_seconds = 0
        self._wait_timer = QTimer(self)
        self._wait_timer.setInterval(1000)
        self._wait_timer.timeout.connect(self._tick_wait_counter)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 18, 20, 18)
        layout.setSpacing(10)

        self.title_label = QLabel("Microsoft 365 sign-in")
        self.title_label.setObjectName("SectionTitle")
        self.title_label.setWordWrap(True)

        self.message_primary_label = QLabel("")
        self.message_primary_label.setObjectName("CardBody")
        self.message_primary_label.setWordWrap(True)

        self.message_emphasis_label = QLabel("")
        self.message_emphasis_label.setObjectName("CardBody")
        self.message_emphasis_label.setWordWrap(True)

        self.message_secondary_label = QLabel("")
        self.message_secondary_label.setObjectName("CardBody")
        self.message_secondary_label.setWordWrap(True)

        self.status_label = QLabel("")
        self.status_label.setObjectName("MutedText")
        self.status_label.setWordWrap(True)
        self._pulse_phase = 0

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.addStretch()

        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.setMinimumWidth(84)

        self.cancel_button.clicked.connect(self._on_cancel_clicked)

        actions.addWidget(self.cancel_button)

        layout.addWidget(self.title_label)
        layout.addWidget(self.message_primary_label)
        layout.addWidget(self.message_emphasis_label)
        layout.addWidget(self.message_secondary_label)
        layout.addWidget(self.status_label)
        layout.addLayout(actions)

    def _on_cancel_clicked(self):
        self.cancel_requested.emit()

    def _tick_wait_counter(self):
        self._wait_seconds += 1
        self.status_label.setText(f"Waiting for Microsoft device code... {self._wait_seconds}s")
        self._pulse_phase += 1
        self._apply_pulse_styles()

    def _apply_pulse_styles(self):
        red_palette = ["#FF9DA0", "#FF858A", "#FFB3B5"]
        green_palette = ["#6DE7A0", "#59D88F", "#8DF0B8"]
        self.message_emphasis_label.setStyleSheet(
            f"color:{red_palette[self._pulse_phase % len(red_palette)]}; font-weight:700; font-size:11pt;"
        )
        self.message_secondary_label.setStyleSheet(
            f"color:{green_palette[self._pulse_phase % len(green_palette)]}; font-weight:700; font-size:11pt;"
        )
        self.status_label.setStyleSheet("color:#B7C9EE; font-weight:600; font-size:11pt;")

    def set_prompt_state(self, entered_email="", *, stage="waiting"):
        if entered_email:
            self.title_label.setText(f"Microsoft 365 sign-in for {entered_email}")
        else:
            self.title_label.setText("Microsoft 365 sign-in")

        if stage == "opened":
            self._wait_timer.stop()
            self.message_primary_label.setText("")
            self.message_emphasis_label.setText("")
            self.message_secondary_label.setText("")
            self.status_label.setText(
                "Microsoft sign-in opened. Your device code is copied to the clipboard. "
                "Press Ctrl+V in the browser sign-in page."
            )
            self.status_label.setStyleSheet("color:#59D88F; font-weight:700; font-size:11pt;")
            self.cancel_button.hide()
        elif stage == "ready":
            self._wait_timer.stop()
            self.message_primary_label.setText("")
            self.message_emphasis_label.setText("")
            self.message_secondary_label.setText("")
            self.status_label.setText(
                "Microsoft sign-in opened. Your device code is copied to the clipboard. "
                "Press Ctrl+V in the browser sign-in page."
            )
            self.status_label.setStyleSheet("color:#59D88F; font-weight:700; font-size:11pt;")
            self.cancel_button.show()
        else:
            self.message_primary_label.setText(
                "We are waiting to receive your Microsoft device authentication code."
            )
            self.message_emphasis_label.setText(
                "We will open Microsoft sign-in automatically."
            )
            self.message_secondary_label.setText(
                "Your device code will be copied to the clipboard. Then press Ctrl+V in the browser sign-in page."
            )
            self._wait_seconds = 0
            self._pulse_phase = 0
            self.status_label.setText("Waiting for Microsoft device code... 0s")
            self._apply_pulse_styles()
            if not self._wait_timer.isActive():
                self._wait_timer.start()
            self.cancel_button.show()


class ArrowComboBox(QComboBox):
    def paintEvent(self, event):
        super().paintEvent(event)

        arrow_box_width = 28
        rect = self.rect().adjusted(self.width() - arrow_box_width, 0, -1, -1)
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setPen(QColor("#2C456F"))
        painter.drawLine(rect.left(), 4, rect.left(), rect.bottom() - 4)
        painter.setBrush(QColor("#EAF0FF"))
        painter.setPen(Qt.NoPen)
        center_x = rect.center().x()
        center_y = rect.center().y() + 1
        painter.drawPolygon(QPolygon([
            QPoint(center_x - 5, center_y - 2),
            QPoint(center_x + 5, center_y - 2),
            QPoint(center_x, center_y + 4),
        ]))
        painter.end()


class SourceTreeRelationshipDelegate(QStyledItemDelegate):
    def __init__(self, window, parent=None):
        super().__init__(parent)
        self.window = window

    def paint(self, painter, option, index):
        node_data = index.data(Qt.UserRole) or {}
        relationship = self.window.get_source_relationship_display(node_data)
        if relationship["mode"] == "none":
            super().paint(painter, option, index)
            return

        custom_option = QStyleOptionViewItem(option)
        self.initStyleOption(custom_option, index)
        base_text = self.window.get_source_item_display_name(node_data, custom_option.text)
        suffix_text = relationship["suffix"]
        custom_option.text = ""

        style = custom_option.widget.style() if custom_option.widget else QApplication.style()
        style.drawControl(QStyle.CE_ItemViewItem, custom_option, painter, custom_option.widget)

        text_rect = style.subElementRect(QStyle.SE_ItemViewItemText, custom_option, custom_option.widget)
        if not text_rect.isValid():
            text_rect = custom_option.rect.adjusted(4, 0, -4, 0)

        painter.save()
        painter.setClipRect(text_rect)

        base_color = QColor("#FFFFFF")
        suffix_color = QColor("#59D88F") if relationship["mode"] == "direct" else QColor("#79B7FF")
        if custom_option.state & QStyle.State_Selected:
            suffix_color = QColor("#C8F7DA") if relationship["mode"] == "direct" else QColor("#D6E8FF")

        metrics = custom_option.fontMetrics
        spacing = 8
        drawn_base = base_text
        drawn_base_width = metrics.horizontalAdvance(drawn_base)
        drawn_suffix = suffix_text

        painter.setPen(base_color)
        painter.drawText(text_rect, Qt.AlignVCenter | Qt.AlignLeft, drawn_base)

        suffix_x = text_rect.x() + drawn_base_width + spacing
        suffix_rect = QRect(suffix_x, text_rect.y(), max(text_rect.right() - suffix_x, 0), text_rect.height())
        painter.setPen(suffix_color)
        painter.drawText(suffix_rect, Qt.AlignVCenter | Qt.AlignLeft, drawn_suffix)
        painter.restore()


class ManifestRunWorker(QThread):
    """Runs transfer_job_runner on a manifest in a background thread."""

    finished_ok = Signal(object)
    failed = Signal(str)

    def __init__(self, manifest: dict, *, dry_run: bool, log_file: str):
        super().__init__()
        self._manifest = manifest
        self._dry_run = dry_run
        self._log_file = log_file

    def run(self):
        try:
            from ozlink_console.transfer_job_runner import run_manifest_local_filesystem

            result = run_manifest_local_filesystem(
                self._manifest,
                dry_run=self._dry_run,
                log_file=self._log_file,
            )
            self.finished_ok.emit(result)
        except Exception as exc:
            self.failed.emit(str(exc))


class MainWindow(QMainWindow):
    @staticmethod
    def _source_tree_model_view_effective() -> bool:
        """True when the source panel should use QTreeView + model (v2 path).

        Precedence: ``OZLINK_SOURCE_QTREEVIEW`` forces on/off when set to a known value;
        otherwise the saved Settings checkbox (``ui/source_use_qtreeview``) applies.
        """
        env = os.environ.get("OZLINK_SOURCE_QTREEVIEW", "").strip().lower()
        if env in ("0", "false", "no"):
            return False
        if env in ("1", "true", "yes"):
            return True
        return QSettings().value("ui/source_use_qtreeview", False, type=bool)

    def __init__(self):
        super().__init__()

        self._base_window_title = "Ozlink IT – SharePoint File Relocation Console"
        self.setWindowTitle(self._base_window_title)
        self.setWindowFlags(
            Qt.Window
            | Qt.WindowTitleHint
            | Qt.WindowSystemMenuHint
            | Qt.WindowMinimizeButtonHint
            | Qt.WindowMaximizeButtonHint
            | Qt.WindowCloseButtonHint
        )
        self.resize(1600, 980)

        self.graph = GraphClient()
        self.worker = None
        self.device_flow_worker = None
        self.device_flow_prompt_dialog = None
        self._device_flow_prompt_ready = False
        self.discovery_worker = None
        self.root_load_workers = {}
        self.root_load_retired_workers = {}
        self.folder_load_workers = {}
        self.folder_load_retired_workers = {}
        self.cache_refresh_worker = None
        self.session_keepalive_worker = None
        self._cache_refresh_restore_active = False
        self._pending_cache_refresh_ui_state = None
        self._pending_cache_refresh_panels = set()
        self._cache_refresh_skip_expanded_restore_panels = set()
        self._pending_cache_refresh_tree_snapshots = {}
        self._pending_session_workspace_ui_state = None
        self._pending_session_tree_snapshots = {}
        self._pending_snapshot_branch_refresh = {"source": set(), "destination": set()}
        self._snapshot_branch_refresh_scheduled = {"source": False, "destination": False}
        self._snapshot_branch_refresh_baseline_by_worker = {}
        self._runtime_session_tree_snapshots = {"source": [], "destination": []}
        self._workspace_ui_persist_timer = QTimer(self)
        self._workspace_ui_persist_timer.setSingleShot(True)
        self._workspace_ui_persist_timer.timeout.connect(self._on_workspace_ui_persist_timer)
        self._workspace_ui_snapshot_dirty_panels = set()
        self._pending_session_workspace_restore_panels = set()
        self._pending_workspace_post_expand_selection = {"source": "", "destination": ""}
        self._tree_snapshot_restore_generation = {"source": 0, "destination": 0}
        self._worker_sequence = 0
        self.active_root_request_signatures = {"source": None, "destination": None}
        self.loaded_root_request_signatures = {"source": None, "destination": None}
        self._startup_geometry_applied = False
        self._startup_post_show_logged = False
        self._was_maximized_before_login = False
        self.memory_manager = None
        self.active_draft_session_id = ""
        self._draft_shell_state = SessionState()
        self._draft_shell_raw = {}
        self._memory_restore_candidate = None
        self._restore_payload_source = ""
        self._restore_selected_candidate_path = ""
        self._memory_restore_in_progress = False
        self._memory_restore_background_trees = False
        self._memory_restore_complete = False
        self._login_in_progress = False
        self._login_error_seen = False
        self._memory_ui_rebind_in_progress = False
        self._suppress_selector_change_handlers = False
        self._restore_destination_overlay_pending = False
        self._root_tree_bind_in_progress = False
        self._skip_root_bind_body_for_isolation = False
        self._suppress_autosave = True
        self._restored_allocation_count = 0
        self._restored_proposed_count = 0
        self._cached_loaded_source_items = 0
        self._source_restore_materialization_queue = []
        self._source_restore_materialization_seen = set()
        self._source_background_preload_pending = False
        self._source_background_preload_queue = []
        self._source_background_preload_seen = set()
        self._restore_queue_tick_delay_ms = 45
        self._restore_queue_initial_delay_ms = 400
        self._live_root_refresh_scheduled = {"source": False, "destination": False}
        self._live_root_refresh_request_signature = {"source": "", "destination": ""}
        self._live_root_refresh_ui_state = {"source": None, "destination": None}
        self._source_projection_refresh_pending = False
        self._destination_future_model_pending_after_source_restore = False
        self._destination_future_model_last_blocked_source_restore = False
        self._destination_future_bind_generation = 0
        self._destination_chunked_bind_state = None
        self._destination_snapshot_chunked_restore_active = False
        self._destination_future_bind_sync_active = False
        self._pending_destination_library_root = None
        self._source_projection_refresh_scheduled = False
        self._source_projection_refresh_context = ("", "")
        self._source_projection_refresh_paths = set()
        self._pending_source_navigation = None
        self._pending_destination_navigation = None
        self._destination_restore_materialization_queue = []
        self._destination_restore_materialization_seen = set()
        self._destination_restore_materialization_user_paused = False
        self._destination_preserved_children_by_worker = {}
        self._destination_real_tree_snapshot = []
        self._destination_full_tree_snapshot = []
        self._destination_full_tree_worker = None
        self._destination_full_tree_requested_drive_id = ""
        self._destination_full_tree_materialization_pending = False
        self._sharepoint_lazy_mode = True
        self._source_tree_model_view = MainWindow._source_tree_model_view_effective()
        self.source_sharepoint_model = None
        self._submitted_visual_cache = {
            "source_keys": {},
            "source_paths": {},
            "source_ancestors": [],
            "destination_keys": {},
            "destination_paths": {},
            "proposed_paths": {},
        }
        self._destination_full_tree_completed_drive_id = ""
        self._destination_full_tree_sequence = 0
        self._active_destination_full_tree_worker_id = 0
        self._retired_destination_full_tree_workers = {}
        self._destination_cut_buffer = None
        self._destination_full_tree_materialize_timer = QTimer(self)
        self._destination_full_tree_materialize_timer.setSingleShot(True)
        self._destination_full_tree_materialize_timer.timeout.connect(
            self._maybe_materialize_destination_full_tree_snapshot
        )
        self._destination_idle_materialize_pending_reason = ""
        self._destination_idle_materialize_timer = QTimer(self)
        self._destination_idle_materialize_timer.setSingleShot(True)
        self._destination_idle_materialize_timer.timeout.connect(self._run_deferred_destination_materialization)
        self._lazy_destination_projection_pending_reason = ""
        self._lazy_destination_projection_timer = QTimer(self)
        self._lazy_destination_projection_timer.setSingleShot(True)
        self._lazy_destination_projection_timer.timeout.connect(self._run_lazy_destination_projection_refresh)
        self._destination_future_projection_async_state = None
        self._destination_future_projection_timer = QTimer(self)
        self._destination_future_projection_timer.setSingleShot(True)
        self._destination_future_projection_timer.timeout.connect(self._run_destination_future_projection_chunk)
        self._expand_all_pending = {"source": False, "destination": False}
        self._expand_all_queue = {"source": deque(), "destination": deque()}
        self._expand_all_source_model_queue = deque()
        self._expand_all_seen = {"source": set(), "destination": set()}
        self._expand_all_processed_seen = {"source": set(), "destination": set()}
        self._expand_all_requeue_attempts = {"source": {}, "destination": {}}
        self._expand_all_requeue_limit = {"source": 3, "destination": 8}
        self._expand_all_processed = {"source": 0, "destination": 0}
        self._expand_all_max_per_tick = {"source": 2, "destination": 1}
        self._expand_all_deferred_refresh = {"source": False, "destination": False}
        self._source_column_refresh_pending = False
        self._source_expand_all_folder_load_log_counter = 0
        self._expand_all_status_last_update_ms = {"source": 0, "destination": 0}
        self._destination_expand_all_after_full_tree = False
        self._workspace_restore_expanded_all_intent = {"source": False, "destination": False}
        self.unresolved_proposed_by_parent_path = {}
        self.unresolved_allocations_by_parent_path = {}
        self.proposed_folders = []
        self.request_store = RequestStore()
        self.full_source_item_count = None
        self.full_source_file_count = None
        self.full_source_folder_count = None
        self.full_count_worker = None
        self._full_count_error_message = ""
        self._full_count_requested_drive_id = ""
        self._full_count_completed_drive_id = ""
        self._full_count_sequence = 0
        self._active_full_count_worker_id = 0
        self._retired_full_count_workers = {}
        self._workflow_not_planned_rows = []
        self._workflow_suggestion_rows = []
        self._workflow_needs_review_rows = []
        self._submission_test_mode = False
        self._pending_login_email = ""
        self._pending_login_restore_args = None
        self._auth_attempt_sequence = 0
        self._active_auth_attempt_id = 0
        self.pending_root_drive_ids = {"source": "", "destination": ""}
        self.pending_folder_loads = {"source": set(), "destination": set()}
        self.current_profile = None
        self.discovered_sites = []
        self.planned_moves = []
        self._manifest_run_worker = None
        self._execution_manifest_path = ""
        self._execution_manifest = None
        self._current_details_node_data = None
        self._current_details_panel_key = ""
        self._current_details_context = None
        self._preview_text_cache = {}
        self._preview_request_sequence = 0
        self._active_preview_request_id = 0
        self._preview_worker = None
        self._retired_preview_workers = {}
        self._progress_summary_refresh_timer = QTimer(self)
        self._progress_summary_refresh_timer.setSingleShot(True)
        self._progress_summary_refresh_timer.timeout.connect(self.update_progress_summaries)
        self._deferred_planning_refresh_timer = QTimer(self)
        self._deferred_planning_refresh_timer.setSingleShot(True)
        self._deferred_planning_refresh_timer.timeout.connect(self._run_deferred_planning_refresh)
        self._deferred_planning_refresh_pending = False
        self._deferred_planning_refresh_reasons = []
        self._drive_delta_workers = {}
        self._deferred_source_projection_paths = set()
        self._loading_visual_phase = 0
        self._loading_visual_timer = QTimer(self)
        self._loading_visual_timer.setInterval(1100)
        self._loading_visual_timer.timeout.connect(self._animate_loading_visuals)
        self._session_keepalive_timer = QTimer(self)
        self._session_keepalive_timer.setSingleShot(False)
        self._session_keepalive_timer.setInterval(15 * 60 * 1000)
        self._session_keepalive_timer.timeout.connect(self._run_session_keepalive)
        self._source_background_preload_timer = QTimer(self)
        self._source_background_preload_timer.setSingleShot(True)
        self._source_background_preload_timer.timeout.connect(self._process_source_background_preload_queue)
        self._deferred_background_load_timers = {}
        self._deferred_background_load_targets = {"source": "", "destination": ""}
        self._restore_finalization_deferred_active = False
        self._restore_finalization_deferred_reason = ""
        self.current_session_context = {
            "connected": False,
            "user_role": "user",
            "operator_display_name": "",
            "operator_upn": "",
            "tenant_domain": "",
            "discovered_sites": [],
        }

        self.nav_buttons = {}
        self.page_map = {}
        self.nav_allowed_by_role = {
            "user": ["Dashboard", "Planning Workspace", "Settings", "Requests"],
            "admin": ["Dashboard", "Planning Workspace", "Settings", "Audit", "Execution", "Requests"],
        }

        self._apply_theme()

        self.central = QWidget()
        self.central.setObjectName("Root")
        self.setCentralWidget(self.central)

        self.root_layout = QVBoxLayout(self.central)
        self.root_layout.setContentsMargins(0, 0, 0, 0)
        self.root_layout.setSpacing(0)

        self.build_top_bar()
        self.build_main_area()
        self.build_bottom_status_bar()
        self._expand_all_timers = {}
        for panel_key in ("source", "destination"):
            timer = QTimer(self)
            timer.setSingleShot(True)
            timer.timeout.connect(lambda panel_key=panel_key: self._process_expand_all_queue(panel_key))
            self._expand_all_timers[panel_key] = timer
            background_timer = QTimer(self)
            background_timer.setSingleShot(True)
            background_timer.timeout.connect(lambda panel_key=panel_key: self._run_deferred_background_load(panel_key))
            self._deferred_background_load_timers[panel_key] = background_timer
        self._load_saved_window_preferences()

        self.switch_page("Dashboard")
        self.apply_role_visibility("user")
        self.update_session_state(False)
        log_info("MainWindow build marker.", build_marker="main_window_refresh_restore_v2")

    def _schedule_progress_summary_refresh(self, delay_ms: int = 180):
        if not hasattr(self, "_progress_summary_refresh_timer") or self._progress_summary_refresh_timer is None:
            self.update_progress_summaries()
            return
        if getattr(self, "_sharepoint_lazy_mode", False) and self._planning_workspace_is_busy():
            delay_ms = max(delay_ms, 2500)
        self._progress_summary_refresh_timer.start(max(0, int(delay_ms)))

    def _planning_workspace_is_busy(self):
        pending_source = bool(self.pending_folder_loads.get("source"))
        pending_destination = bool(self.pending_folder_loads.get("destination"))
        return bool(
            self._root_tree_bind_in_progress
            or self._expand_all_pending.get("source")
            or self._expand_all_pending.get("destination")
            or pending_source
            or pending_destination
            or getattr(self, "_lazy_destination_projection_pending_reason", "")
        )

    def _start_session_keepalive(self):
        timer = getattr(self, "_session_keepalive_timer", None)
        if timer is None or not self.current_session_context.get("connected"):
            return
        if not timer.isActive():
            timer.start()
        log_info("Session keepalive started.", interval_ms=timer.interval())

    def _stop_session_keepalive(self):
        timer = getattr(self, "_session_keepalive_timer", None)
        if timer is not None and timer.isActive():
            timer.stop()
        worker = getattr(self, "session_keepalive_worker", None)
        if worker is not None and worker.isRunning():
            worker.requestInterruption()
        self.session_keepalive_worker = None
        log_info("Session keepalive stopped.")

    def _run_session_keepalive(self):
        try:
            if not self.current_session_context.get("connected"):
                self._stop_session_keepalive()
                return
            if self.session_keepalive_worker and self.session_keepalive_worker.isRunning():
                return
            if self._login_in_progress or (self.worker and self.worker.isRunning()) or (self.device_flow_worker and self.device_flow_worker.isRunning()):
                return
            self.session_keepalive_worker = SessionKeepAliveWorker(self.graph)
            self.session_keepalive_worker.success.connect(
                lambda payload: self._safe_invoke("session_keepalive.success", self.on_session_keepalive_success, payload)
            )
            self.session_keepalive_worker.error.connect(
                lambda error: self._safe_invoke("session_keepalive.error", self.on_session_keepalive_error, error)
            )
            self.session_keepalive_worker.finished.connect(
                lambda: self._safe_invoke("session_keepalive.finished", self.on_session_keepalive_finished)
            )
            self.session_keepalive_worker.start()
        except Exception as exc:
            self._log_restore_exception("run_session_keepalive", exc)

    def on_session_keepalive_success(self, payload):
        refreshed = bool((payload or {}).get("refreshed"))
        log_info("Session keepalive completed.", refreshed=refreshed)

    def on_session_keepalive_error(self, error):
        log_warn("Session keepalive failed.", error=str(error or ""))

    def on_session_keepalive_finished(self):
        self.session_keepalive_worker = None

    def _schedule_deferred_background_load(self, panel_key, drive_id):
        if getattr(self, "_sharepoint_lazy_mode", False):
            self._deferred_background_load_targets[panel_key] = str(drive_id or "")
            if panel_key == "destination" and drive_id:
                self.start_destination_full_tree_worker(drive_id)
            elif panel_key == "source" and drive_id:
                self._schedule_full_count_with_restore_backoff(drive_id)
            return
        timer = self._deferred_background_load_timers.get(panel_key)
        if timer is None:
            if panel_key == "source":
                self._schedule_full_count_with_restore_backoff(drive_id)
            elif panel_key == "destination":
                self.start_destination_full_tree_worker(drive_id)
            return

        self._deferred_background_load_targets[panel_key] = str(drive_id or "")
        timer.stop()
        timer.start(1500)

    def _run_deferred_background_load(self, panel_key):
        if getattr(self, "_sharepoint_lazy_mode", False):
            return
        drive_id = self._deferred_background_load_targets.get(panel_key, "")
        if not drive_id:
            return
        if panel_key == "source":
            self._schedule_full_count_with_restore_backoff(drive_id)
        elif panel_key == "destination":
            self.start_destination_full_tree_worker(drive_id)

    def _refresh_tree_column_width(self, panel_key):
        tree = self.source_tree_widget if panel_key == "source" else self.destination_tree_widget
        if tree is None:
            return
        try:
            tree.resizeColumnToContents(0)
        except Exception:
            return

    def _reset_source_background_preload_state(self):
        self._source_background_preload_pending = False
        self._source_background_preload_queue = []
        self._source_background_preload_seen = set()
        timer = getattr(self, "_source_background_preload_timer", None)
        if timer is not None:
            timer.stop()

    def _queue_source_background_preload_item(self, item):
        if item is None:
            return
        node_data = item.data(0, Qt.UserRole) or {}
        if node_data.get("placeholder") or not node_data.get("is_folder"):
            return
        item_key = id(item)
        if item_key in self._source_background_preload_seen:
            return
        self._source_background_preload_seen.add(item_key)
        self._source_background_preload_queue.append(item)

    def _schedule_source_background_preload(self, delay_ms=10):
        timer = getattr(self, "_source_background_preload_timer", None)
        if timer is not None and not timer.isActive():
            timer.start(max(0, int(delay_ms)))

    def _start_source_background_preload(self):
        if not getattr(self, "_sharepoint_lazy_mode", False):
            return
        tree = getattr(self, "source_tree_widget", None)
        if tree is None:
            return
        self._source_background_preload_pending = True
        self._source_background_preload_queue = []
        self._source_background_preload_seen = set()
        for index in range(tree.topLevelItemCount()):
            self._queue_source_background_preload_item(tree.topLevelItem(index))
        self._schedule_source_background_preload(delay_ms=0)

    def _continue_source_background_preload(self, item=None):
        if not self._source_background_preload_pending:
            return
        if item is not None:
            for index in range(item.childCount()):
                self._queue_source_background_preload_item(item.child(index))
        self._schedule_source_background_preload(delay_ms=0)

    def _process_source_background_preload_queue(self):
        if not self._source_background_preload_pending:
            return
        if self._expand_all_pending.get("source"):
            self._reset_source_background_preload_state()
            return

        tree = getattr(self, "source_tree_widget", None)
        if tree is None:
            self._reset_source_background_preload_state()
            return

        processed = 0
        max_per_tick = 2
        waiting_for_async_load = False

        while self._source_background_preload_queue and processed < max_per_tick:
            item = self._source_background_preload_queue.pop(0)
            if item is None:
                continue
            node_data = item.data(0, Qt.UserRole) or {}
            if node_data.get("placeholder") or not node_data.get("is_folder"):
                continue

            if bool(node_data.get("children_loaded")):
                for index in range(item.childCount()):
                    self._queue_source_background_preload_item(item.child(index))
                processed += 1
                continue

            if self._ensure_tree_item_load_started("source", item):
                waiting_for_async_load = True
                break
            processed += 1

        if waiting_for_async_load or self.pending_folder_loads.get("source"):
            self._schedule_source_background_preload(delay_ms=25)
            return

        if self._source_background_preload_queue:
            self._schedule_source_background_preload(delay_ms=10)
            return

        self._source_background_preload_pending = False

    def _apply_theme(self):
        self.setStyleSheet("""
            QWidget#Root {
                background-color: #05070B;
                color: #EAF0FF;
            }

            QWidget {
                background-color: transparent;
                color: #EAF0FF;
                font-family: Segoe UI;
                font-size: 10pt;
            }

            QFrame#TopBar {
                background-color: #000000;
                border-bottom: 1px solid #1B2C54;
            }

            QFrame#LeftNav {
                background-color: #000000;
                border-right: 1px solid #10203E;
            }

            QFrame#ContentArea {
                background-color: #05070B;
            }

            QFrame#BottomStatusBar {
                background-color: #000000;
                border-top: 1px solid #10203E;
            }

            QFrame#PageCard {
                background-color: #05070B;
                border: 1px solid #536B9F;
            }

            QFrame#InfoBanner {
                background-color: #042C08;
                border: 1px solid #0F7A20;
            }

            QFrame#SectionBox {
                background-color: #05070B;
                border: 1px solid #536B9F;
            }

            QFrame#HeroCard {
                background-color: #07101F;
                border: 1px solid #32548F;
            }

            QFrame#InsightCard {
                background-color: #08101D;
                border: 1px solid #284471;
            }

            QFrame#TopMetaCard {
                background-color: #07101F;
                border: 1px solid #27406B;
            }

            QFrame#MetricCard {
                background-color: #091224;
                border: 1px solid #284471;
            }

            QFrame#SoftBanner {
                background-color: #0A1730;
                border: 1px solid #21457D;
            }

            QFrame#TreeSurface {
                background-color: #071225;
                border: 1px solid #20355E;
            }

            QFrame#TabSurface {
                background-color: #07101F;
                border: 1px solid #284471;
            }

            QLabel#AppTitle {
                font-size: 20pt;
                font-weight: 700;
                color: #F3F7FF;
            }

            QLabel#AppSubtitle {
                font-size: 11pt;
                color: #2E8BFF;
                font-weight: 600;
            }

            QLabel#NavHeader {
                font-size: 18px;
                font-weight: 700;
                color: #EAF0FF;
            }

            QLabel#NavSubHeader {
                font-size: 11px;
                color: #7E92C5;
            }

            QLabel#CardTitle {
                font-size: 22pt;
                font-weight: 700;
                color: #F3F7FF;
            }

            QLabel#CardBody {
                font-size: 11pt;
                color: #A9B8DF;
            }

            QLabel#HeaderEyebrow {
                font-size: 10pt;
                font-weight: 700;
                color: #78A8FF;
                text-transform: uppercase;
            }

            QLabel#SectionTitle {
                font-size: 12pt;
                font-weight: 700;
                color: #EAF0FF;
            }

            QLabel#StatusGood {
                color: #66D97A;
                font-weight: 700;
            }

            QLabel#StatusBad {
                color: #FF5D5D;
                font-weight: 700;
            }

            QLabel#MutedText {
                color: #8CA0D2;
            }

            QLabel#FooterText {
                color: #7E92C5;
                font-size: 10pt;
            }

            QLabel#SummaryValue {
                font-size: 20pt;
                font-weight: 700;
                color: #F3F7FF;
            }

            QLabel#SummaryLabel {
                color: #8CA0D2;
                font-size: 10pt;
                text-transform: uppercase;
            }

            QLabel#ContextText {
                color: #B7C7EF;
                font-size: 10.5pt;
            }

            QLabel#DetailLabel {
                color: #9EB2E3;
                font-size: 10pt;
                font-weight: 600;
            }

            QLabel#DetailValue {
                color: #F3F7FF;
                font-size: 10.5pt;
            }

            QLabel#TopMetaLabel {
                color: #90A6D8;
                font-size: 9.5pt;
            }

            QLabel#TopMetaValue {
                color: #F3F7FF;
                font-size: 10.5pt;
                font-weight: 600;
            }

            QLabel#PanelSubtitle {
                color: #8CA0D2;
                font-size: 10pt;
            }

            QLabel#MetricValue {
                font-size: 22pt;
                font-weight: 700;
                color: #F3F7FF;
            }

            QLabel#MetricLabel {
                color: #9EB2E3;
                font-size: 10pt;
                font-weight: 600;
            }

            QPushButton {
                background-color: #08101D;
                border: 1px solid #233B6C;
                color: #EAF0FF;
                padding: 10px 16px;
                min-height: 18px;
            }

            QPushButton:hover {
                background-color: #0F1930;
                border: 1px solid #2E8BFF;
            }

            QPushButton:pressed {
                background-color: #132242;
            }

            QPushButton#PrimaryButton {
                background-color: #2E6DFF;
                border: 1px solid #5F95FF;
                color: white;
                font-weight: 700;
            }

            QPushButton#PrimaryButton:hover {
                background-color: #3B7CFF;
            }

            QPushButton#PanelToggleButton {
                background-color: #FFB020;
                border: 1px solid #FFD27A;
                color: #091224;
                font-weight: 800;
                padding: 8px 14px;
            }

            QPushButton#PanelToggleButton:hover {
                background-color: #FFC44D;
                border: 1px solid #FFE2A6;
            }

            QPushButton#NavButton {
                text-align: left;
                padding: 12px 18px;
                font-size: 12pt;
                border: 1px solid transparent;
                background-color: transparent;
            }

            QPushButton#NavButton:hover {
                background-color: #0A1631;
                border-left: 4px solid #2E8BFF;
            }

            QPushButton#NavButtonActive {
                text-align: left;
                padding: 12px 18px;
                font-size: 12pt;
                color: white;
                background-color: #0F57FF;
                border: 1px solid #2C6FFF;
            }

            QLineEdit {
                background-color: #02060C;
                border: 1px solid #2C456F;
                color: #EAF0FF;
                padding: 8px 10px;
                min-height: 20px;
            }

            QLineEdit:focus {
                border: 1px solid #2E8BFF;
            }

            QComboBox {
                background-color: #08101D;
                border: 1px solid #2C456F;
                color: #F3F7FF;
                padding: 6px 36px 6px 10px;
                min-height: 22px;
                selection-background-color: #1849B7;
                selection-color: #F8FBFF;
            }

            QComboBox:focus {
                border: 1px solid #2E8BFF;
            }

            QComboBox::drop-down {
                subcontrol-origin: padding;
                subcontrol-position: top right;
                border-left: 1px solid #2C456F;
                width: 28px;
                background-color: #0C1527;
            }

            QComboBox::down-arrow {
                image: none;
                width: 12px;
                height: 12px;
            }

            QComboBox QAbstractItemView {
                background-color: #08101D;
                border: 1px solid #2C456F;
                color: #F3F7FF;
                selection-background-color: #1849B7;
                selection-color: #FFFFFF;
                outline: 0;
            }

            QMenu {
                background-color: #08101D;
                color: #FFFFFF;
                border: 1px solid #365A97;
                padding: 6px 2px;
                opacity: 1;
            }

            QMenu::item {
                background-color: #08101D;
                color: #FFFFFF;
                padding: 8px 24px 8px 14px;
                margin: 1px 4px;
            }

            QMenu::item:selected {
                background-color: #20499E;
                color: #FFFFFF;
                border: 1px solid #5F95FF;
            }

            QMenu::item:disabled {
                background-color: #08101D;
                color: #B2C4E8;
            }

            QMenu::separator {
                height: 1px;
                background-color: #20355E;
                margin: 6px 10px;
            }

            QTreeWidget, QTreeView {
                background-color: #040914;
                border: 1px solid #20355E;
                color: #EAF0FF;
                padding: 4px;
            }

            QScrollBar:horizontal {
                background-color: #07101F;
                height: 16px;
                margin: 0px 16px 0px 16px;
                border: 1px solid #20355E;
            }

            QScrollBar::handle:horizontal {
                background-color: #3B82F6;
                min-width: 40px;
                border-radius: 4px;
                border: 1px solid #93C5FD;
            }

            QScrollBar::handle:horizontal:hover {
                background-color: #5F95FF;
                border: 1px solid #BFDBFE;
            }

            QScrollBar::handle:horizontal:pressed {
                background-color: #2E6DFF;
            }

            QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
                background-color: #0C1831;
                width: 16px;
                subcontrol-origin: margin;
            }

            QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal {
                background: none;
            }

            QScrollBar:vertical {
                background-color: #07101F;
                width: 16px;
                margin: 16px 0px 16px 0px;
                border: 1px solid #20355E;
            }

            QScrollBar::handle:vertical {
                background-color: #3B82F6;
                min-height: 48px;
                border-radius: 4px;
                border: 1px solid #93C5FD;
            }

            QScrollBar::handle:vertical:hover {
                background-color: #5F95FF;
                border: 1px solid #BFDBFE;
            }

            QScrollBar::handle:vertical:pressed {
                background-color: #2E6DFF;
            }

            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                background-color: #0C1831;
                height: 16px;
                subcontrol-origin: margin;
            }

            QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
                background: none;
            }

            QTreeWidget::item, QTreeView::item {
                padding: 6px 4px;
            }

            QTreeWidget::item:selected, QTreeView::item:selected {
                background-color: #163C8F;
                color: #FFFFFF;
            }

            QTreeWidget::item:hover, QTreeView::item:hover {
                background-color: #0C1831;
            }

            QTableWidget {
                background-color: #040914;
                border: 1px solid #20355E;
                color: #EAF0FF;
                gridline-color: #183055;
            }

            QHeaderView::section {
                background-color: #0C1527;
                color: #F3F7FF;
                padding: 8px 6px;
                border: none;
                border-right: 1px solid #183055;
                border-bottom: 1px solid #183055;
                font-weight: 700;
            }

            QTableWidget::item {
                padding: 6px;
            }

            QTableWidget::item:selected {
                background-color: #163C8F;
                color: #FFFFFF;
            }

            QTextEdit#DetailsNotes {
                background-color: #040914;
                border: 1px solid #20355E;
                color: #EAF0FF;
                padding: 8px;
            }

            QTabWidget::pane {
                border: 1px solid #284471;
                background-color: #07101F;
                top: -1px;
            }

            QTabBar::tab {
                background-color: #091224;
                color: #AFC0E8;
                padding: 8px 14px;
                border: 1px solid #21385F;
                margin-right: 4px;
            }

            QTabBar::tab:selected {
                background-color: #12356F;
                color: #FFFFFF;
                border-color: #2E6DFF;
            }
        """)

    def build_top_bar(self):
        self.top_bar = QFrame()
        self.top_bar.setObjectName("TopBar")
        self.top_bar.setFixedHeight(92)

        top_layout = QHBoxLayout(self.top_bar)
        top_layout.setContentsMargins(16, 12, 20, 12)
        top_layout.setSpacing(18)

        brand_wrap = QHBoxLayout()
        brand_wrap.setSpacing(14)

        self.logo_block = QFrame()
        self.logo_block.setFixedSize(34, 34)
        self.logo_block.setStyleSheet("background-color:#2E6DFF; border:1px solid #5F95FF;")

        brand_text_layout = QVBoxLayout()
        brand_text_layout.setContentsMargins(0, 0, 0, 0)
        brand_text_layout.setSpacing(2)

        self.app_title = QLabel("Ozlink IT – SharePoint File Relocation Console")
        self.app_title.setObjectName("AppTitle")

        self.app_subtitle = QLabel("Connect")
        self.app_subtitle.setObjectName("AppSubtitle")

        brand_text_layout.addWidget(self.app_title)
        brand_text_layout.addWidget(self.app_subtitle)

        brand_wrap.addWidget(self.logo_block)
        brand_wrap.addLayout(brand_text_layout)
        brand_wrap.addStretch()

        right_wrap = QHBoxLayout()
        right_wrap.setContentsMargins(0, 0, 0, 0)
        right_wrap.setSpacing(8)

        meta_card = QFrame()
        meta_card.setObjectName("TopMetaCard")
        meta_layout = QGridLayout(meta_card)
        meta_layout.setContentsMargins(10, 6, 10, 6)
        meta_layout.setHorizontalSpacing(10)
        meta_layout.setVerticalSpacing(1)

        operator_caption = QLabel("Operator")
        operator_caption.setObjectName("TopMetaLabel")
        self.operator_label = QLabel("Not Signed In")
        self.operator_label.setObjectName("TopMetaValue")

        tenant_caption = QLabel("Tenant")
        tenant_caption.setObjectName("TopMetaLabel")
        self.top_tenant_label = QLabel("Not Connected")
        self.top_tenant_label.setObjectName("TopMetaValue")

        meta_layout.addWidget(operator_caption, 0, 0)
        meta_layout.addWidget(self.operator_label, 0, 1)
        meta_layout.addWidget(tenant_caption, 1, 0)
        meta_layout.addWidget(self.top_tenant_label, 1, 1)
        meta_layout.setColumnStretch(1, 1)

        self.top_connect_btn = QPushButton("Connect to Microsoft 365")
        self.top_connect_btn.setObjectName("PrimaryButton")
        self.top_connect_btn.setFixedWidth(170)
        self.top_connect_btn.setMinimumHeight(26)
        self.top_connect_btn.clicked.connect(self.handle_connect)

        self.session_badge = QLabel("■ Session: Not Connected")
        self.session_badge.setObjectName("StatusBad")
        self.session_badge.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)

        status_wrap = QVBoxLayout()
        status_wrap.setContentsMargins(0, 0, 0, 0)
        status_wrap.setSpacing(4)
        status_wrap.addWidget(self.session_badge)
        status_wrap.addWidget(self.top_connect_btn, 0, Qt.AlignRight)

        right_wrap.addWidget(meta_card, 1)
        right_wrap.addLayout(status_wrap)

        top_layout.addLayout(brand_wrap, 1)
        top_layout.addLayout(right_wrap, 0)

        self.root_layout.addWidget(self.top_bar)

    def build_main_area(self):
        self.main_wrap = QHBoxLayout()
        self.main_wrap.setContentsMargins(0, 0, 0, 0)
        self.main_wrap.setSpacing(0)

        self.build_left_nav()
        self.build_content_area()

        self.root_layout.addLayout(self.main_wrap, 1)

    def build_left_nav(self):
        self.left_nav = QFrame()
        self.left_nav.setObjectName("LeftNav")
        self.left_nav.setFixedWidth(235)

        nav_layout = QVBoxLayout(self.left_nav)
        nav_layout.setContentsMargins(18, 18, 18, 18)
        nav_layout.setSpacing(14)

        nav_header = QLabel("Navigation")
        nav_header.setObjectName("NavHeader")

        nav_subheader = QLabel("Client Planning Workspace")
        nav_subheader.setObjectName("NavSubHeader")

        nav_layout.addWidget(nav_header)
        nav_layout.addWidget(nav_subheader)

        divider = QFrame()
        divider.setFixedHeight(1)
        divider.setStyleSheet("background-color:#0C2312; border:none;")
        nav_layout.addWidget(divider)

        buttons = [
            "Dashboard",
            "Planning Workspace",
            "Settings",
            "Audit",
            "Execution",
            "Requests"
        ]

        for name in buttons:
            btn = QPushButton(name)
            btn.setObjectName("NavButton")
            btn.setCursor(Qt.PointingHandCursor)
            btn.clicked.connect(lambda _, n=name: self.switch_page(n))
            nav_layout.addWidget(btn)
            self.nav_buttons[name] = btn

        nav_layout.addStretch()

        bottom_divider = QFrame()
        bottom_divider.setFixedHeight(1)
        bottom_divider.setStyleSheet("background-color:#0C2312; border:none;")
        nav_layout.addWidget(bottom_divider)

        self.mode_label = QLabel("Client-facing planning mode")
        self.mode_label.setObjectName("FooterText")
        nav_layout.addWidget(self.mode_label)

        self.main_wrap.addWidget(self.left_nav)

    def build_content_area(self):
        self.content_area = QFrame()
        self.content_area.setObjectName("ContentArea")
        self.content_area.setMinimumWidth(0)
        self.content_area.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        content_layout = QVBoxLayout(self.content_area)
        content_layout.setContentsMargins(24, 24, 24, 24)
        content_layout.setSpacing(0)

        self.pages = QStackedWidget()
        self.pages.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        self.dashboard_page = self.build_dashboard_page()
        self.planning_page = self.build_planning_page()
        self.settings_page = self.build_settings_page()
        self.audit_page = self.build_placeholder_page("Audit")
        self.execution_page = self.build_execution_page()
        self.requests_page = self.build_requests_page()

        page_defs = {
            "Dashboard": self.dashboard_page,
            "Planning Workspace": self.planning_page,
            "Settings": self.settings_page,
            "Audit": self.audit_page,
            "Execution": self.execution_page,
            "Requests": self.requests_page,
        }

        for name, widget in page_defs.items():
            self.pages.addWidget(widget)
            self.page_map[name] = widget

        content_layout.addWidget(self.pages)
        self.main_wrap.addWidget(self.content_area, 1)

    def build_dashboard_page(self):
        page = QWidget()
        outer = QVBoxLayout(page)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(12)

        self.dashboard_stack = QStackedWidget()

        connect_view = QWidget()
        connect_layout = QVBoxLayout(connect_view)
        connect_layout.setContentsMargins(0, 0, 0, 0)
        connect_layout.setSpacing(0)

        connect_shell = QFrame()
        connect_shell.setObjectName("HeroCard")
        connect_shell_layout = QVBoxLayout(connect_shell)
        connect_shell_layout.setContentsMargins(0, 0, 0, 0)
        connect_shell_layout.setSpacing(0)

        connect_intro = QFrame()
        connect_intro_layout = QHBoxLayout(connect_intro)
        connect_intro_layout.setContentsMargins(28, 28, 28, 28)
        connect_intro_layout.setSpacing(18)

        connect_icon = QFrame()
        connect_icon.setFixedSize(46, 46)
        connect_icon.setStyleSheet("background-color:#2E6DFF; border:1px solid #5F95FF;")

        connect_intro_text = QVBoxLayout()
        connect_intro_text.setContentsMargins(0, 0, 0, 0)
        connect_intro_text.setSpacing(8)

        connect_title = QLabel("Connect to Microsoft 365")
        connect_title.setObjectName("CardTitle")
        connect_body = QLabel(
            "Enter your work email, then continue to the Microsoft 365 sign-in experience. After sign-in, the console will "
            "automatically load all valid SharePoint sites it can discover for this session."
        )
        connect_body.setObjectName("CardBody")
        connect_body.setWordWrap(True)

        connect_intro_text.addWidget(connect_title)
        connect_intro_text.addWidget(connect_body)
        connect_intro_text.addStretch()

        connect_intro_layout.addWidget(connect_icon, 0, Qt.AlignTop)
        connect_intro_layout.addLayout(connect_intro_text, 1)

        connect_info_wrap = QFrame()
        connect_info_wrap_layout = QVBoxLayout(connect_info_wrap)
        connect_info_wrap_layout.setContentsMargins(28, 16, 28, 16)
        connect_info_wrap_layout.setSpacing(0)

        connect_info_banner = QFrame()
        connect_info_banner.setObjectName("SoftBanner")
        connect_info_banner_layout = QVBoxLayout(connect_info_banner)
        connect_info_banner_layout.setContentsMargins(18, 14, 18, 14)
        connect_info_banner_layout.setSpacing(4)
        connect_info_text = QLabel(
            "Ozlink IT will handle execution separately. Your role here is to review the content and help map where it should go."
        )
        connect_info_text.setObjectName("CardBody")
        connect_info_text.setWordWrap(True)
        connect_info_banner_layout.addWidget(connect_info_text)
        connect_info_wrap_layout.addWidget(connect_info_banner)

        connect_form = QFrame()
        connect_form_layout = QVBoxLayout(connect_form)
        connect_form_layout.setContentsMargins(28, 24, 28, 24)
        connect_form_layout.setSpacing(14)

        email_title = QLabel("Work email")
        email_title.setObjectName("SectionTitle")

        self.work_email_input = QLineEdit()
        self.work_email_input.setPlaceholderText("name@company.com")
        self.work_email_input.setFixedWidth(460)
        self.work_email_input.returnPressed.connect(self.handle_connect)

        email_help = QLabel("Use the same Microsoft 365 account you normally use for work.")
        email_help.setObjectName("ContextText")
        email_help.setWordWrap(True)

        loading_title = QLabel("SharePoint site loading")
        loading_title.setObjectName("SectionTitle")
        loading_body = QLabel(
            "After sign-in, the console will automatically discover accessible SharePoint sites, validate them, and load all sites "
            "that return usable document libraries. You will then choose source and destination directly from the Planning Workspace pickers."
        )
        loading_body.setObjectName("CardBody")
        loading_body.setWordWrap(True)

        loading_body_2 = QLabel(
            "This build opens the planning session without collecting a password in the application. The Planning Workspace pickers will "
            "be populated automatically after discovery completes."
        )
        loading_body_2.setObjectName("CardBody")
        loading_body_2.setWordWrap(True)

        self.dashboard_status = QLabel("Ready to open your planning session.")
        self.dashboard_status.setObjectName("FooterText")
        self.dashboard_status.setWordWrap(True)

        connect_action_row = QHBoxLayout()
        connect_action_row.setContentsMargins(0, 12, 0, 0)
        connect_action_row.setSpacing(12)

        self.dashboard_connect_btn = QPushButton("Connect to Microsoft 365")
        self.dashboard_connect_btn.setObjectName("PrimaryButton")
        self.dashboard_connect_btn.setFixedWidth(235)
        self.dashboard_connect_btn.clicked.connect(self.handle_connect)

        self.dashboard_clear_btn = QPushButton("Clear")
        self.dashboard_clear_btn.setFixedWidth(140)
        self.dashboard_clear_btn.clicked.connect(self.clear_dashboard_fields)

        connect_action_row.addWidget(self.dashboard_connect_btn)
        connect_action_row.addWidget(self.dashboard_clear_btn)
        connect_action_row.addWidget(self.dashboard_status)
        connect_action_row.addStretch()

        connect_form_layout.addWidget(email_title)
        connect_form_layout.addWidget(self.work_email_input, 0, Qt.AlignLeft)
        connect_form_layout.addWidget(email_help)
        connect_form_layout.addSpacing(8)
        connect_form_layout.addWidget(loading_title)
        connect_form_layout.addWidget(loading_body)
        connect_form_layout.addWidget(loading_body_2)
        connect_form_layout.addStretch()
        connect_form_layout.addLayout(connect_action_row)

        connect_shell_layout.addWidget(connect_intro)
        connect_shell_layout.addWidget(connect_info_wrap)
        connect_shell_layout.addWidget(connect_form)
        connect_layout.addWidget(connect_shell)
        connect_layout.addStretch()

        dashboard_view = QWidget()
        dashboard_layout = QVBoxLayout(dashboard_view)
        dashboard_layout.setContentsMargins(0, 0, 0, 0)
        dashboard_layout.setSpacing(12)

        intro_panel = QFrame()
        intro_panel.setObjectName("HeroCard")
        intro_layout = QVBoxLayout(intro_panel)
        intro_layout.setContentsMargins(22, 18, 22, 18)
        intro_layout.setSpacing(10)

        intro_title = QLabel("Welcome to your migration planning workspace")
        intro_title.setObjectName("CardTitle")
        intro_body = QLabel(
            "Use this console to review your current SharePoint content, choose where items should live, and send your planning request "
            "to Ozlink IT. This screen is designed for planning only and does not move files."
        )
        intro_body.setObjectName("CardBody")
        intro_body.setWordWrap(True)

        self.dashboard_next_step = QLabel(
            "Your planning session is connected, but the source and destination context are not loaded yet. Please contact Ozlink IT if this does not update."
        )
        self.dashboard_next_step.setObjectName("ContextText")
        self.dashboard_next_step.setWordWrap(True)

        intro_layout.addWidget(intro_title)
        intro_layout.addWidget(intro_body)
        intro_layout.addSpacing(8)
        intro_layout.addWidget(self.dashboard_next_step)

        cards_row = QHBoxLayout()
        cards_row.setContentsMargins(0, 0, 0, 0)
        cards_row.setSpacing(14)

        session_card = QFrame()
        session_card.setObjectName("SectionBox")
        session_layout = QVBoxLayout(session_card)
        session_layout.setContentsMargins(20, 18, 20, 18)
        session_layout.setSpacing(10)

        session_title = QLabel("Current session")
        session_title.setObjectName("SectionTitle")
        session_body = QLabel(
            "Choose the source and destination locations for this planning session. Then continue to the Planning Workspace to review items and decide where they should go."
        )
        session_body.setObjectName("CardBody")
        session_body.setWordWrap(True)

        self.dashboard_session_state = QLabel("Not connected")
        self.dashboard_session_state.setObjectName("ContextText")
        self.dashboard_session_mode = QLabel("Mode: Client")
        self.dashboard_session_mode.setObjectName("ContextText")
        self.dashboard_source_summary = QLabel("Source: Not set")
        self.dashboard_source_summary.setObjectName("ContextText")
        self.dashboard_destination_summary = QLabel("Destination: Not set")
        self.dashboard_destination_summary.setObjectName("ContextText")
        self.dashboard_session_operator = QLabel("Operator: Not signed in")
        self.dashboard_session_operator.setObjectName("ContextText")
        self.dashboard_session_tenant = QLabel("Tenant: Not connected")
        self.dashboard_session_tenant.setObjectName("ContextText")

        self.dashboard_connected_continue_btn = QPushButton("Continue Planning")
        self.dashboard_connected_continue_btn.setObjectName("PrimaryButton")
        self.dashboard_connected_continue_btn.setFixedWidth(170)
        self.dashboard_connected_continue_btn.clicked.connect(lambda: self.switch_page("Planning Workspace"))

        self.dashboard_connected_switch_btn = QPushButton("Sign In / Switch Account")
        self.dashboard_connected_switch_btn.setFixedWidth(180)
        self.dashboard_connected_switch_btn.clicked.connect(self.handle_connect)

        session_button_row = QHBoxLayout()
        session_button_row.setContentsMargins(0, 8, 0, 0)
        session_button_row.setSpacing(10)
        session_button_row.addWidget(self.dashboard_connected_continue_btn)
        session_button_row.addWidget(self.dashboard_connected_switch_btn)
        session_button_row.addStretch()

        session_layout.addWidget(session_title)
        session_layout.addWidget(session_body)
        session_layout.addSpacing(4)
        session_layout.addWidget(self.dashboard_session_state)
        session_layout.addWidget(self.dashboard_session_mode)
        session_layout.addWidget(self.dashboard_source_summary)
        session_layout.addWidget(self.dashboard_destination_summary)
        session_layout.addStretch()
        session_layout.addLayout(session_button_row)

        progress_card = QFrame()
        progress_card.setObjectName("SectionBox")
        progress_layout = QVBoxLayout(progress_card)
        progress_layout.setContentsMargins(20, 18, 20, 18)
        progress_layout.setSpacing(10)

        progress_title = QLabel("Planning progress")
        progress_title.setObjectName("SectionTitle")
        progress_body = QLabel("These numbers show how much of the migration plan has been mapped so far.")
        progress_body.setObjectName("CardBody")
        progress_body.setWordWrap(True)

        progress_grid = QGridLayout()
        progress_grid.setHorizontalSpacing(12)
        progress_grid.setVerticalSpacing(12)

        self.dashboard_loaded_items = QLabel("0")
        self.dashboard_loaded_items.setObjectName("SummaryValue")
        self.dashboard_graph_files = QLabel("0")
        self.dashboard_graph_files.setObjectName("SummaryValue")
        self.dashboard_graph_folders = QLabel("0")
        self.dashboard_graph_folders.setObjectName("SummaryValue")
        self.dashboard_planned_items = QLabel("0")
        self.dashboard_planned_items.setObjectName("SummaryValue")
        self.dashboard_not_planned_items = QLabel("0")
        self.dashboard_not_planned_items.setObjectName("SummaryValue")
        self.dashboard_needs_review_items = QLabel("0")
        self.dashboard_needs_review_items.setObjectName("SummaryValue")

        progress_pairs = [
            ("Rows in source tree", self.dashboard_loaded_items),
            ("Files (Graph)", self.dashboard_graph_files),
            ("Folders (Graph)", self.dashboard_graph_folders),
            ("Planned moves", self.dashboard_planned_items),
            ("Not yet planned", self.dashboard_not_planned_items),
            ("Needs review", self.dashboard_needs_review_items),
        ]
        self.dashboard_loaded_items.setToolTip(
            "Non-placeholder rows currently in the source tree widget: each file and each folder is one row. "
            "Collapsed branches still count. This is not the library-wide Graph total."
        )
        _graph_files_tip = (
            "Non-folder drive items under the source library root from Microsoft Graph (recursive count). "
            "Uses the same paged /children API as browsing; may reflect cached listings until refresh."
        )
        _graph_folders_tip = (
            "Folder drive items under the source library root from Microsoft Graph (recursive count). "
            "Files (Graph) + Folders (Graph) equals total items under the root."
        )
        self.dashboard_graph_files.setToolTip(_graph_files_tip)
        self.dashboard_graph_folders.setToolTip(_graph_folders_tip)

        for index, (label_text, value_label) in enumerate(progress_pairs):
            progress_grid.addWidget(self.build_metric_card(label_text, value_label), index // 2, index % 2)

        self.dashboard_open_workspace_btn = QPushButton("Open Planning Workspace")
        self.dashboard_open_workspace_btn.setFixedWidth(220)
        self.dashboard_open_workspace_btn.clicked.connect(lambda: self.switch_page("Planning Workspace"))

        progress_button_row = QHBoxLayout()
        progress_button_row.setContentsMargins(0, 4, 0, 0)
        progress_button_row.addStretch()
        progress_button_row.addWidget(self.dashboard_open_workspace_btn)

        progress_layout.addWidget(progress_title)
        progress_layout.addWidget(progress_body)
        progress_layout.addLayout(progress_grid)
        progress_layout.addLayout(progress_button_row)
        progress_layout.addStretch()

        cards_row.addWidget(session_card, 1)
        cards_row.addWidget(progress_card, 1)

        dashboard_layout.addWidget(intro_panel)
        dashboard_layout.addLayout(cards_row)
        dashboard_layout.addStretch()

        self.dashboard_stack.addWidget(connect_view)
        self.dashboard_stack.addWidget(dashboard_view)
        outer.addWidget(self.dashboard_stack)

        return page

    def build_planning_page(self):
        page = QWidget()
        outer = QVBoxLayout(page)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(3)

        header = QFrame()
        header.setObjectName("HeroCard")
        header_shell_layout = QVBoxLayout(header)
        header_shell_layout.setContentsMargins(9, 7, 9, 7)
        header_shell_layout.setSpacing(6)

        header_top_bar = QHBoxLayout()
        header_top_bar.setContentsMargins(0, 0, 0, 0)
        header_top_bar.setSpacing(8)

        title = QLabel("Planning Workspace")
        title.setObjectName("CardTitle")
        self.planning_header_toggle_button = QPushButton("Collapse Header")
        self.planning_header_toggle_button.setObjectName("PanelToggleButton")
        self.planning_header_toggle_button.setMinimumHeight(22)
        self.planning_header_toggle_button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.planning_header_toggle_button.clicked.connect(self.toggle_planning_header_collapsed)
        header_top_bar.addWidget(title, 0, Qt.AlignLeft | Qt.AlignVCenter)
        header_top_bar.addStretch()
        header_top_bar.addWidget(self.planning_header_toggle_button, 0, Qt.AlignRight | Qt.AlignVCenter)

        self.planning_header_content = QWidget()
        header_layout = QGridLayout(self.planning_header_content)
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.setHorizontalSpacing(8)
        header_layout.setVerticalSpacing(2)

        subtitle = QLabel(
            "Choose the source and destination libraries, review the folders you want to move, "
            "and build your planned move list for Ozlink IT."
        )
        subtitle.setObjectName("CardBody")
        subtitle.setWordWrap(True)
        subtitle.setMaximumHeight(28)
        subtitle.hide()

        self.workspace_loaded_items = QLabel("0")
        self.workspace_loaded_items.setObjectName("MetricValue")
        self.workspace_graph_files = QLabel("0")
        self.workspace_graph_files.setObjectName("MetricValue")
        self.workspace_graph_folders = QLabel("0")
        self.workspace_graph_folders.setObjectName("MetricValue")
        self.workspace_planned_items = QLabel("0")
        self.workspace_planned_items.setObjectName("MetricValue")
        self.workspace_not_planned_items = QLabel("0")
        self.workspace_not_planned_items.setObjectName("MetricValue")
        self.workspace_needs_review_items = QLabel("0")
        self.workspace_needs_review_items.setObjectName("MetricValue")

        filter_labels = [
            "Source Site", "Source Library",
            "Destination Site", "Destination Library"
        ]

        self.planning_inputs = {}

        from_card = QFrame()
        from_card.setObjectName("HeroCard")
        from_card.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        from_layout = QGridLayout(from_card)
        from_layout.setContentsMargins(0, 0, 0, 0)
        from_layout.setHorizontalSpacing(8)
        from_layout.setVerticalSpacing(2)

        to_card = QFrame()
        to_card.setObjectName("HeroCard")
        to_card.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        to_layout = QGridLayout(to_card)
        to_layout.setContentsMargins(0, 0, 0, 0)
        to_layout.setHorizontalSpacing(8)
        to_layout.setVerticalSpacing(2)

        for label_text in filter_labels:
            selector = ArrowComboBox()
            selector.setObjectName("PlanningSelector")
            selector.setMinimumWidth(220)
            selector.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            selector.setMinimumHeight(34)
            selector.setEnabled(False)
            selector.addItem("Not loaded yet")
            self.planning_inputs[label_text] = selector

        from_site_label = QLabel("Source Site")
        from_site_label.setObjectName("SummaryLabel")
        from_library_label = QLabel("Source Library")
        from_library_label.setObjectName("SummaryLabel")
        to_site_label = QLabel("Destination Site")
        to_site_label.setObjectName("SummaryLabel")
        to_library_label = QLabel("Destination Library")
        to_library_label.setObjectName("SummaryLabel")

        self.source_context_label = QLabel("Source: Not selected")
        self.source_context_label.setObjectName("ContextText")
        self.source_context_label.setWordWrap(False)
        self.destination_context_label = QLabel("Destination: Not selected")
        self.destination_context_label.setObjectName("ContextText")
        self.destination_context_label.setWordWrap(False)

        self.planning_loading_banner = QFrame()
        self.planning_loading_banner.setObjectName("SoftBanner")
        planning_loading_layout = QVBoxLayout(self.planning_loading_banner)
        planning_loading_layout.setContentsMargins(14, 10, 14, 10)
        planning_loading_layout.setSpacing(2)
        self.planning_loading_label = QLabel("Please wait while we load your SharePoint sites and libraries.")
        self.planning_loading_label.setObjectName("SectionTitle")
        self.planning_loading_loading_detail = QLabel("The workspace will finish populating source, destination, and planned move data automatically.")
        self.planning_loading_loading_detail.setObjectName("CardBody")
        self.planning_loading_loading_detail.setWordWrap(True)
        planning_loading_layout.addWidget(self.planning_loading_label)
        planning_loading_layout.addWidget(self.planning_loading_loading_detail)
        self.planning_loading_banner.hide()

        self.planning_inputs["Source Site"].currentIndexChanged.connect(
            lambda _: self.on_site_selector_changed("source")
        )
        self.planning_inputs["Destination Site"].currentIndexChanged.connect(
            lambda _: self.on_site_selector_changed("destination")
        )
        self.planning_inputs["Source Library"].currentIndexChanged.connect(
            lambda _: self.on_library_selector_changed("source")
        )
        self.planning_inputs["Destination Library"].currentIndexChanged.connect(
            lambda _: self.on_library_selector_changed("destination")
        )

        from_layout.addWidget(from_site_label, 0, 0)
        from_layout.addWidget(self.planning_inputs["Source Site"], 1, 0)
        from_layout.addWidget(from_library_label, 0, 1)
        from_layout.addWidget(self.planning_inputs["Source Library"], 1, 1)
        from_layout.setColumnStretch(0, 1)
        from_layout.setColumnStretch(1, 1)

        to_layout.addWidget(to_site_label, 0, 0)
        to_layout.addWidget(self.planning_inputs["Destination Site"], 1, 0)
        to_layout.addWidget(to_library_label, 0, 1)
        to_layout.addWidget(self.planning_inputs["Destination Library"], 1, 1)
        to_layout.setColumnStretch(0, 1)
        to_layout.setColumnStretch(1, 1)

        self.action_buttons = {}
        for text in [
            "Propose Folder",
            "Assign",
            "Unassign",
            "Import Draft",
            "Export Draft",
            "Unlock Draft",
            "Submit Request to Ozlink IT"
        ]:
            btn = QPushButton(text)
            if text == "Submit Request to Ozlink IT":
                btn.setObjectName("PrimaryButton")
                btn.setFixedWidth(220)
            elif text in ["Import Draft", "Export Draft", "Unlock Draft"]:
                btn.setFixedWidth(118)
                btn.setMinimumHeight(24)
            self.action_buttons[text] = btn

        self.action_buttons["Assign"].clicked.connect(self.handle_assign)
        self.action_buttons["Unassign"].clicked.connect(self.handle_unassign)
        self.action_buttons["Propose Folder"].clicked.connect(self.handle_new_proposed_folder)
        self.action_buttons["Import Draft"].clicked.connect(self._handle_import_draft)
        self.action_buttons["Export Draft"].clicked.connect(self._handle_export_draft)
        self.action_buttons["Unlock Draft"].clicked.connect(self._handle_unlock_draft)
        self.action_buttons["Submit Request to Ozlink IT"].clicked.connect(self._handle_submit_request)

        for key in ["Propose Folder", "Assign", "Unassign", "Import Draft", "Export Draft", "Unlock Draft"]:
            self.action_buttons[key].setObjectName("")

        counts_wrap = QHBoxLayout()
        counts_wrap.setContentsMargins(0, 0, 0, 0)
        counts_wrap.setSpacing(8)
        compact_metrics = [
            ("Rows in tree:", self.workspace_loaded_items),
            ("Files (Graph):", self.workspace_graph_files),
            ("Folders (Graph):", self.workspace_graph_folders),
            ("Planned:", self.workspace_planned_items),
            ("Not Yet Planned:", self.workspace_not_planned_items),
            ("Needs Review:", self.workspace_needs_review_items),
        ]
        self.workspace_loaded_items.setToolTip(
            "Source tree rows (excludes placeholders): one row per file and per folder currently materialized in the widget."
        )
        self.workspace_graph_files.setToolTip(
            "Graph recursive file count for the source library (non-folder drive items)."
        )
        self.workspace_graph_folders.setToolTip(
            "Graph recursive folder count for the source library. Sum with Files (Graph) for total items."
        )
        for index, (caption, value_label) in enumerate(compact_metrics):
            metric_text = QLabel(caption)
            metric_text.setObjectName("HeaderEyebrow")
            metric_value = value_label
            metric_value.setObjectName("ContextText")
            metric_value.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
            metric_pair = QLabel(f"{caption} ")
            metric_pair.setObjectName("HeaderEyebrow")
            metric_pair.hide()
            metric_widget = QWidget()
            metric_widget_layout = QHBoxLayout(metric_widget)
            metric_widget_layout.setContentsMargins(0, 0, 0, 0)
            metric_widget_layout.setSpacing(2)
            label_widget = QLabel(caption)
            label_widget.setObjectName("HeaderEyebrow")
            metric_widget_layout.addWidget(label_widget)
            metric_widget_layout.addWidget(metric_value)
            counts_wrap.addWidget(metric_widget)
            if index < len(compact_metrics) - 1:
                separator = QLabel("|")
                separator.setObjectName("HeaderEyebrow")
                counts_wrap.addWidget(separator)
        counts_wrap.addStretch()

        header_action_panel = QWidget()
        header_action_panel.setMaximumWidth(420)
        header_action_panel.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Maximum)
        header_action_wrap = QVBoxLayout(header_action_panel)
        header_action_wrap.setContentsMargins(0, 0, 0, 0)
        header_action_wrap.setSpacing(3)
        self.test_mode_toggle = QPushButton("Test Mode: Off")
        self.test_mode_toggle.setCheckable(True)
        self.test_mode_toggle.setChecked(False)
        self.test_mode_toggle.setMinimumHeight(24)
        self.test_mode_toggle.setMinimumWidth(110)
        self.test_mode_toggle.setSizePolicy(QSizePolicy.Maximum, QSizePolicy.Fixed)
        self.test_mode_toggle.setToolTip("Store a test submission without locking the live draft.")
        def _handle_test_mode_toggled(checked):
            self._submission_test_mode = bool(checked)
            self.test_mode_toggle.setText("Test Mode: On" if checked else "Test Mode: Off")
        self.test_mode_toggle.toggled.connect(_handle_test_mode_toggled)
        toggle_row = QHBoxLayout()
        toggle_row.setContentsMargins(0, 0, 0, 0)
        toggle_row.addStretch()
        toggle_row.addWidget(self.test_mode_toggle, 0, Qt.AlignRight)
        header_action_wrap.addLayout(toggle_row)
        header_action_wrap.addWidget(self.action_buttons["Submit Request to Ozlink IT"], 0, Qt.AlignRight)
        secondary_actions_row = QHBoxLayout()
        secondary_actions_row.setContentsMargins(0, 0, 0, 0)
        secondary_actions_row.setSpacing(4)
        secondary_actions_row.addWidget(self.action_buttons["Unlock Draft"])
        secondary_actions_row.addWidget(self.action_buttons["Export Draft"])
        secondary_actions_row.addWidget(self.action_buttons["Import Draft"])
        header_action_wrap.addLayout(secondary_actions_row)
        header_action_wrap.addStretch()

        selectors_row = QGridLayout()
        selectors_row.setContentsMargins(0, 0, 0, 0)
        selectors_row.setHorizontalSpacing(6)
        selectors_row.setVerticalSpacing(1)
        selectors_row.addWidget(from_site_label, 0, 0)
        selectors_row.addWidget(from_library_label, 0, 1)
        selectors_row.addWidget(to_site_label, 0, 2)
        selectors_row.addWidget(to_library_label, 0, 3)
        selectors_row.addWidget(self.planning_inputs["Source Site"], 1, 0)
        selectors_row.addWidget(self.planning_inputs["Source Library"], 1, 1)
        selectors_row.addWidget(self.planning_inputs["Destination Site"], 1, 2)
        selectors_row.addWidget(self.planning_inputs["Destination Library"], 1, 3)
        for col in range(4):
            selectors_row.setColumnStretch(col, 1)

        context_row = QGridLayout()
        context_row.setContentsMargins(0, 0, 0, 0)
        context_row.setHorizontalSpacing(12)
        context_row.setVerticalSpacing(0)
        context_row.addWidget(self.source_context_label, 0, 0)
        context_row.addWidget(self.destination_context_label, 0, 1)
        context_row.setColumnStretch(0, 1)
        context_row.setColumnStretch(1, 1)

        header_layout.addLayout(counts_wrap, 0, 0, 1, 3)
        header_layout.addWidget(header_action_panel, 0, 3, 3, 1, Qt.AlignTop | Qt.AlignRight)
        header_layout.addLayout(selectors_row, 1, 0, 1, 3)
        header_layout.addLayout(context_row, 2, 0, 1, 3)
        header_layout.addWidget(self.planning_loading_banner, 3, 0, 1, 4)
        header_layout.setColumnStretch(0, 2)
        header_layout.setColumnStretch(1, 2)
        header_layout.setColumnStretch(2, 2)
        header_layout.setColumnStretch(3, 1)

        self._planning_header_collapsed = False
        self._planning_header_expanded_min_height = 0

        header_shell_layout.addLayout(header_top_bar)
        header_shell_layout.addWidget(self.planning_header_content)
        self._planning_header_expanded_min_height = header.sizeHint().height()

        outer.addWidget(header)

        middle_grid = QGridLayout()
        middle_grid.setContentsMargins(0, 0, 0, 0)
        middle_grid.setHorizontalSpacing(6)
        middle_grid.setVerticalSpacing(6)

        self.source_tree_box, self.source_tree_widget, self.source_tree_status = self.build_tree_panel(
            "Source Content",
            "Review the current source folders here. Select a source library to load the current folder structure.",
            "source",
        )
        self.destination_tree_box, self.destination_tree_widget, self.destination_tree_status = self.build_tree_panel(
            "Destination Structure",
            "Review the destination folders here. Select a destination library to load the target folder structure.",
            "destination",
        )
        self.planned_moves_box, self.planned_moves_table, self.planned_moves_status = self.build_planned_moves_panel()
        self.details_box = self.build_details_panel()
        (
            self.suggestions_box,
            self.suggestions_table,
            self.suggestions_status,
            self.not_planned_table,
            self.not_planned_status,
        ) = self.build_suggestions_panel()
        self.needs_review_box, self.needs_review_table, self.needs_review_status = self.build_needs_review_panel()

        if getattr(self, "_source_tree_model_view", False):
            self.source_tree_widget.expanded.connect(self._on_source_sharepoint_model_expanded)
            self.source_tree_widget.collapsed.connect(self._on_source_sharepoint_model_collapsed)
            self.source_tree_widget.selectionModel().selectionChanged.connect(
                lambda *_args: self.on_tree_selection_changed("source")
            )
            self.source_tree_widget.setItemDelegate(SourceTreeRelationshipDelegate(self, self.source_tree_widget))
        else:
            self.source_tree_widget.itemExpanded.connect(
                lambda item: self.on_tree_item_expanded("source", item)
            )
            self.source_tree_widget.itemCollapsed.connect(self._on_source_tree_item_collapsed)
            self.source_tree_widget.itemSelectionChanged.connect(
                lambda: self.on_tree_selection_changed("source")
            )
            self.source_tree_widget.setItemDelegate(SourceTreeRelationshipDelegate(self, self.source_tree_widget))
        self.destination_tree_widget.itemExpanded.connect(
            lambda item: self.on_tree_item_expanded("destination", item)
        )
        self.destination_tree_widget.itemCollapsed.connect(self._on_destination_tree_item_collapsed)
        self.destination_tree_widget.itemSelectionChanged.connect(
            lambda: self.on_tree_selection_changed("destination")
        )
        self.destination_tree_widget.itemChanged.connect(self.on_destination_tree_item_changed)
        self.source_tree_widget.setContextMenuPolicy(Qt.CustomContextMenu)
        self.source_tree_widget.customContextMenuRequested.connect(self.show_source_context_menu)
        self.destination_tree_widget.setContextMenuPolicy(Qt.CustomContextMenu)
        self.destination_tree_widget.customContextMenuRequested.connect(self.show_destination_context_menu)
        self._inline_proposed_commit_item_id = ""

        middle_grid.addWidget(self.source_tree_box, 0, 0)
        middle_grid.addWidget(self.destination_tree_box, 0, 1)

        middle_grid.setColumnStretch(0, 1)
        middle_grid.setColumnStretch(1, 1)
        middle_grid.setRowStretch(0, 1)
        self.source_tree_box.setMinimumHeight(240)
        self.destination_tree_box.setMinimumHeight(240)
        self.source_tree_box.setMinimumWidth(0)
        self.destination_tree_box.setMinimumWidth(0)
        self.source_tree_box.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.destination_tree_box.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        middle_wrap = QWidget()
        middle_wrap.setLayout(middle_grid)
        middle_wrap.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        self.workspace_tabs = QTabWidget()
        self.workspace_tabs.addTab(self.planned_moves_box, "Planned Moves")
        self.workspace_tabs.addTab(self.suggestions_box, "Suggestions")
        self.workspace_tabs.addTab(self.needs_review_box, "Needs Review")
        self.workspace_tabs.addTab(self.details_box, "Selection Details")
        self._workspace_tabs_expanded_min_height = 240
        self._workspace_tabs_expanded_max_height = 340
        self._workspace_tabs_collapsed = False
        self.workspace_tabs.setMinimumHeight(240)
        self.workspace_tabs.setMaximumHeight(340)
        self.workspace_tabs.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.workspace_tabs_toggle_button = QPushButton("Collapse Panel")
        self.workspace_tabs_toggle_button.setObjectName("PanelToggleButton")
        self.workspace_tabs_toggle_button.setMinimumHeight(22)
        self.workspace_tabs_toggle_button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.workspace_tabs_toggle_button.clicked.connect(self.toggle_workspace_tabs_collapsed)
        self.workspace_tabs.setCornerWidget(self.workspace_tabs_toggle_button, Qt.TopRightCorner)
        self.workspace_tabs.currentChanged.connect(self.on_workspace_tab_changed)
        outer.addWidget(middle_wrap, 1)
        outer.addWidget(self.workspace_tabs, 0)

        outer.setStretch(0, 0)
        outer.setStretch(1, 3)
        outer.setStretch(2, 3)

        return page

    def build_metric_card(self, label_text, value_label):
        card = QFrame()
        card.setObjectName("MetricCard")

        layout = QVBoxLayout(card)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(4)

        label = QLabel(label_text)
        label.setObjectName("MetricLabel")

        layout.addWidget(value_label)
        layout.addWidget(label)
        layout.addStretch()

        return card

    def build_panel_box(self, title_text, body_text):
        box = QFrame()
        box.setObjectName("SectionBox")
        layout = QVBoxLayout(box)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        title = QLabel(title_text)
        title.setObjectName("SectionTitle")

        body = QLabel(body_text)
        body.setObjectName("CardBody")
        body.setWordWrap(True)
        body.setAlignment(Qt.AlignCenter)

        filler = QWidget()
        filler_layout = QVBoxLayout(filler)
        filler_layout.addStretch()
        filler_layout.addWidget(body)
        filler_layout.addStretch()

        layout.addWidget(title)
        layout.addWidget(filler, 1)

        return box

    def build_workflow_table(self, column_labels):
        table = QTableWidget(0, len(column_labels))
        table.setHorizontalHeaderLabels(column_labels)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.setSelectionMode(QAbstractItemView.SingleSelection)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.verticalHeader().setVisible(False)
        table.horizontalHeader().setStretchLastSection(True)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        table.setAlternatingRowColors(False)
        return table

    def build_requests_page(self):
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        hero = QFrame()
        hero.setObjectName("HeroCard")
        hero_layout = QVBoxLayout(hero)
        hero_layout.setContentsMargins(18, 16, 18, 16)
        hero_layout.setSpacing(8)

        title = QLabel("Requests")
        title.setObjectName("CardTitle")
        body = QLabel(
            "Review submitted and test request batches created from the Planning Workspace. "
            "This page is read-only and shows exactly what has been packaged for Ozlink IT."
        )
        body.setObjectName("CardBody")
        body.setWordWrap(True)

        actions_row = QHBoxLayout()
        actions_row.setContentsMargins(0, 0, 0, 0)
        actions_row.setSpacing(10)
        self.requests_live_count = QLabel("0")
        self.requests_live_count.setObjectName("MetricValue")
        self.requests_test_count = QLabel("0")
        self.requests_test_count.setObjectName("MetricValue")
        self.requests_total_moves = QLabel("0")
        self.requests_total_moves.setObjectName("MetricValue")
        for caption, label in [
            ("Live Batches", self.requests_live_count),
            ("Test Batches", self.requests_test_count),
            ("Submitted Moves", self.requests_total_moves),
        ]:
            chip = QFrame()
            chip.setObjectName("TreeSurface")
            chip_layout = QVBoxLayout(chip)
            chip_layout.setContentsMargins(12, 8, 12, 8)
            chip_layout.setSpacing(2)
            chip_layout.addWidget(label)
            chip_label = QLabel(caption)
            chip_label.setObjectName("SummaryLabel")
            chip_layout.addWidget(chip_label)
            actions_row.addWidget(chip)
        actions_row.addStretch()
        self.requests_refresh_btn = QPushButton("Refresh Requests")
        self.requests_refresh_btn.clicked.connect(self.refresh_requests_page)
        actions_row.addWidget(self.requests_refresh_btn, 0, Qt.AlignRight)
        self.requests_export_zip_btn = QPushButton("Export Zip")
        self.requests_export_zip_btn.setEnabled(False)
        self.requests_export_zip_btn.clicked.connect(self.handle_export_request_zip)
        actions_row.addWidget(self.requests_export_zip_btn, 0, Qt.AlignRight)
        self.requests_email_btn = QPushButton("Email Ozlink IT")
        self.requests_email_btn.setEnabled(False)
        self.requests_email_btn.clicked.connect(self.handle_email_request_batch)
        actions_row.addWidget(self.requests_email_btn, 0, Qt.AlignRight)
        self.requests_delete_test_btn = QPushButton("Delete Test Batch")
        self.requests_delete_test_btn.setEnabled(False)
        self.requests_delete_test_btn.clicked.connect(self.handle_delete_test_request)
        actions_row.addWidget(self.requests_delete_test_btn, 0, Qt.AlignRight)

        hero_layout.addWidget(title)
        hero_layout.addWidget(body)
        hero_layout.addLayout(actions_row)

        content = QGridLayout()
        content.setContentsMargins(0, 0, 0, 0)
        content.setHorizontalSpacing(12)
        content.setVerticalSpacing(12)

        table_box = QFrame()
        table_box.setObjectName("SectionBox")
        table_layout = QVBoxLayout(table_box)
        table_layout.setContentsMargins(14, 14, 14, 14)
        table_layout.setSpacing(8)
        table_title = QLabel("Submission Batches")
        table_title.setObjectName("SectionTitle")
        self.requests_table = self.build_workflow_table([
            "Batch",
            "Type",
            "Status",
            "Submitted",
            "Moves",
            "Proposed",
            "Draft",
        ])
        self.requests_table.itemSelectionChanged.connect(self.on_requests_selection_changed)
        self.requests_status = QLabel("No submission batches yet.")
        self.requests_status.setObjectName("MutedText")
        self.requests_status.setWordWrap(True)
        table_layout.addWidget(table_title)
        table_layout.addWidget(self.requests_table, 1)
        table_layout.addWidget(self.requests_status)

        details_box = QFrame()
        details_box.setObjectName("SectionBox")
        details_layout = QVBoxLayout(details_box)
        details_layout.setContentsMargins(14, 14, 14, 14)
        details_layout.setSpacing(8)
        details_title = QLabel("Request Details")
        details_title.setObjectName("SectionTitle")
        self.requests_detail_text = QTextEdit()
        self.requests_detail_text.setReadOnly(True)
        self.requests_detail_text.setObjectName("DetailsNotes")
        self.requests_detail_text.setMinimumHeight(170)
        self.requests_detail_text.setPlainText("Select a request batch to review its packaged details.")
        details_layout.addWidget(details_title)
        details_layout.addWidget(self.requests_detail_text, 1)

        content.addWidget(table_box, 0, 0)
        content.addWidget(details_box, 0, 1)
        content.setColumnStretch(0, 3)
        content.setColumnStretch(1, 2)

        layout.addWidget(hero)
        layout.addLayout(content, 1)
        return page

    def build_suggestions_panel(self):
        box = QFrame()
        box.setObjectName("SectionBox")
        layout = QVBoxLayout(box)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        title = QLabel("Suggestions")
        title.setObjectName("SectionTitle")

        suggestions_heading = QLabel("Suggested next moves")
        suggestions_heading.setObjectName("SummaryLabel")
        suggestions_table = self.build_workflow_table([
            "Source Name",
            "Source Path",
            "Suggested Destination",
            "Confidence / Reason",
        ])
        suggestions_table.itemDoubleClicked.connect(
            lambda item: self.handle_workflow_source_row_activated(item, "suggestions")
        )
        suggestions_status = QLabel("Suggestions will appear as source content is reviewed.")
        suggestions_status.setObjectName("MutedText")
        suggestions_status.setWordWrap(True)

        not_planned_heading = QLabel("Not Yet Planned")
        not_planned_heading.setObjectName("SummaryLabel")
        not_planned_table = self.build_workflow_table([
            "Source Name",
            "Source Path",
            "Planning Status",
        ])
        not_planned_table.itemDoubleClicked.connect(
            lambda item: self.handle_workflow_source_row_activated(item, "not_yet_planned")
        )
        not_planned_status = QLabel("Unplanned visible source items will appear here.")
        not_planned_status.setObjectName("MutedText")
        not_planned_status.setWordWrap(True)

        layout.addWidget(title)
        layout.addWidget(suggestions_heading)
        layout.addWidget(suggestions_table, 1)
        layout.addWidget(suggestions_status)
        layout.addSpacing(8)
        layout.addWidget(not_planned_heading)
        layout.addWidget(not_planned_table, 1)
        layout.addWidget(not_planned_status)
        return box, suggestions_table, suggestions_status, not_planned_table, not_planned_status

    def build_needs_review_panel(self):
        box = QFrame()
        box.setObjectName("SectionBox")
        layout = QVBoxLayout(box)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        title = QLabel("Needs Review")
        title.setObjectName("SectionTitle")

        table = self.build_workflow_table([
            "Item",
            "Source Path",
            "Review Reason",
            "Suggested Action",
        ])
        table.itemDoubleClicked.connect(
            lambda item: self.handle_workflow_source_row_activated(item, "needs_review")
        )

        status = QLabel("Planning items that need review will appear here.")
        status.setObjectName("MutedText")
        status.setWordWrap(True)

        layout.addWidget(title)
        layout.addWidget(table, 1)
        layout.addWidget(status)
        return box, table, status

    def refresh_requests_page(self):
        if not hasattr(self, "requests_table"):
            return

        rows = self.request_store.list_submission_batches()
        self._request_batch_rows = rows
        self.requests_table.setRowCount(len(rows))
        live_count = 0
        test_count = 0
        total_moves = 0

        for row_index, row in enumerate(rows):
            live_count += 0 if row.get("is_test") else 1
            test_count += 1 if row.get("is_test") else 0
            total_moves += int(row.get("planned_move_count", 0) or 0)
            values = [
                row.get("batch_id", ""),
                "Test" if row.get("is_test") else "Live",
                row.get("status", ""),
                row.get("submitted_utc", ""),
                str(row.get("planned_move_count", 0)),
                str(row.get("proposed_folder_count", 0)),
                row.get("draft_name", "") or row.get("draft_id", ""),
            ]
            for column_index, value in enumerate(values):
                item = QTableWidgetItem(value)
                item.setData(Qt.UserRole, row_index)
                self.requests_table.setItem(row_index, column_index, item)

        self.requests_live_count.setText(str(live_count))
        self.requests_test_count.setText(str(test_count))
        self.requests_total_moves.setText(str(total_moves))

        if rows:
            self.requests_status.setText(f"{len(rows)} request batch(es) available.")
            if self.requests_table.currentRow() < 0:
                self.requests_table.selectRow(0)
                self.on_requests_selection_changed()
        else:
            self.requests_status.setText("No submission batches yet.")
            self.requests_detail_text.setPlainText("Select a request batch to review its packaged details.")
            if hasattr(self, "requests_export_zip_btn"):
                self.requests_export_zip_btn.setEnabled(False)
            if hasattr(self, "requests_email_btn"):
                self.requests_email_btn.setEnabled(False)
            if hasattr(self, "requests_delete_test_btn"):
                self.requests_delete_test_btn.setEnabled(False)

    def on_requests_selection_changed(self):
        if not hasattr(self, "requests_table"):
            return
        selected_ranges = self.requests_table.selectedRanges()
        if not selected_ranges:
            self.requests_detail_text.setPlainText("Select a request batch to review its packaged details.")
            if hasattr(self, "requests_export_zip_btn"):
                self.requests_export_zip_btn.setEnabled(False)
            if hasattr(self, "requests_email_btn"):
                self.requests_email_btn.setEnabled(False)
            if hasattr(self, "requests_delete_test_btn"):
                self.requests_delete_test_btn.setEnabled(False)
            return
        row_index = selected_ranges[0].topRow()
        if row_index < 0 or row_index >= len(getattr(self, "_request_batch_rows", [])):
            self.requests_detail_text.setPlainText("Select a request batch to review its packaged details.")
            if hasattr(self, "requests_export_zip_btn"):
                self.requests_export_zip_btn.setEnabled(False)
            if hasattr(self, "requests_email_btn"):
                self.requests_email_btn.setEnabled(False)
            if hasattr(self, "requests_delete_test_btn"):
                self.requests_delete_test_btn.setEnabled(False)
            return

        row = self._request_batch_rows[row_index]
        if hasattr(self, "requests_export_zip_btn"):
            self.requests_export_zip_btn.setEnabled(True)
        if hasattr(self, "requests_email_btn"):
            self.requests_email_btn.setEnabled(True)
        if hasattr(self, "requests_delete_test_btn"):
            self.requests_delete_test_btn.setEnabled(
                bool(self.current_session_context.get("user_role", "user") == "admin" and row.get("is_test"))
            )
        payload = self.request_store.load_submission_batch(
            row.get("batch_id", ""),
            test_mode=bool(row.get("is_test")),
        )
        default_zip = Path(str(payload.get("path", ""))).with_suffix(".zip") if payload.get("path") else ""
        request = payload.get("request", {}) if isinstance(payload.get("request", {}), dict) else {}
        allocations = payload.get("allocations", []) if isinstance(payload.get("allocations", []), list) else []
        proposed = payload.get("proposed_folders", []) if isinstance(payload.get("proposed_folders", []), list) else []

        lines = [
            f"Batch ID: {row.get('batch_id', '')}",
            f"Type: {'Test' if row.get('is_test') else 'Live'}",
            f"Status: {row.get('status', '')}",
            f"Submitted: {row.get('submitted_utc', '')}",
            f"Submitted By: {row.get('submitted_by', '')}",
            f"Tenant: {row.get('tenant_domain', '') or 'Not available'}",
            f"Draft: {row.get('draft_name', '') or row.get('draft_id', '') or 'Not available'}",
            f"Source: {row.get('source_site', '')} / {row.get('source_library', '')}",
            f"Destination: {row.get('destination_site', '')} / {row.get('destination_library', '')}",
            f"Stored At: {payload.get('path', '')}",
            f"Default Zip: {default_zip or 'Not created yet'}",
            "",
            f"Allocations: {len(allocations)}",
        ]
        for allocation in allocations[:12]:
            lines.append(
                f"- {allocation.get('SourceItemName', '')} -> {allocation.get('RequestedDestinationPath', '')}"
            )
        if len(allocations) > 12:
            lines.append(f"... and {len(allocations) - 12} more allocation(s)")

        lines.extend(["", f"Proposed Folders: {len(proposed)}"])
        for proposed_folder in proposed[:12]:
            lines.append(f"- {proposed_folder.get('DestinationPath', '')}")
        if len(proposed) > 12:
            lines.append(f"... and {len(proposed) - 12} more proposed folder(s)")

        warnings = request.get("ValidationWarnings", []) if isinstance(request.get("ValidationWarnings", []), list) else []
        if warnings:
            lines.extend(["", "Validation Warnings:"])
            lines.extend(f"- {warning}" for warning in warnings)

        self.requests_detail_text.setPlainText("\n".join(lines))

    def _selected_request_batch_row(self):
        selected_ranges = self.requests_table.selectedRanges() if hasattr(self, "requests_table") else []
        if not selected_ranges:
            return None
        row_index = selected_ranges[0].topRow()
        if row_index < 0 or row_index >= len(getattr(self, "_request_batch_rows", [])):
            return None
        return self._request_batch_rows[row_index]

    def handle_export_request_zip(self):
        row = self._selected_request_batch_row()
        if row is None:
            QMessageBox.information(self, "Export Request Zip", "Select a request batch first.")
            return

        batch_id = str(row.get("batch_id", "")).strip()
        is_test = bool(row.get("is_test"))
        default_zip = self.request_store.export_submission_batch_zip(batch_id, test_mode=is_test)
        custom_zip_path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Request Zip Copy",
            str(default_zip),
            "Submission Zip Bundle (*.zip)",
        )
        custom_zip_destination = None
        if custom_zip_path:
            custom_zip_destination = Path(custom_zip_path)
            if custom_zip_destination.suffix.lower() != ".zip":
                custom_zip_destination = custom_zip_destination.with_suffix(".zip")
            self.request_store.export_submission_batch_zip(
                batch_id,
                test_mode=is_test,
                destination_zip=custom_zip_destination,
            )

        if custom_zip_destination is not None:
            QMessageBox.information(
                self,
                "Export Request Zip",
                f"Default zip created at:\n{default_zip}\n\nAdditional zip copy saved to:\n{custom_zip_destination}",
            )
        else:
            QMessageBox.information(
                self,
                "Export Request Zip",
                f"Default zip created at:\n{default_zip}",
            )

    def handle_email_request_batch(self):
        row = self._selected_request_batch_row()
        if row is None:
            QMessageBox.information(self, "Email Ozlink IT", "Select a request batch first.")
            return

        batch_id = str(row.get("batch_id", "")).strip()
        is_test = bool(row.get("is_test"))
        default_zip = self.request_store.export_submission_batch_zip(batch_id, test_mode=is_test)
        subject = f"SharePoint Relocation Request {batch_id}"
        body = (
            f"Hello Ozlink IT,%0D%0A%0D%0A"
            f"Please find attached submission batch {batch_id}.%0D%0A%0D%0A"
            f"Default zip path:%0D%0A{default_zip}%0D%0A"
        )
        mail_url = QUrl(f"mailto:support@ozlink.it?subject={QUrl.toPercentEncoding(subject).data().decode()}&body={body}")
        QDesktopServices.openUrl(mail_url)
        QMessageBox.information(
            self,
            "Email Ozlink IT",
            f"Your mail client was opened for batch {batch_id}.\n\nPlease attach this zip:\n{default_zip}",
        )

    def handle_delete_test_request(self):
        if self.current_session_context.get("user_role", "user") != "admin":
            QMessageBox.warning(self, "Delete Test Batch", "Only admins can delete test batches.")
            return
        selected_ranges = self.requests_table.selectedRanges() if hasattr(self, "requests_table") else []
        if not selected_ranges:
            QMessageBox.information(self, "Delete Test Batch", "Select a test batch first.")
            return
        row_index = selected_ranges[0].topRow()
        if row_index < 0 or row_index >= len(getattr(self, "_request_batch_rows", [])):
            QMessageBox.information(self, "Delete Test Batch", "Select a test batch first.")
            return
        row = self._request_batch_rows[row_index]
        if not row.get("is_test"):
            QMessageBox.information(self, "Delete Test Batch", "Only test batches can be deleted.")
            return

        batch_id = str(row.get("batch_id", "")).strip()
        decision = QMessageBox.question(
            self,
            "Delete Test Batch",
            f"Delete test batch {batch_id}?\n\nThis will remove the packaged test request from disk.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if decision != QMessageBox.Yes:
            return

        self.request_store.delete_submission_batch(batch_id, test_mode=True)
        self.refresh_requests_page()
        self.requests_detail_text.setPlainText("Select a request batch to review its packaged details.")
        QMessageBox.information(self, "Delete Test Batch", f"Deleted test batch {batch_id}.")

    def build_tree_panel(self, title_text, empty_message, panel_key):
        box = QFrame()
        box.setObjectName("SectionBox")
        layout = QVBoxLayout(box)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        title = QLabel(title_text)
        title.setObjectName("SectionTitle")
        refresh_cache_button = QPushButton("Refresh Cache")
        refresh_cache_button.setMinimumHeight(24)
        refresh_cache_button.clicked.connect(
            lambda _=False, panel_key=panel_key: self.handle_refresh_cache_for_panel(panel_key)
        )
        expand_button = QPushButton("Expand All")
        expand_button.clicked.connect(lambda _=False, panel_key=panel_key: self.handle_expand_all(panel_key))
        if panel_key == "source":
            self.source_expand_all_button = expand_button
            self.source_refresh_cache_button = refresh_cache_button
        else:
            self.destination_expand_all_button = expand_button
            self.destination_refresh_cache_button = refresh_cache_button

        title_row = QHBoxLayout()
        title_row.setContentsMargins(0, 0, 0, 0)
        title_row.setSpacing(8)
        title_row.addWidget(title)
        title_row.addStretch()
        title_row.addWidget(refresh_cache_button)
        title_row.addWidget(expand_button)

        surface = QFrame()
        surface.setObjectName("TreeSurface")
        surface_layout = QVBoxLayout(surface)
        surface_layout.setContentsMargins(8, 8, 8, 8)
        surface_layout.setSpacing(8)

        if panel_key == "source" and getattr(self, "_source_tree_model_view", False):
            tree = QTreeView()
            self.source_sharepoint_model = SharePointSourceTreeModel(parent=tree)
            tree.setModel(self.source_sharepoint_model)
        else:
            tree = DestinationPlanningTreeWidget() if panel_key == "destination" else QTreeWidget()
        tree.setHeaderHidden(True)
        tree.setRootIsDecorated(True)
        tree.setItemsExpandable(True)
        tree.setExpandsOnDoubleClick(True)
        tree.setAlternatingRowColors(False)
        tree.setUniformRowHeights(True)
        tree.setSelectionMode(QAbstractItemView.SingleSelection)
        tree.setIndentation(18)
        tree.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        tree.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        tree.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        tree.setTextElideMode(Qt.ElideNone)
        tree.header().setStretchLastSection(False)
        tree.header().setMinimumSectionSize(240)
        tree.header().setSectionResizeMode(0, QHeaderView.Interactive)
        if panel_key == "destination":
            tree.setDragEnabled(True)
            tree.setAcceptDrops(True)
            tree.setDropIndicatorShown(True)
            tree.setDragDropMode(QAbstractItemView.DragDrop)
            tree.setDefaultDropAction(Qt.MoveAction)
            tree.proposedBranchMoveRequested.connect(self.handle_destination_draft_move)

        status = QLabel(empty_message)
        status.setObjectName("MutedText")
        status.setWordWrap(False)
        status.setStyleSheet("padding-top:6px; border-top:1px solid #20355E;")

        selection_summary = QLabel("")
        selection_summary.setVisible(False)
        selection_summary.setWordWrap(False)
        selection_summary.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        selection_summary.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Fixed)
        selection_summary.setFixedHeight(24)
        selection_summary.setMaximumWidth(520)
        selection_summary.setStyleSheet(
            "padding-top:6px; border-top:1px solid #20355E; color:#51E3F6; font-weight:700;"
        )

        footer_row = QHBoxLayout()
        footer_row.setContentsMargins(0, 0, 0, 0)
        footer_row.setSpacing(12)
        footer_row.addWidget(status, 1)
        footer_row.addWidget(selection_summary, 0)

        surface_layout.addWidget(tree, 1)
        surface_layout.addLayout(footer_row)

        if panel_key == "source":
            self.source_tree_selection_summary = selection_summary
        else:
            self.destination_tree_selection_summary = selection_summary

        layout.addLayout(title_row)
        layout.addWidget(surface, 1)

        return box, tree, status

    def _loading_palette_color(self):
        colors = ["#FF5A5F", "#59D88F", "#51E3F6"]
        return colors[self._loading_visual_phase % len(colors)]

    def _success_palette_color(self):
        colors = ["#59D88F", "#7BE3A8", "#A7F1C4"]
        return colors[self._loading_visual_phase % len(colors)]

    def _refresh_loading_visual_timer(self):
        timer = getattr(self, "_loading_visual_timer", None)
        if timer is None:
            return
        tracked_labels = [
            getattr(self, "dashboard_status", None),
            getattr(self, "source_tree_status", None),
            getattr(self, "destination_tree_status", None),
        ]
        active = any(bool(label and label.property("loading_emphasis")) for label in tracked_labels)
        active = active or bool(
            getattr(self, "planning_loading_banner", None)
            and self.planning_loading_banner.property("loading_emphasis")
        )
        if active:
            if not timer.isActive():
                timer.start()
        else:
            timer.stop()

    def _apply_loading_visual_state(self, label, *, loading=False, kind="generic"):
        if label is None:
            return
        label.setProperty("loading_emphasis", bool(loading))
        label.setProperty("loading_emphasis_kind", kind)
        if loading:
            color = self._success_palette_color() if kind == "dashboard_success" else self._loading_palette_color()
            if kind == "tree":
                label.setStyleSheet(
                    f"padding-top:6px; border-top:1px solid #20355E; color:{color}; font-weight:700; font-size:10.8pt;"
                )
            else:
                label.setStyleSheet(
                    f"color:{color}; font-weight:700; font-size:11pt;"
                )
        else:
            if kind == "tree":
                label.setStyleSheet("padding-top:6px; border-top:1px solid #20355E;")
            else:
                label.setStyleSheet("")
        self._refresh_loading_visual_timer()

    def _animate_loading_visuals(self):
        self._loading_visual_phase += 1
        tracked_labels = [
            getattr(self, "dashboard_status", None),
            getattr(self, "source_tree_status", None),
            getattr(self, "destination_tree_status", None),
        ]
        for label in tracked_labels:
            if not label or not label.property("loading_emphasis"):
                continue
            kind = str(label.property("loading_emphasis_kind") or "generic")
            self._apply_loading_visual_state(label, loading=True, kind=kind)
        self._apply_planning_loading_banner_visual_state(
            bool(getattr(self, "planning_loading_banner", None) and self.planning_loading_banner.property("loading_emphasis"))
        )

    def _apply_planning_loading_banner_visual_state(self, loading=False):
        banner = getattr(self, "planning_loading_banner", None)
        if banner is None:
            return
        banner.setProperty("loading_emphasis", bool(loading))
        if loading:
            color = self._loading_palette_color()
            banner.setStyleSheet(
                f"QFrame#SoftBanner {{"
                f"background-color:#101C33;"
                f"border:1px solid {color};"
                f"border-radius:0px;"
                f"}}"
                f"QLabel#SectionTitle {{ color:{color}; font-weight:800; }}"
                f"QLabel#CardBody {{ color:#DCE8FF; }}"
            )
        else:
            banner.setStyleSheet("")
        self._refresh_loading_visual_timer()

    def _set_dashboard_status_message(self, message, *, loading=False):
        if hasattr(self, "dashboard_status") and self.dashboard_status is not None:
            self.dashboard_status.setText(message)
            self._apply_loading_visual_state(self.dashboard_status, loading=loading, kind="dashboard")

    def _set_dashboard_success_message(self, message):
        if hasattr(self, "dashboard_status") and self.dashboard_status is not None:
            self.dashboard_status.setText(message)
            self._apply_loading_visual_state(self.dashboard_status, loading=True, kind="dashboard_success")

    def _set_tree_status_message(self, panel_key, message, *, loading=False):
        # Keep destination expand-all status stable while the queue is active.
        # Other background status writes can rapidly toggle loading styles/colors.
        if (
            panel_key == "destination"
            and bool(getattr(self, "_expand_all_pending", {}).get("destination"))
        ):
            message_text = str(message or "")
            allowed_prefixes = (
                "Expanding branches",
                "Loading full destination tree",
                "All loaded branches expanded.",
                "Expand all cancelled.",
                "Loaded branches collapsed.",
                "Building destination preview",
                "Merging destination",
            )
            if not any(message_text.startswith(prefix) for prefix in allowed_prefixes):
                return
        label = self.source_tree_status if panel_key == "source" else self.destination_tree_status
        if label is not None:
            label.setText(message)
            self._apply_loading_visual_state(label, loading=loading, kind="tree")

    def _set_window_title_status(self, status_text=""):
        title = str(getattr(self, "_base_window_title", "Ozlink IT – SharePoint File Relocation Console"))
        status_text = str(status_text or "").strip()
        if status_text:
            title = f"{title} • {status_text}"
        self.setWindowTitle(title)

    def _collect_current_source_projection_paths(self):
        source_projection_paths = set(self._build_source_materialization_paths())
        source_projection_paths.update(self._collect_visible_source_relationship_paths())
        return source_projection_paths

    def _notify_planning_change_saved(self, message="Change saved. Background refresh queued."):
        if hasattr(self, "planned_moves_status") and self.planned_moves_status is not None:
            self.planned_moves_status.setText(message)
        self._set_window_title_status("Changes saved")

    def _queue_deferred_planning_refresh(
        self, reason, *, source_projection_paths=None, delay_ms=None, notify_saved=True
    ):
        reason_text = str(reason or "").strip() or "planning_change"
        if reason_text not in self._deferred_planning_refresh_reasons:
            self._deferred_planning_refresh_reasons.append(reason_text)
        if source_projection_paths:
            self._deferred_source_projection_paths.update(
                str(path or "").strip() for path in source_projection_paths if str(path or "").strip()
            )
        self._deferred_planning_refresh_pending = True
        if notify_saved:
            self._notify_planning_change_saved()

        timer = getattr(self, "_deferred_planning_refresh_timer", None)
        if timer is None:
            return

        interval = 450 if delay_ms is None else max(0, int(delay_ms))
        if getattr(self, "_sharepoint_lazy_mode", False) and self._planning_workspace_is_busy():
            interval = max(interval, 1200)
        timer.stop()
        timer.start(interval)

    def _run_deferred_planning_refresh(self):
        if not getattr(self, "_deferred_planning_refresh_pending", False):
            return

        reasons = list(getattr(self, "_deferred_planning_refresh_reasons", []))
        combined_reason = "__".join(reasons) if reasons else "deferred_planning_refresh"
        source_projection_paths = set(getattr(self, "_deferred_source_projection_paths", set()))

        self._deferred_planning_refresh_pending = False
        self._deferred_planning_refresh_reasons = []
        self._deferred_source_projection_paths = set()

        # Skip synchronous destination future-model bind when the UI was already updated
        # incrementally (add/remove allocation overlays). Full binds can freeze for large libraries.
        skip_full_destination_future_model = bool(reasons) and all(
            r
            in (
                "planned_move_removed",
                "planned_allocation_removed",
                "planned_move_added",
                "planned_move_override_added",
            )
            for r in reasons
        )

        try:
            if getattr(self, "destination_tree_widget", None) is not None:
                if skip_full_destination_future_model:
                    log_info(
                        "Planning deferred refresh skipped full destination rebuild (incremental planning).",
                        reasons=combined_reason,
                    )
                else:
                    self._materialize_destination_future_model(f"deferred_{combined_reason}")
        except Exception as exc:
            self._log_restore_exception("deferred_planning_refresh.destination", exc)

        try:
            if getattr(self, "source_tree_widget", None) is not None and source_projection_paths:
                self._schedule_source_projection_refresh_for_paths(
                    source_projection_paths,
                    f"source_projection_deferred_{combined_reason}",
                    delay_ms=50,
                )
        except Exception as exc:
            self._log_restore_exception("deferred_planning_refresh.source", exc)

        self.update_progress_summaries()
        self._set_window_title_status()

    def _flush_deferred_planning_refresh(self):
        timer = getattr(self, "_deferred_planning_refresh_timer", None)
        if timer is not None:
            timer.stop()
        self._run_deferred_planning_refresh()

    def _tree_selection_summary_label(self, panel_key):
        if panel_key == "source":
            return getattr(self, "source_tree_selection_summary", None)
        return getattr(self, "destination_tree_selection_summary", None)

    def _set_tree_selection_summary(self, panel_key, text=""):
        label = self._tree_selection_summary_label(panel_key)
        if label is None:
            return
        message = str(text or "").strip()
        label.setToolTip(message)
        if message:
            metrics = label.fontMetrics()
            available_width = max(220, label.width() or 420)
            message = metrics.elidedText(message, Qt.ElideRight, available_width)
        label.setText(message)
        label.setVisible(bool(message))

    def _expand_all_button_for_panel(self, panel_key):
        if panel_key == "source":
            return getattr(self, "source_expand_all_button", None)
        return getattr(self, "destination_expand_all_button", None)

    def _set_expand_all_button_label(self, panel_key, expanded):
        button = self._expand_all_button_for_panel(panel_key)
        if button is not None:
            button.setText("Collapse All" if expanded else "Expand All")

    def _panel_loaded_branch_state(self, panel_key):
        tree = self.source_tree_widget if panel_key == "source" else self.destination_tree_widget
        if tree is None:
            return False, False

        has_loaded_branches = False
        all_loaded_branches_expanded = True
        queue = [tree.topLevelItem(index) for index in range(tree.topLevelItemCount())]
        while queue:
            item = queue.pop(0)
            if item is None:
                continue
            node_data = item.data(0, Qt.UserRole) or {}
            if node_data.get("placeholder"):
                continue
            real_children = []
            for index in range(item.childCount()):
                child = item.child(index)
                child_data = child.data(0, Qt.UserRole) or {}
                if child_data.get("placeholder"):
                    continue
                real_children.append(child)
            if bool(node_data.get("is_folder")) and real_children:
                has_loaded_branches = True
                if not item.isExpanded():
                    all_loaded_branches_expanded = False
            queue.extend(real_children)
        return has_loaded_branches, all_loaded_branches_expanded

    def _panel_is_expanded_all(self, panel_key):
        has_loaded_branches, all_loaded_branches_expanded = self._panel_loaded_branch_state(panel_key)
        # "All expanded" should also require no remaining lazy/unloaded folders.
        # Otherwise the button can claim completion while branches still show
        # "Expand to load contents" placeholders.
        if self._tree_has_unloaded_folder_nodes(panel_key):
            return False
        if has_loaded_branches:
            return all_loaded_branches_expanded
        return False

    def _panel_can_collapse_loaded_branches(self, panel_key):
        """True when every folder that already has real (non-placeholder) children is expanded.

        Unlike _panel_is_expanded_all, this ignores lazy/unloaded folder rows. Otherwise after
        expand-all stops early (allocations, throttling), the button still said Expand All and
        the next click started another expand pass (bigger counter, Expanding...) instead of
        Collapse All.
        """
        has_loaded_branches, all_loaded_branches_expanded = self._panel_loaded_branch_state(panel_key)
        return bool(has_loaded_branches and all_loaded_branches_expanded)

    def _sync_expand_all_button_from_tree(self, panel_key, fallback_expanded=False):
        has_loaded_branches, all_loaded_branches_expanded = self._panel_loaded_branch_state(panel_key)
        expanded = all_loaded_branches_expanded if has_loaded_branches else bool(fallback_expanded)
        self._set_expand_all_button_label(panel_key, expanded)
        return expanded

    def _refresh_expand_all_button_for_panel(self, panel_key):
        """Align expand/collapse label with session intent only when it matches the real tree.

        If the session says expand-all was saved but loaded branches are not all expanded yet
        (large snapshot restore, lazy folders, or destination semantic-path mismatch), forcing
        *Collapse All* makes the next click run the collapse path and feels like expand-all is
        broken.
        """
        intent = getattr(self, "_workspace_restore_expanded_all_intent", None) or {}
        if intent.get(panel_key) and self._panel_can_collapse_loaded_branches(panel_key):
            self._set_expand_all_button_label(panel_key, True)
            return
        self._sync_expand_all_button_from_tree(panel_key, fallback_expanded=False)

    def _persist_workspace_ui_state_safely(self):
        try:
            self._save_draft_shell(force=True, include_workspace_ui=True)
        except Exception as exc:
            self._log_restore_exception("persist_workspace_ui_state", exc)

    def _refresh_runtime_tree_snapshot(self, panel_key):
        if panel_key not in {"source", "destination"}:
            return []
        snapshots = self._capture_tree_items_snapshot(panel_key)
        runtime_snapshots = getattr(self, "_runtime_session_tree_snapshots", None)
        if not isinstance(runtime_snapshots, dict):
            runtime_snapshots = {"source": [], "destination": []}
            self._runtime_session_tree_snapshots = runtime_snapshots
        runtime_snapshots[panel_key] = list(snapshots or [])
        return runtime_snapshots[panel_key]

    def _schedule_workspace_ui_persist(self, delay_ms=1200, *, panel_key=None):
        try:
            if panel_key in {"source", "destination"}:
                self._workspace_ui_snapshot_dirty_panels.add(panel_key)
                if self._expand_all_pending.get(panel_key):
                    return
            else:
                self._workspace_ui_snapshot_dirty_panels.update({"source", "destination"})
                if self._expand_all_pending.get("source") or self._expand_all_pending.get("destination"):
                    return
            if self._planning_workspace_is_busy():
                delay_ms = max(delay_ms, 1800)
            if hasattr(self, "_workspace_ui_persist_timer") and self._workspace_ui_persist_timer is not None:
                self._workspace_ui_persist_timer.start(max(0, int(delay_ms)))
        except Exception as exc:
            self._log_restore_exception("schedule_workspace_ui_persist", exc)

    def _on_workspace_ui_persist_timer(self):
        try:
            if self._planning_workspace_is_busy():
                # Avoid heavy snapshot capture while expand/load bursts are active.
                self._workspace_ui_persist_timer.start(1800)
                return
            dirty_panels = set(self._workspace_ui_snapshot_dirty_panels or set())
            if not dirty_panels:
                dirty_panels = {"source", "destination"}
            for panel in sorted(dirty_panels):
                self._refresh_runtime_tree_snapshot(panel)
            self._workspace_ui_snapshot_dirty_panels.clear()
            self._persist_workspace_ui_state_safely()
        except Exception as exc:
            self._log_restore_exception("on_workspace_ui_persist_timer", exc)

    def showEvent(self, event):
        super().showEvent(event)
        print(
            f"[window-startup] showEvent state={self._window_state_repr()} "
            f"geometry={self.geometry().getRect()}"
        )
        if hasattr(self, "work_email_input") and self.work_email_input is not None:
            self._schedule_safe_timer(0, "startup_focus_work_email", self.work_email_input.setFocus)
        if not self._startup_post_show_logged:
            self._startup_post_show_logged = True
            self._schedule_safe_timer(0, "startup_post_show_log", self._log_post_startup_state)

    def show(self):
        print("[window-startup] show() called")
        if not self._startup_geometry_applied:
            self._startup_geometry_applied = True
            self.apply_startup_window_geometry()
            print(
                "[window-startup] pre-final-show "
                f"flags={int(self.windowFlags())} "
                f"geometry={self.geometry().getRect()}"
            )
            super().showMaximized()
            return

        super().show()

    def closeEvent(self, event):
        try:
            self._flush_deferred_planning_refresh()
        except Exception:
            pass
        try:
            self._shutdown_running_workers_for_close()
        except Exception as exc:
            self._log_restore_exception("closeEvent.shutdown_workers", exc)
        try:
            self._save_draft_shell(force=True, include_workspace_ui=True)
        except Exception:
            pass
        try:
            self._auto_export_draft_on_exit()
        except Exception:
            pass
        self.save_window_preferences()
        super().closeEvent(event)

    def _shutdown_running_workers_for_close(self):
        """Best-effort worker shutdown to avoid QThread destruction during app exit."""
        workers = []
        seen_ids = set()

        def _add_worker(worker):
            if worker is None:
                return
            worker_id = id(worker)
            if worker_id in seen_ids:
                return
            seen_ids.add(worker_id)
            workers.append(worker)

        for attr_name in (
            "worker",
            "device_flow_worker",
            "discovery_worker",
            "full_count_worker",
            "cache_refresh_worker",
            "_destination_full_tree_worker",
            "_preview_worker",
            "session_keepalive_worker",
        ):
            _add_worker(getattr(self, attr_name, None))

        for entry in (self.root_load_workers or {}).values():
            if isinstance(entry, dict):
                _add_worker(entry.get("worker"))
        for entry in (self.folder_load_workers or {}).values():
            if isinstance(entry, dict):
                _add_worker(entry.get("worker"))

        running_workers = [worker for worker in workers if hasattr(worker, "isRunning") and worker.isRunning()]
        if not running_workers:
            return

        log_info("Close requested with running workers.", running_worker_count=len(running_workers))
        for worker in running_workers:
            try:
                worker.requestInterruption()
            except Exception:
                pass

        for worker in running_workers:
            try:
                if not worker.wait(2500):
                    log_warn(
                        "Worker still running during close wait.",
                        worker_type=type(worker).__name__,
                    )
            except Exception:
                pass

    def _auto_export_draft_on_exit(self):
        if self.memory_manager is None:
            return None
        if not self.active_draft_session_id and not self.planned_moves and not self.proposed_folders:
            return None

        try:
            bundle_folder = self.memory_manager.export_bundle(reason="Auto Exit Export")
            bundle_zip = self.memory_manager.export_bundle_zip(bundle_folder)
            log_info(
                "Draft auto-export completed on exit.",
                bundle_folder=str(bundle_folder),
                bundle_zip=str(bundle_zip),
            )
            return bundle_zip
        except Exception as exc:
            log_warn("Draft auto-export failed on exit.", error=str(exc))
            return None

    def _init_memory_services(self):
        try:
            self.memory_manager = MemoryManager(
                tenant_domain=self.current_session_context.get("tenant_domain", ""),
                operator_upn=self.current_session_context.get("operator_upn", ""),
            )
            log_info(
                "Memory services initialized.",
                memory_root=str(self.memory_manager.root),
                python_primary_storage_root=str(self.memory_manager.python_primary_storage_root),
                legacy_compatibility_root=str(self.memory_manager.legacy_compatibility_root),
                memory_write_root=str(self.memory_manager.current_write_root),
                memory_scope_root=str(self.memory_manager.storage_scope_root),
                memory_scope_user=self.current_session_context.get("operator_upn", ""),
            )
            self._load_draft_shell_into_runtime()
        except Exception as exc:
            self.memory_manager = None
            self.active_draft_session_id = ""
            self._draft_shell_state = SessionState()
            self._draft_shell_raw = {}
            self._memory_restore_candidate = None
            self._memory_restore_in_progress = False
            self._memory_restore_complete = False
            self._suppress_autosave = True
            log_warn("Memory services unavailable.", error=str(exc))

    def _clear_runtime_draft_state(self, *, refresh_ui: bool = True):
        self.active_draft_session_id = ""
        self._draft_shell_state = SessionState()
        self._draft_shell_raw = {}
        self._memory_restore_candidate = None
        self._restore_payload_source = ""
        self._restore_selected_candidate_path = ""
        self.planned_moves = []
        self.proposed_folders = []
        self._memory_restore_complete = False
        self._restore_destination_overlay_pending = False
        self._restored_allocation_count = 0
        self._restored_proposed_count = 0
        self._reset_unresolved_proposed_queue()
        self._reset_unresolved_allocation_queue()
        if refresh_ui:
            self.refresh_planned_moves_table()

    def _finish_login_workspace_restore(self, *, role: str, had_login_error: bool):
        self._pending_login_restore_args = None
        self._clear_runtime_draft_state(refresh_ui=True)
        self._init_memory_services()
        if self.planned_moves or self.proposed_folders:
            self.refresh_planned_moves_table()
            self._log_restore_state_snapshot("restore_ui_bound", destination_replay_invoked=False)

        self.bottom_mode.setText(f"Mode: {role.title()}")
        self.bottom_refresh.setText("Last Refresh: SharePoint Sites Loaded")
        self._apply_restored_selector_state()
        self._log_restore_state_snapshot(
            "login_success_post_error",
            had_restored_state=self._has_restored_runtime_state(),
            login_in_progress=self._login_in_progress,
            clear_allowed=False,
            reason="post_error_success" if had_login_error else "success_without_prior_error",
        )
        self.try_restore_main_window()
        self.flash_taskbar()

    def _build_current_draft_shell_state(self, *, include_workspace_ui: bool = False):
        state = SessionState()
        existing_state = self._draft_shell_state if isinstance(self._draft_shell_state, SessionState) else SessionState()
        workspace_ui_state = self._capture_workspace_tree_state() if include_workspace_ui and hasattr(self, "source_tree_widget") else {
            "source_expanded_paths": set(),
            "destination_expanded_paths": set(),
            "source_selected_path": "",
            "destination_selected_path": "",
        }
        workspace_tree_snapshots = (
            {
                "source": self._capture_tree_items_snapshot("source"),
                "destination": self._capture_tree_items_snapshot("destination"),
            }
            if include_workspace_ui and hasattr(self, "source_tree_widget")
            else {
                "source": list(getattr(existing_state, "SourceTreeSnapshot", []) or []),
                "destination": list(getattr(existing_state, "DestinationTreeSnapshot", []) or []),
            }
        )

        source_site_selector = self.planning_inputs.get("Source Site") if hasattr(self, "planning_inputs") else None
        source_library_selector = self.planning_inputs.get("Source Library") if hasattr(self, "planning_inputs") else None
        destination_site_selector = self.planning_inputs.get("Destination Site") if hasattr(self, "planning_inputs") else None
        destination_library_selector = self.planning_inputs.get("Destination Library") if hasattr(self, "planning_inputs") else None

        source_site = source_site_selector.currentData() if source_site_selector is not None else None
        source_library = source_library_selector.currentData() if source_library_selector is not None else None
        destination_site = destination_site_selector.currentData() if destination_site_selector is not None else None
        destination_library = destination_library_selector.currentData() if destination_library_selector is not None else None

        state.DraftId = self.active_draft_session_id or existing_state.DraftId
        state.DraftName = existing_state.DraftName or state.DraftId
        state.IsActiveDraft = True
        state.CreatedUtc = existing_state.CreatedUtc or datetime.utcnow().isoformat()
        state.LastWorkspace = "Planning Workspace" if hasattr(self, "app_subtitle") else (existing_state.LastWorkspace or "Planning Workspace")
        state.LastSavedUtc = datetime.utcnow().isoformat()
        state.EnvironmentMode = existing_state.EnvironmentMode or self.current_session_context.get("user_role", "Client").title()
        state.SelectedSourceSite = source_site.get("name", "") if isinstance(source_site, dict) else ""
        state.SelectedSourceSiteKey = ""
        if isinstance(source_site, dict):
            state.SelectedSourceSiteKey = source_site.get("site_key") or source_site.get("web_url") or source_site.get("id", "")
        state.SelectedSourceLibrary = source_library.get("name", "") if isinstance(source_library, dict) else ""
        state.SelectedDestinationSite = destination_site.get("name", "") if isinstance(destination_site, dict) else ""
        state.SelectedDestinationSiteKey = ""
        if isinstance(destination_site, dict):
            state.SelectedDestinationSiteKey = destination_site.get("site_key") or destination_site.get("web_url") or destination_site.get("id", "")
        state.SelectedDestinationLibrary = destination_library.get("name", "") if isinstance(destination_library, dict) else ""
        operator_upn = self.current_session_context.get("operator_upn", "")
        tenant_domain = self.current_session_context.get("tenant_domain", "")
        state.SessionFingerprint = existing_state.SessionFingerprint or f"{operator_upn}|{tenant_domain}".strip("|")
        if include_workspace_ui:
            state.SourceExpandedAll = self._panel_is_expanded_all("source")
            state.DestinationExpandedAll = self._panel_is_expanded_all("destination")
            state.PlanningHeaderCollapsed = bool(getattr(self, "_planning_header_collapsed", False))
            state.WorkspacePanelCollapsed = bool(getattr(self, "_workspace_tabs_collapsed", False))
            state.SourceExpandedPaths = [] if state.SourceExpandedAll else sorted(workspace_ui_state.get("source_expanded_paths", set()))
            state.DestinationExpandedPaths = [] if state.DestinationExpandedAll else sorted(workspace_ui_state.get("destination_expanded_paths", set()))
            state.SourceSelectedPath = str(workspace_ui_state.get("source_selected_path", "") or "")
            state.DestinationSelectedPath = str(workspace_ui_state.get("destination_selected_path", "") or "")
            state.SourceTreeSnapshot = list(workspace_tree_snapshots.get("source", []) or [])
            state.DestinationTreeSnapshot = list(workspace_tree_snapshots.get("destination", []) or [])
        else:
            state.SourceExpandedAll = bool(getattr(existing_state, "SourceExpandedAll", False))
            state.DestinationExpandedAll = bool(getattr(existing_state, "DestinationExpandedAll", False))
            state.PlanningHeaderCollapsed = bool(getattr(existing_state, "PlanningHeaderCollapsed", False))
            state.WorkspacePanelCollapsed = bool(getattr(existing_state, "WorkspacePanelCollapsed", False))
            state.SourceExpandedPaths = list(getattr(existing_state, "SourceExpandedPaths", []) or [])
            state.DestinationExpandedPaths = list(getattr(existing_state, "DestinationExpandedPaths", []) or [])
            state.SourceSelectedPath = str(getattr(existing_state, "SourceSelectedPath", "") or "")
            state.DestinationSelectedPath = str(getattr(existing_state, "DestinationSelectedPath", "") or "")
            state.SourceTreeSnapshot = list(workspace_tree_snapshots.get("source", []) or [])
            state.DestinationTreeSnapshot = list(workspace_tree_snapshots.get("destination", []) or [])
        if not state.DraftName:
            operator_display = self.current_session_context.get("operator_display_name", "") or "Planning Session"
            state.DraftName = f"{operator_display} Draft"
        return state

    def _create_new_draft_session_id(self):
        operator_upn = str(self.current_session_context.get("operator_upn", "") or "").strip()
        operator_token = operator_upn.split("@", 1)[0] if operator_upn else "SESSION"
        operator_token = re.sub(r"[^A-Za-z0-9]+", "", operator_token).upper() or "SESSION"
        timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S-%f")[:-3]
        return f"DRAFT-{timestamp}-{operator_token}"

    def _ensure_active_draft_session(self):
        if self.active_draft_session_id:
            return True

        if isinstance(self._draft_shell_state, SessionState) and self._draft_shell_state.DraftId:
            self.active_draft_session_id = self._draft_shell_state.DraftId
            return True

        self.active_draft_session_id = self._create_new_draft_session_id()
        if not isinstance(self._draft_shell_state, SessionState):
            self._draft_shell_state = SessionState()
        self._draft_shell_state.DraftId = self.active_draft_session_id
        if not self._draft_shell_state.CreatedUtc:
            self._draft_shell_state.CreatedUtc = datetime.utcnow().isoformat()
        if not self._draft_shell_state.DraftName:
            operator_display = self.current_session_context.get("operator_display_name", "") or "Planning Session"
            self._draft_shell_state.DraftName = f"{operator_display} Draft"
        self._draft_shell_raw = self._draft_shell_state.to_dict()
        log_info(
            "Active draft session created for empty runtime state.",
            draft_id=self.active_draft_session_id,
            restore_complete=self._memory_restore_complete,
            planned_moves_count=len(self.planned_moves),
            proposed_folders_count=len(self.proposed_folders),
        )
        return True

    def _maybe_show_restore_backup_banner(self, selected: dict):
        if not isinstance(selected, dict):
            return
        name = str(selected.get("name", "") or "").lower()
        if "backup" not in name:
            return
        path_hint = str(selected.get("session_path", "") or "")
        msg = (
            "Session restored from a backup file (live session was empty or older). "
            f"Path: {path_hint}"
        )
        log_info("restore_from_backup_notice", candidate=selected.get("name", ""), session_path=path_hint)
        status = self.statusBar()
        if status is not None:
            status.showMessage(msg, 45000)

    def _log_restore_phase(self, phase, *, verbose=False, **data):
        """
        Restore paths can iterate thousands of nodes; avoid per-item INFO logs by default.
        Set OZLINK_RESTORE_VERBOSE_LOG=1 to log verbose_only phases (per file/branch, etc.).
        """
        if verbose:
            flag = os.environ.get("OZLINK_RESTORE_VERBOSE_LOG", "").strip().lower()
            if flag not in ("1", "true", "yes"):
                return
        restore_busy_for_log = bool(self._memory_restore_in_progress) and not bool(
            getattr(self, "_memory_restore_background_trees", False)
        )
        data.setdefault("restore_in_progress", restore_busy_for_log)
        data.setdefault("selector_suppressed", self._suppress_selector_change_handlers)
        data.setdefault("window_visible", self.isVisible())
        log_info(f"Memory restore {phase}.", **data)

    def _log_restore_state_snapshot(self, phase, *, selected_candidate_path="", destination_replay_invoked=False, **extra):
        payload_source = self._restore_payload_source or "unknown"
        candidate_path = selected_candidate_path or self._restore_selected_candidate_path
        data = {
            "planned_moves_count": len(self.planned_moves),
            "proposed_folders_count": len(self.proposed_folders),
            "selected_candidate_path": candidate_path,
            "source_of_payload": payload_source,
            "destination_replay_invoked": destination_replay_invoked,
        }
        data.update(extra)
        self._log_restore_phase(phase, **data)

    def _destination_root_bind_is_authoritative(self):
        if not hasattr(self, "destination_tree_widget"):
            return False
        if self.destination_tree_widget.topLevelItemCount() == 0:
            return False
        loaded_signature = self.loaded_root_request_signatures.get("destination")
        active_signature = self.active_root_request_signatures.get("destination")
        if loaded_signature and active_signature:
            return loaded_signature == active_signature
        return bool(loaded_signature)

    def _count_visible_destination_future_state_nodes(self):
        tree = getattr(self, "destination_tree_widget", None)
        if tree is None:
            return 0
        count = 0
        for index in range(tree.topLevelItemCount()):
            for item in self._iter_tree_items(tree.topLevelItem(index)):
                node_data = item.data(0, Qt.UserRole) or {}
                if (
                    self.node_is_proposed(node_data)
                    or self.node_is_planned_allocation(node_data)
                    or str(node_data.get("node_origin", "")).lower() == "projecteddestination"
                ):
                    count += 1
        return count

    def _has_restored_runtime_state(self):
        return bool(self.planned_moves or self.proposed_folders or self._memory_restore_candidate or self._memory_restore_complete)

    def _log_restore_exception(self, phase, exc):
        log_error(
            "Memory restore step failed.",
            phase=phase,
            error=str(exc),
            traceback=traceback.format_exc(),
        )

    def _projection_diag_verbose_enabled(self):
        flag = os.environ.get("OZLINK_PROJECTION_DIAG_LOG", "").strip().lower()
        return flag in ("1", "true", "yes")

    def _full_trace_enabled(self):
        """Set OZLINK_FULL_TRACE=1 for exhaustive tree/projection/worker decision logs."""
        flag = os.environ.get("OZLINK_FULL_TRACE", "").strip().lower()
        return flag in ("1", "true", "yes")

    def _tree_item_ui_snapshot(self, panel_key, item):
        """Compact row state for tracing (source or destination trees)."""
        if item is None:
            return {"panel": panel_key, "item_alive": False}
        node_data = item.data(0, Qt.UserRole) or {}
        path = self._tree_item_path(node_data)
        placeholders = []
        for index in range(item.childCount()):
            ch = item.child(index)
            cd = ch.data(0, Qt.UserRole) or {}
            if cd.get("placeholder"):
                placeholders.append(str(cd.get("placeholder_role", "") or "unknown"))
        policy = item.childIndicatorPolicy()
        snap = {
            "panel": panel_key,
            "path": path,
            "is_expanded": bool(item.isExpanded()),
            "child_count": item.childCount(),
            "placeholder_roles": placeholders,
            "child_indicator_show": policy == QTreeWidgetItem.ShowIndicator,
            "children_loaded": bool(node_data.get("children_loaded")),
            "load_failed": bool(node_data.get("load_failed")),
            "is_folder": bool(node_data.get("is_folder")),
            "row_is_placeholder": bool(node_data.get("placeholder")),
            "label_excerpt": (item.text(0) or "")[:160],
        }
        if panel_key == "destination":
            snap["is_planned_allocation"] = bool(self.node_is_planned_allocation(node_data))
            snap["planned_allocation_descendant"] = bool(node_data.get("planned_allocation_descendant"))
            snap["projection_unresolved_terminal"] = bool(node_data.get("projection_unresolved_terminal"))
            snap["node_origin"] = str(node_data.get("node_origin", ""))[:120]
        elif panel_key == "source":
            snap["source_relationship_mode"] = str(node_data.get("source_relationship_mode", ""))[:40]
        return snap

    def _ui_trace(self, category, action, *, panel_key=None, item=None, **extra):
        if not self._full_trace_enabled():
            return
        payload = dict(extra)
        if panel_key is not None and item is not None:
            payload.update(self._tree_item_ui_snapshot(panel_key, item))
        elif item is not None:
            payload.update(self._tree_item_ui_snapshot("unknown", item))
        log_trace(category, action, **payload)

    def _destination_projection_diag_payload(self, source_root_data, move, source_item):
        """Structured fields for projection troubleshooting (no tokens)."""
        move = move or {}
        path_from_move = self._canonical_source_projection_path(move.get("source_path", ""))
        path_from_root = self._canonical_source_projection_path(self._tree_item_path(source_root_data))
        drive_id = source_root_data.get("drive_id", "") or ""
        item_id = source_root_data.get("id", "") or ""
        payload = {
            "canonical_source_path_from_move": path_from_move,
            "canonical_source_path_from_root_data": path_from_root,
            "drive_id_present": bool(drive_id),
            "item_id_present": bool(item_id),
            "site_id_present": bool(source_root_data.get("site_id")),
            "source_is_folder": bool(source_root_data.get("is_folder", True)),
            "source_item_found_in_tree": source_item is not None,
            "drive_id_suffix": drive_id[-16:] if len(drive_id) > 16 else drive_id,
            "item_id_suffix": item_id[-16:] if len(item_id) > 16 else item_id,
        }
        if source_item is not None:
            sid = source_item.data(0, Qt.UserRole) or {}
            payload["source_root_children_loaded"] = bool(sid.get("children_loaded"))
            payload["source_root_child_count"] = source_item.childCount()
            payload["source_subtree_fully_loaded"] = self._source_subtree_fully_loaded_in_tree(source_item)
        else:
            payload["source_root_children_loaded"] = None
            payload["source_root_child_count"] = None
            payload["source_subtree_fully_loaded"] = False
        if self._projection_diag_verbose_enabled():
            src = move.get("source") or {}
            payload["move_source_path_raw"] = move.get("source_path", "")
            payload["root_item_path_raw"] = source_root_data.get("item_path", "")
            payload["move_source_keys"] = sorted(src.keys())
        return payload

    def _log_destination_projection_collect_result(
        self,
        descendants,
        *,
        branch,
        graph_attempted=False,
        graph_error=None,
        **diag,
    ):
        count = len(descendants)
        sample_paths = []
        for data in descendants[:5]:
            sample_paths.append(self._canonical_source_projection_path(self._tree_item_path(data)))
        if count == 0:
            self._log_restore_phase(
                "destination_projection_collect_empty",
                branch=branch,
                descendant_count=0,
                graph_attempted=graph_attempted,
                graph_error=graph_error,
                sample_paths=sample_paths,
                **diag,
            )
        elif self._projection_diag_verbose_enabled():
            self._log_restore_phase(
                "destination_projection_collect_ok",
                branch=branch,
                descendant_count=count,
                graph_attempted=graph_attempted,
                graph_error=graph_error,
                sample_paths=sample_paths,
                **diag,
            )

    def _destination_projection_ui_snapshot(self, item):
        """Visible row state for projection / chevron / placeholder diagnostics."""
        if item is None:
            return {"item": None}
        node_data = item.data(0, Qt.UserRole) or {}
        path = self._tree_item_path(node_data)
        placeholders = []
        for index in range(item.childCount()):
            ch = item.child(index)
            cd = ch.data(0, Qt.UserRole) or {}
            if cd.get("placeholder"):
                placeholders.append(str(cd.get("placeholder_role", "") or "unknown"))
        policy = item.childIndicatorPolicy()
        show_chevron = policy == QTreeWidgetItem.ShowIndicator
        text_excerpt = (item.text(0) or "")[:160]
        return {
            "destination_path": path,
            "is_expanded": bool(item.isExpanded()),
            "child_count": item.childCount(),
            "placeholder_roles": placeholders,
            "child_indicator_show": show_chevron,
            "children_loaded": bool(node_data.get("children_loaded")),
            "load_failed": bool(node_data.get("load_failed")),
            "projection_unresolved_terminal": bool(node_data.get("projection_unresolved_terminal")),
            "is_planned_allocation": bool(self.node_is_planned_allocation(node_data)),
            "is_allocation_descendant": bool(node_data.get("planned_allocation_descendant")),
            "label_excerpt": text_excerpt,
        }

    def _log_destination_projection_ui(self, phase, *, item=None, **extra):
        if not self._projection_diag_verbose_enabled():
            return
        payload = dict(extra)
        if item is not None:
            payload.update(self._destination_projection_ui_snapshot(item))
        self._log_restore_phase(phase, **payload)

    def _run_restore_phase(self, phase_name, func, *, fatal=False):
        self._log_restore_phase(f"{phase_name} start")
        try:
            result = func()
            self._log_restore_phase(f"{phase_name} end")
            return result
        except Exception as exc:
            self._log_restore_exception(phase_name, exc)
            if fatal:
                raise
            return None

    def _safe_invoke(self, callback_name, fn, *args, **kwargs):
        should_log = self._should_log_safe_callback(callback_name)
        if should_log:
            restore_log = bool(self._memory_restore_in_progress) and not bool(
                getattr(self, "_memory_restore_background_trees", False)
            )
            log_info(
                "Safe callback entered.",
                callback=callback_name,
                restore_in_progress=restore_log,
                selector_suppressed=self._suppress_selector_change_handlers,
                destination_tree_ready=hasattr(self, "destination_tree_widget") and self.destination_tree_widget.topLevelItemCount() > 0,
                window_visible=self.isVisible(),
            )
        try:
            result = fn(*args, **kwargs)
            if should_log:
                log_info("Safe callback exited.", callback=callback_name, window_visible=self.isVisible())
            return result
        except Exception as exc:
            self._log_restore_exception(callback_name, exc)
            return None

    def _should_log_safe_callback(self, callback_name):
        # Keep callback logging lightweight by default; very chatty logging can choke the UI thread.
        flag = os.environ.get("OZLINK_SAFE_CALLBACK_VERBOSE", "").strip().lower()
        if flag in ("1", "true", "yes"):
            return True
        important_callbacks = {
            "phase2_post_login_restore_timer",
            "phase4_destination_overlay_timer",
            "phase4_destination_overlay_immediate",
            "login.restore_workspace",
            "login_worker.success",
            "discovery_worker.success",
            "discovery_worker.finished",
            "device_flow_worker.success",
            "destination_full_tree.success",
            "destination_full_tree.finished",
        }
        return callback_name in important_callbacks

    def _schedule_safe_timer(self, delay_ms, callback_name, fn, *args, **kwargs):
        if self._should_log_safe_callback(callback_name):
            log_info("Scheduling safe timer callback.", callback=callback_name, delay_ms=delay_ms)
        QTimer.singleShot(max(0, int(delay_ms)), lambda: self._safe_invoke(callback_name, fn, *args, **kwargs))

    def _next_worker_id(self, prefix):
        self._worker_sequence += 1
        return f"{prefix}-{self._worker_sequence:04d}"

    def _log_worker_lifecycle(self, event, worker_kind, worker_id, worker_key, **data):
        data.setdefault("worker_kind", worker_kind)
        data.setdefault("worker_id", worker_id)
        data.setdefault("worker_key", worker_key)
        data.setdefault("active_root_workers", len(self.root_load_workers))
        data.setdefault("retired_root_workers", len(self.root_load_retired_workers))
        data.setdefault("active_folder_workers", len(self.folder_load_workers))
        data.setdefault("retired_folder_workers", len(self.folder_load_retired_workers))
        log_info(f"Worker lifecycle {event}.", **data)

    def _register_root_worker(self, panel_key, worker):
        worker_id = self._next_worker_id("root")
        existing_entry = self.root_load_workers.get(panel_key)
        if existing_entry:
            existing_entry["stale"] = True
            self.root_load_retired_workers[existing_entry["id"]] = existing_entry
            self._log_worker_lifecycle(
                "superseded",
                "root",
                existing_entry["id"],
                panel_key,
                running=existing_entry["worker"].isRunning(),
            )

        entry = {
            "id": worker_id,
            "worker": worker,
            "panel_key": panel_key,
            "stale": False,
        }
        self.root_load_workers[panel_key] = entry
        self._log_worker_lifecycle("created", "root", worker_id, panel_key)
        self._log_worker_lifecycle("registered_active", "root", worker_id, panel_key)
        return entry

    def _register_folder_worker(self, worker_key, worker, item, *, source_folder_parent_persistent=None):
        worker_id = self._next_worker_id("folder")
        existing_entry = self.folder_load_workers.get(worker_key)
        if existing_entry:
            existing_entry["stale"] = True
            self.folder_load_retired_workers[existing_entry["id"]] = existing_entry
            self._log_worker_lifecycle(
                "superseded",
                "folder",
                existing_entry["id"],
                worker_key,
                running=existing_entry["worker"].isRunning(),
            )

        entry = {
            "id": worker_id,
            "worker": worker,
            "item": item,
            "worker_key": worker_key,
            "stale": False,
            "source_folder_parent_persistent": source_folder_parent_persistent,
        }
        self.folder_load_workers[worker_key] = entry
        self._log_worker_lifecycle("created", "folder", worker_id, worker_key)
        self._log_worker_lifecycle("registered_active", "folder", worker_id, worker_key)
        return entry

    def _build_root_request_signature(self, panel_key, site, library):
        site_payload = site if isinstance(site, dict) else {}
        library_payload = library if isinstance(library, dict) else {}
        return {
            "panel_key": panel_key,
            "site_id": site_payload.get("id", ""),
            "library_id": library_payload.get("id", ""),
            "drive_id": library_payload.get("id", ""),
            "tree_role": panel_key,
        }

    def _tree_has_bound_root_content(self, panel_key):
        tree, _status = self._get_tree_and_status(panel_key)
        if tree is None:
            return False
        if panel_key == "source" and self._source_tree_uses_model_view():
            model = getattr(self, "source_sharepoint_model", None)
            if model is None or model.rowCount(QModelIndex()) == 0:
                return False
            first_ix = model.index(0, 0, QModelIndex())
            first_data = first_ix.data(Qt.UserRole) or {}
            return not first_data.get("placeholder")
        if tree.topLevelItemCount() == 0:
            return False
        first_item = tree.topLevelItem(0)
        first_data = first_item.data(0, Qt.UserRole) or {}
        return not first_data.get("placeholder")

    def _cleanup_root_worker(self, panel_key, worker_id):
        entry = None
        active_entry = self.root_load_workers.get(panel_key)
        if active_entry and active_entry.get("id") == worker_id:
            entry = self.root_load_workers.pop(panel_key, None)
        else:
            entry = self.root_load_retired_workers.pop(worker_id, None)

        if not entry:
            self._log_worker_lifecycle("cleanup_missing", "root", worker_id, panel_key)
            return

        worker = entry.get("worker")
        self._log_worker_lifecycle(
            "finished",
            "root",
            worker_id,
            panel_key,
            running=worker.isRunning() if worker else False,
        )
        if worker is not None:
            worker.deleteLater()
        self._log_worker_lifecycle("cleaned_up", "root", worker_id, panel_key)

    def _cleanup_folder_worker(self, worker_key, worker_id):
        entry = None
        active_entry = self.folder_load_workers.get(worker_key)
        if active_entry and active_entry.get("id") == worker_id:
            entry = self.folder_load_workers.pop(worker_key, None)
        else:
            entry = self.folder_load_retired_workers.pop(worker_id, None)

        if not entry:
            self._log_worker_lifecycle("cleanup_missing", "folder", worker_id, worker_key)
            return

        worker = entry.get("worker")
        self._log_worker_lifecycle(
            "finished",
            "folder",
            worker_id,
            worker_key,
            running=worker.isRunning() if worker else False,
        )
        if worker is not None:
            worker.deleteLater()
        self._log_worker_lifecycle("cleaned_up", "folder", worker_id, worker_key)

    def _tree_item_is_alive(self, item):
        if item is None:
            return False
        try:
            item.childCount()
            item.data(0, Qt.UserRole)
            return True
        except RuntimeError:
            return False

    def _build_restored_planned_move_record(self, row: AllocationRow):
        source_path = str(row.SourcePath or "")
        destination_path = str(row.RequestedDestinationPath or "")
        destination_leaf = destination_path.replace("/", "\\").rstrip("\\").split("\\")[-1] if destination_path else row.SourceItemName
        source_is_folder = row.SourceType.lower() == "folder"
        return {
            "request_id": row.RequestId,
            "allocation_method": row.AllocationMethod,
            "requested_by": row.RequestedBy,
            "requested_date": row.RequestedDate,
            "status": row.Status,
            "source_id": "",
            "source_name": row.SourceItemName,
            "source_path": source_path,
            "source": {
                "id": "",
                "name": row.SourceItemName,
                "real_name": row.SourceItemName,
                "display_path": source_path,
                "item_path": source_path,
                "tree_role": "source",
                "drive_id": "",
                "is_folder": source_is_folder,
            },
            "destination_id": "",
            "destination_name": destination_leaf,
            "destination_path": destination_path,
            "destination": {
                "id": "",
                "name": destination_leaf,
                "real_name": destination_leaf,
                "display_path": destination_path,
                "item_path": destination_path,
                "tree_role": "destination",
                "drive_id": "",
                "is_folder": True,
            },
        }

    def _restore_memory_payload(self, session_state: SessionState, allocations: list[AllocationRow], proposed: list[ProposedFolder], session_raw: dict):
        self._draft_shell_state = session_state
        self._draft_shell_raw = dict(session_raw or {})
        self.active_draft_session_id = session_state.DraftId or self.active_draft_session_id
        self.planned_moves = [self._build_restored_planned_move_record(row) for row in allocations]
        self.proposed_folders = list(proposed)
        self._reset_unresolved_proposed_queue()
        self._reset_unresolved_allocation_queue()
        self._restored_allocation_count = len(allocations)
        self._restored_proposed_count = len(proposed)
        self._restore_destination_overlay_pending = bool(proposed or allocations)
        self.refresh_planned_moves_table()
        log_info(
            "Draft payload restored into runtime.",
            draft_id=self.active_draft_session_id,
            planned_moves_restored=len(allocations),
            proposed_folders_restored=len(proposed),
        )
        self._log_restore_state_snapshot("restore_runtime_applied")

    def _load_draft_shell_into_runtime(self):
        if self.memory_manager is None:
            return

        try:
            self._memory_restore_in_progress = True
            self._memory_restore_complete = False
            self._suppress_autosave = True
            self._restore_finalization_deferred_active = False
            self._restore_finalization_deferred_reason = ""
            candidates = self.memory_manager.discover_restore_candidates()
            for candidate in candidates:
                log_info(
                    "Memory restore candidate found.",
                    candidate=candidate.get("name"),
                    draft_id=candidate.get("draft_id", ""),
                    allocation_count=candidate.get("allocation_count", 0),
                    proposed_count=candidate.get("proposed_count", 0),
                    populated=bool(candidate.get("populated")),
                    session_path=str(candidate.get("session_path", "")),
                )

            selected, reason = self.memory_manager.select_restore_candidate(candidates)
            selected = self.memory_manager.prepare_selected_candidate_for_runtime(selected, candidates)
            self._memory_restore_candidate = selected
            if selected is None:
                self._clear_runtime_draft_state(refresh_ui=True)
                log_warn(
                    "No draft restore candidate available.",
                    memory_root=str(self.memory_manager.root),
                    python_primary_storage_root=str(self.memory_manager.python_primary_storage_root),
                    legacy_compatibility_root=str(self.memory_manager.legacy_compatibility_root),
                )
                return

            self._restore_payload_source = (
                "powershell_compat"
                if str(selected.get("name", "")).startswith("legacy_")
                else "python_root"
            )
            self._restore_selected_candidate_path = str(selected.get("session_path", ""))
            self._log_restore_state_snapshot(
                "restore_candidate_selected",
                selected_candidate_path=self._restore_selected_candidate_path,
                selection_reason=reason,
                selected_candidate=selected.get("name", ""),
            )

            session_state, allocations, proposed, session_raw = self.memory_manager.load_candidate_payload(selected)
            log_info(
                "Memory restore candidate selected.",
                reason=reason,
                selected_candidate=selected.get("name"),
                selected_storage_root=selected.get("storage_root", ""),
                restore_source=self.memory_manager.current_restore_source,
                write_root=str(self.memory_manager.current_write_root),
            )
            self._log_restore_state_snapshot(
                "restore_payload_loaded",
                selected_candidate_path=self._restore_selected_candidate_path,
                raw_allocation_count=len(allocations),
                raw_proposed_count=len(proposed),
            )
            self._maybe_show_restore_backup_banner(selected)
            self._run_restore_phase(
                "phase1_load_canonical_state",
                lambda: self._restore_memory_payload(session_state, allocations, proposed, session_raw),
                fatal=True,
            )
        except Exception as exc:
            self._log_restore_exception("phase1_load_canonical_state", exc)
        finally:
            self._memory_restore_in_progress = False

    def _build_memory_allocation_rows(self):
        rows = []
        for index, move in enumerate(self.planned_moves):
            rows.append(self._build_allocation_row_for_move(move, index))
        return rows

    def _build_memory_proposed_folders(self):
        return list(self.proposed_folders)

    def _save_draft_shell(self, *, force: bool = False, include_workspace_ui: bool = False):
        if self.memory_manager is None:
            return False

        if self._memory_restore_in_progress:
            if not force:
                log_info("Draft save suppressed while restore is in progress.", autosave_suppressed=True)
                return False
            log_info("Forced draft save allowed while restore is still in progress.", autosave_forced=True)

        if self._suppress_autosave and not force:
            log_info("Draft save suppressed until restore completes.", autosave_suppressed=True)
            return False

        try:
            if not self._ensure_active_draft_session():
                return False

            state = self._build_current_draft_shell_state(include_workspace_ui=include_workspace_ui)
            self._draft_shell_state = state
            self._draft_shell_raw = state.to_dict()
            self.active_draft_session_id = state.DraftId
            allocation_rows = self._build_memory_allocation_rows()
            proposed_rows = self._build_memory_proposed_folders()
            allow_empty_overwrite = bool(force)
            self.memory_manager.save_allocations(
                allocation_rows,
                allow_empty=allow_empty_overwrite or self._restored_allocation_count == 0,
            )
            self.memory_manager.save_proposed(
                proposed_rows,
                allow_empty=allow_empty_overwrite or self._restored_proposed_count == 0,
            )
            self.memory_manager.save_session(state)
            self.memory_manager.refresh_manifest(
                draft_id=state.DraftId,
                fingerprint=state.SessionFingerprint,
                status="Healthy",
            )
            log_info(
                "Draft shell saved.",
                draft_id=state.DraftId,
                workspace=state.LastWorkspace,
                allocation_count=len(allocation_rows),
                proposed_count=len(proposed_rows),
            )
            return True
        except Exception as exc:
            log_warn("Draft shell save failed.", error=str(exc))
            return False

    def _handle_export_draft(self):
        if self.memory_manager is None:
            QMessageBox.information(self, "Export Draft", "Draft export is not available right now.")
            return

        try:
            if self._memory_restore_complete:
                self._save_draft_shell(force=True)
            default_destination = self.memory_manager.export_bundle(reason="Manual Export")
            default_zip = self.memory_manager.export_bundle_zip(default_destination)
            custom_zip_path, _ = QFileDialog.getSaveFileName(
                self,
                "Save Draft Zip Copy As",
                str(default_zip),
                "Draft Zip Bundle (*.zip)",
            )
            custom_zip_destination = None
            if custom_zip_path:
                custom_zip_destination = Path(custom_zip_path)
                if custom_zip_destination.suffix.lower() != ".zip":
                    custom_zip_destination = custom_zip_destination.with_suffix(".zip")
                self.memory_manager.export_bundle_zip(default_destination, custom_zip_destination)
            log_info(
                "Draft export completed.",
                default_destination=str(default_destination),
                default_zip=str(default_zip),
                custom_zip_destination=str(custom_zip_destination) if custom_zip_destination else "",
            )
            if custom_zip_destination is not None:
                QMessageBox.information(
                    self,
                    "Export Draft",
                    f"Draft exported to default folder:\n{default_destination}\n\n"
                    f"Default zip created at:\n{default_zip}\n\n"
                    f"Additional zip copy saved to:\n{custom_zip_destination}",
                )
            else:
                QMessageBox.information(
                    self,
                    "Export Draft",
                    f"Draft exported to default folder:\n{default_destination}\n\n"
                    f"Default zip created at:\n{default_zip}",
                )
        except Exception as exc:
            log_error("Draft export failed.", error=str(exc))
            QMessageBox.warning(self, "Export Draft", "Could not export the draft.")

    def _handle_import_draft(self):
        if self.memory_manager is None:
            QMessageBox.information(self, "Import Draft", "Draft import is not available right now.")
            return

        try:
            source_file, _ = QFileDialog.getOpenFileName(
                self,
                "Import Draft Zip Bundle",
                "",
                "Draft Zip Bundle (*.zip)",
            )
            source_folder = ""
            if source_file:
                self.memory_manager.import_bundle_zip(Path(source_file))
                source_description = source_file
            else:
                source_folder = QFileDialog.getExistingDirectory(self, "Import Draft Bundle Folder")
                if not source_folder:
                    return
                self.memory_manager.import_bundle(Path(source_folder))
                source_description = source_folder
            self._load_draft_shell_into_runtime()
            if self.current_session_context.get("connected"):
                self._apply_restored_selector_state()
            log_info("Draft import completed.", source=source_description)
            self.refresh_requests_page()
            QMessageBox.information(self, "Import Draft", "Draft bundle imported.")
        except Exception as exc:
            log_error("Draft import failed.", error=str(exc), source=source_file if 'source_file' in locals() else source_folder)
            QMessageBox.warning(self, "Import Draft", "Could not import the selected draft bundle.")

    def _handle_unlock_draft(self):
        submitted_moves = [
            move for move in self.planned_moves
            if str((move or {}).get("status", "")).strip().lower() in {"submitted", "testsubmitted"}
        ]
        submitted_proposed = [
            proposed_folder for proposed_folder in self.proposed_folders
            if str(getattr(proposed_folder, "Status", "")).strip().lower() in {"submitted", "testsubmitted"}
            or bool(getattr(proposed_folder, "SubmittedBatchId", ""))
        ]

        if not submitted_moves and not submitted_proposed:
            QMessageBox.information(self, "Unlock Draft", "The current draft has no submitted items to unlock.")
            return

        decision = QMessageBox.question(
            self,
            "Unlock Draft",
            "\n".join(
                [
                    "This will clear the submitted and locked state from the current draft.",
                    "",
                    f"Planned moves to unlock: {len(submitted_moves)}",
                    f"Proposed folders to unlock: {len(submitted_proposed)}",
                    "",
                    "Saved submission batches in Requests are not deleted.",
                    "Continue?",
                ]
            ),
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if decision != QMessageBox.Yes:
            return

        for move in submitted_moves:
            move["status"] = "Draft"
            move.pop("submitted_batch_id", None)
            move.pop("submitted_utc", None)

        for proposed_folder in submitted_proposed:
            proposed_folder.Status = "Draft"
            if hasattr(proposed_folder, "SubmittedBatchId"):
                delattr(proposed_folder, "SubmittedBatchId")

        self._rebuild_submission_visual_cache()
        self.refresh_planned_moves_table()
        self._persist_planning_change("draft_unlocked")
        self.planned_moves_status.setText(
            f"Unlocked {len(submitted_moves)} move(s) and {len(submitted_proposed)} proposed folder(s) in the draft."
        )
        QMessageBox.information(
            self,
            "Unlock Draft",
            f"Unlocked {len(submitted_moves)} move(s) and {len(submitted_proposed)} proposed folder(s).",
        )

    def handle_refresh_cache(self):
        self._request_cache_refresh_for_panels({"source", "destination"})

    def handle_refresh_cache_for_panel(self, panel_key):
        if panel_key not in {"source", "destination"}:
            return
        self._request_cache_refresh_for_panels({panel_key})

    def _request_cache_refresh_for_panels(self, panel_keys):
        try:
            if self.cache_refresh_worker and self.cache_refresh_worker.isRunning():
                return
            target_panels = {key for key in (panel_keys or set()) if key in {"source", "destination"}}
            if not target_panels:
                return
            self._flush_deferred_planning_refresh()
            self._cache_refresh_restore_active = True
            self._pending_cache_refresh_ui_state = self._capture_workspace_tree_state()
            self._pending_cache_refresh_panels = set()
            # Restoring thousands of expanded paths immediately after cache refresh causes
            # aggressive lazy-load churn and UI scroll jank, especially for source.
            self._cache_refresh_skip_expanded_restore_panels = set(target_panels)
            self._pending_cache_refresh_tree_snapshots = {
                "source": self._capture_tree_items_snapshot("source"),
                "destination": self._capture_tree_items_snapshot("destination"),
            }
            self._cancel_expand_all("source")
            self._cancel_expand_all("destination")
            self._destination_expand_all_after_full_tree = False
            self._set_expand_all_button_label("source", False)
            self._set_expand_all_button_label("destination", False)
            destination_button = self._expand_all_button_for_panel("destination")
            if destination_button is not None:
                destination_button.setEnabled(True)
            source_site = self.planning_inputs.get("Source Site").currentData() if hasattr(self, "planning_inputs") else None
            source_library = self.planning_inputs.get("Source Library").currentData() if hasattr(self, "planning_inputs") else None
            destination_site = self.planning_inputs.get("Destination Site").currentData() if hasattr(self, "planning_inputs") else None
            destination_library = self.planning_inputs.get("Destination Library").currentData() if hasattr(self, "planning_inputs") else None
            drive_ids = []
            if "source" in target_panels and isinstance(source_library, dict) and source_library.get("id"):
                drive_ids.append(source_library.get("id"))
                self._pending_cache_refresh_panels.add("source")
            if "destination" in target_panels and isinstance(destination_library, dict) and destination_library.get("id"):
                drive_ids.append(destination_library.get("id"))
                self._pending_cache_refresh_panels.add("destination")

            if not drive_ids:
                self._cache_refresh_restore_active = False
                self._pending_cache_refresh_ui_state = None
                self._pending_cache_refresh_panels = set()
                self._cache_refresh_skip_expanded_restore_panels = set()
                self._pending_cache_refresh_tree_snapshots = {}
                return

            if "source" in self._pending_cache_refresh_panels:
                self._set_tree_status_message("source", "Refreshing source cache...", loading=True)
                if hasattr(self, "source_refresh_cache_button"):
                    self.source_refresh_cache_button.setEnabled(False)
                    self.source_refresh_cache_button.setText("Refreshing...")
            if "destination" in self._pending_cache_refresh_panels:
                self._set_tree_status_message("destination", "Refreshing destination cache...", loading=True)
                if hasattr(self, "destination_refresh_cache_button"):
                    self.destination_refresh_cache_button.setEnabled(False)
                    self.destination_refresh_cache_button.setText("Refreshing...")
            if hasattr(self, "planned_moves_status"):
                label = (
                    "Refreshing source and destination cache..."
                    if self._pending_cache_refresh_panels == {"source", "destination"}
                    else (
                        "Refreshing source cache..."
                        if "source" in self._pending_cache_refresh_panels
                        else "Refreshing destination cache..."
                    )
                )
                self.planned_moves_status.setText(label)

            self.cache_refresh_worker = CacheRefreshWorker(self.graph, drive_ids)
            self.cache_refresh_worker.success.connect(lambda payload: self._safe_invoke("cache_refresh.success", self.on_cache_refresh_success, payload))
            self.cache_refresh_worker.error.connect(lambda error: self._safe_invoke("cache_refresh.error", self.on_cache_refresh_error, error))
            self.cache_refresh_worker.finished.connect(lambda: self._safe_invoke("cache_refresh.finished", self.on_cache_refresh_worker_finished))
            self.cache_refresh_worker.start()
            log_info(
                "graph_cache_refresh_requested",
                panels=sorted(self._pending_cache_refresh_panels),
                source_library=str((source_library or {}).get("name", "")) if isinstance(source_library, dict) else "",
                destination_library=str((destination_library or {}).get("name", "")) if isinstance(destination_library, dict) else "",
            )
        except Exception as exc:
            self._cache_refresh_restore_active = False
            self._pending_cache_refresh_ui_state = None
            self._pending_cache_refresh_panels = set()
            self._cache_refresh_skip_expanded_restore_panels = set()
            log_error("Graph cache refresh failed.", error=str(exc))
            QMessageBox.warning(self, "Refresh Cache", "Could not refresh the SharePoint cache right now.")

    def _handle_submit_request(self):
        try:
            test_mode = bool(getattr(self, "_submission_test_mode", False))
            issues, warnings, draft_moves, draft_proposed = self._validate_submission_readiness()
            if issues:
                QMessageBox.warning(
                    self,
                    "Submit Request",
                    "Please fix the following before submitting:\n\n- " + "\n- ".join(issues),
                )
                return

            source_site = self.planning_inputs["Source Site"].currentData() or {}
            source_library = self.planning_inputs["Source Library"].currentData() or {}
            destination_site = self.planning_inputs["Destination Site"].currentData() or {}
            destination_library = self.planning_inputs["Destination Library"].currentData() or {}
            submitted_utc = datetime.utcnow().isoformat()
            batch_id = self._next_submission_batch_id()

            confirmation_lines = [
                "This will create an immutable submission batch for Ozlink IT."
                if not test_mode
                else "This will create a TEST submission batch without locking the live draft.",
                "",
                f"Planned moves to submit: {len(draft_moves)}",
                f"Proposed folders to submit: {len(draft_proposed)}",
                f"Needs review currently shown: {len(self._workflow_needs_review_rows)}",
                "",
                (
                    "Submitted items will remain visible in the draft but will become locked."
                    if not test_mode
                    else "Test submissions are stored separately and do not change the live draft."
                ),
                (
                    "Only new draft items will be included in later submissions."
                    if not test_mode
                    else "Use this to validate the handoff package safely before a real submit."
                ),
            ]
            if warnings:
                confirmation_lines.extend(["", "Warnings:"])
                confirmation_lines.extend(f"- {warning}" for warning in warnings)

            decision = QMessageBox.question(
                self,
                "Submit Request to Ozlink IT",
                "\n".join(confirmation_lines),
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if decision != QMessageBox.Yes:
                return

            allocation_rows = []
            allocation_payload = []
            allocation_request_ids = []
            for index, move in enumerate(draft_moves):
                row = self._build_allocation_row_for_move(move, index)
                row.Status = "TestSubmitted" if test_mode else "Submitted"
                allocation_rows.append(row)
                allocation_payload.append(row.to_dict())
                allocation_request_ids.append(row.RequestId)

            proposed_payload = []
            proposed_destination_ids = []
            for index, proposed_folder in enumerate(draft_proposed):
                self._ensure_proposed_destination_id(proposed_folder, index)
                proposed_data = proposed_folder.to_dict()
                proposed_data["RequestedBy"] = proposed_data.get("RequestedBy", "") or self.current_session_context.get("operator_display_name", "")
                proposed_data["RequestedDate"] = proposed_data.get("RequestedDate", "") or submitted_utc
                proposed_data["Status"] = "TestSubmitted" if test_mode else "Submitted"
                proposed_payload.append(proposed_data)
                proposed_destination_ids.append(proposed_folder.DestinationId)

            batch = SubmissionBatch(
                BatchId=batch_id,
                DraftId=self.active_draft_session_id,
                DraftName=getattr(self, "_draft_shell_state", SessionState()).DraftName if isinstance(getattr(self, "_draft_shell_state", None), SessionState) else "",
                SubmittedUtc=submitted_utc,
                SubmittedBy=self.current_session_context.get("operator_display_name", ""),
                SubmittedByUpn=self.current_session_context.get("operator_upn", ""),
                TenantDomain=self.current_session_context.get("tenant_domain", ""),
                Status="TestSubmitted" if test_mode else "Submitted",
                SourceSite=source_site.get("name", ""),
                SourceLibrary=source_library.get("name", ""),
                DestinationSite=destination_site.get("name", ""),
                DestinationLibrary=destination_library.get("name", ""),
                PlannedMoveCount=len(allocation_payload),
                ProposedFolderCount=len(proposed_payload),
                NeedsReviewCount=len(self._workflow_needs_review_rows),
                ValidationWarnings=list(warnings),
                AllocationRequestIds=allocation_request_ids,
                ProposedDestinationIds=proposed_destination_ids,
            )

            batch_path = self.request_store.create_submission_batch(
                batch,
                allocation_payload,
                proposed_payload,
                test_mode=test_mode,
            )

            if not test_mode:
                self._mark_items_submitted(batch_id, submitted_utc, draft_moves, draft_proposed)
                self.refresh_planned_moves_table()
                self._persist_planning_change("request_submitted")
                self.planned_moves_status.setText(
                    f"Submission batch {batch_id} created with {len(allocation_payload)} move(s) and {len(proposed_payload)} proposed folder(s)."
                )
            else:
                self.planned_moves_status.setText(
                    f"Test submission batch {batch_id} created with {len(allocation_payload)} move(s) and {len(proposed_payload)} proposed folder(s)."
                )
            log_info(
                "Submission batch created.",
                batch_id=batch_id,
                draft_id=self.active_draft_session_id,
                destination=str(batch_path),
                planned_move_count=len(allocation_payload),
                proposed_folder_count=len(proposed_payload),
                test_mode=test_mode,
            )
            default_zip = self.request_store.export_submission_batch_zip(batch_id, test_mode=test_mode)
            zip_save_path, _ = QFileDialog.getSaveFileName(
                self,
                "Save Submission Zip Copy",
                str(default_zip),
                "Submission Zip Bundle (*.zip)",
            )
            custom_zip_destination = None
            if zip_save_path:
                custom_zip_destination = Path(zip_save_path)
                if custom_zip_destination.suffix.lower() != ".zip":
                    custom_zip_destination = custom_zip_destination.with_suffix(".zip")
                self.request_store.export_submission_batch_zip(
                    batch_id,
                    test_mode=test_mode,
                    destination_zip=custom_zip_destination,
                )
            self.refresh_requests_page()
            info_lines = [
                "Test submission batch created successfully." if test_mode else "Submission batch created successfully.",
                "",
                f"Batch ID: {batch_id}",
                f"Planned moves: {len(allocation_payload)}",
                f"Proposed folders: {len(proposed_payload)}",
                "",
                (
                    "This test batch was stored separately and did not lock the live draft."
                    if test_mode
                    else "Submitted items are now locked in the draft."
                ),
                "",
                "A submission zip was created for email handoff.",
                f"Default zip: {default_zip}",
            ]
            if custom_zip_destination is not None:
                info_lines.extend(["", f"Additional zip copy: {custom_zip_destination}"])
            info_lines.extend(
                [
                    "",
                    "Please email the zip to: support@ozlink.it",
                    "The app can open your mail client with the address and subject prefilled.",
                ]
            )

            email_prompt = QMessageBox(self)
            email_prompt.setIcon(QMessageBox.Information)
            email_prompt.setWindowTitle("Submit Request")
            email_prompt.setText("\n".join(info_lines))
            open_email_btn = email_prompt.addButton("Open Email Draft", QMessageBox.AcceptRole)
            email_prompt.addButton(QMessageBox.Ok)
            email_prompt.exec()

            if email_prompt.clickedButton() == open_email_btn:
                subject = f"SharePoint Relocation Request {batch_id}"
                body = (
                    f"Hello Ozlink IT,%0D%0A%0D%0A"
                    f"Please find attached submission batch {batch_id}.%0D%0A%0D%0A"
                    f"Default zip path:%0D%0A{default_zip}%0D%0A"
                )
                mail_url = QUrl(f"mailto:support@ozlink.it?subject={QUrl.toPercentEncoding(subject).data().decode()}&body={body}")
                QDesktopServices.openUrl(mail_url)
        except Exception as exc:
            log_error("Submit request failed.", error=str(exc))
            QMessageBox.warning(self, "Submit Request", "Could not prepare the current draft.")

    def _load_saved_window_preferences(self):
        settings = QSettings()
        self._saved_window_rect = settings.value("main_window/normal_geometry", QRect(), type=QRect)
        self._saved_window_maximized = settings.value("main_window/maximized", True, type=bool)
        self._saved_window_rect_loaded = isinstance(self._saved_window_rect, QRect) and self._saved_window_rect.isValid()

    def save_window_preferences(self):
        settings = QSettings()
        settings.setValue("main_window/maximized", self.isMaximized())
        normal_rect = self.normalGeometry() if self.isMaximized() else self.geometry()
        if isinstance(normal_rect, QRect) and normal_rect.isValid():
            settings.setValue("main_window/normal_geometry", normal_rect)

    def apply_startup_window_geometry(self):
        print("[window-startup] apply_startup_window_geometry() called")
        self.setWindowState(Qt.WindowNoState)
        self._apply_native_window_flags()

        target_screen = self._current_target_screen()
        available_geometry = target_screen.availableGeometry() if target_screen else QRect(80, 80, 1600, 980)
        safe_normal = available_geometry.adjusted(32, 32, -32, -32)
        self._apply_clamped_normal_geometry(safe_normal, available_geometry)
        print(
            "[window-startup] "
            f"flags={int(self.windowFlags())} "
            f"screen={target_screen.name() if target_screen else 'None'} "
            f"available_geometry={available_geometry.getRect()} "
            f"saved_geometry_loaded={getattr(self, '_saved_window_rect_loaded', False)} "
            f"saved_geometry_rejected=True "
            f"fallback_maximize_used=True"
        )

    def _current_target_screen(self):
        cursor_screen = QGuiApplication.screenAt(QCursor.pos())
        if cursor_screen is not None:
            return cursor_screen

        screen = self.screen()
        if screen is not None:
            return screen

        frame_center = self.frameGeometry().center()
        screen = QGuiApplication.screenAt(frame_center)
        if screen is not None:
            return screen

        return QGuiApplication.primaryScreen()

    def _apply_clamped_normal_geometry(self, target_rect, available_geometry):
        if not isinstance(target_rect, QRect) or target_rect.isEmpty():
            self.setGeometry(available_geometry)
            return

        width = min(max(target_rect.width(), 1100), available_geometry.width())
        height = min(max(target_rect.height(), 760), available_geometry.height())

        x = target_rect.x()
        y = target_rect.y()
        max_x = available_geometry.right() - width + 1
        max_y = available_geometry.bottom() - height + 1
        min_x = available_geometry.x()
        min_y = available_geometry.y()

        if x < min_x or x > max_x:
            x = min(max(x, min_x), max_x)
        if y < min_y or y > max_y:
            y = min(max(y, min_y), max_y)

        clamped = QRect(x, y, width, height)
        if not available_geometry.contains(clamped):
            clamped.moveLeft(max(min_x, min(clamped.x(), max_x)))
            clamped.moveTop(max(min_y, min(clamped.y(), max_y)))

        self.setGeometry(clamped)

    def _saved_rect_is_safe(self, target_rect, available_geometry):
        if not isinstance(target_rect, QRect) or not target_rect.isValid() or target_rect.isEmpty():
            return False

        title_bar_height = 48
        safe_rect = QRect(target_rect)
        safe_rect.setHeight(max(safe_rect.height(), title_bar_height))

        if safe_rect.top() < available_geometry.top():
            return False
        if safe_rect.left() < available_geometry.left():
            return False
        if safe_rect.top() + title_bar_height > available_geometry.bottom():
            return False

        visible_width = min(safe_rect.width(), available_geometry.width())
        visible_height = min(safe_rect.height(), available_geometry.height())
        visible_rect = QRect(safe_rect.x(), safe_rect.y(), visible_width, visible_height)
        return available_geometry.intersects(visible_rect)

    def _apply_native_window_flags(self):
        self.setWindowFlags(
            Qt.Window
            | Qt.WindowTitleHint
            | Qt.WindowSystemMenuHint
            | Qt.WindowMinimizeButtonHint
            | Qt.WindowMaximizeButtonHint
            | Qt.WindowCloseButtonHint
        )

    def _log_post_startup_state(self):
        print(
            "[window-startup] post-show "
            f"state={self._window_state_repr()} "
            f"geometry={self.geometry().getRect()} "
            f"frame_geometry={self.frameGeometry().getRect()}"
        )

    def _window_state_repr(self):
        state = self.windowState()
        return getattr(state, "value", state)

    def _log_post_login_window_state(self, prefix="[window-login]"):
        print(
            f"{prefix} "
            f"post_restore maximized={self.isMaximized()} "
            f"state={self._window_state_repr()} "
            f"geometry={self.geometry().getRect()} "
            f"frame_geometry={self.frameGeometry().getRect()}"
        )

    def _clear_bad_window_preferences(self):
        settings = QSettings()
        settings.remove("main_window/normal_geometry")
        settings.remove("main_window/maximized")
        self._saved_window_rect = QRect()
        self._saved_window_maximized = True
        self._saved_window_rect_loaded = False

    def build_details_panel(self):
        box = QFrame()
        box.setObjectName("SectionBox")
        box.setMinimumWidth(0)
        box.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        layout = QVBoxLayout(box)
        layout.setContentsMargins(6, 4, 6, 4)
        layout.setSpacing(3)

        title = QLabel("Selection Details")
        title.setObjectName("SectionTitle")

        content_grid = QGridLayout()
        content_grid.setContentsMargins(0, 0, 0, 0)
        content_grid.setHorizontalSpacing(7)
        content_grid.setVerticalSpacing(3)

        summary_card = QFrame()
        summary_card.setObjectName("TreeSurface")
        summary_card.setMinimumWidth(0)
        summary_layout = QVBoxLayout(summary_card)
        summary_layout.setContentsMargins(5, 5, 5, 5)
        summary_layout.setSpacing(2)

        self.details_fields = {}

        actions_row = QHBoxLayout()
        actions_row.setContentsMargins(0, 1, 0, 0)
        actions_row.setSpacing(5)

        self.details_action_buttons = {}
        for index, (text, handler) in enumerate([
            ("Open File", self.handle_open_selected_file),
            ("Open in SharePoint", self.handle_open_selected_in_browser),
            ("Copy Link", self.handle_copy_selected_link),
            ("Open Source Folder", self.handle_open_selected_source_folder),
            ("Go to Destination", self.handle_go_to_destination),
        ]):
            button = QPushButton(text)
            button.setMinimumHeight(18)
            button.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            button.clicked.connect(handler)
            button.setEnabled(False)
            self.details_action_buttons[text] = button
            actions_row.addWidget(button)

        metadata_wrap = QFrame()
        metadata_wrap.setObjectName("TreeSurface")
        metadata_wrap.setMinimumWidth(0)
        metadata_layout = QVBoxLayout(metadata_wrap)
        metadata_layout.setContentsMargins(5, 5, 5, 5)
        metadata_layout.setSpacing(3)
        metadata_title = QLabel("Item Metadata")
        metadata_title.setObjectName("HeaderEyebrow")
        metadata_layout.addWidget(metadata_title)
        self.details_metadata_summary = QTextEdit()
        self.details_metadata_summary.setReadOnly(True)
        self.details_metadata_summary.setObjectName("DetailsNotes")
        self.details_metadata_summary.setMinimumHeight(64)
        self.details_metadata_summary.setPlainText("Select an item to review its metadata.")
        metadata_layout.addWidget(self.details_metadata_summary, 1)

        summary_layout.addWidget(metadata_wrap, 1)
        summary_layout.addLayout(actions_row)

        side_stack = QVBoxLayout()
        side_stack.setContentsMargins(0, 0, 0, 0)
        side_stack.setSpacing(4)

        preview_card = QFrame()
        preview_card.setObjectName("TreeSurface")
        preview_card.setMinimumWidth(140)
        preview_card.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        preview_layout = QVBoxLayout(preview_card)
        preview_layout.setContentsMargins(5, 5, 5, 5)
        preview_layout.setSpacing(2)
        preview_title = QLabel("Preview")
        preview_title.setObjectName("HeaderEyebrow")
        self.details_preview = QTextEdit()
        self.details_preview.setReadOnly(True)
        self.details_preview.setObjectName("DetailsNotes")
        self.details_preview.setMinimumHeight(64)
        self.details_preview.setPlainText("Preview not available in this version.")
        preview_layout.addWidget(preview_title)
        preview_layout.addWidget(self.details_preview, 1)

        self.details_notes = QTextEdit()
        self.details_notes.setReadOnly(True)
        self.details_notes.setObjectName("DetailsNotes")
        self.details_notes.setMinimumHeight(40)
        self.details_notes.setMaximumHeight(48)
        self.details_notes.setPlainText("Selection guidance and allocation notes will appear here.")
        self.details_notes.hide()

        side_stack.addWidget(preview_card, 1)

        content_grid.addWidget(summary_card, 0, 0)
        content_grid.addLayout(side_stack, 0, 1)
        content_grid.setColumnStretch(0, 7)
        content_grid.setColumnStretch(1, 3)

        layout.addWidget(title)
        layout.addLayout(content_grid, 1)

        return box

    def build_planned_moves_panel(self):
        box = QFrame()
        box.setObjectName("SectionBox")
        layout = QVBoxLayout(box)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(8)

        loading_banner = QFrame()
        loading_banner.setObjectName("SoftBanner")
        loading_banner.hide()
        loading_layout = QVBoxLayout(loading_banner)
        loading_layout.setContentsMargins(12, 10, 12, 10)
        loading_layout.setSpacing(2)

        loading_title = QLabel("Please wait while we load your SharePoint sites.")
        loading_title.setObjectName("SectionTitle")
        loading_detail = QLabel("SharePoint content and planning data are still loading.")
        loading_detail.setObjectName("CardBody")
        loading_detail.setWordWrap(True)

        loading_layout.addWidget(loading_title)
        loading_layout.addWidget(loading_detail)

        table = QTableWidget(0, 5)
        table.setHorizontalHeaderLabels([
            "Source Name",
            "Source Path",
            "Destination Name",
            "Destination Path",
            "Status",
        ])
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.setSelectionMode(QAbstractItemView.SingleSelection)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.verticalHeader().setVisible(False)
        table.horizontalHeader().setStretchLastSection(True)
        table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
        table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)

        status = QLabel("No planned moves yet.")
        status.setObjectName("MutedText")
        status.setWordWrap(True)

        self.planned_moves_loading_banner = loading_banner
        self.planned_moves_loading_title = loading_title
        self.planned_moves_loading_detail = loading_detail

        simulate_row = QHBoxLayout()
        self.simulate_run_manifest_button = QPushButton("Simulate run (save manifest)…")
        self.simulate_run_manifest_button.setToolTip(
            "Save a JSON file listing planned file steps and proposed folders. "
            "Nothing is copied or moved. Use the Execution page to dry-run or run eligible local paths."
        )
        self.simulate_run_manifest_button.clicked.connect(self._on_simulate_run_save_manifest)
        self.run_manifest_button = QPushButton("Run manifest (local paths)…")
        self.run_manifest_button.setToolTip(
            "Open a saved manifest and run local copies/mkdir for absolute Windows paths "
            "(C:\\… or UNC). SharePoint-only paths are skipped; Graph execution is not implemented yet."
        )
        self.run_manifest_button.clicked.connect(self._on_run_transfer_manifest)
        simulate_row.addWidget(self.simulate_run_manifest_button)
        simulate_row.addWidget(self.run_manifest_button)
        simulate_row.addStretch(1)

        layout.addWidget(loading_banner)
        layout.addWidget(table, 1)
        layout.addLayout(simulate_row)
        layout.addWidget(status)

        return box, table, status

    def _planning_workspace_loading_message(self):
        if not self.current_session_context.get("connected"):
            return ""

        if self.discovery_worker and self.discovery_worker.isRunning():
            return "Loading SharePoint sites and libraries. Your planned moves will appear here shortly."

        loading_selector = False
        for label_text in ("Source Site", "Source Library", "Destination Site", "Destination Library"):
            selector = self.planning_inputs.get(label_text) if hasattr(self, "planning_inputs") else None
            if selector is None:
                continue
            if str(selector.currentText() or "").strip().lower().startswith("loading"):
                loading_selector = True
                break

        active_root_workers = any(
            bool(entry and entry.get("worker") and entry["worker"].isRunning())
            for entry in self.root_load_workers.values()
        )
        if loading_selector or active_root_workers:
            return "Loading SharePoint content and restoring your planning workspace. Planned moves will appear here shortly."

        return ""

    def _refresh_planning_loading_banner(self):
        banner = getattr(self, "planning_loading_banner", None)
        if banner is None:
            return
        message = self._planning_workspace_loading_message()
        if message:
            detail = "Please wait while we load your SharePoint sites and libraries."
            if self.discovery_worker and self.discovery_worker.isRunning():
                detail = "Please wait while we discover your accessible SharePoint sites and document libraries."
            self.planning_loading_label.setText(detail)
            self.planning_loading_loading_detail.setText(message)
            self._apply_planning_loading_banner_visual_state(True)
            banner.show()
        else:
            self._apply_planning_loading_banner_visual_state(False)
            banner.hide()

    def update_dashboard_session_summary(self, connected, display_name="", tenant="", role="user"):
        if hasattr(self, "dashboard_stack"):
            self.dashboard_stack.setCurrentIndex(1 if connected else 0)

        if not hasattr(self, "dashboard_session_state"):
            return

        if connected:
            self.dashboard_session_state.setText("Connection: Connected")
            self.dashboard_session_operator.setText(f"Operator: {display_name or 'Connected User'}")
            self.dashboard_session_tenant.setText(f"Tenant: {tenant or 'Connected'}")
            self.dashboard_session_mode.setText(f"Mode: {role.title()}")
            self.dashboard_next_step.setText(
                "Your planning session is connected, but the source and destination context are not loaded yet. Please contact Ozlink IT if this does not update."
            )
        else:
            self.dashboard_session_state.setText("Connection: Not connected")
            self.dashboard_session_operator.setText("Operator: Not signed in")
            self.dashboard_session_tenant.setText("Tenant: Not connected")
            self.dashboard_session_mode.setText("Mode: Client")
            self.dashboard_next_step.setText(
                "Ready to open your planning session."
            )

    def count_tree_items(self, tree):
        if isinstance(tree, QTreeView) and tree.model() is getattr(self, "source_sharepoint_model", None):
            model = tree.model()
            total_count = 0
            for ix in model.iter_depth_first():
                data = ix.data(Qt.UserRole) or {}
                if not data.get("placeholder"):
                    total_count += 1
            return total_count

        def count_item(item):
            data = item.data(0, Qt.UserRole) or {}
            total = 0 if data.get("placeholder") else 1
            for index in range(item.childCount()):
                total += count_item(item.child(index))
            return total

        total_count = 0
        for index in range(tree.topLevelItemCount()):
            total_count += count_item(tree.topLevelItem(index))
        return total_count

    def _current_selected_source_drive_id(self):
        if not hasattr(self, "planning_inputs"):
            return ""
        selector = self.planning_inputs.get("Source Library")
        if selector is None:
            return ""
        library = selector.currentData()
        if isinstance(library, dict):
            return library.get("id", "")
        return ""

    def _current_selected_destination_drive_id(self):
        if not hasattr(self, "planning_inputs"):
            return ""
        selector = self.planning_inputs.get("Destination Library")
        if selector is None:
            return ""
        library = selector.currentData()
        if isinstance(library, dict):
            return library.get("id", "")
        return ""

    def _graph_file_count_display(self):
        current_drive_id = self._current_selected_source_drive_id()
        if not current_drive_id:
            return "0"
        if (
            self.full_source_file_count is not None
            and self._full_count_completed_drive_id == current_drive_id
        ):
            return str(int(self.full_source_file_count))
        if self._full_count_error_message and self._full_count_requested_drive_id == current_drive_id:
            return "—"
        if self._full_count_requested_drive_id == current_drive_id:
            return "…"
        if getattr(self, "_sharepoint_lazy_mode", False):
            return "On demand"
        return "0"

    def _graph_folder_count_display(self):
        current_drive_id = self._current_selected_source_drive_id()
        if not current_drive_id:
            return "0"
        if (
            self.full_source_folder_count is not None
            and self._full_count_completed_drive_id == current_drive_id
        ):
            return str(int(self.full_source_folder_count))
        if self._full_count_error_message and self._full_count_requested_drive_id == current_drive_id:
            return "—"
        if self._full_count_requested_drive_id == current_drive_id:
            return "…"
        if getattr(self, "_sharepoint_lazy_mode", False):
            return "On demand"
        return "0"

    def _update_source_count_labels(self, loaded_items):
        self._cached_loaded_source_items = int(loaded_items or 0)
        loaded_text = str(loaded_items)
        files_text = self._graph_file_count_display()
        folders_text = self._graph_folder_count_display()

        for label in [
            getattr(self, "dashboard_loaded_items", None),
            getattr(self, "workspace_loaded_items", None),
        ]:
            if label is not None:
                label.setText(loaded_text)

        for label in [
            getattr(self, "dashboard_graph_files", None),
            getattr(self, "workspace_graph_files", None),
        ]:
            if label is not None:
                label.setText(files_text)

        for label in [
            getattr(self, "dashboard_graph_folders", None),
            getattr(self, "workspace_graph_folders", None),
        ]:
            if label is not None:
                label.setText(folders_text)

    def _reset_full_count_state(self):
        self.full_source_item_count = None
        self.full_source_file_count = None
        self.full_source_folder_count = None
        self._full_count_error_message = ""
        self._full_count_requested_drive_id = ""
        self._full_count_completed_drive_id = ""
        self._update_source_count_labels(0)

    def _reset_destination_full_tree_state(self):
        self._destination_full_tree_snapshot = []
        self._destination_full_tree_requested_drive_id = ""
        self._destination_full_tree_completed_drive_id = ""
        self._destination_full_tree_materialization_pending = False
        timer = getattr(self, "_destination_full_tree_materialize_timer", None)
        if timer is not None:
            timer.stop()

    def _destination_full_tree_context(self):
        if not hasattr(self, "planning_inputs"):
            return {}
        destination_site_selector = self.planning_inputs.get("Destination Site")
        destination_library_selector = self.planning_inputs.get("Destination Library")
        site = destination_site_selector.currentData() if destination_site_selector is not None else {}
        library = destination_library_selector.currentData() if destination_library_selector is not None else {}
        site = site if isinstance(site, dict) else {}
        library = library if isinstance(library, dict) else {}
        return {
            "site_id": site.get("id", ""),
            "site_name": site.get("name", ""),
            "library_id": library.get("id", ""),
            "library_name": library.get("name", ""),
            "tree_role": "destination",
        }

    def _destination_full_tree_ready(self):
        current_drive_id = self._current_selected_destination_drive_id() or self.pending_root_drive_ids.get("destination", "")
        return bool(
            current_drive_id
            and self._destination_full_tree_snapshot
            and self._destination_full_tree_completed_drive_id == current_drive_id
        )

    def _schedule_destination_full_tree_materialization(self, delay_ms=2500):
        timer = getattr(self, "_destination_full_tree_materialize_timer", None)
        if timer is None:
            self._maybe_materialize_destination_full_tree_snapshot()
            return
        timer.start(max(0, int(delay_ms)))

    def _schedule_lazy_destination_projection_refresh(self, reason, delay_ms=2200):
        if getattr(self, "_sharepoint_lazy_mode", False):
            return
        self._lazy_destination_projection_pending_reason = str(reason or "lazy_projection")
        timer = getattr(self, "_lazy_destination_projection_timer", None)
        if timer is None:
            self._run_lazy_destination_projection_refresh()
            return
        timer.start(max(0, int(delay_ms)))

    def _run_lazy_destination_projection_refresh(self):
        if getattr(self, "_sharepoint_lazy_mode", False):
            self._lazy_destination_projection_pending_reason = ""
            return
        reason = getattr(self, "_lazy_destination_projection_pending_reason", "") or "lazy_projection"
        if self._root_tree_bind_in_progress or self.pending_folder_loads.get("destination") or self._expand_all_pending.get("destination"):
            self._schedule_lazy_destination_projection_refresh(reason, 1800)
            return
        self._lazy_destination_projection_pending_reason = ""
        self._materialize_destination_future_model(f"lazy_{reason}")

    def _should_defer_destination_materialization(self, reason):
        destination_tree = getattr(self, "destination_tree_widget", None)
        if destination_tree is None:
            return False
        if self._expand_all_pending.get("destination"):
            return True
        if self.pending_folder_loads.get("destination"):
            return True
        if self._root_tree_bind_in_progress:
            return True
        large_tree = self._count_expandable_tree_nodes("destination") > 120
        heavy_reasons = {
            "root_bind",
            "folder_worker_success",
            "destination_expand_all_complete",
            "destination_fast_expand_all",
            "source_expand_all_complete",
            "destination_expand_all_full_tree",
            "destination_full_tree_idle_success",
            "destination_full_tree_success",
        }
        return large_tree and reason in heavy_reasons

    def _destination_future_model_blocked_by_source_restore(self, reason):
        """Avoid building/binding destination projection while source folder loads are in flight.

        Previously we also blocked when `_source_restore_materialization_queue` was non-empty. That
        queue can hold many deferred branch jobs while **no** `FolderLoadWorker` is running
        (`pending_source_folder_loads` == 0), which blocked destination materialization for a long
        time and produced logs like `destination_future_model_materialize_deferred` with
        `waiting_for_source_restore` + large `source_restore_queue_size` while loads were idle.
        """
        skip = {
            "destination_expand_all_full_tree",
            "destination_root_error_fallback",
            "destination_full_tree_idle_success",
            "destination_expand_all_complete",
            "destination_fast_expand_all",
            "source_expand_all_complete",
        }
        if reason in skip or str(reason or "").startswith("source_restore_flush_"):
            return False
        if not self.planned_moves:
            return False
        pending_source = self.pending_folder_loads.get("source") or set()
        if len(pending_source) > 0:
            return True
        return False

    def _try_flush_destination_future_model_after_source_restore(self, trigger=""):
        if getattr(self, "_sharepoint_lazy_mode", False):
            self._destination_future_model_pending_after_source_restore = False
            return
        if self._expand_all_pending.get("source"):
            return
        if not self._destination_future_model_pending_after_source_restore:
            return
        if self._destination_future_model_blocked_by_source_restore("source_restore_flush"):
            return
        self._materialize_destination_future_model(f"source_restore_flush_{trigger}", allow_defer=False)

    def _schedule_deferred_destination_materialization(self, reason, delay_ms=180):
        self._destination_idle_materialize_pending_reason = str(reason or "idle_destination_materialize")
        timer = getattr(self, "_destination_idle_materialize_timer", None)
        if timer is None:
            self._run_deferred_destination_materialization()
            return
        # Do not override expand-all status or flash "Finalizing..." on a tight timer while
        # destination expand-all is active (materialization is deferred until loads finish).
        if not self._expand_all_pending.get("destination"):
            self._set_tree_status_message("destination", "Finalizing destination structure...", loading=True)
        timer.stop()
        timer.start(max(0, int(delay_ms)))

    def _run_deferred_destination_materialization(self):
        reason = getattr(self, "_destination_idle_materialize_pending_reason", "") or "idle_destination_materialize"
        if self._expand_all_pending.get("destination") or self.pending_folder_loads.get("destination") or self._root_tree_bind_in_progress:
            self._schedule_deferred_destination_materialization(reason, delay_ms=220)
            return
        if getattr(self, "_destination_chunked_bind_state", None) is not None:
            self._schedule_deferred_destination_materialization(reason, delay_ms=220)
            return
        if getattr(self, "_destination_snapshot_chunked_restore_active", False):
            self._schedule_deferred_destination_materialization(reason, delay_ms=220)
            return
        if getattr(self, "_destination_future_projection_async_state", None) is not None:
            return
        self._destination_idle_materialize_pending_reason = ""
        applied_count = self._materialize_destination_future_model(
            reason, allow_defer=False, prefer_chunked_projection=True
        )
        if getattr(self, "_destination_future_model_last_blocked_source_restore", False):
            self._destination_idle_materialize_pending_reason = reason
            self._schedule_deferred_destination_materialization(reason, delay_ms=220)
            return
        if getattr(self, "_destination_future_projection_async_state", None) is not None:
            return
        if getattr(self, "_destination_chunked_bind_state", None) is not None:
            self._destination_idle_materialize_pending_reason = reason
            self._schedule_deferred_destination_materialization(reason, delay_ms=220)
            return
        if applied_count or not self.pending_folder_loads.get("destination"):
            self._set_tree_status_message("destination", "Destination structure ready.", loading=False)

    def _maybe_materialize_destination_full_tree_snapshot(self):
        if not self._destination_full_tree_materialization_pending:
            return
        if self._expand_all_pending.get("destination"):
            self._schedule_destination_full_tree_materialization(2500)
            return
        if self.pending_folder_loads.get("destination"):
            self._schedule_destination_full_tree_materialization(2500)
            return
        if self._root_tree_bind_in_progress:
            self._schedule_destination_full_tree_materialization(2000)
            return
        if getattr(self, "_destination_chunked_bind_state", None) is not None:
            self._schedule_destination_full_tree_materialization(2500)
            return
        if getattr(self, "_destination_snapshot_chunked_restore_active", False):
            self._schedule_destination_full_tree_materialization(2500)
            return
        if getattr(self, "_destination_future_projection_async_state", None) is not None:
            self._schedule_destination_full_tree_materialization(2500)
            return
        destination_tree = getattr(self, "destination_tree_widget", None)
        source_tree = getattr(self, "source_tree_widget", None)
        if (destination_tree is not None and destination_tree.hasFocus()) or (
            source_tree is not None and source_tree.hasFocus()
        ):
            self._schedule_destination_full_tree_materialization(2000)
            return
        self._destination_full_tree_materialization_pending = False
        self._materialize_destination_future_model("destination_full_tree_idle_success")

    def start_destination_full_tree_worker(self, drive_id):
        if not drive_id:
            self._reset_destination_full_tree_state()
            return

        if (
            self._destination_full_tree_worker is not None
            and self._destination_full_tree_worker.isRunning()
            and self._destination_full_tree_requested_drive_id == drive_id
        ):
            return

        if (
            self._destination_full_tree_snapshot
            and self._destination_full_tree_completed_drive_id == drive_id
        ):
            return

        if self._destination_full_tree_worker is not None and self._destination_full_tree_worker.isRunning():
            self._retired_destination_full_tree_workers[self._active_destination_full_tree_worker_id] = self._destination_full_tree_worker

        self._destination_full_tree_snapshot = []
        self._destination_full_tree_requested_drive_id = drive_id
        self._destination_full_tree_completed_drive_id = ""
        self._destination_full_tree_sequence += 1
        worker_id = self._destination_full_tree_sequence
        worker = DestinationFullTreeWorker(self.graph, drive_id, self._destination_full_tree_context())
        self._destination_full_tree_worker = worker
        self._active_destination_full_tree_worker_id = worker_id
        log_info("destination_full_tree_started", drive_id=drive_id)
        worker.success.connect(lambda payload, worker_id=worker_id: self._safe_invoke("destination_full_tree.success", self.on_destination_full_tree_success, payload, worker_id))
        worker.error.connect(lambda payload, worker_id=worker_id: self._safe_invoke("destination_full_tree.error", self.on_destination_full_tree_error, payload, worker_id))
        worker.finished.connect(lambda worker_id=worker_id: self._safe_invoke("destination_full_tree.finished", self.on_destination_full_tree_finished, worker_id))
        worker.start()

    def on_destination_full_tree_success(self, payload, worker_id):
        drive_id = payload.get("drive_id", "")
        if worker_id != self._active_destination_full_tree_worker_id or drive_id != self._destination_full_tree_requested_drive_id:
            return

        snapshot_entries = []
        for item in payload.get("items", []):
            semantic_path = self._canonical_destination_projection_path(
                item.get("display_path") or item.get("item_path") or item.get("destination_path") or ""
            )
            if not semantic_path:
                continue
            item_data = dict(item)
            item_data["children_loaded"] = True
            snapshot_entries.append({
                "semantic_path": semantic_path,
                "parent_semantic_path": self.normalize_memory_path("\\".join(self._path_segments(semantic_path)[:-1])),
                "data": item_data,
                "children": [],
            })

        self._destination_full_tree_snapshot = snapshot_entries
        self._destination_full_tree_completed_drive_id = drive_id
        log_info("destination_full_tree_completed", drive_id=drive_id, total_count=len(snapshot_entries))
        if self._destination_expand_all_after_full_tree:
            self._destination_full_tree_materialization_pending = False
            # expand_all_pending is still True here; defer would block materialize until later.
            self._materialize_destination_future_model(
                "destination_expand_all_full_tree",
                allow_defer=False,
                prefer_chunked_projection=True,
            )
            self._destination_expand_all_after_full_tree = False
            button = self._expand_all_button_for_panel("destination")
            if button is not None:
                button.setEnabled(True)
            if self._can_fast_bulk_expand("destination"):
                self._fast_expand_all_loaded_tree("destination")
            else:
                self._expand_all_pending["destination"] = True
                self._reset_expand_all_progress("destination")
                self._set_expand_all_button_label("destination", True)
                self._continue_expand_all("destination")
        else:
            self._destination_full_tree_materialization_pending = True
            self._schedule_destination_full_tree_materialization(4000)

    def on_destination_full_tree_error(self, payload, worker_id):
        drive_id = payload.get("drive_id", "")
        if worker_id != self._active_destination_full_tree_worker_id or drive_id != self._destination_full_tree_requested_drive_id:
            return
        self._destination_full_tree_snapshot = []
        self._destination_full_tree_completed_drive_id = ""
        self._destination_full_tree_materialization_pending = False
        self._destination_expand_all_after_full_tree = False
        self._expand_all_pending["destination"] = False
        button = self._expand_all_button_for_panel("destination")
        if button is not None:
            button.setEnabled(True)
            self._set_expand_all_button_label("destination", False)
        log_warn("destination_full_tree_failed", drive_id=drive_id, error=payload.get("error", "Unknown error"))

    def on_destination_full_tree_finished(self, worker_id):
        if worker_id == self._active_destination_full_tree_worker_id:
            worker = self._destination_full_tree_worker
            self._destination_full_tree_worker = None
        else:
            worker = self._retired_destination_full_tree_workers.pop(worker_id, None)
        if worker is not None:
            worker.deleteLater()

    def start_full_count_worker(self, drive_id):
        if not drive_id:
            self._reset_full_count_state()
            return

        if (
            self.full_count_worker is not None
            and self.full_count_worker.isRunning()
            and self._full_count_requested_drive_id == drive_id
        ):
            self._update_source_count_labels(
                self.count_tree_items(self.source_tree_widget) if hasattr(self, "source_tree_widget") else 0
            )
            return

        if (
            self.full_source_file_count is not None
            and self.full_source_folder_count is not None
            and self._full_count_completed_drive_id == drive_id
        ):
            self._update_source_count_labels(
                self.count_tree_items(self.source_tree_widget) if hasattr(self, "source_tree_widget") else 0
            )
            return

        if self.full_count_worker is not None and self.full_count_worker.isRunning():
            self._retired_full_count_workers[self._active_full_count_worker_id] = self.full_count_worker

        self.full_source_item_count = None
        self.full_source_file_count = None
        self.full_source_folder_count = None
        self._full_count_error_message = ""
        self._full_count_requested_drive_id = drive_id
        self._full_count_completed_drive_id = ""
        self._update_source_count_labels(
            self.count_tree_items(self.source_tree_widget) if hasattr(self, "source_tree_widget") else 0
        )

        self._full_count_sequence += 1
        worker_id = self._full_count_sequence
        worker = FullCountWorker(self.graph, drive_id)
        self.full_count_worker = worker
        self._active_full_count_worker_id = worker_id

        worker.success.connect(lambda payload, worker_id=worker_id: self._safe_invoke("full_count.success", self.on_full_count_success, payload, worker_id))
        worker.error.connect(lambda payload, worker_id=worker_id: self._safe_invoke("full_count.error", self.on_full_count_error, payload, worker_id))
        worker.finished.connect(lambda worker_id=worker_id: self._safe_invoke("full_count.finished", self.on_full_count_finished, worker_id))
        log_info("full_count_started", drive_id=drive_id)
        worker.start()

    def _heavy_session_for_background_graph_walk(self):
        ep = getattr(self, "_expand_all_pending", None) or {}
        return (
            len(self.planned_moves) > 12
            or len(getattr(self, "_source_restore_materialization_queue", []) or []) > 15
            or bool(getattr(self, "_memory_restore_in_progress", False))
            or bool(ep.get("source"))
            or bool(ep.get("destination"))
        )

    def _schedule_full_count_with_restore_backoff(self, drive_id):
        if not drive_id:
            return
        if self._heavy_session_for_background_graph_walk():
            delay_ms = 42000
            log_info(
                "full_count_deferred",
                drive_id=drive_id,
                delay_ms=delay_ms,
                reason="heavy_restore_session",
                planned_moves=len(self.planned_moves),
            )

            def _deferred():
                if self._current_selected_source_drive_id() != drive_id:
                    return
                if self.full_count_worker is not None and self.full_count_worker.isRunning():
                    return
                self.start_full_count_worker(drive_id)

            QTimer.singleShot(delay_ms, _deferred)
            return
        self.start_full_count_worker(drive_id)

    def on_full_count_success(self, payload, worker_id):
        drive_id = payload.get("drive_id", "")
        total_count = int(payload.get("total_count", 0))
        file_count = int(payload.get("file_count", 0))
        folder_count = int(payload.get("folder_count", 0))
        if file_count == 0 and folder_count == 0 and total_count > 0:
            file_count = total_count
            folder_count = 0
        if worker_id != self._active_full_count_worker_id or drive_id != self._full_count_requested_drive_id:
            return
        self.full_source_item_count = total_count
        self.full_source_file_count = file_count
        self.full_source_folder_count = folder_count
        self._full_count_completed_drive_id = drive_id
        self._full_count_error_message = ""
        log_info(
            "full_count_completed",
            drive_id=drive_id,
            total_count=total_count,
            file_count=file_count,
            folder_count=folder_count,
        )
        self._update_source_count_labels(
            self.count_tree_items(self.source_tree_widget) if hasattr(self, "source_tree_widget") else 0
        )

    def on_full_count_error(self, payload, worker_id):
        drive_id = payload.get("drive_id", "")
        if worker_id != self._active_full_count_worker_id or drive_id != self._full_count_requested_drive_id:
            return
        self.full_source_item_count = None
        self.full_source_file_count = None
        self.full_source_folder_count = None
        self._full_count_completed_drive_id = ""
        self._full_count_error_message = payload.get("error", "Unknown counting error.")
        log_warn("full_count_failed", drive_id=drive_id, error=self._full_count_error_message)
        self._update_source_count_labels(
            self.count_tree_items(self.source_tree_widget) if hasattr(self, "source_tree_widget") else 0
        )

    def on_full_count_finished(self, worker_id):
        if worker_id == self._active_full_count_worker_id:
            worker = self.full_count_worker
            self.full_count_worker = None
        else:
            worker = self._retired_full_count_workers.pop(worker_id, None)
        if worker is not None:
            worker.deleteLater()

    def update_progress_summaries(self):
        if getattr(self, "_sharepoint_lazy_mode", False):
            if self._planning_workspace_is_busy() or getattr(self, "_memory_restore_in_progress", False):
                loaded_items = int(getattr(self, "_cached_loaded_source_items", 0) or 0)
            else:
                loaded_items = self.count_tree_items(self.source_tree_widget) if hasattr(self, "source_tree_widget") else 0
            planned_items = len(self.planned_moves)
            self._update_source_count_labels(loaded_items)
            for label in [
                getattr(self, "dashboard_planned_items", None),
                getattr(self, "workspace_planned_items", None),
            ]:
                if label is not None:
                    label.setText(str(planned_items))
            existing_not_planned = len(getattr(self, "_workflow_not_planned_rows", []))
            existing_needs_review = len(getattr(self, "_workflow_needs_review_rows", []))
            for label in [
                getattr(self, "dashboard_not_planned_items", None),
                getattr(self, "workspace_not_planned_items", None),
            ]:
                if label is not None:
                    label.setText(str(existing_not_planned))
            for label in [
                getattr(self, "dashboard_needs_review_items", None),
                getattr(self, "workspace_needs_review_items", None),
            ]:
                if label is not None:
                    label.setText(str(existing_needs_review))
            if self._planning_workspace_is_busy():
                self._schedule_progress_summary_refresh(2500)
            return

        workflow_state = self._compute_planning_workflow_state()
        loaded_items = workflow_state["total_items"]
        planned_items = len(self.planned_moves)
        not_planned_items = workflow_state["not_planned_count"]
        needs_review_items = workflow_state["needs_review_count"]

        self._apply_planning_workflow_state(workflow_state)
        self._update_source_count_labels(loaded_items)

        for label in [
            getattr(self, "dashboard_planned_items", None),
            getattr(self, "workspace_planned_items", None),
        ]:
            if label is not None:
                label.setText(str(planned_items))

        for label in [
            getattr(self, "dashboard_not_planned_items", None),
            getattr(self, "workspace_not_planned_items", None),
        ]:
            if label is not None:
                label.setText(str(not_planned_items))

        for label in [
            getattr(self, "dashboard_needs_review_items", None),
            getattr(self, "workspace_needs_review_items", None),
        ]:
            if label is not None:
                label.setText(str(needs_review_items))

    def _refresh_workflow_state_on_demand(self):
        workflow_state = self._compute_planning_workflow_state()
        self._apply_planning_workflow_state(workflow_state)

        not_planned_items = workflow_state["not_planned_count"]
        needs_review_items = workflow_state["needs_review_count"]

        for label in [
            getattr(self, "dashboard_not_planned_items", None),
            getattr(self, "workspace_not_planned_items", None),
        ]:
            if label is not None:
                label.setText(str(not_planned_items))

        for label in [
            getattr(self, "dashboard_needs_review_items", None),
            getattr(self, "workspace_needs_review_items", None),
        ]:
            if label is not None:
                label.setText(str(needs_review_items))

    def on_workspace_tab_changed(self, index):
        if not getattr(self, "_sharepoint_lazy_mode", False):
            if hasattr(self, "workspace_tabs") and getattr(self, "_workspace_tabs_collapsed", False):
                self._apply_workspace_tabs_collapsed_state(False)
            return
        if not hasattr(self, "workspace_tabs"):
            return
        if getattr(self, "_workspace_tabs_collapsed", False):
            self._apply_workspace_tabs_collapsed_state(False)
        widget = self.workspace_tabs.widget(index)
        if widget in {getattr(self, "suggestions_box", None), getattr(self, "needs_review_box", None)}:
            self._schedule_safe_timer(0, "workspace_tab_workflow_refresh", self._refresh_workflow_state_on_demand)

    def _planning_header_collapsed_height(self):
        if not hasattr(self, "planning_header_toggle_button"):
            return 40
        return max(38, self.planning_header_toggle_button.sizeHint().height() + 12)

    def _apply_planning_header_collapsed_state(self, collapsed):
        header_content = getattr(self, "planning_header_content", None)
        toggle_button = getattr(self, "planning_header_toggle_button", None)
        if header_content is None or toggle_button is None:
            return
        self._planning_header_collapsed = bool(collapsed)
        header_content.setVisible(not self._planning_header_collapsed)
        header_frame = header_content.parentWidget()
        if self._planning_header_collapsed:
            collapsed_height = self._planning_header_collapsed_height()
            header_frame.setMinimumHeight(collapsed_height)
            header_frame.setMaximumHeight(collapsed_height)
            header_frame.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            toggle_button.setText("Expand Header")
        else:
            header_frame.setMinimumHeight(self._planning_header_expanded_min_height)
            header_frame.setMaximumHeight(16777215)
            header_frame.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Maximum)
            toggle_button.setText("Collapse Header")
        header_frame.updateGeometry()
        header_frame.repaint()

    def toggle_planning_header_collapsed(self):
        self._apply_planning_header_collapsed_state(not getattr(self, "_planning_header_collapsed", False))

    def _workspace_tabs_collapsed_height(self):
        if not hasattr(self, "workspace_tabs"):
            return 42
        tab_bar = self.workspace_tabs.tabBar()
        if tab_bar is None:
            return 42
        return max(38, tab_bar.sizeHint().height() + 10)

    def _apply_workspace_tabs_collapsed_state(self, collapsed):
        if not hasattr(self, "workspace_tabs"):
            return
        self._workspace_tabs_collapsed = bool(collapsed)
        if self._workspace_tabs_collapsed:
            collapsed_height = self._workspace_tabs_collapsed_height()
            self.workspace_tabs.setMinimumHeight(collapsed_height)
            self.workspace_tabs.setMaximumHeight(collapsed_height)
            self.workspace_tabs.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            self.workspace_tabs_toggle_button.setText("Expand Panel")
            self.workspace_tabs.setDocumentMode(True)
        else:
            self.workspace_tabs.setMinimumHeight(self._workspace_tabs_expanded_min_height)
            self.workspace_tabs.setMaximumHeight(self._workspace_tabs_expanded_max_height)
            self.workspace_tabs.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
            self.workspace_tabs_toggle_button.setText("Collapse Panel")
            self.workspace_tabs.setDocumentMode(False)
        self.workspace_tabs.updateGeometry()
        self.workspace_tabs.repaint()

    def toggle_workspace_tabs_collapsed(self):
        self._apply_workspace_tabs_collapsed_state(not getattr(self, "_workspace_tabs_collapsed", False))

    def _source_parent_path(self, source_path):
        segments = self._path_segments(self._canonical_source_projection_path(source_path))
        if len(segments) <= 1:
            return ""
        return "\\".join(segments[:-1])

    def _destination_parent_path(self, destination_path):
        segments = self._path_segments(self._canonical_destination_projection_path(destination_path))
        if len(segments) <= 1:
            return ""
        return "\\".join(segments[:-1])

    def _show_proposed_path_builder_dialog(self, base_path):
        dialog = QDialog(self)
        dialog.setWindowTitle("New Proposed Folder")
        dialog.setModal(True)
        dialog.resize(520, 170)

        layout = QVBoxLayout(dialog)
        form = QFormLayout()

        base_path_label = QLabel(base_path or "Root")
        base_path_label.setWordWrap(True)
        relative_path_input = QLineEdit()
        relative_path_input.setPlaceholderText(r"Example: Projects\Completed\YMCA")
        preview_label = QLabel(base_path or "Root")
        preview_label.setWordWrap(True)
        preview_label.setObjectName("MutedText")

        def update_preview():
            relative_path = self.normalize_memory_path(relative_path_input.text())
            if relative_path:
                preview_label.setText(
                    self.normalize_memory_path("\\".join(part for part in [base_path, relative_path] if part))
                )
            else:
                preview_label.setText(base_path or "Root")

        relative_path_input.textChanged.connect(lambda _text: update_preview())
        update_preview()

        form.addRow("Base path:", base_path_label)
        form.addRow("Folder or relative path:", relative_path_input)
        form.addRow("Preview:", preview_label)
        layout.addLayout(form)

        hint = QLabel("Add a single folder name or a nested relative path. Click OK to create the proposed branch.")
        hint.setWordWrap(True)
        hint.setObjectName("MutedText")
        layout.addWidget(hint)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        relative_path_input.setFocus()
        if dialog.exec() != QDialog.Accepted:
            return "", False
        return self.normalize_memory_path(relative_path_input.text()), True

    def _most_common_path(self, paths):
        counts = {}
        first_seen = {}
        for index, path in enumerate(paths):
            if not path:
                continue
            counts[path] = counts.get(path, 0) + 1
            first_seen.setdefault(path, index)
        if not counts:
            return "", 0, 0
        ordered = sorted(counts.items(), key=lambda item: (-item[1], first_seen[item[0]], item[0].lower()))
        best_path, best_count = ordered[0]
        return best_path, best_count, sum(counts.values())

    def _iter_visible_source_nodes(self):
        tree = getattr(self, "source_tree_widget", None)
        if tree is None:
            return []
        visible_nodes = []
        seen_paths = set()
        for index in range(tree.topLevelItemCount()):
            for item in self._iter_tree_items(tree.topLevelItem(index)):
                node_data = item.data(0, Qt.UserRole) or {}
                if node_data.get("placeholder"):
                    continue
                canonical_path = self._canonical_source_projection_path(self._tree_item_path(node_data))
                if not canonical_path or canonical_path in seen_paths:
                    continue
                seen_paths.add(canonical_path)
                visible_nodes.append(dict(node_data))
        visible_nodes.sort(key=lambda node: self._canonical_source_projection_path(self._tree_item_path(node)).lower())
        return visible_nodes

    def _build_suggestion_for_source_node(self, node_data):
        source_name = node_data.get("name", "Unnamed Item")
        source_path = self._canonical_source_projection_path(self._tree_item_path(node_data))
        if not source_path:
            return None

        source_parent = self._source_parent_path(source_path)
        sibling_destination_parents = []
        nearby_destination_parents = []
        repeated_name_destination_parents = []
        source_segments = self._path_segments(source_path)

        for move in self.planned_moves:
            move_source_path = self._canonical_source_projection_path(move.get("source_path", ""))
            if not move_source_path or self._paths_equivalent(move_source_path, source_path, "source"):
                continue
            destination_parent = self._canonical_destination_projection_path(self._allocation_parent_path(move))
            if not destination_parent:
                continue

            move_source_parent = self._source_parent_path(move_source_path)
            if source_parent and move_source_parent == source_parent:
                sibling_destination_parents.append(destination_parent)

            move_name = str(move.get("source_name", "")).strip().lower()
            if move_name and move_name == str(source_name).strip().lower():
                repeated_name_destination_parents.append(destination_parent)

            move_segments = self._path_segments(move_source_path)
            common_prefix = 0
            for left, right in zip(source_segments, move_segments):
                if left.lower() != right.lower():
                    break
                common_prefix += 1
            if common_prefix >= 2:
                nearby_destination_parents.append(destination_parent)

        if sibling_destination_parents:
            parent_path, best_count, total = self._most_common_path(sibling_destination_parents)
            confidence = 0.92 if best_count == total else 0.78
            return {
                "destination_path": self.normalize_memory_path(f"{parent_path}\\{source_name}"),
                "confidence": confidence,
                "reason": "Matched sibling planning pattern in the same source branch.",
                "rule": "sibling_pattern",
            }

        if repeated_name_destination_parents:
            parent_path, best_count, total = self._most_common_path(repeated_name_destination_parents)
            confidence = 0.74 if best_count == total else 0.64
            return {
                "destination_path": self.normalize_memory_path(f"{parent_path}\\{source_name}"),
                "confidence": confidence,
                "reason": "Matched another planned item with the same source name.",
                "rule": "repeated_name_pattern",
            }

        if nearby_destination_parents:
            parent_path, best_count, total = self._most_common_path(nearby_destination_parents)
            confidence = 0.7 if best_count == total else 0.58
            return {
                "destination_path": self.normalize_memory_path(f"{parent_path}\\{source_name}"),
                "confidence": confidence,
                "reason": "Matched nearby planning patterns within the same source area.",
                "rule": "nearby_branch_pattern",
            }

        return None

    def _append_needs_review_row(self, rows, seen_keys, *, item_name, source_path, reason, action, review_type):
        key = (review_type, self.normalize_memory_path(source_path), reason)
        if key in seen_keys:
            return
        seen_keys.add(key)
        rows.append({
            "item_name": item_name,
            "source_path": self.normalize_memory_path(source_path),
            "reason": reason,
            "action": action,
            "review_type": review_type,
        })

    def _compute_planning_workflow_state(self):
        visible_nodes = self._iter_visible_source_nodes()
        total_items = len(visible_nodes)
        not_planned_rows = []
        suggestion_rows = []
        needs_review_rows = []
        review_seen = set()

        duplicate_projection_paths = {}
        for move in self.planned_moves:
            projection_path = self._canonical_destination_projection_path(self._allocation_projection_path(move))
            if not projection_path:
                continue
            duplicate_projection_paths.setdefault(projection_path, []).append(move)

        proposed_paths = {
            self._canonical_destination_projection_path(folder.DestinationPath)
            for folder in self.proposed_folders
            if self._canonical_destination_projection_path(folder.DestinationPath)
        }

        for node_data in visible_nodes:
            source_name = node_data.get("name", "Unnamed Item")
            source_path = self.normalize_memory_path(self._tree_item_path(node_data))
            relationship = self._evaluate_source_relationship(node_data)
            relationship_mode = relationship.get("mode", "none")

            if relationship_mode == "none":
                not_planned_rows.append({
                    "source_name": source_name,
                    "source_path": source_path,
                    "status": "No direct or inherited mapping yet.",
                })
                suggestion = self._build_suggestion_for_source_node(node_data)
                if suggestion is not None:
                    suggestion_row = {
                        "source_name": source_name,
                        "source_path": source_path,
                        "destination_path": suggestion["destination_path"],
                        "confidence": suggestion["confidence"],
                        "reason": suggestion["reason"],
                        "rule": suggestion["rule"],
                    }
                    suggestion_rows.append(suggestion_row)
                    if suggestion["confidence"] < 0.75:
                        self._append_needs_review_row(
                            needs_review_rows,
                            review_seen,
                            item_name=source_name,
                            source_path=source_path,
                            reason="Low-confidence suggestion needs confirmation.",
                            action=suggestion["destination_path"],
                            review_type="weak_suggestion",
                        )
            elif relationship_mode == "inherited":
                self._append_needs_review_row(
                    needs_review_rows,
                    review_seen,
                    item_name=source_name,
                    source_path=source_path,
                    reason="Inherited mapping from a parent folder should be confirmed.",
                    action=relationship.get("suffix", ""),
                    review_type="inherited_mapping",
                )

        for projection_path, moves in duplicate_projection_paths.items():
            if len(moves) <= 1:
                continue
            for move in moves:
                self._append_needs_review_row(
                    needs_review_rows,
                    review_seen,
                    item_name=move.get("source_name", "Unnamed Item"),
                    source_path=move.get("source_path", ""),
                    reason="Multiple planned moves resolve to the same projected destination path.",
                    action=projection_path,
                    review_type="duplicate_destination_projection",
                )

        for move in self.planned_moves:
            allocation_parent_path = self._canonical_destination_projection_path(self._allocation_parent_path(move))
            if not allocation_parent_path:
                continue
            depends_on_proposed = False
            for proposed_path in proposed_paths:
                if self._paths_equivalent(allocation_parent_path, proposed_path, "destination") or self._path_is_descendant(
                    allocation_parent_path,
                    proposed_path,
                    "destination",
                ):
                    depends_on_proposed = True
                    break
            if depends_on_proposed:
                self._append_needs_review_row(
                    needs_review_rows,
                    review_seen,
                    item_name=move.get("source_name", "Unnamed Item"),
                    source_path=move.get("source_path", ""),
                    reason="Mapped into a proposed destination branch that should be confirmed.",
                    action=move.get("destination_path", ""),
                    review_type="proposed_branch_dependency",
                )

        suggestion_rows.sort(key=lambda row: (-row["confidence"], row["source_path"].lower()))
        not_planned_rows.sort(key=lambda row: row["source_path"].lower())
        needs_review_rows.sort(key=lambda row: (row["review_type"], row["source_path"].lower()))

        self._workflow_not_planned_rows = not_planned_rows
        self._workflow_suggestion_rows = suggestion_rows
        self._workflow_needs_review_rows = needs_review_rows
        return {
            "total_items": total_items,
            "not_planned_count": len(not_planned_rows),
            "needs_review_count": len(needs_review_rows),
            "not_planned_rows": not_planned_rows,
            "suggestion_rows": suggestion_rows,
            "needs_review_rows": needs_review_rows,
        }

    def _set_workflow_table_rows(self, table, rows, columns):
        if table is None:
            return
        table.setRowCount(len(rows))
        for row_index, row_data in enumerate(rows):
            for column_index, column_key in enumerate(columns):
                value = row_data.get(column_key, "")
                item = QTableWidgetItem(value)
                item.setData(Qt.UserRole, dict(row_data))
                table.setItem(row_index, column_index, item)
        table.clearSelection()

    def _apply_planning_workflow_state(self, workflow_state):
        self._set_workflow_table_rows(
            getattr(self, "not_planned_table", None),
            workflow_state["not_planned_rows"],
            ["source_name", "source_path", "status"],
        )
        self._set_workflow_table_rows(
            getattr(self, "suggestions_table", None),
            [
                {
                    **row,
                    "confidence_reason": f"{int(round(row['confidence'] * 100))}% - {row['reason']}",
                }
                for row in workflow_state["suggestion_rows"]
            ],
            ["source_name", "source_path", "destination_path", "confidence_reason"],
        )
        self._set_workflow_table_rows(
            getattr(self, "needs_review_table", None),
            workflow_state["needs_review_rows"],
            ["item_name", "source_path", "reason", "action"],
        )

        if hasattr(self, "not_planned_status"):
            self.not_planned_status.setText(
                "All visible source items are covered by direct or inherited planning."
                if not workflow_state["not_planned_rows"]
                else f"{len(workflow_state['not_planned_rows'])} visible source item(s) still need planning."
            )
        if hasattr(self, "suggestions_status"):
            self.suggestions_status.setText(
                "No deterministic suggestions are available yet."
                if not workflow_state["suggestion_rows"]
                else f"{len(workflow_state['suggestion_rows'])} suggested mapping(s) are ready for review."
            )
        if hasattr(self, "needs_review_status"):
            self.needs_review_status.setText(
                "No review issues detected from the current planning state."
                if not workflow_state["needs_review_rows"]
                else f"{len(workflow_state['needs_review_rows'])} planning item(s) need review."
            )
        self._refresh_tree_visual_states("source")
        self._refresh_tree_visual_states("destination")

    def handle_workflow_source_row_activated(self, item, workflow_name):
        row_data = item.data(Qt.UserRole) or {}
        source_path = row_data.get("source_path", "")
        source_item = self._find_visible_source_item_by_path(source_path)
        if source_item is None:
            QMessageBox.information(
                self,
                "Planning Workflow",
                "That source item is not loaded in the visible source tree yet.",
            )
            return

        self.source_tree_widget.clearSelection()
        self.source_tree_widget.setCurrentItem(source_item)
        source_item.setSelected(True)
        self.source_tree_widget.scrollToItem(source_item)
        self.on_tree_selection_changed("source")
        if hasattr(self, "workspace_tabs"):
            self.workspace_tabs.setCurrentWidget(self.details_box)

    def update_selector_context_labels(self):
        if not hasattr(self, "planning_inputs"):
            return

        source_site = self.planning_inputs.get("Source Site").currentText() if self.planning_inputs.get("Source Site") else ""
        source_library = self.planning_inputs.get("Source Library").currentText() if self.planning_inputs.get("Source Library") else ""
        destination_site = self.planning_inputs.get("Destination Site").currentText() if self.planning_inputs.get("Destination Site") else ""
        destination_library = self.planning_inputs.get("Destination Library").currentText() if self.planning_inputs.get("Destination Library") else ""

        if hasattr(self, "source_context_label"):
            self.source_context_label.setText(
                f"Current source: {source_site or 'Not selected'} -> {source_library or 'Not selected'}"
            )
        if hasattr(self, "dashboard_source_summary"):
            self.dashboard_source_summary.setText(
                f"Source: {(source_site or 'Not set')} / {(source_library or 'Not set')}"
            )
        if hasattr(self, "destination_context_label"):
            self.destination_context_label.setText(
                f"Current destination: {destination_site or 'Not selected'} -> {destination_library or 'Not selected'}"
            )
        if hasattr(self, "dashboard_destination_summary"):
            self.dashboard_destination_summary.setText(
                f"Destination: {(destination_site or 'Not set')} / {(destination_library or 'Not set')}"
            )

    def build_placeholder_page(self, name):
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)

        card = QFrame()
        card.setObjectName("SectionBox")
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(20, 20, 20, 20)

        title = QLabel(name)
        title.setObjectName("CardTitle")

        body = QLabel(f"{name} page shell is not built yet.")
        body.setObjectName("CardBody")

        card_layout.addWidget(title)
        card_layout.addWidget(body)
        card_layout.addStretch()

        layout.addWidget(card)
        return page

    def build_execution_page(self):
        page = QWidget()
        outer = QVBoxLayout(page)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(12)

        card = QFrame()
        card.setObjectName("SectionBox")
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(20, 20, 20, 20)
        card_layout.setSpacing(14)

        title = QLabel("Execution")
        title.setObjectName("CardTitle")

        intro = QLabel(
            "Run a saved transfer manifest on this PC when steps use absolute local paths "
            "(for example C:\\… or UNC \\\\server\\share\\…). Typical SharePoint library paths are listed for review only "
            "until a Microsoft Graph executor is implemented.\n\n"
            "Heavy work runs in the background so the window stays responsive."
        )
        intro.setObjectName("CardBody")
        intro.setWordWrap(True)

        btn_row = QHBoxLayout()
        btn_row.setSpacing(10)
        self.execution_open_manifest_button = QPushButton("Open manifest…")
        self.execution_open_manifest_button.setToolTip("Load a JSON manifest saved from Planned Moves or elsewhere.")
        self.execution_open_manifest_button.clicked.connect(self._on_execution_open_manifest)
        self.execution_save_manifest_button = QPushButton("Save manifest from current plan…")
        self.execution_save_manifest_button.setToolTip("Same as Planned Moves → Simulate run: export current planned moves and proposed folders to JSON.")
        self.execution_save_manifest_button.clicked.connect(self._on_simulate_run_save_manifest)
        btn_row.addWidget(self.execution_open_manifest_button)
        btn_row.addWidget(self.execution_save_manifest_button)
        btn_row.addStretch()

        self.execution_manifest_path_label = QLabel("No manifest loaded.")
        self.execution_manifest_path_label.setObjectName("CardBody")
        self.execution_manifest_path_label.setWordWrap(True)
        self.execution_manifest_path_label.setStyleSheet("color: palette(mid);")

        self.execution_summary_text = QTextEdit()
        self.execution_summary_text.setReadOnly(True)
        self.execution_summary_text.setMinimumHeight(160)
        self.execution_summary_text.setPlaceholderText("Open a manifest to see step counts and eligibility.")

        run_row = QHBoxLayout()
        run_row.setSpacing(10)
        self.execution_dry_run_button = QPushButton("Dry run (log only)")
        self.execution_dry_run_button.setEnabled(False)
        self.execution_dry_run_button.setToolTip("Log what would be copied or created; no files changed.")
        self.execution_dry_run_button.clicked.connect(self._on_execution_dry_run_manifest)
        self.execution_run_button = QPushButton("Run local copies / mkdir…")
        self.execution_run_button.setEnabled(False)
        self.execution_run_button.setToolTip("Execute mkdir and file copies for eligible local paths (confirms before running).")
        self.execution_run_button.clicked.connect(self._on_execution_run_manifest)
        run_row.addWidget(self.execution_dry_run_button)
        run_row.addWidget(self.execution_run_button)
        run_row.addStretch()

        self.execution_status_label = QLabel("")
        self.execution_status_label.setObjectName("CardBody")
        self.execution_status_label.setWordWrap(True)

        card_layout.addWidget(title)
        card_layout.addWidget(intro)
        card_layout.addLayout(btn_row)
        card_layout.addWidget(self.execution_manifest_path_label)
        card_layout.addWidget(self.execution_summary_text, 1)
        card_layout.addLayout(run_row)
        card_layout.addWidget(self.execution_status_label)

        outer.addWidget(card, 1)
        return page

    def build_settings_page(self):
        page = QWidget()
        outer = QVBoxLayout(page)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(12)

        card = QFrame()
        card.setObjectName("SectionBox")
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(20, 20, 20, 20)
        card_layout.setSpacing(14)

        title = QLabel("Microsoft Graph cache")
        title.setObjectName("CardTitle")

        intro = QLabel(
            "Folder listings are cached on disk so trees stay responsive. Incremental sync uses "
            "Microsoft Graph delta to clear only the parts of the cache that changed in SharePoint."
        )
        intro.setObjectName("CardBody")
        intro.setWordWrap(True)

        env_sync_off = os.environ.get("OZLINK_GRAPH_DELTA_SYNC", "").strip().lower() in ("0", "false", "no")
        env_bootstrap_off = os.environ.get("OZLINK_GRAPH_DELTA_DISABLE_BOOTSTRAP", "").strip().lower() in (
            "1",
            "true",
            "yes",
        )

        self._settings_delta_sync_checkbox = QCheckBox(
            "Run background incremental cache sync after a library finishes loading"
        )
        self._settings_delta_sync_checkbox.setChecked(self._graph_settings_delta_sync_enabled())
        self._settings_delta_sync_checkbox.setEnabled(not env_sync_off)
        if env_sync_off:
            self._settings_delta_sync_checkbox.setToolTip(
                "Disabled by OZLINK_GRAPH_DELTA_SYNC environment variable."
            )
        self._settings_delta_sync_checkbox.toggled.connect(self._persist_settings_delta_sync)

        self._settings_delta_bootstrap_checkbox = QCheckBox(
            "Automatically establish delta tracking for each library (recommended; first run can take time on very large libraries)"
        )
        self._settings_delta_bootstrap_checkbox.setChecked(self._graph_settings_delta_bootstrap_allowed())
        self._settings_delta_bootstrap_checkbox.setEnabled(not env_bootstrap_off)
        if env_bootstrap_off:
            self._settings_delta_bootstrap_checkbox.setToolTip(
                "Disabled by OZLINK_GRAPH_DELTA_DISABLE_BOOTSTRAP environment variable."
            )
        self._settings_delta_bootstrap_checkbox.toggled.connect(self._persist_settings_delta_bootstrap)

        hint = QLabel(
            "Administrators can force behaviour with OZLINK_GRAPH_DELTA_SYNC=0 or "
            "OZLINK_GRAPH_DELTA_DISABLE_BOOTSTRAP=1. Optional: OZLINK_GRAPH_DELTA_MAX_PAGES caps delta pages."
        )
        hint.setObjectName("MutedText")
        hint.setWordWrap(True)

        card_layout.addWidget(title)
        card_layout.addWidget(intro)
        card_layout.addWidget(self._settings_delta_sync_checkbox)
        card_layout.addWidget(self._settings_delta_bootstrap_checkbox)
        card_layout.addWidget(hint)
        card_layout.addStretch()

        outer.addWidget(card)

        ui_card = QFrame()
        ui_card.setObjectName("SectionBox")
        ui_card_layout = QVBoxLayout(ui_card)
        ui_card_layout.setContentsMargins(20, 20, 20, 20)
        ui_card_layout.setSpacing(14)

        ui_title = QLabel("Planning workspace — faster source tree (optional)")
        ui_title.setObjectName("CardTitle")

        ui_intro = QLabel(
            "If the source library tree feels slow when scrolling or expanding folders, enable the option "
            "below and restart the app. Mapping and normal planning behave the same. "
            "Source Expand All loads folders in the background (up to 3 at a time). Very large libraries may take a while."
        )
        ui_intro.setObjectName("CardBody")
        ui_intro.setWordWrap(True)

        env_qtv = os.environ.get("OZLINK_SOURCE_QTREEVIEW", "").strip()
        env_qtv_norm = env_qtv.lower()
        env_qtv_forces = env_qtv_norm in ("0", "false", "no", "1", "true", "yes")

        self._settings_source_qtreeview_checkbox = QCheckBox(
            "Use faster source tree (restart required)"
        )
        self._settings_source_qtreeview_checkbox.setChecked(
            QSettings().value("ui/source_use_qtreeview", False, type=bool)
        )
        self._settings_source_qtreeview_checkbox.setEnabled(not env_qtv_forces)
        if env_qtv_forces:
            self._settings_source_qtreeview_checkbox.setToolTip(
                f"This session uses OZLINK_SOURCE_QTREEVIEW={env_qtv!r}. Remove or clear that variable to use the checkbox."
            )
        self._settings_source_qtreeview_checkbox.toggled.connect(self._persist_settings_source_qtreeview)

        ui_hint = QLabel(
            "Off by default. Your choice is saved on this PC. "
            "Administrators can force one launch with OZLINK_SOURCE_QTREEVIEW=1 (on) or =0 (off) when the variable is set."
        )
        ui_hint.setObjectName("MutedText")
        ui_hint.setWordWrap(True)

        ui_card_layout.addWidget(ui_title)
        ui_card_layout.addWidget(ui_intro)
        ui_card_layout.addWidget(self._settings_source_qtreeview_checkbox)
        ui_card_layout.addWidget(ui_hint)
        ui_card_layout.addStretch()

        outer.addWidget(ui_card)
        outer.addStretch()
        return page

    def _persist_settings_source_qtreeview(self, checked: bool):
        QSettings().setValue("ui/source_use_qtreeview", bool(checked))

    def _graph_settings_delta_sync_enabled(self) -> bool:
        env = os.environ.get("OZLINK_GRAPH_DELTA_SYNC", "").strip().lower()
        if env in ("0", "false", "no"):
            return False
        if env in ("1", "true", "yes"):
            return True
        return QSettings().value("graph/delta_sync_enabled", True, type=bool)

    def _graph_settings_delta_bootstrap_allowed(self) -> bool:
        env = os.environ.get("OZLINK_GRAPH_DELTA_DISABLE_BOOTSTRAP", "").strip().lower()
        if env in ("1", "true", "yes"):
            return False
        return QSettings().value("graph/delta_auto_bootstrap", True, type=bool)

    def _persist_settings_delta_sync(self, checked: bool):
        if os.environ.get("OZLINK_GRAPH_DELTA_SYNC", "").strip().lower() in ("0", "false", "no"):
            return
        QSettings().setValue("graph/delta_sync_enabled", bool(checked))

    def _persist_settings_delta_bootstrap(self, checked: bool):
        if os.environ.get("OZLINK_GRAPH_DELTA_DISABLE_BOOTSTRAP", "").strip().lower() in ("1", "true", "yes"):
            return
        QSettings().setValue("graph/delta_auto_bootstrap", bool(checked))

    def build_bottom_status_bar(self):
        self.bottom_bar = QFrame()
        self.bottom_bar.setObjectName("BottomStatusBar")
        self.bottom_bar.setFixedHeight(54)

        layout = QHBoxLayout(self.bottom_bar)
        layout.setContentsMargins(18, 8, 18, 8)
        layout.setSpacing(22)

        self.bottom_tenant = QLabel("Tenant: Not Connected")
        self.bottom_source = QLabel("Source: Not Set")
        self.bottom_destination = QLabel("Destination: Not Set")
        self.bottom_mode = QLabel("Mode: Client")
        self.bottom_version = QLabel("Version: v1.0.0")
        self.bottom_refresh = QLabel("Last Refresh: Not Started")

        for lbl in [
            self.bottom_tenant,
            self.bottom_source,
            self.bottom_destination,
            self.bottom_mode,
            self.bottom_version,
            self.bottom_refresh,
        ]:
            lbl.setObjectName("FooterText")
            layout.addWidget(lbl)

        layout.addStretch()
        self.root_layout.addWidget(self.bottom_bar)

    def clear_dashboard_fields(self):
        self.work_email_input.clear()
        self._set_dashboard_status_message("Ready to open your planning session.", loading=False)

    def _planning_workspace_available(self):
        return bool(self.current_session_context.get("connected"))

    def _show_planning_sign_in_message(self):
        QMessageBox.information(
            self,
            "Sign In Required",
            "Please sign in first to load your SharePoint sites and libraries.",
        )

    def _update_planning_workspace_access(self):
        planning_available = self._planning_workspace_available()

        planning_nav_btn = self.nav_buttons.get("Planning Workspace")
        if planning_nav_btn is not None:
            planning_nav_btn.setEnabled(planning_available)
            if planning_available:
                planning_nav_btn.setToolTip("")
            else:
                planning_nav_btn.setToolTip("Please sign in first to load your SharePoint sites and libraries.")

        if hasattr(self, "dashboard_open_workspace_btn"):
            self.dashboard_open_workspace_btn.setEnabled(planning_available)
        if hasattr(self, "dashboard_connected_continue_btn"):
            self.dashboard_connected_continue_btn.setEnabled(planning_available)

    def switch_page(self, name):
        if name not in self.page_map:
            return

        if name == "Planning Workspace" and not self._planning_workspace_available():
            self.pages.setCurrentWidget(self.page_map["Dashboard"])
            self._set_active_nav_button("Dashboard")
            self.app_subtitle.setText("Dashboard")
            self._set_dashboard_status_message("Please sign in first to load your SharePoint sites and libraries.", loading=False)
            self._show_planning_sign_in_message()
            return

        if not self._is_page_allowed(name):
            allowed_pages = self.nav_allowed_by_role.get(
                self.current_session_context.get("user_role", "user"),
                ["Dashboard", "Planning Workspace"],
            )
            fallback_page = allowed_pages[0] if allowed_pages else "Dashboard"
            name = fallback_page

        if name == "Requests":
            self.refresh_requests_page()

        self.pages.setCurrentWidget(self.page_map[name])
        self._set_active_nav_button(name)
        self.app_subtitle.setText(name)
        if name == "Dashboard" and hasattr(self, "work_email_input") and self.work_email_input is not None:
            self._schedule_safe_timer(0, "dashboard_focus_work_email", self.work_email_input.setFocus)

    def _set_active_nav_button(self, active_name):
        for name, btn in self.nav_buttons.items():
            if not btn.isVisible():
                continue

            if name == active_name:
                btn.setObjectName("NavButtonActive")
            else:
                btn.setObjectName("NavButton")
            btn.style().unpolish(btn)
            btn.style().polish(btn)
            btn.update()

    def _is_page_allowed(self, page_name):
        role = self.current_session_context.get("user_role", "user")
        allowed_pages = self.nav_allowed_by_role.get(role, self.nav_allowed_by_role["user"])
        return page_name in allowed_pages

    def apply_role_visibility(self, role):
        allowed_pages = self.nav_allowed_by_role.get(role, self.nav_allowed_by_role["user"])

        for name, btn in self.nav_buttons.items():
            btn.setVisible(name in allowed_pages)

        self._update_planning_workspace_access()
        if hasattr(self, "test_mode_toggle"):
            is_admin = role == "admin"
            self.test_mode_toggle.setVisible(is_admin)
            if not is_admin:
                self.test_mode_toggle.setChecked(False)
        if hasattr(self, "requests_delete_test_btn"):
            self.requests_delete_test_btn.setVisible(role == "admin")
            self.requests_delete_test_btn.setEnabled(False)

        current_page_name = self.app_subtitle.text()
        if current_page_name not in allowed_pages:
            self.switch_page(allowed_pages[0])

        if role == "admin":
            self.mode_label.setText("Admin planning and operations mode")
        else:
            self.mode_label.setText("Client-facing planning mode")

    def _is_valid_work_email(self, email):
        value = str(email or "").strip()
        if not value:
            return False
        if " " in value:
            return False
        return re.fullmatch(r"[A-Za-z0-9._%+\-']+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", value) is not None

    def _normalize_login_email(self, email):
        return str(email or "").strip().lower()

    def _cancel_pending_sign_in(self, *, message="Microsoft 365 sign-in cancelled. You can try again."):
        self._active_auth_attempt_id += 1
        self._login_in_progress = False
        self._login_error_seen = False
        self._pending_login_email = ""
        self._pending_login_restore_args = None
        self._close_device_flow_prompt_dialog()
        try:
            if isinstance(getattr(self.graph, "device_flow", None), dict):
                self.graph.device_flow["expires_at"] = 0
        except Exception:
            pass
        self.update_session_state(False)
        self._set_dashboard_status_message(message, loading=False)

    def _show_device_flow_prompt_dialog(self, entered_email="", *, ready=False, attempt_id=None):
        dialog = getattr(self, "device_flow_prompt_dialog", None)
        if dialog is None:
            dialog = DeviceFlowPromptDialog(self)
            dialog.finished.connect(self._on_device_flow_prompt_finished)
            dialog.cancel_button.clicked.connect(self._on_device_flow_prompt_cancel_requested)
            self.device_flow_prompt_dialog = dialog

        self._device_flow_prompt_ready = bool(ready)
        dialog.setProperty("attempt_id", attempt_id if attempt_id is not None else self._active_auth_attempt_id)
        dialog.set_prompt_state(entered_email, stage="waiting")
        dialog.show()
        dialog.raise_()
        dialog.activateWindow()

    def _close_device_flow_prompt_dialog(self):
        dialog = getattr(self, "device_flow_prompt_dialog", None)
        if dialog is None:
            return
        try:
            dialog.blockSignals(True)
            if hasattr(dialog, "_wait_timer"):
                dialog._wait_timer.stop()
            dialog.hide()
            dialog.deleteLater()
        finally:
            self.device_flow_prompt_dialog = None
            self._device_flow_prompt_ready = False

    def _launch_device_login_page(self):
        verification_uri = ""
        try:
            if isinstance(getattr(self.graph, "device_flow", None), dict):
                verification_uri = str(self.graph.device_flow.get("verification_uri", "")).strip()
        except Exception:
            verification_uri = ""

        opened = False
        if verification_uri:
            try:
                subprocess.Popen(["cmd", "/c", "start", "", verification_uri], shell=False)
                opened = True
            except Exception:
                opened = False
        if not opened and verification_uri:
            try:
                os.startfile(verification_uri)
                opened = True
            except Exception:
                opened = False
        if not opened and verification_uri:
            try:
                shell_result = ctypes.windll.shell32.ShellExecuteW(None, "open", verification_uri, None, None, 1)
                opened = int(shell_result) > 32
            except Exception:
                opened = False
        if not opened and verification_uri:
            try:
                opened = QDesktopServices.openUrl(QUrl(verification_uri))
            except Exception:
                opened = False
        if not opened:
            try:
                self.graph.open_device_login_page()
                opened = True
            except Exception:
                opened = False
        if not opened and verification_uri:
            QMessageBox.information(
                self,
                "Open Microsoft Sign-In",
                f"We could not automatically open your browser.\n\nPlease open this address manually:\n{verification_uri}",
            )
        return opened

    def _start_login_worker_after_browser_open(self, attempt_id=None):
        self.update_session_state(False, waiting=True)

        self._login_in_progress = True
        self._login_error_seen = False
        self._pending_login_restore_args = None
        self.worker = LoginWorker(self.graph)
        self.worker.success.connect(lambda payload, attempt_id=attempt_id: self._safe_invoke("login_worker.success", self.on_login_success, payload, attempt_id))
        self.worker.error.connect(lambda error, attempt_id=attempt_id: self._safe_invoke("login_worker.error", self.on_login_error, error, attempt_id))
        self.worker.finished.connect(lambda attempt_id=attempt_id: self._safe_invoke("login_worker.finished", self.on_login_worker_finished, attempt_id))
        self.worker.start()

    def _on_device_flow_prompt_finished(self, result):
        if result == int(QDialog.Accepted):
            self._close_device_flow_prompt_dialog()
            return
        dialog = self.sender()
        if dialog is None:
            return
        attempt_id = dialog.property("attempt_id")
        if attempt_id is not None and attempt_id != self._active_auth_attempt_id:
            return
        self._close_device_flow_prompt_dialog()
        self.graph.device_flow = None
        self._pending_login_email = ""
        self.top_connect_btn.setEnabled(True)
        self.dashboard_connect_btn.setEnabled(True)
        self.update_session_state(False)
        self._set_dashboard_status_message("Microsoft 365 sign-in cancelled. Update the email and click Connect again.", loading=False)

    def _on_device_flow_prompt_cancel_requested(self, *_args):
        dialog = getattr(self, "device_flow_prompt_dialog", None)
        if dialog is None:
            return
        dialog.reject()

    def handle_connect(self):
        if self.current_session_context.get("connected"):
            self.handle_sign_out()
            return

        if (
            (self.device_flow_worker and self.device_flow_worker.isRunning())
            or self._login_in_progress
        ):
            self._cancel_pending_sign_in()
            return

        try:
            self._was_maximized_before_login = self.isMaximized()
            print(
                "[window-login] "
                f"before_sign_in maximized={self.isMaximized()} "
                f"state={self._window_state_repr()} "
                f"geometry={self.geometry().getRect()}"
            )

            entered_email = ""
            if hasattr(self, "work_email_input") and self.work_email_input is not None:
                entered_email = self.work_email_input.text().strip()

            if not self._is_valid_work_email(entered_email):
                self._set_dashboard_status_message("Enter a valid work email address before continuing.", loading=False)
                if hasattr(self, "work_email_input") and self.work_email_input is not None:
                    self.work_email_input.setFocus()
                    self.work_email_input.selectAll()
                QMessageBox.information(
                    self,
                    "Work Email Required",
                    "Enter a valid work email address before continuing to Microsoft sign-in.",
                )
                return

            if entered_email:
                self._set_dashboard_status_message(f"Preparing Microsoft 365 sign-in for {entered_email}...", loading=True)
            else:
                self._set_dashboard_status_message("Preparing Microsoft 365 sign-in...", loading=True)
            self._pending_login_email = self._normalize_login_email(entered_email)
            self._auth_attempt_sequence += 1
            attempt_id = self._auth_attempt_sequence
            self._active_auth_attempt_id = attempt_id

            self.top_connect_btn.setText("Cancel Sign-In")
            self.dashboard_connect_btn.setText("Cancel Sign-In")
            self.top_connect_btn.setEnabled(True)
            self.dashboard_connect_btn.setEnabled(True)
            self._show_device_flow_prompt_dialog(entered_email, ready=False, attempt_id=attempt_id)
            QApplication.processEvents()

            self.device_flow_worker = DeviceFlowWorker(self.graph)
            self.device_flow_worker.success.connect(lambda payload, attempt_id=attempt_id: self._safe_invoke("device_flow_worker.success", self.on_device_flow_ready, payload, attempt_id))
            self.device_flow_worker.error.connect(lambda error, attempt_id=attempt_id: self._safe_invoke("device_flow_worker.error", self.on_device_flow_error, error, attempt_id))
            self.device_flow_worker.finished.connect(lambda attempt_id=attempt_id: self._safe_invoke("device_flow_worker.finished", self.on_device_flow_worker_finished, attempt_id))
            self.device_flow_worker.start()

        except Exception as e:
            self._pending_login_email = ""
            self._login_in_progress = False
            self._close_device_flow_prompt_dialog()
            self.device_flow_worker = None
            self.worker = None
            self.update_session_state(False)
            self._set_dashboard_status_message("Could not start Microsoft 365 sign-in.", loading=False)
            QMessageBox.critical(self, "Error", str(e))

    def handle_sign_out(self):
        if (self.worker and self.worker.isRunning()) or (self.device_flow_worker and self.device_flow_worker.isRunning()):
            return

        self._stop_session_keepalive()
        self._login_in_progress = False
        self._login_error_seen = False
        self._pending_login_email = ""
        self._pending_login_restore_args = None
        self._destination_expand_all_after_full_tree = False
        self._log_restore_state_snapshot("restore_state_cleared", reason="sign_out")
        self._save_draft_shell(include_workspace_ui=True)
        self.graph.disconnect()
        self.current_profile = None
        self.discovered_sites = []
        self._clear_runtime_draft_state(refresh_ui=False)
        self._reset_full_count_state()
        self._reset_destination_full_tree_state()
        self._memory_restore_complete = False
        self._suppress_autosave = True
        self.current_session_context = {
            "connected": False,
            "user_role": "user",
            "operator_display_name": "",
            "operator_upn": "",
            "tenant_domain": "",
            "discovered_sites": [],
        }

        self.apply_role_visibility("user")
        self.switch_page("Dashboard")
        self.update_session_state(False)
        self.reset_planning_selectors()
        self.reset_root_panels()
        self.clear_selection_details()
        self.refresh_planned_moves_table()
        self.bottom_source.setText("Source: Not Set")
        self.bottom_destination.setText("Destination: Not Set")
        self.bottom_refresh.setText("Last Refresh: Not Started")
        self._set_dashboard_status_message("Signed out of Microsoft 365.", loading=False)

    def on_login_worker_finished(self, attempt_id=None):
        try:
            if attempt_id is not None and attempt_id != self._active_auth_attempt_id:
                return
            self.worker = None
            if not self.current_session_context.get("connected"):
                self._login_in_progress = False
        except Exception as exc:
            self._log_restore_exception("on_login_worker_finished", exc)

    def on_device_flow_worker_finished(self, attempt_id=None):
        try:
            if attempt_id is not None and attempt_id != self._active_auth_attempt_id:
                return
            self.device_flow_worker = None
        except Exception as exc:
            self._log_restore_exception("on_device_flow_worker_finished", exc)

    def on_device_flow_ready(self, flow_payload, attempt_id=None):
        try:
            if attempt_id is not None and attempt_id != self._active_auth_attempt_id:
                return
            self._launch_device_login_page()
            self._close_device_flow_prompt_dialog()
            self._start_login_worker_after_browser_open(attempt_id=attempt_id)
        except Exception as exc:
            self._pending_login_email = ""
            self._login_in_progress = False
            self._close_device_flow_prompt_dialog()
            self.update_session_state(False)
            self.top_connect_btn.setEnabled(True)
            self.dashboard_connect_btn.setEnabled(True)
            self._log_restore_exception("on_device_flow_ready", exc)
            QMessageBox.critical(self, "Error", str(exc))

    def on_device_flow_error(self, error, attempt_id=None):
        if attempt_id is not None and attempt_id != self._active_auth_attempt_id:
            return
        self._close_device_flow_prompt_dialog()
        self._pending_login_email = ""
        self._login_in_progress = False
        self.update_session_state(False)
        self.top_connect_btn.setEnabled(True)
        self.dashboard_connect_btn.setEnabled(True)
        self._set_dashboard_status_message("Could not start Microsoft 365 sign-in.", loading=False)
        QMessageBox.critical(self, "Error", error)

    def on_discovery_worker_finished(self):
        try:
            self.discovery_worker = None
            self._refresh_planning_loading_banner()
        except Exception as exc:
            self._log_restore_exception("on_discovery_worker_finished", exc)

    def on_cache_refresh_worker_finished(self):
        try:
            self.cache_refresh_worker = None
            if hasattr(self, "source_refresh_cache_button"):
                self.source_refresh_cache_button.setEnabled(True)
                self.source_refresh_cache_button.setText("Refresh Cache")
            if hasattr(self, "destination_refresh_cache_button"):
                self.destination_refresh_cache_button.setEnabled(True)
                self.destination_refresh_cache_button.setText("Refresh Cache")
            if not self._pending_cache_refresh_panels:
                self._cache_refresh_restore_active = False
        except Exception as exc:
            self._log_restore_exception("on_cache_refresh_worker_finished", exc)

    def on_cache_refresh_success(self, payload):
        try:
            source_site = self.planning_inputs.get("Source Site").currentData() if hasattr(self, "planning_inputs") else None
            source_library = self.planning_inputs.get("Source Library").currentData() if hasattr(self, "planning_inputs") else None
            destination_site = self.planning_inputs.get("Destination Site").currentData() if hasattr(self, "planning_inputs") else None
            destination_library = self.planning_inputs.get("Destination Library").currentData() if hasattr(self, "planning_inputs") else None

            if (
                "source" in self._pending_cache_refresh_panels
                and isinstance(source_site, dict)
                and isinstance(source_library, dict)
                and source_library.get("id")
            ):
                self.loaded_root_request_signatures["source"] = None
                self.load_library_root("source", source_site, source_library, force_refresh=False)

            if (
                "destination" in self._pending_cache_refresh_panels
                and isinstance(destination_site, dict)
                and isinstance(destination_library, dict)
                and destination_library.get("id")
            ):
                self.loaded_root_request_signatures["destination"] = None
                self.load_library_root("destination", destination_site, destination_library, force_refresh=False)

            if hasattr(self, "planned_moves_status"):
                label = (
                    "Source and destination cache refreshed. Reloading libraries."
                    if self._pending_cache_refresh_panels == {"source", "destination"}
                    else (
                        "Source cache refreshed. Reloading source library."
                        if "source" in self._pending_cache_refresh_panels
                        else "Destination cache refreshed. Reloading destination library."
                    )
                )
                self.planned_moves_status.setText(label)
        except Exception as exc:
            self._log_restore_exception("on_cache_refresh_success", exc)

    def _finalize_cache_refresh_workspace_restore(self):
        ui_state = self._pending_cache_refresh_ui_state
        refreshed_panels = set(self._pending_cache_refresh_panels or set())
        ui_state_for_restore = dict(ui_state or {})
        for panel_key in refreshed_panels:
            if panel_key in {"source", "destination"} and panel_key in self._cache_refresh_skip_expanded_restore_panels:
                ui_state_for_restore[f"{panel_key}_expanded_paths"] = set()
        self._restore_workspace_tree_state(ui_state_for_restore)
        try:
            if getattr(self, "destination_tree_widget", None) is not None:
                self._materialize_destination_future_model("cache_refresh_restore_complete")
                self._start_destination_restore_materialization()
                self.destination_tree_widget.viewport().update()
        except Exception as exc:
            self._log_restore_exception("cache_refresh_restore.destination_overlay", exc)
        try:
            if getattr(self, "source_tree_widget", None) is not None:
                self._refresh_source_projection("source_projection_cache_refresh_restore")
                self.source_tree_widget.viewport().update()
        except Exception as exc:
            self._log_restore_exception("cache_refresh_restore.source_projection", exc)
        self._cache_refresh_restore_active = False
        self._pending_cache_refresh_ui_state = None
        self._pending_cache_refresh_panels = set()
        self._cache_refresh_skip_expanded_restore_panels = set()
        self._pending_cache_refresh_tree_snapshots = {}
        self._schedule_progress_summary_refresh()

    def on_cache_refresh_error(self, error):
        try:
            self._cache_refresh_restore_active = False
            self._pending_cache_refresh_ui_state = None
            self._pending_cache_refresh_panels = set()
            self._cache_refresh_skip_expanded_restore_panels = set()
            self._pending_cache_refresh_tree_snapshots = {}
            if hasattr(self, "planned_moves_status"):
                self.planned_moves_status.setText("Could not refresh the SharePoint cache.")
            QMessageBox.warning(self, "Refresh Cache", "The SharePoint cache could not be refreshed.")
            self._log_restore_exception("on_cache_refresh_error", Exception(str(error)))
        except Exception as exc:
            self._log_restore_exception("on_cache_refresh_error_handler", exc)

    def _start_discovery_worker(self):
        if self.discovery_worker and self.discovery_worker.isRunning():
            return
        self.discovery_worker = DiscoverSitesWorker(self.graph)
        self.discovery_worker.success.connect(lambda payload: self._safe_invoke("discovery_worker.success", self.on_discovery_success, payload))
        self.discovery_worker.error.connect(lambda error: self._safe_invoke("discovery_worker.error", self.on_discovery_error, error))
        self.discovery_worker.finished.connect(lambda: self._safe_invoke("discovery_worker.finished", self.on_discovery_worker_finished))
        self.discovery_worker.start()

    def _complete_login_restore_after_discovery(self):
        args = self._pending_login_restore_args or {}
        role = args.get("role", self.current_session_context.get("user_role", "user"))
        had_login_error = bool(args.get("had_login_error", False))
        self._safe_invoke(
            "login.restore_workspace",
            self._finish_login_workspace_restore,
            role=role,
            had_login_error=had_login_error,
        )

    def on_discovery_success(self, payload):
        discovered_sites = payload.get("discovered_sites", [])
        self.discovered_sites = discovered_sites
        self.current_session_context["discovered_sites"] = discovered_sites
        pending_restore = bool(self._pending_login_restore_args)
        self.populate_planning_selectors(discovered_sites, auto_load_initial=not pending_restore)
        self._set_dashboard_status_message("Microsoft 365 sign-in completed successfully.", loading=False)
        self._complete_login_restore_after_discovery()

    def on_discovery_error(self, error):
        self.discovered_sites = []
        self.current_session_context["discovered_sites"] = []
        self.populate_planning_selectors([], auto_load_initial=False)
        self._set_dashboard_status_message("Signed in, but SharePoint site discovery could not complete.", loading=False)
        self._complete_login_restore_after_discovery()

    def on_login_success(self, login_result, attempt_id=None):
        try:
            if attempt_id is not None and attempt_id != self._active_auth_attempt_id:
                return
            self._close_device_flow_prompt_dialog()
            print(
                "[window-login] "
                f"on_login_success before_refresh maximized={self.isMaximized()} "
                f"state={self._window_state_repr()} "
                f"geometry={self.geometry().getRect()}"
            )
            profile = login_result.get("profile", {})
            session_context = login_result.get("session_context", {})
            had_login_error = self._login_error_seen
            expected_email = self._normalize_login_email(getattr(self, "_pending_login_email", ""))
            actual_upn = self._normalize_login_email(session_context.get("operator_upn", profile.get("userPrincipalName", "")))
            actual_mail = self._normalize_login_email(profile.get("mail", ""))
            actual_email = actual_upn or actual_mail

            if expected_email and actual_email and expected_email != actual_email:
                self.graph.disconnect()
                self.worker = None
                self._login_in_progress = False
                self._login_error_seen = False
                self._pending_login_email = ""
                self.current_profile = None
                self.discovered_sites = []
                self.current_session_context = {
                    "connected": False,
                    "user_role": "user",
                    "operator_display_name": "",
                    "operator_upn": "",
                    "tenant_domain": "",
                    "discovered_sites": [],
                }
                self.apply_role_visibility("user")
                self.update_session_state(False)
                self._set_dashboard_status_message("The Microsoft account did not match the email entered in the app.", loading=False)
                if hasattr(self, "work_email_input") and self.work_email_input is not None:
                    self.work_email_input.setFocus()
                    self.work_email_input.selectAll()
                log_warn(
                    "Login email mismatch blocked.",
                    expected_email=expected_email,
                    actual_email=actual_email,
                )
                QMessageBox.warning(
                    self,
                    "Email Mismatch",
                    "The Microsoft account used in the browser does not match the email entered in the app.\n\n"
                    f"Entered email: {expected_email}\n"
                    f"Signed-in account: {actual_email}\n\n"
                    "Please try again with the matching Microsoft 365 account.",
                )
                return

            self.current_profile = profile
            self.worker = None
            self._login_in_progress = False
            self._login_error_seen = False
            self._pending_login_email = ""

            session_context["profile"] = profile
            session_context["discovered_sites"] = []
            self.discovered_sites = []
            self.current_session_context = session_context

            display_name = session_context.get("operator_display_name", profile.get("displayName", "Unknown User"))
            upn = session_context.get("operator_upn", profile.get("userPrincipalName", ""))
            domain = session_context.get("tenant_domain", upn.split("@", 1)[1] if "@" in upn else "Connected")
            role = session_context.get("user_role", "user")
            if role not in self.nav_allowed_by_role:
                role = "user"

            self.apply_role_visibility(role)
            self._set_planning_workspace_loading_state("Loading SharePoint...")
            self.switch_page("Planning Workspace")
            self._set_dashboard_status_message("Signed in. Loading SharePoint sites and libraries...", loading=True)
            self.update_session_state(True, display_name, upn, domain, role=role)
            self._start_session_keepalive()
            self._pending_login_restore_args = {"role": role, "had_login_error": had_login_error}
            self._start_discovery_worker()
            print(
                "[window-login] "
                f"on_login_success after_refresh maximized={self.isMaximized()} "
                f"state={self._window_state_repr()} "
                f"geometry={self.geometry().getRect()}"
            )
        except Exception as exc:
            self._log_restore_exception("on_login_success", exc)
            self._set_dashboard_status_message("Microsoft 365 sign-in completed, but draft restore was skipped.", loading=False)
            self.update_session_state(True)

    def on_login_error(self, error, attempt_id=None):
        if attempt_id is not None and attempt_id != self._active_auth_attempt_id:
            return
        self._close_device_flow_prompt_dialog()
        if not self._login_in_progress:
            self.worker = None
            self.update_session_state(False)
            self.top_connect_btn.setEnabled(True)
            self.dashboard_connect_btn.setEnabled(True)
            self._set_dashboard_status_message("Microsoft 365 sign-in cancelled. You can try again.", loading=False)
            return
        had_restored_state = self._has_restored_runtime_state()
        self._login_error_seen = True
        self._log_restore_state_snapshot(
            "login_error_received",
            had_restored_state=had_restored_state,
            login_in_progress=self._login_in_progress,
            clear_allowed=False,
            reason="login_worker_error_received",
            error=str(error),
        )

        clear_allowed = not had_restored_state
        classification_reason = (
            "fatal_clear_empty_state"
            if clear_allowed
            else "transient_preserve_restored_state"
        )
        self._log_restore_state_snapshot(
            "login_error_classified",
            had_restored_state=had_restored_state,
            login_in_progress=self._login_in_progress,
            clear_allowed=clear_allowed,
            reason=classification_reason,
            error=str(error),
        )

        self.worker = None
        self._pending_login_email = ""
        self.current_session_context = {
            "connected": False,
            "user_role": "user",
            "operator_display_name": "",
            "operator_upn": "",
            "tenant_domain": "",
            "discovered_sites": [],
        }
        self._stop_session_keepalive()
        self.apply_role_visibility("user")
        self.update_session_state(False)
        if clear_allowed:
            self._log_restore_state_snapshot("login_error_state_cleared", had_restored_state=had_restored_state, login_in_progress=self._login_in_progress, clear_allowed=clear_allowed, reason=classification_reason)
            self._log_restore_state_snapshot("restore_state_cleared", reason="login_error")
            self._memory_restore_complete = False
            self._suppress_autosave = True
            self._login_in_progress = False
            self.discovered_sites = []
            self.planned_moves = []
            self.proposed_folders = []
            self._reset_full_count_state()
            self._reset_destination_full_tree_state()
            self.reset_planning_selectors()
            self.reset_root_panels()
            self.clear_selection_details()
            self.refresh_planned_moves_table()
            self._set_dashboard_status_message("Microsoft 365 sign-in failed.", loading=False)
        else:
            self._log_restore_state_snapshot("login_error_state_preserved", had_restored_state=had_restored_state, login_in_progress=self._login_in_progress, clear_allowed=clear_allowed, reason=classification_reason)
            self._set_dashboard_status_message("Microsoft 365 sign-in reported an error. Restored planning state was preserved.", loading=False)
            self.refresh_planned_moves_table()
        QMessageBox.critical(self, "Login Failed", error)

    def update_session_state(self, connected, display_name="", upn="", tenant="", waiting=False, role="user"):
        if waiting:
            self.operator_label.setText("Waiting For Sign-In")
            if hasattr(self, "top_tenant_label"):
                self.top_tenant_label.setText("Connecting...")
            self.session_badge.setObjectName("FooterText")
            self.session_badge.setText("Session: Waiting For Sign-In")
            self.bottom_tenant.setText("Tenant: Connecting...")
            self.bottom_mode.setText("Mode: Pending")
            self.top_connect_btn.setText("Cancel Sign-In")
            self.dashboard_connect_btn.setText("Cancel Sign-In")
            if hasattr(self, "dashboard_connected_continue_btn"):
                self.dashboard_connected_continue_btn.setEnabled(False)
            if hasattr(self, "dashboard_connected_switch_btn"):
                self.dashboard_connected_switch_btn.setEnabled(False)
            self._update_planning_workspace_access()
            self.update_dashboard_session_summary(False)
            self.statusBar_update()
            self.top_connect_btn.setEnabled(True)
            self.dashboard_connect_btn.setEnabled(True)
            return

        if connected:
            self._start_session_keepalive()
            operator_text = display_name if display_name else "Connected User"
            self.operator_label.setText(operator_text)
            self.session_badge.setObjectName("StatusGood")
            self.session_badge.setText("■ Session: Connected")
            tenant_text = tenant if tenant else "Connected"
            if hasattr(self, "top_tenant_label"):
                self.top_tenant_label.setText(tenant_text)
            self.bottom_tenant.setText(f"Tenant: {tenant_text}")
            self.bottom_mode.setText(f"Mode: {role.title()}")
            self._set_dashboard_status_message("Planning session ready.", loading=False)
            self.top_connect_btn.setText("Sign Out")
            self.top_connect_btn.setEnabled(True)
            self.dashboard_connect_btn.setText("Connect to Microsoft 365")
            self.dashboard_connect_btn.setEnabled(True)
            if hasattr(self, "dashboard_connected_continue_btn"):
                self.dashboard_connected_continue_btn.setEnabled(True)
            if hasattr(self, "dashboard_connected_switch_btn"):
                self.dashboard_connected_switch_btn.setEnabled(True)
                self.dashboard_connected_switch_btn.setText("Sign Out / Switch Account")
            self._update_planning_workspace_access()
            self.update_dashboard_session_summary(True, display_name=operator_text, tenant=tenant_text, role=role)
        else:
            self._stop_session_keepalive()
            self.operator_label.setText("Not Signed In")
            self.session_badge.setObjectName("StatusBad")
            self.session_badge.setText("■ Session: Not Connected")
            if hasattr(self, "top_tenant_label"):
                self.top_tenant_label.setText("Not Connected")
            self.bottom_tenant.setText("Tenant: Not Connected")
            self.bottom_mode.setText("Mode: Client")
            self.top_connect_btn.setText("Connect to Microsoft 365")
            self.dashboard_connect_btn.setText("Connect to Microsoft 365")
            self.top_connect_btn.setEnabled(True)
            self.dashboard_connect_btn.setEnabled(True)
            if hasattr(self, "dashboard_connected_continue_btn"):
                self.dashboard_connected_continue_btn.setEnabled(False)
            if hasattr(self, "dashboard_connected_switch_btn"):
                self.dashboard_connected_switch_btn.setEnabled(False)
                self.dashboard_connected_switch_btn.setText("Sign In / Switch Account")
            self._update_planning_workspace_access()
            self.update_dashboard_session_summary(False)

        self.statusBar_update()

    def statusBar_update(self):
        self.session_badge.style().unpolish(self.session_badge)
        self.session_badge.style().polish(self.session_badge)
        self.session_badge.update()

    def reset_planning_selectors(self):
        for label_text in [
            "Source Site",
            "Source Library",
            "Destination Site",
            "Destination Library",
        ]:
            selector = self.planning_inputs.get(label_text)
            if selector is None:
                continue

            selector.blockSignals(True)
            selector.clear()
            selector.addItem("Not loaded yet")
            selector.setEnabled(False)
            selector.blockSignals(False)

        self.bottom_source.setText("Source: Not Set")
        self.bottom_destination.setText("Destination: Not Set")
        self.update_selector_context_labels()

    def _set_planning_workspace_loading_state(self, message):
        for label_text in [
            "Source Site",
            "Source Library",
            "Destination Site",
            "Destination Library",
        ]:
            selector = self.planning_inputs.get(label_text)
            if selector is None:
                continue
            selector.blockSignals(True)
            selector.clear()
            selector.addItem(message)
            selector.setEnabled(False)
            selector.blockSignals(False)

        self.set_tree_placeholder("source", "Loading source library...")
        self.set_tree_placeholder("destination", "Loading destination library...")
        self._set_tree_status_message("source", "Loading source structure...", loading=True)
        self._set_tree_status_message("destination", "Loading destination structure...", loading=True)
        self.bottom_source.setText("Source: Loading...")
        self.bottom_destination.setText("Destination: Loading...")
        self.update_selector_context_labels()
        self._refresh_planning_loading_banner()

    def populate_planning_selectors(self, sites, *, auto_load_initial=True):
        self.discovered_sites = sites or []
        site_labels = ["Source Site", "Destination Site"]
        library_labels = ["Source Library", "Destination Library"]

        for label_text in site_labels:
            selector = self.planning_inputs.get(label_text)
            if selector is None:
                continue

            selector.blockSignals(True)
            selector.clear()

            if self.discovered_sites:
                for site in self.discovered_sites:
                    selector.addItem(site.get("name", "Unnamed Site"), site)
                selector.setEnabled(True)
            else:
                selector.addItem("No SharePoint sites found")
                selector.setEnabled(False)

            selector.blockSignals(False)

        for label_text in library_labels:
            selector = self.planning_inputs.get(label_text)
            if selector is None:
                continue

            selector.blockSignals(True)
            selector.clear()
            selector.addItem("Select a site first")
            selector.setEnabled(False)
            selector.blockSignals(False)

        restore_state = self._draft_shell_state if isinstance(self._draft_shell_state, SessionState) else SessionState()
        has_saved_selector_restore = any(
            [
                bool(getattr(restore_state, "SelectedSourceSite", "") or getattr(restore_state, "SelectedSourceSiteKey", "")),
                bool(getattr(restore_state, "SelectedSourceLibrary", "")),
                bool(getattr(restore_state, "SelectedDestinationSite", "") or getattr(restore_state, "SelectedDestinationSiteKey", "")),
                bool(getattr(restore_state, "SelectedDestinationLibrary", "")),
            ]
        )

        if self.discovered_sites and auto_load_initial and not has_saved_selector_restore:
            self.on_site_selector_changed("source")
            self.on_site_selector_changed("destination")
        else:
            if not self.discovered_sites:
                self.reset_root_panels()
        self.update_selector_context_labels()
        self._refresh_planning_loading_banner()

    def _normalize_selector_token(self, value):
        return str(value or "").strip().lower()

    def _find_selector_index(self, selector, expected_values, *, data_keys=()):
        if selector is None:
            return -1

        expected = {self._normalize_selector_token(value) for value in expected_values if str(value or "").strip()}
        if not expected:
            return -1

        for index in range(selector.count()):
            payload = selector.itemData(index)
            if isinstance(payload, dict):
                candidates = {self._normalize_selector_token(payload.get(key, "")) for key in data_keys}
                candidates.add(self._normalize_selector_token(payload.get("name", "")))
            else:
                candidates = set()
            candidates.add(self._normalize_selector_token(selector.itemText(index)))
            if expected.intersection(candidates):
                return index

        return -1

    def _set_selector_index_safely(self, selector, index):
        if selector is None or index < 0:
            return False

        selector.blockSignals(True)
        try:
            selector.setCurrentIndex(index)
            return True
        finally:
            selector.blockSignals(False)

    def _populate_library_selector_for_group(self, selector_group):
        if selector_group == "source":
            site_selector = self.planning_inputs.get("Source Site")
            library_selector = self.planning_inputs.get("Source Library")
        else:
            site_selector = self.planning_inputs.get("Destination Site")
            library_selector = self.planning_inputs.get("Destination Library")

        if site_selector is None or library_selector is None:
            return False

        selected_site = site_selector.currentData()
        libraries = selected_site.get("libraries", []) if isinstance(selected_site, dict) else []

        library_selector.blockSignals(True)
        try:
            library_selector.clear()
            if libraries:
                for library in libraries:
                    library_selector.addItem(library.get("name", "Unnamed Library"), library)
                library_selector.setEnabled(True)
            else:
                library_selector.addItem("No usable libraries found")
                library_selector.setEnabled(False)
        finally:
            library_selector.blockSignals(False)

        return True

    def _restore_selector_matches(self):
        self._log_restore_phase("phase2_restore_selector_matches_start")
        state = self._draft_shell_state if isinstance(self._draft_shell_state, SessionState) else SessionState()
        source_site_selector = self.planning_inputs.get("Source Site")
        destination_site_selector = self.planning_inputs.get("Destination Site")
        source_library_selector = self.planning_inputs.get("Source Library")
        destination_library_selector = self.planning_inputs.get("Destination Library")

        source_site_index = self._find_selector_index(
            source_site_selector,
            [state.SelectedSourceSite, state.SelectedSourceSiteKey],
            data_keys=("id", "site_key", "web_url"),
        )
        destination_site_index = self._find_selector_index(
            destination_site_selector,
            [state.SelectedDestinationSite, state.SelectedDestinationSiteKey],
            data_keys=("id", "site_key", "web_url"),
        )

        source_site_matched = self._set_selector_index_safely(source_site_selector, source_site_index)
        destination_site_matched = self._set_selector_index_safely(destination_site_selector, destination_site_index)
        self._log_restore_phase(
            "phase2_selector_match",
            source_site_matched=source_site_matched,
            source_site_index=source_site_index,
            destination_site_matched=destination_site_matched,
            destination_site_index=destination_site_index,
        )

        if source_site_matched:
            self._populate_library_selector_for_group("source")
        if destination_site_matched:
            self._populate_library_selector_for_group("destination")

        source_library_index = self._find_selector_index(
            source_library_selector,
            [state.SelectedSourceLibrary],
            data_keys=("id", "name"),
        )
        destination_library_index = self._find_selector_index(
            destination_library_selector,
            [state.SelectedDestinationLibrary],
            data_keys=("id", "name"),
        )
        source_library_matched = self._set_selector_index_safely(source_library_selector, source_library_index)
        destination_library_matched = self._set_selector_index_safely(destination_library_selector, destination_library_index)
        self._log_restore_phase(
            "phase2_library_match",
            source_library_matched=source_library_matched,
            source_library_index=source_library_index,
            destination_library_matched=destination_library_matched,
            destination_library_index=destination_library_index,
            selector_signals_suppressed=self._suppress_selector_change_handlers,
        )

        if source_library_matched:
            self._log_restore_phase("phase2_restore_source_library_start")
            self.on_library_selector_changed("source", force=True)
            self._log_restore_phase("phase2_restore_source_library_end")
        if destination_library_matched:
            if getattr(self, "_sharepoint_lazy_mode", False):
                self._schedule_safe_timer(
                    900,
                    "phase2_restore_destination_library_delayed",
                    self._trigger_delayed_destination_library_restore,
                )
            else:
                self._log_restore_phase("phase2_restore_destination_library_start")
                self.on_library_selector_changed("destination", force=True)
                self._log_restore_phase("phase2_restore_destination_library_end")
        self._log_restore_phase("phase2_restore_selector_matches_end")

    def _trigger_delayed_destination_library_restore(self):
        self.on_library_selector_changed("destination", force=True)

    def _post_login_restore_phase2(self):
        if not self.current_session_context.get("connected"):
            self._log_restore_phase("phase2_restore_selectors skipped", reason="session not connected")
            return

        self._log_restore_phase("phase2_post_login_restore_enter")
        self._memory_ui_rebind_in_progress = True
        self._memory_restore_in_progress = True
        self._memory_restore_background_trees = False
        self._suppress_selector_change_handlers = True
        self._restore_finalization_deferred_active = False
        self._restore_finalization_deferred_reason = ""
        try:
            self._run_restore_phase("phase2_restore_selectors", self._restore_selector_matches)
            self._log_restore_phase("phase2_post_login_restore_after_selectors")
            self._run_restore_phase(
                "phase3_refresh_planned_moves",
                lambda: self.refresh_planned_moves_table(),
            )
            self._log_restore_phase("phase2_post_login_restore_after_planned_moves")
            self._log_restore_state_snapshot("restore_ui_bound", destination_replay_invoked=False)
        finally:
            self._suppress_selector_change_handlers = False
            self._memory_ui_rebind_in_progress = False
            # Async tree materialization continues while this flag is True; logs use
            # restore_in_progress=False so operators are not misled during long expand/projection.
            self._memory_restore_background_trees = True
            self._log_restore_state_snapshot(
                "restore_phase2_complete_waiting_for_destination_queue",
                destination_replay_invoked=False,
                draft_id=self.active_draft_session_id,
                autosave_suppressed=self._suppress_autosave,
            )
            # Phase 4 must not use QTimer.singleShot(0): long synchronous post-restore work
            # (expanded-path replay, allocation projection Graph calls on the UI thread) can
            # starve the timer for minutes, leaving restore_in_progress stuck and the app frozen.
            self._safe_invoke("phase4_destination_overlay_immediate", self._post_login_restore_phase4)
            self._log_restore_phase("phase2_post_login_restore_exit")

    def _post_login_restore_phase4(self):
        if not self._restore_destination_overlay_pending:
            self._log_restore_phase("phase4_destination_overlay skipped", reason="no proposed folders pending")
            self._finalize_memory_restore_if_ready("phase4_no_overlay_pending")
            return

        try:
            destination_ready = bool(
                hasattr(self, "destination_tree_widget")
                and self.destination_tree_widget.topLevelItemCount() > 0
            )
            if not destination_ready:
                self._log_restore_phase(
                    "phase4_destination_overlay skipped",
                    reason="destination tree not ready",
                    top_level_count=getattr(self.destination_tree_widget, "topLevelItemCount", lambda: 0)(),
                )
                return
            if not self._destination_root_bind_is_authoritative():
                self._restore_destination_overlay_pending = True
                self._log_restore_phase(
                    "destination_replay_deferred_until_final_root_bind",
                    reason="destination_root_bind_not_authoritative_yet",
                    planned_moves_count=len(self.planned_moves),
                    proposed_folders_count=len(self.proposed_folders),
                    active_request_signature=self.active_root_request_signatures.get("destination"),
                    loaded_request_signature=self.loaded_root_request_signatures.get("destination"),
                )
                return

            applied_count = 0
            if getattr(self, "_sharepoint_lazy_mode", False):
                self._restore_destination_overlay_pending = bool(self.proposed_folders or self.planned_moves)
                self._log_restore_phase(
                    "phase4_destination_overlay skipped",
                    reason="lazy_mode_uses_restore_queue",
                    top_level_count=self.destination_tree_widget.topLevelItemCount(),
                    queue_size=self._unresolved_proposed_queue_size(),
                    allocation_queue_size=self._unresolved_allocation_queue_size(),
                )
                self._finalize_memory_restore_if_ready("phase4_lazy_mode_queue")
                return
            for index in range(self.destination_tree_widget.topLevelItemCount()):
                applied_count += self._apply_proposed_children_to_item(self.destination_tree_widget.topLevelItem(index))
            applied_count += self._replay_unresolved_proposed_overlay("phase4_destination_overlay")
            applied_count += self._replay_unresolved_allocation_overlay("phase4_destination_allocation_overlay")
            applied_count += self._reconcile_destination_semantic_duplicates("phase4_destination_overlay")
            if not getattr(self, "_sharepoint_lazy_mode", False):
                applied_count += self._materialize_destination_future_model("phase4_destination_overlay")
            self._restore_destination_overlay_pending = bool(
                self._unresolved_proposed_queue_size() > 0 or self._unresolved_allocation_queue_size() > 0
            )
            self._log_restore_phase(
                "phase4_destination_overlay applied",
                proposed_folders=len(self.proposed_folders),
                applied_count=applied_count,
                visible_proposed_count=self._count_visible_destination_proposed_nodes(),
            )
            self._log_restore_state_snapshot(
                "restore_post_bind_counts",
                destination_replay_invoked=True,
                applied_count=applied_count,
                visible_proposed_count=self._count_visible_destination_proposed_nodes(),
            )
        except Exception as exc:
            self._log_restore_exception("phase4_destination_overlay", exc)
        finally:
            self._finalize_memory_restore_if_ready("phase4_complete")

    def _finalize_memory_restore_if_ready(self, reason=""):
        unresolved_count = self._unresolved_proposed_queue_size() + self._unresolved_allocation_queue_size()
        destination_queue_size = len(getattr(self, "_destination_restore_materialization_queue", []) or [])
        pending_destination_root_load = 1 if self.root_load_workers.get("destination") else 0
        destination_busy = bool(
            unresolved_count
            or destination_queue_size
            or getattr(self, "_restore_destination_overlay_pending", False)
            or getattr(self, "_destination_root_prime_pending", False)
            or bool(self.pending_folder_loads.get("destination"))
            or pending_destination_root_load
        )
        if destination_busy:
            if not getattr(self, "_restore_finalization_deferred_active", False):
                self._restore_finalization_deferred_active = True
                self._restore_finalization_deferred_reason = str(reason or "")
                self._log_restore_phase(
                    "restore_finalization_deferred_start",
                    reason=reason,
                    unresolved_count=unresolved_count,
                    destination_queue_size=destination_queue_size,
                    overlay_pending=bool(getattr(self, "_restore_destination_overlay_pending", False)),
                    root_prime_pending=bool(getattr(self, "_destination_root_prime_pending", False)),
                    pending_destination_folder_loads=len(self.pending_folder_loads.get("destination", set())),
                    pending_destination_root_loads=pending_destination_root_load,
                )
            return False
        if getattr(self, "_restore_finalization_deferred_active", False):
            self._log_restore_phase(
                "restore_finalization_deferred_resolved",
                start_reason=self._restore_finalization_deferred_reason,
                resolve_reason=reason,
                unresolved_count=unresolved_count,
                destination_queue_size=destination_queue_size,
                pending_destination_folder_loads=len(self.pending_folder_loads.get("destination", set())),
                pending_destination_root_loads=pending_destination_root_load,
            )
            self._restore_finalization_deferred_active = False
            self._restore_finalization_deferred_reason = ""
        if not self._memory_restore_in_progress and self._memory_restore_complete and not self._suppress_autosave:
            return True
        self._memory_restore_in_progress = False
        self._memory_restore_background_trees = False
        self._memory_restore_complete = True
        self._suppress_autosave = False
        # Stop restore-driven source expansion work once restore is finalized.
        # This prevents long-lived background source queue churn from competing
        # with user-triggered expand-all actions.
        self._source_restore_materialization_queue = []
        self._source_restore_materialization_seen = set()
        self._pending_source_navigation = None
        self._pending_destination_navigation = None
        self._prime_source_root_children_after_snapshot()
        self._prime_destination_root_children_after_snapshot()
        self._try_flush_destination_future_model_after_source_restore("memory_restore_finalize")
        self._log_restore_state_snapshot(
            "restore_final_ready",
            destination_replay_invoked=False,
            draft_id=self.active_draft_session_id,
            autosave_suppressed=self._suppress_autosave,
            finalization_reason=reason,
        )
        # After restoring from local memory/cache snapshot, do not force-clear tree cache.
        # This keeps startup fast and avoids immediately replacing snapshot content with live root fetches.
        self._schedule_live_root_refresh("source", delay_ms=900, force_refresh=False)
        self._schedule_live_root_refresh("destination", delay_ms=1200, force_refresh=False)
        return True

    def _schedule_live_root_refresh(self, panel_key, delay_ms=1200, site=None, library=None, force_refresh=True):
        if panel_key not in {"source", "destination"}:
            return
        if self._live_root_refresh_scheduled.get(panel_key):
            return
        if getattr(self, "_memory_restore_in_progress", False):
            return
        if panel_key == "source":
            site_selector = self.planning_inputs.get("Source Site")
            library_selector = self.planning_inputs.get("Source Library")
        else:
            site_selector = self.planning_inputs.get("Destination Site")
            library_selector = self.planning_inputs.get("Destination Library")
        selected_site = site if isinstance(site, dict) else (site_selector.currentData() if site_selector is not None else None)
        selected_library = library if isinstance(library, dict) else (library_selector.currentData() if library_selector is not None else None)
        if not isinstance(selected_site, dict) or not isinstance(selected_library, dict):
            return
        drive_id = str(selected_library.get("id", "")).strip()
        if not drive_id:
            return
        self._live_root_refresh_scheduled[panel_key] = True
        self._live_root_refresh_request_signature[panel_key] = self._build_root_request_signature(panel_key, selected_site, selected_library)
        self._live_root_refresh_ui_state[panel_key] = self._capture_workspace_tree_state()

        def _run():
            self._live_root_refresh_scheduled[panel_key] = False
            if getattr(self, "_memory_restore_in_progress", False):
                self._schedule_live_root_refresh(panel_key, delay_ms=delay_ms, site=selected_site, library=selected_library)
                return
            if panel_key == "destination" and self._destination_live_refresh_still_blocked():
                self._schedule_live_root_refresh(panel_key, delay_ms=max(500, int(delay_ms)), site=selected_site, library=selected_library)
                return
            current_site = site_selector.currentData() if site_selector is not None else None
            current_library = library_selector.currentData() if library_selector is not None else None
            current_signature = self._build_root_request_signature(panel_key, current_site or {}, current_library or {})
            if current_signature != self._live_root_refresh_request_signature.get(panel_key, ""):
                self._live_root_refresh_request_signature[panel_key] = ""
                self._live_root_refresh_ui_state[panel_key] = None
                return
            if (
                not bool(force_refresh)
                and current_signature == self.loaded_root_request_signatures.get(panel_key)
                and self._tree_has_bound_root_content(panel_key)
            ):
                self._live_root_refresh_request_signature[panel_key] = ""
                self._live_root_refresh_ui_state[panel_key] = None
                return
            self.load_library_root(panel_key, current_site, current_library, force_refresh=bool(force_refresh))

        QTimer.singleShot(max(0, int(delay_ms)), _run)

    def _run_drive_delta_sync_best_effort(self, drive_id: str):
        drive_id = str(drive_id or "").strip()
        if not drive_id or not getattr(self, "graph", None):
            return
        if not self._graph_settings_delta_sync_enabled():
            log_trace(
                "drive_delta_sync",
                "skipped_settings_disabled",
                drive_id_suffix=drive_id[-16:] if len(drive_id) > 16 else drive_id,
            )
            return
        workers = self._drive_delta_workers
        existing = workers.get(drive_id)
        if existing is not None and existing.isRunning():
            return
        allow_bootstrap = self._graph_settings_delta_bootstrap_allowed()
        worker = DriveDeltaSyncWorker(
            self.graph,
            drive_id,
            allow_initial_bootstrap=allow_bootstrap,
        )
        workers[drive_id] = worker

        def _cleanup():
            if self._drive_delta_workers.get(drive_id) is worker:
                self._drive_delta_workers.pop(drive_id, None)

        worker.finished.connect(_cleanup)
        worker.success.connect(
            lambda payload, d=drive_id: self._safe_invoke(
                "drive_delta.success",
                self._on_drive_delta_sync_success,
                payload,
                d,
            )
        )
        worker.error.connect(
            lambda msg, d=drive_id: self._safe_invoke(
                "drive_delta.error",
                self._on_drive_delta_sync_error,
                msg,
                d,
            )
        )
        worker.start()

    def _on_drive_delta_sync_success(self, payload: dict, drive_id: str):
        if payload.get("skipped"):
            log_trace(
                "drive_delta_sync",
                "skipped",
                drive_id_suffix=drive_id[-16:] if len(drive_id) > 16 else drive_id,
                reason=payload.get("reason", ""),
            )
            return
        log_info(
            "drive_delta_sync_completed",
            drive_id_suffix=drive_id[-16:] if len(drive_id) > 16 else drive_id,
            pages=int(payload.get("pages") or 0),
            invalidated_folders=int(payload.get("invalidated_folders") or 0),
            initial_token_run=bool(payload.get("initial_token_run")),
        )
        inv = int(payload.get("invalidated_folders") or 0)
        initial = bool(payload.get("initial_token_run"))
        entries = payload.get("invalidated_entries") or []
        if isinstance(entries, list) and inv > 0:
            self._apply_graph_delta_to_visible_trees(drive_id, entries)
        status = getattr(self, "planned_moves_status", None)
        if status is not None:
            if initial:
                status.setText(
                    "Incremental SharePoint sync is ready. Later updates only refresh folders that changed remotely."
                )
            elif inv > 0:
                status.setText(
                    f"SharePoint cache updated ({inv} folder branch(es)). Expanded folders reload when needed."
                )

    def _on_drive_delta_sync_error(self, message: str, drive_id: str):
        log_warn(
            "drive_delta_sync_failed",
            drive_id_suffix=drive_id[-16:] if len(drive_id) > 16 else drive_id,
            error=str(message)[:500],
        )
        status = getattr(self, "planned_moves_status", None)
        if status is not None:
            status.setText(
                "Incremental SharePoint sync failed (cache still works; use Refresh Cache if needed)."
            )

    def _apply_graph_delta_to_visible_trees(self, drive_id: str, entries: list):
        """Mark affected folders stale and reload expanded Graph-backed branches so the UI matches cache invalidation."""
        ids = set()
        for row in entries:
            if not isinstance(row, dict):
                continue
            if str(row.get("drive_id") or "").strip() != drive_id:
                continue
            iid = str(row.get("item_id") or "").strip()
            if iid:
                ids.add(iid)
        if not ids:
            return
        for panel_key in ("source", "destination"):
            tree = self.source_tree_widget if panel_key == "source" else self.destination_tree_widget
            if tree is None:
                continue
            for ti in range(tree.topLevelItemCount()):
                top = tree.topLevelItem(ti)
                for item in self._iter_tree_items(top):
                    node_data = item.data(0, Qt.UserRole) or {}
                    if node_data.get("placeholder"):
                        continue
                    item_id = str(node_data.get("id") or "").strip()
                    if item_id not in ids:
                        continue
                    resolved_drive = self._resolve_tree_item_drive_id(panel_key, node_data)
                    if resolved_drive != drive_id:
                        continue
                    if panel_key == "destination" and self.node_is_planned_allocation(node_data):
                        node_data["children_loaded"] = False
                        item.setData(0, Qt.UserRole, node_data)
                        continue
                    node_data["children_loaded"] = False
                    node_data["load_failed"] = False
                    item.setData(0, Qt.UserRole, node_data)
                    if item.isExpanded():
                        self.on_tree_item_expanded(panel_key, item)

    def _destination_live_refresh_still_blocked(self):
        if getattr(self, "_memory_restore_in_progress", False):
            return True
        if getattr(self, "_restore_destination_overlay_pending", False):
            return True
        if self._unresolved_proposed_queue_size() > 0 or self._unresolved_allocation_queue_size() > 0:
            return True
        if bool(getattr(self, "_destination_restore_materialization_queue", []) or []):
            return True
        if bool(getattr(self, "_destination_idle_materialize_pending_reason", "")):
            return True
        timer = getattr(self, "_destination_idle_materialize_timer", None)
        if timer is not None and timer.isActive():
            return True
        return False

    def _schedule_post_login_restore(self):
        self._log_restore_phase(
            "schedule_post_login_restore",
            connected=self.current_session_context.get("connected", False),
            planned_moves=len(self.planned_moves),
            proposed_folders=len(self.proposed_folders),
        )
        # Run phase-2 restore immediately to avoid occasional QTimer starvation
        # windows that make startup appear hung.
        self._safe_invoke("phase2_post_login_restore_immediate", self._post_login_restore_phase2)

    def _apply_restored_selector_state(self):
        self._begin_session_workspace_ui_restore()
        self._schedule_post_login_restore()

    def _session_workspace_ui_state(self):
        state = self._draft_shell_state if isinstance(self._draft_shell_state, SessionState) else SessionState()
        return {
            "source_expanded_paths": set(getattr(state, "SourceExpandedPaths", []) or []),
            "destination_expanded_paths": set(getattr(state, "DestinationExpandedPaths", []) or []),
            "source_selected_path": str(getattr(state, "SourceSelectedPath", "") or ""),
            "destination_selected_path": str(getattr(state, "DestinationSelectedPath", "") or ""),
            "source_expanded_all": bool(getattr(state, "SourceExpandedAll", False)),
            "destination_expanded_all": bool(getattr(state, "DestinationExpandedAll", False)),
            "planning_header_collapsed": bool(getattr(state, "PlanningHeaderCollapsed", False)),
            "workspace_panel_collapsed": bool(getattr(state, "WorkspacePanelCollapsed", False)),
        }

    def _session_workspace_tree_snapshots(self):
        state = self._draft_shell_state if isinstance(self._draft_shell_state, SessionState) else SessionState()
        return {
            "source": list(getattr(state, "SourceTreeSnapshot", []) or []),
            "destination": list(getattr(state, "DestinationTreeSnapshot", []) or []),
        }

    def _runtime_tree_snapshot_for_panel(self, panel_key):
        snapshots = getattr(self, "_runtime_session_tree_snapshots", {}) or {}
        return list(snapshots.get(panel_key, []) or [])

    def _count_tree_snapshot_nodes(self, snapshots):
        count = 0

        def _walk(snapshot_node):
            nonlocal count
            if not isinstance(snapshot_node, dict):
                return
            count += 1
            for child_snapshot in list(snapshot_node.get("children", []) or []):
                _walk(child_snapshot)

        for snapshot in list(snapshots or []):
            _walk(snapshot)
        return count

    def _tree_snapshot_node_count_gt(self, snapshots, threshold: int) -> bool:
        """True if total snapshot nodes strictly exceed threshold. Stops after threshold+1 nodes."""
        if threshold < 0:
            threshold = 0
        count = 0

        def _walk(snapshot_node) -> None:
            nonlocal count
            if count > threshold:
                return
            if not isinstance(snapshot_node, dict):
                return
            count += 1
            if count > threshold:
                return
            for child_snapshot in list(snapshot_node.get("children", []) or []):
                _walk(child_snapshot)
                if count > threshold:
                    return

        for snapshot in list(snapshots or []):
            _walk(snapshot)
            if count > threshold:
                return True
        return False

    def _count_root_payload_nodes(self, items):
        count = 0

        def _walk(payload_node):
            nonlocal count
            if not isinstance(payload_node, dict):
                return
            count += 1
            for child_node in list(payload_node.get("children", []) or []):
                _walk(child_node)

        for item in list(items or []):
            _walk(item)
        return count

    def _snapshot_refresh_targets_for_panel(self, panel_key, ui_state):
        raw_paths = set(ui_state.get(f"{panel_key}_expanded_paths", set()) or set())
        if panel_key == "destination":
            return {self._canonical_destination_projection_path(path) for path in raw_paths if path}
        return {self._canonical_source_projection_path(path) for path in raw_paths if path}

    def _snapshot_refresh_targets_from_snapshot(self, panel_key, snapshots):
        targets = set()
        source_depth_limit = 3

        def _walk(snapshot_node):
            if not isinstance(snapshot_node, dict):
                return
            data = dict(snapshot_node.get("data", {}) or {})
            item_path = str(
                data.get("item_path")
                or data.get("display_path")
                or data.get("semantic_path")
                or ""
            ).strip()
            is_folder = bool(data.get("is_folder"))
            is_expanded = bool(snapshot_node.get("expanded", False))
            if item_path and is_folder and is_expanded:
                if panel_key == "destination":
                    targets.add(self._canonical_destination_projection_path(item_path))
                else:
                    canonical_path = self._canonical_source_projection_path(item_path)
                    if self._source_branch_depth(canonical_path) <= source_depth_limit:
                        targets.add(canonical_path)
            for child_snapshot in list(snapshot_node.get("children", []) or []):
                _walk(child_snapshot)

        for snapshot in list(snapshots or []):
            _walk(snapshot)
        return {path for path in targets if path}

    def _schedule_snapshot_branch_refresh(self, panel_key, delay_ms=0):
        pending = self._pending_snapshot_branch_refresh.get(panel_key, set())
        if not pending or self._snapshot_branch_refresh_scheduled.get(panel_key):
            return
        self._snapshot_branch_refresh_scheduled[panel_key] = True

        def _run():
            self._snapshot_branch_refresh_scheduled[panel_key] = False
            self._process_snapshot_branch_refresh(panel_key)

        QTimer.singleShot(max(0, int(delay_ms)), _run)

    def _process_snapshot_branch_refresh(self, panel_key):
        if panel_key == "source" and self._expand_all_pending.get("source"):
            # Avoid competing branch refresh traversal during source expand-all.
            self._schedule_snapshot_branch_refresh(panel_key, delay_ms=220)
            return
        pending_paths = set(self._pending_snapshot_branch_refresh.get(panel_key, set()) or set())
        if not pending_paths:
            self._set_tree_status_message(panel_key, "Saved branches are up to date.", loading=False)
            return

        tree = getattr(self, f"{panel_key}_tree_widget", None)
        if tree is None:
            return

        find_item = (
            self._find_visible_destination_item_by_path
            if panel_key == "destination"
            else self._find_visible_source_item_by_path
        )
        remaining_paths = set()
        started_load = False
        started_count = 0
        max_starts_per_tick = 1 if panel_key == "destination" else 2

        for target_path in sorted(pending_paths, key=lambda path: len(self._path_segments(path))):
            item = find_item(target_path)
            if item is None:
                remaining_paths.add(target_path)
                continue

            node_data = item.data(0, Qt.UserRole) or {}
            if node_data.get("placeholder") or not node_data.get("is_folder"):
                continue

            tree.expandItem(item)
            if bool(node_data.get("load_failed")):
                continue
            if not bool(node_data.get("children_loaded")):
                started_load = self._ensure_tree_item_load_started(panel_key, item) or started_load
                remaining_paths.add(target_path)
                if started_load:
                    started_count += 1
                    if started_count >= max_starts_per_tick:
                        break

        self._pending_snapshot_branch_refresh[panel_key] = remaining_paths
        if remaining_paths and (
            started_load
            or bool(self.pending_folder_loads.get(panel_key))
            or bool(self.root_load_workers.get(panel_key))
        ):
            completed_count = max(0, len(pending_paths) - len(remaining_paths))
            total_count = max(len(pending_paths), completed_count)
            self._set_tree_status_message(
                panel_key,
                f"Refreshing saved branches... ({completed_count}/{total_count})",
                loading=True,
            )
            self._schedule_snapshot_branch_refresh(panel_key, delay_ms=150)
        else:
            self._set_tree_status_message(panel_key, "Saved branches are up to date.", loading=False)

    def _capture_child_path_set(self, item):
        child_paths = set()
        if item is None:
            return child_paths
        for index in range(item.childCount()):
            child = item.child(index)
            child_data = child.data(0, Qt.UserRole) or {}
            if child_data.get("placeholder"):
                continue
            child_path = self._tree_item_path(child_data)
            if child_path:
                child_paths.add(str(child_path))
        return child_paths

    def _begin_session_workspace_ui_restore(self):
        ui_state = self._session_workspace_ui_state()
        tree_snapshots = self._session_workspace_tree_snapshots()
        self._runtime_session_tree_snapshots = {
            "source": list(tree_snapshots.get("source", []) or []),
            "destination": list(tree_snapshots.get("destination", []) or []),
        }
        self._apply_planning_header_collapsed_state(bool(ui_state.get("planning_header_collapsed", False)))
        self._apply_workspace_tabs_collapsed_state(bool(ui_state.get("workspace_panel_collapsed", False)))
        has_state = any([
            ui_state["source_expanded_paths"],
            ui_state["destination_expanded_paths"],
            ui_state["source_selected_path"],
            ui_state["destination_selected_path"],
            ui_state["source_expanded_all"],
            ui_state["destination_expanded_all"],
        ])
        has_tree_snapshots = any([
            tree_snapshots.get("source"),
            tree_snapshots.get("destination"),
        ])
        if not has_state and not has_tree_snapshots:
            self._pending_session_workspace_ui_state = None
            self._pending_session_tree_snapshots = {}
            self._runtime_session_tree_snapshots = {"source": [], "destination": []}
            self._pending_session_workspace_restore_panels = set()
            self._pending_workspace_post_expand_selection = {"source": "", "destination": ""}
            self._workspace_restore_expanded_all_intent = {"source": False, "destination": False}
            return

        # Only treat *explicit* expand-all as eager projection intent. Using a path-count
        # threshold falsely flagged normal sessions (several folders expanded while browsing) as
        # expand-all, which forced every [Allocated] branch to materialize thousands of projected
        # rows during destination bind and froze the UI for minutes.
        self._workspace_restore_expanded_all_intent = {
            "source": bool(ui_state.get("source_expanded_all", False)),
            "destination": bool(ui_state.get("destination_expanded_all", False)),
        }

        pending_panels = set()
        state = self._draft_shell_state if isinstance(self._draft_shell_state, SessionState) else SessionState()
        if state.SelectedSourceLibrary:
            pending_panels.add("source")
        if state.SelectedDestinationLibrary:
            pending_panels.add("destination")

        self._pending_session_workspace_ui_state = ui_state
        self._pending_session_tree_snapshots = {
            panel_key: list(tree_snapshots.get(panel_key, []) or [])
            for panel_key in ("source", "destination")
            if tree_snapshots.get(panel_key)
        }
        source_snapshot_targets = self._snapshot_refresh_targets_from_snapshot("source", self._pending_session_tree_snapshots.get("source", []))
        destination_snapshot_targets = self._snapshot_refresh_targets_from_snapshot("destination", self._pending_session_tree_snapshots.get("destination", []))
        self._pending_snapshot_branch_refresh = {
            "source": (
                source_snapshot_targets
                if ui_state.get("source_expanded_all", False)
                else self._snapshot_refresh_targets_for_panel("source", ui_state)
            ) if "source" in pending_panels else set(),
            "destination": (
                destination_snapshot_targets
                if ui_state.get("destination_expanded_all", False)
                else self._snapshot_refresh_targets_for_panel("destination", ui_state)
            ) if "destination" in pending_panels else set(),
        }
        self._pending_session_workspace_restore_panels = pending_panels
        self._pending_workspace_post_expand_selection = {"source": "", "destination": ""}

    def _restore_workspace_tree_panel_state(self, panel_key, ui_state):
        if not ui_state:
            return
        expanded_paths = ui_state.get(f"{panel_key}_expanded_paths", set()) or set()
        selected_path = str(ui_state.get(f"{panel_key}_selected_path", "") or "")
        expanded_all = bool(ui_state.get(f"{panel_key}_expanded_all", False))
        self._restore_expanded_tree_paths(panel_key, expanded_paths)
        self._restore_selected_tree_path(panel_key, selected_path)
        if expanded_all:
            self._restore_panel_expanded_all_state(panel_key)
        self._refresh_expand_all_button_for_panel(panel_key)

    def _restore_panel_expanded_all_state(self, panel_key):
        self._destination_restore_materialization_user_paused = False
        if self._panel_is_expanded_all(panel_key):
            self._refresh_expand_all_button_for_panel(panel_key)
            return
        cached_snapshots = (
            self._pending_session_tree_snapshots.get(panel_key)
            or self._runtime_tree_snapshot_for_panel(panel_key)
        )
        if cached_snapshots:
            visible_node_count = self._count_expandable_tree_nodes(panel_key)
            if self._tree_snapshot_node_count_gt(cached_snapshots, visible_node_count):
                status_message = (
                    "Expanded from local snapshot. Refreshing live content..."
                )
                restored = self._restore_tree_items_snapshot_if_reasonable(
                    panel_key, cached_snapshots, status_message, context="expanded_all_restore"
                )
                if not restored:
                    self._schedule_snapshot_branch_refresh(panel_key, delay_ms=400)
            self._refresh_expand_all_button_for_panel(panel_key)
            self._set_tree_status_message(
                panel_key,
                "Expanded from local snapshot. Refreshing live content...",
                loading=bool(self._pending_snapshot_branch_refresh.get(panel_key)),
            )
            return
        if self._can_fast_bulk_expand(panel_key):
            self._fast_expand_all_loaded_tree(panel_key)
            return
        self._expand_all_pending[panel_key] = True
        self._reset_expand_all_progress(panel_key)
        self._set_expand_all_button_label(panel_key, True)
        self._continue_expand_all(panel_key)

    def on_site_selector_changed(self, selector_group, *, force=False):
        try:
            if self._pending_login_restore_args and not force:
                self._log_restore_phase(
                    "selector_change_deferred_for_pending_login_restore",
                    selector_group=selector_group,
                    handler="on_site_selector_changed",
                    ui_rebind_in_progress=self._memory_ui_rebind_in_progress,
                )
                return
            if self._suppress_selector_change_handlers and not force:
                self._log_restore_phase(
                    "selector_change_suppressed",
                    selector_group=selector_group,
                    handler="on_site_selector_changed",
                    ui_rebind_in_progress=self._memory_ui_rebind_in_progress,
                )
                return

            if selector_group == "source":
                site_selector = self.planning_inputs.get("Source Site")
                library_selector = self.planning_inputs.get("Source Library")
            else:
                site_selector = self.planning_inputs.get("Destination Site")
                library_selector = self.planning_inputs.get("Destination Library")

            if site_selector is None or library_selector is None:
                return

            self._populate_library_selector_for_group(selector_group)
            self.on_library_selector_changed(selector_group, force=force)
        except Exception as exc:
            self._log_restore_exception("on_site_selector_changed", exc)

    def on_library_selector_changed(self, selector_group, *, force=False):
        try:
            self._log_library_restore_step("step_01_enter", selector_group=selector_group, force=force)
            if self._pending_login_restore_args and not force:
                self._log_restore_phase(
                    "selector_change_deferred_for_pending_login_restore",
                    selector_group=selector_group,
                    handler="on_library_selector_changed",
                    ui_rebind_in_progress=self._memory_ui_rebind_in_progress,
                )
                return
            if self._suppress_selector_change_handlers and not force:
                self._log_restore_phase(
                    "selector_change_suppressed",
                    selector_group=selector_group,
                    handler="on_library_selector_changed",
                    ui_rebind_in_progress=self._memory_ui_rebind_in_progress,
                )
                return

            if selector_group == "source":
                self._log_library_restore_step("step_02_resolve_source_selectors_enter", selector_group=selector_group)
                site_selector = self.planning_inputs.get("Source Site")
                library_selector = self.planning_inputs.get("Source Library")
                self.bottom_source.setText(f"Source: {library_selector.currentText()}" if library_selector else "Source: Not Set")
                self._log_library_restore_step("step_02_resolve_source_selectors_exit", selector_group=selector_group, has_site_selector=site_selector is not None, has_library_selector=library_selector is not None)
            else:
                self._log_library_restore_step("step_02_resolve_destination_selectors_enter", selector_group=selector_group)
                site_selector = self.planning_inputs.get("Destination Site")
                library_selector = self.planning_inputs.get("Destination Library")
                self.bottom_destination.setText(f"Destination: {library_selector.currentText()}" if library_selector else "Destination: Not Set")
                self._log_library_restore_step("step_02_resolve_destination_selectors_exit", selector_group=selector_group, has_site_selector=site_selector is not None, has_library_selector=library_selector is not None)

            self._log_library_restore_step("step_03_read_current_data_enter", selector_group=selector_group)
            selected_site = site_selector.currentData() if site_selector is not None else None
            selected_library = library_selector.currentData() if library_selector is not None else None
            self._log_library_restore_step(
                "step_03_read_current_data_exit",
                selector_group=selector_group,
                site_is_dict=isinstance(selected_site, dict),
                library_is_dict=isinstance(selected_library, dict),
                library_id=(selected_library or {}).get("id", "") if isinstance(selected_library, dict) else "",
                library_name=(selected_library or {}).get("name", "") if isinstance(selected_library, dict) else "",
            )
            if not isinstance(selected_site, dict) or not isinstance(selected_library, dict) or not selected_library.get("id"):
                self._log_library_restore_step("step_04_invalid_selection_enter", selector_group=selector_group)
                self.set_tree_placeholder(
                    selector_group,
                    "Select a library to load root content.",
                )
                self.update_selector_context_labels()
                self._log_library_restore_step("step_04_invalid_selection_exit", selector_group=selector_group)
                return

            self._log_library_restore_step("step_05_update_labels_enter", selector_group=selector_group)
            self.update_selector_context_labels()
            self._log_library_restore_step("step_05_update_labels_exit", selector_group=selector_group)
            self._log_library_restore_step("step_06_load_library_root_enter", selector_group=selector_group)
            self.load_library_root(selector_group, selected_site, selected_library)
            if not getattr(self, "_memory_restore_in_progress", False):
                self._schedule_live_root_refresh(
                    selector_group,
                    delay_ms=700,
                    site=selected_site,
                    library=selected_library,
                )
            self._log_library_restore_step("step_06_load_library_root_exit", selector_group=selector_group)
            self._log_library_restore_step("step_07_exit", selector_group=selector_group)
        except Exception as exc:
            self._log_restore_exception("on_library_selector_changed", exc)

    def reset_root_panels(self):
        self.pending_root_drive_ids = {"source": "", "destination": ""}
        self.pending_folder_loads = {"source": set(), "destination": set()}
        self._pending_source_navigation = None
        self._pending_destination_navigation = None
        self._workspace_restore_expanded_all_intent = {"source": False, "destination": False}
        self._expand_all_pending = {"source": False, "destination": False}
        self._expand_all_queue = {"source": deque(), "destination": deque()}
        self._expand_all_source_model_queue = deque()
        self._expand_all_seen = {"source": set(), "destination": set()}
        self._expand_all_requeue_attempts = {"source": {}, "destination": {}}
        self._expand_all_deferred_refresh = {"source": False, "destination": False}
        self._source_column_refresh_pending = False
        self._source_expand_all_folder_load_log_counter = 0
        self._expand_all_status_last_update_ms = {"source": 0, "destination": 0}
        for timer in getattr(self, "_expand_all_timers", {}).values():
            timer.stop()
        self.active_root_request_signatures = {"source": None, "destination": None}
        self.loaded_root_request_signatures = {"source": None, "destination": None}
        self._reset_full_count_state()
        self._reset_destination_full_tree_state()
        self.set_tree_placeholder("source", "Select a source library to load root content.")
        self.set_tree_placeholder("destination", "Select a destination library to load root content.")
        self._set_expand_all_button_label("source", False)
        self._set_expand_all_button_label("destination", False)
        self._destination_restore_materialization_user_paused = False
        self._schedule_progress_summary_refresh()

    def set_tree_placeholder(self, panel_key, message):
        if panel_key == "source":
            tree = self.source_tree_widget
            status = self.source_tree_status
        else:
            tree = self.destination_tree_widget
            status = self.destination_tree_status

        tree.clear()
        placeholder = QTreeWidgetItem([message])
        placeholder.setData(0, Qt.UserRole, {"placeholder": True})
        tree.addTopLevelItem(placeholder)
        tree.setEnabled(False)
        self._set_tree_status_message(panel_key, message, loading=str(message or "").lower().startswith("loading"))
        if self._full_trace_enabled():
            log_trace(
                "tree",
                "set_tree_placeholder",
                panel_key=panel_key,
                message_excerpt=(message or "")[:200],
                tree_enabled=False,
            )

    def _capture_tree_items_snapshot(self, panel_key):
        tree, _status = self._get_tree_and_status(panel_key)
        snapshots = []
        if tree is None:
            return snapshots
        for index in range(tree.topLevelItemCount()):
            item = tree.topLevelItem(index)
            node_data = item.data(0, Qt.UserRole) or {}
            if node_data.get("placeholder"):
                continue
            snapshots.append(self._serialize_tree_item_snapshot(item))
        if panel_key == "destination":
            for snap in snapshots:
                self._normalize_destination_snapshot_tree_for_persist(snap)
        return snapshots

    def _serialize_tree_item_snapshot(self, item):
        snapshot = {
            "text": item.text(0),
            "data": dict(item.data(0, Qt.UserRole) or {}),
            "expanded": bool(item.isExpanded()),
            "children": [],
        }
        for index in range(item.childCount()):
            snapshot["children"].append(self._serialize_tree_item_snapshot(item.child(index)))
        return snapshot

    def _deserialize_tree_item_snapshot(self, snapshot):
        item = QTreeWidgetItem([str((snapshot or {}).get("text", ""))])
        item.setData(0, Qt.UserRole, dict((snapshot or {}).get("data", {}) or {}))
        for child_snapshot in list((snapshot or {}).get("children", []) or []):
            item.addChild(self._deserialize_tree_item_snapshot(child_snapshot))
        item.setExpanded(bool((snapshot or {}).get("expanded", False)))
        return item

    def _deserialize_tree_item_snapshot_shallow(self, snapshot):
        """Build one QTreeWidgetItem from snapshot without attaching children (for chunked restore)."""
        item = QTreeWidgetItem([str((snapshot or {}).get("text", ""))])
        item.setData(0, Qt.UserRole, dict((snapshot or {}).get("data", {}) or {}))
        item.setExpanded(bool((snapshot or {}).get("expanded", False)))
        children = list((snapshot or {}).get("children", []) or [])
        return item, children

    def _snapshot_sync_restore_max_nodes(self):
        """Snapshots with more nodes use time-sliced restore to avoid blocking the UI thread."""
        raw = os.environ.get("OZLINK_SNAPSHOT_SYNC_NODE_MAX", "").strip()
        if not raw:
            return 500
        try:
            v = int(raw)
            return max(50, v)
        except ValueError:
            return 500

    def _snapshot_chunk_node_budget(self):
        raw = os.environ.get("OZLINK_SNAPSHOT_CHUNK_NODES", "").strip()
        if not raw:
            return 70
        try:
            v = int(raw)
            return max(20, min(2000, v))
        except ValueError:
            return 70

    def _tree_snapshot_restore_skip_legacy_enabled(self):
        return os.environ.get("OZLINK_SKIP_LARGE_SNAPSHOT_RESTORE", "").strip() in ("1", "true", "True", "yes", "YES")

    def _finalize_tree_snapshot_restore(self, panel_key, snapshots, status_message):
        if panel_key == "destination":
            self._destination_snapshot_chunked_restore_active = False
        tree, _status = self._get_tree_and_status(panel_key)
        if panel_key == "destination" and self._tree_snapshot_node_count_gt(snapshots, 1):
            self._destination_root_prime_pending = False
        if tree is not None:
            tree.setEnabled(True)
            tree.setUpdatesEnabled(True)
        self._set_tree_status_message(panel_key, status_message, loading=False)
        if tree is not None:
            tree.viewport().update()
        runtime_snapshots = getattr(self, "_runtime_session_tree_snapshots", None)
        if not isinstance(runtime_snapshots, dict):
            runtime_snapshots = {"source": [], "destination": []}
            self._runtime_session_tree_snapshots = runtime_snapshots
        runtime_snapshots[panel_key] = list(snapshots or [])

    def _abort_destination_chunked_bind_for_snapshot_restore(self):
        st = getattr(self, "_destination_chunked_bind_state", None)
        if not isinstance(st, dict):
            return
        self._destination_future_bind_generation = int(getattr(self, "_destination_future_bind_generation", 0)) + 1
        tree = getattr(self, "destination_tree_widget", None)
        try:
            if tree is not None:
                tree.blockSignals(False)
        except Exception:
            pass
        self._destination_chunked_bind_state = None
        self._log_restore_phase(
            "destination_chunked_bind_aborted",
            phase_failed="destination_snapshot_restore_preempt",
            bind_generation=st.get("gen"),
            wall_duration_ms=0.0,
            error_type="SnapshotRestorePreempt",
        )
        self._flush_pending_destination_library_root_if_any(flush_reason="after_snapshot_preempt_bind")

    def _restore_tree_items_snapshot_chunked(
        self, panel_key, snapshots, status_message, *, context, on_complete=None
    ):
        tree, _status = self._get_tree_and_status(panel_key)
        if tree is None or not snapshots:
            if on_complete:
                on_complete()
            return
        self._tree_snapshot_restore_generation[panel_key] = (
            int(self._tree_snapshot_restore_generation.get(panel_key, 0)) + 1
        )
        generation = self._tree_snapshot_restore_generation[panel_key]
        total_nodes = self._count_tree_snapshot_nodes(snapshots)
        budget = self._snapshot_chunk_node_budget()
        built = [0]

        if panel_key == "destination":
            self._abort_destination_chunked_bind_for_snapshot_restore()
            self._destination_snapshot_chunked_restore_active = False

        tree.clear()
        tree.setUpdatesEnabled(False)
        tree.setEnabled(True)
        work = deque()
        for snap in snapshots:
            item, kids = self._deserialize_tree_item_snapshot_shallow(snap)
            tree.addTopLevelItem(item)
            built[0] += 1
            if kids:
                work.append((item, kids, 0))

        self._set_tree_status_message(
            panel_key,
            f"Loading tree snapshot… ({built[0]}/{total_nodes})",
            loading=True,
        )
        log_info(
            "tree_snapshot_chunked_restore_started",
            panel_key=panel_key,
            total_nodes=total_nodes,
            chunk_nodes=budget,
            context=context,
        )

        def tick():
            if generation != self._tree_snapshot_restore_generation.get(panel_key):
                return
            slice_timer = QElapsedTimer()
            slice_timer.start()
            max_slice_ms = 10
            raw_ms = os.environ.get("OZLINK_SNAPSHOT_CHUNK_MAX_MS", "").strip()
            if raw_ms:
                try:
                    max_slice_ms = max(4, min(50, int(raw_ms)))
                except ValueError:
                    pass
            b = budget
            while b > 0 and work:
                if slice_timer.elapsed() > max_slice_ms:
                    QTimer.singleShot(0, tick)
                    return
                parent, children, idx = work[0]
                if idx >= len(children):
                    work.popleft()
                    continue
                child_snap = children[idx]
                work[0] = (parent, children, idx + 1)
                child_item, grand = self._deserialize_tree_item_snapshot_shallow(child_snap)
                try:
                    parent.addChild(child_item)
                except RuntimeError as exc:
                    # Re-entrancy (e.g. combo currentData() while another path cleared the tree)
                    # can delete parents before this tick runs; invalidate this restore and stop.
                    err = str(exc).lower()
                    if "already deleted" not in err and "internal c++ object" not in err:
                        raise
                    self._tree_snapshot_restore_generation[panel_key] = (
                        int(self._tree_snapshot_restore_generation.get(panel_key, 0)) + 1
                    )
                    if panel_key == "destination":
                        self._destination_snapshot_chunked_restore_active = False
                    log_info(
                        "tree_snapshot_chunked_restore_aborted",
                        panel_key=panel_key,
                        context=context,
                        built_nodes=built[0],
                        total_nodes=total_nodes,
                        error=str(exc),
                    )
                    t_abort, _ = self._get_tree_and_status(panel_key)
                    if t_abort is not None:
                        t_abort.setEnabled(True)
                        t_abort.setUpdatesEnabled(True)
                        t_abort.viewport().update()
                    self._set_tree_status_message(
                        panel_key,
                        "Tree snapshot restore interrupted; try Refresh Cache.",
                        loading=False,
                    )
                    if on_complete:
                        on_complete()
                    return
                built[0] += 1
                b -= 1
                if grand:
                    work.append((child_item, grand, 0))
            if generation != self._tree_snapshot_restore_generation.get(panel_key):
                return
            if work:
                if built[0] % max(1, budget * 3) < budget or built[0] == total_nodes:
                    self._set_tree_status_message(
                        panel_key,
                        f"Loading tree snapshot… ({min(built[0], total_nodes)}/{total_nodes})",
                        loading=True,
                    )
                QTimer.singleShot(0, tick)
                return
            self._finalize_tree_snapshot_restore(panel_key, snapshots, status_message)
            log_info(
                "tree_snapshot_chunked_restore_complete",
                panel_key=panel_key,
                total_nodes=built[0],
                context=context,
            )
            if on_complete:
                on_complete()

        if work:
            if panel_key == "destination":
                self._destination_snapshot_chunked_restore_active = True
            QTimer.singleShot(0, tick)
        else:
            self._finalize_tree_snapshot_restore(panel_key, snapshots, status_message)
            log_info(
                "tree_snapshot_chunked_restore_complete",
                panel_key=panel_key,
                total_nodes=built[0],
                context=context,
            )
            if on_complete:
                on_complete()

    def _destination_snapshot_shows_materialized_allocation_children(self, snapshot):
        children = list((snapshot or {}).get("children") or [])
        if not children:
            return False
        for ch in children:
            d = (ch or {}).get("data") or {}
            if not d.get("placeholder"):
                return True
            role = str(d.get("placeholder_role") or "").strip()
            if role in ("projection_pending", "lazy_unloaded", "loading_in_progress"):
                return False
        return True

    def _normalize_destination_snapshot_tree_for_persist(self, snapshot):
        """Post-order: persist children_loaded + projection cache keys for [Allocated] folders in JSON snapshots."""
        for ch in list((snapshot or {}).get("children") or []):
            self._normalize_destination_snapshot_tree_for_persist(ch)
        data = snapshot.get("data")
        if not isinstance(data, dict):
            return
        if not self.node_is_planned_allocation(data) or not bool(data.get("is_folder", True)):
            return
        if not self._destination_snapshot_shows_materialized_allocation_children(snapshot):
            return
        data["children_loaded"] = True
        data["projection_unresolved_terminal"] = False
        path = self._canonical_destination_projection_path(
            data.get("item_path") or data.get("display_path") or data.get("destination_path") or ""
        )
        if path:
            data["allocation_projection_destination_path_saved"] = path
        parts = []
        for ch in (snapshot or {}).get("children") or []:
            cd = (ch or {}).get("data") or {}
            if cd.get("placeholder"):
                continue
            cp = self._canonical_destination_projection_path(
                cd.get("item_path") or cd.get("display_path") or cd.get("destination_path") or ""
            )
            parts.append((cp, bool(cd.get("is_folder")), str(cd.get("name") or "")))
        try:
            data["allocation_projection_children_signature"] = hashlib.sha256(
                repr(sorted(parts)).encode("utf-8")
            ).hexdigest()[:32]
        except Exception:
            pass

    def _allocation_projection_children_signature_from_item(self, item):
        parts = []
        if item is None:
            return ""
        for i in range(item.childCount()):
            cd = item.child(i).data(0, Qt.UserRole) or {}
            if cd.get("placeholder"):
                continue
            cp = self._canonical_destination_projection_path(
                cd.get("item_path") or cd.get("display_path") or cd.get("destination_path") or ""
            )
            parts.append((cp, bool(cd.get("is_folder")), str(cd.get("name") or "")))
        try:
            return hashlib.sha256(repr(sorted(parts)).encode("utf-8")).hexdigest()[:32]
        except Exception:
            return ""

    def _stamp_allocation_projection_cache_metadata(self, item, node_data, move):
        nd = dict(node_data or {})
        expected = ""
        if move is not None:
            expected = self._canonical_destination_projection_path(self._allocation_projection_path(move))
        if not expected:
            expected = self._canonical_destination_projection_path(self._tree_item_path(nd))
        if expected:
            nd["allocation_projection_destination_path_saved"] = expected
        sig = self._allocation_projection_children_signature_from_item(item)
        if sig:
            nd["allocation_projection_children_signature"] = sig
        item.setData(0, Qt.UserRole, nd)
        return nd

    def _invalidate_stale_destination_allocation_projection(self, item, node_data, move):
        if item is None or not isinstance(node_data, dict):
            return node_data
        if not self.node_is_planned_allocation(node_data) or not bool(node_data.get("children_loaded")):
            return node_data
        saved = str(node_data.get("allocation_projection_destination_path_saved") or "").strip()
        if move is not None:
            expected = self._canonical_destination_projection_path(self._allocation_projection_path(move))
            if saved and expected and saved != expected:
                nd = dict(node_data)
                nd["children_loaded"] = False
                nd.pop("allocation_projection_destination_path_saved", None)
                nd.pop("allocation_projection_children_signature", None)
                item.setData(0, Qt.UserRole, nd)
                return item.data(0, Qt.UserRole) or {}
        sig = str(node_data.get("allocation_projection_children_signature") or "").strip()
        if sig and self._destination_allocation_folder_shows_materialized_children(item):
            current_sig = self._allocation_projection_children_signature_from_item(item)
            if current_sig and current_sig != sig:
                nd = dict(node_data)
                nd["children_loaded"] = False
                nd.pop("allocation_projection_destination_path_saved", None)
                nd.pop("allocation_projection_children_signature", None)
                item.setData(0, Qt.UserRole, nd)
                return item.data(0, Qt.UserRole) or {}
        return node_data

    def _startup_tree_snapshot_node_limit(self):
        """Used only when OZLINK_SKIP_LARGE_SNAPSHOT_RESTORE=1: skip restore above this node count.

        Default 10_000. Set OZLINK_MAX_STARTUP_SNAPSHOT_NODES=0 with skip enabled to never skip.
        Normal mode always restores snapshots (sync or chunked); see OZLINK_SNAPSHOT_SYNC_NODE_MAX.
        """
        raw = os.environ.get("OZLINK_MAX_STARTUP_SNAPSHOT_NODES", "").strip()
        if not raw:
            return 10_000
        try:
            return int(raw)
        except ValueError:
            return 10_000

    def _restore_tree_items_snapshot_if_reasonable(
        self, panel_key, snapshots, status_message, *, context="startup", on_complete=None
    ):
        if not snapshots:
            return False
        limit = self._startup_tree_snapshot_node_limit()
        if (
            self._tree_snapshot_restore_skip_legacy_enabled()
            and limit > 0
            and self._tree_snapshot_node_count_gt(snapshots, limit)
        ):
            log_info(
                "Tree snapshot restore skipped (too many nodes for responsive startup).",
                panel_key=panel_key,
                snapshot_node_count_at_least=limit + 1,
                limit=limit,
                context=context,
            )
            self._set_tree_status_message(
                panel_key,
                "Loading library (large tree snapshot skipped for faster startup)...",
                loading=True,
            )
            return False
        total_nodes = self._count_tree_snapshot_nodes(snapshots)
        sync_max = self._snapshot_sync_restore_max_nodes()
        if total_nodes <= sync_max:
            ok = self._restore_tree_items_snapshot(panel_key, snapshots, status_message)
            if ok and on_complete:
                on_complete()
            return ok
        self._restore_tree_items_snapshot_chunked(
            panel_key, snapshots, status_message, context=context, on_complete=on_complete
        )
        return True

    def _restore_tree_items_snapshot(self, panel_key, snapshots, status_message):
        tree, _status = self._get_tree_and_status(panel_key)
        if tree is None or not snapshots:
            return False
        tree.clear()
        tree.setUpdatesEnabled(False)
        try:
            for snapshot in snapshots:
                tree.addTopLevelItem(self._deserialize_tree_item_snapshot(snapshot))
        finally:
            tree.setUpdatesEnabled(True)
        self._finalize_tree_snapshot_restore(panel_key, snapshots, status_message)
        return True

    def _maybe_restore_runtime_snapshot_after_root_bind(self, panel_key):
        ui_state = self._session_workspace_ui_state()
        if not bool(ui_state.get(f"{panel_key}_expanded_all", False)):
            return False
        runtime_snapshots = self._runtime_tree_snapshot_for_panel(panel_key)
        if not runtime_snapshots:
            return False
        current_node_count = self._count_expandable_tree_nodes(panel_key)
        if not self._tree_snapshot_node_count_gt(runtime_snapshots, current_node_count):
            return False
        def _after_runtime_snap():
            self._refresh_expand_all_button_for_panel(panel_key)
            self._restore_selected_tree_path(
                panel_key,
                str(ui_state.get(f"{panel_key}_selected_path", "") or ""),
            )

        restored = self._restore_tree_items_snapshot_if_reasonable(
            panel_key,
            runtime_snapshots,
            "Expanded from local snapshot. Refreshing live content...",
            context="runtime_snapshot_after_root_bind",
            on_complete=_after_runtime_snap,
        )
        if not restored:
            self._schedule_snapshot_branch_refresh(panel_key, delay_ms=400)
        return restored

    def load_library_root(self, panel_key, site, library, force_refresh=False):
        try:
            self._log_library_restore_step(
                "load_root_step_01_enter",
                panel_key=panel_key,
                site_name=site.get("name", "") if isinstance(site, dict) else "",
                library_name=library.get("name", "") if isinstance(library, dict) else "",
            )
            request_signature = self._build_root_request_signature(panel_key, site, library)
            self._log_library_restore_step("load_root_step_01b_signature", panel_key=panel_key, request_signature=request_signature)
            drive_id = library.get("id", "")
            self._log_library_restore_step("load_root_step_02_drive_id", panel_key=panel_key, drive_id=drive_id)
            if not drive_id:
                self._log_library_restore_step("load_root_step_03_missing_drive_enter", panel_key=panel_key)
                self.active_root_request_signatures[panel_key] = None
                self.loaded_root_request_signatures[panel_key] = None
                if panel_key == "source":
                    self._reset_full_count_state()
                if panel_key == "destination":
                    self._reset_destination_full_tree_state()
                self.set_tree_placeholder(panel_key, "Select a library to load root content.")
                self._log_library_restore_step("load_root_step_03_missing_drive_exit", panel_key=panel_key)
                return

            if force_refresh:
                self.graph.clear_drive_children_cache(drive_id)
                self.loaded_root_request_signatures[panel_key] = None

            active_entry = self.root_load_workers.get(panel_key)
            active_signature = active_entry.get("request_signature") if active_entry else None
            if active_entry and active_signature == request_signature and active_entry["worker"].isRunning():
                self._log_restore_phase(
                    "root_load deduped",
                    panel_key=panel_key,
                    reason="already_active",
                    worker_id=active_entry.get("id"),
                    request_signature=request_signature,
                )
                return

            if self.loaded_root_request_signatures.get(panel_key) == request_signature and self._tree_has_bound_root_content(panel_key):
                if not getattr(self, "_sharepoint_lazy_mode", False):
                    if panel_key == "source":
                        self._schedule_full_count_with_restore_backoff(drive_id)
                    if panel_key == "destination":
                        self.start_destination_full_tree_worker(drive_id)
                self._log_restore_phase(
                    "root_load reused_existing",
                    panel_key=panel_key,
                    reason="same_request_already_bound",
                    request_signature=request_signature,
                )
                return

            if panel_key == "destination" and self._destination_future_tree_bind_busy():
                self._queue_deferred_destination_library_root(site, library, force_refresh)
                return

            self._log_library_restore_step("load_root_step_04_pending_state_enter", panel_key=panel_key)
            self.pending_root_drive_ids[panel_key] = drive_id
            self.pending_folder_loads[panel_key] = set()
            self.active_root_request_signatures[panel_key] = request_signature
            self._log_library_restore_step("load_root_step_04_pending_state_exit", panel_key=panel_key)

            self._log_library_restore_step("load_root_step_05_placeholder_enter", panel_key=panel_key)
            self.set_tree_placeholder(panel_key, "Loading root content...")
            self._log_restore_phase("root_load placeholder_set", panel_key=panel_key, request_signature=request_signature)
            self._log_library_restore_step("load_root_step_05_placeholder_exit", panel_key=panel_key)

            self._log_library_restore_step("load_root_step_06_context_enter", panel_key=panel_key)
            use_cache_only = bool(
                (
                    getattr(self, "_memory_restore_in_progress", False)
                    or getattr(self, "_suppress_autosave", False)
                )
                and self.graph.has_cached_drive_root_children(drive_id)
            )
            worker_context = {
                "site_id": site.get("id", ""),
                "site_name": site.get("name", ""),
                "library_id": library.get("id", drive_id),
                "library_name": library.get("name", ""),
                "tree_role": panel_key,
                "cache_only": use_cache_only,
            }
            self._log_library_restore_step("load_root_step_06_context_exit", panel_key=panel_key, worker_context=worker_context)

            self._log_library_restore_step("load_root_step_07_worker_create_enter", panel_key=panel_key)
            worker = RootLoadWorker(self.graph, panel_key, drive_id, worker_context)
            self._log_library_restore_step("load_root_step_07_worker_create_exit", panel_key=panel_key)

            self._log_library_restore_step("load_root_step_07b_register_worker_enter", panel_key=panel_key)
            worker_entry = self._register_root_worker(panel_key, worker)
            worker_entry["request_signature"] = request_signature
            self._log_library_restore_step(
                "load_root_step_07b_register_worker_exit",
                panel_key=panel_key,
                worker_id=worker_entry["id"],
            )

            self._log_library_restore_step("load_root_step_08_connect_enter", panel_key=panel_key)
            worker.success.connect(lambda payload, worker_id=worker_entry["id"]: self._safe_invoke("root_worker.success", self.on_root_load_success, payload, worker_id))
            worker.error.connect(lambda payload, worker_id=worker_entry["id"]: self._safe_invoke("root_worker.error", self.on_root_load_error, payload, worker_id))
            worker.finished.connect(lambda key=panel_key, worker_id=worker_entry["id"]: self._safe_invoke("root_worker.finished", self.on_root_worker_finished, key, worker_id))
            self._log_library_restore_step("load_root_step_08_connect_exit", panel_key=panel_key)

            self._log_library_restore_step("load_root_step_10_start_enter", panel_key=panel_key)
            worker.start()
            self._log_restore_phase(
                "root_load started_new",
                panel_key=panel_key,
                worker_id=worker_entry["id"],
                request_signature=request_signature,
            )
            self._log_library_restore_step("load_root_step_10_start_exit", panel_key=panel_key)
        except Exception as exc:
            self._log_restore_exception("load_library_root", exc)

    def on_root_worker_finished(self, panel_key, worker_id):
        try:
            self._cleanup_root_worker(panel_key, worker_id)
            self._refresh_planning_loading_banner()
        except Exception as exc:
            self._log_restore_exception("on_root_worker_finished", exc)

    def on_root_load_success(self, payload, worker_id):
        try:
            self._log_restore_phase(
                "root_worker_success handler",
                payload_panel=payload.get("panel_key", ""),
                payload_drive=payload.get("drive_id", ""),
                worker_id=worker_id,
            )
            panel_key = payload.get("panel_key", "")
            drive_id = payload.get("drive_id", "")
            active_entry = self.root_load_workers.get(panel_key)
            if not active_entry or active_entry.get("id") != worker_id:
                self._log_worker_lifecycle("stale_success_skipped", "root", worker_id, panel_key, drive_id=drive_id)
                return
            if self.pending_root_drive_ids.get(panel_key) != drive_id:
                self._log_restore_phase("root_worker_success stale_payload_skipped", panel_key=panel_key, drive_id=drive_id)
                return

            items = payload.get("items", [])
            self._log_restore_phase("root_worker_success payload_received", panel_key=panel_key, item_count=len(items))
            if (
                panel_key == "source"
                and not getattr(self, "_memory_restore_in_progress", False)
            ):
                incoming_node_count = self._count_root_payload_nodes(items)
                visible_node_count = self._count_expandable_tree_nodes("source")
                if incoming_node_count > 0 and visible_node_count > incoming_node_count:
                    self._log_restore_phase(
                        "source_shallow_root_payload_skipped",
                        worker_id=worker_id,
                        incoming_node_count=incoming_node_count,
                        visible_node_count=visible_node_count,
                    )
                    return
            if (
                panel_key == "destination"
                and not getattr(self, "_memory_restore_in_progress", False)
            ):
                incoming_node_count = self._count_root_payload_nodes(items)
                visible_node_count = self._count_expandable_tree_nodes("destination")
                visible_future_state_count = self._count_visible_destination_future_state_nodes()
                current_richness = visible_node_count + visible_future_state_count
                if incoming_node_count > 0 and current_richness > incoming_node_count:
                    self._log_restore_phase(
                        "destination_shallow_root_payload_skipped",
                        worker_id=worker_id,
                        incoming_node_count=incoming_node_count,
                        visible_node_count=visible_node_count,
                        visible_future_state_count=visible_future_state_count,
                        current_richness=current_richness,
                    )
                    return
            if panel_key == "source":
                self._reset_source_background_preload_state()
            if panel_key == "destination":
                existing_future_state_count = self._count_visible_destination_future_state_nodes()
                if existing_future_state_count > 0:
                    self._log_restore_phase(
                        "destination_projection_lost_on_root_clear",
                        panel_key=panel_key,
                        future_state_count=existing_future_state_count,
                        reason="destination_root_rebuild_incoming",
                    )
            self._log_restore_phase(
                "root_worker_success isolation_flag_state",
                panel_key=panel_key,
                skip_root_bind_body_for_isolation=self._skip_root_bind_body_for_isolation,
            )
            if self._skip_root_bind_body_for_isolation:
                self._log_restore_phase(
                    "root_worker_success body_skipped_for_isolation",
                    panel_key=panel_key,
                    item_count=len(items),
                    reason="temporary binary isolation flag enabled",
                )
                return
            pre_bind_top_item_name = ""
            tree_for_top_item, _ = self._get_tree_and_status(panel_key)
            if tree_for_top_item is not None and tree_for_top_item.topLevelItemCount() > 0:
                pre_bind_top_item_name = str(tree_for_top_item.topLevelItem(0).text(0) or "")
            self._apply_root_payload_to_tree(panel_key, items)
            self._refresh_tree_column_width(panel_key)
            self.loaded_root_request_signatures[panel_key] = active_entry.get("request_signature")
            self._log_restore_phase(
                "root_bind applied",
                panel_key=panel_key,
                worker_id=worker_id,
                request_signature=self.loaded_root_request_signatures.get(panel_key),
            )
            post_bind_top_item_name = ""
            post_bind_top_item_semantic_path = ""
            tree_for_top_item, _ = self._get_tree_and_status(panel_key)
            if tree_for_top_item is not None and tree_for_top_item.topLevelItemCount() > 0:
                top_item = tree_for_top_item.topLevelItem(0)
                post_bind_top_item_name = str(top_item.text(0) or "")
                node_data = top_item.data(0, Qt.UserRole) or {}
                post_bind_top_item_semantic_path = str(node_data.get("semantic_path") or "")
            if panel_key == "destination":
                self._log_restore_phase(
                    "destination_root_top_item_bound",
                    before_name=pre_bind_top_item_name,
                    after_name=post_bind_top_item_name,
                    after_semantic_path=post_bind_top_item_semantic_path,
                    still_root_name=post_bind_top_item_name.strip().lower() == "root",
                )
            restored_runtime_snapshot = False
            pending_session_panels_pre = (
                set(self._pending_session_workspace_restore_panels)
                if self._pending_session_workspace_ui_state
                else set()
            )
            pending_session_chunked_ui_deferred = False
            if (
                self._live_root_refresh_request_signature.get(panel_key, "")
                and self.loaded_root_request_signatures.get(panel_key) == self._live_root_refresh_request_signature.get(panel_key, "")
                and self._live_root_refresh_ui_state.get(panel_key)
            ):
                self._restore_workspace_tree_panel_state(panel_key, self._live_root_refresh_ui_state[panel_key])
                self._live_root_refresh_request_signature[panel_key] = ""
                self._live_root_refresh_ui_state[panel_key] = None
            elif panel_key not in pending_session_panels_pre and self._maybe_restore_runtime_snapshot_after_root_bind(panel_key):
                restored_runtime_snapshot = True
                self._schedule_snapshot_branch_refresh(panel_key, delay_ms=0)

            if panel_key in pending_session_panels_pre and self._pending_session_workspace_ui_state:
                panel_ui_state = self._pending_session_workspace_ui_state
                panel_snapshots = list(self._pending_session_tree_snapshots.get(panel_key, []) or [])
                pending_session_panels = set(self._pending_session_workspace_restore_panels)
                if bool(panel_ui_state.get(f"{panel_key}_expanded_all", False)) and panel_snapshots:
                    snap_msg = "Expanded from local snapshot. Refreshing live content..."
                    node_count = self._count_tree_snapshot_nodes(panel_snapshots)
                    sync_max = self._snapshot_sync_restore_max_nodes()
                    chunked_pending = node_count > sync_max

                    def _pending_snap_selection_only():
                        self._refresh_expand_all_button_for_panel(panel_key)
                        self._restore_selected_tree_path(
                            panel_key,
                            str(panel_ui_state.get(f"{panel_key}_selected_path", "") or ""),
                        )

                    def _pending_snap_after_chunked():
                        _pending_snap_selection_only()
                        self._refresh_tree_ui_after_root_bind(panel_key, restored_runtime_snapshot=True)

                    on_done = _pending_snap_after_chunked if chunked_pending else _pending_snap_selection_only
                    ok_snap = self._restore_tree_items_snapshot_if_reasonable(
                        panel_key,
                        panel_snapshots,
                        snap_msg,
                        context="pending_session_after_root_bind",
                        on_complete=on_done,
                    )
                    if ok_snap:
                        if not chunked_pending:
                            restored_runtime_snapshot = True
                        else:
                            pending_session_chunked_ui_deferred = True
                    else:
                        self._restore_workspace_tree_panel_state(panel_key, panel_ui_state)
                else:
                    self._restore_workspace_tree_panel_state(panel_key, panel_ui_state)
                pending_session_panels.discard(panel_key)
                self._pending_session_workspace_restore_panels = pending_session_panels
                self._schedule_snapshot_branch_refresh(panel_key, delay_ms=0)
                if not pending_session_panels:
                    self._pending_session_workspace_ui_state = None
                    self._pending_session_tree_snapshots = {}

            if panel_key == "source":
                self._schedule_deferred_background_load("source", drive_id)
            if panel_key == "destination":
                self._schedule_deferred_background_load("destination", drive_id)
                self._log_restore_phase(
                    "destination_replay_after_root_rebuild_started",
                    request_signature=self.loaded_root_request_signatures.get(panel_key),
                    planned_moves_count=len(self.planned_moves),
                    proposed_folders_count=len(self.proposed_folders),
                )
                self._reset_unresolved_proposed_queue()
                self._reset_unresolved_allocation_queue()
                self._restore_destination_overlay_pending = bool(self.proposed_folders or self.planned_moves)
            if not pending_session_chunked_ui_deferred:
                self._refresh_tree_ui_after_root_bind(panel_key, restored_runtime_snapshot=restored_runtime_snapshot)
            pending_refresh_panels = self._pending_cache_refresh_panels if self._cache_refresh_restore_active else set()
            if self._cache_refresh_restore_active and panel_key in pending_refresh_panels:
                pending_refresh_panels.discard(panel_key)
                self._pending_cache_refresh_panels = pending_refresh_panels
                if not pending_refresh_panels:
                    self._finalize_cache_refresh_workspace_restore()
            if panel_key == "destination":
                future_state_count = self._count_visible_destination_future_state_nodes()
                self._log_restore_phase(
                    "destination_replay_after_root_rebuild_complete",
                    request_signature=self.loaded_root_request_signatures.get(panel_key),
                    future_state_count=future_state_count,
                    visible_proposed_count=self._count_visible_destination_proposed_nodes(),
                )
                self._log_restore_phase(
                    "destination_projection_survived_root_bind",
                    request_signature=self.loaded_root_request_signatures.get(panel_key),
                    future_state_count=future_state_count,
                    survived=future_state_count > 0,
                )
            self._schedule_workspace_ui_persist(panel_key=panel_key)
            if drive_id and self._graph_settings_delta_sync_enabled():
                delta_delay_ms = 4200 if panel_key == "source" else 6400
                QTimer.singleShot(
                    int(delta_delay_ms),
                    lambda d=drive_id: self._run_drive_delta_sync_best_effort(d),
                )
            self._log_restore_phase("root_worker_success completed", panel_key=panel_key, item_count=len(items))
        except Exception as exc:
            self._log_restore_exception("on_root_load_success", exc)

    def on_root_load_error(self, payload, worker_id):
        try:
            panel_key = payload.get("panel_key", "")
            drive_id = payload.get("drive_id", "")
            error_text = str(payload.get("error", "") or "")
            active_entry = self.root_load_workers.get(panel_key)
            if not active_entry or active_entry.get("id") != worker_id:
                self._log_worker_lifecycle("stale_error_skipped", "root", worker_id, panel_key, drive_id=drive_id)
                return
            if self.pending_root_drive_ids.get(panel_key) != drive_id:
                return
            self._log_restore_phase(
                "root_worker_error_received",
                panel_key=panel_key,
                drive_id=drive_id,
                worker_id=worker_id,
                error=error_text,
            )

            if panel_key == "source":
                self._reset_full_count_state()
                self._reset_source_background_preload_state()
            if panel_key == "destination":
                self._reset_destination_full_tree_state()
            self.set_tree_placeholder(panel_key, "Could not load library content.")
            if panel_key == "source":
                self._set_tree_status_message("source", "Could not load source library content from Microsoft 365.", loading=False)
            else:
                self._set_tree_status_message("destination", "Could not load destination library content from Microsoft 365.", loading=False)
            pending_refresh_panels = self._pending_cache_refresh_panels if self._cache_refresh_restore_active else set()
            if self._cache_refresh_restore_active and panel_key in pending_refresh_panels:
                fallback_snapshots = dict(getattr(self, "_pending_cache_refresh_tree_snapshots", {}) or {})
                status_message = (
                    "Refresh failed. Showing the last loaded source content."
                    if panel_key == "source"
                    else "Refresh failed. Showing the last loaded destination content."
                )
                restored_snapshot = self._restore_tree_items_snapshot(
                    panel_key,
                    fallback_snapshots.get(panel_key, []),
                    status_message,
                )
                if restored_snapshot and panel_key == "destination":
                    try:
                        self._materialize_destination_future_model("cache_refresh_root_error_restore")
                    except Exception as restore_exc:
                        self._log_restore_exception("on_root_load_error.destination_snapshot_restore", restore_exc)
                fallback_snapshots.pop(panel_key, None)
                self._pending_cache_refresh_tree_snapshots = fallback_snapshots
                pending_refresh_panels.discard(panel_key)
                self._pending_cache_refresh_panels = pending_refresh_panels
                if not pending_refresh_panels:
                    self._cache_refresh_restore_active = False
                    self._pending_cache_refresh_ui_state = None
                    self._pending_cache_refresh_panels = set()
                    self._pending_cache_refresh_tree_snapshots = {}
            self._refresh_planning_loading_banner()
        except Exception as exc:
            self._log_restore_exception("on_root_load_error", exc)

    def _get_tree_and_status(self, panel_key):
        if panel_key == "source":
            return self.source_tree_widget, self.source_tree_status
        return self.destination_tree_widget, self.destination_tree_status

    def _log_root_success_step(self, step_name, **data):
        tree_visible = False
        destination_ready = False
        try:
            tree_visible = self.isVisible()
            destination_ready = hasattr(self, "destination_tree_widget") and self.destination_tree_widget is not None
        except Exception:
            pass
        self._log_restore_phase(
            f"root_success {step_name}",
            window_visible=tree_visible,
            destination_tree_ready=destination_ready,
            root_tree_bind_in_progress=self._root_tree_bind_in_progress,
            **data,
        )

    def _log_library_restore_step(self, step_name, **data):
        self._log_restore_phase(
            f"library_restore {step_name}",
            root_tree_bind_in_progress=self._root_tree_bind_in_progress,
            ui_rebind_in_progress=self._memory_ui_rebind_in_progress,
            **data,
        )

    def _root_tree_identity(self, tree):
        top = -1
        if tree is not None:
            if isinstance(tree, QTreeView) and tree.model() is not None:
                top = tree.model().rowCount(QModelIndex())
            elif hasattr(tree, "topLevelItemCount"):
                top = tree.topLevelItemCount()
        return {
            "is_none": tree is None,
            "class_name": tree.__class__.__name__ if tree is not None else "",
            "object_name": tree.objectName() if tree is not None else "",
            "top_level_count": top,
        }

    def _iter_tree_items(self, parent_item):
        if parent_item is None:
            return
        if not self._tree_item_is_alive(parent_item):
            return
        yield parent_item
        try:
            count = parent_item.childCount()
        except RuntimeError:
            return
        for index in range(count):
            try:
                child = parent_item.child(index)
            except RuntimeError:
                continue
            if not self._tree_item_is_alive(child):
                continue
            yield from self._iter_tree_items(child)

    def _iter_source_visible_payloads(self):
        tree = getattr(self, "source_tree_widget", None)
        if tree is None:
            return
        if self._source_tree_uses_model_view():
            model = getattr(self, "source_sharepoint_model", None)
            if model is None:
                return
            for ix in model.iter_depth_first():
                yield ix.data(Qt.UserRole) or {}
            return
        for index in range(tree.topLevelItemCount()):
            for item in self._iter_tree_items(tree.topLevelItem(index)):
                yield item.data(0, Qt.UserRole) or {}

    def _count_visible_source_relationship_nodes(self):
        count = 0
        for node_data in self._iter_source_visible_payloads():
            if self.get_source_relationship_display(node_data).get("mode") != "none":
                count += 1
        return count

    def _collect_visible_source_relationship_paths(self):
        paths = set()
        for node_data in self._iter_source_visible_payloads():
            if node_data.get("placeholder"):
                continue
            if node_data.get("source_relationship_mode") in {"direct", "inherited"}:
                source_path = self._canonical_source_projection_path(self._tree_item_path(node_data))
                if source_path:
                    paths.add(source_path)
        return paths

    def _collect_source_projection_counts(self):
        counts = {
            "visible_source_projection_count": 0,
            "direct_match_count": 0,
            "inherited_match_count": 0,
            "skipped_source_projection_count": 0,
        }
        for node_data in self._iter_source_visible_payloads():
            if node_data.get("placeholder"):
                continue
            relationship = self._evaluate_source_relationship(node_data)
            mode = relationship.get("mode", "none")
            if mode == "direct":
                counts["direct_match_count"] += 1
                counts["visible_source_projection_count"] += 1
            elif mode == "inherited":
                counts["inherited_match_count"] += 1
                counts["visible_source_projection_count"] += 1
            else:
                counts["skipped_source_projection_count"] += 1

        return counts

    def _schedule_source_projection_refresh(self, phase_name, trigger_path="", delay_ms=900):
        self._source_projection_refresh_context = (phase_name, self.normalize_memory_path(trigger_path))
        if self._source_projection_refresh_scheduled:
            return

        self._source_projection_refresh_scheduled = True

        def _run():
            self._source_projection_refresh_scheduled = False
            phase, queued_trigger_path = self._source_projection_refresh_context
            if self._expand_all_pending.get("source"):
                self._schedule_source_projection_refresh(phase, queued_trigger_path, delay_ms=350)
                return
            if getattr(self, "_memory_restore_in_progress", False):
                self._schedule_source_projection_refresh(phase, queued_trigger_path, delay_ms=350)
                return
            self._refresh_source_projection(phase, trigger_path=queued_trigger_path)

        QTimer.singleShot(max(0, int(delay_ms)), _run)

    def _refresh_source_projection_for_paths(self, paths, phase_name, trigger_path=""):
        if self._expand_all_pending.get("source"):
            self._source_projection_refresh_paths.update(paths or [])
            self._schedule_source_projection_refresh_for_paths(
                paths or set(),
                phase_name,
                trigger_path=trigger_path,
                delay_ms=350,
            )
            return
        tree = getattr(self, "source_tree_widget", None)
        if tree is None:
            self._log_restore_phase(
                phase_name,
                trigger_path=self.normalize_memory_path(trigger_path),
                refreshed_item_count=0,
                reason="source_tree_missing",
            )
            return

        if not self.planned_moves:
            self._log_restore_phase(
                phase_name,
                trigger_path=self.normalize_memory_path(trigger_path),
                refreshed_item_count=0,
                reason="no_planned_moves",
            )
            return

        normalized_paths = {
            self._canonical_source_projection_path(path)
            for path in (paths or [])
            if self._canonical_source_projection_path(path)
        }
        if not normalized_paths:
            self._log_restore_phase(
                phase_name,
                trigger_path=self.normalize_memory_path(trigger_path),
                refreshed_item_count=0,
                reason="no_visible_target_paths",
            )
            return

        self._rebuild_submission_visual_cache()
        refreshed_paths = set()
        refreshed_count = 0
        process_event_every = 300
        for source_path in sorted(normalized_paths, key=len):
            item = self._find_visible_source_item_by_path(source_path)
            if item is None:
                continue
            for subtree_item in self._iter_tree_items(item):
                subtree_data = subtree_item.data(0, Qt.UserRole) or {}
                if subtree_data.get("placeholder"):
                    continue
                subtree_path = self._canonical_source_projection_path(self._tree_item_path(subtree_data))
                if subtree_path and subtree_path in refreshed_paths:
                    continue
                self._apply_tree_item_visual_state(subtree_item, subtree_data)
                if subtree_path:
                    refreshed_paths.add(subtree_path)
                refreshed_count += 1
                if refreshed_count % process_event_every == 0:
                    QApplication.processEvents(QEventLoop.ProcessEventsFlag.ExcludeUserInputEvents)

        tree.viewport().update()
        self._log_restore_phase(
            phase_name,
            trigger_path=self.normalize_memory_path(trigger_path),
            refreshed_item_count=refreshed_count,
            refreshed_path_count=len(normalized_paths),
        )

    def _schedule_source_projection_refresh_for_paths(self, paths, phase_name, trigger_path="", delay_ms=250):
        for path in paths or []:
            normalized_path = self._canonical_source_projection_path(path)
            if normalized_path:
                self._source_projection_refresh_paths.add(normalized_path)
        self._source_projection_refresh_context = (phase_name, self.normalize_memory_path(trigger_path))
        if self._source_projection_refresh_scheduled:
            return

        self._source_projection_refresh_scheduled = True

        def _run():
            self._source_projection_refresh_scheduled = False
            phase, queued_trigger_path = self._source_projection_refresh_context
            queued_paths = set(self._source_projection_refresh_paths)
            self._source_projection_refresh_paths.clear()
            if self._expand_all_pending.get("source"):
                self._source_projection_refresh_paths.update(queued_paths)
                self._schedule_source_projection_refresh_for_paths(
                    queued_paths,
                    phase,
                    queued_trigger_path,
                    delay_ms=350,
                )
                return
            if getattr(self, "_memory_restore_in_progress", False):
                self._source_projection_refresh_paths.update(queued_paths)
                self._schedule_source_projection_refresh_for_paths(
                    queued_paths,
                    phase,
                    queued_trigger_path,
                    delay_ms=350,
                )
                return
            self._refresh_source_projection_for_paths(queued_paths, phase, trigger_path=queued_trigger_path)

        QTimer.singleShot(max(0, int(delay_ms)), _run)

    def _refresh_source_projection(self, phase_name, trigger_path=""):
        if not hasattr(self, "source_tree_widget") or self.source_tree_widget is None:
            self._log_restore_phase(
                phase_name,
                trigger_path=self.normalize_memory_path(trigger_path),
                skipped_source_projection_count=0,
                reason="source_tree_missing",
            )
            return

        if not self.planned_moves:
            self._log_restore_phase(
                phase_name,
                trigger_path=self.normalize_memory_path(trigger_path),
                skipped_source_projection_count=0,
                reason="no_planned_moves",
            )
            return

        if getattr(self, "_memory_restore_in_progress", False):
            self._log_restore_phase(
                f"{phase_name}_deferred",
                trigger_path=self.normalize_memory_path(trigger_path),
                planned_moves_count=len(self.planned_moves),
                reason="restore_in_progress",
            )
            return

        self._refresh_tree_visual_states("source")
        self.source_tree_widget.viewport().update()
        verbose_flag = os.environ.get("OZLINK_RESTORE_VERBOSE_LOG", "").strip().lower()
        if verbose_flag in ("1", "true", "yes"):
            counts = self._collect_source_projection_counts()
        else:
            counts = {
                "counts_skipped": True,
                "planned_moves_count": len(self.planned_moves),
            }
        self._log_restore_phase(
            phase_name,
            trigger_path=self.normalize_memory_path(trigger_path),
            **counts,
        )

    def _source_branch_depth(self, source_path):
        return len(self._path_segments(source_path))

    def _find_visible_source_item_by_path(self, source_path):
        tree = getattr(self, "source_tree_widget", None)
        if tree is None:
            return None
        normalized_target = self._canonical_source_projection_path(source_path)
        for index in range(tree.topLevelItemCount()):
            for item in self._iter_tree_items(tree.topLevelItem(index)):
                node_data = item.data(0, Qt.UserRole) or {}
                if node_data.get("placeholder"):
                    continue
                visible_path = self._canonical_source_projection_path(self._tree_item_path(node_data))
                if visible_path and visible_path == normalized_target:
                    return item
        return None

    def _find_source_item_for_planned_move(self, move):
        source_path = self._canonical_source_projection_path(move.get("source_path", ""))
        source_item = self._find_visible_source_item_by_path(source_path)
        if source_item is not None:
            return source_item

        source_node = move.get("source", {})
        expected_key = self.build_node_key(source_node, "source")
        tree = getattr(self, "source_tree_widget", None)
        if tree is None:
            return None

        for index in range(tree.topLevelItemCount()):
            for item in self._iter_tree_items(tree.topLevelItem(index)):
                node_data = item.data(0, Qt.UserRole) or {}
                if node_data.get("placeholder"):
                    continue
                if expected_key is not None and self.build_node_key(node_data, "source") == expected_key:
                    return item
                visible_path = self._canonical_source_projection_path(self._tree_item_path(node_data))
                if visible_path and source_path and visible_path == source_path:
                    return item

        # Some saved planned-move payloads store shortened source paths
        # (for example leaf-only or partial branch paths). Fall back to a
        # suffix match against visible paths so destination projected descendants
        # can still resolve deterministically.
        if source_path:
            normalized_suffix = source_path.lower()
            suffix_matches = []
            for index in range(tree.topLevelItemCount()):
                for item in self._iter_tree_items(tree.topLevelItem(index)):
                    node_data = item.data(0, Qt.UserRole) or {}
                    if node_data.get("placeholder"):
                        continue
                    visible_path = self._canonical_source_projection_path(self._tree_item_path(node_data))
                    if not visible_path:
                        continue
                    visible_lower = visible_path.lower()
                    if visible_lower == normalized_suffix or visible_lower.endswith(f"\\{normalized_suffix}"):
                        suffix_matches.append(item)
            if len(suffix_matches) == 1:
                return suffix_matches[0]
            if len(suffix_matches) > 1:
                # Prefer the shortest matching path to avoid over-scoping.
                suffix_matches.sort(
                    key=lambda item: len(
                        self._path_segments(
                            self._canonical_source_projection_path(
                                self._tree_item_path(item.data(0, Qt.UserRole) or {})
                            )
                        )
                    )
                )
                return suffix_matches[0]
        return None

    def _build_source_materialization_paths(self):
        ordered_paths = []
        seen = set()
        for move in self.planned_moves:
            source_node = move.get("source", {})
            raw_source_path = move.get("source_path") or source_node.get("item_path") or source_node.get("display_path") or ""
            canonical_path = self._canonical_source_projection_path(raw_source_path)
            segments = self._path_segments(canonical_path)
            if not segments:
                continue
            branch_segments = segments if source_node.get("is_folder", True) else segments[:-1]
            for depth in range(1, len(branch_segments) + 1):
                branch_path = "\\".join(branch_segments[:depth])
                if branch_path and branch_path not in seen:
                    seen.add(branch_path)
                    ordered_paths.append(branch_path)
        return ordered_paths

    def _schedule_source_restore_materialization_queue(self, reason, trigger_path="", delay_ms=None):
        if self._expand_all_pending.get("source") and reason == "folder_load":
            return
        if reason == "folder_load":
            # Avoid cascaded restore queue churn on every source folder callback.
            # Root-bind scheduling is sufficient to drive the queue deterministically.
            return
        if (
            reason == "folder_load"
            and not getattr(self, "_memory_restore_in_progress", False)
            and getattr(self, "_memory_restore_complete", False)
        ):
            # After restore finalization, avoid continuing restore-driven source
            # branch expansion in the background; user-driven expansion takes over.
            return
        delay = self._restore_queue_tick_delay_ms if delay_ms is None else max(0, int(delay_ms))
        QTimer.singleShot(
            delay,
            lambda: self._process_source_restore_materialization_queue(reason, trigger_path=trigger_path),
        )

    def _start_source_restore_materialization(self):
        # During post-restore live root refresh, avoid replaying the full restore queue.
        # A lightweight visible-path projection refresh keeps labels current without
        # re-expanding dozens of source branches again.
        if (
            not getattr(self, "_memory_restore_in_progress", False)
            and getattr(self, "_memory_restore_complete", False)
        ):
            tree = getattr(self, "source_tree_widget", None)
            if tree is not None and tree.topLevelItemCount() > 0:
                root_item = tree.topLevelItem(0)
                root_data = root_item.data(0, Qt.UserRole) or {} if root_item is not None else {}
                should_prime_source_root = bool(
                    root_item is not None
                    and bool(root_data.get("is_folder"))
                    and not bool(root_data.get("children_loaded"))
                    and not bool(root_data.get("load_failed"))
                    and bool(root_data.get("id"))
                )
                if should_prime_source_root:
                    self._ensure_tree_item_load_started("source", root_item)
                    self._set_tree_status_message("source", "Loading top-level source folders...", loading=True)
            visible_paths = self._collect_visible_source_relationship_paths()
            if visible_paths:
                self._schedule_source_projection_refresh_for_paths(
                    visible_paths,
                    "source_projection_post_restore_root_bind",
                    delay_ms=80,
                )
            self._log_restore_phase(
                "source_restore_materialization_skipped",
                reason="post_restore_live_root_refresh",
                visible_path_count=len(visible_paths),
                planned_moves_count=len(self.planned_moves),
            )
            return

        self._source_restore_materialization_queue = []
        self._source_restore_materialization_seen = set()
        self._source_projection_refresh_pending = False
        if not self.planned_moves:
            self._log_restore_phase(
                "source_restore_materialization_complete",
                queue_size=0,
                reason="no_planned_moves",
            )
            return

        queued_paths = self._build_source_materialization_paths()
        self._log_restore_phase(
            "source_restore_materialization_started",
            queue_size=len(queued_paths),
            planned_moves_count=len(self.planned_moves),
        )
        max_depth = 0
        for source_path in queued_paths:
            normalized_source_path = self._canonical_source_projection_path(source_path)
            self._source_restore_materialization_queue.append(normalized_source_path)
            self._source_restore_materialization_seen.add(normalized_source_path)
            max_depth = max(max_depth, self._source_branch_depth(normalized_source_path))
        self._log_restore_phase(
            "source_restore_branches_queued_summary",
            branch_count=len(queued_paths),
            max_branch_depth=max_depth,
            queue_size=len(self._source_restore_materialization_queue),
        )

        self._schedule_source_restore_materialization_queue(
            "root_bind",
            delay_ms=self._restore_queue_initial_delay_ms,
        )

    def _prime_source_root_children_after_snapshot(self, force=False):
        if self._source_tree_uses_model_view():
            return False
        tree = getattr(self, "source_tree_widget", None)
        if tree is None or tree.topLevelItemCount() == 0:
            return False
        root_item = tree.topLevelItem(0)
        if root_item is None:
            return False
        root_data = root_item.data(0, Qt.UserRole) or {}
        if (
            not bool(root_data.get("is_folder"))
            or bool(root_data.get("load_failed"))
            or not bool(root_data.get("id"))
        ):
            return False
        root_has_visible_children = any(
            not ((root_item.child(index).data(0, Qt.UserRole) or {}).get("placeholder"))
            for index in range(root_item.childCount())
        )
        if not force and bool(root_data.get("children_loaded")) and root_has_visible_children:
            return False
        root_data["children_loaded"] = False
        root_data["load_failed"] = False
        root_item.setData(0, Qt.UserRole, root_data)
        load_started = self._ensure_tree_item_load_started("source", root_item)
        if load_started:
            self._set_tree_status_message("source", "Loading top-level source folders...", loading=True)
        return load_started

    def _prime_destination_root_children_after_snapshot(self, force=False):
        tree = getattr(self, "destination_tree_widget", None)
        if tree is None or tree.topLevelItemCount() == 0:
            return False
        root_item = tree.topLevelItem(0)
        if root_item is None:
            return False
        root_data = root_item.data(0, Qt.UserRole) or {}
        if (
            not bool(root_data.get("is_folder"))
            or bool(root_data.get("load_failed"))
            or not bool(root_data.get("id"))
        ):
            return False
        if self._destination_semantic_path(root_data) != "Root":
            return False
        root_has_visible_children = any(
            not ((root_item.child(index).data(0, Qt.UserRole) or {}).get("placeholder"))
            for index in range(root_item.childCount())
        )
        if not force and bool(root_data.get("children_loaded")) and root_has_visible_children:
            return False
        root_data["children_loaded"] = False
        root_data["load_failed"] = False
        root_item.setData(0, Qt.UserRole, root_data)
        load_started = self._ensure_tree_item_load_started("destination", root_item)
        self._destination_root_prime_pending = True
        if load_started:
            self._set_tree_status_message("destination", "Loading top-level destination folders...", loading=True)
        return load_started

    def _process_source_restore_materialization_queue(self, reason, trigger_path=""):
        if self._expand_all_pending.get("source"):
            return
        if (
            not getattr(self, "_memory_restore_in_progress", False)
            and getattr(self, "_memory_restore_complete", False)
            and reason == "folder_load"
        ):
            return
        queue = self._source_restore_materialization_queue
        # Drain paths whose nodes are already expanded/loaded in one pass. Previously each
        # already-loaded pop consumed a full timer tick (~60ms), so 50+ branches added seconds
        # of idle delay before the next real expand.
        max_drain_per_tick = 500
        drained = 0
        while queue and drained < max_drain_per_tick:
            source_path = queue[0]
            item = self._find_visible_source_item_by_path(source_path)
            if item is None:
                self._log_restore_phase(
                    "source_restore_branch_skipped",
                    source_path=source_path,
                    normalized_source_path=source_path,
                    queue_size=len(queue),
                    branch_depth=self._source_branch_depth(source_path),
                    already_loaded=False,
                    loaded_successfully=False,
                    projection_refresh_invoked=False,
                    reason=f"{reason}_not_visible_yet",
                    trigger_path=self.normalize_memory_path(trigger_path),
                    verbose=True,
                )
                break

            node_data = item.data(0, Qt.UserRole) or {}
            already_loaded = bool(node_data.get("children_loaded")) or not bool(node_data.get("is_folder"))
            pending_key = f"{node_data.get('drive_id', '')}:{node_data.get('id', '')}"
            if pending_key in self.pending_folder_loads["source"]:
                self._log_restore_phase(
                    "source_restore_branch_skipped",
                    source_path=source_path,
                    normalized_source_path=source_path,
                    queue_size=len(queue),
                    branch_depth=self._source_branch_depth(source_path),
                    already_loaded=False,
                    loaded_successfully=False,
                    projection_refresh_invoked=False,
                    reason=f"{reason}_already_pending",
                    trigger_path=self.normalize_memory_path(trigger_path),
                    verbose=True,
                )
                break

            queue.pop(0)
            drained += 1
            if already_loaded:
                self._log_restore_phase(
                    "source_restore_branch_loaded",
                    source_path=source_path,
                    normalized_source_path=source_path,
                    queue_size=len(queue),
                    branch_depth=self._source_branch_depth(source_path),
                    already_loaded=True,
                    loaded_successfully=True,
                    projection_refresh_invoked=False,
                    trigger_path=self.normalize_memory_path(trigger_path),
                    verbose=True,
                )
                if drained % 40 == 0:
                    QApplication.processEvents()
                continue

            self._log_restore_phase(
                "source_restore_branch_expand_requested",
                source_path=source_path,
                normalized_source_path=source_path,
                queue_size=len(queue),
                branch_depth=self._source_branch_depth(source_path),
                already_loaded=False,
                loaded_successfully=False,
                projection_refresh_invoked=False,
                trigger_path=self.normalize_memory_path(trigger_path),
                verbose=True,
            )
            self.source_tree_widget.expandItem(item)
            break

        if not queue:
            if self._source_projection_refresh_pending:
                source_projection_paths = set(self._build_source_materialization_paths())
                source_projection_paths.update(self._collect_visible_source_relationship_paths())
                self._schedule_source_projection_refresh_for_paths(
                    source_projection_paths,
                    "source_projection_restore_complete",
                    trigger_path=trigger_path,
                )
                self._source_projection_refresh_pending = False
            self._log_restore_phase(
                "source_restore_materialization_complete",
                queue_size=0,
                trigger_path=self.normalize_memory_path(trigger_path),
                reason=reason,
            )
            self._try_flush_destination_future_model_after_source_restore("source_queue_empty")
        else:
            self._schedule_source_restore_materialization_queue(reason, trigger_path=trigger_path)

    def _evaluate_source_relationship(self, node_data):
        if not node_data or node_data.get("placeholder"):
            return {"mode": "none", "suffix": "", "mismatch_reason": "placeholder_or_empty"}

        source_path = self._tree_item_path(node_data)
        normalized_source_path = self._canonical_source_projection_path(source_path)
        direct_move = None
        inherited_move = None
        inherited_path_length = -1
        mismatch_reason = "no_matching_planned_move"

        for move in self.planned_moves:
            move_source = move.get("source", {})
            move_source_path = self._tree_item_path(move_source)
            normalized_move_source_path = self._canonical_source_projection_path(move_source_path)
            move_drive_id = move_source.get("drive_id", "")
            node_drive_id = node_data.get("drive_id", "")

            if self.node_keys_match(move_source, node_data, "source"):
                direct_move = move
                mismatch_reason = ""
                break

            same_source_tree = (
                move_source.get("tree_role", "source") == node_data.get("tree_role", "source") and
                (not move_drive_id or not node_drive_id or move_drive_id == node_drive_id)
            )
            if not same_source_tree and move_drive_id and node_drive_id:
                mismatch_reason = "drive_id_mismatch"
                continue

            if self.source_item_path_is_descendant_of(source_path, move_source_path):
                if len(normalized_move_source_path) > inherited_path_length:
                    inherited_move = move
                    inherited_path_length = len(normalized_move_source_path)
                    mismatch_reason = ""

        if direct_move is not None:
            destination_leaf = direct_move.get("destination_name", "Mapped Item")
            move_source = direct_move.get("source", {})
            move_source_path = self._tree_item_path(move_source)
            return {
                "mode": "direct",
                "suffix": f"→ {destination_leaf}",
                "raw_planned_source_path": move_source_path,
                "normalized_planned_source_path": self._canonical_source_projection_path(move_source_path),
                "mismatch_reason": "",
            }

        if inherited_move is not None:
            parent_name = inherited_move.get("source_name", "Mapped Parent")
            move_source = inherited_move.get("source", {})
            move_source_path = self._tree_item_path(move_source)
            return {
                "mode": "inherited",
                "suffix": f"↳ via {parent_name}",
                "raw_planned_source_path": move_source_path,
                "normalized_planned_source_path": self._canonical_source_projection_path(move_source_path),
                "mismatch_reason": "",
            }

        return {
            "mode": "none",
            "suffix": "",
            "raw_planned_source_path": "",
            "normalized_planned_source_path": "",
            "mismatch_reason": mismatch_reason,
        }

    def _count_visible_destination_proposed_nodes(self):
        tree = getattr(self, "destination_tree_widget", None)
        if tree is None:
            return 0
        count = 0
        for index in range(tree.topLevelItemCount()):
            for item in self._iter_tree_items(tree.topLevelItem(index)):
                node_data = item.data(0, Qt.UserRole) or {}
                if self.is_proposed_destination_node(node_data):
                    count += 1
        return count

    def _build_root_node_for_payload(self, item, index):
        self._log_root_success_step("step_07_build_node_enter", item_index=index, item_name=item.get("name", ""), is_folder=bool(item.get("is_folder", False)))
        node = self.build_tree_item(item)
        self._log_root_success_step("step_07_build_node_exit", item_index=index, item_name=item.get("name", ""))
        return node

    def _add_root_node_to_tree(self, tree, node, index, item_name):
        self._log_root_success_step("step_08_add_node_enter", item_index=index, item_name=item_name)
        tree.addTopLevelItem(node)
        self._log_root_success_step("step_08_add_node_exit", item_index=index, item_name=item_name, top_level_count=tree.topLevelItemCount())

    def _source_tree_uses_model_view(self):
        return bool(getattr(self, "_source_tree_model_view", False))

    def _source_payload_from_graph_item(self, item):
        prefix = "Folder" if item.get("is_folder") else "File"
        base_label = f"{prefix}: {item.get('name', 'Unnamed Item')}"
        node_data = dict(item)
        node_data.setdefault("children_loaded", False)
        node_data.setdefault("load_failed", False)
        node_data.setdefault("tree_label", prefix)
        node_data.setdefault("base_display_label", base_label)
        node_data.setdefault("tree_role", "source")
        return node_data

    def _apply_root_payload_to_source_model_view(self, panel_key, items):
        self._log_root_success_step("step_01_resolve_tree_enter", panel_key=panel_key)
        tree, status = self._get_tree_and_status(panel_key)
        model = getattr(self, "source_sharepoint_model", None)
        self._log_root_success_step("step_01_resolve_tree_exit", panel_key=panel_key, tree_info=self._root_tree_identity(tree))
        if tree is None or status is None or model is None:
            self._log_root_success_step("step_02_validate_tree_exit", panel_key=panel_key, valid=False)
            return
        self._root_tree_bind_in_progress = True
        tree.blockSignals(True)
        tree.setUpdatesEnabled(False)
        try:
            tree.setEnabled(True)
            model.clear()
            if not items:
                self._set_tree_status_message(panel_key, "This library is empty.", loading=False)
                model.set_empty_library_message("This library is empty.")
                tree.setEnabled(False)
                return
            sorted_items = sorted(items, key=lambda value: (not value.get("is_folder", False), value.get("name", "").lower()))
            payloads = []
            for it in sorted_items:
                pl = self._source_payload_from_graph_item(it)
                self._apply_tree_item_visual_state(None, pl)
                payloads.append(pl)
            model.reset_root_payloads(payloads)
            self._set_tree_status_message(panel_key, f"{len(items)} root item(s) loaded.", loading=False)
        finally:
            tree.setUpdatesEnabled(True)
            tree.blockSignals(False)
            self._root_tree_bind_in_progress = False

    def _capture_child_path_set_for_model_index(self, parent: QModelIndex):
        paths = set()
        model = getattr(self, "source_sharepoint_model", None)
        if model is None or not parent.isValid():
            return paths
        rows = model.rowCount(parent)
        for r in range(rows):
            ix = model.index(r, 0, parent)
            pl = ix.data(Qt.UserRole) or {}
            if pl.get("placeholder"):
                continue
            p = self._tree_item_path(pl)
            if p:
                paths.add(str(p))
        return paths

    def _on_source_sharepoint_model_collapsed(self, index):
        if not self._full_trace_enabled():
            return
        pl = index.data(Qt.UserRole) or {}
        if pl.get("placeholder"):
            self._ui_trace(
                "tree",
                "collapse_skipped_placeholder",
                panel_key="source",
                item=None,
            )
        else:
            self._ui_trace("tree", "collapsed", panel_key="source", item=None)

    def _source_model_request_folder_children_load(self, index, *, triggered_by_user_expand: bool) -> str:
        """Start a Graph folder load for a source model row if needed.

        Returns route name for tracing: ``noop``, ``throttled``, or ``started``.
        """
        panel_key = "source"
        node_data = index.data(Qt.UserRole) or {}
        base_label = str(node_data.get("base_display_label", "") or "").strip().lower()
        tree_label = str(node_data.get("tree_label", "") or "").strip().lower()
        text_label = str(node_data.get("base_display_label", "") or "").strip().lower()
        is_folder_like = bool(
            node_data.get("is_folder")
            or tree_label == "folder"
            or base_label.startswith("folder:")
            or text_label.startswith("folder:")
        )
        if node_data.get("placeholder") or not is_folder_like:
            return "noop"
        if node_data.get("children_loaded") or node_data.get("load_failed"):
            return "noop"

        drive_id = self._resolve_tree_item_drive_id(panel_key, node_data)
        item_id = node_data.get("id", "")
        if not drive_id or not item_id:
            return "noop"

        pending_key = f"{drive_id}:{item_id}"
        if pending_key in self.pending_folder_loads[panel_key]:
            return "noop"

        pending_count = len(self.pending_folder_loads.get(panel_key, set()))
        max_inflight_loads = 1 if triggered_by_user_expand else 3
        if pending_count >= max_inflight_loads:
            return "throttled"

        self.pending_folder_loads[panel_key].add(pending_key)
        worker_key = f"{panel_key}:{item_id}"
        item_path = self._tree_item_path(node_data)
        if item_path and item_path in self._pending_snapshot_branch_refresh.get(panel_key, set()):
            self._snapshot_branch_refresh_baseline_by_worker[worker_key] = self._capture_child_path_set_for_model_index(
                index
            )
        if self._full_trace_enabled():
            self._ui_trace(
                "tree",
                "folder_load_take_children",
                panel_key=panel_key,
                item=None,
                worker_key=worker_key,
                prior_child_count_hint="cleared",
            )
        self.source_sharepoint_model.set_loading_children(index)

        use_cache_only = bool(
            (
                getattr(self, "_memory_restore_in_progress", False)
                or getattr(self, "_suppress_autosave", False)
            )
            and self.graph.has_cached_drive_item_children(drive_id, item_id)
        )
        worker_context = {
            "site_id": node_data.get("site_id", ""),
            "site_name": node_data.get("site_name", ""),
            "library_id": node_data.get("library_id", drive_id),
            "library_name": node_data.get("library_name", ""),
            "tree_role": panel_key,
            "parent_item_path": node_data.get("item_path", ""),
            "cache_only": use_cache_only,
        }
        worker = FolderLoadWorker(self.graph, panel_key, drive_id, item_id, worker_context)
        pmi = QPersistentModelIndex(index)
        worker_entry = self._register_folder_worker(
            worker_key, worker, None, source_folder_parent_persistent=pmi
        )
        worker.success.connect(
            lambda payload, worker_id=worker_entry["id"]: self._safe_invoke(
                "folder_worker.success", self.on_folder_load_success, payload, worker_id
            )
        )
        worker.error.connect(
            lambda payload, worker_id=worker_entry["id"]: self._safe_invoke(
                "folder_worker.error", self.on_folder_load_error, payload, worker_id
            )
        )
        worker.finished.connect(
            lambda key=worker_key, worker_id=worker_entry["id"]: self._safe_invoke(
                "folder_worker.finished", self.on_folder_worker_finished, key, worker_id
            )
        )
        if self._full_trace_enabled():
            self._ui_trace(
                "tree",
                "expand_route",
                panel_key=panel_key,
                item=None,
                route="start_graph_folder_load_worker",
                worker_key=worker_key,
                cache_only=use_cache_only,
            )
        worker.start()
        return "started"

    def _on_source_sharepoint_model_expanded(self, index):
        panel_key = "source"
        if self._full_trace_enabled():
            self._ui_trace("tree", "expand_signal", panel_key=panel_key, item=None)
        route = self._source_model_request_folder_children_load(index, triggered_by_user_expand=True)
        if self._full_trace_enabled():
            self._ui_trace("tree", "expand_route", panel_key=panel_key, item=None, route=route)

    def _apply_source_folder_load_model_tail(self, parent_index, items, previous_child_paths, panel_key):
        model = self.source_sharepoint_model
        child_payloads = []
        for child in sorted(items, key=lambda value: (not value.get("is_folder", False), value.get("name", "").lower())):
            pl = self._source_payload_from_graph_item(child)
            self._apply_tree_item_visual_state(None, pl)
            child_payloads.append(pl)
        model.replace_all_children(parent_index, child_payloads)

        def _mut_parent(p):
            p["children_loaded"] = True
            p["load_failed"] = False

        model.update_payload_for_index(parent_index, _mut_parent)
        node_data = parent_index.data(Qt.UserRole) or {}
        trigger_path = node_data.get("item_path") or node_data.get("display_path") or ""
        current_child_paths = self._capture_child_path_set_for_model_index(parent_index)
        if previous_child_paths:
            added_count = len(current_child_paths - previous_child_paths)
            removed_count = len(previous_child_paths - current_child_paths)
            if added_count or removed_count:
                self._set_tree_status_message(
                    panel_key,
                    f"Refreshing saved branches... {added_count} added, {removed_count} removed in {node_data.get('name', 'folder')}.",
                    loading=bool(self._pending_snapshot_branch_refresh.get(panel_key)),
                )

        projection_refresh_invoked = False
        if self._expand_all_pending.get("source"):
            self._expand_all_deferred_refresh["source"] = True
        elif self._memory_restore_in_progress or bool(self._source_restore_materialization_queue):
            self._source_projection_refresh_pending = True
        else:
            self._schedule_source_projection_refresh_for_paths(
                [trigger_path],
                "source_projection_folder_load_applied",
                trigger_path=trigger_path,
            )
            projection_refresh_invoked = True
        destination_future_model_applied_count = 0
        if (
            not self._expand_all_pending.get("source")
            and getattr(self, "destination_tree_widget", None) is not None
            and self.planned_moves
            and not getattr(self, "_sharepoint_lazy_mode", False)
        ):
            destination_future_model_applied_count = self._materialize_destination_future_model(
                "source_folder_load_success"
            )
        should_log_source_branch_loaded = not self._expand_all_pending.get("source")
        if self._expand_all_pending.get("source"):
            self._source_expand_all_folder_load_log_counter = (
                int(getattr(self, "_source_expand_all_folder_load_log_counter", 0) or 0) + 1
            )
            should_log_source_branch_loaded = self._source_expand_all_folder_load_log_counter % 24 == 0
        if should_log_source_branch_loaded:
            self._log_restore_phase(
                "source_restore_branch_loaded",
                source_path=trigger_path,
                normalized_source_path=self._canonical_source_projection_path(trigger_path),
                queue_size=len(self._source_restore_materialization_queue),
                branch_depth=self._source_branch_depth(trigger_path),
                already_loaded=False,
                loaded_successfully=True,
                projection_refresh_invoked=projection_refresh_invoked,
                destination_future_model_applied_count=destination_future_model_applied_count,
                trigger_path=self.normalize_memory_path(trigger_path),
                verbose=True,
            )
        self._schedule_source_restore_materialization_queue("folder_load", trigger_path=trigger_path)
        self._process_pending_source_navigation("folder_load", trigger_path=trigger_path)
        if not self._expand_all_pending.get("source"):
            self._continue_source_background_preload()
        self._continue_expand_all("source", None)
        if self._expand_all_pending.get("source"):
            self._source_column_refresh_pending = True
        else:
            self._refresh_tree_column_width("source")
        if self._source_branch_depth(trigger_path) <= 1:
            self.source_tree_widget.expand(parent_index)
            rc = model.rowCount(parent_index)
            self._set_tree_status_message(
                "source",
                f"{rc} top-level source folder(s) loaded.",
                loading=False,
            )
        self._try_flush_destination_future_model_after_source_restore("source_folder_load")

    def _apply_root_payload_to_tree(self, panel_key, items):
        if panel_key == "source" and self._source_tree_uses_model_view():
            self._apply_root_payload_to_source_model_view(panel_key, items)
            return
        self._log_root_success_step("step_01_resolve_tree_enter", panel_key=panel_key)
        tree, status = self._get_tree_and_status(panel_key)
        self._log_root_success_step("step_01_resolve_tree_exit", panel_key=panel_key, tree_info=self._root_tree_identity(tree))

        self._log_root_success_step("step_02_validate_tree_enter", panel_key=panel_key)
        if tree is None or status is None:
            self._log_root_success_step("step_02_validate_tree_exit", panel_key=panel_key, valid=False)
            return
        self._log_root_success_step("step_02_validate_tree_exit", panel_key=panel_key, valid=True)

        self._log_root_success_step("step_03_disable_updates_enter", panel_key=panel_key, top_level_count=tree.topLevelItemCount())
        self._root_tree_bind_in_progress = True
        tree.blockSignals(True)
        tree.setUpdatesEnabled(False)
        self._log_root_success_step("step_03_disable_updates_exit", panel_key=panel_key)
        try:
            self._log_root_success_step("step_04_clear_tree_enter", panel_key=panel_key, top_level_count=tree.topLevelItemCount())
            tree.clear()
            tree.setEnabled(True)
            self._log_restore_phase("root_load placeholder_cleared", panel_key=panel_key)
            self._log_root_success_step("step_04_clear_tree_exit", panel_key=panel_key, top_level_count=tree.topLevelItemCount())

            if not items:
                self._log_root_success_step("step_05_empty_library_enter", panel_key=panel_key)
                self._set_tree_status_message(panel_key, "This library is empty.", loading=False)
                tree.addTopLevelItem(QTreeWidgetItem(["This library is empty."]))
                tree.setEnabled(False)
                self._log_root_success_step("step_05_empty_library_exit", panel_key=panel_key, top_level_count=tree.topLevelItemCount())
                return

            sorted_items = sorted(items, key=lambda value: (not value.get("is_folder", False), value.get("name", "").lower()))
            self._log_root_success_step("step_06_sorted_items_ready", panel_key=panel_key, item_count=len(sorted_items))
            for index, item in enumerate(sorted_items):
                node = self._build_root_node_for_payload(item, index)
                self._add_root_node_to_tree(tree, node, index, item.get("name", ""))

            self._log_root_success_step("step_09_status_text_enter", panel_key=panel_key)
            self._set_tree_status_message(panel_key, f"{len(items)} root item(s) loaded.", loading=False)
            self._log_root_success_step("step_09_status_text_exit", panel_key=panel_key)
        finally:
            self._log_root_success_step("step_10_restore_updates_enter", panel_key=panel_key)
            tree.setUpdatesEnabled(True)
            tree.blockSignals(False)
            self._root_tree_bind_in_progress = False
            self._log_root_success_step("step_10_restore_updates_exit", panel_key=panel_key, top_level_count=tree.topLevelItemCount())

    def _apply_proposed_overlay_after_root_bind(self, panel_key):
        tree, _status = self._get_tree_and_status(panel_key)
        if panel_key != "destination":
            return 0

        if getattr(self, "_sharepoint_lazy_mode", False):
            self._log_restore_phase(
                "root_bind destination_overlay_deferred",
                panel_key=panel_key,
                reason="lazy_destination_root_bind_uses_restore_queue",
                top_level_count=tree.topLevelItemCount() if tree is not None else 0,
            )
            self._restore_destination_overlay_pending = bool(self.proposed_folders or self.planned_moves)
            return 0

        if getattr(self, "_sharepoint_lazy_mode", False) and tree is not None and tree.topLevelItemCount() <= 1:
            self._log_restore_phase(
                "root_bind destination_overlay_deferred",
                panel_key=panel_key,
                reason="top_level_destination_tree_not_ready",
                top_level_count=tree.topLevelItemCount(),
            )
            self._restore_destination_overlay_pending = bool(self.proposed_folders or self.planned_moves)
            return 0

        root_item = tree.topLevelItem(0) if tree is not None else None
        root_data = root_item.data(0, Qt.UserRole) or {} if root_item is not None else {}
        root_semantic_path = self._destination_semantic_path(root_data) if root_data else ""
        root_children_loaded = bool(root_data.get("children_loaded")) if root_data else False
        root_load_failed = bool(root_data.get("load_failed")) if root_data else False

        if (
            getattr(self, "_sharepoint_lazy_mode", False)
            and root_item is not None
            and root_semantic_path == "Root"
            and not root_children_loaded
            and not root_load_failed
        ):
            self._log_restore_phase(
                "root_bind destination_overlay_deferred",
                panel_key=panel_key,
                reason="waiting_for_root_children",
                top_level_count=tree.topLevelItemCount(),
            )
            self._restore_destination_overlay_pending = bool(self.proposed_folders or self.planned_moves)
            return 0

        if self._memory_ui_rebind_in_progress or self._memory_restore_in_progress:
            self._log_restore_phase(
                "root_bind destination_overlay_deferred",
                panel_key=panel_key,
                reason="restore_or_ui_rebind_in_progress",
            )
            self._restore_destination_overlay_pending = bool(self.proposed_folders or self.planned_moves)
            return 0

        if not self.proposed_folders and not self.planned_moves:
            self._log_restore_phase("root_bind destination_overlay_skipped", panel_key=panel_key, reason="no_destination_overlays")
            return 0

        try:
            applied_count = 0
            self._log_restore_phase(
                "root_bind destination_overlay_start",
                panel_key=panel_key,
                top_level_count=tree.topLevelItemCount(),
                proposed_count=len(self.proposed_folders),
                allocation_count=len(self.planned_moves),
            )
            for index in range(tree.topLevelItemCount()):
                applied_count += self._apply_proposed_children_to_item(tree.topLevelItem(index))
                applied_count += self._apply_allocation_children_to_item(tree.topLevelItem(index))
            applied_count += self._replay_unresolved_proposed_overlay("root_bind_destination_overlay")
            applied_count += self._replay_unresolved_allocation_overlay("root_bind_destination_allocation_overlay")
            applied_count += self._reconcile_destination_semantic_duplicates("root_bind_destination_overlay")
            self._refresh_destination_tree_indicators()
            self._restore_destination_overlay_pending = False
            self._log_restore_phase(
                "root_bind destination_overlay_end",
                panel_key=panel_key,
                applied_count=applied_count,
                visible_proposed_count=self._count_visible_destination_proposed_nodes(),
            )
            return applied_count
        except Exception as exc:
            self._log_restore_exception("root_bind destination_overlay", exc)
            self._log_restore_phase(
                "root_bind destination_overlay_safe_skip",
                panel_key=panel_key,
                reason="overlay_exception",
                proposed_count=len(self.proposed_folders),
                allocation_count=len(self.planned_moves),
            )
            self._restore_destination_overlay_pending = bool(self.proposed_folders or self.planned_moves)
            return 0

    def _refresh_tree_ui_after_root_bind(self, panel_key, restored_runtime_snapshot=False):
        self._log_root_success_step("step_11_ui_refresh_enter", panel_key=panel_key)
        if panel_key == "source":
            if restored_runtime_snapshot:
                self._refresh_expand_all_button_for_panel("source")
                self._set_tree_status_message(
                    "source",
                    "Expanded from local snapshot. Refreshing live content...",
                    loading=bool(self._pending_snapshot_branch_refresh.get("source")),
                )
                load_started = self._prime_source_root_children_after_snapshot(force=True)
                if not load_started:
                    self._schedule_safe_timer(220, "source_root_prime_retry", self._prime_source_root_children_after_snapshot)
            elif not getattr(self, "_memory_restore_in_progress", False):
                self._refresh_source_projection("source_projection_root_bind_applied")
            if not restored_runtime_snapshot:
                self._start_source_restore_materialization()
        else:
            self._refresh_destination_real_tree_snapshot()
            destination_top_level_count = self.destination_tree_widget.topLevelItemCount() if getattr(self, "destination_tree_widget", None) is not None else 0
            root_item = self.destination_tree_widget.topLevelItem(0) if getattr(self, "destination_tree_widget", None) is not None else None
            root_data = root_item.data(0, Qt.UserRole) or {} if root_item is not None else {}
            root_has_visible_children = bool(
                root_item is not None
                and any(
                    not ((root_item.child(index).data(0, Qt.UserRole) or {}).get("placeholder"))
                    for index in range(root_item.childCount())
                )
            )
            if getattr(self, "_sharepoint_lazy_mode", False) and destination_top_level_count <= 1:
                self._restore_destination_overlay_pending = bool(self.proposed_folders or self.planned_moves)
                load_started = self._prime_destination_root_children_after_snapshot(force=True)
                if not load_started:
                    self._schedule_safe_timer(220, "destination_root_prime_retry", self._prime_destination_root_children_after_snapshot)
                self._log_restore_phase(
                    "root_bind destination_top_level_deferred",
                    panel_key=panel_key,
                    top_level_count=destination_top_level_count,
                    trigger_path=self.normalize_memory_path(root_data.get("item_path", "")),
                    verbose=True,
                )
                self._schedule_progress_summary_refresh()
                self._log_root_success_step("step_11_ui_refresh_exit", panel_key=panel_key)
                return
            should_prime_destination_root = (
                getattr(self, "_sharepoint_lazy_mode", False)
                and root_item is not None
                and self._destination_semantic_path(root_data) == "Root"
                and bool(root_data.get("is_folder"))
                and (not bool(root_data.get("children_loaded")) or not root_has_visible_children)
                and not bool(root_data.get("load_failed"))
                and bool(root_data.get("id"))
            )
            if should_prime_destination_root:
                load_started = self._prime_destination_root_children_after_snapshot(force=True)
                if not load_started:
                    self._schedule_safe_timer(220, "destination_root_prime_retry", self._prime_destination_root_children_after_snapshot)
                self._log_restore_phase(
                    "root_bind destination_root_prime_started" if load_started else "root_bind destination_root_prime_waiting",
                    panel_key=panel_key,
                    trigger_path=self.normalize_memory_path(root_data.get("item_path", "")),
                )
                self._schedule_progress_summary_refresh()
                self._log_root_success_step("step_11_ui_refresh_exit", panel_key=panel_key)
                return
            if getattr(self, "_sharepoint_lazy_mode", False):
                applied_count = 0
                self._restore_destination_overlay_pending = bool(self.proposed_folders or self.planned_moves)
                self._start_destination_restore_materialization()
                self.destination_tree_widget.viewport().update()
                self._log_restore_phase(
                    "root_bind destination_projection_applied",
                    panel_key=panel_key,
                    applied_count=applied_count,
                    future_model_applied_count=0,
                    visible_proposed_count=self._count_visible_destination_proposed_nodes(),
                    deferred=self._restore_destination_overlay_pending,
                )
                self._schedule_progress_summary_refresh()
                self._log_root_success_step("step_11_ui_refresh_exit", panel_key=panel_key)
                return
            applied_count = self._apply_proposed_overlay_after_root_bind(panel_key)
            self._start_destination_restore_materialization()
            if getattr(self, "_sharepoint_lazy_mode", False):
                future_model_applied_count = 0
            else:
                future_model_applied_count = self._materialize_destination_future_model("root_bind")
                applied_count += future_model_applied_count
            self.destination_tree_widget.viewport().update()
            self._log_restore_phase(
                "root_bind destination_projection_applied",
                panel_key=panel_key,
                applied_count=applied_count,
                future_model_applied_count=future_model_applied_count,
                visible_proposed_count=self._count_visible_destination_proposed_nodes(),
                deferred=self._restore_destination_overlay_pending,
            )
        self._schedule_progress_summary_refresh()
        self._log_root_success_step("step_11_ui_refresh_exit", panel_key=panel_key)

    def populate_root_tree(self, panel_key, items):
        restore_destination_state = not bool(self._expand_all_pending.get("destination"))
        if restore_destination_state:
            destination_expanded_paths = self._collect_expanded_tree_paths("destination")
            destination_selected_path = self._collect_selected_tree_path("destination")
        else:
            destination_expanded_paths = set()
            destination_selected_path = ""
        self._apply_root_payload_to_tree(panel_key, items)
        self._refresh_tree_ui_after_root_bind(panel_key)
        self._restore_workspace_tree_state(ui_state)

    def build_tree_item(self, item):
        prefix = "Folder" if item.get("is_folder") else "File"
        base_label = f"{prefix}: {item.get('name', 'Unnamed Item')}"
        node = QTreeWidgetItem([base_label])

        node_data = dict(item)
        node_data.setdefault("children_loaded", False)
        node_data.setdefault("load_failed", False)
        node_data.setdefault("tree_label", prefix)
        node_data.setdefault("base_display_label", base_label)
        node.setData(0, Qt.UserRole, node_data)
        self._apply_tree_item_visual_state(node, node_data)

        if item.get("is_folder"):
            node.setChildIndicatorPolicy(QTreeWidgetItem.ShowIndicator)
            node.addChild(
                self.build_loading_placeholder_item("Expand to load contents", role="lazy_unloaded")
            )
        else:
            node.setChildIndicatorPolicy(QTreeWidgetItem.DontShowIndicator)

        return node

    def build_loading_placeholder_item(self, text, *, role="lazy_unloaded"):
        """role: lazy_unloaded | loading_in_progress | terminal_empty | error | projection_pending"""
        placeholder = QTreeWidgetItem([text])
        placeholder.setData(0, Qt.UserRole, {"placeholder": True, "placeholder_role": role})
        if self._full_trace_enabled():
            log_trace(
                "placeholder",
                "build_loading_placeholder_item",
                role=role,
                text_excerpt=(text or "")[:160],
            )
        return placeholder

    def _has_needs_review_for_source_path(self, source_path):
        canonical_path = self._canonical_source_projection_path(source_path)
        if not canonical_path:
            return False
        for row in getattr(self, "_workflow_needs_review_rows", []):
            row_source_path = self._canonical_source_projection_path(row.get("source_path", ""))
            if row_source_path and row_source_path == canonical_path:
                return True
        return False

    def _apply_tree_item_visual_state(self, item, node_data):
        if not node_data or node_data.get("placeholder"):
            return

        if item is not None:
            item.setForeground(0, QBrush())
            item.setBackground(0, QBrush())
            item.setToolTip(0, "")
        elif node_data.get("tree_role") == "source":
            node_data.pop("_model_foreground", None)
            node_data.pop("_model_background", None)
            node_data.pop("_model_tooltip", None)

        color = None
        background_color = None
        role = node_data.get("tree_role", "")
        if role == "source":
            relationship = self._evaluate_source_relationship(node_data)
            node_data["source_relationship_mode"] = relationship.get("mode", "none")
            node_data["source_relationship_suffix"] = relationship.get("suffix", "")
            source_path = self._tree_item_path(node_data)
            if self._has_needs_review_for_source_path(source_path):
                color = QColor("#F5A623")
            elif relationship.get("mode") == "direct":
                color = QColor("#59D88F")
            elif relationship.get("mode") == "inherited":
                color = QColor("#8FC9FF")
        elif role == "destination":
            origin = str(node_data.get("node_origin", "")).lower()
            branch_color, branch_depth = self._destination_branch_visual_color(node_data)
            background_color = self._destination_branch_background_color(branch_color, branch_depth)
            if self.node_is_proposed(node_data):
                color = QColor("#FFC14D")
            elif self.node_is_planned_allocation(node_data):
                color = QColor("#51E3F6")
            elif origin == "projectedallocationdescendant":
                color = QColor("#89C9D8")
            elif branch_color is not None:
                color = branch_color

        submitted_state = self._submitted_visual_state_for_node(node_data)
        node_data["submitted_visual"] = bool(submitted_state["submitted"])
        node_data["submitted_batch_visual"] = submitted_state["batch_id"]
        if item is not None:
            item.setData(0, Qt.UserRole, node_data)
            if role == "destination":
                base_label = str(node_data.get("base_display_label", "")).strip() or item.text(0).replace(
                    " [Submitted]", ""
                )
                item.setText(0, f"{base_label} [Submitted]" if submitted_state["submitted"] else base_label)

            if color is not None:
                item.setForeground(0, QBrush(color))
            if background_color is not None and not submitted_state["submitted"]:
                item.setBackground(0, QBrush(background_color))
            if submitted_state["submitted"]:
                item.setBackground(0, QBrush(QColor("#1B2942")))
                item.setToolTip(
                    0,
                    (
                        f"Submitted and locked in batch {submitted_state['batch_id']}."
                        if submitted_state["batch_id"]
                        else "Submitted and locked."
                    ),
                )
        elif role == "source":
            if color is not None:
                node_data["_model_foreground"] = color
            if submitted_state["submitted"]:
                node_data["_model_background"] = QColor("#1B2942")
                node_data["_model_tooltip"] = (
                    f"Submitted and locked in batch {submitted_state['batch_id']}."
                    if submitted_state["batch_id"]
                    else "Submitted and locked."
                )
            elif background_color is not None:
                node_data["_model_background"] = background_color
        if self._full_trace_enabled():
            pk = str(role or "unknown")
            if pk in ("source", "destination"):
                self._ui_trace(
                    "visual",
                    "apply_tree_item_visual_state",
                    panel_key=pk,
                    item=item,
                    relationship_mode=str(node_data.get("source_relationship_mode", ""))[:40] if role == "source" else "",
                    node_origin_excerpt=str(node_data.get("node_origin", ""))[:80] if role == "destination" else "",
                    foreground_tint=bool(color is not None),
                    background_tint=bool(background_color is not None and not submitted_state["submitted"]),
                    submitted_locked=submitted_state["submitted"],
                )

    def _destination_branch_color_map(self):
        return {
            "finance": "#7FD36B",
            "hr": "#F3B45A",
            "it": "#5DD0FF",
            "management": "#E78AB2",
            "marketing": "#C38BFF",
            "operations": "#3FD0B0",
            "projects": "#E9C15F",
            "sales": "#FF8A66",
        }

    def _blend_colors(self, start_color, end_color, ratio):
        ratio = max(0.0, min(1.0, float(ratio)))
        inverse_ratio = 1.0 - ratio
        return QColor(
            int((start_color.red() * inverse_ratio) + (end_color.red() * ratio)),
            int((start_color.green() * inverse_ratio) + (end_color.green() * ratio)),
            int((start_color.blue() * inverse_ratio) + (end_color.blue() * ratio)),
        )

    def _destination_branch_visual_context(self, node_data):
        destination_path = self._canonical_destination_projection_path(self._tree_item_path(node_data))
        segments = self._path_segments(destination_path)
        if len(segments) < 2 or str(segments[0]).lower() != "root":
            return "", 0
        return segments[1], len(segments) - 1

    def _destination_branch_visual_color(self, node_data):
        branch_name, branch_depth = self._destination_branch_visual_context(node_data)
        branch_hex = self._destination_branch_color_map().get(str(branch_name).lower())
        if not branch_hex:
            return None, branch_depth

        branch_color = QColor(branch_hex)
        neutral_color = QColor("#D6E8FF")
        if branch_depth <= 1:
            return branch_color, branch_depth
        if branch_depth == 2:
            return self._blend_colors(branch_color, neutral_color, 0.18), branch_depth
        if branch_depth == 3:
            return self._blend_colors(branch_color, neutral_color, 0.34), branch_depth
        return self._blend_colors(branch_color, neutral_color, 0.52), branch_depth

    def _destination_branch_background_color(self, branch_color, branch_depth):
        if branch_color is None or branch_depth <= 0:
            return None

        base_background = QColor("#09101A")
        if branch_depth == 1:
            return self._blend_colors(base_background, branch_color, 0.20)
        if branch_depth == 2:
            return self._blend_colors(base_background, branch_color, 0.12)
        if branch_depth == 3:
            return self._blend_colors(base_background, branch_color, 0.07)
        return self._blend_colors(base_background, branch_color, 0.04)

    def normalize_memory_path(self, path):
        text = str(path or "").strip().replace("/", "\\")
        text = re.sub(r"\s*\\\s*", "\\\\", text)
        text = re.sub(r"\\{2,}", "\\\\", text)
        segments = [segment.strip() for segment in text.split("\\") if segment.strip()]
        return "\\".join(segments)

    def _path_segments(self, path):
        normalized = self.normalize_memory_path(path)
        if not normalized:
            return []
        return [segment for segment in normalized.split("\\") if segment]

    def _destination_projection_segments(self, path):
        segments = self._path_segments(path)
        if not segments:
            return []

        lowered = [segment.lower() for segment in segments]
        if "root" in lowered:
            return segments[lowered.index("root"):]

        context_segments = [segment.lower() for segment in self._current_destination_context_segments()]
        if context_segments:
            library_segment = context_segments[-1]
            if library_segment in lowered:
                library_index = lowered.index(library_segment)
                trimmed = segments[library_index + 1:]
                return ["Root", *trimmed] if trimmed else ["Root"]

            site_segment = context_segments[0]
            if site_segment in lowered:
                site_index = lowered.index(site_segment)
                trimmed = segments[site_index + 1:]
                if trimmed:
                    trimmed_lowered = [segment.lower() for segment in trimmed]
                    if library_segment in trimmed_lowered:
                        library_index = trimmed_lowered.index(library_segment)
                        trimmed = trimmed[library_index + 1:]
                    return ["Root", *trimmed] if trimmed else ["Root"]

        return ["Root", *segments]

    def _canonical_destination_projection_path(self, path):
        segments = self._destination_projection_segments(path)
        if not segments:
            return ""
        canonical_path = self.normalize_memory_path("\\".join(segments))
        return self._destination_semantic_alias_path(canonical_path)

    def _destination_parent_match_details(self, requested_parent_path, candidate_tree_path):
        normalized_requested = self._canonical_destination_projection_path(requested_parent_path)
        normalized_candidate = self._canonical_destination_projection_path(candidate_tree_path)
        requested_segments = [segment.lower() for segment in self._path_segments(normalized_requested)]
        candidate_segments = [segment.lower() for segment in self._path_segments(normalized_candidate)]
        exact_match = bool(requested_segments and candidate_segments and requested_segments == candidate_segments)
        prefix_only_match = False
        if requested_segments and candidate_segments and not exact_match:
            min_length = min(len(requested_segments), len(candidate_segments))
            prefix_only_match = requested_segments[:min_length] == candidate_segments[:min_length]
        return {
            "normalized_requested": normalized_requested,
            "normalized_candidate": normalized_candidate,
            "exact_match": exact_match,
            "prefix_only_match": prefix_only_match,
        }

    def _destination_projection_prefixes(self, path):
        segments = self._destination_projection_segments(path)
        prefixes = []
        for depth in range(1, len(segments) + 1):
            prefixes.append(self.normalize_memory_path("\\".join(segments[:depth])))
        return prefixes

    def _destination_semantic_alias_path(self, path):
        canonical_path = self.normalize_memory_path(path)
        return canonical_path

    def _current_destination_context_segments(self):
        site_name = ""
        library_name = ""
        if hasattr(self, "planning_inputs"):
            destination_site_selector = self.planning_inputs.get("Destination Site")
            destination_library_selector = self.planning_inputs.get("Destination Library")
            destination_site = destination_site_selector.currentData() if destination_site_selector is not None else None
            destination_library = destination_library_selector.currentData() if destination_library_selector is not None else None
            if isinstance(destination_site, dict):
                site_name = destination_site.get("name", "")
            if isinstance(destination_library, dict):
                library_name = destination_library.get("name", "")

        if not site_name and isinstance(self._draft_shell_state, SessionState):
            site_name = self._draft_shell_state.SelectedDestinationSite or ""
        if not library_name and isinstance(self._draft_shell_state, SessionState):
            library_name = self._draft_shell_state.SelectedDestinationLibrary or ""

        return [segment for segment in (site_name, library_name) if segment]

    def _current_source_context_segments(self):
        site_name = ""
        library_name = ""
        if hasattr(self, "planning_inputs"):
            source_site_selector = self.planning_inputs.get("Source Site")
            source_library_selector = self.planning_inputs.get("Source Library")
            source_site = source_site_selector.currentData() if source_site_selector is not None else None
            source_library = source_library_selector.currentData() if source_library_selector is not None else None
            if isinstance(source_site, dict):
                site_name = source_site.get("name", "")
            if isinstance(source_library, dict):
                library_name = source_library.get("name", "")

        if not site_name and isinstance(self._draft_shell_state, SessionState):
            site_name = self._draft_shell_state.SelectedSourceSite or ""
        if not library_name and isinstance(self._draft_shell_state, SessionState):
            library_name = self._draft_shell_state.SelectedSourceLibrary or ""

        return [segment for segment in (site_name, library_name) if segment]

    def _canonical_source_projection_path(self, path):
        segments = self._path_segments(path)
        if not segments:
            return ""

        lowered_segments = [segment.lower() for segment in segments]
        context_segments = [segment.lower() for segment in self._current_source_context_segments()]
        if context_segments:
            library_segment = context_segments[-1]
            if library_segment in lowered_segments:
                library_index = lowered_segments.index(library_segment)
                trimmed = segments[library_index + 1:]
                if trimmed:
                    return "\\".join(trimmed)

            site_segment = context_segments[0]
            if site_segment in lowered_segments:
                site_index = lowered_segments.index(site_segment)
                trimmed = segments[site_index + 1:]
                if trimmed:
                    return "\\".join(trimmed)

        return "\\".join(segments)

    def _normalized_path_variants(self, path, tree_role=""):
        if tree_role == "source":
            path = self._canonical_source_projection_path(path)
        segments = self._path_segments(path)
        if not segments:
            return set()

        variants = {"\\".join(segments)}
        min_suffix_length = 1 if tree_role == "destination" else (2 if len(segments) > 1 else 1)
        for start_index in range(max(0, len(segments) - 6), len(segments)):
            suffix = segments[start_index:]
            if len(suffix) >= min_suffix_length:
                variants.add("\\".join(suffix))

        lowered = [segment.lower() for segment in segments]
        if tree_role in {"source", "destination"} and "root" in lowered:
            root_index = lowered.index("root")
            variants.add("\\".join(segments[root_index:]))

        return {self.normalize_memory_path(variant) for variant in variants if variant}

    def _paths_equivalent(self, left_path, right_path, tree_role=""):
        left_variants = self._normalized_path_variants(left_path, tree_role)
        right_variants = self._normalized_path_variants(right_path, tree_role)
        return bool(left_variants and right_variants and left_variants.intersection(right_variants))

    def _path_is_descendant(self, child_path, parent_path, tree_role=""):
        child_variants = self._normalized_path_variants(child_path, tree_role)
        parent_variants = self._normalized_path_variants(parent_path, tree_role)
        for child_variant in child_variants:
            child_lower = child_variant.lower()
            for parent_variant in parent_variants:
                parent_lower = parent_variant.lower()
                if child_lower == parent_lower:
                    continue
                if child_lower.startswith(f"{parent_lower}\\"):
                    return True
        return False

    def _proposed_parent_path(self, proposed_folder):
        if isinstance(proposed_folder, ProposedFolder):
            raw_path = self.normalize_memory_path(proposed_folder.ParentPath or proposed_folder.DestinationPath)
            return self._canonical_destination_projection_path(raw_path)
        return ""

    def _proposed_destination_path(self, proposed_folder):
        if isinstance(proposed_folder, ProposedFolder):
            raw_path = self.normalize_memory_path(proposed_folder.DestinationPath)
            return self._canonical_destination_projection_path(raw_path)
        return ""

    def _proposed_folder_key(self, proposed_folder):
        return self._proposed_destination_path(proposed_folder)

    def _unresolved_proposed_queue_size(self):
        return sum(len(entries) for entries in self.unresolved_proposed_by_parent_path.values())

    def _allocation_parent_path(self, move):
        return self._canonical_destination_projection_path(self.normalize_memory_path(move.get("destination_path", "")))

    def _move_target_name(self, move):
        if not isinstance(move, dict):
            return ""
        target_name = str(move.get("target_name", "") or "").strip()
        if target_name:
            return target_name
        source_name = str(move.get("source_name", "") or "").strip()
        if source_name:
            return source_name
        source_node = move.get("source", {}) if isinstance(move.get("source", {}), dict) else {}
        return str(source_node.get("name", "") or "").strip()

    def _allocation_projection_path(self, move):
        parent_path = self._allocation_parent_path(move)
        target_name = self._move_target_name(move)
        if not parent_path or not target_name:
            return self._canonical_destination_projection_path(self.normalize_memory_path(parent_path or target_name))
        return self._canonical_destination_projection_path(self.normalize_memory_path(f"{parent_path}\\{target_name}"))

    def _allocation_move_key(self, move):
        return self._allocation_projection_path(move)

    def _unresolved_allocation_queue_size(self):
        return sum(len(entries) for entries in self.unresolved_allocations_by_parent_path.values())

    def _reset_unresolved_proposed_queue(self):
        self.unresolved_proposed_by_parent_path = {}
        for proposed_folder in self.proposed_folders:
            self._queue_unresolved_proposed_folder(proposed_folder, "restore_payload_loaded")
        self._log_restore_phase(
            "unresolved_proposed_queue_reset",
            queue_size=self._unresolved_proposed_queue_size(),
            parent_count=len(self.unresolved_proposed_by_parent_path),
        )

    def _reset_unresolved_allocation_queue(self):
        self.unresolved_allocations_by_parent_path = {}
        for move in self.planned_moves:
            self._queue_unresolved_allocation(move, "restore_payload_loaded")
        self._log_restore_phase(
            "unresolved_allocation_queue_reset",
            queue_size=self._unresolved_allocation_queue_size(),
            parent_count=len(self.unresolved_allocations_by_parent_path),
        )

    def _queue_unresolved_proposed_folder(self, proposed_folder, reason):
        parent_path = self._proposed_parent_path(proposed_folder)
        if not parent_path:
            return
        bucket = self.unresolved_proposed_by_parent_path.setdefault(parent_path, {})
        bucket[self._proposed_folder_key(proposed_folder)] = proposed_folder
        self._log_restore_phase(
            "unresolved_proposed_parent_queued",
            reason=reason,
            parent_path=parent_path,
            destination_path=self._proposed_destination_path(proposed_folder),
            queue_size=self._unresolved_proposed_queue_size(),
            verbose=True,
        )

    def _queue_unresolved_allocation(self, move, reason):
        parent_path = self._allocation_parent_path(move)
        if not parent_path:
            return
        bucket = self.unresolved_allocations_by_parent_path.setdefault(parent_path, {})
        bucket[self._allocation_move_key(move)] = move
        self._log_restore_phase(
            "unresolved_allocation_parent_queued",
            reason=reason,
            parent_path=parent_path,
            destination_path=self._allocation_projection_path(move),
            queue_size=self._unresolved_allocation_queue_size(),
            verbose=True,
        )

    def _quick_apply_allocated_node_under_parent(self, move):
        """Attach projected allocation under its destination parent without a full future-model rebuild."""
        if not isinstance(move, dict):
            return 0
        parent_path = self._allocation_parent_path(move)
        if not parent_path:
            return 0
        try:
            parent_item = self._ensure_destination_projection_path(parent_path)
            if parent_item is None:
                return 0
            return self._apply_allocation_children_to_item(parent_item)
        except Exception as exc:
            self._log_restore_exception("quick_apply_allocated_node_under_parent", exc)
            return 0

    def _register_and_project_planned_move(self, move, *, reason="planned_move"):
        self._queue_unresolved_allocation(move, reason)
        return self._quick_apply_allocated_node_under_parent(move)

    def _defer_destination_projection_if_quick_apply_failed(self, applied, *, reconcile_reason, delay_ms=120):
        if applied and int(applied) > 0:
            return
        self._schedule_deferred_destination_materialization(reconcile_reason, delay_ms=delay_ms)

    def _take_destination_tree_item(self, item):
        tree = getattr(self, "destination_tree_widget", None)
        if tree is None or item is None:
            return
        parent = item.parent()
        if parent is not None:
            parent.removeChild(item)
            self._refresh_destination_item_visibility(parent, expand=False)
        else:
            ix = tree.indexOfTopLevelItem(item)
            if ix >= 0:
                tree.takeTopLevelItem(ix)

    def _destination_tree_item_matches_move(self, item, move):
        if item is None or not isinstance(move, dict):
            return False
        d = item.data(0, Qt.UserRole) or {}
        ms = self._canonical_source_projection_path(move.get("source_path", ""))
        ns = self._canonical_source_projection_path(d.get("source_path", ""))
        if ms and ns and ms.casefold() == ns.casefold():
            return True
        src = move.get("source") if isinstance(move.get("source"), dict) else {}
        mid = str(src.get("id", "") or "").strip()
        if mid and mid == str(d.get("source_item_id", "") or "").strip():
            return True
        return False

    def _remove_planned_allocation_projection_children(self, parent_item):
        if parent_item is None:
            return
        index = parent_item.childCount() - 1
        while index >= 0:
            child = parent_item.child(index)
            d = child.data(0, Qt.UserRole) or {}
            cid = str(d.get("id", "") or "")
            if d.get("planned_allocation_descendant") or cid.startswith("allocated-descendant::"):
                parent_item.takeChild(index)
            elif cid.startswith("allocated::") and self.node_is_planned_allocation(d):
                parent_item.takeChild(index)
            index -= 1

    def _strip_planned_allocation_overlay_for_move(self, item, move):
        if item is None or not isinstance(move, dict):
            return
        node_data = dict(item.data(0, Qt.UserRole) or {})
        dest = move.get("destination") if isinstance(move.get("destination"), dict) else {}
        for key in ("display_path", "item_path", "destination_path"):
            v = dest.get(key)
            if v:
                node_data[key] = v
        node_data.pop("planned_allocation", None)
        node_data["node_origin"] = "Real"
        node_data.pop("overlay_state", None)
        name = str(node_data.get("real_name") or node_data.get("name", "") or "").strip() or "Item"
        is_folder = bool(node_data.get("is_folder", True))
        prefix = "Folder" if is_folder else "File"
        base_label = f"{prefix}: {name}"
        node_data["base_display_label"] = base_label
        item.setData(0, Qt.UserRole, node_data)
        self._apply_tree_item_visual_state(item, node_data)
        self._refresh_destination_item_visibility(item)

    def _find_destination_tree_item_for_planned_move(self, move):
        """Locate the tree row for a planned move when parent/child equivalence lookup misses."""
        if not isinstance(move, dict):
            return None
        tree = getattr(self, "destination_tree_widget", None)
        if tree is None:
            return None
        target_alloc = self._canonical_destination_projection_path(self._allocation_projection_path(move))
        if not target_alloc:
            return None
        for index in range(tree.topLevelItemCount()):
            for item in self._iter_tree_items(tree.topLevelItem(index)):
                nd = item.data(0, Qt.UserRole) or {}
                if nd.get("placeholder"):
                    continue
                item_path = self._canonical_destination_projection_path(self._tree_item_path(nd))
                if item_path == target_alloc and (
                    self.node_is_planned_allocation(nd) or nd.get("planned_allocation")
                ):
                    return item
        return None

    def _quick_remove_planned_move_from_destination_tree(self, move):
        """Remove or unmark the visible destination projection for one planned move (no full tree bind)."""
        if not isinstance(move, dict):
            return 0
        self._mark_allocation_resolved(move)
        parent_path = self._allocation_parent_path(move)
        child = None
        parent_item = self._find_visible_destination_item_by_path(parent_path) if parent_path else None
        if parent_item is not None:
            child = self._find_destination_child_by_allocation_equivalence(parent_item, move)
        if child is None:
            proj = self._allocation_projection_path(move)
            if proj:
                child = self._find_visible_destination_item_by_path(proj)
        if child is None:
            child = self._find_destination_tree_item_for_planned_move(move)
        if child is None:
            return 0
        if not self._destination_tree_item_matches_move(child, move):
            node_data_probe = child.data(0, Qt.UserRole) or {}
            alloc = self._canonical_destination_projection_path(self._allocation_projection_path(move))
            ip = self._canonical_destination_projection_path(self._tree_item_path(node_data_probe))
            if not (
                alloc
                and ip
                and ip == alloc
                and (
                    self.node_is_planned_allocation(node_data_probe)
                    or bool(node_data_probe.get("planned_allocation"))
                )
            ):
                return 0
        node_data = child.data(0, Qt.UserRole) or {}
        cid = str(node_data.get("id", "") or "")
        if cid.startswith("allocated::"):
            self._take_destination_tree_item(child)
            return 1
        if self.node_is_planned_allocation(node_data):
            self._remove_planned_allocation_projection_children(child)
            self._strip_planned_allocation_overlay_for_move(child, move)
            return 1
        return 0

    def _mark_proposed_folder_resolved(self, proposed_folder):
        parent_path = self._proposed_parent_path(proposed_folder)
        bucket = self.unresolved_proposed_by_parent_path.get(parent_path, {})
        bucket.pop(self._proposed_folder_key(proposed_folder), None)
        if not bucket and parent_path in self.unresolved_proposed_by_parent_path:
            self.unresolved_proposed_by_parent_path.pop(parent_path, None)

    def _mark_allocation_resolved(self, move):
        parent_path = self._allocation_parent_path(move)
        bucket = self.unresolved_allocations_by_parent_path.get(parent_path, {})
        bucket.pop(self._allocation_move_key(move), None)
        if not bucket and parent_path in self.unresolved_allocations_by_parent_path:
            self.unresolved_allocations_by_parent_path.pop(parent_path, None)

    def _get_unresolved_candidates_for_parent(self, parent_path):
        matches = []
        for stored_parent_path, bucket in self.unresolved_proposed_by_parent_path.items():
            if self._paths_equivalent(stored_parent_path, parent_path, "destination"):
                matches.extend(bucket.values())
        return matches

    def _get_unresolved_allocation_candidates_for_parent(self, parent_path):
        matches = []
        for stored_parent_path, bucket in self.unresolved_allocations_by_parent_path.items():
            if self._paths_equivalent(stored_parent_path, parent_path, "destination"):
                matches.extend(bucket.values())
        return matches

    def _tree_item_path(self, node_data):
        return self.normalize_memory_path(
            node_data.get("display_path")
            or node_data.get("item_path")
            or node_data.get("source_path")
            or node_data.get("destination_path")
            or ""
        )

    def _resolve_tree_item_drive_id(self, panel_key, node_data):
        """Drive id for Graph folder loads: prefer the row's drive/library id, then the panel root."""
        if not isinstance(node_data, dict):
            return ""
        return (
            str(node_data.get("drive_id") or node_data.get("library_id") or "").strip()
            or str(self.pending_root_drive_ids.get(panel_key, "") or "").strip()
        )

    def _proposed_folder_exists_under(self, parent_item, destination_path):
        normalized_target = self.normalize_memory_path(destination_path)
        for index in range(parent_item.childCount()):
            child = parent_item.child(index)
            child_data = child.data(0, Qt.UserRole) or {}
            child_path = child_data.get("item_path") or child_data.get("display_path") or ""
            if self._paths_equivalent(child_path, normalized_target, "destination"):
                return True
        return False

    def _build_proposed_tree_node(self, proposed_folder: ProposedFolder, parent_data):
        parent_path = self._tree_item_path(parent_data)
        destination_path = (
            self._canonical_destination_projection_path(f"{parent_path}\\{proposed_folder.FolderName}")
            if parent_path
            else self._canonical_destination_projection_path(proposed_folder.DestinationPath)
        )
        destination_path = destination_path or self.normalize_memory_path(proposed_folder.DestinationPath)
        base_label = f"Folder: {proposed_folder.FolderName}"
        node = QTreeWidgetItem([base_label])
        node_data = {
            "id": proposed_folder.DestinationId or f"proposed::{proposed_folder.DestinationPath}",
            "name": proposed_folder.FolderName,
            "real_name": proposed_folder.FolderName,
            "display_path": destination_path,
            "item_path": destination_path,
            "destination_path": destination_path,
            "tree_role": "destination",
            "drive_id": parent_data.get("drive_id", ""),
            "site_id": parent_data.get("site_id", ""),
            "site_name": parent_data.get("site_name", ""),
            "library_id": parent_data.get("library_id", ""),
            "library_name": parent_data.get("library_name", ""),
            "is_folder": True,
            "children_loaded": True,
            "load_failed": False,
            "node_origin": "Proposed",
            "proposed": True,
            "web_url": parent_data.get("web_url", ""),
            "base_display_label": base_label,
        }
        node.setData(0, Qt.UserRole, node_data)
        self._apply_tree_item_visual_state(node, node_data)
        self._refresh_destination_item_visibility(node)
        return node

    def _find_destination_child_by_allocation_equivalence(self, parent_item, move):
        """Match planned allocation path using the same rules as the old exists_under check.

        `_find_destination_child_by_path` can miss real SharePoint rows when only
        `item_path`/`display_path` line up via `_paths_equivalent`, which previously caused
        `_apply_allocation_children_to_item` to skip without upgrading the real child.
        """
        if parent_item is None or move is None:
            return None
        normalized_target = self._allocation_projection_path(move)
        for index in range(parent_item.childCount()):
            child = parent_item.child(index)
            child_data = child.data(0, Qt.UserRole) or {}
            if child_data.get("placeholder"):
                continue
            child_path = child_data.get("item_path") or child_data.get("display_path") or ""
            if self._paths_equivalent(child_path, normalized_target, "destination"):
                return child
        return None

    def _planned_allocation_exists_under(self, parent_item, move):
        return self._find_destination_child_by_allocation_equivalence(parent_item, move) is not None

    def _ensure_planned_allocation_overlay_on_real_item(self, parent_item, item, move):
        """When a real destination row already exists at the allocation path, keep its drive id
        but apply the same planned-allocation metadata and expand affordance as a virtual node.
        """
        if parent_item is None or item is None or move is None:
            return
        node_data = dict(item.data(0, Qt.UserRole) or {})
        if self.node_is_planned_allocation(node_data):
            self._apply_tree_item_visual_state(item, node_data)
            self._refresh_destination_item_visibility(item)
            return

        parent_data = parent_item.data(0, Qt.UserRole) or {}
        parent_path = self._tree_item_path(parent_data)
        source_node = move.get("source", {})
        source_name = self._move_target_name(move) or source_node.get("name", "Allocated Item")
        is_folder = bool(source_node.get("is_folder", True))
        prefix = "Folder" if is_folder else "File"
        base_label = f"{prefix}: {source_name} [Allocated]"
        projection_path = (
            self._canonical_destination_projection_path(f"{parent_path}\\{source_name}")
            if parent_path
            else self._allocation_projection_path(move)
        )
        projection_path = projection_path or self._allocation_projection_path(move)

        node_data["planned_allocation"] = True
        node_data["node_origin"] = "PlannedAllocation"
        node_data["overlay_state"] = "PlannedAllocation"
        node_data["source_path"] = move.get("source_path", "")
        node_data["display_path"] = projection_path
        node_data["item_path"] = projection_path
        node_data["destination_path"] = projection_path
        node_data["base_display_label"] = base_label
        item.setData(0, Qt.UserRole, node_data)
        self._apply_tree_item_visual_state(item, node_data)
        if is_folder:
            item.setChildIndicatorPolicy(QTreeWidgetItem.ShowIndicator)
        else:
            item.setChildIndicatorPolicy(QTreeWidgetItem.DontShowIndicator)
        self._refresh_destination_item_visibility(item)

    def _build_allocation_tree_node(self, move, parent_data):
        source_node = move.get("source", {})
        source_name = self._move_target_name(move) or source_node.get("name", "Allocated Item")
        is_folder = source_node.get("is_folder", True)
        prefix = "Folder" if is_folder else "File"
        base_label = f"{prefix}: {source_name} [Allocated]"
        node = QTreeWidgetItem([base_label])
        parent_path = self._tree_item_path(parent_data)
        projection_path = (
            self._canonical_destination_projection_path(f"{parent_path}\\{source_name}")
            if parent_path
            else self._allocation_projection_path(move)
        )
        projection_path = projection_path or self._allocation_projection_path(move)
        node_data = {
            "id": f"allocated::{projection_path}",
            "name": source_name,
            "real_name": source_name,
            "display_path": projection_path,
            "item_path": projection_path,
            "destination_path": projection_path,
            "source_path": move.get("source_path", ""),
            "tree_role": "destination",
            "drive_id": parent_data.get("drive_id", ""),
            "site_id": parent_data.get("site_id", ""),
            "site_name": parent_data.get("site_name", ""),
            "library_id": parent_data.get("library_id", ""),
            "library_name": parent_data.get("library_name", ""),
            "is_folder": is_folder,
            "children_loaded": not bool(is_folder),
            "load_failed": False,
            "node_origin": "PlannedAllocation",
            "overlay_state": "PlannedAllocation",
            "planned_allocation": True,
            "web_url": parent_data.get("web_url", ""),
            "base_display_label": base_label,
        }
        node.setData(0, Qt.UserRole, node_data)
        node.setChildIndicatorPolicy(QTreeWidgetItem.ShowIndicator if is_folder else QTreeWidgetItem.DontShowIndicator)
        self._apply_tree_item_visual_state(node, node_data)
        self._log_restore_phase(
            "destination_projection_node_created",
            destination_path=projection_path,
            normalized_destination_path=projection_path,
            parent_path=self._allocation_parent_path(move),
            node_state="allocated",
            created_virtual=True,
        )
        return node

    def _build_projected_allocation_descendant_item(self, source_node_data, destination_path, parent_data):
        source_name = source_node_data.get("name", "") or self._path_segments(destination_path)[-1]
        is_folder = bool(source_node_data.get("is_folder", False))
        prefix = "Folder" if is_folder else "File"
        base_label = f"{prefix}: {source_name}"
        node = QTreeWidgetItem([base_label])
        normalized_path = self._canonical_destination_projection_path(destination_path) or self.normalize_memory_path(destination_path)
        node_data = self._build_destination_allocation_descendant_node_data(source_node_data, normalized_path, parent_data)
        node_data["base_display_label"] = base_label
        node.setData(0, Qt.UserRole, node_data)
        node.setChildIndicatorPolicy(QTreeWidgetItem.ShowIndicator if is_folder else QTreeWidgetItem.DontShowIndicator)
        self._apply_tree_item_visual_state(node, node_data)
        return node

    def _allocation_projection_relative_source_segments(self, source_root_path, descendant_source_path):
        """Segments of descendant path under allocation source root.

        Graph often returns paths relative to the resolved folder (e.g. ``Working with Children\\a.pdf``)
        while the tree uses a full canonical root (``...\\Working with Children``). Match via full prefix
        first, then longest suffix/prefix segment overlap (case-insensitive).
        """
        source_root_path = self._canonical_source_projection_path(source_root_path or "")
        descendant_source_path = self._canonical_source_projection_path(descendant_source_path or "")
        if not source_root_path or not descendant_source_path:
            return None
        if descendant_source_path == source_root_path:
            return []
        root_segs = self._path_segments(source_root_path)
        desc_segs = self._path_segments(descendant_source_path)
        if not desc_segs:
            return None
        if descendant_source_path.startswith(source_root_path + "\\") or descendant_source_path.startswith(
            source_root_path + "/"
        ):
            rel = descendant_source_path[len(source_root_path) :].lstrip("\\/")
            return self._path_segments(rel)
        if not root_segs:
            return None
        rlow = [s.casefold() for s in root_segs]
        dlow = [s.casefold() for s in desc_segs]
        max_k = min(len(root_segs), len(desc_segs))
        for k in range(max_k, 0, -1):
            if rlow[-k:] == dlow[:k]:
                return desc_segs[k:]
        return None

    def _apply_allocation_descendants_to_item(self, parent_item, move):
        if parent_item is None or move is None:
            return 0

        parent_data = parent_item.data(0, Qt.UserRole) or {}
        allocation_destination_path = self._canonical_destination_projection_path(
            self._tree_item_path(parent_data) or self._allocation_projection_path(move)
        )
        if not allocation_destination_path:
            self._log_restore_phase(
                "destination_projection_apply_skipped",
                reason="no_allocation_destination_path",
                move_correlation_id=id(move),
                destination_path_raw=self._tree_item_path(parent_data),
            )
            return 0

        source_item = self._find_source_item_for_planned_move(move)
        source_root_data = source_item.data(0, Qt.UserRole) or {} if source_item is not None else {}
        if not source_root_data:
            source_root_data = dict(move.get("source", {}) or {})
            source_root_data.setdefault("item_path", move.get("source_path", ""))
            source_root_data.setdefault("display_path", move.get("source_path", ""))

        expected_is_folder = bool(parent_data.get("is_folder", True))
        # Some planned-move payloads omit `is_folder`; default based on the planned allocation node.
        if source_root_data and source_root_data.get("is_folder", None) is None:
            source_root_data["is_folder"] = expected_is_folder

        if not source_root_data or not bool(source_root_data.get("is_folder", expected_is_folder)):
            self._log_restore_phase(
                "destination_projection_apply_skipped",
                reason="source_not_folder_or_missing_root_data",
                move_correlation_id=id(move),
                allocation_destination_path=allocation_destination_path,
                source_item_found_in_tree=source_item is not None,
                source_is_folder=bool(source_root_data.get("is_folder", False)) if source_root_data else None,
                has_source_payload_keys=bool(move.get("source")),
            )
            return 0

        source_root_path = self._canonical_source_projection_path(self._tree_item_path(source_root_data))
        descendants = self._collect_source_descendants_for_projection(source_root_data, move)
        descendants = sorted(
            descendants,
            key=lambda data: (
                len(self._path_segments(self._canonical_source_projection_path(self._tree_item_path(data)))),
                0 if bool(data.get("is_folder", False)) else 1,
                str(data.get("name", "")).lower(),
            ),
        )

        added_count = 0
        for desc_index, descendant_data in enumerate(descendants):
            # Yield for repaints without dispatching user input (avoids re-entrant expand/snapshot work).
            if desc_index > 0 and desc_index % 50 == 0:
                QApplication.processEvents(QEventLoop.ProcessEventsFlag.ExcludeUserInputEvents)
            descendant_source_path = self._canonical_source_projection_path(self._tree_item_path(descendant_data))
            # Only skip when the descendant actually equals the allocation root.
            if descendant_source_path == source_root_path:
                continue

            relative_segments = self._allocation_projection_relative_source_segments(
                source_root_path, descendant_source_path
            )
            if not relative_segments:
                continue
            descendant_destination_path = self._canonical_destination_projection_path(
                "\\".join([allocation_destination_path] + relative_segments)
            )
            if not descendant_destination_path:
                continue

            exact_move = self._find_exact_planned_move_for_source_path(descendant_source_path)
            if exact_move is not None:
                exact_destination_path = self._canonical_destination_projection_path(
                    self.normalize_memory_path(
                        "\\".join([exact_move.get("destination_path", ""), self._move_target_name(exact_move)])
                    )
                )
                if exact_destination_path and exact_destination_path != descendant_destination_path:
                    continue

            current_parent_item = parent_item
            current_parent_data = parent_data
            for index, segment in enumerate(relative_segments):
                branch_path = self._canonical_destination_projection_path(
                    "\\".join([allocation_destination_path] + relative_segments[: index + 1])
                )
                is_leaf = index == len(relative_segments) - 1
                existing_child = self._find_destination_child_by_path(current_parent_item, branch_path)
                if existing_child is None:
                    self._remove_placeholder_children(current_parent_item)
                    new_child = self._build_projected_allocation_descendant_item(
                        descendant_data if is_leaf else {"name": segment, "real_name": segment, "is_folder": True},
                        branch_path,
                        current_parent_data,
                    )
                    current_parent_item.addChild(new_child)
                    self._refresh_destination_item_visibility(current_parent_item, expand=True)
                    existing_child = new_child
                    if is_leaf:
                        added_count += 1

                current_parent_item = existing_child
                current_parent_data = current_parent_item.data(0, Qt.UserRole) or {}

        if len(descendants) > 0 and added_count == 0:
            sample = []
            for d in descendants[:8]:
                sample.append(self._canonical_source_projection_path(self._tree_item_path(d)))
            self._log_restore_phase(
                "destination_projection_apply_descendants_all_filtered",
                move_correlation_id=id(move),
                allocation_destination_path=allocation_destination_path,
                source_root_path=source_root_path,
                descendant_count=len(descendants),
                sample_descendant_paths=sample,
            )
        if added_count > 0 and self._projection_diag_verbose_enabled():
            self._log_restore_phase(
                "destination_projection_apply_ok",
                move_correlation_id=id(move),
                allocation_destination_path=allocation_destination_path,
                source_root_path=source_root_path,
                added_count=added_count,
                descendant_count=len(descendants),
            )
        return added_count

    def _destination_bind_normalized_expanded_targets(self, destination_expanded_paths):
        target_paths = {p for p in (destination_expanded_paths or set()) if p}
        target_paths.add("Root")
        normalized_targets = set(target_paths)
        for path in list(target_paths):
            k = self._destination_expansion_state_key(path)
            if k:
                normalized_targets.add(k)
        return normalized_targets

    def _destination_projection_eager_expand_all_destination(self):
        return bool((getattr(self, "_workspace_restore_expanded_all_intent", None) or {}).get("destination"))

    def _destination_bind_should_apply_allocation_descendants_now(self, allocation_semantic_path, normalized_targets):
        """True when saved workspace expansion reaches this allocation or one of its descendants.

        Otherwise projected file/folder rows load on expand via ``_load_destination_projected_descendants``.
        """
        if self._destination_projection_eager_expand_all_destination():
            return True
        if not allocation_semantic_path:
            return False
        ap = self._canonical_destination_projection_path(allocation_semantic_path)
        if not ap:
            return False
        if not normalized_targets:
            return False
        prefix = ap + "\\"
        for t in normalized_targets:
            ts = self._canonical_destination_projection_path(str(t).strip()) if t else ""
            if not ts:
                continue
            if ts == ap or ts.startswith(prefix):
                return True
        return False

    def _destination_incremental_merge_root_needed_now(self, projected_root_semantic_path, normalized_targets):
        if self._destination_projection_eager_expand_all_destination():
            return True
        sp = self._canonical_destination_projection_path(projected_root_semantic_path)
        if not sp:
            return True
        if not normalized_targets:
            return False
        pfx = sp + "\\"
        for t in normalized_targets:
            ts = self._canonical_destination_projection_path(str(t).strip()) if t else ""
            if not ts:
                continue
            if ts == sp or ts.startswith(pfx):
                return True
        return False

    def _apply_visible_destination_allocation_descendants(self, *, destination_expanded_paths=None):
        tree = getattr(self, "destination_tree_widget", None)
        if tree is None or tree.topLevelItemCount() == 0:
            return 0

        if destination_expanded_paths is not None:
            ep = set(destination_expanded_paths or set())
        else:
            ui_state = self._capture_workspace_tree_state()
            ep = set(ui_state.get("destination_expanded_paths", set()) or set())
        nt = self._destination_bind_normalized_expanded_targets(ep)

        applied_count = 0
        allocation_pass = 0
        visited = 0
        for index in range(tree.topLevelItemCount()):
            for item in self._iter_tree_items(tree.topLevelItem(index)):
                visited += 1
                if visited % 40 == 0:
                    QApplication.processEvents(QEventLoop.ProcessEventsFlag.ExcludeUserInputEvents)
                node_data = item.data(0, Qt.UserRole) or {}
                if not self.node_is_planned_allocation(node_data):
                    continue
                if not bool(node_data.get("is_folder", False)):
                    continue
                move = self._find_planned_move_for_destination_node(node_data)
                if move is None:
                    continue
                allocation_pass += 1
                # Future-state bind already attached projected / real children for many allocations.
                # Re-running Graph + path matching here duplicates work and freezes the UI for minutes.
                if item.childCount() > 0 and self._destination_allocation_folder_shows_materialized_children(item):
                    nd = dict(node_data)
                    if not bool(nd.get("children_loaded")):
                        nd["children_loaded"] = True
                        nd["projection_unresolved_terminal"] = False
                        item.setData(0, Qt.UserRole, nd)
                else:
                    sem = self._destination_semantic_path(node_data)
                    if self._destination_bind_should_apply_allocation_descendants_now(sem, nt):
                        applied_count += self._apply_allocation_descendants_to_item(item, move)
                if allocation_pass % 2 == 0:
                    QApplication.processEvents(QEventLoop.ProcessEventsFlag.ExcludeUserInputEvents)

        if applied_count:
            tree.viewport().update()
        return applied_count

    def _destination_node_state(self, node_data):
        if self.node_is_planned_allocation(node_data):
            return "allocated"
        if self.node_is_proposed(node_data):
            return "proposed"
        if str(node_data.get("node_origin", "")).lower() == "projecteddestination":
            return "projected"
        return "real"

    def _destination_resolution_rank(self, node_data):
        state = self._destination_node_state(node_data)
        if state == "real":
            return 0
        if state == "projected":
            return 1
        if state == "proposed":
            return 2
        return 3

    def _select_canonical_destination_item(self, items):
        if not items:
            return None
        return min(
            items,
            key=lambda item: (
                self._destination_resolution_rank(item.data(0, Qt.UserRole) or {}),
                -len(self._path_segments(self._tree_item_path(item.data(0, Qt.UserRole) or {}))),
            ),
        )

    def _destination_semantic_path(self, node_data):
        return self._canonical_destination_projection_path(self._tree_item_path(node_data))

    def _destination_expansion_state_key(self, path: str) -> str:
        """Normalize paths for expand/collapse persistence across tree rebuilds.

        Live SharePoint rows often use ``Site\\Library\\Root\\...`` while future-model binds use
        ``Root\\...`` only. Without this, ``_restore_expanded_destination_paths`` after
        ``_bind_destination_tree_from_future_state_model`` only restores ``Root``, leaving deep
        nodes visually collapsed while the expand-all button still says *Collapse All*.
        """
        canon = self._canonical_destination_projection_path(path or "")
        if not canon:
            return ""
        segs = self._path_segments(canon)
        for i, seg in enumerate(segs):
            if str(seg).strip().lower() == "root":
                return "\\".join(segs[i:])
        return canon

    def _destination_future_state_rank(self, node_state):
        if node_state == "projected":
            return 0
        if node_state == "proposed":
            return 1
        if node_state == "allocated":
            return 2
        return 3

    def _destination_model_state_priority(self, node_state):
        if node_state == "real":
            return 3
        if node_state == "proposed":
            return 2
        if node_state == "projected":
            return 1
        if node_state == "projected_descendant":
            return 1
        return 0

    def _snapshot_real_destination_node(self, item):
        if item is None:
            return None
        node_data = item.data(0, Qt.UserRole) or {}
        if node_data.get("placeholder") or self._destination_is_future_state_node(node_data):
            return None
        semantic_path = self._destination_semantic_path(node_data)
        if not semantic_path:
            return None
        snapshot = {
            "semantic_path": semantic_path,
            "parent_semantic_path": self.normalize_memory_path("\\".join(self._path_segments(semantic_path)[:-1])),
            "data": dict(node_data),
            "children": [],
        }
        for index in range(item.childCount()):
            child_snapshot = self._snapshot_real_destination_node(item.child(index))
            if child_snapshot is not None:
                snapshot["children"].append(child_snapshot)
        return snapshot

    def _collect_real_destination_snapshot_entries(self, item, entries):
        if item is None:
            return
        node_data = item.data(0, Qt.UserRole) or {}
        if not node_data.get("placeholder"):
            semantic_path = self._destination_semantic_path(node_data)
            if semantic_path and not self._destination_is_future_state_node(node_data):
                entries.append({
                    "semantic_path": semantic_path,
                    "parent_semantic_path": self.normalize_memory_path("\\".join(self._path_segments(semantic_path)[:-1])),
                    "data": dict(node_data),
                    "children": [],
                })
        for index in range(item.childCount()):
            self._collect_real_destination_snapshot_entries(item.child(index), entries)

    def _refresh_destination_real_tree_snapshot(self):
        current_drive_id = self._current_selected_destination_drive_id() or self.pending_root_drive_ids.get("destination", "")
        snapshot = []
        tree = getattr(self, "destination_tree_widget", None)
        if tree is None:
            self._destination_real_tree_snapshot = []
            return
        for index in range(tree.topLevelItemCount()):
            self._collect_real_destination_snapshot_entries(tree.topLevelItem(index), snapshot)

        full_snapshot_available = (
            bool(self._destination_full_tree_snapshot)
            and self._destination_full_tree_completed_drive_id == current_drive_id
        )
        if not full_snapshot_available:
            self._destination_real_tree_snapshot = snapshot
            return

        merged_by_path = {}
        for entry in self._destination_full_tree_snapshot:
            semantic_path = self.normalize_memory_path(entry.get("semantic_path", ""))
            if semantic_path:
                merged_by_path[semantic_path] = dict(entry)
        for entry in snapshot:
            semantic_path = self.normalize_memory_path(entry.get("semantic_path", ""))
            if semantic_path:
                merged_by_path[semantic_path] = dict(entry)

        merged_snapshot = list(merged_by_path.values())
        merged_snapshot.sort(key=lambda value: ([segment.lower() for segment in self._path_segments(value.get("semantic_path", ""))],))
        self._destination_real_tree_snapshot = merged_snapshot

    def _ensure_visible_destination_root_children_in_model(self, model_nodes):
        tree = getattr(self, "destination_tree_widget", None)
        if tree is None or tree.topLevelItemCount() == 0:
            return 0
        root_item = tree.topLevelItem(0)
        if root_item is None:
            return 0
        root_data = root_item.data(0, Qt.UserRole) or {}
        if self._destination_semantic_path(root_data) != "Root":
            return 0

        ensured_count = 0
        for index in range(root_item.childCount()):
            child_item = root_item.child(index)
            child_data = child_item.data(0, Qt.UserRole) or {}
            if child_data.get("placeholder") or self._destination_is_future_state_node(child_data):
                continue
            semantic_path = self._destination_semantic_path(child_data)
            if not semantic_path:
                continue
            name = child_data.get("name") or self._path_segments(semantic_path)[-1]
            self._upsert_destination_model_node(
                model_nodes,
                semantic_path,
                name=name,
                node_state="real",
                data=dict(child_data),
                parent_semantic_path="Root",
            )
            self._attach_destination_model_child(model_nodes, "Root", semantic_path)
            ensured_count += 1
        return ensured_count

    def _destination_root_base_data(self):
        tree = getattr(self, "destination_tree_widget", None)
        if tree is not None:
            for index in range(tree.topLevelItemCount()):
                item = tree.topLevelItem(index)
                node_data = item.data(0, Qt.UserRole) or {}
                if node_data.get("placeholder"):
                    continue
                semantic_path = self._destination_semantic_path(node_data)
                if semantic_path == "Root":
                    base_data = dict(node_data)
                    base_data.setdefault("tree_role", "destination")
                    base_data.setdefault("name", "Root")
                    base_data.setdefault("real_name", "Root")
                    base_data.setdefault("display_path", "Root")
                    base_data.setdefault("item_path", "Root")
                    base_data.setdefault("destination_path", "Root")
                    base_data.setdefault("is_folder", True)
                    return base_data
        root_data = self._build_destination_projected_node_data("Root", "Root", {})
        root_data.setdefault("tree_role", "destination")
        root_data.setdefault("name", "Root")
        root_data.setdefault("real_name", "Root")
        root_data.setdefault("display_path", "Root")
        root_data.setdefault("item_path", "Root")
        root_data.setdefault("destination_path", "Root")
        root_data.setdefault("is_folder", True)
        return root_data

    def _make_destination_model_node(self, semantic_path, name, node_state, data, parent_semantic_path=""):
        return {
            "semantic_path": semantic_path,
            "parent_semantic_path": parent_semantic_path,
            "name": name,
            "node_state": node_state,
            "data": dict(data),
            "children": [],
        }

    def _coerce_destination_model_node_state(self, semantic_path, node_state, data):
        normalized_path = self.normalize_memory_path(semantic_path)
        effective_state = node_state
        effective_data = dict(data)
        if normalized_path and node_state == "real":
            proposed_folder = self._find_proposed_folder_record_by_path(normalized_path)
            if proposed_folder is not None:
                effective_state = "proposed"
                effective_data = self._build_destination_proposed_node_data(
                    proposed_folder,
                    effective_data,
                )
        return effective_state, effective_data

    def _upsert_destination_model_node(self, model_nodes, semantic_path, *, name, node_state, data, parent_semantic_path=""):
        node_state, data = self._coerce_destination_model_node_state(semantic_path, node_state, data)
        node = model_nodes.get(semantic_path)
        if node is None:
            node = self._make_destination_model_node(semantic_path, name, node_state, data, parent_semantic_path=parent_semantic_path)
            model_nodes[semantic_path] = node
        else:
            if self._destination_model_state_priority(node_state) > self._destination_model_state_priority(node["node_state"]):
                node["node_state"] = node_state
                node["data"] = dict(data)
            node["name"] = name or node["name"]
            if parent_semantic_path:
                node["parent_semantic_path"] = parent_semantic_path
        return node

    def _attach_destination_model_child(self, model_nodes, parent_semantic_path, child_semantic_path):
        if not parent_semantic_path:
            return
        parent_node = model_nodes.get(parent_semantic_path)
        child_node = model_nodes.get(child_semantic_path)
        if parent_node is None or child_node is None:
            return
        if child_semantic_path not in parent_node["children"]:
            parent_node["children"].append(child_semantic_path)
            child_node["parent_semantic_path"] = parent_semantic_path

    def _build_destination_projected_node_data(self, semantic_path, name, parent_data):
        return {
            "id": f"projected::{semantic_path}",
            "name": name,
            "real_name": name,
            "display_path": semantic_path,
            "item_path": semantic_path,
            "destination_path": semantic_path,
            "tree_role": "destination",
            "drive_id": parent_data.get("drive_id", ""),
            "site_id": parent_data.get("site_id", ""),
            "site_name": parent_data.get("site_name", ""),
            "library_id": parent_data.get("library_id", ""),
            "library_name": parent_data.get("library_name", ""),
            "is_folder": True,
            "children_loaded": True,
            "load_failed": False,
            "node_origin": "ProjectedDestination",
            "overlay_state": "ProjectedDestination",
            "projected": True,
            "web_url": parent_data.get("web_url", ""),
        }

    def _build_destination_proposed_node_data(self, proposed_folder, parent_data):
        destination_path = self._canonical_destination_projection_path(proposed_folder.DestinationPath)
        return {
            "id": proposed_folder.DestinationId or f"proposed::{destination_path}",
            "name": proposed_folder.FolderName,
            "real_name": proposed_folder.FolderName,
            "display_path": destination_path,
            "item_path": destination_path,
            "destination_path": destination_path,
            "tree_role": "destination",
            "drive_id": parent_data.get("drive_id", ""),
            "site_id": parent_data.get("site_id", ""),
            "site_name": parent_data.get("site_name", ""),
            "library_id": parent_data.get("library_id", ""),
            "library_name": parent_data.get("library_name", ""),
            "is_folder": True,
            "children_loaded": True,
            "load_failed": False,
            "node_origin": "Proposed",
            "proposed": True,
            "web_url": parent_data.get("web_url", ""),
        }

    def _build_destination_allocation_node_data(self, move, parent_data):
        source_node = move.get("source", {})
        source_name = self._move_target_name(move) or source_node.get("name", "Allocated Item")
        destination_path = self._canonical_destination_projection_path(self._allocation_projection_path(move))
        is_folder = bool(source_node.get("is_folder", True))
        return {
            "id": f"allocated::{destination_path}",
            "name": source_name,
            "real_name": source_name,
            "display_path": destination_path,
            "item_path": destination_path,
            "destination_path": destination_path,
            "source_path": move.get("source_path", ""),
            "tree_role": "destination",
            "drive_id": parent_data.get("drive_id", ""),
            "site_id": parent_data.get("site_id", ""),
            "site_name": parent_data.get("site_name", ""),
            "library_id": parent_data.get("library_id", ""),
            "library_name": parent_data.get("library_name", ""),
            "is_folder": is_folder,
            "children_loaded": not is_folder,
            "load_failed": False,
            "node_origin": "PlannedAllocation",
            "overlay_state": "PlannedAllocation",
            "planned_allocation": True,
            "web_url": parent_data.get("web_url", ""),
        }

    def _build_destination_allocation_descendant_node_data(self, source_node_data, destination_semantic_path, parent_data):
        return {
            "id": f"allocated-descendant::{destination_semantic_path}",
            "name": source_node_data.get("name", ""),
            "real_name": source_node_data.get("real_name", source_node_data.get("name", "")),
            "display_path": destination_semantic_path,
            "item_path": destination_semantic_path,
            "destination_path": destination_semantic_path,
            "source_path": self._tree_item_path(source_node_data),
            "tree_role": "destination",
            "drive_id": parent_data.get("drive_id", ""),
            "site_id": parent_data.get("site_id", ""),
            "site_name": parent_data.get("site_name", ""),
            "library_id": parent_data.get("library_id", ""),
            "library_name": parent_data.get("library_name", ""),
            "is_folder": bool(source_node_data.get("is_folder", False)),
            # Descendant projection materializes the full nested subtree in one pass,
            # so projected descendant folders must not lazy-expand themselves again.
            "children_loaded": True,
            "load_failed": False,
            "node_origin": "ProjectedAllocationDescendant",
            "overlay_state": "PlannedAllocationDescendant",
            "planned_allocation_descendant": True,
            "web_url": parent_data.get("web_url", ""),
        }

    def _destination_preview_descendant_exists(self, model_nodes, semantic_path):
        existing = model_nodes.get(semantic_path)
        return existing is not None

    def _find_exact_planned_move_for_source_path(self, source_path):
        canonical_source_path = self._canonical_source_projection_path(source_path)
        if not canonical_source_path:
            return None
        for move in self.planned_moves:
            move_source_path = self._canonical_source_projection_path(move.get("source_path", ""))
            if move_source_path and move_source_path == canonical_source_path:
                return move
        return None

    def _find_inherited_planned_move_for_source_path(self, source_path):
        canonical_source_path = self._canonical_source_projection_path(source_path)
        if not canonical_source_path:
            return None
        inherited_move = None
        inherited_path_length = -1
        for move in self.planned_moves:
            move_source_path = self._canonical_source_projection_path(move.get("source_path", ""))
            if not move_source_path or move_source_path == canonical_source_path:
                continue
            if self._path_is_descendant(canonical_source_path, move_source_path, "source"):
                if len(move_source_path) > inherited_path_length:
                    inherited_move = move
                    inherited_path_length = len(move_source_path)
        return inherited_move

    def _source_subtree_fully_loaded_in_tree(self, item):
        """True when this node and all descendant folders have been expanded/loaded (no lazy placeholders)."""
        if item is None:
            return False
        data = item.data(0, Qt.UserRole) or {}
        if data.get("placeholder"):
            return False
        if not bool(data.get("is_folder", False)):
            return True
        if not bool(data.get("children_loaded")):
            return False
        for index in range(item.childCount()):
            if not self._source_subtree_fully_loaded_in_tree(item.child(index)):
                return False
        return True

    def _enrich_source_root_for_projection_graph_lookup(self, source_root_data, move):
        """
        Restored allocations often have SourcePath but empty source.id / source.drive_id.
        Resolve the folder via GET /drives/{id}/root:/path using the active source library drive.
        """
        out = dict(source_root_data or {})
        drive_id = str(out.get("drive_id", "") or "").strip()
        item_id = str(out.get("id", "") or "").strip()
        if drive_id and item_id:
            return out
        path = self._canonical_source_projection_path(
            self._tree_item_path(out) or (move or {}).get("source_path", "") or ""
        )
        fallback_drive = (
            drive_id
            or str(self.pending_root_drive_ids.get("source", "") or "").strip()
            or str(self._current_selected_source_drive_id() or "").strip()
        )
        if item_id and not drive_id and fallback_drive:
            out["drive_id"] = fallback_drive
            return out
        if not path or not fallback_drive:
            return out
        resolved_drive = drive_id or fallback_drive
        if item_id:
            out.setdefault("drive_id", resolved_drive)
            return out
        try:
            item = self.graph.get_drive_item_by_path(resolved_drive, path)
        except Exception:
            return out
        if not item or not item.get("id") or "folder" not in item:
            return out
        out["drive_id"] = resolved_drive
        out["id"] = str(item.get("id", "") or "")
        pr = item.get("parentReference") or {}
        if pr.get("driveId"):
            out.setdefault("library_id", str(pr.get("driveId", "")))
        try:
            out["item_path"] = self.graph.build_item_path(item, parent_item_path="/")
        except Exception:
            pass
        if move and isinstance(move.get("source"), dict):
            src = move["source"]
            src["id"] = out["id"]
            src["drive_id"] = out["drive_id"]
            if out.get("item_path"):
                src["item_path"] = out["item_path"]
        return out

    def _collect_source_descendants_for_projection(self, source_root_data, move=None):
        """
        Prefer walking the in-memory source tree when the allocation root is visible and fully loaded.
        Otherwise enumerate via Graph (slow on large folders) or partially-loaded tree as last resort.
        """
        if move is None:
            move = {"source": source_root_data, "source_path": self._tree_item_path(source_root_data)}
        source_root_data = self._enrich_source_root_for_projection_graph_lookup(
            dict(source_root_data or {}),
            move,
        )
        source_item = self._find_source_item_for_planned_move(move)
        base_diag = self._destination_projection_diag_payload(source_root_data, move, source_item)

        if source_item is not None and self._source_subtree_fully_loaded_in_tree(source_item):
            descendants = []
            for descendant_item in self._iter_tree_items(source_item):
                if descendant_item is source_item:
                    continue
                descendant_data = descendant_item.data(0, Qt.UserRole) or {}
                if descendant_data.get("placeholder"):
                    continue
                descendants.append(dict(descendant_data))
            self._log_destination_projection_collect_result(
                descendants,
                branch="tree_fully_loaded",
                graph_attempted=False,
                graph_error=None,
                **base_diag,
            )
            return descendants

        drive_id = source_root_data.get("drive_id", "")
        item_id = source_root_data.get("id", "")
        item_path = source_root_data.get("item_path", "")
        graph_attempted = False
        graph_error = None
        # Some saved move payloads omit item_path. Graph subtree enumeration
        # only needs drive_id + item_id; use a safe default when item_path
        # is missing so projected descendant expansion can resolve.
        if drive_id and item_id:
            graph_attempted = True
            try:
                descendants = self.graph.list_drive_subtree_items_normalized(
                    drive_id,
                    item_id,
                    site_id=source_root_data.get("site_id", ""),
                    site_name=source_root_data.get("site_name", ""),
                    library_id=source_root_data.get("library_id", drive_id),
                    library_name=source_root_data.get("library_name", ""),
                    tree_role="source",
                    parent_item_path=item_path or "/",
                )
                self._log_destination_projection_collect_result(
                    descendants,
                    branch="graph",
                    graph_attempted=True,
                    graph_error=None,
                    **base_diag,
                )
                return descendants
            except Exception as exc:
                graph_error = type(exc).__name__
                self._log_restore_exception("collect_source_descendants_for_projection", exc)
                self._log_restore_phase(
                    "destination_projection_graph_failed",
                    graph_error=graph_error,
                    graph_attempted=True,
                    **base_diag,
                )

        if source_item is None:
            source_item = self._find_source_item_for_planned_move(
                {"source": source_root_data, "source_path": self._tree_item_path(source_root_data)}
            )
            base_diag = self._destination_projection_diag_payload(source_root_data, move, source_item)

        if source_item is None:
            extra = {}
            if not graph_attempted and (not drive_id or not item_id):
                extra["graph_skip_reason"] = "missing_drive_id_or_item_id"
            self._log_destination_projection_collect_result(
                [],
                branch="no_source_item",
                graph_attempted=graph_attempted,
                graph_error=graph_error,
                **extra,
                **base_diag,
            )
            return []

        descendants = []
        for descendant_item in self._iter_tree_items(source_item):
            if descendant_item is source_item:
                continue
            descendant_data = descendant_item.data(0, Qt.UserRole) or {}
            if descendant_data.get("placeholder"):
                continue
            descendants.append(dict(descendant_data))
        self._log_destination_projection_collect_result(
            descendants,
            branch="partial_tree_walk",
            graph_attempted=graph_attempted,
            graph_error=graph_error,
            **base_diag,
        )
        return descendants

    def _try_add_single_allocation_descendant_to_future_model(
        self,
        move,
        model_nodes,
        *,
        allocation_path,
        allocation_node,
        allocation_destination_path,
        source_root_path,
        source_root_segments,
        descendant_data,
        log_added_count,
    ):
        """Insert one projected descendant row if applicable. Returns True when a node was added."""
        descendant_source_path = self._canonical_source_projection_path(self._tree_item_path(descendant_data))
        descendant_segments = self._path_segments(descendant_source_path)
        if descendant_source_path == source_root_path:
            return False

        relative_segments = []
        if source_root_path and descendant_source_path:
            prefix = source_root_path + "\\"
            prefix_slash = source_root_path + "/"
            if descendant_source_path.startswith(prefix):
                rel = descendant_source_path[len(source_root_path) :].lstrip("\\/")
                relative_segments = self._path_segments(rel)
            elif descendant_source_path.startswith(prefix_slash):
                rel = descendant_source_path[len(source_root_path) :].lstrip("\\/")
                relative_segments = self._path_segments(rel)
        if not relative_segments:
            if len(descendant_segments) <= len(source_root_segments):
                return False
            relative_segments = descendant_segments[len(source_root_segments) :]
        descendant_destination_path = self.normalize_memory_path(
            "\\".join([allocation_destination_path] + relative_segments)
        )
        exact_move = self._find_exact_planned_move_for_source_path(descendant_source_path)
        if exact_move is not None:
            exact_destination_path = self.normalize_memory_path(
                "\\".join([exact_move.get("destination_path", ""), self._move_target_name(exact_move)])
            )
            if (
                exact_destination_path
                and self._canonical_destination_projection_path(exact_destination_path)
                != self._canonical_destination_projection_path(descendant_destination_path)
            ):
                self._log_restore_phase(
                    "destination_allocation_descendant_duplicate_skipped",
                    allocation_destination_path=allocation_destination_path,
                    source_path=source_root_path,
                    descendant_source_path=descendant_source_path,
                    descendant_destination_path=descendant_destination_path,
                    is_folder=bool(descendant_data.get("is_folder", False)),
                    added_count=log_added_count,
                    reason="explicit_allocation_elsewhere",
                    verbose=True,
                )
                return False
        parent_destination_path = self.normalize_memory_path(
            "\\".join([allocation_destination_path] + relative_segments[:-1])
        )
        parent_node = model_nodes.get(parent_destination_path) or allocation_node
        parent_data = parent_node["data"] if parent_node else allocation_node["data"]
        descendant_node_data = self._build_destination_allocation_descendant_node_data(
            descendant_data,
            descendant_destination_path,
            parent_data,
        )

        if self._destination_preview_descendant_exists(model_nodes, descendant_destination_path):
            self._log_restore_phase(
                "destination_allocation_descendant_duplicate_skipped",
                allocation_destination_path=allocation_destination_path,
                source_path=source_root_path,
                descendant_source_path=descendant_source_path,
                descendant_destination_path=descendant_destination_path,
                is_folder=bool(descendant_data.get("is_folder", False)),
                added_count=log_added_count,
                verbose=True,
            )
            return False

        self._upsert_destination_model_node(
            model_nodes,
            descendant_destination_path,
            name=descendant_node_data.get("name", ""),
            node_state="projected_descendant",
            data=descendant_node_data,
            parent_semantic_path=parent_destination_path,
        )
        self._attach_destination_model_child(model_nodes, parent_destination_path, descendant_destination_path)
        return True

    def _project_source_descendants_into_destination_model(self, move, allocation_path, model_nodes):
        source_path = self._canonical_source_projection_path(move.get("source_path", ""))
        if not source_path:
            return 0

        source_item = self._find_source_item_for_planned_move(move)
        source_root_data = source_item.data(0, Qt.UserRole) or {} if source_item is not None else {}
        if not source_root_data:
            source_root_data = dict(move.get("source", {}) or {})
            source_root_data.setdefault("item_path", move.get("source_path", ""))
            source_root_data.setdefault("display_path", move.get("source_path", ""))

        if not source_root_data:
            self._log_restore_phase(
                "destination_allocation_descendant_projection_complete",
                allocation_destination_path=self._canonical_destination_projection_path(allocation_path),
                source_path=source_path,
                added_count=0,
                reason="source_item_not_found",
            )
            return 0

        if not bool(source_root_data.get("is_folder", False)):
            self._log_restore_phase(
                "destination_allocation_descendant_projection_complete",
                allocation_destination_path=self._canonical_destination_projection_path(allocation_path),
                source_path=source_path,
                added_count=0,
                reason="source_item_not_folder",
            )
            return 0

        allocation_node = model_nodes.get(allocation_path)
        if allocation_node is None:
            self._log_restore_phase(
                "destination_allocation_descendant_projection_complete",
                allocation_destination_path=self._canonical_destination_projection_path(allocation_path),
                source_path=source_path,
                added_count=0,
                reason="allocation_node_missing",
            )
            return 0

        allocation_destination_path = self._canonical_destination_projection_path(allocation_path)
        source_root_path = self._canonical_source_projection_path(self._tree_item_path(source_root_data))
        source_root_segments = self._path_segments(source_root_path)
        added_count = 0
        self._log_restore_phase(
            "destination_allocation_descendant_projection_started",
            allocation_destination_path=allocation_destination_path,
            source_path=source_root_path,
            added_count=0,
        )

        for descendant_data in self._collect_source_descendants_for_projection(source_root_data, move):
            if self._try_add_single_allocation_descendant_to_future_model(
                move,
                model_nodes,
                allocation_path=allocation_path,
                allocation_node=allocation_node,
                allocation_destination_path=allocation_destination_path,
                source_root_path=source_root_path,
                source_root_segments=source_root_segments,
                descendant_data=descendant_data,
                log_added_count=added_count,
            ):
                added_count += 1

        self._log_restore_phase(
            "destination_allocation_descendant_projection_complete",
            allocation_destination_path=allocation_destination_path,
            source_path=source_root_path,
            added_count=added_count,
        )
        return added_count

    def _start_allocation_descendant_projection_chunk(self, move, allocation_path, model_nodes):
        """Prepare one allocation's descendant list for chunked UI-thread processing. Returns None to skip."""
        source_path = self._canonical_source_projection_path(move.get("source_path", ""))
        if not source_path:
            return None

        source_item = self._find_source_item_for_planned_move(move)
        source_root_data = source_item.data(0, Qt.UserRole) or {} if source_item is not None else {}
        if not source_root_data:
            source_root_data = dict(move.get("source", {}) or {})
            source_root_data.setdefault("item_path", move.get("source_path", ""))
            source_root_data.setdefault("display_path", move.get("source_path", ""))

        if not source_root_data:
            self._log_restore_phase(
                "destination_allocation_descendant_projection_complete",
                allocation_destination_path=self._canonical_destination_projection_path(allocation_path),
                source_path=source_path,
                added_count=0,
                reason="source_item_not_found",
            )
            return None

        if not bool(source_root_data.get("is_folder", False)):
            self._log_restore_phase(
                "destination_allocation_descendant_projection_complete",
                allocation_destination_path=self._canonical_destination_projection_path(allocation_path),
                source_path=source_path,
                added_count=0,
                reason="source_item_not_folder",
            )
            return None

        allocation_node = model_nodes.get(allocation_path)
        if allocation_node is None:
            self._log_restore_phase(
                "destination_allocation_descendant_projection_complete",
                allocation_destination_path=self._canonical_destination_projection_path(allocation_path),
                source_path=source_path,
                added_count=0,
                reason="allocation_node_missing",
            )
            return None

        allocation_destination_path = self._canonical_destination_projection_path(allocation_path)
        source_root_path = self._canonical_source_projection_path(self._tree_item_path(source_root_data))
        source_root_segments = self._path_segments(source_root_path)
        descendants = self._collect_source_descendants_for_projection(source_root_data, move)
        self._log_restore_phase(
            "destination_allocation_descendant_projection_started",
            allocation_destination_path=allocation_destination_path,
            source_path=source_root_path,
            added_count=0,
        )
        return {
            "move": move,
            "allocation_path": allocation_path,
            "allocation_node": allocation_node,
            "allocation_destination_path": allocation_destination_path,
            "source_root_path": source_root_path,
            "source_root_segments": source_root_segments,
            "descendants": descendants,
            "added_count": 0,
        }

    def _cancel_destination_future_async_projection(self, reason=""):
        had = getattr(self, "_destination_future_projection_async_state", None) is not None
        self._destination_future_projection_async_state = None
        timer = getattr(self, "_destination_future_projection_timer", None)
        if timer is not None:
            timer.stop()
        if had:
            if reason:
                self._log_restore_phase(
                    "destination_future_projection_async_cancelled",
                    reason=reason,
                )
            self._set_tree_status_message("destination", "", loading=False)

    def _should_chunk_destination_future_projection(self):
        for move in self.planned_moves:
            src = move.get("source") or {}
            if bool(src.get("is_folder", True)):
                return True
        return False

    def _finish_destination_future_async_projection(self, state):
        proj_t0 = float(state.get("projection_wall_t0") or time.perf_counter())
        self._destination_future_projection_async_state = None
        reason = state.get("reason", "background_projection")
        model = self._package_destination_future_model(
            state["model_nodes"],
            state["real_child_counts"],
            total_real_nodes=state["total_real_nodes"],
            total_proposed_nodes=state["total_proposed_nodes"],
            total_allocation_nodes=state["total_allocation_nodes"],
        )
        self._log_restore_phase(
            "destination_future_model_materialize_started",
            reason=reason,
            materialize_phase="background_incremental_merge",
            root_path=model.get("root_path", "Root"),
            visible_future_branch_count=len(model.get("nodes", {})),
        )
        visible_future_branch_count = self._incremental_merge_destination_future_projection(
            model,
            state.get("fast_paint_paths") or frozenset(),
        )
        applied_count = visible_future_branch_count
        self._log_restore_phase(
            "destination_future_model_materialize_applied",
            reason=reason,
            materialize_phase="background_incremental_merge",
            root_path=model.get("root_path", "Root"),
            applied_count=applied_count,
            visible_future_branch_count=visible_future_branch_count,
        )
        proj_wall_ms = round((time.perf_counter() - proj_t0) * 1000.0, 1)
        self._log_restore_phase(
            "destination_future_model_materialize_complete",
            reason=reason,
            materialize_phase="background_incremental_merge",
            root_path=model.get("root_path", "Root"),
            applied_count=applied_count,
            visible_future_branch_count=visible_future_branch_count,
            wall_duration_ms=proj_wall_ms,
            planned_moves_count=len(state.get("moves") or []),
        )
        self._log_restore_phase(
            "destination_visible_future_branch_count",
            reason=reason,
            materialize_phase="background_incremental_merge",
            root_path=model.get("root_path", "Root"),
            visible_future_branch_count=visible_future_branch_count,
            wall_duration_ms=proj_wall_ms,
        )
        self._set_tree_status_message("destination", "Destination preview ready.", loading=False)
        self._destination_future_model_pending_after_source_restore = False

    def _run_destination_future_projection_chunk(self):
        state = self._destination_future_projection_async_state
        if state is None:
            return
        moves = state.get("moves") or []
        move_index = int(state.get("move_index", 0))
        total_moves = len(moves)
        if total_moves > 0:
            display_n = min(move_index + 1, total_moves)
            status_msg = f"Building destination preview… ({display_n}/{total_moves})"
        else:
            status_msg = "Building destination preview…"
        self._set_tree_status_message("destination", status_msg, loading=True)
        timer = QElapsedTimer()
        timer.start()
        ms_budget = 12
        max_items = 280
        items_this_tick = 0
        while True:
            if state["move_index"] >= len(state["moves"]):
                self._finish_destination_future_async_projection(state)
                return
            chunk = state.get("current_chunk")
            if chunk is None:
                move = state["moves"][state["move_index"]]
                allocation_path = self._canonical_destination_projection_path(self._allocation_projection_path(move))
                chunk = self._start_allocation_descendant_projection_chunk(move, allocation_path, state["model_nodes"])
                state["current_chunk"] = chunk
                state["descendant_index"] = 0
                if chunk is None:
                    state["move_index"] += 1
                    state["current_chunk"] = None
                    continue
                chunk["added_count"] = 0

            descendants = chunk["descendants"]
            i = state["descendant_index"]
            move_added = chunk["added_count"]
            while i < len(descendants):
                if items_this_tick >= max_items or timer.elapsed() > ms_budget:
                    state["descendant_index"] = i
                    chunk["added_count"] = move_added
                    self._destination_future_projection_timer.start(0)
                    return
                descendant_data = descendants[i]
                if self._try_add_single_allocation_descendant_to_future_model(
                    chunk["move"],
                    state["model_nodes"],
                    allocation_path=chunk["allocation_path"],
                    allocation_node=chunk["allocation_node"],
                    allocation_destination_path=chunk["allocation_destination_path"],
                    source_root_path=chunk["source_root_path"],
                    source_root_segments=chunk["source_root_segments"],
                    descendant_data=descendant_data,
                    log_added_count=move_added,
                ):
                    move_added += 1
                i += 1
                items_this_tick += 1

            self._log_restore_phase(
                "destination_allocation_descendant_projection_complete",
                allocation_destination_path=chunk["allocation_destination_path"],
                source_path=chunk["source_root_path"],
                added_count=move_added,
            )
            state["move_index"] += 1
            state["current_chunk"] = None
            state["descendant_index"] = 0

    def _import_destination_real_snapshot(self, snapshot_node, model_nodes, parent_semantic_path=""):
        semantic_path = snapshot_node["semantic_path"]
        parent_semantic_path = snapshot_node.get("parent_semantic_path", parent_semantic_path)
        data = dict(snapshot_node["data"])
        name = data.get("name") or self._path_segments(semantic_path)[-1]
        self._log_restore_phase(
            "destination_real_child_preserved",
            parent_path=parent_semantic_path,
            real_child_count=1,
            projected_child_count=0,
            merged_child_count=1,
            replaced_incorrectly=False,
            verbose=True,
        )
        self._upsert_destination_model_node(
            model_nodes,
            semantic_path,
            name=name,
            node_state="real",
            data=data,
            parent_semantic_path=parent_semantic_path,
        )
        if parent_semantic_path:
            self._attach_destination_model_child(model_nodes, parent_semantic_path, semantic_path)
        self._log_restore_phase(
            "destination_future_model_real_node_added",
            semantic_path=semantic_path,
            node_origin=data.get("node_origin", "Real"),
            parent_semantic_path=parent_semantic_path,
            verbose=True,
        )
        for child_snapshot in snapshot_node["children"]:
            self._import_destination_real_snapshot(child_snapshot, model_nodes, semantic_path)

    def _record_destination_future_branch(self, branches, branch_path, node_state):
        normalized_branch = self._canonical_destination_projection_path(branch_path)
        if not normalized_branch:
            return
        current_state = branches.get(normalized_branch)
        if current_state is None or self._destination_future_state_rank(node_state) > self._destination_future_state_rank(current_state):
            branches[normalized_branch] = node_state

    def _package_destination_future_model(
        self,
        model_nodes,
        real_child_counts,
        *,
        total_real_nodes,
        total_proposed_nodes,
        total_allocation_nodes,
    ):
        state_counts = Counter(node["node_state"] for node in model_nodes.values())
        self._log_restore_phase(
            "destination_future_model_branch_summary",
            total_branches=len(model_nodes),
            node_state_counts=dict(state_counts),
        )

        merge_summary_max_merged = 0
        merge_summary_total_projected = 0
        merge_summary_regression_count = 0
        for parent_path, real_child_count in real_child_counts.items():
            child_paths = [
                child_path
                for child_path, child_node in model_nodes.items()
                if child_node.get("parent_semantic_path", "") == parent_path
            ]
            projected_child_count = sum(
                1 for child_path in child_paths
                if model_nodes[child_path]["node_state"] != "real"
            )
            merged_child_count = len(child_paths)
            merge_summary_max_merged = max(merge_summary_max_merged, merged_child_count)
            merge_summary_total_projected += projected_child_count
            if merged_child_count < real_child_count:
                merge_summary_regression_count += 1
                self._log_restore_phase(
                    "destination_real_children_lost_regression",
                    parent_path=parent_path,
                    real_child_count=real_child_count,
                    projected_child_count=projected_child_count,
                    merged_child_count=merged_child_count,
                    replaced_incorrectly=True,
                )
        self._log_restore_phase(
            "destination_real_children_merge_summary",
            parent_folder_count=len(real_child_counts),
            largest_merged_child_count=merge_summary_max_merged,
            total_projected_child_nodes_across_parents=merge_summary_total_projected,
            regression_parent_count=merge_summary_regression_count,
        )

        total_overlay_nodes = sum(1 for node in model_nodes.values() if node["node_state"] != "real")
        self._log_restore_phase(
            "destination_projection_overlay_applied",
            parent_path=model_nodes.get("Root", {}).get("semantic_path", "Root"),
            real_child_count=total_real_nodes,
            projected_child_count=total_overlay_nodes,
            merged_child_count=len(model_nodes),
            replaced_incorrectly=False,
        )

        for node in model_nodes.values():
            node["children"] = []
        for semantic_path, node in model_nodes.items():
            parent_semantic_path = node.get("parent_semantic_path", "")
            if parent_semantic_path and parent_semantic_path in model_nodes:
                parent_node = model_nodes[parent_semantic_path]
                if semantic_path not in parent_node["children"]:
                    parent_node["children"].append(semantic_path)

        top_level_paths = [path for path, node in model_nodes.items() if not node["parent_semantic_path"]]
        top_level_paths.sort(key=lambda value: ([segment.lower() for segment in self._path_segments(value)],))
        root_path = top_level_paths[0] if top_level_paths else "Root"
        self._log_restore_phase(
            "destination_future_model_merge_complete",
            root_path=root_path,
            total_real_nodes=total_real_nodes,
            total_proposed_nodes=total_proposed_nodes,
            total_allocation_nodes=total_allocation_nodes,
        )
        self._log_restore_phase(
            "destination_future_model_built",
            root_path=root_path,
            visible_future_branch_count=len(model_nodes),
            planned_moves_count=len(self.planned_moves),
            proposed_folders_count=len(self.proposed_folders),
        )
        return {
            "nodes": model_nodes,
            "top_level_paths": top_level_paths,
            "root_path": root_path,
            "total_real_nodes": total_real_nodes,
            "total_proposed_nodes": total_proposed_nodes,
            "total_allocation_nodes": total_allocation_nodes,
        }

    def _build_destination_future_model(self, *, skip_allocation_descendants=False):
        self._log_restore_phase(
            "destination_future_model_build_started",
            planned_moves_count=len(self.planned_moves),
            proposed_folders_count=len(self.proposed_folders),
            skip_allocation_descendants=bool(skip_allocation_descendants),
        )
        model_nodes = {}
        total_real_nodes = 0
        total_proposed_nodes = 0
        total_allocation_nodes = 0

        root_data = self._destination_root_base_data()
        self._upsert_destination_model_node(
            model_nodes,
            "Root",
            name="Root",
            node_state="real" if not self._destination_is_future_state_node(root_data) else "projected",
            data=root_data,
            parent_semantic_path="",
        )

        self._refresh_destination_real_tree_snapshot()
        real_child_counts = {}
        for snapshot_node in self._destination_real_tree_snapshot:
            parent_path = snapshot_node.get("parent_semantic_path", "")
            real_child_counts[parent_path] = real_child_counts.get(parent_path, 0) + 1
            self._import_destination_real_snapshot(snapshot_node, model_nodes)
            total_real_nodes += 1

        ensured_root_child_count = self._ensure_visible_destination_root_children_in_model(model_nodes)
        if ensured_root_child_count:
            self._log_restore_phase(
                "destination_visible_root_children_preserved",
                preserved_count=ensured_root_child_count,
            )

        for proposed_folder in self.proposed_folders:
            destination_path = self._canonical_destination_projection_path(proposed_folder.DestinationPath)
            parent_path = self.normalize_memory_path("\\".join(self._path_segments(destination_path)[:-1]))
            parent_semantic_path = ""
            for prefix in self._destination_projection_prefixes(parent_path):
                prefix_name = self._path_segments(prefix)[-1]
                parent_data = model_nodes.get(parent_semantic_path or prefix, {}).get("data", {}) if parent_semantic_path else model_nodes["Root"]["data"]
                projected_data = self._build_destination_projected_node_data(prefix, prefix_name, parent_data)
                self._upsert_destination_model_node(
                    model_nodes,
                    prefix,
                    name=prefix_name,
                    node_state="projected",
                    data=projected_data,
                    parent_semantic_path=parent_semantic_path,
                )
                if parent_semantic_path:
                    self._attach_destination_model_child(model_nodes, parent_semantic_path, prefix)
                parent_semantic_path = prefix

            parent_node = model_nodes.get(parent_path) or model_nodes.get("Root")
            parent_data = parent_node["data"] if parent_node else {}
            proposed_data = self._build_destination_proposed_node_data(proposed_folder, parent_data)
            self._upsert_destination_model_node(
                model_nodes,
                destination_path,
                name=proposed_folder.FolderName,
                node_state="proposed",
                data=proposed_data,
                parent_semantic_path=parent_path,
            )
            if parent_path:
                self._attach_destination_model_child(model_nodes, parent_path, destination_path)
            total_proposed_nodes += 1
            self._log_restore_phase(
                "destination_future_model_proposed_node_added",
                semantic_path=destination_path,
                node_origin="Proposed",
                parent_semantic_path=parent_path,
            )

        for move in self.planned_moves:
            parent_path = self._canonical_destination_projection_path(self._allocation_parent_path(move))
            allocation_path = self._canonical_destination_projection_path(self._allocation_projection_path(move))
            parent_semantic_path = ""
            for prefix in self._destination_projection_prefixes(parent_path):
                prefix_name = self._path_segments(prefix)[-1]
                parent_data = model_nodes.get(parent_semantic_path or prefix, {}).get("data", {}) if parent_semantic_path else model_nodes["Root"]["data"]
                projected_data = self._build_destination_projected_node_data(prefix, prefix_name, parent_data)
                self._upsert_destination_model_node(
                    model_nodes,
                    prefix,
                    name=prefix_name,
                    node_state="projected",
                    data=projected_data,
                    parent_semantic_path=parent_semantic_path,
                )
                if parent_semantic_path:
                    self._attach_destination_model_child(model_nodes, parent_semantic_path, prefix)
                parent_semantic_path = prefix

            parent_node = model_nodes.get(parent_path) or model_nodes.get("Root")
            parent_data = parent_node["data"] if parent_node else {}
            allocation_data = self._build_destination_allocation_node_data(move, parent_data)
            source_name = allocation_data.get("name", "Allocated Item")
            self._upsert_destination_model_node(
                model_nodes,
                allocation_path,
                name=source_name,
                node_state="allocated",
                data=allocation_data,
                parent_semantic_path=parent_path,
            )
            if parent_path:
                self._attach_destination_model_child(model_nodes, parent_path, allocation_path)
            total_allocation_nodes += 1
            self._log_restore_phase(
                "destination_future_model_allocation_node_added",
                semantic_path=allocation_path,
                node_origin="PlannedAllocation",
                parent_semantic_path=parent_path,
            )
            if not skip_allocation_descendants:
                self._project_source_descendants_into_destination_model(move, allocation_path, model_nodes)

        payload = self._package_destination_future_model(
            model_nodes,
            real_child_counts,
            total_real_nodes=total_real_nodes,
            total_proposed_nodes=total_proposed_nodes,
            total_allocation_nodes=total_allocation_nodes,
        )
        payload["_real_child_counts"] = real_child_counts
        return payload

    def _sort_destination_future_children(self, parent_item):
        if parent_item is None or parent_item.childCount() <= 1:
            return
        children = [parent_item.takeChild(0) for _ in range(parent_item.childCount())]
        children.sort(
            key=lambda child: (
                not bool((child.data(0, Qt.UserRole) or {}).get("is_folder", False)),
                str((child.data(0, Qt.UserRole) or {}).get("name", child.text(0))).lower(),
            )
        )
        for child in children:
            parent_item.addChild(child)
            self._sort_destination_future_children(child)

    def _sort_destination_future_tree(self):
        tree = getattr(self, "destination_tree_widget", None)
        if tree is None:
            return
        for index in range(tree.topLevelItemCount()):
            self._sort_destination_future_children(tree.topLevelItem(index))

    def _collect_expanded_destination_paths(self):
        tree = getattr(self, "destination_tree_widget", None)
        if tree is None:
            return set()

        expanded_paths = set()

        def visit(item):
            if item is None:
                return
            node_data = item.data(0, Qt.UserRole) or {}
            semantic_path = self._destination_semantic_path(node_data)
            if semantic_path and item.isExpanded():
                expanded_paths.add(semantic_path)
                key = self._destination_expansion_state_key(semantic_path)
                if key:
                    expanded_paths.add(key)
            for index in range(item.childCount()):
                visit(item.child(index))

        for index in range(tree.topLevelItemCount()):
            visit(tree.topLevelItem(index))

        return expanded_paths

    def _collect_expanded_tree_paths(self, panel_key):
        if panel_key == "destination":
            return self._collect_expanded_destination_paths()

        tree = getattr(self, "source_tree_widget", None)
        if tree is None:
            return set()

        expanded_paths = set()

        def visit(item):
            if item is None:
                return
            node_data = item.data(0, Qt.UserRole) or {}
            source_path = self._canonical_source_projection_path(self._tree_item_path(node_data))
            if source_path and item.isExpanded():
                expanded_paths.add(source_path)
            for index in range(item.childCount()):
                visit(item.child(index))

        for index in range(tree.topLevelItemCount()):
            visit(tree.topLevelItem(index))

        return expanded_paths

    def _restore_expanded_destination_paths(self, expanded_paths):
        tree = getattr(self, "destination_tree_widget", None)
        if tree is None:
            return

        target_paths = {path for path in expanded_paths if path}
        target_paths.add("Root")
        normalized_targets = set(target_paths)
        for path in list(target_paths):
            k = self._destination_expansion_state_key(path)
            if k:
                normalized_targets.add(k)

        all_items = []

        def visit(item):
            if item is None:
                return
            all_items.append(item)
            for index in range(item.childCount()):
                visit(item.child(index))

        for index in range(tree.topLevelItemCount()):
            visit(tree.topLevelItem(index))

        all_items.sort(
            key=lambda item: len(self._path_segments(self._tree_item_path(item.data(0, Qt.UserRole) or {})))
        )

        for i, item in enumerate(all_items):
            if i > 0 and i % 80 == 0:
                QApplication.processEvents(QEventLoop.ProcessEventsFlag.ExcludeUserInputEvents)
            node_data = item.data(0, Qt.UserRole) or {}
            semantic_path = self._destination_semantic_path(node_data)
            item_keys = {semantic_path}
            sk = self._destination_expansion_state_key(semantic_path)
            if sk:
                item_keys.add(sk)
            if item_keys & normalized_targets:
                tree.expandItem(item)

    def _restore_expanded_tree_paths(self, panel_key, expanded_paths):
        if panel_key == "destination":
            self._restore_expanded_destination_paths(expanded_paths)
            return

        tree = getattr(self, "source_tree_widget", None)
        if tree is None:
            return

        targets = {path for path in expanded_paths if path}
        for index in range(tree.topLevelItemCount()):
            for item in self._iter_tree_items(tree.topLevelItem(index)):
                node_data = item.data(0, Qt.UserRole) or {}
                source_path = self._canonical_source_projection_path(self._tree_item_path(node_data))
                if source_path in targets:
                    tree.expandItem(item)

    def _collect_selected_tree_path(self, panel_key):
        node_data = self.get_selected_tree_node_data(panel_key)
        if not node_data:
            return ""
        if panel_key == "destination":
            return self._destination_semantic_path(node_data)
        return self._canonical_source_projection_path(self._tree_item_path(node_data))

    def _restore_selected_tree_path(self, panel_key, selected_path):
        if not selected_path:
            return

        if panel_key == "destination":
            tree = getattr(self, "destination_tree_widget", None)
            item = self._find_visible_destination_item_by_path(selected_path)
        else:
            tree = getattr(self, "source_tree_widget", None)
            item = self._find_visible_source_item_by_path(selected_path)

        if tree is None or item is None:
            return

        tree.setCurrentItem(item)
        item.setSelected(True)

    def _capture_workspace_tree_state(self):
        return {
            "source_expanded_paths": self._collect_expanded_tree_paths("source"),
            "destination_expanded_paths": self._collect_expanded_tree_paths("destination"),
            "source_selected_path": self._collect_selected_tree_path("source"),
            "destination_selected_path": self._collect_selected_tree_path("destination"),
        }

    def _restore_workspace_tree_state(self, ui_state):
        if not ui_state:
            return
        self._restore_expanded_tree_paths("source", ui_state.get("source_expanded_paths", set()))
        self._restore_selected_tree_path("source", ui_state.get("source_selected_path", ""))
        self._restore_expanded_tree_paths("destination", ui_state.get("destination_expanded_paths", set()))
        self._restore_selected_tree_path("destination", ui_state.get("destination_selected_path", ""))

    def _destination_model_sort_key(self, model_nodes, semantic_path):
        node = model_nodes[semantic_path]
        data = node["data"]
        return (
            not bool(data.get("is_folder", False)),
            self._destination_future_state_rank(node["node_state"]),
            str(node["name"]).lower(),
        )

    def _build_destination_tree_item_from_future_model(self, model_nodes, semantic_path):
        self._future_bind_nodes_built = getattr(self, "_future_bind_nodes_built", 0) + 1
        if self._future_bind_nodes_built % 300 == 0:
            # Keep the UI interactive while expand-all is running so scrolling and clicks
            # do not feel frozen during long lazy-load traversals.
            QApplication.processEvents()

        node = model_nodes[semantic_path]
        data = dict(node["data"])
        name = data.get("name") or node["name"] or self._path_segments(semantic_path)[-1]
        is_folder = bool(data.get("is_folder", False))
        node_state = node["node_state"]
        # Tree items only see `data`, not the outer model node. Sync future-state flags so
        # indicators, `_destination_node_state`, and `node_is_planned_allocation` match `node_state`.
        if node_state == "allocated":
            data.setdefault("planned_allocation", True)
        elif node_state == "proposed":
            data.setdefault("proposed", True)
        destination_full_tree_ready = self._destination_full_tree_ready()

        if node_state == "allocated":
            prefix = "Folder" if is_folder else "File"
            label = f"{prefix}: {name} [Allocated]"
        elif node_state == "proposed":
            label = f"Folder: {name} (Proposed)"
        elif node_state == "projected_descendant":
            prefix = "Folder" if is_folder else "File"
            label = f"{prefix}: {name}"
        else:
            prefix = "Folder" if is_folder else "File"
            label = f"{prefix}: {name}"

        item = QTreeWidgetItem([label])
        data.setdefault("tree_role", "destination")
        data.setdefault("name", name)
        data.setdefault("real_name", name)
        data.setdefault("display_path", semantic_path)
        data.setdefault("item_path", semantic_path)
        data.setdefault("destination_path", semantic_path)
        data.setdefault("base_display_label", label)
        item.setData(0, Qt.UserRole, data)
        self._apply_tree_item_visual_state(item, data)

        child_paths = sorted(node["children"], key=lambda child_path: self._destination_model_sort_key(model_nodes, child_path))
        for child_path in child_paths:
            item.addChild(self._build_destination_tree_item_from_future_model(model_nodes, child_path))

        if is_folder:
            if item.childCount() > 0:
                item.setChildIndicatorPolicy(QTreeWidgetItem.ShowIndicator)
            elif (
                node_state == "real"
                and destination_full_tree_ready
                and bool(data.get("children_loaded"))
            ):
                item.setChildIndicatorPolicy(QTreeWidgetItem.ShowIndicator)
                item.addChild(
                    self.build_loading_placeholder_item("This folder is empty.", role="terminal_empty")
                )
            elif (
                node_state == "real"
                and not destination_full_tree_ready
                and not bool(data.get("children_loaded"))
                and not bool(data.get("load_failed"))
                and data.get("id")
            ):
                item.setChildIndicatorPolicy(QTreeWidgetItem.ShowIndicator)
                item.addChild(
                    self.build_loading_placeholder_item("Expand to load contents", role="lazy_unloaded")
                )
            else:
                should_show_for_allocation = bool(
                    node_state in ("allocated", "proposed", "projected_descendant")
                    or data.get("planned_allocation")
                    or (
                        data.get("planned_allocation_descendant")
                        and bool(data.get("is_folder"))
                    )
                )
                item.setChildIndicatorPolicy(
                    QTreeWidgetItem.ShowIndicator
                    if (self._destination_has_future_descendants(data) or should_show_for_allocation)
                    else QTreeWidgetItem.DontShowIndicator
                )
        else:
            item.setChildIndicatorPolicy(QTreeWidgetItem.DontShowIndicator)
        return item

    def _destination_future_tree_bind_busy(self):
        if getattr(self, "_destination_chunked_bind_state", None) is not None:
            return True
        return bool(getattr(self, "_destination_future_bind_sync_active", False))

    def _queue_deferred_destination_library_root(self, site, library, force_refresh):
        prev = getattr(self, "_pending_destination_library_root", None)
        force_refresh = bool(force_refresh) or (
            isinstance(prev, dict) and bool(prev.get("force_refresh"))
        )
        self._pending_destination_library_root = {
            "site": dict(site) if isinstance(site, dict) else site,
            "library": dict(library) if isinstance(library, dict) else library,
            "force_refresh": force_refresh,
        }
        st = getattr(self, "_destination_chunked_bind_state", None)
        deferred_reason = "sync_bind"
        chunked_phase = None
        bind_generation = None
        model_node_count = 0
        if isinstance(st, dict):
            deferred_reason = "chunked_async"
            chunked_phase = st.get("phase")
            bind_generation = st.get("gen")
            m = st.get("model") or {}
            model_node_count = len(m.get("nodes") or {})
        self._log_restore_phase(
            "destination_root_load_deferred_during_future_bind",
            deferred_reason=deferred_reason,
            chunked_phase=chunked_phase,
            bind_generation=bind_generation,
            model_node_count=model_node_count,
            force_refresh=force_refresh,
            drive_id=(library or {}).get("id", "") if isinstance(library, dict) else "",
            superseded_previous_pending=bool(prev),
        )

    def _flush_pending_destination_library_root_if_any(self, *, flush_reason="future_bind_complete"):
        pending = getattr(self, "_pending_destination_library_root", None)
        if not isinstance(pending, dict):
            return
        self._pending_destination_library_root = None
        site = pending.get("site")
        library = pending.get("library")
        force_refresh = bool(pending.get("force_refresh"))
        drive_id = library.get("id", "") if isinstance(library, dict) else ""
        self._log_restore_phase(
            "destination_root_load_flush_after_future_bind",
            flush_reason=flush_reason,
            drive_id=drive_id,
            force_refresh=force_refresh,
        )
        try:
            self.load_library_root("destination", site, library, force_refresh=force_refresh)
        except Exception as exc:
            self._log_restore_exception("destination_root_load_flush_after_future_bind", exc)

    def _destination_chunked_bind_node_threshold(self):
        return 550

    def _destination_future_bind_should_chunk_async(self, model, *, on_complete):
        """Use QTimer-sliced bind when the unique-path map is large or allocation merge will be heavy.

        A small ``nodes`` dict (e.g. 169 paths) can still expand into thousands of tree rows and
        minutes of synchronous ``_apply_visible_destination_allocation_descendants`` work.
        """
        nodes = model.get("nodes") or {}
        n_paths = len(nodes)
        thr = self._destination_chunked_bind_node_threshold()
        if n_paths >= thr:
            return True
        if on_complete is None:
            return False
        alloc = int(model.get("total_allocation_nodes", 0) or 0)
        if n_paths >= 100 and alloc >= 20:
            return True
        if n_paths >= 60 and alloc >= 45:
            return True
        return False

    def _bind_destination_future_model_sync(self, model):
        tree = getattr(self, "destination_tree_widget", None)
        if tree is None:
            return 0, 0

        model_nodes = model.get("nodes", {})
        top_level_paths = model.get("top_level_paths", [])
        node_count = len(model_nodes)
        t0 = time.perf_counter()
        self._destination_future_bind_sync_active = True
        self._log_restore_phase(
            "destination_future_model_bind_started",
            root_path=model.get("root_path", "Root"),
            top_level_count=len(top_level_paths),
            bind_mode="sync",
            model_node_count=node_count,
        )

        ui_state = self._capture_workspace_tree_state()
        destination_expanded_paths = set(ui_state.get("destination_expanded_paths", set()) or set())
        destination_selected_path = str(ui_state.get("destination_selected_path", "") or "")

        try:
            tree.blockSignals(True)
            try:
                tree.clear()
                self._future_bind_nodes_built = 0
                for semantic_path in top_level_paths:
                    tree.addTopLevelItem(self._build_destination_tree_item_from_future_model(model_nodes, semantic_path))

                visible_future_branch_count = 0
                for _semantic_path, node in model_nodes.items():
                    if node.get("node_state") != "real":
                        visible_future_branch_count += 1

                for index in range(tree.topLevelItemCount()):
                    top_item = tree.topLevelItem(index)
                    self._refresh_destination_item_visibility(top_item, expand=True)

                visible_descendant_count = self._apply_visible_destination_allocation_descendants(
                    destination_expanded_paths=destination_expanded_paths
                )
                self._refresh_destination_tree_indicators()
                self._restore_expanded_tree_paths("destination", destination_expanded_paths)
                self._restore_selected_tree_path("destination", destination_selected_path)
                self._refresh_expand_all_button_for_panel("destination")

                wall_ms = round((time.perf_counter() - t0) * 1000.0, 1)
                self._log_restore_phase(
                    "destination_future_model_bind_complete",
                    root_path=model.get("root_path", "Root"),
                    top_level_count=tree.topLevelItemCount(),
                    visible_descendant_count=visible_descendant_count,
                    bind_mode="sync",
                    wall_duration_ms=wall_ms,
                    model_node_count=node_count,
                )
                self._log_restore_phase(
                    "destination_future_model_visible_summary",
                    root_path=model.get("root_path", "Root"),
                    total_real_nodes=model.get("total_real_nodes", 0),
                    total_proposed_nodes=model.get("total_proposed_nodes", 0),
                    total_allocation_nodes=model.get("total_allocation_nodes", 0),
                    top_level_count=tree.topLevelItemCount(),
                    visible_future_branch_count=visible_future_branch_count,
                    bind_mode="sync",
                    wall_duration_ms=wall_ms,
                    model_node_count=node_count,
                )
                self._schedule_workspace_ui_persist(panel_key="destination")
                return visible_future_branch_count, visible_descendant_count
            finally:
                tree.blockSignals(False)
        except Exception as exc:
            self._log_restore_phase(
                "destination_future_model_bind_failed",
                bind_mode="sync",
                model_node_count=node_count,
                wall_duration_ms=round((time.perf_counter() - t0) * 1000.0, 1),
                error_type=type(exc).__name__,
            )
            raise
        finally:
            self._destination_future_bind_sync_active = False

    def _schedule_chunked_destination_future_bind(self, model, on_complete):
        self._destination_future_bind_generation = int(getattr(self, "_destination_future_bind_generation", 0)) + 1
        gen = self._destination_future_bind_generation
        nodes = (model.get("nodes") or {}) if isinstance(model, dict) else {}
        node_count = len(nodes)
        self._destination_chunked_bind_state = {
            "gen": gen,
            "model": model,
            "on_complete": on_complete,
            "phase": "build",
            "bind_t0": time.perf_counter(),
        }
        thr = self._destination_chunked_bind_node_threshold()
        schedule_reason = "node_threshold" if node_count >= thr else "allocation_heuristic"
        self._log_restore_phase(
            "destination_chunked_bind_scheduled",
            bind_generation=gen,
            model_node_count=node_count,
            root_path=model.get("root_path", "Root") if isinstance(model, dict) else "Root",
            schedule_reason=schedule_reason,
            total_allocation_nodes=int(model.get("total_allocation_nodes", 0) or 0) if isinstance(model, dict) else 0,
        )
        QTimer.singleShot(0, self._run_destination_chunked_bind_tick)

    def _run_destination_chunked_bind_tick(self):
        st = getattr(self, "_destination_chunked_bind_state", None)
        if not isinstance(st, dict):
            return
        gen = int(st.get("gen", 0) or 0)
        if gen != int(getattr(self, "_destination_future_bind_generation", -1) or -1):
            return
        tree = getattr(self, "destination_tree_widget", None)
        model = st.get("model") or {}
        on_complete = st.get("on_complete")
        phase = str(st.get("phase") or "")

        def _fail(exc, *, phase_name="chunked_destination_bind"):
            self._log_restore_exception(phase_name, exc)
            t_bind = float((st or {}).get("bind_t0") or time.perf_counter())
            self._log_restore_phase(
                "destination_chunked_bind_aborted",
                phase_failed=phase_name,
                bind_generation=st.get("gen") if isinstance(st, dict) else None,
                wall_duration_ms=round((time.perf_counter() - t_bind) * 1000.0, 1),
                error_type=type(exc).__name__,
            )
            try:
                if tree is not None:
                    tree.blockSignals(False)
            except Exception:
                pass
            self._destination_chunked_bind_state = None
            self._set_tree_status_message("destination", "Destination bind failed; try Refresh Cache.", loading=False)
            if callable(on_complete):
                try:
                    on_complete(0, 0)
                except Exception as cb_exc:
                    self._log_restore_exception("chunked_destination_bind.on_complete_fail", cb_exc)
            self._flush_pending_destination_library_root_if_any(flush_reason="after_chunked_bind_fail")

        if phase == "build":
            try:
                if tree is None:
                    _fail(RuntimeError("no destination tree"), phase_name="chunked_destination_bind.build")
                    return
                if getattr(self, "_destination_snapshot_chunked_restore_active", False):
                    self._log_restore_phase(
                        "destination_chunked_bind_deferred",
                        bind_generation=gen,
                        waiting_for_destination_snapshot_restore=True,
                    )
                    QTimer.singleShot(0, self._run_destination_chunked_bind_tick)
                    return
                ui_state = self._capture_workspace_tree_state()
                st["destination_expanded_paths"] = set(ui_state.get("destination_expanded_paths", set()) or set())
                st["destination_selected_path"] = str(ui_state.get("destination_selected_path", "") or "")
                model_nodes = model.get("nodes", {})
                top_level_paths = model.get("top_level_paths", [])
                self._log_restore_phase(
                    "destination_future_model_bind_started",
                    root_path=model.get("root_path", "Root"),
                    top_level_count=len(top_level_paths),
                    bind_mode="chunked_async",
                    chunked_async=True,
                    model_node_count=len(model_nodes),
                    bind_generation=gen,
                )
                tree.blockSignals(True)
                tree.clear()
                self._future_bind_nodes_built = 0
                for semantic_path in top_level_paths:
                    tree.addTopLevelItem(
                        self._build_destination_tree_item_from_future_model(model_nodes, semantic_path)
                    )
                visible_future_branch_count = sum(
                    1 for _p, n in model_nodes.items() if n.get("node_state") != "real"
                )
                st["visible_future_branch_count"] = visible_future_branch_count
                for index in range(tree.topLevelItemCount()):
                    self._refresh_destination_item_visibility(tree.topLevelItem(index), expand=True)
                flat_items = []
                for index in range(tree.topLevelItemCount()):
                    for item in self._iter_tree_items(tree.topLevelItem(index)):
                        flat_items.append(item)
                st["flat_items"] = flat_items
                st["alloc_i"] = 0
                st["descendant_applied"] = 0
                st["allocation_pass"] = 0
                target_paths = {p for p in st["destination_expanded_paths"] if p}
                target_paths.add("Root")
                normalized_targets = set(target_paths)
                for path in list(target_paths):
                    k = self._destination_expansion_state_key(path)
                    if k:
                        normalized_targets.add(k)
                st["normalized_targets"] = normalized_targets
                st["destination_projection_eager_all"] = self._destination_projection_eager_expand_all_destination()
                st["allocation_descendants_deferred"] = 0
                restore_items = list(flat_items)
                restore_items.sort(
                    key=lambda it: len(self._path_segments(self._tree_item_path(it.data(0, Qt.UserRole) or {})))
                )
                st["restore_items"] = restore_items
                st["restore_i"] = 0
                st["phase"] = "allocation"
                self._set_tree_status_message("destination", "Merging destination structure…", loading=True)
            except Exception as exc:
                _fail(exc, phase_name="chunked_destination_bind.build")
                return
            QTimer.singleShot(0, self._run_destination_chunked_bind_tick)
            return

        if phase == "allocation":
            budget = 72
            flat_items = st.get("flat_items") or []
            i = int(st.get("alloc_i", 0) or 0)
            end = min(i + budget, len(flat_items))
            try:
                while i < end:
                    item = flat_items[i]
                    i += 1
                    node_data = item.data(0, Qt.UserRole) or {}
                    if not self.node_is_planned_allocation(node_data):
                        continue
                    if not bool(node_data.get("is_folder", False)):
                        continue
                    move = self._find_planned_move_for_destination_node(node_data)
                    if move is None:
                        continue
                    st["allocation_pass"] = int(st.get("allocation_pass", 0) or 0) + 1
                    if item.childCount() > 0 and self._destination_allocation_folder_shows_materialized_children(item):
                        nd = dict(node_data)
                        if not bool(nd.get("children_loaded")):
                            nd["children_loaded"] = True
                            nd["projection_unresolved_terminal"] = False
                            item.setData(0, Qt.UserRole, nd)
                    else:
                        sem = self._destination_semantic_path(node_data)
                        nt = st.get("normalized_targets") or set()
                        if st.get("destination_projection_eager_all") or self._destination_bind_should_apply_allocation_descendants_now(
                            sem, nt
                        ):
                            st["descendant_applied"] = int(st.get("descendant_applied", 0) or 0) + int(
                                self._apply_allocation_descendants_to_item(item, move)
                            )
                        else:
                            st["allocation_descendants_deferred"] = int(st.get("allocation_descendants_deferred", 0) or 0) + 1
                    if st["allocation_pass"] % 2 == 0:
                        QApplication.processEvents(QEventLoop.ProcessEventsFlag.ExcludeUserInputEvents)
            except Exception as exc:
                _fail(exc, phase_name="chunked_destination_bind.allocation")
                return
            st["alloc_i"] = i
            if i < len(flat_items):
                QTimer.singleShot(0, self._run_destination_chunked_bind_tick)
                return
            if int(st.get("descendant_applied", 0) or 0) and tree is not None:
                tree.viewport().update()
            st["phase"] = "indicators"
            QTimer.singleShot(0, self._run_destination_chunked_bind_tick)
            return

        if phase == "indicators":
            try:
                self._refresh_destination_tree_indicators()
            except Exception as exc:
                _fail(exc, phase_name="chunked_destination_bind.indicators")
                return
            st["phase"] = "restore_expand"
            QTimer.singleShot(0, self._run_destination_chunked_bind_tick)
            return

        if phase == "restore_expand":
            budget = 100
            items = st.get("restore_items") or []
            j = int(st.get("restore_i", 0) or 0)
            nt = st.get("normalized_targets") or set()
            end = min(j + budget, len(items))
            try:
                while j < end:
                    item = items[j]
                    j += 1
                    node_data = item.data(0, Qt.UserRole) or {}
                    semantic_path = self._destination_semantic_path(node_data)
                    item_keys = {semantic_path}
                    sk = self._destination_expansion_state_key(semantic_path)
                    if sk:
                        item_keys.add(sk)
                    if item_keys & nt:
                        tree.expandItem(item)
            except Exception as exc:
                _fail(exc, phase_name="chunked_destination_bind.restore_expand")
                return
            st["restore_i"] = j
            if j < len(items):
                QTimer.singleShot(0, self._run_destination_chunked_bind_tick)
                return
            st["phase"] = "finalize"
            QTimer.singleShot(0, self._run_destination_chunked_bind_tick)
            return

        if phase == "finalize":
            vf = int(st.get("visible_future_branch_count", 0) or 0)
            da = int(st.get("descendant_applied", 0) or 0)
            t_bind = float(st.get("bind_t0") or time.perf_counter())
            wall_ms = round((time.perf_counter() - t_bind) * 1000.0, 1)
            node_count = len((model.get("nodes") or {}))
            try:
                self._set_tree_status_message("destination", "", loading=False)
                deferred_n = int(st.get("allocation_descendants_deferred", 0) or 0)
                if deferred_n:
                    self._log_restore_phase(
                        "destination_bind_allocation_descendants_deferred",
                        deferred_allocation_count=deferred_n,
                        bind_generation=st.get("gen"),
                    )
                self._restore_selected_tree_path("destination", str(st.get("destination_selected_path", "") or ""))
                self._refresh_expand_all_button_for_panel("destination")
                self._log_restore_phase(
                    "destination_future_model_bind_complete",
                    root_path=model.get("root_path", "Root"),
                    top_level_count=tree.topLevelItemCount() if tree is not None else 0,
                    visible_descendant_count=da,
                    bind_mode="chunked_async",
                    chunked_async=True,
                    wall_duration_ms=wall_ms,
                    model_node_count=node_count,
                    bind_generation=st.get("gen"),
                )
                self._log_restore_phase(
                    "destination_future_model_visible_summary",
                    root_path=model.get("root_path", "Root"),
                    total_real_nodes=model.get("total_real_nodes", 0),
                    total_proposed_nodes=model.get("total_proposed_nodes", 0),
                    total_allocation_nodes=model.get("total_allocation_nodes", 0),
                    top_level_count=tree.topLevelItemCount() if tree is not None else 0,
                    visible_future_branch_count=vf,
                    bind_mode="chunked_async",
                    chunked_async=True,
                    wall_duration_ms=wall_ms,
                    model_node_count=node_count,
                    bind_generation=st.get("gen"),
                )
                self._schedule_workspace_ui_persist(panel_key="destination")
            except Exception as exc:
                _fail(exc, phase_name="chunked_destination_bind.finalize")
                return
            finally:
                try:
                    if tree is not None:
                        tree.blockSignals(False)
                except Exception:
                    pass
                self._destination_chunked_bind_state = None
            if callable(on_complete):
                try:
                    on_complete(vf, da)
                except Exception as cb_exc:
                    self._log_restore_exception("chunked_destination_bind.on_complete", cb_exc)
            self._flush_pending_destination_library_root_if_any(flush_reason="after_chunked_bind")
            return

    def _bind_destination_tree_from_future_state_model(self, model, *, on_complete=None):
        if getattr(self, "_destination_snapshot_chunked_restore_active", False):
            self._log_restore_phase(
                "destination_future_bind_deferred",
                waiting_for_destination_snapshot_restore=True,
                root_path=model.get("root_path", "Root") if isinstance(model, dict) else "Root",
            )

            def _retry_bind():
                self._bind_destination_tree_from_future_state_model(model, on_complete=on_complete)

            QTimer.singleShot(0, _retry_bind)
            return None
        nodes = model.get("nodes") or {}
        if self._destination_future_bind_should_chunk_async(model, on_complete=on_complete):
            if on_complete is None:
                out = self._bind_destination_future_model_sync(model)
                self._flush_pending_destination_library_root_if_any(flush_reason="after_sync_bind_large_no_callback")
                return out
            self._schedule_chunked_destination_future_bind(model, on_complete)
            return None
        out = self._bind_destination_future_model_sync(model)
        if on_complete is not None:
            try:
                on_complete(out[0], out[1])
            except Exception as cb_exc:
                self._log_restore_exception("destination_future_bind.on_complete", cb_exc)
        self._flush_pending_destination_library_root_if_any(flush_reason="after_sync_bind")
        return out

    def _incremental_merge_destination_future_projection(self, model, fast_paths):
        """Attach projected-allocation descendant subtrees without clearing the destination tree."""
        tree = getattr(self, "destination_tree_widget", None)
        if tree is None:
            return 0

        model_nodes = model.get("nodes", {})
        fast_set = fast_paths if isinstance(fast_paths, (set, frozenset)) else frozenset(fast_paths or ())
        new_paths = {
            p
            for p, n in model_nodes.items()
            if n.get("node_state") == "projected_descendant" and p not in fast_set
        }
        self._log_restore_phase(
            "destination_future_incremental_merge_started",
            root_path=model.get("root_path", "Root"),
            new_branch_count=len(new_paths),
        )

        visible_future_branch_count = sum(
            1 for node in model_nodes.values() if node.get("node_state") != "real"
        )

        if not new_paths:
            self._log_restore_phase(
                "destination_future_incremental_merge_skipped",
                root_path=model.get("root_path", "Root"),
                reason="no_projected_descendants",
            )
            return visible_future_branch_count

        entry_roots = [
            p
            for p in new_paths
            if model_nodes[p].get("parent_semantic_path", "") not in new_paths
        ]
        entry_roots.sort(
            key=lambda path: (
                len(self._path_segments(path)),
                [s.lower() for s in self._path_segments(path)],
            )
        )

        ui_state = self._capture_workspace_tree_state()
        destination_expanded_paths = set(ui_state.get("destination_expanded_paths", set()) or set())
        destination_selected_path = str(ui_state.get("destination_selected_path", "") or "")
        nt = self._destination_bind_normalized_expanded_targets(destination_expanded_paths)
        merge_roots_before = len(entry_roots)
        if not self._destination_projection_eager_expand_all_destination():
            entry_roots = [p for p in entry_roots if self._destination_incremental_merge_root_needed_now(p, nt)]
        if merge_roots_before != len(entry_roots):
            self._log_restore_phase(
                "destination_incremental_merge_entry_roots_filtered",
                entry_roots_before=merge_roots_before,
                entry_roots_after=len(entry_roots),
            )

        tree.blockSignals(True)
        try:
            self._future_bind_nodes_built = 0
            for root_path in entry_roots:
                parent_path = model_nodes[root_path].get("parent_semantic_path", "")
                parent_item = self._find_visible_destination_item_by_path(parent_path) if parent_path else None
                if parent_item is None:
                    self._log_restore_phase(
                        "destination_future_incremental_merge_parent_missing",
                        child_path=root_path,
                        parent_path=parent_path,
                    )
                    continue
                if self._find_destination_child_by_path(parent_item, root_path):
                    continue
                self._remove_placeholder_children(parent_item)
                child_item = self._build_destination_tree_item_from_future_model(model_nodes, root_path)
                parent_item.addChild(child_item)
                self._sort_destination_future_children(parent_item)

            for index in range(tree.topLevelItemCount()):
                top_item = tree.topLevelItem(index)
                self._refresh_destination_item_visibility(top_item, expand=True)

            visible_descendant_count = self._apply_visible_destination_allocation_descendants(
                destination_expanded_paths=destination_expanded_paths
            )
            self._refresh_destination_tree_indicators()
            self._restore_expanded_tree_paths("destination", destination_expanded_paths)
            self._restore_selected_tree_path("destination", destination_selected_path)
            self._refresh_expand_all_button_for_panel("destination")

            self._log_restore_phase(
                "destination_future_incremental_merge_complete",
                root_path=model.get("root_path", "Root"),
                top_level_count=tree.topLevelItemCount(),
                visible_descendant_count=visible_descendant_count,
                entry_roots=len(entry_roots),
            )
            self._log_restore_phase(
                "destination_future_model_visible_summary",
                root_path=model.get("root_path", "Root"),
                total_real_nodes=model.get("total_real_nodes", 0),
                total_proposed_nodes=model.get("total_proposed_nodes", 0),
                total_allocation_nodes=model.get("total_allocation_nodes", 0),
                top_level_count=tree.topLevelItemCount(),
                visible_future_branch_count=visible_future_branch_count,
                merge_mode="incremental",
            )
            self._schedule_workspace_ui_persist(panel_key="destination")
            return visible_future_branch_count
        finally:
            tree.blockSignals(False)

    def _materialize_destination_future_model(
        self, reason, *, allow_defer=True, prefer_chunked_projection=False
    ):
        self._destination_future_model_last_blocked_source_restore = False
        self._cancel_destination_future_async_projection(reason or "materialize")
        if getattr(self, "_destination_root_prime_pending", False) and reason not in {
            "folder_worker_success",
            "destination_expand_all_complete",
            "destination_full_tree_success",
            "destination_expand_all_full_tree",
        }:
            self._log_restore_phase(
                "destination_future_model_materialize_deferred",
                reason=reason,
                waiting_for_root_prime=True,
            )
            return 0
        if (
            reason != "destination_full_tree_success"
            and not self._destination_full_tree_ready()
            and self._destination_full_tree_worker is not None
            and self._destination_full_tree_worker.isRunning()
        ):
            self._log_restore_phase(
                "destination_future_model_materialize_deferred",
                reason=reason,
                waiting_for_full_tree=True,
                requested_drive_id=self._destination_full_tree_requested_drive_id,
                completed_drive_id=self._destination_full_tree_completed_drive_id,
            )
            return 0
        if self._destination_future_model_blocked_by_source_restore(reason):
            self._destination_future_model_pending_after_source_restore = True
            self._destination_future_model_last_blocked_source_restore = True
            self._log_restore_phase(
                "destination_future_model_materialize_deferred",
                reason=reason,
                waiting_for_source_restore=True,
                source_restore_queue_size=len(getattr(self, "_source_restore_materialization_queue", None) or []),
                pending_source_folder_loads=len(self.pending_folder_loads.get("source", set())),
                memory_restore_in_progress=bool(getattr(self, "_memory_restore_in_progress", False)),
            )
            return 0
        if getattr(self, "_destination_snapshot_chunked_restore_active", False):
            self._log_restore_phase(
                "destination_future_model_materialize_deferred",
                reason=reason,
                waiting_for_destination_snapshot_restore=True,
            )

            def _retry_materialize():
                self._materialize_destination_future_model(
                    reason, allow_defer=allow_defer, prefer_chunked_projection=prefer_chunked_projection
                )

            QTimer.singleShot(0, _retry_materialize)
            return 0
        if allow_defer and self._should_defer_destination_materialization(reason):
            self._log_restore_phase(
                "destination_future_model_materialize_deferred",
                reason=reason,
                waiting_for_idle_window=True,
                expandable_node_count=self._count_expandable_tree_nodes("destination"),
                pending_folder_loads=len(self.pending_folder_loads.get("destination", set())),
                expand_all_pending=bool(self._expand_all_pending.get("destination")),
            )
            self._schedule_deferred_destination_materialization(reason)
            return 0

        use_chunked = (
            (allow_defer or prefer_chunked_projection)
            and self._should_chunk_destination_future_projection()
        )
        if use_chunked and prefer_chunked_projection and not allow_defer:
            self._log_restore_phase(
                "destination_future_model_prefer_chunked_sync",
                reason=reason,
            )
        if use_chunked:
            self._set_tree_status_message("destination", "Building destination preview…", loading=True)
            model_fast = self._build_destination_future_model(skip_allocation_descendants=True)
            self._log_restore_phase(
                "destination_future_model_fast_first_paint",
                reason=reason,
                root_path=model_fast.get("root_path", "Root"),
                branch_count=len(model_fast.get("nodes", {})),
                prefer_chunked_projection=bool(prefer_chunked_projection),
            )
            self._log_restore_phase(
                "destination_future_model_materialize_started",
                reason=reason,
                materialize_phase="fast_first_paint",
                root_path=model_fast.get("root_path", "Root"),
                visible_future_branch_count=len(model_fast.get("nodes", {})),
            )

            def _complete_fast_first_paint_bind(visible_future_branch_count, _da):
                applied_count = int(visible_future_branch_count)
                self._log_restore_phase(
                    "destination_future_model_materialize_applied",
                    reason=reason,
                    materialize_phase="fast_first_paint",
                    root_path=model_fast.get("root_path", "Root"),
                    applied_count=applied_count,
                    visible_future_branch_count=applied_count,
                )
                self._log_restore_phase(
                    "destination_future_model_materialize_complete",
                    reason=reason,
                    materialize_phase="fast_first_paint",
                    root_path=model_fast.get("root_path", "Root"),
                    applied_count=applied_count,
                    visible_future_branch_count=applied_count,
                )
                self._log_restore_phase(
                    "destination_visible_future_branch_count",
                    reason=reason,
                    materialize_phase="fast_first_paint",
                    root_path=model_fast.get("root_path", "Root"),
                    visible_future_branch_count=applied_count,
                )
                self._destination_future_model_pending_after_source_restore = False
                self._destination_future_projection_async_state = {
                    "reason": reason,
                    "model_nodes": model_fast["nodes"],
                    "real_child_counts": model_fast.get("_real_child_counts") or {},
                    "total_real_nodes": model_fast.get("total_real_nodes", 0),
                    "total_proposed_nodes": model_fast.get("total_proposed_nodes", 0),
                    "total_allocation_nodes": model_fast.get("total_allocation_nodes", 0),
                    "moves": list(self.planned_moves),
                    "move_index": 0,
                    "current_chunk": None,
                    "descendant_index": 0,
                    "fast_paint_paths": frozenset(model_fast["nodes"].keys()),
                    "projection_wall_t0": time.perf_counter(),
                }
                self._set_tree_status_message(
                    "destination",
                    f"Building destination preview… (1/{max(1, len(self.planned_moves))})",
                    loading=True,
                )
                self._destination_future_projection_timer.start(0)

            bind_out = self._bind_destination_tree_from_future_state_model(
                model_fast,
                on_complete=_complete_fast_first_paint_bind,
            )
            if bind_out is None:
                return 0
            return bind_out[0]

        model = self._build_destination_future_model()
        if "_real_child_counts" in model:
            del model["_real_child_counts"]
        self._log_restore_phase(
            "destination_future_model_materialize_started",
            reason=reason,
            root_path=model.get("root_path", "Root"),
            visible_future_branch_count=len(model.get("nodes", {})),
        )

        def _complete_full_destination_bind(visible_future_branch_count, _da):
            applied_count = int(visible_future_branch_count)
            self._log_restore_phase(
                "destination_future_model_materialize_applied",
                reason=reason,
                root_path=model.get("root_path", "Root"),
                applied_count=applied_count,
                visible_future_branch_count=applied_count,
            )
            self._log_restore_phase(
                "destination_future_model_materialize_complete",
                reason=reason,
                root_path=model.get("root_path", "Root"),
                applied_count=applied_count,
                visible_future_branch_count=applied_count,
            )
            self._log_restore_phase(
                "destination_visible_future_branch_count",
                reason=reason,
                root_path=model.get("root_path", "Root"),
                visible_future_branch_count=applied_count,
            )
            self._destination_future_model_pending_after_source_restore = False

        bind_out = self._bind_destination_tree_from_future_state_model(
            model,
            on_complete=_complete_full_destination_bind,
        )
        if bind_out is None:
            return 0
        return bind_out[0]

    def _destination_is_future_state_node(self, node_data):
        state = self._destination_node_state(node_data)
        return state in {"projected", "proposed", "allocated"}

    def _refresh_destination_item_visibility(self, item, *, expand=False):
        if item is None:
            return
        node_data = item.data(0, Qt.UserRole) or {}
        if node_data.get("placeholder"):
            return

        # Apply label/colour first. Previously we set ChildIndicatorPolicy and then called
        # _apply_tree_item_visual_state, which rewrites item.text(0) from base_display_label — so
        # chevron logic often ran with stale/empty text and stripped expanders from [Allocated] rows.
        self._apply_tree_item_visual_state(item, node_data)
        node_data = item.data(0, Qt.UserRole) or {}

        base_label = str(node_data.get("base_display_label", "") or "").strip().lower()
        tree_label = str(node_data.get("tree_label", "") or "").strip().lower()
        text_label = str(item.text(0) or "").strip().lower()
        # item.text(0) can lag behind base_display_label during replay/overlay; chevrons used to
        # disappear for virtual [Allocated] folders when only UserRole had the full label.
        is_allocated_label = "[allocated]" in text_label or "[allocated]" in base_label
        is_folder_like = bool(
            node_data.get("is_folder")
            or tree_label == "folder"
            or base_label.startswith("folder:")
            or text_label.startswith("folder:")
        )
        has_visible_children = any(
            not ((item.child(index).data(0, Qt.UserRole) or {}).get("placeholder"))
            for index in range(item.childCount())
        )
        has_placeholder_children = any(
            ((item.child(index).data(0, Qt.UserRole) or {}).get("placeholder"))
            for index in range(item.childCount())
        )
        has_projected_descendants = self._destination_has_future_descendants(node_data)
        is_planned_allocation = bool(self.node_is_planned_allocation(node_data))
        is_proposed_node = bool(self.node_is_proposed(node_data))
        is_allocation_descendant = bool(node_data.get("planned_allocation_descendant"))
        is_allocated_node = bool(is_planned_allocation or is_allocation_descendant)
        label_says_allocated_folder = bool(
            base_label.startswith("folder:") and "[allocated]" in base_label
        )
        has_unresolved_projection = bool(
            is_planned_allocation
            and bool(node_data.get("projection_unresolved_terminal"))
        )
        has_pending_folder_lazy_load = bool(
            is_folder_like
            and not bool(node_data.get("children_loaded"))
            and not bool(node_data.get("load_failed"))
        )
        if is_folder_like:
            show_chevron = bool(
                has_visible_children
                or has_placeholder_children
                or has_projected_descendants
                or is_allocated_node
                or (is_proposed_node and is_folder_like)
                or (is_allocated_label and is_folder_like)
                or has_unresolved_projection
                or has_pending_folder_lazy_load
                or label_says_allocated_folder
            )
            item.setChildIndicatorPolicy(
                QTreeWidgetItem.ShowIndicator if show_chevron else QTreeWidgetItem.DontShowIndicator
            )
            if self._full_trace_enabled() or (
                self._projection_diag_verbose_enabled() and is_allocated_node
            ):
                self._log_restore_phase(
                    "destination_projection_ui_chevron",
                    destination_path=self._tree_item_path(node_data),
                    show_chevron=show_chevron,
                    has_visible_children=has_visible_children,
                    has_placeholder_children=has_placeholder_children,
                    has_projected_descendants=has_projected_descendants,
                    has_unresolved_projection=has_unresolved_projection,
                    has_pending_folder_lazy_load=has_pending_folder_lazy_load,
                    label_says_allocated_folder=label_says_allocated_folder,
                    is_allocated_label=is_allocated_label,
                    is_proposed_node=is_proposed_node,
                    is_allocated_node=is_allocated_node,
                    child_count=item.childCount(),
                    expand_requested=expand,
                )
        if expand and has_visible_children:
            current = item
            while current is not None:
                current.setExpanded(True)
                current = current.parent()

    def _refresh_destination_tree_indicators(self):
        tree = getattr(self, "destination_tree_widget", None)
        if tree is None:
            return
        pumped = 0
        try:
            top_n = tree.topLevelItemCount()
        except RuntimeError:
            return
        for index in range(top_n):
            try:
                top_item = tree.topLevelItem(index)
            except RuntimeError:
                continue
            if not self._tree_item_is_alive(top_item):
                continue
            for item in self._iter_tree_items(top_item):
                try:
                    self._refresh_destination_item_visibility(item)
                except RuntimeError:
                    continue
                pumped += 1
                if pumped % 48 == 0:
                    QApplication.processEvents(QEventLoop.ProcessEventsFlag.ExcludeUserInputEvents)
        if self._full_trace_enabled():
            log_trace("tree", "refresh_destination_tree_indicators_complete", nodes_refreshed=pumped)

    def _destination_has_future_descendants(self, node_data):
        if not node_data or not bool(node_data.get("is_folder")):
            return False
        semantic_path = self._destination_semantic_path(node_data)
        if not semantic_path:
            return False
        prefix = f"{semantic_path}\\"
        for proposed in self.proposed_folders:
            proposed_path = self._canonical_destination_projection_path(
                getattr(proposed, "DestinationPath", "")
            )
            if proposed_path and proposed_path.startswith(prefix):
                return True
        for move in self.planned_moves:
            move_path = self._canonical_destination_projection_path(
                self.normalize_memory_path(
                    "\\".join([move.get("destination_path", ""), self._move_target_name(move)])
                )
            )
            if move_path and move_path.startswith(prefix):
                return True
        return False

    def _iter_destination_items(self):
        tree = getattr(self, "destination_tree_widget", None)
        if tree is None:
            return
        try:
            top_n = tree.topLevelItemCount()
        except RuntimeError:
            return
        for index in range(top_n):
            try:
                top_item = tree.topLevelItem(index)
            except RuntimeError:
                continue
            if not self._tree_item_is_alive(top_item):
                continue
            for item in self._iter_tree_items(top_item):
                yield item

    def _find_destination_child_by_semantic_path(self, parent_item, semantic_path):
        if parent_item is None or not semantic_path:
            return None
        matches = []
        for index in range(parent_item.childCount()):
            child = parent_item.child(index)
            child_data = child.data(0, Qt.UserRole) or {}
            if child_data.get("placeholder"):
                continue
            if self._destination_semantic_path(child_data) == semantic_path:
                matches.append(child)
        return self._select_canonical_destination_item(matches)

    def _detach_destination_item(self, item):
        if item is None:
            return
        parent = item.parent()
        if parent is not None:
            for index in range(parent.childCount()):
                if parent.child(index) is item:
                    parent.takeChild(index)
                    return
            return
        tree = getattr(self, "destination_tree_widget", None)
        if tree is None:
            return
        for index in range(tree.topLevelItemCount()):
            if tree.topLevelItem(index) is item:
                tree.takeTopLevelItem(index)
                return

    def _merge_destination_projection_children(self, source_item, target_item, semantic_path):
        moved_count = 0
        # If the target currently shows a terminal empty/placeholder child, it must be cleared
        # once we attach projected/proposed children; otherwise the "This folder is empty."
        # row can incorrectly persist.
        self._remove_placeholder_children(target_item)
        while source_item.childCount():
            child = source_item.takeChild(0)
            child_data = child.data(0, Qt.UserRole) or {}
            child_semantic_path = self._destination_semantic_path(child_data)
            existing = self._find_destination_child_by_semantic_path(target_item, child_semantic_path)
            if existing is not None:
                self._merge_destination_projection_children(child, existing, child_semantic_path)
                continue
            target_item.addChild(child)
            moved_count += 1
            self._refresh_destination_item_visibility(target_item, expand=True)
            self._log_restore_phase(
                "destination_projected_child_reparented",
                semantic_path=child_semantic_path or semantic_path,
                projected_tree_path=self._tree_item_path(child_data),
                real_tree_path=self._tree_item_path(target_item.data(0, Qt.UserRole) or {}),
                child_count_moved=moved_count,
                node_origin_before=child_data.get("node_origin", ""),
                node_origin_after=(target_item.data(0, Qt.UserRole) or {}).get("node_origin", ""),
            )
        return moved_count

    def _preserve_destination_future_state_children(self, parent_item):
        preserved = []
        if parent_item is None:
            return preserved
        for index in range(parent_item.childCount() - 1, -1, -1):
            child = parent_item.child(index)
            child_data = child.data(0, Qt.UserRole) or {}
            if self._destination_is_future_state_node(child_data):
                preserved.append(parent_item.takeChild(index))
        preserved.reverse()
        return preserved

    def _restore_destination_future_state_children(self, parent_item, preserved_children):
        if parent_item is None or not preserved_children:
            return 0
        # When restoring future-state children into an existing destination node, the node
        # might still contain a terminal empty placeholder. Clear it before adding children.
        self._remove_placeholder_children(parent_item)

        parent_data = parent_item.data(0, Qt.UserRole) or {}
        parent_path = self._tree_item_path(parent_data)
        moved_count = 0
        for child in preserved_children:
            child_data = child.data(0, Qt.UserRole) or {}
            semantic_path = self._destination_semantic_path(child_data)
            existing = self._find_destination_child_by_semantic_path(parent_item, semantic_path)
            if existing is not None and existing is not child:
                existing_data = existing.data(0, Qt.UserRole) or {}
                if self._destination_node_state(existing_data) == "real":
                    self._log_restore_phase(
                        "destination_real_child_loaded_for_semantic_path",
                        requested_parent_path=parent_path,
                        semantic_path=semantic_path,
                        real_tree_path=self._tree_item_path(existing_data),
                        projected_tree_path=self._tree_item_path(child_data),
                        child_count_moved=child.childCount(),
                    )
                    self._log_restore_phase(
                        "destination_projected_to_real_handoff_started",
                        requested_parent_path=parent_path,
                        semantic_path=semantic_path,
                        real_tree_path=self._tree_item_path(existing_data),
                        projected_tree_path=self._tree_item_path(child_data),
                        child_count_moved=child.childCount(),
                    )
                moved_count += self._merge_destination_projection_children(child, existing, semantic_path)
                if self._destination_node_state(existing_data) == "real":
                    self._log_restore_phase(
                        "destination_projected_to_real_handoff_complete",
                        requested_parent_path=parent_path,
                        semantic_path=semantic_path,
                        real_tree_path=self._tree_item_path(existing_data),
                        projected_tree_path=self._tree_item_path(child_data),
                        child_count_moved=moved_count,
                    )
                continue

            parent_item.addChild(child)
            self._refresh_destination_item_visibility(parent_item, expand=True)
        return moved_count

    def _reconcile_destination_semantic_duplicates(self, reason):
        semantic_groups = {}
        for item in self._iter_destination_items() or []:
            node_data = item.data(0, Qt.UserRole) or {}
            if node_data.get("placeholder"):
                continue
            semantic_path = self._destination_semantic_path(node_data)
            if semantic_path:
                semantic_groups.setdefault(semantic_path, []).append(item)

        total_moved = 0
        merged_groups = 0
        for semantic_path, items in semantic_groups.items():
            if len(items) < 2:
                continue
            canonical_item = self._select_canonical_destination_item(items)
            canonical_data = canonical_item.data(0, Qt.UserRole) or {}
            canonical_state = self._destination_node_state(canonical_data)
            duplicates = [item for item in items if item is not canonical_item]
            if not duplicates:
                continue
            self._log_restore_phase(
                "destination_semantic_duplicate_detected",
                semantic_path=semantic_path,
                projected_tree_path="; ".join(self._tree_item_path(item.data(0, Qt.UserRole) or {}) for item in duplicates),
                real_tree_path=self._tree_item_path(canonical_data),
                child_count_moved=0,
                node_origin_before="duplicate_group",
                node_origin_after=canonical_data.get("node_origin", ""),
            )
            if canonical_state == "real":
                self._log_restore_phase(
                    "destination_real_parent_promoted_canonical",
                    semantic_path=semantic_path,
                    projected_tree_path="; ".join(self._tree_item_path(item.data(0, Qt.UserRole) or {}) for item in duplicates),
                    real_tree_path=self._tree_item_path(canonical_data),
                    child_count_moved=0,
                    node_origin_before="duplicate_group",
                    node_origin_after=canonical_data.get("node_origin", ""),
                )

            for duplicate in duplicates:
                duplicate_data = duplicate.data(0, Qt.UserRole) or {}
                self._log_restore_phase(
                    "destination_projected_parent_merge_started",
                    semantic_path=semantic_path,
                    projected_tree_path=self._tree_item_path(duplicate_data),
                    real_tree_path=self._tree_item_path(canonical_data),
                    child_count_moved=duplicate.childCount(),
                    node_origin_before=duplicate_data.get("node_origin", ""),
                    node_origin_after=canonical_data.get("node_origin", ""),
                )
                moved_count = self._merge_destination_projection_children(duplicate, canonical_item, semantic_path)
                total_moved += moved_count
                self._detach_destination_item(duplicate)
                merged_groups += 1
                self._log_restore_phase(
                    "destination_projected_parent_retired",
                    semantic_path=semantic_path,
                    projected_tree_path=self._tree_item_path(duplicate_data),
                    real_tree_path=self._tree_item_path(canonical_data),
                    child_count_moved=moved_count,
                    node_origin_before=duplicate_data.get("node_origin", ""),
                    node_origin_after=canonical_data.get("node_origin", ""),
                )

        if merged_groups:
            self._log_restore_phase(
                "destination_projection_reconciled",
                reason=reason,
                merged_group_count=merged_groups,
                child_count_moved=total_moved,
            )
        return total_moved

    def _find_destination_child_by_path(self, parent_item, destination_path):
        if parent_item is None:
            return None
        normalized_target = self._canonical_destination_projection_path(destination_path) or self.normalize_memory_path(destination_path)
        emit_restore_match_logs = bool(getattr(self, "_verbose_destination_match_logging", False))
        if emit_restore_match_logs:
            self._log_restore_phase(
                "destination_parent_exact_match_required",
                requested_parent_path=destination_path,
                normalized_parent_path=normalized_target,
            )
        matches = []
        for index in range(parent_item.childCount()):
            child = parent_item.child(index)
            child_data = child.data(0, Qt.UserRole) or {}
            if child_data.get("placeholder"):
                continue
            child_path = self._tree_item_path(child_data)
            if not child_path:
                continue
            match_details = self._destination_parent_match_details(destination_path, child_path)
            if match_details["exact_match"]:
                if emit_restore_match_logs:
                    self._log_restore_phase(
                        "destination_parent_exact_match_succeeded",
                        requested_parent_path=destination_path,
                        normalized_parent_path=match_details["normalized_requested"],
                        candidate_tree_path=child_path,
                        candidate_normalized_path=match_details["normalized_candidate"],
                        exact_match=True,
                        prefix_only_match=False,
                        accepted_or_rejected="accepted",
                    )
                matches.append(child)
                continue
            if match_details["prefix_only_match"]:
                if emit_restore_match_logs:
                    self._log_restore_phase(
                        "destination_parent_prefix_rejected",
                        requested_parent_path=destination_path,
                        normalized_parent_path=match_details["normalized_requested"],
                        candidate_tree_path=child_path,
                        candidate_normalized_path=match_details["normalized_candidate"],
                        exact_match=False,
                        prefix_only_match=True,
                        accepted_or_rejected="rejected",
                    )
                continue
            if emit_restore_match_logs:
                self._log_restore_phase(
                    "destination_parent_exact_match_failed",
                    requested_parent_path=destination_path,
                    normalized_parent_path=match_details["normalized_requested"],
                    candidate_tree_path=child_path,
                    candidate_normalized_path=match_details["normalized_candidate"],
                    exact_match=False,
                    prefix_only_match=False,
                    accepted_or_rejected="rejected",
                )
        return self._select_canonical_destination_item(matches)

    def _build_projected_destination_folder_node(self, folder_name, destination_path, parent_data):
        parent_path = self._tree_item_path(parent_data)
        normalized_path = (
            self._canonical_destination_projection_path(f"{parent_path}\\{folder_name}")
            if parent_path
            else (self._canonical_destination_projection_path(destination_path) or self.normalize_memory_path(destination_path))
        )
        normalized_path = normalized_path or self.normalize_memory_path(destination_path)
        node = QTreeWidgetItem([f"Folder: {folder_name}"])
        node_data = {
            "id": f"projected::{normalized_path}",
            "name": folder_name,
            "real_name": folder_name,
            "display_path": normalized_path,
            "item_path": normalized_path,
            "destination_path": normalized_path,
            "tree_role": "destination",
            "drive_id": parent_data.get("drive_id", ""),
            "site_id": parent_data.get("site_id", ""),
            "site_name": parent_data.get("site_name", ""),
            "library_id": parent_data.get("library_id", ""),
            "library_name": parent_data.get("library_name", ""),
            "is_folder": True,
            "children_loaded": True,
            "load_failed": False,
            "node_origin": "ProjectedDestination",
            "overlay_state": "ProjectedDestination",
            "projected": True,
            "web_url": parent_data.get("web_url", ""),
        }
        node.setData(0, Qt.UserRole, node_data)
        node.setChildIndicatorPolicy(
            QTreeWidgetItem.ShowIndicator if self._destination_has_future_descendants(node_data) else QTreeWidgetItem.DontShowIndicator
        )
        self._apply_tree_item_visual_state(node, node_data)
        return node

    def _ensure_destination_projection_path(self, destination_path):
        normalized_target = self._canonical_destination_projection_path(destination_path)
        if not normalized_target:
            return None

        tree = getattr(self, "destination_tree_widget", None)
        if tree is None:
            return None

        self._log_restore_phase(
            "destination_parent_resolution_attempt",
            requested_parent_path=destination_path,
            normalized_parent_path=normalized_target,
        )

        prefixes = self._destination_projection_prefixes(normalized_target)
        anchor_item = None
        anchor_index = -1
        for index in range(len(prefixes) - 1, -1, -1):
            existing = self._find_visible_destination_item_by_path(prefixes[index])
            if existing is not None:
                anchor_item = existing
                anchor_index = index
                anchor_data = existing.data(0, Qt.UserRole) or {}
                anchor_state = self._destination_node_state(anchor_data)
                if index == len(prefixes) - 1:
                    self._log_restore_phase(
                        "destination_parent_resolution_reused_real" if anchor_state == "real" else "destination_parent_resolution_reused_projected",
                        requested_parent_path=destination_path,
                        normalized_parent_path=normalized_target,
                        resolved_tree_path=self._tree_item_path(anchor_data),
                        resolved_node_origin=anchor_data.get("node_origin", ""),
                        reused_existing=True,
                        created_new=False,
                    )
                else:
                    self._log_restore_phase(
                        "destination_root_anchor_used_for_projection_only",
                        requested_parent_path=destination_path,
                        anchor_tree_path=self._tree_item_path(anchor_data),
                        semantic_path=normalized_target,
                        child_count_moved=0,
                    )
                    if anchor_state == "real":
                        self._log_restore_phase(
                            "destination_bad_root_fallback_blocked",
                            requested_parent_path=destination_path,
                            anchor_tree_path=self._tree_item_path(anchor_data),
                            semantic_path=normalized_target,
                            child_count_moved=0,
                        )
                break

        current_item = anchor_item
        current_parent_data = {}
        if current_item is not None:
            current_parent_data = current_item.data(0, Qt.UserRole) or {}

        start_index = anchor_index + 1
        if current_item is None and prefixes:
            visible_root = self._find_visible_destination_item_by_path("Root")
            if visible_root is not None:
                current_item = visible_root
                current_parent_data = current_item.data(0, Qt.UserRole) or {}
                start_index = 1
                self._log_restore_phase(
                    "destination_parent_resolution_reused_real",
                    requested_parent_path=destination_path,
                    normalized_parent_path=normalized_target,
                    resolved_tree_path=self._tree_item_path(current_parent_data),
                    resolved_node_origin=current_parent_data.get("node_origin", ""),
                    reused_existing=True,
                    created_new=False,
                )
            else:
                base_data = {}
                if tree.topLevelItemCount() > 0:
                    base_data = tree.topLevelItem(0).data(0, Qt.UserRole) or {}
                branch_path = prefixes[0]
                folder_name = self._path_segments(branch_path)[-1]
                current_item = self._build_projected_destination_folder_node(folder_name, branch_path, base_data)
                tree.addTopLevelItem(current_item)
                current_parent_data = current_item.data(0, Qt.UserRole) or {}
                start_index = 1
                self._log_restore_phase(
                    "destination_parent_resolution_created_projected",
                    requested_parent_path=destination_path,
                    normalized_parent_path=normalized_target,
                    resolved_tree_path=self._tree_item_path(current_parent_data),
                    resolved_node_origin=current_parent_data.get("node_origin", ""),
                    reused_existing=False,
                    created_new=True,
                )

        for index in range(start_index, len(prefixes)):
            branch_path = prefixes[index]
            existing_child = self._find_destination_child_by_path(current_item, branch_path) if current_item is not None else None
            if existing_child is not None:
                current_item = existing_child
                current_parent_data = current_item.data(0, Qt.UserRole) or {}
                self._log_restore_phase(
                    "destination_parent_resolution_duplicate_detected",
                    requested_parent_path=destination_path,
                    normalized_parent_path=normalized_target,
                    resolved_tree_path=self._tree_item_path(current_parent_data),
                    resolved_node_origin=current_parent_data.get("node_origin", ""),
                    reused_existing=True,
                    created_new=False,
                )
                continue

            folder_name = self._path_segments(branch_path)[-1]
            new_child = self._build_projected_destination_folder_node(folder_name, branch_path, current_parent_data)
            if current_item is not None:
                current_item.addChild(new_child)
                self._refresh_destination_item_visibility(current_item, expand=True)
            else:
                tree.addTopLevelItem(new_child)
            current_item = new_child
            current_parent_data = current_item.data(0, Qt.UserRole) or {}
            self._log_restore_phase(
                "destination_parent_resolution_created_projected",
                requested_parent_path=destination_path,
                normalized_parent_path=normalized_target,
                resolved_tree_path=self._tree_item_path(current_parent_data),
                resolved_node_origin=current_parent_data.get("node_origin", ""),
                reused_existing=False,
                created_new=True,
            )

        return current_item

    def _apply_allocation_children_to_item(self, parent_item):
        if parent_item is None:
            return 0
        parent_data = parent_item.data(0, Qt.UserRole) or {}
        parent_path = self._tree_item_path(parent_data)
        if not parent_path:
            return 0

        matching_moves = self._get_unresolved_allocation_candidates_for_parent(parent_path)
        if not matching_moves:
            return 0

        applied_count = 0
        for move in matching_moves:
            allocation_path = self._allocation_projection_path(move)
            existing_child = self._find_destination_child_by_path(parent_item, allocation_path)
            if existing_child is None:
                existing_child = self._find_destination_child_by_allocation_equivalence(parent_item, move)
            if existing_child is not None:
                self._ensure_planned_allocation_overlay_on_real_item(parent_item, existing_child, move)
                self._mark_allocation_resolved(move)
                applied_count += 1
                self._log_restore_phase(
                    "destination_projection_node_reused",
                    destination_path=allocation_path,
                    normalized_destination_path=allocation_path,
                    parent_path=parent_path,
                    node_state="allocated",
                    reused_existing=True,
                )
                self._log_restore_phase(
                    "destination_replay_duplicate_skipped",
                    destination_path=allocation_path,
                    normalized_destination_path=allocation_path,
                    parent_path=parent_path,
                    node_state="allocated",
                    reused_existing=True,
                )
                continue
            self._remove_placeholder_children(parent_item)
            allocation_item = self._build_allocation_tree_node(move, parent_data)
            parent_item.addChild(allocation_item)
            self._refresh_destination_item_visibility(parent_item, expand=True)
            self._mark_allocation_resolved(move)
            applied_count += 1
            self._log_restore_phase(
                "destination_allocation_attached",
                destination_path=allocation_path,
                normalized_destination_path=allocation_path,
                parent_path=parent_path,
                node_state="allocated",
                applied_count=applied_count,
            )
            self._log_restore_phase(
                "destination_child_attached_to_canonical_parent",
                requested_parent_path=self._allocation_parent_path(move),
                normalized_parent_path=self._canonical_destination_projection_path(self._allocation_parent_path(move)),
                resolved_tree_path=parent_path,
                resolved_node_origin=parent_data.get("node_origin", ""),
                reused_existing=True,
                created_new=False,
                child_destination_path=allocation_path,
            )
        return applied_count

    def _remove_placeholder_children(self, parent_item):
        if parent_item is None:
            return
        removed = 0
        index = parent_item.childCount() - 1
        while index >= 0:
            child = parent_item.child(index)
            child_data = child.data(0, Qt.UserRole) or {}
            if child_data.get("placeholder"):
                parent_item.takeChild(index)
                removed += 1
            index -= 1
        if removed and self._full_trace_enabled():
            pdata = parent_item.data(0, Qt.UserRole) or {}
            pk = str(pdata.get("tree_role", "unknown"))
            self._ui_trace(
                "placeholder",
                "remove_placeholder_children",
                panel_key=pk if pk in ("source", "destination") else "unknown",
                item=parent_item,
                removed_count=removed,
            )

    def _remove_placeholder_children_with_role(self, parent_item, role):
        if parent_item is None or not role:
            return
        removed = 0
        index = parent_item.childCount() - 1
        while index >= 0:
            child = parent_item.child(index)
            child_data = child.data(0, Qt.UserRole) or {}
            if child_data.get("placeholder") and child_data.get("placeholder_role") == role:
                parent_item.takeChild(index)
                removed += 1
            index -= 1
        if removed and self._full_trace_enabled():
            pdata = parent_item.data(0, Qt.UserRole) or {}
            pk = str(pdata.get("tree_role", "unknown"))
            self._ui_trace(
                "placeholder",
                "remove_placeholder_children_with_role",
                panel_key=pk if pk in ("source", "destination") else "unknown",
                item=parent_item,
                removed_count=removed,
                role=role,
            )

    def _find_visible_destination_item_by_path(self, destination_path):
        tree = getattr(self, "destination_tree_widget", None)
        if tree is None:
            return None
        normalized_target = self._canonical_destination_projection_path(destination_path) or self.normalize_memory_path(destination_path)
        emit_restore_match_logs = bool(getattr(self, "_verbose_destination_match_logging", False))
        if emit_restore_match_logs:
            self._log_restore_phase(
                "destination_parent_exact_match_required",
                requested_parent_path=destination_path,
                normalized_parent_path=normalized_target,
            )
        matches = []
        for index in range(tree.topLevelItemCount()):
            for item in self._iter_tree_items(tree.topLevelItem(index)):
                node_data = item.data(0, Qt.UserRole) or {}
                if node_data.get("placeholder"):
                    continue
                visible_path = self._tree_item_path(node_data)
                if not visible_path:
                    continue
                match_details = self._destination_parent_match_details(destination_path, visible_path)
                if match_details["exact_match"]:
                    if emit_restore_match_logs:
                        self._log_restore_phase(
                            "destination_parent_exact_match_succeeded",
                            requested_parent_path=destination_path,
                            normalized_parent_path=match_details["normalized_requested"],
                            candidate_tree_path=visible_path,
                            candidate_normalized_path=match_details["normalized_candidate"],
                            exact_match=True,
                            prefix_only_match=False,
                            accepted_or_rejected="accepted",
                        )
                    matches.append(item)
                    continue
                if match_details["prefix_only_match"]:
                    if emit_restore_match_logs:
                        self._log_restore_phase(
                            "destination_parent_prefix_rejected",
                            requested_parent_path=destination_path,
                            normalized_parent_path=match_details["normalized_requested"],
                            candidate_tree_path=visible_path,
                            candidate_normalized_path=match_details["normalized_candidate"],
                            exact_match=False,
                            prefix_only_match=True,
                            accepted_or_rejected="rejected",
                        )
                    continue
                if emit_restore_match_logs:
                    self._log_restore_phase(
                        "destination_parent_exact_match_failed",
                        requested_parent_path=destination_path,
                        normalized_parent_path=match_details["normalized_requested"],
                        candidate_tree_path=visible_path,
                        candidate_normalized_path=match_details["normalized_candidate"],
                        exact_match=False,
                        prefix_only_match=False,
                        accepted_or_rejected="rejected",
                    )
        return self._select_canonical_destination_item(matches)

    def _build_destination_materialization_paths(self):
        ordered_paths = []
        seen = set()
        for proposed_folder in self.proposed_folders:
            path = self._proposed_parent_path(proposed_folder)
            segments = self._path_segments(path)
            for depth in range(1, len(segments) + 1):
                branch_path = "\\".join(segments[:depth])
                if branch_path and branch_path not in seen:
                    seen.add(branch_path)
                    ordered_paths.append(branch_path)
        for move in self.planned_moves:
            path = self._allocation_parent_path(move)
            segments = self._path_segments(path)
            for depth in range(1, len(segments) + 1):
                branch_path = "\\".join(segments[:depth])
                if branch_path and branch_path not in seen:
                    seen.add(branch_path)
                    ordered_paths.append(branch_path)
        return ordered_paths

    def _start_destination_restore_materialization(self):
        self._destination_restore_materialization_user_paused = False
        self._destination_restore_materialization_queue = []
        self._destination_restore_materialization_seen = set()
        queued_paths = self._build_destination_materialization_paths()
        self._log_restore_phase(
            "destination_restore_materialization_started",
            queue_size=len(queued_paths),
            planned_moves_count=len(self.planned_moves),
            proposed_folders_count=len(self.proposed_folders),
        )
        max_depth = 0
        for destination_path in queued_paths:
            normalized_destination_path = self.normalize_memory_path(destination_path)
            self._destination_restore_materialization_queue.append(normalized_destination_path)
            self._destination_restore_materialization_seen.add(normalized_destination_path)
            max_depth = max(max_depth, self._source_branch_depth(normalized_destination_path))
        self._log_restore_phase(
            "destination_restore_branches_queued_summary",
            branch_count=len(queued_paths),
            max_branch_depth=max_depth,
            queue_size=len(self._destination_restore_materialization_queue),
        )
        self._schedule_destination_restore_materialization_queue(
            "root_bind",
            delay_ms=self._restore_queue_initial_delay_ms,
        )

    def _rebuild_destination_restore_queue_from_unresolved(self):
        ordered_paths = []
        seen = set()
        unresolved_parent_paths = list(self.unresolved_proposed_by_parent_path.keys()) + list(self.unresolved_allocations_by_parent_path.keys())
        for parent_path in unresolved_parent_paths:
            normalized_parent_path = self.normalize_memory_path(parent_path)
            segments = self._path_segments(normalized_parent_path)
            for depth in range(1, len(segments) + 1):
                branch_path = "\\".join(segments[:depth])
                if branch_path and branch_path not in seen:
                    seen.add(branch_path)
                    ordered_paths.append(branch_path)
        self._destination_restore_materialization_queue = ordered_paths
        self._destination_restore_materialization_seen = set(ordered_paths)
        return len(ordered_paths)

    def _schedule_destination_restore_materialization_queue(self, reason, trigger_path="", delay_ms=None):
        delay = self._restore_queue_tick_delay_ms if delay_ms is None else max(0, int(delay_ms))
        QTimer.singleShot(
            delay,
            lambda: self._process_destination_restore_materialization_queue(reason, trigger_path=trigger_path),
        )

    def _process_destination_restore_materialization_queue(self, reason, trigger_path=""):
        if getattr(self, "_destination_restore_materialization_user_paused", False):
            return
        queue = self._destination_restore_materialization_queue
        if not queue:
            if getattr(self, "_sharepoint_lazy_mode", False):
                unresolved_count = self._unresolved_proposed_queue_size() + self._unresolved_allocation_queue_size()
                if unresolved_count > 0:
                    rebuilt_count = self._rebuild_destination_restore_queue_from_unresolved()
                    if rebuilt_count > 0:
                        self._log_restore_phase(
                            "destination_restore_materialization_rebuilt_from_unresolved",
                            queue_size=rebuilt_count,
                            trigger_path=self.normalize_memory_path(trigger_path),
                            reason=reason,
                            unresolved_count=unresolved_count,
                        )
                        queue = self._destination_restore_materialization_queue
            if getattr(self, "_sharepoint_lazy_mode", False):
                if not queue:
                    applied_count = 0
                    applied_count += self._replay_unresolved_proposed_overlay("destination_restore_complete", trigger_path=trigger_path)
                    applied_count += self._replay_unresolved_allocation_overlay("destination_restore_complete", trigger_path=trigger_path)
                    applied_count += self._reconcile_destination_semantic_duplicates("destination_restore_complete")
                    if applied_count:
                        self.destination_tree_widget.viewport().update()
                    if self._unresolved_proposed_queue_size() == 0 and self._unresolved_allocation_queue_size() == 0:
                        self._restore_destination_overlay_pending = False
            if not queue:
                self._log_restore_phase(
                    "destination_restore_materialization_complete",
                    queue_size=0,
                    trigger_path=self.normalize_memory_path(trigger_path),
                    reason=reason,
                )
                self._finalize_memory_restore_if_ready(f"destination_queue_complete:{reason}")
                return

        deferred_paths = []
        initial_queue_size = len(queue)
        load_started = False
        made_progress = False
        processed_count = 0
        max_items_per_tick = 4

        for _ in range(initial_queue_size):
            if processed_count >= max_items_per_tick:
                break
            if not queue:
                break
            destination_path = queue.pop(0)
            processed_count += 1
            item = self._find_visible_destination_item_by_path(destination_path)
            if item is None:
                deferred_paths.append(destination_path)
                self._log_restore_phase(
                    "destination_restore_branch_skipped",
                    destination_path=destination_path,
                    normalized_destination_path=destination_path,
                    queue_size=len(queue) + len(deferred_paths),
                    branch_depth=self._source_branch_depth(destination_path),
                    already_loaded=False,
                    loaded_successfully=False,
                    trigger_path=self.normalize_memory_path(trigger_path),
                    reason=f"{reason}_not_visible_yet",
                    verbose=True,
                )
                continue

            node_data = item.data(0, Qt.UserRole) or {}
            already_loaded = bool(node_data.get("children_loaded")) or not bool(node_data.get("is_folder")) or self.node_is_proposed(node_data) or self.node_is_planned_allocation(node_data)
            pending_key = f"{node_data.get('drive_id', '')}:{node_data.get('id', '')}"
            if pending_key in self.pending_folder_loads["destination"]:
                queue.insert(0, destination_path)
                self._log_restore_phase(
                    "destination_restore_branch_skipped",
                    destination_path=destination_path,
                    normalized_destination_path=destination_path,
                    queue_size=len(queue) + len(deferred_paths),
                    branch_depth=self._source_branch_depth(destination_path),
                    already_loaded=False,
                    loaded_successfully=False,
                    trigger_path=self.normalize_memory_path(trigger_path),
                    reason=f"{reason}_already_pending",
                    verbose=True,
                )
                load_started = True
                break

            if already_loaded:
                applied_count = 0
                applied_count += self._apply_proposed_children_to_item(item)
                applied_count += self._apply_allocation_children_to_item(item)
                if applied_count:
                    self.destination_tree_widget.viewport().update()
                made_progress = True
                self._log_restore_phase(
                    "destination_restore_branch_loaded",
                    destination_path=destination_path,
                    normalized_destination_path=destination_path,
                    queue_size=len(queue) + len(deferred_paths),
                    branch_depth=self._source_branch_depth(destination_path),
                    already_loaded=True,
                    loaded_successfully=True,
                    applied_count=applied_count,
                    trigger_path=self.normalize_memory_path(trigger_path),
                    verbose=True,
                )
                item.setExpanded(True)
                continue

            self._log_restore_phase(
                "destination_restore_branch_expand_requested",
                destination_path=destination_path,
                normalized_destination_path=destination_path,
                queue_size=len(queue) + len(deferred_paths),
                branch_depth=self._source_branch_depth(destination_path),
                already_loaded=False,
                loaded_successfully=False,
                trigger_path=self.normalize_memory_path(trigger_path),
                verbose=True,
            )
            self.destination_tree_widget.expandItem(item)
            item.setExpanded(True)
            load_started = True
            break

        queue[:0] = deferred_paths

        if not queue:
            if getattr(self, "_sharepoint_lazy_mode", False):
                if self._unresolved_proposed_queue_size() == 0 and self._unresolved_allocation_queue_size() == 0:
                    self._restore_destination_overlay_pending = False
            self._log_restore_phase(
                "destination_restore_materialization_complete",
                queue_size=0,
                trigger_path=self.normalize_memory_path(trigger_path),
                reason=reason,
            )
            self._finalize_memory_restore_if_ready(f"destination_queue_complete:{reason}")
        elif made_progress and not load_started:
            self._schedule_destination_restore_materialization_queue(reason, trigger_path=trigger_path)
        elif processed_count >= max_items_per_tick and not load_started:
            self._schedule_destination_restore_materialization_queue(reason, trigger_path=trigger_path)

    def _apply_proposed_children_to_item(self, parent_item):
        if parent_item is None:
            self._log_restore_phase("proposed_overlay_skipped", reason="parent_item_none")
            return 0

        parent_data = parent_item.data(0, Qt.UserRole) or {}
        parent_path = self._tree_item_path(parent_data)
        parent_variants = sorted(self._normalized_path_variants(parent_path, "destination"))
        if not parent_path:
            self._log_restore_phase("proposed_overlay_skipped", reason="missing_parent_path")
            return 0

        matching_candidates = self._get_unresolved_candidates_for_parent(parent_path)
        if not matching_candidates:
            self._log_restore_phase(
                "proposed_overlay_attempt",
                parent_path=parent_path,
                parent_path_variants=parent_variants,
                proposed_count=len(self.proposed_folders),
                applied_count=0,
                matched_candidate_count=0,
                queue_size=self._unresolved_proposed_queue_size(),
            )
            return 0

        applied_count = 0
        for proposed_folder in matching_candidates:
            stored_parent_path = proposed_folder.ParentPath if isinstance(proposed_folder, ProposedFolder) else ""
            normalized_parent_path = self._proposed_parent_path(proposed_folder)
            match_succeeded = self._paths_equivalent(normalized_parent_path, parent_path, "destination")
            self._log_restore_phase(
                "proposed_overlay_parent_check",
                stored_parent_path=stored_parent_path,
                normalized_parent_path=normalized_parent_path,
                candidate_tree_path=parent_path,
                candidate_tree_variants=parent_variants,
                match_succeeded=match_succeeded,
            )
            if not match_succeeded:
                continue
            if self._proposed_folder_exists_under(parent_item, self._proposed_destination_path(proposed_folder)):
                self._mark_proposed_folder_resolved(proposed_folder)
                self._log_restore_phase(
                    "proposed_overlay_node_skipped",
                    reason="already_exists_under_parent",
                    parent_path=parent_path,
                    destination_path=self._proposed_destination_path(proposed_folder),
                    queue_size=self._unresolved_proposed_queue_size(),
                )
                continue
            self._remove_placeholder_children(parent_item)
            parent_item.addChild(self._build_proposed_tree_node(proposed_folder, parent_data))
            self._refresh_destination_item_visibility(parent_item, expand=True)
            self._mark_proposed_folder_resolved(proposed_folder)
            applied_count += 1
            self._log_restore_phase(
                "destination_child_attached_to_canonical_parent",
                requested_parent_path=stored_parent_path,
                normalized_parent_path=normalized_parent_path,
                resolved_tree_path=parent_path,
                resolved_node_origin=parent_data.get("node_origin", ""),
                reused_existing=True,
                created_new=False,
                child_destination_path=self._proposed_destination_path(proposed_folder),
            )
        self._log_restore_phase(
            "proposed_overlay_attempt",
            parent_path=parent_path,
            parent_path_variants=parent_variants,
            proposed_count=len(self.proposed_folders),
            applied_count=applied_count,
            matched_candidate_count=len(matching_candidates),
            queue_size=self._unresolved_proposed_queue_size(),
        )
        return applied_count

    def _replay_unresolved_proposed_overlay(self, reason, trigger_path=""):
        queue_size = self._unresolved_proposed_queue_size()
        self._log_restore_phase(
            "unresolved_proposed_replay_attempted",
            reason=reason,
            trigger_path=self.normalize_memory_path(trigger_path),
            queue_size=queue_size,
        )
        if queue_size == 0:
            return 0

        tree = getattr(self, "destination_tree_widget", None)
        if tree is None or tree.topLevelItemCount() == 0:
            self._log_restore_phase(
                "unresolved_proposed_still_waiting",
                reason="destination_tree_not_ready",
                trigger_path=self.normalize_memory_path(trigger_path),
                queue_size=self._unresolved_proposed_queue_size(),
            )
            return 0

        applied_count = 0
        pending_candidates = []
        for bucket in self.unresolved_proposed_by_parent_path.values():
            pending_candidates.extend(bucket.values())
        for proposed_folder in pending_candidates:
            parent_item = self._ensure_destination_projection_path(self._proposed_parent_path(proposed_folder))
            if parent_item is not None:
                applied_count += self._apply_proposed_children_to_item(parent_item)

        if applied_count > 0:
            self._log_restore_phase(
                "unresolved_proposed_replay_applied",
                reason=reason,
                trigger_path=self.normalize_memory_path(trigger_path),
                applied_count=applied_count,
                queue_size=self._unresolved_proposed_queue_size(),
            )

        if self._unresolved_proposed_queue_size() > 0:
            self._log_restore_phase(
                "unresolved_proposed_still_waiting",
                reason=reason,
                trigger_path=self.normalize_memory_path(trigger_path),
                unresolved_parent_paths=sorted(self.unresolved_proposed_by_parent_path.keys()),
                queue_size=self._unresolved_proposed_queue_size(),
            )

        return applied_count

    def _replay_unresolved_allocation_overlay(self, reason, trigger_path=""):
        queue_size = self._unresolved_allocation_queue_size()
        self._log_restore_phase(
            "unresolved_allocation_replay_attempted",
            reason=reason,
            trigger_path=self.normalize_memory_path(trigger_path),
            queue_size=queue_size,
        )
        if queue_size == 0:
            return 0

        tree = getattr(self, "destination_tree_widget", None)
        if tree is None or tree.topLevelItemCount() == 0:
            self._log_restore_phase(
                "unresolved_allocation_still_waiting",
                reason="destination_tree_not_ready",
                trigger_path=self.normalize_memory_path(trigger_path),
                queue_size=self._unresolved_allocation_queue_size(),
            )
            return 0

        applied_count = 0
        pending_moves = []
        for bucket in self.unresolved_allocations_by_parent_path.values():
            pending_moves.extend(bucket.values())
        for move in pending_moves:
            parent_item = self._ensure_destination_projection_path(self._allocation_parent_path(move))
            if parent_item is not None:
                applied_count += self._apply_allocation_children_to_item(parent_item)

        if applied_count > 0:
            self._log_restore_phase(
                "destination_replay_projection_complete",
                reason=reason,
                trigger_path=self.normalize_memory_path(trigger_path),
                applied_count=applied_count,
                queue_size=self._unresolved_allocation_queue_size(),
            )

        if self._unresolved_allocation_queue_size() > 0:
            self._log_restore_phase(
                "unresolved_allocation_still_waiting",
                reason=reason,
                trigger_path=self.normalize_memory_path(trigger_path),
                unresolved_parent_paths=sorted(self.unresolved_allocations_by_parent_path.keys()),
                queue_size=self._unresolved_allocation_queue_size(),
            )

        return applied_count

    def get_source_item_display_name(self, node_data, fallback_text=""):
        if node_data.get("placeholder"):
            return fallback_text

        base_name = node_data.get("name") or fallback_text or "Unnamed Item"
        if bool(node_data.get("submitted_visual")):
            return f"{base_name} [Submitted]"
        return base_name

    def build_source_relationship_key(self, node_data):
        if not node_data:
            return None

        return self.build_node_key(node_data, "source")

    def source_item_path_is_descendant_of(self, child_path, parent_path):
        return self._path_is_descendant(child_path, parent_path, "source")

    def get_source_relationship_display(self, node_data):
        relationship = self._evaluate_source_relationship(node_data)
        return {"mode": relationship.get("mode", "none"), "suffix": relationship.get("suffix", "")}

    def _display_detail_value(self, value, fallback="Not available"):
        if value is None:
            return fallback
        text = str(value).strip()
        return text if text else fallback

    def _format_node_origin(self, node_data):
        origin = str(node_data.get("node_origin", "")).strip()
        if not origin:
            return "Real"
        mapping = {
            "projecteddestination": "Projected",
            "plannedallocation": "Allocated",
            "projectedallocationdescendant": "Projected descendant",
            "proposed": "Proposed future-state folder",
        }
        return mapping.get(origin.lower(), origin)

    def _format_item_size(self, node_data):
        size = node_data.get("size", 0)
        if isinstance(size, int) and size > 0:
            return f"{size:,} bytes"
        if node_data.get("is_folder"):
            return "Folder"
        return "Not available"

    def _library_context_text(self, node_data):
        site_name = node_data.get("site_name", "")
        library_name = node_data.get("library_name", "")
        parts = [part for part in [site_name, library_name] if part]
        return " / ".join(parts) if parts else "Not available"

    def _find_planned_move_for_destination_node(self, node_data):
        if node_data is None:
            return None

        destination_path = self._canonical_destination_projection_path(
            node_data.get("destination_path", "")
            or node_data.get("item_path", "")
            or node_data.get("display_path", "")
            or self._tree_item_path(node_data)
        )
        source_trace_path = self._canonical_source_projection_path(node_data.get("source_path", ""))

        best_move = None
        best_length = -1
        for move in self.planned_moves:
            allocation_path = self._canonical_destination_projection_path(self._allocation_projection_path(move))
            if destination_path and allocation_path and destination_path == allocation_path:
                return move

            move_source_path = self._canonical_source_projection_path(move.get("source_path", ""))
            if source_trace_path and move_source_path and (
                self._paths_equivalent(source_trace_path, move_source_path, "source")
                or self._path_is_descendant(source_trace_path, move_source_path, "source")
            ):
                if len(move_source_path) > best_length:
                    best_move = move
                    best_length = len(move_source_path)

        return best_move

    def _find_proposed_folder_for_node(self, node_data):
        destination_path = self._canonical_destination_projection_path(self._tree_item_path(node_data))
        for proposed_folder in self.proposed_folders:
            proposed_path = self._canonical_destination_projection_path(proposed_folder.DestinationPath)
            if proposed_path and destination_path and proposed_path == destination_path:
                return proposed_folder
        return None

    def _matching_workflow_rows_for_source_path(self, rows, source_path):
        canonical_source_path = self._canonical_source_projection_path(source_path)
        matches = []
        for row in rows:
            row_source_path = self._canonical_source_projection_path(row.get("source_path", ""))
            if row_source_path and canonical_source_path and row_source_path == canonical_source_path:
                matches.append(row)
        return matches

    def _resolve_selection_planning_state(self, panel_key, node_data, traceability=None):
        item_name = node_data.get("name", "Unnamed Item")
        item_path = self._tree_item_path(node_data)

        if panel_key != "source":
            move = None
            if isinstance(traceability, dict):
                move = traceability.get("move")
            state = {
                "mode": "direct" if move is not None else "none",
                "direct_mapping": move is not None,
                "inherited_mapping": False,
                "inherited_from": "",
                "resolved_destination_path": move.get("destination_path", "") if move is not None else "",
                "move": move,
            }
            log_info(
                "selection_planning_state_resolved",
                item_name=item_name,
                item_path=item_path,
                tree_role=panel_key,
                direct_mapping=state["direct_mapping"],
                inherited_mapping=state["inherited_mapping"],
                inherited_from=state["inherited_from"],
                resolved_destination_path=state["resolved_destination_path"],
            )
            return state

        source_path = self._tree_item_path(node_data)
        direct_move = None
        inherited_move = None
        inherited_path_length = -1

        for move in self.planned_moves:
            move_source = move.get("source", {})
            move_source_path = self._tree_item_path(move_source)
            normalized_move_source_path = self._canonical_source_projection_path(move_source_path)
            move_drive_id = move_source.get("drive_id", "")
            node_drive_id = node_data.get("drive_id", "")

            if self.node_keys_match(move_source, node_data, "source"):
                direct_move = move
                break

            same_source_tree = (
                move_source.get("tree_role", "source") == node_data.get("tree_role", "source")
                and (not move_drive_id or not node_drive_id or move_drive_id == node_drive_id)
            )
            if not same_source_tree and move_drive_id and node_drive_id:
                continue

            if self.source_item_path_is_descendant_of(source_path, move_source_path):
                if len(normalized_move_source_path) > inherited_path_length:
                    inherited_move = move
                    inherited_path_length = len(normalized_move_source_path)

        if direct_move is not None:
            state = {
                "mode": "direct",
                "direct_mapping": True,
                "inherited_mapping": False,
                "inherited_from": "",
                "resolved_destination_path": direct_move.get("destination_path", ""),
                "move": direct_move,
            }
            log_info(
                "selection_planning_state_direct",
                item_name=item_name,
                item_path=item_path,
                tree_role=panel_key,
                direct_mapping=True,
                inherited_mapping=False,
                inherited_from="",
                resolved_destination_path=state["resolved_destination_path"],
            )
        elif inherited_move is not None:
            state = {
                "mode": "inherited",
                "direct_mapping": False,
                "inherited_mapping": True,
                "inherited_from": inherited_move.get("source_name", ""),
                "resolved_destination_path": inherited_move.get("destination_path", ""),
                "move": inherited_move,
            }
            log_info(
                "selection_planning_state_inherited",
                item_name=item_name,
                item_path=item_path,
                tree_role=panel_key,
                direct_mapping=False,
                inherited_mapping=True,
                inherited_from=state["inherited_from"],
                resolved_destination_path=state["resolved_destination_path"],
            )
        else:
            state = {
                "mode": "none",
                "direct_mapping": False,
                "inherited_mapping": False,
                "inherited_from": "",
                "resolved_destination_path": "",
                "move": None,
            }
            log_info(
                "selection_planning_state_unplanned",
                item_name=item_name,
                item_path=item_path,
                tree_role=panel_key,
                direct_mapping=False,
                inherited_mapping=False,
                inherited_from="",
                resolved_destination_path="",
            )

        log_info(
            "selection_planning_state_resolved",
            item_name=item_name,
            item_path=item_path,
            tree_role=panel_key,
            direct_mapping=state["direct_mapping"],
            inherited_mapping=state["inherited_mapping"],
            inherited_from=state["inherited_from"],
            resolved_destination_path=state["resolved_destination_path"],
        )
        return state

    def _resolve_source_traceability(self, panel_key, node_data):
        source_path = ""
        source_node = None
        move = None

        if panel_key == "source":
            source_path = self._canonical_source_projection_path(self._tree_item_path(node_data))
            source_node = dict(node_data)
            planning_state = self._resolve_selection_planning_state(panel_key, node_data)
            move = planning_state.get("move")
        else:
            source_path = self._canonical_source_projection_path(node_data.get("source_path", ""))
            move = self._find_planned_move_for_destination_node(node_data)
            if not source_path and move is not None:
                source_path = self._canonical_source_projection_path(move.get("source_path", ""))
            if source_path:
                source_item = self._find_visible_source_item_by_path(source_path) or self._find_source_item_for_planned_move(move or {})
                if source_item is not None:
                    source_node = source_item.data(0, Qt.UserRole) or {}

        return {
            "traceable_to_source": bool(source_path),
            "source_path": source_path,
            "source_node": dict(source_node) if isinstance(source_node, dict) else (source_node or {}),
            "move": move,
        }

    def _resolve_selection_metadata(self, panel_key, node_data, traceability):
        raw = node_data.get("raw", {}) if isinstance(node_data.get("raw", {}), dict) else {}
        item_path = self._display_detail_value(self._tree_item_path(node_data))
        item_type = "Folder" if node_data.get("is_folder") else "File"
        source_path = traceability.get("source_path", "")
        planning_state_info = self._resolve_selection_planning_state(panel_key, node_data, traceability)
        move = planning_state_info.get("move")
        needs_review = self._has_needs_review_for_source_path(source_path or item_path)

        if panel_key == "source":
            if needs_review:
                planning_state = "Needs Review"
                destination_path = planning_state_info.get("resolved_destination_path", "")
            elif planning_state_info.get("mode") == "direct":
                planning_state = "Direct mapping"
                destination_path = planning_state_info.get("resolved_destination_path", "")
            elif planning_state_info.get("mode") == "inherited":
                planning_state = "Inherited mapping"
                destination_path = planning_state_info.get("resolved_destination_path", "")
            else:
                planning_state = "Unplanned"
                destination_path = ""
        else:
            destination_path = item_path
            if self.node_is_proposed(node_data):
                planning_state = "Proposed"
            elif self.node_is_planned_allocation(node_data):
                planning_state = "Allocated"
            elif str(node_data.get("node_origin", "")).lower() == "projectedallocationdescendant":
                planning_state = "Projected descendant"
            elif str(node_data.get("node_origin", "")).lower() == "projecteddestination":
                planning_state = "Projected"
            elif move is not None:
                planning_state = "Direct mapping"
            else:
                planning_state = "Unplanned"

        return {
            "item_name": self._display_detail_value(node_data.get("name"), "Unnamed Item"),
            "item_path": item_path,
            "item_type": item_type,
            "item_area": "Source" if panel_key == "source" else "Destination",
            "node_origin": self._format_node_origin(node_data),
            "planning_state": planning_state,
            "destination_path": self._display_detail_value(destination_path, "Not mapped"),
            "source_path": self._display_detail_value(source_path, "Preview-only item" if panel_key == "destination" else "Not available"),
            "item_size": self._format_item_size(node_data),
            "item_modified": self._display_detail_value(raw.get("lastModifiedDateTime") or node_data.get("last_modified")),
            "library_context": self._library_context_text(node_data),
            "item_link": self._display_detail_value(node_data.get("web_url")),
        }

    def _resolve_selection_notes_preview(self, panel_key, node_data, traceability, metadata):
        planning_state_info = self._resolve_selection_planning_state(panel_key, node_data, traceability)
        move = planning_state_info.get("move")
        source_matches = self._matching_workflow_rows_for_source_path(self._workflow_needs_review_rows, traceability.get("source_path", ""))
        suggestion_matches = self._matching_workflow_rows_for_source_path(self._workflow_suggestion_rows, traceability.get("source_path", ""))
        proposed_folder = self._find_proposed_folder_for_node(node_data) if panel_key == "destination" else None

        if panel_key == "source":
            if planning_state_info.get("mode") == "direct" and move is not None:
                body_text = f"This source {metadata['item_type'].lower()} is directly mapped to {move.get('destination_path', 'the planned destination')}."
            elif planning_state_info.get("mode") == "inherited" and move is not None:
                body_text = f"This source {metadata['item_type'].lower()} inherits its mapping via {planning_state_info.get('inherited_from') or move.get('source_name', 'a mapped parent')}."
            else:
                body_text = f"This source {metadata['item_type'].lower()} is currently not planned."
        else:
            if self.node_is_proposed(node_data):
                body_text = "This destination folder is a proposed future-state folder."
            elif self.node_is_planned_allocation(node_data):
                body_text = f"This destination {metadata['item_type'].lower()} is an allocated future-state node for {metadata['source_path']}."
            elif str(node_data.get("node_origin", "")).lower() == "projectedallocationdescendant":
                body_text = "This destination item is a projected descendant of an allocated source folder."
            elif str(node_data.get("node_origin", "")).lower() == "projecteddestination":
                body_text = "This destination folder is a projected future-state branch."
            else:
                body_text = f"This destination {metadata['item_type'].lower()} is a live SharePoint item."

        notes_lines = [body_text]
        if move is not None:
            notes_lines.append("")
            notes_lines.append(f"Source: {self._display_detail_value(move.get('source_path', ''), 'Not available')}")
            notes_lines.append(f"Destination: {self._display_detail_value(move.get('destination_path', ''), 'Not available')}")
        if proposed_folder is not None:
            notes_lines.append("")
            notes_lines.append("Proposed folder awaiting final confirmation.")
        if source_matches:
            notes_lines.append("")
            for row in source_matches:
                notes_lines.append(f"Needs review: {row.get('reason', '')}")
        if suggestion_matches:
            notes_lines.append("")
            for row in suggestion_matches:
                notes_lines.append(f"Suggestion: {row.get('reason', '')} -> {row.get('destination_path', '')}")

        if metadata["item_type"] == "File":
            preview_text = (
                f"{metadata['item_type']} preview summary\n\n"
                f"Name: {metadata['item_name']}\n"
                f"State: {metadata['planning_state']}\n"
                f"Size: {metadata['item_size']}\n"
                f"Modified: {metadata['item_modified']}"
            )
        else:
            preview_text = (
                f"{metadata['item_type']} preview summary\n\n"
                f"State: {metadata['planning_state']}\n"
                f"Path: {metadata['item_path']}\n"
                f"Library: {metadata['library_context']}"
            )

        return {
            "body_text": body_text,
            "notes_text": "\n".join(notes_lines).strip(),
            "preview_text": preview_text,
        }

    def _preview_target_for_context(self, context):
        if not context:
            return {}

        panel_key = context.get("panel_key", "")
        node_data = context.get("node_data", {}) or {}
        traceability = context.get("traceability", {}) or {}
        source_node = traceability.get("source_node", {}) if isinstance(traceability.get("source_node", {}), dict) else {}

        if panel_key == "source":
            target = node_data
        else:
            node_origin = str(node_data.get("node_origin", "")).lower()
            if node_data.get("is_folder"):
                target = {}
            elif node_data.get("drive_id") and node_data.get("id") and not (
                self.node_is_proposed(node_data)
                or self.node_is_planned_allocation(node_data)
                or node_origin in {"projectedallocationdescendant", "projecteddestination"}
            ):
                target = node_data
            else:
                target = source_node

        if not target or target.get("is_folder"):
            return {}

        return {
            "drive_id": target.get("drive_id", ""),
            "item_id": target.get("id", ""),
            "name": target.get("name", ""),
            "path": self._tree_item_path(target),
        }

    def _preview_file_extension(self, name):
        suffix = Path(str(name or "")).suffix.lower()
        return suffix

    def _preview_fallback_text(self, context):
        metadata = context.get("metadata", {}) if context else {}
        item_type = metadata.get("item_type", "Item")
        planning_state = metadata.get("planning_state", "Not available")
        item_name = metadata.get("item_name", "Selected item")
        item_path = metadata.get("item_path", "Not available")
        return (
            f"{item_type} preview summary\n\n"
            f"Name: {item_name}\n"
            f"State: {planning_state}\n"
            f"Path: {item_path}\n"
            "A text preview is not available for this file type."
        )

    def _extract_docx_preview_text(self, content):
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                document_xml = archive.read("word/document.xml")
        except Exception:
            return ""

        try:
            root = ET.fromstring(document_xml)
        except Exception:
            return ""

        texts = []
        for element in root.iter():
            if element.tag.endswith("}t") and element.text:
                texts.append(element.text)
            elif element.tag.endswith("}p"):
                texts.append("\n")
        extracted = "".join(texts)
        extracted = re.sub(r"\n{3,}", "\n\n", extracted)
        return extracted.strip()

    def _extract_preview_text(self, item_name, content):
        extension = self._preview_file_extension(item_name)
        text_extensions = {
            ".txt", ".md", ".csv", ".json", ".xml", ".html", ".htm",
            ".log", ".ps1", ".py", ".js", ".ts", ".css", ".yml", ".yaml",
            ".ini", ".cfg", ".bat", ".sql",
        }

        if extension == ".docx":
            text = self._extract_docx_preview_text(content)
        elif extension in text_extensions or not extension:
            text = ""
            for encoding in ("utf-8-sig", "utf-8", "utf-16", "cp1252", "latin-1"):
                try:
                    text = content.decode(encoding)
                    break
                except Exception:
                    continue
        else:
            text = ""

        if not text:
            return ""

        text = text.replace("\x00", "")
        text = re.sub(r"\r\n?", "\n", text)
        text = text.strip()
        if len(text) > 12000:
            text = text[:12000].rstrip() + "\n\n[Preview truncated]"
        return text

    def _set_details_preview_text(self, text):
        if hasattr(self, "details_preview"):
            self.details_preview.setPlainText(text)

    def _start_selection_preview(self, context):
        target = self._preview_target_for_context(context)
        if not target:
            self._set_details_preview_text(context.get("notes_preview", {}).get("preview_text", "Preview not available."))
            return

        cache_key = (target.get("drive_id", ""), target.get("item_id", ""))
        cached_text = self._preview_text_cache.get(cache_key)
        if cached_text:
            self._set_details_preview_text(cached_text)
            return

        self._preview_request_sequence += 1
        request_id = self._preview_request_sequence
        previous_request_id = self._active_preview_request_id
        self._active_preview_request_id = request_id
        self._set_details_preview_text("Loading file preview...")

        if self._preview_worker is not None and self._preview_worker.isRunning():
            self._retired_preview_workers[previous_request_id] = self._preview_worker

        worker = FilePreviewWorker(
            self.graph,
            target.get("drive_id", ""),
            target.get("item_id", ""),
            item_name=target.get("name", ""),
        )
        self._preview_worker = worker
        worker.success.connect(lambda payload, request_id=request_id, context=context: self._safe_invoke("preview.success", self.on_preview_success, payload, request_id, context))
        worker.error.connect(lambda payload, request_id=request_id, context=context: self._safe_invoke("preview.error", self.on_preview_error, payload, request_id, context))
        worker.finished.connect(lambda request_id=request_id: self._safe_invoke("preview.finished", self.on_preview_finished, request_id))
        worker.start()

    def on_preview_success(self, payload, request_id, context):
        if request_id != self._active_preview_request_id:
            return

        drive_id = payload.get("drive_id", "")
        item_id = payload.get("item_id", "")
        item_name = payload.get("item_name", "")
        content = payload.get("content", b"") or b""
        preview_text = self._extract_preview_text(item_name, content)
        if not preview_text:
            preview_text = self._preview_fallback_text(context)
        self._preview_text_cache[(drive_id, item_id)] = preview_text
        self._set_details_preview_text(preview_text)
        log_info("selection_preview_loaded", item_name=item_name, item_id=item_id, preview_length=len(preview_text))

    def on_preview_error(self, payload, request_id, context):
        if request_id != self._active_preview_request_id:
            return
        preview_text = self._preview_fallback_text(context)
        error = payload.get("error", "Unknown preview error.")
        self._set_details_preview_text(f"{preview_text}\n\nPreview could not be loaded.\n{error}")
        log_warn("selection_preview_failed", item_name=payload.get("item_name", ""), item_id=payload.get("item_id", ""), error=error)

    def on_preview_finished(self, request_id):
        if request_id == self._active_preview_request_id:
            worker = self._preview_worker
            self._preview_worker = None
        else:
            worker = self._retired_preview_workers.pop(request_id, None)
        if worker is not None:
            worker.deleteLater()

    def _resolve_selection_actions(self, panel_key, node_data, traceability, metadata, planning_state=None):
        source_node = traceability.get("source_node", {}) if isinstance(traceability.get("source_node", {}), dict) else {}
        open_target = ""
        browser_url = ""
        copy_link = ""
        open_source_target = ""
        open_source_mode = ""
        dest_jump_target = ""
        dest_jump_enabled = False

        node_origin = str(node_data.get("node_origin", "")).lower()
        is_destination_real = panel_key == "destination" and not (
            self.node_is_proposed(node_data)
            or self.node_is_planned_allocation(node_data)
            or node_origin in {"projectedallocationdescendant", "projecteddestination"}
        )

        if panel_key == "source":
            open_target = node_data.get("local_path") or node_data.get("file_path") or node_data.get("web_url", "")
            browser_url = node_data.get("web_url", "")
            copy_link = browser_url or metadata["item_path"]
            if node_data.get("is_folder"):
                open_source_target = traceability.get("source_path", metadata["item_path"])
                open_source_mode = "select_source_item"
            else:
                open_source_target = self._source_parent_path(traceability.get("source_path", metadata["item_path"]))
                open_source_mode = "select_source_container"
        else:
            open_target = source_node.get("local_path") or source_node.get("file_path") or source_node.get("web_url", "")
            browser_url = node_data.get("web_url", "")
            copy_link = browser_url or metadata["item_path"]
            if is_destination_real and browser_url:
                open_source_target = browser_url
                open_source_mode = "open_destination_sharepoint"
            elif traceability.get("source_path"):
                open_source_target = traceability.get("source_path", "")
                open_source_mode = "select_traceable_source"

        if panel_key == "source" and isinstance(planning_state, dict):
            mode = planning_state.get("mode")
            move = planning_state.get("move")
            if mode in ("direct", "inherited") and move:
                dest_jump_target = self._destination_jump_path_for_source_item(node_data, move)
                dest_jump_enabled = bool(dest_jump_target)

        actions = {
            "Open File": {
                "enabled": bool(not node_data.get("is_folder") and open_target),
                "target": open_target,
                "tooltip": "Open this file directly." if (not node_data.get("is_folder") and open_target) else "Open File is only available for files with a usable target.",
            },
            "Open in SharePoint": {
                "enabled": bool(browser_url),
                "target": browser_url,
                "tooltip": "Open this item in SharePoint." if browser_url else "This preview item does not have a SharePoint URL.",
            },
            "Copy Link": {
                "enabled": bool(copy_link),
                "target": copy_link,
                "tooltip": "Copy the SharePoint URL or best available path." if copy_link else "No usable link is available for this item.",
            },
            "Open Source Folder": {
                "enabled": bool(open_source_target),
                "target": open_source_target,
                "mode": open_source_mode,
                "tooltip": (
                    "Open this real destination folder in SharePoint."
                    if open_source_mode == "open_destination_sharepoint"
                    else "Select the related source folder in the source tree."
                ) if open_source_target else "This item cannot be traced back to a source folder.",
            },
            "Go to Destination": {
                "enabled": dest_jump_enabled,
                "target": dest_jump_target,
                "tooltip": (
                    "Select this item on the destination tree (folder, file, or path under an assigned folder)."
                    if dest_jump_enabled
                    else "Only assigned source items (including inherited mappings) can jump to the destination tree."
                ),
            },
        }
        return actions

    def _resolve_selected_item_context(self, panel_key, node_data):
        traceability = self._resolve_source_traceability(panel_key, node_data)
        planning_state = self._resolve_selection_planning_state(panel_key, node_data, traceability)
        metadata = self._resolve_selection_metadata(panel_key, node_data, traceability)
        notes_preview = self._resolve_selection_notes_preview(panel_key, node_data, traceability, metadata)
        actions = self._resolve_selection_actions(
            panel_key, node_data, traceability, metadata, planning_state=planning_state
        )
        context = {
            "panel_key": panel_key,
            "node_data": dict(node_data),
            "traceability": traceability,
            "planning_state": planning_state,
            "metadata": metadata,
            "notes_preview": notes_preview,
            "actions": actions,
        }
        log_info(
            "selection_details_context_resolved",
            item_name=metadata["item_name"],
            item_path=metadata["item_path"],
            tree_role=panel_key,
            node_origin=metadata["node_origin"],
            traceable_to_source=traceability.get("traceable_to_source", False),
            selected_item_type=metadata["item_type"],
        )
        return context

    def _update_selection_details(self, context):
        metadata = context["metadata"]
        notes_preview = context["notes_preview"]

        metadata_lines = [
            f"Item Name: {self._display_detail_value(metadata.get('item_name'))}",
            f"Full Path: {self._display_detail_value(metadata.get('item_path'))}",
            f"Item Type: {self._display_detail_value(metadata.get('item_type'))}",
            f"Tree Role: {self._display_detail_value(metadata.get('item_area'))}",
            f"Node Origin: {self._display_detail_value(metadata.get('node_origin'))}",
            f"Planning State: {self._display_detail_value(metadata.get('planning_state'))}",
            f"Destination Path: {self._display_detail_value(metadata.get('destination_path'), 'Not mapped')}",
            f"Source Path: {self._display_detail_value(metadata.get('source_path'), 'Not traceable')}",
            f"Size: {self._display_detail_value(metadata.get('item_size'))}",
            f"Modified: {self._display_detail_value(metadata.get('item_modified'))}",
            f"Library Context: {self._display_detail_value(metadata.get('library_context'))}",
            f"Link: {self._display_detail_value(metadata.get('item_link'))}",
        ]
        if hasattr(self, "details_metadata_summary"):
            self.details_metadata_summary.setPlainText("\n".join(metadata_lines))
        if hasattr(self, "details_notes"):
            self.details_notes.setPlainText(notes_preview["notes_text"])
        self.details_preview.setPlainText(notes_preview["preview_text"])
        self._current_details_context = context
        self._current_details_node_data = dict(context["node_data"])
        self._current_details_panel_key = context["panel_key"]
        self._start_selection_preview(context)

        log_info(
            "selection_details_metadata_bound",
            item_name=metadata["item_name"],
            item_path=metadata["item_path"],
            tree_role=context["panel_key"],
            node_origin=metadata["node_origin"],
            selected_item_type=metadata["item_type"],
        )
        log_info(
            "selection_metadata_bound",
            item_name=metadata["item_name"],
            item_path=metadata["item_path"],
            tree_role=context["panel_key"],
            direct_mapping=context.get("planning_state", {}).get("direct_mapping", False),
            inherited_mapping=context.get("planning_state", {}).get("inherited_mapping", False),
            inherited_from=context.get("planning_state", {}).get("inherited_from", ""),
            resolved_destination_path=context.get("planning_state", {}).get("resolved_destination_path", ""),
        )
        log_info(
            "selection_details_notes_updated",
            item_name=metadata["item_name"],
            item_path=metadata["item_path"],
            tree_role=context["panel_key"],
            node_origin=metadata["node_origin"],
            traceable_to_source=context["traceability"].get("traceable_to_source", False),
            selected_item_type=metadata["item_type"],
        )

    def _select_source_item_by_path(self, source_path, prefer_container=False):
        target_path = self._canonical_source_projection_path(source_path)
        if prefer_container:
            parent_path = self._source_parent_path(target_path)
            if parent_path:
                target_path = parent_path
        source_item = self._find_visible_source_item_by_path(target_path)
        if source_item is None:
            return False

        parent = source_item.parent()
        while parent is not None:
            parent.setExpanded(True)
            parent = parent.parent()
        self.source_tree_widget.clearSelection()
        self.source_tree_widget.setCurrentItem(source_item)
        source_item.setSelected(True)
        self.source_tree_widget.scrollToItem(source_item)
        self.on_tree_selection_changed("source")
        return True

    def _build_source_navigation_paths(self, target_path):
        canonical_target = self._canonical_source_projection_path(target_path)
        segments = self._path_segments(canonical_target)
        if len(segments) <= 1:
            return []
        prefixes = []
        for index in range(1, len(segments)):
            prefixes.append("\\".join(segments[:index]))
        return prefixes

    def _process_pending_source_navigation(self, reason, trigger_path=""):
        request = self._pending_source_navigation
        if not request:
            return False

        target_path = request.get("target_path", "")
        prefer_container = bool(request.get("prefer_container"))
        if self._select_source_item_by_path(target_path, prefer_container=prefer_container):
            self._pending_source_navigation = None
            return True

        queue = request.get("queue", [])
        while queue:
            source_path = queue[0]
            item = self._find_visible_source_item_by_path(source_path)
            if item is None:
                break

            node_data = item.data(0, Qt.UserRole) or {}
            if not node_data.get("is_folder"):
                queue.pop(0)
                continue

            pending_key = f"{node_data.get('drive_id', '')}:{node_data.get('id', '')}"
            if pending_key in self.pending_folder_loads["source"]:
                break

            queue.pop(0)
            if bool(node_data.get("children_loaded")):
                continue

            self.source_tree_widget.expandItem(item)
            break

        if not queue and self._select_source_item_by_path(target_path, prefer_container=prefer_container):
            self._pending_source_navigation = None
            return True
        return False

    def _start_source_navigation(self, source_path, prefer_container=False):
        target_path = self._canonical_source_projection_path(source_path)
        if prefer_container:
            parent_path = self._source_parent_path(target_path)
            if parent_path:
                target_path = parent_path
        if not target_path:
            return False
        if self._select_source_item_by_path(target_path, prefer_container=False):
            return True

        self._pending_source_navigation = {
            "target_path": target_path,
            "prefer_container": False,
            "queue": self._build_source_navigation_paths(target_path),
        }
        self._process_pending_source_navigation("selection_request", trigger_path=target_path)
        return True

    def _select_destination_item_by_path(self, destination_path):
        tree = getattr(self, "destination_tree_widget", None)
        if tree is None:
            return False
        raw = destination_path or ""
        target_path = self._canonical_destination_projection_path(raw) or self.normalize_memory_path(raw)
        if not target_path:
            return False
        dest_item = self._find_visible_destination_item_by_path(target_path)
        if dest_item is None:
            return False

        parent = dest_item.parent()
        while parent is not None:
            parent.setExpanded(True)
            parent = parent.parent()
        tree.clearSelection()
        tree.setCurrentItem(dest_item)
        dest_item.setSelected(True)
        tree.scrollToItem(dest_item)
        self.on_tree_selection_changed("destination")
        return True

    def _build_destination_navigation_paths(self, target_path):
        canonical_target = self._canonical_destination_projection_path(target_path) or self.normalize_memory_path(
            target_path
        )
        segments = self._path_segments(canonical_target)
        if len(segments) <= 1:
            return []
        prefixes = []
        for index in range(1, len(segments)):
            prefixes.append("\\".join(segments[:index]))
        return prefixes

    def _process_pending_destination_navigation(self, reason, trigger_path=""):
        request = self._pending_destination_navigation
        if not request:
            return False

        target_path = request.get("target_path", "")
        if self._select_destination_item_by_path(target_path):
            self._pending_destination_navigation = None
            return True

        queue = request.get("queue", [])
        while queue:
            dest_path = queue[0]
            item = self._find_visible_destination_item_by_path(dest_path)
            if item is None:
                break

            node_data = item.data(0, Qt.UserRole) or {}
            if not node_data.get("is_folder"):
                queue.pop(0)
                continue

            pending_key = f"{node_data.get('drive_id', '')}:{node_data.get('id', '')}"
            if pending_key in self.pending_folder_loads["destination"]:
                break

            queue.pop(0)
            if bool(node_data.get("children_loaded")):
                continue

            dest_tree = getattr(self, "destination_tree_widget", None)
            if dest_tree is None:
                break
            dest_tree.expandItem(item)
            break

        if not queue and self._select_destination_item_by_path(target_path):
            self._pending_destination_navigation = None
            return True
        return False

    def _start_destination_navigation(self, destination_path):
        raw = destination_path or ""
        target_path = self._canonical_destination_projection_path(raw) or self.normalize_memory_path(raw)
        if not target_path:
            return False
        if self._select_destination_item_by_path(target_path):
            return True

        self._pending_destination_navigation = {
            "target_path": target_path,
            "queue": self._build_destination_navigation_paths(target_path),
        }
        self._process_pending_destination_navigation("selection_request", trigger_path=target_path)
        return True

    def _planned_move_for_source_node(self, source_node):
        if source_node is None or self.build_node_key(source_node, "source") is None:
            return None
        index = self.find_planned_move_index_by_source(source_node)
        if index is not None:
            return self.planned_moves[index]
        planning = self._resolve_selection_planning_state("source", source_node)
        move = planning.get("move")
        if planning.get("mode") in ("direct", "inherited") and move:
            return move
        return None

    def _source_path_suffix_under_parent(self, child_path, parent_path):
        c = (self._canonical_source_projection_path(child_path or "") or "").replace("/", "\\").strip()
        p = (self._canonical_source_projection_path(parent_path or "") or "").replace("/", "\\").strip()
        if not c or not p:
            return ""
        if c.casefold() == p.casefold():
            return ""
        pnorm = p.rstrip("\\")
        prefix = pnorm + "\\"
        if not c.casefold().startswith(prefix.casefold()):
            return ""
        return c[len(prefix) :]

    def _destination_jump_path_for_source_item(self, source_node, move):
        if not move or not source_node:
            return ""
        base = self._allocation_projection_path(move) or self._move_destination_target_path(move)
        if not base:
            return ""
        move_source = move.get("source") if isinstance(move.get("source"), dict) else {}
        if self.node_keys_match(move_source, source_node, "source"):
            return base
        ms_path = self._tree_item_path(move_source)
        sel_path = self._tree_item_path(source_node)
        if not ms_path or not sel_path:
            return base
        if not self.source_item_path_is_descendant_of(sel_path, ms_path):
            return base
        rel = self._source_path_suffix_under_parent(sel_path, ms_path)
        if not rel:
            return base
        merged = self.normalize_memory_path(f"{base}\\{rel}")
        return self._canonical_destination_projection_path(merged) or merged

    def _handle_go_to_destination_for_source(self, source_node):
        title = "Go to Destination"
        if source_node is None:
            QMessageBox.information(self, title, "Select a source item first.")
            return
        move = self._planned_move_for_source_node(source_node)
        if not move:
            QMessageBox.information(
                self,
                title,
                "This item has no assignment to show in the destination tree.",
            )
            return
        target_path = self._destination_jump_path_for_source_item(source_node, move)
        if not target_path:
            QMessageBox.information(
                self,
                title,
                "The assignment does not reference a destination path yet.",
            )
            return
        navigated = self._select_destination_item_by_path(target_path)
        if not navigated:
            navigated = self._start_destination_navigation(target_path)
        if navigated:
            if hasattr(self, "planned_moves_status"):
                self.planned_moves_status.setText("Showing assignment in the destination tree.")
            log_info(
                "selection_action_go_to_destination",
                source_name=source_node.get("name", ""),
                destination_projection=target_path,
            )
            return
        QMessageBox.information(
            self,
            title,
            "The assigned destination could not be located in the destination tree. "
            "Try expanding parent folders or refreshing the destination library.",
        )

    def _refresh_tree_visual_states(self, panel_key):
        tree = self.source_tree_widget if panel_key == "source" else self.destination_tree_widget
        if tree is None:
            return
        self._rebuild_submission_visual_cache()
        refreshed = 0
        if panel_key == "source" and self._source_tree_uses_model_view():
            model = getattr(self, "source_sharepoint_model", None)
            if model is not None:
                for ix in model.iter_depth_first():
                    node_data = ix.data(Qt.UserRole) or {}
                    if node_data.get("placeholder"):
                        continue
                    self._apply_tree_item_visual_state(None, node_data)
                    model.emit_payload_changed(ix)
                    refreshed += 1
            tree.viewport().update()
            if self._full_trace_enabled():
                log_trace("tree", "refresh_tree_visual_states_complete", panel_key=panel_key, nodes_refreshed=refreshed)
            return

        for index in range(tree.topLevelItemCount()):
            for item in self._iter_tree_items(tree.topLevelItem(index)):
                node_data = item.data(0, Qt.UserRole) or {}
                if node_data.get("placeholder"):
                    continue
                self._apply_tree_item_visual_state(item, node_data)
                refreshed += 1
        tree.viewport().update()
        if self._full_trace_enabled():
            log_trace("tree", "refresh_tree_visual_states_complete", panel_key=panel_key, nodes_refreshed=refreshed)

    def on_tree_item_expanded(self, panel_key, item):
        node_data = item.data(0, Qt.UserRole) or {}
        base_label = str(node_data.get("base_display_label", "") or "").strip().lower()
        tree_label = str(node_data.get("tree_label", "") or "").strip().lower()
        text_label = str(item.text(0) or "").strip().lower()
        is_folder_like = bool(
            node_data.get("is_folder")
            or tree_label == "folder"
            or base_label.startswith("folder:")
            or text_label.startswith("folder:")
        )
        is_destination_planned_allocation = bool(
            panel_key == "destination" and self.node_is_planned_allocation(node_data)
        )
        log_dest_expand_detail = bool(
            panel_key == "destination"
            and (
                self.node_is_planned_allocation(node_data)
                or bool(node_data.get("planned_allocation_descendant"))
                or self._projection_diag_verbose_enabled()
            )
        )
        if self._full_trace_enabled():
            self._ui_trace("tree", "expand_signal", panel_key=panel_key, item=item)
        if node_data.get("placeholder") or (not is_folder_like and not is_destination_planned_allocation):
            if log_dest_expand_detail:
                self._log_destination_projection_ui(
                    "destination_projection_ui_expand_route",
                    item=item,
                    route="skipped_not_folder_or_placeholder",
                )
            if self._full_trace_enabled():
                self._ui_trace(
                    "tree",
                    "expand_route",
                    panel_key=panel_key,
                    item=item,
                    route="skipped_not_folder_or_placeholder",
                )
            return
        if (
            panel_key == "destination"
            and self.node_is_planned_allocation(node_data)
            and bool(node_data.get("projection_unresolved_terminal"))
        ):
            # Allow manual retries for unresolved allocation projections.
            if log_dest_expand_detail:
                self._log_destination_projection_ui(
                    "destination_projection_ui_expand_route",
                    item=item,
                    route="cleared_projection_unresolved_terminal",
                )
            if self._full_trace_enabled():
                self._ui_trace(
                    "tree",
                    "expand_route",
                    panel_key=panel_key,
                    item=item,
                    route="cleared_projection_unresolved_terminal",
                )
            node_data["children_loaded"] = False
            node_data["load_failed"] = False
            node_data["projection_unresolved_terminal"] = False
            item.setData(0, Qt.UserRole, node_data)
            node_data = item.data(0, Qt.UserRole) or {}
        if panel_key == "destination" and self.node_is_planned_allocation(node_data):
            mv_inval = self._find_planned_move_for_destination_node(node_data)
            node_data = self._invalidate_stale_destination_allocation_projection(item, node_data, mv_inval)
            node_data = item.data(0, Qt.UserRole) or {}
        if (
            panel_key == "destination"
            and self.node_is_planned_allocation(node_data)
            and bool(node_data.get("is_folder", True))
            and self._destination_allocation_folder_shows_materialized_children(item)
            and not bool(node_data.get("children_loaded"))
        ):
            node_data["children_loaded"] = True
            node_data["projection_unresolved_terminal"] = False
            item.setData(0, Qt.UserRole, node_data)
            node_data = item.data(0, Qt.UserRole) or {}
        if (
            panel_key == "destination"
            and not node_data.get("children_loaded")
            and (
                self.node_is_planned_allocation(node_data)
                or bool(node_data.get("planned_allocation_descendant"))
            )
        ):
            if log_dest_expand_detail:
                self._log_destination_projection_ui(
                    "destination_projection_ui_expand_route",
                    item=item,
                    route="load_projected_descendants",
                )
            if self._full_trace_enabled():
                self._ui_trace(
                    "tree",
                    "expand_route",
                    panel_key=panel_key,
                    item=item,
                    route="load_projected_descendants",
                )
            item_path = self._tree_item_path(node_data)
            if item_path:
                QTimer.singleShot(0, lambda p=item_path: self._deferred_load_destination_projected_descendants(p))
            else:
                self._load_destination_projected_descendants(item)
            return
        if node_data.get("children_loaded") or node_data.get("load_failed"):
            if log_dest_expand_detail:
                self._log_destination_projection_ui(
                    "destination_projection_ui_expand_route",
                    item=item,
                    route="skipped_children_already_loaded_or_failed",
                )
            if self._full_trace_enabled():
                self._ui_trace(
                    "tree",
                    "expand_route",
                    panel_key=panel_key,
                    item=item,
                    route="skipped_children_already_loaded_or_failed",
                )
            return

        drive_id = self._resolve_tree_item_drive_id(panel_key, node_data)
        item_id = node_data.get("id", "")
        if not drive_id or not item_id:
            if log_dest_expand_detail:
                self._log_destination_projection_ui(
                    "destination_projection_ui_expand_route",
                    item=item,
                    route="skipped_missing_drive_or_item_id",
                    drive_id_resolved=bool(drive_id),
                    item_id_resolved=bool(item_id),
                )
            if self._full_trace_enabled():
                self._ui_trace(
                    "tree",
                    "expand_route",
                    panel_key=panel_key,
                    item=item,
                    route="skipped_missing_drive_or_item_id",
                    drive_id_resolved=bool(drive_id),
                    item_id_resolved=bool(item_id),
                )
            return

        pending_key = f"{drive_id}:{item_id}"
        if pending_key in self.pending_folder_loads[panel_key]:
            if log_dest_expand_detail:
                self._log_destination_projection_ui(
                    "destination_projection_ui_expand_route",
                    item=item,
                    route="skipped_folder_worker_already_pending",
                    pending_key=pending_key,
                )
            if self._full_trace_enabled():
                self._ui_trace(
                    "tree",
                    "expand_route",
                    panel_key=panel_key,
                    item=item,
                    route="skipped_folder_worker_already_pending",
                    pending_key=pending_key,
                )
            return

        # Global worker throttle to keep UI responsive across restore, expand-all,
        # and manual expansion flows. Match destination concurrency with
        # `_process_expand_all_queue` (3) so expand-all does not starve most branches.
        pending_count = len(self.pending_folder_loads.get(panel_key, set()))
        max_inflight_loads = 1 if panel_key == "source" else 3
        if pending_count >= max_inflight_loads:
            if log_dest_expand_detail:
                self._log_destination_projection_ui(
                    "destination_projection_ui_expand_route",
                    item=item,
                    route="skipped_folder_worker_throttle",
                    pending_count=pending_count,
                    max_inflight_loads=max_inflight_loads,
                )
            if self._full_trace_enabled():
                self._ui_trace(
                    "tree",
                    "expand_route",
                    panel_key=panel_key,
                    item=item,
                    route="skipped_folder_worker_throttle",
                    pending_count=pending_count,
                    max_inflight_loads=max_inflight_loads,
                )
            return

        self.pending_folder_loads[panel_key].add(pending_key)
        worker_key = f"{panel_key}:{item_id}"
        item_path = self._tree_item_path(node_data)
        if item_path and item_path in self._pending_snapshot_branch_refresh.get(panel_key, set()):
            self._snapshot_branch_refresh_baseline_by_worker[worker_key] = self._capture_child_path_set(item)
        if panel_key == "destination":
            preserved_children = self._preserve_destination_future_state_children(item)
            if preserved_children:
                self._destination_preserved_children_by_worker[worker_key] = preserved_children
        if self._full_trace_enabled():
            self._ui_trace(
                "tree",
                "folder_load_take_children",
                panel_key=panel_key,
                item=item,
                worker_key=worker_key,
                prior_child_count_hint="cleared",
            )
        item.takeChildren()
        item.addChild(
            self.build_loading_placeholder_item("Loading folder contents...", role="loading_in_progress")
        )

        use_cache_only = bool(
            (
                getattr(self, "_memory_restore_in_progress", False)
                or getattr(self, "_suppress_autosave", False)
            )
            and self.graph.has_cached_drive_item_children(drive_id, item_id)
        )
        worker_context = {
            "site_id": node_data.get("site_id", ""),
            "site_name": node_data.get("site_name", ""),
            "library_id": node_data.get("library_id", drive_id),
            "library_name": node_data.get("library_name", ""),
            "tree_role": panel_key,
            "parent_item_path": node_data.get("item_path", ""),
            "cache_only": use_cache_only,
        }
        worker = FolderLoadWorker(self.graph, panel_key, drive_id, item_id, worker_context)
        worker_entry = self._register_folder_worker(worker_key, worker, item)
        worker.success.connect(lambda payload, worker_id=worker_entry["id"]: self._safe_invoke("folder_worker.success", self.on_folder_load_success, payload, worker_id))
        worker.error.connect(lambda payload, worker_id=worker_entry["id"]: self._safe_invoke("folder_worker.error", self.on_folder_load_error, payload, worker_id))
        worker.finished.connect(lambda key=worker_key, worker_id=worker_entry["id"]: self._safe_invoke("folder_worker.finished", self.on_folder_worker_finished, key, worker_id))
        if log_dest_expand_detail:
            self._log_destination_projection_ui(
                "destination_projection_ui_expand_route",
                item=item,
                route="start_graph_folder_load_worker",
                panel_key=panel_key,
                worker_key=worker_key,
                cache_only=use_cache_only,
            )
        if self._full_trace_enabled():
            self._ui_trace(
                "tree",
                "expand_route",
                panel_key=panel_key,
                item=item,
                route="start_graph_folder_load_worker",
                worker_key=worker_key,
                cache_only=use_cache_only,
            )
        worker.start()

    def _destination_allocation_folder_shows_materialized_children(self, item):
        """True when the folder row already lists real or resolved-empty children in the widget.

        Session snapshot restore and earlier expands often build these items without setting
        ``children_loaded`` on the parent [Allocated] row, which otherwise forces a full
        Graph subtree fetch on every expand / expand-all pass.
        """
        if item is None or item.childCount() == 0:
            return False
        for i in range(item.childCount()):
            d = item.child(i).data(0, Qt.UserRole) or {}
            if not d.get("placeholder"):
                return True
            role = str(d.get("placeholder_role") or "").strip()
            if role in ("projection_pending", "lazy_unloaded", "loading_in_progress"):
                return False
        return True

    def _deferred_load_destination_projected_descendants(self, item_path: str):
        """Run projected-descendant materialization after the expand gesture finishes (next event).

        Keeps expand/collapse responsive like Explorer; path lookup avoids dangling QTreeWidgetItem refs.
        """
        path = (item_path or "").strip()
        if not path:
            return
        item = self._find_visible_destination_item_by_path(path)
        if item is None or not item.isExpanded():
            return
        self._load_destination_projected_descendants(item)

    def _load_destination_projected_descendants(self, item):
        if item is None:
            return
        node_data = item.data(0, Qt.UserRole) or {}
        if self._full_trace_enabled():
            self._ui_trace(
                "projection",
                "load_projected_descendants_enter",
                panel_key="destination",
                item=item,
            )
        if not bool(node_data.get("is_folder", False)):
            if self._projection_diag_verbose_enabled():
                self._log_destination_projection_ui(
                    "destination_projection_ui_load_outcome",
                    item=item,
                    outcome="skipped_not_a_folder",
                )
            return
        move_for_row = None
        if self.node_is_planned_allocation(node_data):
            move_for_row = self._find_planned_move_for_destination_node(node_data)
            node_data = self._invalidate_stale_destination_allocation_projection(item, node_data, move_for_row)
            node_data = item.data(0, Qt.UserRole) or {}
        if self.node_is_planned_allocation(node_data) and self._destination_allocation_folder_shows_materialized_children(item):
            if not bool(node_data.get("children_loaded")):
                updated = dict(node_data)
                updated["children_loaded"] = True
                updated["projection_unresolved_terminal"] = False
                item.setData(0, Qt.UserRole, updated)
                if move_for_row is not None:
                    updated = self._stamp_allocation_projection_cache_metadata(item, updated, move_for_row)
                self._refresh_destination_item_visibility(item)
                self._apply_tree_item_visual_state(item, updated)
                if getattr(self, "destination_tree_widget", None) is not None:
                    self.destination_tree_widget.viewport().update()
                if self._projection_diag_verbose_enabled():
                    self._log_destination_projection_ui(
                        "destination_projection_ui_load_outcome",
                        item=item,
                        outcome="skipped_already_materialized_under_allocation",
                    )
            else:
                if move_for_row is not None and not (node_data.get("allocation_projection_destination_path_saved") or "").strip():
                    updated = self._stamp_allocation_projection_cache_metadata(item, dict(node_data), move_for_row)
                    self._apply_tree_item_visual_state(item, updated)
            return
        if bool(node_data.get("planned_allocation_descendant")):
            node_data["children_loaded"] = True
            item.setData(0, Qt.UserRole, node_data)
            self._refresh_destination_item_visibility(item)
            self._log_restore_phase(
                "destination_projected_descendant_lazy_load_skipped",
                reason="projected_descendants_are_pre_materialized",
                **self._destination_projection_ui_snapshot(item),
            )
            self._log_destination_projection_ui(
                "destination_projection_ui_load_outcome",
                item=item,
                outcome="descendant_row_pre_materialized",
            )
            return
        if node_data.get("children_loaded"):
            if self.node_is_planned_allocation(node_data):
                move_skip = move_for_row or self._find_planned_move_for_destination_node(node_data)
                if move_skip is not None and not (node_data.get("allocation_projection_destination_path_saved") or "").strip():
                    node_data = self._stamp_allocation_projection_cache_metadata(item, dict(node_data), move_skip)
            if self._projection_diag_verbose_enabled():
                self._log_destination_projection_ui(
                    "destination_projection_ui_load_outcome",
                    item=item,
                    outcome="skipped_children_already_marked_loaded",
                )
            return
        move = move_for_row if move_for_row is not None else self._find_planned_move_for_destination_node(node_data)
        if move is None:
            # Keep allocation folders lazy-loadable when the move cannot be resolved yet;
            # otherwise indicators disappear and users lose expand affordance.
            if self.node_is_planned_allocation(node_data):
                node_data["children_loaded"] = False
                node_data["load_failed"] = False
                node_data["projection_unresolved_terminal"] = False
                item.setData(0, Qt.UserRole, node_data)
                if item.childCount() == 0:
                    item.addChild(
                        self.build_loading_placeholder_item(
                            "Expand to load projected descendants",
                            role="projection_pending",
                        )
                    )
                self._log_restore_phase(
                    "destination_projected_descendant_move_unresolved",
                    pending_source_folder_loads=len(self.pending_folder_loads.get("source", set())),
                    source_restore_queue_size=len(getattr(self, "_source_restore_materialization_queue", [])),
                    **self._destination_projection_ui_snapshot(item),
                )
                self._log_destination_projection_ui(
                    "destination_projection_ui_load_outcome",
                    item=item,
                    outcome="move_unresolved_expand_placeholder",
                )
            else:
                node_data["children_loaded"] = True
                item.setData(0, Qt.UserRole, node_data)
                self._log_destination_projection_ui(
                    "destination_projection_ui_load_outcome",
                    item=item,
                    outcome="move_unresolved_non_allocation_marked_loaded",
                )
            self._refresh_destination_item_visibility(item)
            return
        self._remove_placeholder_children(item)
        applied_count = self._apply_allocation_descendants_to_item(item, move)
        move_src = move.get("source", {}) or {}
        canonical_move_source = self._canonical_source_projection_path(move.get("source_path", ""))
        if applied_count == 0 and bool(move_src.get("is_folder", True)):
            # Keep destination deterministic: do not block on source-tree visibility
            # or source restore queues. If descendants cannot be materialized now,
            # keep a retry placeholder and let expand-all revisit this node.
            self._log_restore_phase(
                "destination_projected_descendant_lazy_load_deferred",
                move_correlation_id=id(move),
                canonical_move_source_path=canonical_move_source,
                source_drive_id_present=bool(move_src.get("drive_id")),
                source_item_id_present=bool(move_src.get("id")),
                waiting_for_source_projection=False,
                pending_source_folder_loads=len(self.pending_folder_loads.get("source", set())),
                source_restore_queue_size=len(getattr(self, "_source_restore_materialization_queue", [])),
                reason="no_projected_descendants_materialized",
                **self._destination_projection_ui_snapshot(item),
            )
        updated_node_data = item.data(0, Qt.UserRole) or {}
        if applied_count == 0 and self.node_is_planned_allocation(updated_node_data):
            updated_node_data["children_loaded"] = False
            updated_node_data["load_failed"] = False
            updated_node_data["projection_unresolved_terminal"] = False
            updated_node_data.pop("allocation_projection_destination_path_saved", None)
            updated_node_data.pop("allocation_projection_children_signature", None)
            item.setData(0, Qt.UserRole, updated_node_data)
            if item.childCount() == 0:
                item.addChild(
                    self.build_loading_placeholder_item(
                        "Expand to retry projected descendants",
                        role="projection_pending",
                    )
                )
            waiting_for_source_projection = bool(
                self.pending_folder_loads.get("source", set())
                or getattr(self, "_source_restore_materialization_queue", [])
            )
            self._refresh_destination_item_visibility(item)
            self._log_restore_phase(
                "destination_projected_descendant_retry_required",
                move_correlation_id=id(move),
                canonical_move_source_path=canonical_move_source,
                source_drive_id_present=bool(move_src.get("drive_id")),
                source_item_id_present=bool(move_src.get("id")),
                waiting_for_source_projection=waiting_for_source_projection,
                pending_source_folder_loads=len(self.pending_folder_loads.get("source", set())),
                source_restore_queue_size=len(getattr(self, "_source_restore_materialization_queue", [])),
                **self._destination_projection_ui_snapshot(item),
            )
            self._log_destination_projection_ui(
                "destination_projection_ui_load_outcome",
                item=item,
                outcome="retry_placeholder_after_zero_applied",
                applied_count=0,
                move_correlation_id=id(move),
            )
            return
        updated_node_data["children_loaded"] = True
        updated_node_data["projection_unresolved_terminal"] = False
        updated_node_data = self._stamp_allocation_projection_cache_metadata(item, updated_node_data, move)
        item.setData(0, Qt.UserRole, updated_node_data)
        self._refresh_destination_item_visibility(item)
        self._apply_tree_item_visual_state(item, updated_node_data)
        self.destination_tree_widget.viewport().update()
        self._log_destination_projection_ui(
            "destination_projection_ui_load_outcome",
            item=item,
            outcome="projection_applied",
            applied_descendant_rows=applied_count,
            move_correlation_id=id(move),
        )

    def _on_source_tree_item_collapsed(self, item):
        if not self._full_trace_enabled():
            return
        node_data = item.data(0, Qt.UserRole) or {}
        if node_data.get("placeholder"):
            self._ui_trace(
                "tree",
                "collapse_skipped_placeholder",
                panel_key="source",
                item=item,
            )
        else:
            self._ui_trace("tree", "collapsed", panel_key="source", item=item)

    def _on_destination_tree_item_collapsed(self, item):
        node_data = item.data(0, Qt.UserRole) or {}
        if self._full_trace_enabled():
            if node_data.get("placeholder"):
                self._ui_trace(
                    "tree",
                    "collapse_skipped_placeholder",
                    panel_key="destination",
                    item=item,
                )
                return
            self._ui_trace("tree", "collapsed", panel_key="destination", item=item)
            return
        if node_data.get("placeholder"):
            return
        if not (
            self.node_is_planned_allocation(node_data)
            or bool(node_data.get("planned_allocation_descendant"))
            or self._projection_diag_verbose_enabled()
        ):
            return
        self._log_destination_projection_ui(
            "destination_projection_ui_collapsed",
            item=item,
        )

    def _ensure_tree_item_load_started(self, panel_key, item):
        if item is None:
            return False
        node_data = item.data(0, Qt.UserRole) or {}
        if node_data.get("placeholder") or not node_data.get("is_folder"):
            return False
        if node_data.get("children_loaded") or node_data.get("load_failed"):
            return False
        drive_id = self._resolve_tree_item_drive_id(panel_key, node_data)
        item_id = node_data.get("id", "")
        if not drive_id or not item_id:
            if self._full_trace_enabled():
                self._ui_trace(
                    "tree",
                    "ensure_load_skipped_missing_ids",
                    panel_key=panel_key,
                    item=item,
                    drive_id_resolved=bool(drive_id),
                    item_id_resolved=bool(item_id),
                )
            return False
        pending_key = f"{drive_id}:{item_id}"
        if pending_key in self.pending_folder_loads.get(panel_key, set()):
            if self._full_trace_enabled():
                self._ui_trace(
                    "tree",
                    "ensure_load_already_pending",
                    panel_key=panel_key,
                    item=item,
                    pending_key=pending_key,
                )
            return True
        if self._full_trace_enabled():
            self._ui_trace(
                "tree",
                "ensure_load_invoke_expand",
                panel_key=panel_key,
                item=item,
                pending_key=pending_key,
            )
        self.on_tree_item_expanded(panel_key, item)
        return pending_key in self.pending_folder_loads.get(panel_key, set())

    def _find_visible_item_by_drive_item_id(self, panel_key, drive_id, item_id):
        if panel_key == "source":
            tree = getattr(self, "source_tree_widget", None)
        elif panel_key == "destination":
            tree = getattr(self, "destination_tree_widget", None)
        else:
            tree = None
        if tree is None or not drive_id or not item_id:
            return None
        for index in range(tree.topLevelItemCount()):
            top_item = tree.topLevelItem(index)
            for item in self._iter_tree_items(top_item):
                node_data = item.data(0, Qt.UserRole) or {}
                if node_data.get("placeholder"):
                    continue
                if node_data.get("id") != item_id:
                    continue
                node_drive_id = (
                    node_data.get("drive_id")
                    or node_data.get("library_id")
                    or self.pending_root_drive_ids.get(panel_key, "")
                )
                if str(node_drive_id or "").strip() == str(drive_id or "").strip():
                    return item
        return None

    def on_folder_worker_finished(self, worker_key, worker_id):
        try:
            self._cleanup_folder_worker(worker_key, worker_id)
        except Exception as exc:
            self._log_restore_exception("on_folder_worker_finished", exc)

    def on_folder_load_success(self, payload, worker_id):
        try:
            panel_key = payload.get("panel_key", "")
            drive_id = payload.get("drive_id", "")
            item_id = payload.get("item_id", "")
            pending_key = f"{drive_id}:{item_id}"
            self.pending_folder_loads.get(panel_key, set()).discard(pending_key)

            worker_key = f"{panel_key}:{item_id}"
            worker_state = self.folder_load_workers.get(worker_key)
            if not worker_state or worker_state.get("id") != worker_id:
                self._log_worker_lifecycle("stale_success_skipped", "folder", worker_id, worker_key, drive_id=drive_id)
                return
            if not worker_state:
                return

            item = worker_state.get("item")
            if panel_key == "source" and self._source_tree_uses_model_view():
                pmi = worker_state.get("source_folder_parent_persistent")
                parent_index = QModelIndex(pmi) if pmi is not None and pmi.isValid() else QModelIndex()
                if not parent_index.isValid():
                    parent_index = self.source_sharepoint_model.find_index_by_drive_item(drive_id, item_id)
                if not parent_index.isValid():
                    self._snapshot_branch_refresh_baseline_by_worker.pop(worker_key, None)
                    self._destination_preserved_children_by_worker.pop(worker_key, None)
                    self._log_worker_lifecycle(
                        "stale_deleted_item_success_skipped", "folder", worker_id, worker_key, drive_id=drive_id
                    )
                    return
                previous_child_paths = self._snapshot_branch_refresh_baseline_by_worker.pop(worker_key, set())
                items_list = payload.get("items", [])
                if self._full_trace_enabled():
                    nd = parent_index.data(Qt.UserRole) or {}
                    log_trace(
                        "worker",
                        "folder_load_success_apply_payload",
                        panel_key=panel_key,
                        worker_id=worker_id,
                        worker_key=worker_key,
                        trigger_path_excerpt=str(nd.get("item_path", "") or "")[:200],
                        payload_item_count=len(items_list or []),
                        skip_destination_child_replace=False,
                        preserved_destination_children=0,
                    )
                self._apply_source_folder_load_model_tail(parent_index, items_list, previous_child_paths, panel_key)
                self._schedule_workspace_ui_persist(panel_key=panel_key)
                if self._pending_snapshot_branch_refresh.get(panel_key) and not self._expand_all_pending.get(panel_key):
                    self._schedule_snapshot_branch_refresh(panel_key, delay_ms=0)
                if not (panel_key == "source" and self._expand_all_pending.get("source")):
                    self._schedule_progress_summary_refresh()
                if panel_key == "source" and self._expand_all_pending.get("source"):
                    QTimer.singleShot(
                        0,
                        lambda: self._safe_invoke(
                            "source_model_expand_all_continue",
                            self._source_model_expand_all_tick,
                        ),
                    )
                return

            if not self._tree_item_is_alive(item):
                recovered_item = self._find_visible_item_by_drive_item_id(panel_key, drive_id, item_id)
                if recovered_item is None:
                    self._snapshot_branch_refresh_baseline_by_worker.pop(worker_key, None)
                    self._destination_preserved_children_by_worker.pop(worker_key, None)
                    self._log_worker_lifecycle("stale_deleted_item_success_skipped", "folder", worker_id, worker_key, drive_id=drive_id)
                    return
                worker_state["item"] = recovered_item
                item = recovered_item
                self._log_worker_lifecycle("stale_item_rebound_for_success", "folder", worker_id, worker_key, drive_id=drive_id)
            previous_child_paths = self._snapshot_branch_refresh_baseline_by_worker.pop(worker_key, set())

            preserved_destination_children = []
            existing_destination_subtree_nodes = 0
            items = payload.get("items", [])
            skip_destination_child_replace = False
            if panel_key == "destination":
                existing_destination_subtree_nodes = self._count_visible_subtree_nodes(item)
                preserved_destination_children = self._destination_preserved_children_by_worker.pop(worker_key, [])
                if not getattr(self, "_memory_restore_in_progress", False):
                    incoming_destination_subtree_nodes = 1 + self._count_folder_payload_nodes(items)
                    if existing_destination_subtree_nodes > incoming_destination_subtree_nodes:
                        self._log_restore_phase(
                            "destination_shallow_folder_payload_skipped",
                            worker_id=worker_id,
                            incoming_node_count=incoming_destination_subtree_nodes,
                            visible_node_count=existing_destination_subtree_nodes,
                            trigger_path=self.normalize_memory_path(
                                ((item.data(0, Qt.UserRole) or {}).get("item_path", ""))
                            ),
                        )
                        # Previously we returned here: pending load was cleared but children_loaded
                        # stayed false and "Loading folder contents..." could remain forever.
                        skip_destination_child_replace = True
            if self._full_trace_enabled():
                nd = item.data(0, Qt.UserRole) or {}
                log_trace(
                    "worker",
                    "folder_load_success_apply_payload",
                    panel_key=panel_key,
                    worker_id=worker_id,
                    worker_key=worker_key,
                    trigger_path_excerpt=str(nd.get("item_path", "") or "")[:200],
                    payload_item_count=len(items or []),
                    skip_destination_child_replace=skip_destination_child_replace,
                    preserved_destination_children=len(preserved_destination_children or []),
                )
            if not skip_destination_child_replace:
                item.takeChildren()
                if items:
                    for child in sorted(items, key=lambda value: (not value.get("is_folder", False), value.get("name", "").lower())):
                        item.addChild(self.build_tree_item(child))
                else:
                    item.addChild(
                        self.build_loading_placeholder_item("This folder is empty.", role="terminal_empty")
                    )

            node_data = item.data(0, Qt.UserRole) or {}
            trigger_path = node_data.get("item_path") or node_data.get("display_path") or ""
            node_data["children_loaded"] = True
            node_data["load_failed"] = False
            item.setData(0, Qt.UserRole, node_data)
            if skip_destination_child_replace:
                self._remove_placeholder_children_with_role(item, "loading_in_progress")
            current_child_paths = self._capture_child_path_set(item)
            if previous_child_paths:
                added_count = len(current_child_paths - previous_child_paths)
                removed_count = len(previous_child_paths - current_child_paths)
                if added_count or removed_count:
                    self._set_tree_status_message(
                        panel_key,
                        f"Refreshing saved branches... {added_count} added, {removed_count} removed in {node_data.get('name', 'folder')}.",
                        loading=bool(self._pending_snapshot_branch_refresh.get(panel_key)),
                    )

            if panel_key == "destination":
                semantic_path = self._destination_semantic_path(node_data)
                if semantic_path == "Root":
                    self._destination_root_prime_pending = False
                try:
                    if self._expand_all_pending.get("destination"):
                        self._expand_all_deferred_refresh["destination"] = True
                        item.setExpanded(True)
                        for index in range(item.childCount()):
                            self._queue_expand_all_item("destination", item.child(index))
                        handoff_moved_count = 0
                        direct_applied_count = 0
                        replay_applied_count = 0
                        allocation_applied_count = 0
                        allocation_replay_count = 0
                        merge_moved_count = 0
                        future_model_applied_count = 0
                    elif (
                        getattr(self, "_sharepoint_lazy_mode", False)
                        and getattr(self, "_destination_root_prime_pending", False)
                        and semantic_path != "Root"
                    ):
                        handoff_moved_count = self._restore_destination_future_state_children(item, preserved_destination_children)
                        direct_applied_count = 0
                        replay_applied_count = 0
                        allocation_applied_count = 0
                        allocation_replay_count = 0
                        merge_moved_count = 0
                        future_model_applied_count = 0
                        self._log_restore_phase(
                            "destination_overlay_deferred_for_root_prime",
                            trigger_path=self.normalize_memory_path(trigger_path),
                            destination_path=semantic_path,
                        )
                    elif (
                        getattr(self, "_sharepoint_lazy_mode", False)
                        and semantic_path == "Root"
                    ):
                        handoff_moved_count = self._restore_destination_future_state_children(item, preserved_destination_children)
                        direct_applied_count = 0
                        replay_applied_count = 0
                        allocation_applied_count = 0
                        allocation_replay_count = 0
                        merge_moved_count = 0
                        future_model_applied_count = 0
                        self._log_restore_phase(
                            "destination_overlay_deferred_for_root_restore_queue",
                            trigger_path=self.normalize_memory_path(trigger_path),
                            destination_path=semantic_path,
                        )
                    else:
                        handoff_moved_count = self._restore_destination_future_state_children(item, preserved_destination_children)
                        direct_applied_count = self._apply_proposed_children_to_item(item)
                        allocation_applied_count = self._apply_allocation_children_to_item(item)
                        replay_applied_count = self._replay_unresolved_proposed_overlay(
                            "folder_worker_success",
                            trigger_path=trigger_path,
                        )
                        allocation_replay_count = self._replay_unresolved_allocation_overlay(
                            "folder_worker_success",
                            trigger_path=trigger_path,
                        )
                        merge_moved_count = self._reconcile_destination_semantic_duplicates("folder_worker_success")
                        if getattr(self, "_sharepoint_lazy_mode", False):
                            future_model_applied_count = 0
                        else:
                            future_model_applied_count = self._materialize_destination_future_model("folder_worker_success")
                    self._log_restore_phase(
                        "destination_replay_attachment_applied",
                        trigger_path=self.normalize_memory_path(trigger_path),
                        handoff_moved_count=handoff_moved_count,
                        direct_applied_count=direct_applied_count,
                        replay_applied_count=replay_applied_count,
                        allocation_applied_count=allocation_applied_count,
                        allocation_replay_count=allocation_replay_count,
                        merge_moved_count=merge_moved_count,
                        future_model_applied_count=future_model_applied_count,
                        queue_size=self._unresolved_proposed_queue_size(),
                    )
                    if not self._expand_all_pending.get("destination"):
                        if (
                            getattr(self, "_sharepoint_lazy_mode", False)
                            and semantic_path == "Root"
                        ):
                            self._start_destination_restore_materialization()
                        if not getattr(self, "_destination_root_prime_pending", False):
                            self._schedule_destination_restore_materialization_queue(
                                "folder_load",
                                trigger_path=trigger_path,
                            )
                        self._refresh_destination_tree_indicators()
                        self.destination_tree_widget.viewport().update()
                        if self._unresolved_proposed_queue_size() == 0 and self._unresolved_allocation_queue_size() == 0:
                            self._log_restore_phase(
                                "destination_replay_refresh_complete",
                                trigger_path=self.normalize_memory_path(trigger_path),
                                queue_size=0,
                                allocation_queue_size=0,
                            )
                            self._finalize_memory_restore_if_ready(f"folder_load:{semantic_path}")
                        else:
                            self._log_restore_phase(
                                "destination_replay_refresh_deferred",
                                trigger_path=self.normalize_memory_path(trigger_path),
                                queue_size=self._unresolved_proposed_queue_size(),
                                allocation_queue_size=self._unresolved_allocation_queue_size(),
                            )
                    if (
                        getattr(self, "_sharepoint_lazy_mode", False)
                        and semantic_path == "Root"
                        and not self._expand_all_pending.get("destination")
                    ):
                        self._set_tree_status_message(
                            "destination",
                            f"{item.childCount()} top-level destination folder(s) loaded.",
                            loading=False,
                        )
                except Exception as exc:
                    self._log_restore_exception("destination_overlay_folder_load_success", exc)
                if self._expand_all_pending.get("destination"):
                    self._schedule_expand_all("destination", delay_ms=0)
                else:
                    self._continue_expand_all("destination", item)
                self._refresh_tree_column_width("destination")
                self._process_pending_destination_navigation("folder_load", trigger_path=trigger_path)
            else:
                projection_refresh_invoked = False
                if self._expand_all_pending.get("source"):
                    self._expand_all_deferred_refresh["source"] = True
                elif self._memory_restore_in_progress or bool(self._source_restore_materialization_queue):
                    self._source_projection_refresh_pending = True
                else:
                    self._schedule_source_projection_refresh_for_paths(
                        [trigger_path],
                        "source_projection_folder_load_applied",
                        trigger_path=trigger_path,
                    )
                    projection_refresh_invoked = True
                destination_future_model_applied_count = 0
                if (
                    not self._expand_all_pending.get("source")
                    and getattr(self, "destination_tree_widget", None) is not None
                    and self.planned_moves
                    and not getattr(self, "_sharepoint_lazy_mode", False)
                ):
                    destination_future_model_applied_count = self._materialize_destination_future_model(
                        "source_folder_load_success"
                    )
                should_log_source_branch_loaded = not self._expand_all_pending.get("source")
                if self._expand_all_pending.get("source"):
                    self._source_expand_all_folder_load_log_counter = (
                        int(getattr(self, "_source_expand_all_folder_load_log_counter", 0) or 0) + 1
                    )
                    should_log_source_branch_loaded = (
                        self._source_expand_all_folder_load_log_counter % 24 == 0
                    )
                if should_log_source_branch_loaded:
                    self._log_restore_phase(
                        "source_restore_branch_loaded",
                        source_path=trigger_path,
                        normalized_source_path=self._canonical_source_projection_path(trigger_path),
                        queue_size=len(self._source_restore_materialization_queue),
                        branch_depth=self._source_branch_depth(trigger_path),
                        already_loaded=False,
                        loaded_successfully=True,
                        projection_refresh_invoked=projection_refresh_invoked,
                        destination_future_model_applied_count=destination_future_model_applied_count,
                        trigger_path=self.normalize_memory_path(trigger_path),
                        verbose=True,
                    )
                self._schedule_source_restore_materialization_queue("folder_load", trigger_path=trigger_path)
                self._process_pending_source_navigation("folder_load", trigger_path=trigger_path)
                if not self._expand_all_pending.get("source"):
                    self._continue_source_background_preload(item)
                self._continue_expand_all("source", item)
                if self._expand_all_pending.get("source"):
                    self._source_column_refresh_pending = True
                else:
                    self._refresh_tree_column_width("source")
                if self._source_branch_depth(trigger_path) <= 1:
                    item.setExpanded(True)
                    self._set_tree_status_message(
                        "source",
                        f"{item.childCount()} top-level source folder(s) loaded.",
                        loading=False,
                    )
                self._try_flush_destination_future_model_after_source_restore("source_folder_load")

            self._schedule_workspace_ui_persist(panel_key=panel_key)
            if self._pending_snapshot_branch_refresh.get(panel_key) and not self._expand_all_pending.get(panel_key):
                self._schedule_snapshot_branch_refresh(panel_key, delay_ms=0)
            if not (panel_key == "source" and self._expand_all_pending.get("source")):
                self._schedule_progress_summary_refresh()
        except Exception as exc:
            self._log_restore_exception("on_folder_load_success", exc)

    def on_folder_load_error(self, payload, worker_id):
        try:
            panel_key = payload.get("panel_key", "")
            drive_id = payload.get("drive_id", "")
            item_id = payload.get("item_id", "")
            pending_key = f"{drive_id}:{item_id}"
            self.pending_folder_loads.get(panel_key, set()).discard(pending_key)

            worker_key = f"{panel_key}:{item_id}"
            worker_state = self.folder_load_workers.get(worker_key)
            if not worker_state or worker_state.get("id") != worker_id:
                self._log_worker_lifecycle("stale_error_skipped", "folder", worker_id, worker_key, drive_id=drive_id)
                return
            if not worker_state:
                return

            item = worker_state.get("item")
            if panel_key == "source" and self._source_tree_uses_model_view():
                pmi = worker_state.get("source_folder_parent_persistent")
                parent_index = QModelIndex(pmi) if pmi is not None and pmi.isValid() else QModelIndex()
                if not parent_index.isValid():
                    parent_index = self.source_sharepoint_model.find_index_by_drive_item(drive_id, item_id)
                if not parent_index.isValid():
                    self._snapshot_branch_refresh_baseline_by_worker.pop(worker_key, None)
                    self._destination_preserved_children_by_worker.pop(worker_key, None)
                    self._log_worker_lifecycle(
                        "stale_deleted_item_error_skipped", "folder", worker_id, worker_key, drive_id=drive_id
                    )
                    return
                self._snapshot_branch_refresh_baseline_by_worker.pop(worker_key, None)
                err_pl = {
                    "placeholder": True,
                    "placeholder_role": "error",
                    "base_display_label": "Could not load folder contents.",
                    "tree_role": "source",
                }
                self.source_sharepoint_model.replace_all_children(parent_index, [err_pl])

                def _mut_err(p):
                    p["children_loaded"] = False
                    p["load_failed"] = True

                self.source_sharepoint_model.update_payload_for_index(parent_index, _mut_err)
                if self._pending_snapshot_branch_refresh.get(panel_key):
                    self._schedule_snapshot_branch_refresh(panel_key, delay_ms=150)
                return

            if not self._tree_item_is_alive(item):
                recovered_item = self._find_visible_item_by_drive_item_id(panel_key, drive_id, item_id)
                if recovered_item is None:
                    self._snapshot_branch_refresh_baseline_by_worker.pop(worker_key, None)
                    self._destination_preserved_children_by_worker.pop(worker_key, None)
                    self._log_worker_lifecycle("stale_deleted_item_error_skipped", "folder", worker_id, worker_key, drive_id=drive_id)
                    return
                worker_state["item"] = recovered_item
                item = recovered_item
                self._log_worker_lifecycle("stale_item_rebound_for_error", "folder", worker_id, worker_key, drive_id=drive_id)
            self._snapshot_branch_refresh_baseline_by_worker.pop(worker_key, None)

            preserved_destination_children = self._destination_preserved_children_by_worker.pop(worker_key, [])
            if self._full_trace_enabled():
                err_txt = payload.get("error") or payload.get("message") or payload.get("detail") or ""
                log_trace(
                    "worker",
                    "folder_load_error",
                    panel_key=panel_key,
                    worker_id=worker_id,
                    worker_key=worker_key,
                    error_message_excerpt=str(err_txt)[:400],
                    preserved_children_to_restore=len(preserved_destination_children or []),
                )
            item.takeChildren()
            if panel_key == "destination" and preserved_destination_children:
                for child in preserved_destination_children:
                    item.addChild(child)
                self._refresh_destination_item_visibility(item, expand=True)
            else:
                item.addChild(
                    self.build_loading_placeholder_item("Could not load folder contents.", role="error")
                )

            node_data = item.data(0, Qt.UserRole) or {}
            node_data["children_loaded"] = False
            node_data["load_failed"] = True
            item.setData(0, Qt.UserRole, node_data)

            if panel_key == "destination" and self._destination_semantic_path(node_data) == "Root":
                self._destination_root_prime_pending = False
                self._set_tree_status_message(
                    "destination",
                    "Could not refresh top-level destination folders. Showing current draft structure.",
                    loading=False,
                )
                try:
                    self._materialize_destination_future_model("destination_root_error_fallback")
                    self._start_destination_restore_materialization()
                    if getattr(self, "destination_tree_widget", None) is not None:
                        self.destination_tree_widget.viewport().update()
                except Exception as fallback_exc:
                    self._log_restore_exception("on_folder_load_error.destination_root_fallback", fallback_exc)
                self._schedule_progress_summary_refresh()
            if self._pending_snapshot_branch_refresh.get(panel_key):
                self._schedule_snapshot_branch_refresh(panel_key, delay_ms=150)
        except Exception as exc:
            self._log_restore_exception("on_folder_load_error", exc)

    def on_tree_selection_changed(self, panel_key):
        try:
            if self._root_tree_bind_in_progress:
                self._log_restore_phase("tree_selection_change_skipped", panel_key=panel_key, reason="root_tree_bind_in_progress")
                return

            tree = self.source_tree_widget if panel_key == "source" else self.destination_tree_widget
            if panel_key == "source" and self._source_tree_uses_model_view():
                ix = tree.currentIndex()
                selected_items = [ix] if ix.isValid() else []
            else:
                selected_items = tree.selectedItems()
            if self._full_trace_enabled():
                first = selected_items[0] if selected_items else None
                if isinstance(first, QModelIndex):
                    nd = (first.data(Qt.UserRole) or {}) if first is not None and first.isValid() else {}
                else:
                    nd = (first.data(0, Qt.UserRole) or {}) if first else {}
                log_trace(
                    "ui",
                    "tree_selection_changed",
                    panel_key=panel_key,
                    selected_count=len(selected_items),
                    primary_path_excerpt=str(self._tree_item_path(nd) or "")[:200],
                )
            if not selected_items:
                self._set_tree_selection_summary(panel_key, "")
                self.clear_selection_details()
                return

            first_sel = selected_items[0]
            if isinstance(first_sel, QModelIndex):
                node_data = first_sel.data(Qt.UserRole) or {}
            else:
                node_data = first_sel.data(0, Qt.UserRole) or {}
            if node_data.get("placeholder"):
                self.clear_selection_details()
                return

            context = self._resolve_selected_item_context(panel_key, node_data)
            self._update_selection_details(context)
            self._set_tree_selection_summary(panel_key, context.get("notes_preview", {}).get("body_text", ""))
            self._set_tree_selection_summary("destination" if panel_key == "source" else "source", "")
            self.update_details_action_state()
            if hasattr(self, "workspace_tabs"):
                self.workspace_tabs.setCurrentWidget(self.details_box)
        except Exception as exc:
            self._log_restore_exception("on_tree_selection_changed", exc)

    def clear_selection_details(self):
        self._current_details_node_data = None
        self._current_details_panel_key = ""
        self._current_details_context = None
        self._set_tree_selection_summary("source", "")
        self._set_tree_selection_summary("destination", "")
        if hasattr(self, "details_metadata_summary"):
            self.details_metadata_summary.setPlainText("Select an item to review its metadata.")
        if hasattr(self, "details_notes"):
            self.details_notes.setPlainText("Selection guidance and allocation notes will appear here.")
        if hasattr(self, "details_preview"):
            self.details_preview.setPlainText("Select an item to preview its planning context.")
        self._active_preview_request_id = 0
        self.update_details_action_state()

    def update_details_action_state(self):
        if not hasattr(self, "details_action_buttons"):
            return

        context = getattr(self, "_current_details_context", None)
        metadata = (context or {}).get("metadata", {})
        actions = (context or {}).get("actions", {})
        for action_name, button in self.details_action_buttons.items():
            action_state = actions.get(action_name, {})
            enabled = bool(action_state.get("enabled"))
            button.setEnabled(enabled)
            button.setToolTip(action_state.get("tooltip", ""))
            log_info(
                "selection_details_actions_updated",
                item_name=metadata.get("item_name", "Not available"),
                item_path=metadata.get("item_path", "Not available"),
                tree_role=(context or {}).get("panel_key", ""),
                node_origin=metadata.get("node_origin", "Not available"),
                action_name=action_name,
                enabled=enabled,
                traceable_to_source=((context or {}).get("traceability", {}).get("traceable_to_source", False)),
                selected_item_type=metadata.get("item_type", "Not available"),
            )

    def handle_open_selected_file(self):
        context = getattr(self, "_current_details_context", None)
        if context is None:
            QMessageBox.information(self, "Open File", "Select an item first.")
            return
        action = context["actions"].get("Open File", {})
        metadata = context["metadata"]
        if not action.get("enabled"):
            log_warn(
                "selection_action_failed",
                item_name=metadata["item_name"],
                item_path=metadata["item_path"],
                tree_role=context["panel_key"],
                node_origin=metadata["node_origin"],
                action_name="Open File",
                enabled=False,
                traceable_to_source=context["traceability"].get("traceable_to_source", False),
                selected_item_type=metadata["item_type"],
            )
            return
        target = action.get("target", "")
        opened = False
        if target and (":\\" in target or target.startswith("\\\\")):
            opened = QDesktopServices.openUrl(QUrl.fromLocalFile(target))
        elif target:
            opened = QDesktopServices.openUrl(QUrl(target))
        log_info(
            "selection_action_open_file",
            item_name=metadata["item_name"],
            item_path=metadata["item_path"],
            tree_role=context["panel_key"],
            node_origin=metadata["node_origin"],
            action_name="Open File",
            enabled=True,
            traceable_to_source=context["traceability"].get("traceable_to_source", False),
            selected_item_type=metadata["item_type"],
        )
        if not opened:
            log_warn(
                "selection_action_failed",
                item_name=metadata["item_name"],
                item_path=metadata["item_path"],
                tree_role=context["panel_key"],
                node_origin=metadata["node_origin"],
                action_name="Open File",
                enabled=True,
                traceable_to_source=context["traceability"].get("traceable_to_source", False),
                selected_item_type=metadata["item_type"],
            )
            QMessageBox.information(self, "Open File", "Open File is not available for this selection.")

    def handle_open_selected_in_browser(self):
        context = getattr(self, "_current_details_context", None)
        if context is None:
            QMessageBox.information(self, "Open in Browser", "Select an item first.")
            return
        action = context["actions"].get("Open in SharePoint", {})
        metadata = context["metadata"]
        if not action.get("enabled"):
            log_warn(
                "selection_action_failed",
                item_name=metadata["item_name"],
                item_path=metadata["item_path"],
                tree_role=context["panel_key"],
                node_origin=metadata["node_origin"],
                action_name="Open in SharePoint",
                enabled=False,
                traceable_to_source=context["traceability"].get("traceable_to_source", False),
                selected_item_type=metadata["item_type"],
            )
            return
        opened = QDesktopServices.openUrl(QUrl(action.get("target", "")))
        log_info(
            "selection_action_open_sharepoint",
            item_name=metadata["item_name"],
            item_path=metadata["item_path"],
            tree_role=context["panel_key"],
            node_origin=metadata["node_origin"],
            action_name="Open in SharePoint",
            enabled=True,
            traceable_to_source=context["traceability"].get("traceable_to_source", False),
            selected_item_type=metadata["item_type"],
        )
        if not opened:
            log_warn(
                "selection_action_failed",
                item_name=metadata["item_name"],
                item_path=metadata["item_path"],
                tree_role=context["panel_key"],
                node_origin=metadata["node_origin"],
                action_name="Open in SharePoint",
                enabled=True,
                traceable_to_source=context["traceability"].get("traceable_to_source", False),
                selected_item_type=metadata["item_type"],
            )
            QMessageBox.information(self, "Open in SharePoint", "Open in SharePoint is not available for this selection.")

    def handle_copy_selected_link(self):
        context = getattr(self, "_current_details_context", None)
        if context is None:
            QMessageBox.information(self, "Copy Link", "Select an item first.")
            return
        action = context["actions"].get("Copy Link", {})
        metadata = context["metadata"]
        if not action.get("enabled"):
            log_warn(
                "selection_action_failed",
                item_name=metadata["item_name"],
                item_path=metadata["item_path"],
                tree_role=context["panel_key"],
                node_origin=metadata["node_origin"],
                action_name="Copy Link",
                enabled=False,
                traceable_to_source=context["traceability"].get("traceable_to_source", False),
                selected_item_type=metadata["item_type"],
            )
            return
        QGuiApplication.clipboard().setText(action.get("target", ""))
        self.planned_moves_status.setText("Link copied to clipboard.")
        log_info(
            "selection_action_copy_link",
            item_name=metadata["item_name"],
            item_path=metadata["item_path"],
            tree_role=context["panel_key"],
            node_origin=metadata["node_origin"],
            action_name="Copy Link",
            enabled=True,
            traceable_to_source=context["traceability"].get("traceable_to_source", False),
            selected_item_type=metadata["item_type"],
        )

    def handle_open_selected_source_folder(self):
        context = getattr(self, "_current_details_context", None)
        if context is None:
            QMessageBox.information(self, "Open Source Folder", "Select an item first.")
            return
        action = context["actions"].get("Open Source Folder", {})
        metadata = context["metadata"]
        if not action.get("enabled"):
            log_warn(
                "selection_action_open_source_folder_failed",
                item_name=metadata["item_name"],
                item_path=metadata["item_path"],
                tree_role=context["panel_key"],
                node_origin=metadata["node_origin"],
                has_source_traceability=context["traceability"].get("traceable_to_source", False),
                used_fallback=False,
                opened_url="",
            )
            log_warn(
                "selection_action_failed",
                item_name=metadata["item_name"],
                item_path=metadata["item_path"],
                tree_role=context["panel_key"],
                node_origin=metadata["node_origin"],
                action_name="Open Source Folder",
                enabled=False,
                traceable_to_source=context["traceability"].get("traceable_to_source", False),
                selected_item_type=metadata["item_type"],
            )
            QMessageBox.information(self, "Open Source Folder", "This item cannot be traced back to a source folder.")
            return
        target_path = action.get("target", "")
        mode = action.get("mode", "")
        used_fallback = mode == "open_destination_sharepoint"
        log_info(
            "selection_action_open_source_folder_branch",
            item_name=metadata["item_name"],
            item_path=metadata["item_path"],
            tree_role=context["panel_key"],
            node_origin=metadata["node_origin"],
            has_source_traceability=context["traceability"].get("traceable_to_source", False),
            used_fallback=used_fallback,
            opened_url=target_path if used_fallback else "",
        )
        if mode == "open_destination_sharepoint":
            log_info(
                "selection_action_open_source_folder_fallback_to_sharepoint",
                item_name=metadata["item_name"],
                item_path=metadata["item_path"],
                tree_role=context["panel_key"],
                node_origin=metadata["node_origin"],
                has_source_traceability=context["traceability"].get("traceable_to_source", False),
                used_fallback=True,
                opened_url=target_path,
            )
            opened = QDesktopServices.openUrl(QUrl(target_path))
            log_info(
                "selection_action_open_source_folder",
                item_name=metadata["item_name"],
                item_path=metadata["item_path"],
                tree_role=context["panel_key"],
                node_origin=metadata["node_origin"],
                action_name="Open Source Folder",
                enabled=True,
                traceable_to_source=context["traceability"].get("traceable_to_source", False),
                selected_item_type=metadata["item_type"],
            )
            if not opened:
                log_warn(
                    "selection_action_open_source_folder_failed",
                    item_name=metadata["item_name"],
                    item_path=metadata["item_path"],
                    tree_role=context["panel_key"],
                    node_origin=metadata["node_origin"],
                    has_source_traceability=context["traceability"].get("traceable_to_source", False),
                    used_fallback=True,
                    opened_url=target_path,
                )
                log_warn(
                    "selection_action_failed",
                    item_name=metadata["item_name"],
                    item_path=metadata["item_path"],
                    tree_role=context["panel_key"],
                    node_origin=metadata["node_origin"],
                    action_name="Open Source Folder",
                    enabled=True,
                    traceable_to_source=context["traceability"].get("traceable_to_source", False),
                    selected_item_type=metadata["item_type"],
                )
                QMessageBox.information(self, "Open Source Folder", "Could not open the destination folder in SharePoint.")
            return

        log_info(
            "selection_action_open_source_folder_trace_to_source",
            item_name=metadata["item_name"],
            item_path=metadata["item_path"],
            tree_role=context["panel_key"],
            node_origin=metadata["node_origin"],
            has_source_traceability=context["traceability"].get("traceable_to_source", False),
            used_fallback=False,
            opened_url="",
        )
        prefer_container = mode == "select_source_container"
        navigated = self._select_source_item_by_path(target_path, prefer_container=prefer_container)
        if not navigated:
            navigated = self._start_source_navigation(target_path, prefer_container=prefer_container)
        log_info(
            "selection_action_open_source_folder",
            item_name=metadata["item_name"],
            item_path=metadata["item_path"],
            tree_role=context["panel_key"],
            node_origin=metadata["node_origin"],
            action_name="Open Source Folder",
            enabled=True,
            traceable_to_source=context["traceability"].get("traceable_to_source", False),
            selected_item_type=metadata["item_type"],
        )
        if not navigated:
            log_warn(
                "selection_action_open_source_folder_failed",
                item_name=metadata["item_name"],
                item_path=metadata["item_path"],
                tree_role=context["panel_key"],
                node_origin=metadata["node_origin"],
                has_source_traceability=context["traceability"].get("traceable_to_source", False),
                used_fallback=False,
                opened_url="",
            )
            log_warn(
                "selection_action_failed",
                item_name=metadata["item_name"],
                item_path=metadata["item_path"],
                tree_role=context["panel_key"],
                node_origin=metadata["node_origin"],
                action_name="Open Source Folder",
                enabled=True,
                traceable_to_source=context["traceability"].get("traceable_to_source", False),
                selected_item_type=metadata["item_type"],
            )
            QMessageBox.information(self, "Open Source Folder", "The related source folder could not be located in the source tree.")

    def handle_go_to_destination(self):
        context = getattr(self, "_current_details_context", None)
        if context is None:
            QMessageBox.information(self, "Go to Destination", "Select an item first.")
            return
        if context.get("panel_key") != "source":
            QMessageBox.information(
                self,
                "Go to Destination",
                "Select a source item that has an assignment.",
            )
            return
        action = context["actions"].get("Go to Destination", {})
        metadata = context["metadata"]
        if not action.get("enabled"):
            log_warn(
                "selection_action_failed",
                item_name=metadata["item_name"],
                item_path=metadata["item_path"],
                tree_role=context["panel_key"],
                node_origin=metadata["node_origin"],
                action_name="Go to Destination",
                enabled=False,
                traceable_to_source=context["traceability"].get("traceable_to_source", False),
                selected_item_type=metadata["item_type"],
            )
            QMessageBox.information(
                self,
                "Go to Destination",
                "This source item has no assignment to show in the destination tree.",
            )
            return
        self._handle_go_to_destination_for_source(dict(context.get("node_data") or {}))

    def get_tree_item_node_data(self, item):
        if item is None:
            return None

        if isinstance(item, QModelIndex):
            node_data = item.data(Qt.UserRole) or {}
        else:
            node_data = item.data(0, Qt.UserRole) or {}
        if node_data.get("placeholder"):
            return None

        return dict(node_data)

    def get_selected_tree_node_data(self, tree_role):
        tree = self.source_tree_widget if tree_role == "source" else self.destination_tree_widget
        if tree_role == "source" and self._source_tree_uses_model_view():
            ix = tree.currentIndex()
            return self.get_tree_item_node_data(ix)

        selected_items = tree.selectedItems()
        if not selected_items:
            return None

        return self.get_tree_item_node_data(selected_items[0])

    def select_tree_item_at_position(self, tree, position):
        if isinstance(tree, QTreeView):
            ix = tree.indexAt(position)
            if not ix.isValid():
                tree.clearSelection()
                return None
            tree.setCurrentIndex(ix)
            return ix

        item = tree.itemAt(position)
        if item is None:
            tree.clearSelection()
            return None

        tree.setCurrentItem(item)
        item.setSelected(True)
        return item

    def build_node_key(self, node_data, default_role):
        if node_data is None:
            return None

        return (
            node_data.get("tree_role", default_role),
            node_data.get("drive_id", ""),
            node_data.get("id", ""),
            self._tree_item_path(node_data).lower(),
        )

    def node_keys_match(self, left_node, right_node, default_role):
        left_key = self.build_node_key(left_node, default_role)
        right_key = self.build_node_key(right_node, default_role)
        if left_key is None or right_key is None:
            return False

        left_role, left_drive, left_id, left_path = left_key
        right_role, right_drive, right_id, right_path = right_key
        if left_role != right_role:
            return False
        if left_drive and right_drive and left_drive != right_drive:
            return False
        if left_id and right_id:
            return left_id == right_id
        return self._paths_equivalent(left_path, right_path, left_role or default_role)

    def find_planned_move_index_by_source(self, source_node):
        if self.build_node_key(source_node, "source") is None:
            return None

        for index, existing_move in enumerate(self.planned_moves):
            if self.node_keys_match(existing_move.get("source", {}), source_node, "source"):
                return index

        return None

    def find_planned_move_index_by_destination(self, destination_node):
        if not isinstance(destination_node, dict):
            return None

        for index, existing_move in enumerate(self.planned_moves):
            if self.node_keys_match(existing_move.get("destination", {}), destination_node, "destination"):
                return index

        dest_path = self._canonical_destination_projection_path(
            self._tree_item_path(destination_node)
            or destination_node.get("display_path")
            or destination_node.get("item_path")
            or destination_node.get("destination_path")
            or ""
        )
        if not dest_path:
            return None
        for index, existing_move in enumerate(self.planned_moves):
            alloc_path = self._canonical_destination_projection_path(
                self._allocation_projection_path(existing_move)
            )
            if alloc_path and alloc_path == dest_path:
                return index

        return None

    def is_proposed_destination_node(self, node_data):
        if node_data is None:
            return False
        if node_data.get("tree_role", "destination") != "destination":
            return False
        return self.node_is_proposed(node_data)

    def node_is_proposed(self, node_data):
        if node_data is None:
            return False

        return bool(node_data.get("proposed")) or str(node_data.get("node_origin", "")).lower() == "proposed"

    def node_is_planned_allocation(self, node_data):
        if node_data is None:
            return False

        origin = str(node_data.get("node_origin", "")).lower()
        overlay_state = str(node_data.get("overlay_state", "")).lower()
        return (
            bool(node_data.get("planned_allocation"))
            or origin == "plannedallocation"
            or overlay_state == "plannedallocation"
        )

    def node_is_valid_destination_target(self, node_data):
        if node_data is None:
            return False

        origin = str(node_data.get("node_origin", "")).lower()
        return (
            bool(node_data.get("is_folder"))
            and not self.node_is_planned_allocation(node_data)
            and origin not in {"projectedallocationdescendant"}
        )

    def show_source_context_menu(self, position):
        item = self.select_tree_item_at_position(self.source_tree_widget, position)
        node_data = self.get_tree_item_node_data(item)
        if node_data is None:
            return

        selected_destination = self.get_selected_tree_node_data("destination")
        has_assignment = self.find_planned_move_index_by_source(node_data) is not None
        can_assign = self.node_is_valid_destination_target(selected_destination)

        menu = QMenu(self)
        menu.setAttribute(Qt.WA_TranslucentBackground, False)
        assign_action = menu.addAction("Assign to Selected Destination")
        assign_action.setEnabled(can_assign)
        assign_action.triggered.connect(self.handle_assign)

        unassign_action = menu.addAction("Unassign")
        unassign_action.setEnabled(has_assignment)
        unassign_action.triggered.connect(self.handle_unassign)

        menu.addSeparator()

        go_to_dest_action = menu.addAction("Go to Destination")
        go_to_dest_action.setEnabled(self._planned_move_for_source_node(node_data) is not None)
        go_to_dest_action.triggered.connect(
            lambda _checked=False, nd=dict(node_data): self._handle_go_to_destination_for_source(nd)
        )

        menu.addSeparator()

        open_file_action = menu.addAction("Open File")
        open_file_action.triggered.connect(lambda: self.handle_open_source_item(node_data))

        open_browser_action = menu.addAction("Open in Browser")
        open_browser_action.triggered.connect(lambda: self.handle_open_item_in_browser(node_data))

        copy_link_action = menu.addAction("Copy Link")
        copy_link_action.triggered.connect(lambda: self.handle_copy_item_link(node_data))

        menu.exec(self.source_tree_widget.viewport().mapToGlobal(position))

    def show_destination_context_menu(self, position):
        item = self.select_tree_item_at_position(self.destination_tree_widget, position)
        node_data = self.get_tree_item_node_data(item)
        if node_data is None:
            return

        selected_source = self.get_selected_tree_node_data("source")
        is_planned_allocation = self.node_is_planned_allocation(node_data)
        is_proposed = self.node_is_proposed(node_data)
        node_origin = str(node_data.get("node_origin", "")).lower()
        is_projected_descendant = node_origin == "projectedallocationdescendant"
        is_projected_destination = node_origin == "projecteddestination"
        is_real_destination = not (is_planned_allocation or is_proposed or is_projected_descendant or is_projected_destination)
        is_folder = bool(node_data.get("is_folder"))

        menu = QMenu(self)
        menu.setAttribute(Qt.WA_TranslucentBackground, False)

        assign_action = menu.addAction("Assign Selected Source Here")
        assign_action.setEnabled(
            selected_source is not None
            and self.node_is_valid_destination_target(node_data)
            and not is_projected_descendant
        )
        assign_action.triggered.connect(self.handle_assign)

        new_proposed_action = menu.addAction("New Proposed Folder Here")
        new_proposed_action.setEnabled(
            is_folder
            and not is_planned_allocation
            and not is_projected_descendant
        )
        new_proposed_action.triggered.connect(lambda: self.handle_new_proposed_folder(node_data))

        rename_proposed_action = menu.addAction("Rename Proposed Folder")
        rename_proposed_action.setEnabled(is_proposed)
        rename_proposed_action.triggered.connect(lambda: self.handle_rename_proposed_folder(item))

        delete_proposed_action = menu.addAction("Delete Proposed Folder")
        delete_proposed_action.setEnabled(is_proposed)
        delete_proposed_action.triggered.connect(lambda: self.handle_delete_proposed_folder(item))

        menu.addSeparator()

        cut_action = menu.addAction("Cut")
        cut_action.setEnabled(self._build_destination_move_payload(node_data) is not None)
        cut_action.triggered.connect(lambda: self.handle_cut_destination_item(node_data))

        paste_action = menu.addAction("Paste Here")
        paste_action.setEnabled(
            bool(getattr(self, "_destination_cut_buffer", None))
            and is_folder
            and not is_planned_allocation
            and not is_projected_descendant
        )
        paste_action.triggered.connect(lambda: self.handle_paste_destination_item(node_data))

        menu.addSeparator()

        rename_planned_action = menu.addAction("Rename Planned Item")
        rename_planned_action.setEnabled(is_planned_allocation or is_projected_descendant)
        rename_planned_action.triggered.connect(lambda: self.handle_rename_planned_item(node_data))

        remove_planned_action = menu.addAction("Remove Planned Allocation")
        remove_planned_action.setEnabled(is_planned_allocation)
        remove_planned_action.triggered.connect(lambda: self.handle_remove_planned_allocation(node_data))

        menu.addSeparator()

        open_file_action = menu.addAction("Open File")
        open_file_action.setEnabled(not is_folder)
        open_file_action.triggered.connect(self.handle_open_selected_file)

        open_browser_action = menu.addAction("Open in SharePoint")
        open_browser_action.setEnabled(bool(node_data.get("web_url")) or is_real_destination)
        open_browser_action.triggered.connect(self.handle_open_selected_in_browser)

        copy_link_action = menu.addAction("Copy Link")
        copy_link_action.setEnabled(bool(node_data.get("web_url") or node_data.get("display_path") or node_data.get("item_path")))
        copy_link_action.triggered.connect(self.handle_copy_selected_link)

        open_source_action = menu.addAction("Open Source Folder")
        open_source_action.setEnabled(not is_real_destination or bool(node_data.get("web_url")))
        open_source_action.triggered.connect(self.handle_open_selected_source_folder)

        menu.exec(self.destination_tree_widget.viewport().mapToGlobal(position))

    def handle_open_source_item(self, node_data):
        local_path = node_data.get("local_path") or node_data.get("file_path")
        if local_path and QDesktopServices.openUrl(QUrl.fromLocalFile(local_path)):
            return

        QMessageBox.information(self, "Open File", "Open File is not available for this item yet.")

    def handle_open_item_in_browser(self, node_data):
        web_url = node_data.get("web_url")
        if web_url and QDesktopServices.openUrl(QUrl(web_url)):
            return

        QMessageBox.information(self, "Open in Browser", "Open in Browser is not available for this item yet.")

    def handle_copy_item_link(self, node_data):
        link = node_data.get("web_url") or node_data.get("display_path") or node_data.get("item_path")
        if not link:
            QMessageBox.information(self, "Copy Link", "No link is available for the selected item.")
            return

        QGuiApplication.clipboard().setText(link)
        self.planned_moves_status.setText("Link copied to clipboard.")

    def _next_inline_proposed_folder_name(self, parent_item):
        existing_names = set()
        for index in range(parent_item.childCount()):
            child = parent_item.child(index)
            child_data = child.data(0, Qt.UserRole) or {}
            existing_names.add(str(child_data.get("name") or child.text(0) or "").strip().lower())

        base_name = "New Folder"
        if base_name.lower() not in existing_names:
            return base_name

        suffix = 2
        while True:
            candidate = f"{base_name} ({suffix})"
            if candidate.lower() not in existing_names:
                return candidate
            suffix += 1

    def _remove_inline_proposed_folder_item(self, item):
        if item is None:
            return
        parent_item = item.parent()
        if parent_item is not None:
            parent_item.removeChild(item)
        else:
            index = self.destination_tree_widget.indexOfTopLevelItem(item)
            if index >= 0:
                self.destination_tree_widget.takeTopLevelItem(index)

    def _refresh_destination_item_label(self, item):
        if item is None:
            return
        node_data = self.get_tree_item_node_data(item) or {}
        if not node_data:
            return
        name = str(node_data.get("name") or item.text(0) or "").strip()
        if self.node_is_proposed(node_data):
            base_label = f"Folder: {name}"
        elif self.node_is_planned_allocation(node_data):
            prefix = "Folder" if node_data.get("is_folder", True) else "File"
            base_label = f"{prefix}: {name} [Allocated]"
        elif str(node_data.get("node_origin", "")).lower() == "projectedallocationdescendant":
            prefix = "Folder" if node_data.get("is_folder", True) else "File"
            base_label = f"{prefix}: {name}"
        else:
            return
        node_data["base_display_label"] = base_label
        item.setData(0, Qt.UserRole, node_data)
        self._apply_tree_item_visual_state(item, node_data)

    def _rename_visible_destination_subtree(self, item, original_path, updated_path):
        if item is None:
            return
        node_data = self.get_tree_item_node_data(item) or {}
        current_path = self.normalize_memory_path(node_data.get("display_path") or node_data.get("item_path") or "")
        if current_path == original_path or current_path.startswith(original_path + "\\"):
            suffix = current_path[len(original_path):]
            next_path = self.normalize_memory_path(updated_path + suffix)
            next_name = self._path_segments(next_path)[-1] if self._path_segments(next_path) else str(node_data.get("name", "")).strip()
            node_data["name"] = next_name
            node_data["real_name"] = next_name
            node_data["display_path"] = next_path
            node_data["item_path"] = next_path
            node_data["destination_path"] = next_path
            item.setData(0, Qt.UserRole, node_data)
            self._refresh_destination_item_label(item)
        for index in range(item.childCount()):
            self._rename_visible_destination_subtree(item.child(index), original_path, updated_path)

    def _remove_visible_destination_subtree_by_prefix(self, item, target_path):
        if item is None:
            return False
        node_data = self.get_tree_item_node_data(item) or {}
        current_path = self._canonical_destination_projection_path(
            node_data.get("display_path") or node_data.get("item_path") or ""
        ) or self.normalize_memory_path(node_data.get("display_path") or node_data.get("item_path") or "")
        normalized_target = self._canonical_destination_projection_path(target_path) or self.normalize_memory_path(target_path)
        if current_path == normalized_target or current_path.startswith(normalized_target + "\\"):
            self._remove_inline_proposed_folder_item(item)
            return True
        for index in range(item.childCount() - 1, -1, -1):
            child = item.child(index)
            self._remove_visible_destination_subtree_by_prefix(child, normalized_target)
        return False

    def _persist_planning_change_lightweight(self):
        self._save_draft_shell(force=True)
        self._rebuild_submission_visual_cache()
        self._queue_deferred_planning_refresh(
            "planning_change_lightweight",
            source_projection_paths=self._collect_current_source_projection_paths(),
        )
        self.update_progress_summaries()
        try:
            if getattr(self, "source_tree_widget", None) is not None:
                self.source_tree_widget.viewport().update()
            if getattr(self, "destination_tree_widget", None) is not None:
                self.destination_tree_widget.viewport().update()
        except Exception as exc:
            self._log_restore_exception("persist_planning_change_lightweight", exc)

    def _destination_path_exists_under_parent(self, parent_item, destination_path, *, ignore_item=None):
        normalized_target = self.normalize_memory_path(destination_path)
        for index in range(parent_item.childCount()):
            child = parent_item.child(index)
            if child is ignore_item:
                continue
            child_data = child.data(0, Qt.UserRole) or {}
            child_path = child_data.get("item_path") or child_data.get("display_path") or ""
            if self._paths_equivalent(child_path, normalized_target, "destination"):
                return True
        return False

    def _begin_inline_proposed_folder_creation(self, destination_node, parent_item):
        proposed_path_base = self._tree_item_path(destination_node)
        default_name = self._next_inline_proposed_folder_name(parent_item)
        proposed_path = self.normalize_memory_path(
            "\\".join(part for part in [proposed_path_base, default_name] if part)
        )
        temp_node = {
            "id": f"INLINE-PROP-{datetime.utcnow().strftime('%H%M%S%f')[-8:]}",
            "name": default_name,
            "real_name": default_name,
            "display_path": proposed_path,
            "item_path": proposed_path,
            "destination_path": proposed_path,
            "tree_role": "destination",
            "is_folder": True,
            "proposed": True,
            "node_origin": "proposed",
            "overlay_state": "proposed",
            "_inline_new_proposed": True,
            "_inline_commit_ready": False,
            "_inline_parent_path": proposed_path_base,
        }
        self._remove_placeholder_children(parent_item)
        item = QTreeWidgetItem([default_name])
        item.setData(0, Qt.UserRole, temp_node)
        item.setFlags(item.flags() | Qt.ItemIsEditable)
        parent_item.addChild(item)
        parent_item.setExpanded(True)
        self.destination_tree_widget.setCurrentItem(item)
        item.setSelected(True)
        self.destination_tree_status.setText("Type the new folder name and press Enter.")
        def _start_inline_edit(target_item=item):
            current_node = self.get_tree_item_node_data(target_item) or {}
            self._inline_proposed_commit_item_id = str(current_node.get("id", "") or "")
            self.destination_tree_widget.setFocus()
            self.destination_tree_widget.scrollToItem(target_item)
            self.destination_tree_widget.setCurrentItem(target_item)
            index = self.destination_tree_widget.indexFromItem(target_item, 0)
            if index.isValid():
                self.destination_tree_widget.edit(index)

        QTimer.singleShot(180, _start_inline_edit)
        return item

    def _begin_inline_proposed_folder_rename(self, item):
        if item is None:
            return
        node_data = self.get_tree_item_node_data(item) or {}
        original_path = self.normalize_memory_path(node_data.get("display_path") or node_data.get("item_path") or "")
        if not original_path:
            return
        node_data["_inline_rename_proposed"] = True
        node_data["_inline_original_path"] = original_path
        item.setData(0, Qt.UserRole, node_data)

        def _start_inline_edit(target_item=item):
            current_node = self.get_tree_item_node_data(target_item) or {}
            self._inline_proposed_commit_item_id = str(current_node.get("id", "") or "")
            target_item.setFlags(target_item.flags() | Qt.ItemIsEditable)
            self.destination_tree_widget.setFocus()
            self.destination_tree_widget.scrollToItem(target_item)
            self.destination_tree_widget.setCurrentItem(target_item)
            index = self.destination_tree_widget.indexFromItem(target_item, 0)
            if index.isValid():
                self.destination_tree_widget.edit(index)

        QTimer.singleShot(180, _start_inline_edit)

    def handle_new_proposed_folder(self, destination_node=None, parent_item=None):
        if destination_node is None:
            destination_node = self.get_selected_tree_node_data("destination")

        if destination_node is None:
            QMessageBox.information(self, "Propose Folder", "Select a destination folder first.")
            return

        if not destination_node.get("is_folder"):
            QMessageBox.information(self, "Propose Folder", "Select a destination folder first.")
            return

        if self.node_is_planned_allocation(destination_node):
            QMessageBox.information(self, "Propose Folder", "Cannot add a proposed folder under a planned allocation node.")
            return

        if self.node_is_proposed(destination_node):
            existing_proposed = self._find_proposed_folder_record_by_path(
                destination_node.get("display_path") or destination_node.get("item_path") or ""
            )
            if existing_proposed is not None and self._is_proposed_folder_submitted(existing_proposed):
                self._show_submitted_item_locked_message(
                    "Propose Folder",
                    f"'{destination_node.get('name', 'This proposed folder')}'",
                    self._submitted_batch_id_for_proposed_folder(existing_proposed),
                )
                return

        if parent_item is None:
            destination_path = self._tree_item_path(destination_node)
            parent_item = self._find_visible_destination_item_by_path(destination_path)
        if parent_item is None:
            QMessageBox.information(self, "Propose Folder", "The selected destination folder is not visible yet.")
            return
        QTimer.singleShot(
            0,
            lambda destination_node=destination_node: self._begin_inline_proposed_folder_creation(
                destination_node,
                self._find_visible_destination_item_by_path(self._tree_item_path(destination_node)),
            ) if self._find_visible_destination_item_by_path(self._tree_item_path(destination_node)) is not None else None,
        )

    def _rewrite_proposed_branch_runtime_paths(self, original_path, updated_path):
        normalized_original = self.normalize_memory_path(original_path)
        normalized_updated = self.normalize_memory_path(updated_path)
        if not normalized_original or not normalized_updated:
            return

        updated_proposed_folders = []
        for proposed_folder in self.proposed_folders:
            folder_path = self._proposed_destination_path(proposed_folder)
            if folder_path == normalized_original or folder_path.startswith(normalized_original + "\\"):
                suffix = folder_path[len(normalized_original):]
                next_path = self.normalize_memory_path(normalized_updated + suffix)
                next_name = self._path_segments(next_path)[-1] if self._path_segments(next_path) else proposed_folder.FolderName
                updated_proposed_folders.append(
                    ProposedFolder(
                        DestinationId=proposed_folder.DestinationId,
                        FolderName=next_name,
                        DestinationPath=next_path,
                        ParentPath=self._destination_parent_path(next_path),
                        IsSelectable=proposed_folder.IsSelectable,
                        IsProposed=proposed_folder.IsProposed,
                        Status=proposed_folder.Status,
                        RequestedBy=proposed_folder.RequestedBy,
                        RequestedDate=proposed_folder.RequestedDate,
                    )
                )
            else:
                updated_proposed_folders.append(proposed_folder)
        self.proposed_folders = updated_proposed_folders

        for move in self.planned_moves:
            destination_path = self.normalize_memory_path(move.get("destination_path", ""))
            if destination_path == normalized_original or destination_path.startswith(normalized_original + "\\"):
                suffix = destination_path[len(normalized_original):]
                next_destination_path = self.normalize_memory_path(normalized_updated + suffix)
                move["destination_path"] = next_destination_path
                move.setdefault("destination", {})
                move["destination"]["display_path"] = next_destination_path
                move["destination"]["item_path"] = next_destination_path
                move["destination"]["destination_path"] = next_destination_path

    def _proposed_branch_contains_submitted_items(self, branch_path):
        normalized_branch = self.normalize_memory_path(branch_path)
        if not normalized_branch:
            return False

        for proposed_folder in self.proposed_folders:
            proposed_path = self._proposed_destination_path(proposed_folder)
            if (
                proposed_path
                and (proposed_path == normalized_branch or proposed_path.startswith(normalized_branch + "\\"))
                and self._is_proposed_folder_submitted(proposed_folder)
            ):
                return True

        for move in self.planned_moves:
            destination_path = self._canonical_destination_projection_path(move.get("destination_path", ""))
            if (
                destination_path
                and (destination_path == normalized_branch or destination_path.startswith(normalized_branch + "\\"))
                and self._is_move_submitted(move)
            ):
                return True

        return False

    def _destination_target_snapshot(self, target_node, target_path):
        destination_node = dict(target_node or {})
        destination_node["name"] = self._path_segments(target_path)[-1] if self._path_segments(target_path) else ""
        destination_node["real_name"] = destination_node["name"]
        destination_node["display_path"] = target_path
        destination_node["item_path"] = target_path
        destination_node["destination_path"] = target_path
        destination_node["tree_role"] = "destination"
        destination_node["is_folder"] = True
        return destination_node

    def _destination_paths_match_exact(self, left_path, right_path):
        left = self._canonical_destination_projection_path(left_path) or self.normalize_memory_path(left_path)
        right = self._canonical_destination_projection_path(right_path) or self.normalize_memory_path(right_path)
        return bool(left and right and left == right)

    def _resolve_planned_move_for_destination_node(self, node_data):
        move_index = self.find_planned_move_index_by_destination(node_data)
        move = self.planned_moves[move_index] if move_index is not None else None
        inherited_move = None

        node_origin = str(node_data.get("node_origin", "")).lower()
        if move is None and node_origin == "projectedallocationdescendant":
            source_path = self._canonical_source_projection_path(node_data.get("source_path", ""))
            move = self._find_exact_planned_move_for_source_path(source_path)
            if move is not None:
                move_index = self.planned_moves.index(move)

        if move is None and node_origin == "projectedallocationdescendant":
            source_path = self._canonical_source_projection_path(node_data.get("source_path", ""))
            inherited_move = self._find_inherited_planned_move_for_source_path(source_path)

        return move_index, move, inherited_move

    def _build_destination_move_payload(self, node_data):
        if self.node_is_proposed(node_data):
            source_path = self._canonical_destination_projection_path(
                node_data.get("display_path") or node_data.get("item_path") or ""
            ) or self.normalize_memory_path(node_data.get("display_path") or node_data.get("item_path") or "")
            if not source_path:
                return None
            if self._proposed_branch_contains_submitted_items(source_path):
                return None
            return {
                "kind": "proposed_branch",
                "path": source_path,
                "label": str(node_data.get("name", "") or "This proposed folder"),
            }

        move_index, move, inherited_move = self._resolve_planned_move_for_destination_node(node_data)
        if move is None and inherited_move is None:
            return None
        if (move is not None and self._is_move_submitted(move)) or (
            inherited_move is not None and self._is_move_submitted(inherited_move)
        ):
            return None

        source_path = self._canonical_source_projection_path(node_data.get("source_path", ""))
        if move is not None:
            source_path = self._canonical_source_projection_path(move.get("source_path", "")) or source_path

        return {
            "kind": "planned_item",
            "source_path": source_path,
            "display_path": self._canonical_destination_projection_path(
                node_data.get("display_path") or node_data.get("item_path") or ""
            ) or self.normalize_memory_path(node_data.get("display_path") or node_data.get("item_path") or ""),
            "label": str(node_data.get("name", "") or self._move_target_name(move or inherited_move) or "This planned item"),
        }

    def _move_planned_destination_node(self, source_node, target_node):
        move_index, move, inherited_move = self._resolve_planned_move_for_destination_node(source_node)
        if move is None and inherited_move is None:
            self.destination_tree_status.setText("No planned allocation exists for the selected destination item.")
            return False
        if move is not None and self._is_move_submitted(move):
            self._show_submitted_item_locked_message(
                "Move Planned Item",
                f"'{move.get('source_name', 'This item')}'",
                self._submitted_batch_id_for_move(move),
            )
            return False
        if inherited_move is not None and self._is_move_submitted(inherited_move):
            self._show_submitted_item_locked_message(
                "Move Planned Item",
                f"'{source_node.get('name', 'This item')}'",
                self._submitted_batch_id_for_move(inherited_move),
            )
            return False

        target_path = self._canonical_destination_projection_path(
            target_node.get("display_path") or target_node.get("item_path") or ""
        ) or self.normalize_memory_path(target_node.get("display_path") or target_node.get("item_path") or "")
        if not target_path:
            self.destination_tree_status.setText("Could not resolve the destination folder for the move.")
            return False

        target_name = str(source_node.get("name") or self._move_target_name(move or inherited_move) or "").strip()
        if not target_name:
            self.destination_tree_status.setText("Could not resolve the planned item name for the move.")
            return False

        target_projection_path = self.normalize_memory_path(f"{target_path}\\{target_name}")
        current_projection_path = self._canonical_destination_projection_path(
            source_node.get("display_path") or source_node.get("item_path") or ""
        ) or self.normalize_memory_path(source_node.get("display_path") or source_node.get("item_path") or "")
        if target_projection_path == current_projection_path:
            return False

        target_parent_item = self._find_visible_destination_item_by_path(target_path)
        if target_parent_item is not None and self._destination_path_exists_under_parent(target_parent_item, target_projection_path):
            QMessageBox.information(
                self,
                "Move Planned Item",
                "A destination item with that name already exists in the selected folder.",
            )
            return False

        destination_node = self._destination_target_snapshot(target_node, target_path)
        if move is None and inherited_move is not None:
            source_item = self._find_visible_source_item_by_path(source_node.get("source_path", ""))
            source_item_node = source_item.data(0, Qt.UserRole) or {} if source_item is not None else {}
            if not source_item_node:
                source_item_node = {
                    "id": "",
                    "name": target_name,
                    "real_name": target_name,
                    "display_path": source_node.get("source_path", ""),
                    "item_path": source_node.get("source_path", ""),
                    "tree_role": "source",
                    "drive_id": "",
                    "is_folder": bool(source_node.get("is_folder", False)),
                }
            override_move = self.build_planned_move_record(source_item_node, destination_node)
            override_move["allocation_method"] = "Manual - Override"
            override_move["target_name"] = target_name
            self.planned_moves.append(override_move)
        else:
            move["destination_path"] = target_path
            move["destination_id"] = destination_node.get("id", "")
            move["destination_name"] = destination_node.get("name", "")
            move["destination"] = dict(destination_node)
            move["target_name"] = target_name

        self.planned_moves_status.setText("Planned item moved.")
        self._persist_planning_change("planned_item_moved")
        return True

    def handle_destination_draft_move(self, source_item, target_item):
        source_node = self.get_tree_item_node_data(source_item)
        target_node = self.get_tree_item_node_data(target_item)
        if not source_node or not target_node:
            return
        if not target_node.get("is_folder"):
            self.destination_tree_status.setText("Drop onto a destination folder.")
            return
        if self.node_is_planned_allocation(target_node):
            self.destination_tree_status.setText("Cannot move items under a planned allocation node.")
            return

        if self.node_is_proposed(source_node):
            source_path = self._canonical_destination_projection_path(
                source_node.get("display_path") or source_node.get("item_path") or ""
            ) or self.normalize_memory_path(source_node.get("display_path") or source_node.get("item_path") or "")
            target_path = self._canonical_destination_projection_path(
                target_node.get("display_path") or target_node.get("item_path") or ""
            ) or self.normalize_memory_path(target_node.get("display_path") or target_node.get("item_path") or "")
            if not source_path or not target_path:
                self.destination_tree_status.setText("Could not resolve the proposed branch move.")
                return
            if target_path == source_path or target_path.startswith(source_path + "\\"):
                QMessageBox.information(self, "Move Proposed Folder", "A proposed folder cannot be moved inside itself.")
                return
            if self._proposed_branch_contains_submitted_items(source_path):
                self._show_submitted_item_locked_message(
                    "Move Proposed Folder",
                    f"'{source_node.get('name', 'This proposed folder')}'",
                )
                return
            if self.node_is_proposed(target_node):
                target_proposed = self._find_proposed_folder_record_by_path(target_path)
                if target_proposed is not None and self._is_proposed_folder_submitted(target_proposed):
                    self._show_submitted_item_locked_message(
                        "Move Proposed Folder",
                        f"'{target_node.get('name', 'This proposed folder')}'",
                        self._submitted_batch_id_for_proposed_folder(target_proposed),
                    )
                    return

            new_path = self.normalize_memory_path("\\".join(part for part in [target_path, source_node.get("name", "")] if part))
            if not new_path or self._destination_paths_match_exact(new_path, source_path):
                return
            if self._destination_path_exists_under_parent(target_item, new_path, ignore_item=source_item):
                QMessageBox.information(
                    self,
                    "Move Proposed Folder",
                    "A folder with that name already exists in the destination branch.",
                )
                return

            original_parent = source_item.parent()
            source_was_expanded = source_item.isExpanded()

            self._rewrite_proposed_branch_runtime_paths(source_path, new_path)
            if original_parent is not None:
                original_parent.removeChild(source_item)
                self._refresh_destination_item_visibility(original_parent)
            else:
                index = self.destination_tree_widget.indexOfTopLevelItem(source_item)
                if index >= 0:
                    self.destination_tree_widget.takeTopLevelItem(index)

            target_item.addChild(source_item)
            target_item.setExpanded(True)
            self._refresh_destination_item_visibility(target_item, expand=True)
            self._rename_visible_destination_subtree(source_item, source_path, new_path)
            source_item.setExpanded(source_was_expanded)
            self.destination_tree_widget.setCurrentItem(source_item)
            source_item.setSelected(True)
            self.destination_tree_status.setText("Proposed folder moved.")
            self._persist_planning_change_lightweight()
            self.on_tree_selection_changed("destination")
            return

        moved = self._move_planned_destination_node(source_node, target_node)
        if moved:
            self.clear_selection_details()

    def handle_cut_destination_item(self, node_data):
        payload = self._build_destination_move_payload(node_data)
        if payload is None:
            self.destination_tree_status.setText("This destination item cannot be moved.")
            return
        self._destination_cut_buffer = payload
        self.destination_tree_status.setText(f"Cut '{payload.get('label', 'item')}'. Use Paste Here on the target folder.")

    def handle_paste_destination_item(self, target_node):
        payload = getattr(self, "_destination_cut_buffer", None)
        if not payload:
            self.destination_tree_status.setText("Nothing is waiting to be pasted.")
            return
        target_item = self._find_visible_destination_item_by_path(
            target_node.get("display_path") or target_node.get("item_path") or ""
        )
        if target_item is None:
            self.destination_tree_status.setText("The destination folder is not visible yet.")
            return

        source_path = payload.get("path") or payload.get("display_path") or ""
        source_item = self._find_visible_destination_item_by_path(source_path)
        if source_item is None:
            self.destination_tree_status.setText("The cut item is no longer visible. Cut it again and retry.")
            self._destination_cut_buffer = None
            return

        self.handle_destination_draft_move(source_item, target_item)
        self._destination_cut_buffer = None

    def on_destination_tree_item_changed(self, item, column):
        if column != 0:
            return
        node_data = self.get_tree_item_node_data(item)
        if not node_data:
            return
        is_new_inline = bool(node_data.get("_inline_new_proposed"))
        is_rename_inline = bool(node_data.get("_inline_rename_proposed"))
        if not is_new_inline and not is_rename_inline:
            return
        inline_id = str(node_data.get("id", "") or "")
        if not inline_id or inline_id != getattr(self, "_inline_proposed_commit_item_id", ""):
            return

        folder_name = str(item.text(0) or "").strip()
        parent_path = self.normalize_memory_path(node_data.get("_inline_parent_path", ""))
        original_path = self.normalize_memory_path(node_data.get("_inline_original_path", ""))
        if not folder_name:
            if is_rename_inline:
                fallback_name = self._path_segments(original_path)[-1] if self._path_segments(original_path) else node_data.get("name", "")
                item.setText(0, fallback_name)
                node_data["_inline_rename_proposed"] = False
                item.setData(0, Qt.UserRole, node_data)
                self._inline_proposed_commit_item_id = ""
                self.destination_tree_status.setText("Proposed folder rename cancelled.")
                return
            self._remove_inline_proposed_folder_item(item)
            self._inline_proposed_commit_item_id = ""
            self.destination_tree_status.setText("Proposed folder creation cancelled.")
            return

        parent_item = item.parent()
        if parent_item is None:
            if is_new_inline:
                self._remove_inline_proposed_folder_item(item)
            return
        target_parent_path = parent_path if is_new_inline else self._destination_parent_path(original_path)
        proposed_path = self.normalize_memory_path(
            "\\".join(part for part in [target_parent_path, folder_name] if part)
        )
        if self._destination_path_exists_under_parent(parent_item, proposed_path, ignore_item=item):
            QMessageBox.information(
                self,
                "Proposed Folder",
                "A folder with that name already exists here.",
            )
            QTimer.singleShot(
                120,
                lambda item=item: self.destination_tree_widget.edit(
                    self.destination_tree_widget.indexFromItem(item, 0)
                ),
            )
            return

        if is_new_inline:
            proposed_folder = ProposedFolder(
                DestinationId=f"PROP-{datetime.utcnow().strftime('%H%M%S%f')[-8:]}",
                FolderName=folder_name,
                DestinationPath=proposed_path,
                ParentPath=target_parent_path,
                IsSelectable=True,
                IsProposed=True,
                Status="Proposed",
            )
            self.proposed_folders = [
                row for row in self.proposed_folders
                if self._proposed_destination_path(row) != self._proposed_destination_path(proposed_folder)
            ]
            self.proposed_folders.append(proposed_folder)
            node_data.update(
                {
                    "name": folder_name,
                    "real_name": folder_name,
                    "display_path": proposed_path,
                    "item_path": proposed_path,
                    "destination_path": proposed_path,
                    "_inline_new_proposed": False,
                }
            )
            item.setData(0, Qt.UserRole, node_data)
            item.setText(0, folder_name)
            self._inline_proposed_commit_item_id = ""
            self.destination_tree_status.setText("Proposed folder added.")
            self._save_draft_shell(force=True)
            self._rebuild_submission_visual_cache()
            self.update_progress_summaries()
            selected_item = self._find_visible_destination_item_by_path(proposed_path)
            if selected_item is not None:
                parent = selected_item.parent()
                while parent is not None:
                    parent.setExpanded(True)
                    parent = parent.parent()
                self.destination_tree_widget.setCurrentItem(selected_item)
                selected_item.setSelected(True)
                self.on_tree_selection_changed("destination")
            return

        self._rewrite_proposed_branch_runtime_paths(original_path, proposed_path)

        self._rename_visible_destination_subtree(item, original_path, proposed_path)
        refreshed_node = self.get_tree_item_node_data(item) or {}
        refreshed_node["_inline_rename_proposed"] = False
        item.setData(0, Qt.UserRole, refreshed_node)
        self._inline_proposed_commit_item_id = ""
        self.destination_tree_status.setText("Proposed folder renamed.")
        self._persist_planning_change_lightweight()
        selected_item = self._find_visible_destination_item_by_path(proposed_path)
        if selected_item is not None:
            self.destination_tree_widget.setCurrentItem(selected_item)
            selected_item.setSelected(True)
            self.on_tree_selection_changed("destination")

    def handle_rename_proposed_folder(self, item):
        node_data = self.get_tree_item_node_data(item)
        if not self.node_is_proposed(node_data):
            return

        existing_proposed = self._find_proposed_folder_record_by_path(
            node_data.get("display_path") or node_data.get("item_path") or ""
        )
        if existing_proposed is not None and self._is_proposed_folder_submitted(existing_proposed):
            self._show_submitted_item_locked_message(
                "Rename Proposed Folder",
                f"'{node_data.get('name', 'This proposed folder')}'",
                self._submitted_batch_id_for_proposed_folder(existing_proposed),
            )
            return
        self.destination_tree_status.setText("Type the new folder name and press Enter.")
        self._begin_inline_proposed_folder_rename(item)

    def handle_delete_proposed_folder(self, item):
        node_data = self.get_tree_item_node_data(item)
        if not self.node_is_proposed(node_data):
            return

        existing_proposed = self._find_proposed_folder_record_by_path(
            node_data.get("display_path") or node_data.get("item_path") or ""
        )
        if existing_proposed is not None and self._is_proposed_folder_submitted(existing_proposed):
            self._show_submitted_item_locked_message(
                "Delete Proposed Folder",
                f"'{node_data.get('name', 'This proposed folder')}'",
                self._submitted_batch_id_for_proposed_folder(existing_proposed),
            )
            return

        target_path = self._canonical_destination_projection_path(
            node_data.get("display_path") or node_data.get("item_path") or ""
        ) or self.normalize_memory_path(node_data.get("display_path") or node_data.get("item_path") or "")
        self.proposed_folders = [
            row for row in self.proposed_folders
            if not (
                self._proposed_destination_path(row) == target_path
                or self._proposed_destination_path(row).startswith(target_path + "\\")
            )
        ]
        self._remove_visible_destination_subtree_by_prefix(item, target_path)

        self.destination_tree_status.setText("Proposed folder deleted.")
        self.clear_selection_details()
        self._persist_planning_change_lightweight()

    def handle_rename_planned_item(self, node_data):
        move_index = self.find_planned_move_index_by_destination(node_data)
        move = self.planned_moves[move_index] if move_index is not None else None

        node_origin = str(node_data.get("node_origin", "")).lower()
        if move is None and node_origin == "projectedallocationdescendant":
            source_path = self._canonical_source_projection_path(node_data.get("source_path", ""))
            move = self._find_exact_planned_move_for_source_path(source_path)
            if move is not None:
                move_index = self.planned_moves.index(move)

        inherited_move = None
        if move is None and node_origin == "projectedallocationdescendant":
            source_path = self._canonical_source_projection_path(node_data.get("source_path", ""))
            inherited_move = self._find_inherited_planned_move_for_source_path(source_path)
            if inherited_move is None:
                QMessageBox.information(
                    self,
                    "Rename Planned Item",
                    "No planned allocation exists for the selected destination item.",
                )
                return
        elif move is None:
            QMessageBox.information(
                self,
                "Rename Planned Item",
                "No planned allocation exists for the selected destination item.",
            )
            return

        if move is not None and self._is_move_submitted(move):
            self._show_submitted_item_locked_message(
                "Rename Planned Item",
                f"'{move.get('source_name', 'This item')}'",
                self._submitted_batch_id_for_move(move),
            )
            return

        current_name = str(self._move_target_name(move) if move is not None else (node_data.get("name") or "")).strip()
        new_name, accepted = QInputDialog.getText(
            self,
            "Rename Planned Item",
            "Planned item name:",
            text=current_name,
        )
        new_name = new_name.strip()
        if not accepted or not new_name or new_name == current_name:
            return

        if move is None and inherited_move is not None:
            source_item = self._find_visible_source_item_by_path(node_data.get("source_path", ""))
            source_node = source_item.data(0, Qt.UserRole) or {} if source_item is not None else {}
            if not source_node:
                source_node = {
                    "id": "",
                    "name": node_data.get("name", ""),
                    "real_name": node_data.get("name", ""),
                    "display_path": node_data.get("source_path", ""),
                    "item_path": node_data.get("source_path", ""),
                    "tree_role": "source",
                    "drive_id": "",
                    "is_folder": bool(node_data.get("is_folder", False)),
                }
            destination_parent_path = self._destination_parent_path(
                node_data.get("display_path") or node_data.get("item_path") or ""
            )
            destination_node = {
                "id": "",
                "name": self._path_segments(destination_parent_path)[-1] if self._path_segments(destination_parent_path) else "",
                "real_name": self._path_segments(destination_parent_path)[-1] if self._path_segments(destination_parent_path) else "",
                "display_path": destination_parent_path,
                "item_path": destination_parent_path,
                "destination_path": destination_parent_path,
                "tree_role": "destination",
                "drive_id": node_data.get("drive_id", ""),
                "site_id": node_data.get("site_id", ""),
                "site_name": node_data.get("site_name", ""),
                "library_id": node_data.get("library_id", ""),
                "library_name": node_data.get("library_name", ""),
                "is_folder": True,
            }
            override_move = self.build_planned_move_record(source_node, destination_node)
            override_move["allocation_method"] = "Manual - Override"
            override_move["target_name"] = new_name
            self.planned_moves.append(override_move)
        else:
            move["target_name"] = new_name
        self.planned_moves_status.setText("Planned item renamed.")
        self._persist_planning_change("planned_item_renamed")

    def handle_remove_planned_allocation(self, node_data):
        move_index = self.find_planned_move_index_by_destination(node_data)
        if move_index is None:
            QMessageBox.information(
                self,
                "Remove Planned Allocation",
                "No planned allocation exists for the selected destination item.",
            )
            return

        move = self.planned_moves[move_index]
        if self._is_move_submitted(move):
            self._show_submitted_item_locked_message(
                "Remove Planned Allocation",
                f"'{move.get('source_name', 'This item')}'",
                self._submitted_batch_id_for_move(move),
            )
            return

        removed_visually = self._quick_remove_planned_move_from_destination_tree(move) > 0
        self.planned_moves.pop(move_index)
        self._reset_unresolved_allocation_queue()
        self.refresh_planned_moves_table()
        self.planned_moves_status.setText("Planned allocation removed.")
        if not removed_visually:
            self._schedule_deferred_destination_materialization("remove_allocation_tree_reconcile", delay_ms=220)
        self._persist_planning_change(
            "planned_allocation_removed",
            source_projection_paths=self._narrow_source_projection_paths_for_move(move),
        )

    def build_planned_move_record(self, source_node, destination_node):
        return {
            "source_id": source_node.get("id", ""),
            "source_name": source_node.get("name", "Unnamed Item"),
            "target_name": source_node.get("name", "Unnamed Item"),
            "source_path": source_node.get("display_path") or source_node.get("item_path") or "/",
            "source": dict(source_node),
            "destination_id": destination_node.get("id", ""),
            "destination_name": destination_node.get("name", "Unnamed Item"),
            "destination_path": destination_node.get("display_path") or destination_node.get("item_path") or "/",
            "destination": dict(destination_node),
            "status": "Draft",
        }

    def _on_simulate_run_save_manifest(self):
        if not self.planned_moves and not self.proposed_folders:
            QMessageBox.information(
                self,
                "Simulate run",
                "There are no planned moves or proposed folders to include in a manifest.",
            )
            return

        draft_key = str(self.active_draft_session_id or "").strip()
        if not draft_key and isinstance(self._draft_shell_state, SessionState):
            draft_key = str(self._draft_shell_state.DraftId or "").strip()
        if not draft_key:
            draft_key = "draft"
        safe_draft = re.sub(r"[^\w\-.]+", "_", draft_key)[:80]
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        default_name = f"Ozlink_transfer_manifest_{safe_draft}_{stamp}.json"
        path, _selected = QFileDialog.getSaveFileName(
            self,
            "Save transfer manifest (simulation)",
            str(Path.home() / default_name),
            "JSON manifest (*.json);;All files (*.*)",
        )
        if not path:
            return
        if not str(path).lower().endswith(".json"):
            path = str(path) + ".json"

        ctx = getattr(self, "current_session_context", None) or {}
        tenant_hint = str(ctx.get("tenant_domain") or "")

        manifest = build_simulation_manifest(
            planned_moves=list(self.planned_moves or []),
            proposed_folders=list(self.proposed_folders or []),
            draft_id=str(self.active_draft_session_id or draft_key or ""),
            tenant_hint=tenant_hint,
            notes="Simulation export from Ozlink Console. Local absolute paths can be run from the Execution page or Planned Moves (Run manifest).",
        )
        try:
            write_manifest_json(path, manifest)
        except OSError as exc:
            log_error("simulate_manifest_write_failed", path=str(path), error=str(exc))
            QMessageBox.warning(self, "Simulate run", f"Could not write the file:\n{exc}")
            return

        log_info(
            "simulate_manifest_written",
            path=str(path),
            transfer_steps=len(manifest.get("transfer_steps", [])),
            proposed_folder_steps=len(manifest.get("proposed_folder_steps", [])),
        )
        QMessageBox.information(
            self,
            "Simulate run",
            f"Manifest saved ({len(manifest.get('transfer_steps', []))} file step(s), "
            f"{len(manifest.get('proposed_folder_steps', []))} proposed folder step(s)).\n\n{path}",
        )

    def _on_manifest_run_worker_finished_cleanup(self):
        self._manifest_run_worker = None
        if getattr(self, "execution_status_label", None) is not None:
            self.execution_status_label.setText("")
        self._update_manifest_run_buttons_state()

    def _set_manifest_run_buttons_enabled(self, enabled: bool):
        for name in (
            "run_manifest_button",
            "simulate_run_manifest_button",
            "execution_open_manifest_button",
            "execution_save_manifest_button",
            "execution_dry_run_button",
            "execution_run_button",
        ):
            w = getattr(self, name, None)
            if w is not None:
                w.setEnabled(enabled)

    def _update_manifest_run_buttons_state(self):
        if self._manifest_run_worker is not None:
            self._set_manifest_run_buttons_enabled(False)
            return
        if getattr(self, "run_manifest_button", None) is not None:
            self.run_manifest_button.setEnabled(True)
        if getattr(self, "simulate_run_manifest_button", None) is not None:
            self.simulate_run_manifest_button.setEnabled(True)
        if getattr(self, "execution_open_manifest_button", None) is not None:
            self.execution_open_manifest_button.setEnabled(True)
        if getattr(self, "execution_save_manifest_button", None) is not None:
            self.execution_save_manifest_button.setEnabled(True)
        has_manifest = self._execution_manifest is not None
        if getattr(self, "execution_dry_run_button", None) is not None:
            self.execution_dry_run_button.setEnabled(has_manifest)
        if getattr(self, "execution_run_button", None) is not None:
            self.execution_run_button.setEnabled(has_manifest)

    def _refresh_execution_summary_panel(self):
        if getattr(self, "execution_summary_text", None) is None:
            return
        if not self._execution_manifest:
            self.execution_summary_text.clear()
            return
        s = manifest_execution_summary(self._execution_manifest)
        text = (
            f"Transfer steps (total): {s['transfer_steps_total']}\n"
            f"  • Eligible for local copy on this PC: {s['local_filesystem_transfer']}\n"
            f"  • Require Microsoft Graph (not implemented yet): {s['graph_transfer_pending']}\n"
            f"  • Skipped (non-local paths or unsupported): {s['transfer_skipped_non_local']}\n\n"
            f"Proposed folder steps (total): {s['proposed_folder_steps_total']}\n"
            f"  • Local mkdir: {s['local_mkdir']}\n"
            f"  • Skipped (non-local paths): {s['proposed_skipped_non_local']}\n"
        )
        self.execution_summary_text.setPlainText(text)

    def _on_execution_open_manifest(self):
        path, _selected = QFileDialog.getOpenFileName(
            self,
            "Open transfer manifest",
            str(Path.home()),
            "JSON (*.json);;All files (*.*)",
        )
        if not path:
            return
        try:
            manifest = load_manifest_json(path)
        except OSError as exc:
            QMessageBox.warning(self, "Execution", f"Could not read file:\n{exc}")
            return
        except json.JSONDecodeError as exc:
            QMessageBox.warning(self, "Execution", f"Invalid JSON:\n{exc}")
            return

        errs = validate_manifest(manifest)
        if errs:
            QMessageBox.warning(self, "Execution", "\n".join(errs))
            return

        self._execution_manifest_path = str(path)
        self._execution_manifest = manifest
        if getattr(self, "execution_manifest_path_label", None) is not None:
            self.execution_manifest_path_label.setText(f"Manifest: {path}")
            self.execution_manifest_path_label.setStyleSheet("")
        self._refresh_execution_summary_panel()
        self._update_manifest_run_buttons_state()
        if getattr(self, "execution_status_label", None) is not None:
            self.execution_status_label.setText("")

    def _on_execution_dry_run_manifest(self):
        if not self._execution_manifest or not self._execution_manifest_path:
            QMessageBox.information(self, "Execution", "Open a manifest first.")
            return
        if (
            QMessageBox.question(
                self,
                "Dry run",
                "Run a dry run on the loaded manifest? Nothing will be copied or created on disk.",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            != QMessageBox.Yes
        ):
            return
        self._start_manifest_run_worker(self._execution_manifest, self._execution_manifest_path, dry_run=True)

    def _on_execution_run_manifest(self):
        if not self._execution_manifest or not self._execution_manifest_path:
            QMessageBox.information(self, "Execution", "Open a manifest first.")
            return
        confirm = QMessageBox.warning(
            self,
            "Confirm execution",
            "This will copy files and create folders on this PC for every eligible path in the manifest.\n\n"
            "Verify paths before continuing.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if confirm != QMessageBox.Yes:
            return
        self._start_manifest_run_worker(self._execution_manifest, self._execution_manifest_path, dry_run=False)

    def _start_manifest_run_worker(self, manifest: dict, path: str, *, dry_run: bool):
        import tempfile

        log_file = Path(tempfile.gettempdir()) / (
            f"Ozlink_transfer_job_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
        )

        worker = ManifestRunWorker(manifest, dry_run=dry_run, log_file=str(log_file))
        self._manifest_run_worker = worker
        worker.finished_ok.connect(self._on_manifest_run_finished)
        worker.failed.connect(self._on_manifest_run_failed)
        worker.finished.connect(self._on_manifest_run_worker_finished_cleanup)
        self._set_manifest_run_buttons_enabled(False)
        if getattr(self, "execution_status_label", None) is not None:
            self.execution_status_label.setText("Running manifest job in the background…")
        worker.start()
        log_info(
            "transfer_manifest_run_started",
            manifest_path=str(path),
            dry_run=dry_run,
            log_file=str(log_file),
        )

    def _run_transfer_manifest_from_path_with_dialogs(self, path: str):
        try:
            manifest = load_manifest_json(path)
        except OSError as exc:
            QMessageBox.warning(self, "Run manifest", f"Could not read file:\n{exc}")
            return
        except json.JSONDecodeError as exc:
            QMessageBox.warning(self, "Run manifest", f"Invalid JSON:\n{exc}")
            return

        errs = validate_manifest(manifest)
        if errs:
            QMessageBox.warning(self, "Run manifest", "\n".join(errs))
            return

        summary = manifest_execution_summary(manifest)
        msg = (
            f"Manifest:\n{path}\n\n"
            f"Transfer steps: {summary['transfer_steps_total']}\n"
            f"  • Eligible for local copy: {summary['local_filesystem_transfer']}\n"
            f"  • Needs Microsoft Graph (not in this build): {summary['graph_transfer_pending']}\n"
            f"  • Skipped (non-local paths): {summary['transfer_skipped_non_local']}\n\n"
            f"Proposed folders: {summary['proposed_folder_steps_total']}\n"
            f"  • Local mkdir: {summary['local_mkdir']}\n"
            f"  • Skipped (non-local): {summary['proposed_skipped_non_local']}\n\n"
            "Only absolute Windows paths (C:\\ or UNC) run on this PC.\n"
            "Typical SharePoint library paths are skipped until a Graph executor exists.\n\n"
            "Continue to run options?"
        )
        if (
            QMessageBox.question(
                self,
                "Run manifest",
                msg,
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            != QMessageBox.Yes
        ):
            return

        dlg = QDialog(self)
        dlg.setWindowTitle("Run manifest — options")
        v = QVBoxLayout(dlg)
        dry_cb = QCheckBox("Dry run only (log steps; no copies or mkdir)")
        dry_cb.setChecked(True)
        v.addWidget(dry_cb)
        bb = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        v.addWidget(bb)
        bb.accepted.connect(dlg.accept)
        bb.rejected.connect(dlg.reject)
        if dlg.exec() != QDialog.Accepted:
            return
        dry = dry_cb.isChecked()

        if not dry:
            confirm = QMessageBox.warning(
                self,
                "Confirm execution",
                "This will copy files and create folders on this PC for every eligible path in the manifest.\n\n"
                "Verify paths before continuing.",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if confirm != QMessageBox.Yes:
                return

        self._start_manifest_run_worker(manifest, path, dry_run=dry)

    def _on_manifest_run_finished(self, result):
        msg = f"{result.summary_line()}\n\nDetail log:\n{result.log_path or '(none)'}"
        QMessageBox.information(self, "Run manifest", msg)

    def _on_manifest_run_failed(self, err):
        QMessageBox.warning(self, "Run manifest", err)

    def _on_run_transfer_manifest(self):
        path, _selected = QFileDialog.getOpenFileName(
            self,
            "Open transfer manifest",
            str(Path.home()),
            "JSON (*.json);;All files (*.*)",
        )
        if not path:
            return
        self._run_transfer_manifest_from_path_with_dialogs(path)

    def refresh_planned_moves_table(self):
        self._rebuild_submission_visual_cache()
        loading_message = self._planning_workspace_loading_message()
        loading_banner = getattr(self, "planned_moves_loading_banner", None)
        if loading_banner is not None:
            if loading_message:
                self.planned_moves_loading_title.setText("Please wait while we load your SharePoint sites.")
                self.planned_moves_loading_detail.setText(loading_message)
                loading_banner.show()
            else:
                loading_banner.hide()
        if not self.planned_moves and loading_message:
            self.planned_moves_table.clearContents()
            self.planned_moves_table.setRowCount(1)
            placeholder = QTableWidgetItem(loading_message)
            placeholder.setFlags(Qt.ItemIsEnabled)
            placeholder.setForeground(QBrush(QColor("#FFC14D")))
            self.planned_moves_table.setItem(0, 0, placeholder)
            self.planned_moves_table.setSpan(0, 0, 1, self.planned_moves_table.columnCount())
            self.planned_moves_status.setText("Planning workspace is still loading...")
            self.planned_moves_table.clearSelection()
            return

        self.planned_moves_table.clearSpans()
        self.planned_moves_table.setRowCount(len(self.planned_moves))

        for row_index, move in enumerate(self.planned_moves):
            is_submitted = self._is_move_submitted(move)
            status_text = "Submitted" if is_submitted else "Draft"
            values = [
                move.get("source_name", ""),
                move.get("source_path", ""),
                move.get("destination_name", ""),
                move.get("destination_path", ""),
                status_text,
            ]

            for column_index, value in enumerate(values):
                item = QTableWidgetItem(value)
                item.setData(Qt.UserRole, row_index)
                if is_submitted:
                    item.setBackground(QBrush(QColor("#1B2942")))
                    if column_index == 4:
                        item.setForeground(QBrush(QColor("#FFC14D")))
                    else:
                        item.setForeground(QBrush(QColor("#D6E8FF")))
                    batch_id = self._submitted_batch_id_for_move(move)
                    item.setToolTip(
                        f"Submitted and locked in batch {batch_id}."
                        if batch_id
                        else "Submitted and locked."
                    )
                self.planned_moves_table.setItem(row_index, column_index, item)

        if self.planned_moves:
            submitted_count = sum(1 for move in self.planned_moves if self._is_move_submitted(move))
            draft_count = len(self.planned_moves) - submitted_count
            self.planned_moves_status.setText(
                f"{len(self.planned_moves)} planned move(s) ready. {submitted_count} submitted, {draft_count} draft."
            )
        else:
            self.planned_moves_status.setText("No planned moves yet.")

        self.planned_moves_table.clearSelection()
        if hasattr(self, "source_tree_widget"):
            self.source_tree_widget.viewport().update()
        self._refresh_planning_loading_banner()
        self._schedule_progress_summary_refresh()

    def _queue_expand_all_item(self, panel_key, item):
        if item is None:
            return
        node_data = item.data(0, Qt.UserRole) or {}
        if node_data.get("placeholder") or not node_data.get("is_folder"):
            return
        item_key = id(item)
        if item_key in self._expand_all_seen[panel_key]:
            return
        self._expand_all_seen[panel_key].add(item_key)
        self._expand_all_queue[panel_key].append(item)

    def _reset_expand_all_progress(self, panel_key):
        if not hasattr(self, "_expand_all_processed") or not isinstance(self._expand_all_processed, dict):
            self._expand_all_processed = {"source": 0, "destination": 0}
        if not hasattr(self, "_expand_all_processed_seen") or not isinstance(self._expand_all_processed_seen, dict):
            self._expand_all_processed_seen = {"source": set(), "destination": set()}
        self._expand_all_queue[panel_key] = deque()
        self._expand_all_seen[panel_key] = set()
        self._expand_all_processed_seen[panel_key] = set()
        requeue_attempts = getattr(self, "_expand_all_requeue_attempts", None)
        if isinstance(requeue_attempts, dict):
            requeue_attempts[panel_key] = {}
        self._expand_all_processed[panel_key] = 0
        if panel_key == "source":
            self._source_expand_all_folder_load_log_counter = 0
            model_queue = getattr(self, "_expand_all_source_model_queue", None)
            if model_queue is not None:
                model_queue.clear()

    def _expand_all_progress_text(self, panel_key, prefix):
        processed_map = getattr(self, "_expand_all_processed", {}) or {}
        processed = int(processed_map.get(panel_key, 0) or 0)
        discovered = len(self._expand_all_seen.get(panel_key, set()))
        if discovered <= 0:
            return prefix
        return f"{prefix} ({processed}/{discovered} folders)"

    def _update_expand_all_status(self, panel_key, prefix, loading=True):
        status = self.source_tree_status if panel_key == "source" else self.destination_tree_status
        if status is not None:
            now_ms = int(time.perf_counter() * 1000)
            last_map = getattr(self, "_expand_all_status_last_update_ms", {}) or {}
            min_gap_ms = 180 if panel_key == "source" else 120
            last_ms = int(last_map.get(panel_key, 0) or 0)
            if loading and (now_ms - last_ms) < min_gap_ms:
                return
            self._expand_all_status_last_update_ms[panel_key] = now_ms
            msg = self._expand_all_progress_text(panel_key, prefix)
            self._set_tree_status_message(
                panel_key,
                msg,
                loading=loading,
            )
            if status is not None:
                status.setToolTip(
                    "Expand-all progress: folders queued or processed for expansion (not files). "
                    "Denominator grows as more folder branches are discovered. "
                    "This is not the Graph library item total or the file count."
                )

    def _schedule_expand_all(self, panel_key, delay_ms=10):
        timer = getattr(self, "_expand_all_timers", {}).get(panel_key)
        if timer is not None and not timer.isActive():
            timer.start(delay_ms)

    def _requeue_expand_all_item(self, panel_key, item, reason):
        if item is None:
            return False
        node_data = item.data(0, Qt.UserRole) or {}
        if node_data.get("placeholder") or not node_data.get("is_folder"):
            return False

        attempts_by_panel = getattr(self, "_expand_all_requeue_attempts", {}).get(panel_key, {})
        if not isinstance(attempts_by_panel, dict):
            attempts_by_panel = {}
            self._expand_all_requeue_attempts[panel_key] = attempts_by_panel
        item_key = id(item)
        attempts = int(attempts_by_panel.get(item_key, 0) or 0) + 1
        attempts_by_panel[item_key] = attempts

        limit = int((getattr(self, "_expand_all_requeue_limit", {}) or {}).get(panel_key, 8) or 8)
        if attempts > limit:
            if panel_key == "destination":
                is_planned_allocation = bool(self.node_is_planned_allocation(node_data))
                if is_planned_allocation:
                    # Terminalize only planned-allocation projection retries. Keep manual
                    # expand affordance via projection_unresolved_terminal, but do not mark as load_failed.
                    try:
                        self._remove_placeholder_children(item)
                        node_data["children_loaded"] = True
                        node_data["load_failed"] = False
                        node_data["projection_unresolved_terminal"] = True
                        item.setData(0, Qt.UserRole, node_data)
                        self._refresh_destination_item_visibility(item)
                        if getattr(self, "destination_tree_widget", None) is not None:
                            self.destination_tree_widget.viewport().update()
                    except Exception:
                        pass
                else:
                    # Avoid endless destination expand-all loops on non-allocation folders
                    # that repeatedly fail to start a real load (e.g. missing load keys).
                    try:
                        node_data["children_loaded"] = True
                        node_data["load_failed"] = True
                        item.setData(0, Qt.UserRole, node_data)
                        self._refresh_destination_item_visibility(item)
                    except Exception:
                        pass
                self._log_restore_phase(
                    "destination_expand_all_allocation_retry_limit_reached",
                    destination_path=self._tree_item_path(node_data),
                    reason=reason,
                    retry_attempt=attempts,
                    retry_limit=limit,
                )
            return False

        # Force requeue even if already seen; retries are bounded by retry_limit.
        self._expand_all_queue[panel_key].append(item)
        if panel_key == "destination":
            self._log_restore_phase(
                "destination_expand_all_allocation_requeued",
                destination_path=self._tree_item_path(node_data),
                reason=reason,
                retry_attempt=attempts,
                retry_limit=limit,
                pending_source_folder_loads=len(self.pending_folder_loads.get("source", set())),
                source_restore_queue_size=len(getattr(self, "_source_restore_materialization_queue", [])),
            )
        return True

    def _cancel_expand_all(self, panel_key):
        self._expand_all_pending[panel_key] = False
        self._reset_expand_all_progress(panel_key)
        self._expand_all_deferred_refresh[panel_key] = False
        timer = getattr(self, "_expand_all_timers", {}).get(panel_key)
        if timer is not None:
            timer.stop()

    def _item_has_unresolved_lazy_placeholder_child(self, item):
        """True when a folder still shows a placeholder that means work remains.

        Terminal rows like "This folder is empty." must not count as unloaded, or expand-all
        can never finish and the seen-set re-queue path below never runs correctly.
        """
        if item is None:
            return False
        blocking_roles = frozenset({"lazy_unloaded", "loading_in_progress", "projection_pending"})
        for index in range(item.childCount()):
            child = item.child(index)
            child_data = child.data(0, Qt.UserRole) or {}
            if not child_data.get("placeholder"):
                continue
            role = child_data.get("placeholder_role")
            if role in blocking_roles:
                return True
            if role is None:
                t = (child.text(0) or "").strip().lower()
                if t.startswith("expand to load") or t.startswith("loading folder"):
                    return True
                if "retry projected" in t or "projected descendants" in t:
                    return True
        return False

    def _tree_folder_item_has_non_placeholder_children(self, item):
        """True when this row already has real child items in the widget (e.g. snapshot restore).

        Persisted snapshots may omit or clear ``children_loaded`` while still attaching child
        rows; without this, expand-all always takes the slow queued path.
        """
        if item is None:
            return False
        for index in range(item.childCount()):
            child_data = item.child(index).data(0, Qt.UserRole) or {}
            if not child_data.get("placeholder"):
                return True
        return False

    def _source_model_tree_has_unloaded_folder_nodes(self) -> bool:
        model = getattr(self, "source_sharepoint_model", None)
        if model is None:
            return False
        for ix in model.iter_depth_first():
            pl = ix.data(Qt.UserRole) or {}
            if pl.get("placeholder") or not pl.get("is_folder"):
                continue
            if pl.get("load_failed"):
                continue
            if pl.get("children_loaded"):
                continue
            drive_id = self._resolve_tree_item_drive_id("source", pl)
            if not drive_id or not pl.get("id"):
                continue
            return True
        return False

    def _tree_has_unloaded_folder_nodes(self, panel_key):
        if panel_key == "source" and self._source_tree_uses_model_view():
            return self._source_model_tree_has_unloaded_folder_nodes()
        tree = self.source_tree_widget if panel_key == "source" else self.destination_tree_widget
        if tree is None:
            return False

        queue = [tree.topLevelItem(index) for index in range(tree.topLevelItemCount())]
        while queue:
            item = queue.pop(0)
            if item is None:
                continue
            node_data = item.data(0, Qt.UserRole) or {}
            if node_data.get("placeholder"):
                continue
            if (
                bool(node_data.get("is_folder"))
                and not bool(node_data.get("load_failed"))
                and not (
                    panel_key == "destination"
                    and self.node_is_planned_allocation(node_data)
                    and bool(node_data.get("projection_unresolved_terminal"))
                )
                and (
                    (
                        not bool(node_data.get("children_loaded"))
                        and not self._tree_folder_item_has_non_placeholder_children(item)
                    )
                    or self._item_has_unresolved_lazy_placeholder_child(item)
                )
            ):
                drive_ok = (
                    str(self._resolve_tree_item_drive_id(panel_key, node_data) or "").strip()
                    and str(node_data.get("id", "") or "").strip()
                )
                if not drive_ok:
                    for index in range(item.childCount()):
                        queue.append(item.child(index))
                    continue
                return True
            for index in range(item.childCount()):
                queue.append(item.child(index))
        return False

    def _can_fast_bulk_expand(self, panel_key):
        """Use a single expandAll() when every visible folder is already materialized (e.g. after Collapse All).

        Queued expand-all is for lazy trees with placeholders; replaying it on a fully-loaded tree
        re-walks the same branches and feels hung. Cap avoids pathological single-thread stalls.
        """
        if self.pending_folder_loads.get(panel_key):
            return False
        if self._tree_has_unloaded_folder_nodes(panel_key):
            return False
        folder_count = self._count_expandable_tree_nodes(panel_key)
        if folder_count > 12000 and self._tree_has_unloaded_folder_nodes(panel_key):
            return False
        if panel_key == "destination":
            if self._destination_full_tree_worker is not None and self._destination_full_tree_worker.isRunning():
                return False
        else:
            if getattr(self, "_source_background_preload_pending", False):
                return False
        return True

    def _count_expandable_tree_nodes(self, panel_key):
        if panel_key == "source" and self._source_tree_uses_model_view():
            model = getattr(self, "source_sharepoint_model", None)
            if model is None:
                return 0
            count = 0
            for ix in model.iter_depth_first():
                pl = ix.data(Qt.UserRole) or {}
                if pl.get("placeholder"):
                    continue
                if pl.get("is_folder"):
                    count += 1
            return count
        tree = self.source_tree_widget if panel_key == "source" else self.destination_tree_widget
        if tree is None:
            return 0
        count = 0
        queue = [tree.topLevelItem(index) for index in range(tree.topLevelItemCount())]
        while queue:
            item = queue.pop(0)
            if item is None:
                continue
            node_data = item.data(0, Qt.UserRole) or {}
            if node_data.get("placeholder"):
                continue
            if bool(node_data.get("is_folder")):
                count += 1
            for index in range(item.childCount()):
                queue.append(item.child(index))
        return count

    def _count_visible_subtree_nodes(self, item):
        if item is None:
            return 0
        count = 0
        queue = [item]
        while queue:
            current = queue.pop(0)
            if current is None:
                continue
            node_data = current.data(0, Qt.UserRole) or {}
            if node_data.get("placeholder"):
                continue
            count += 1
            for index in range(current.childCount()):
                queue.append(current.child(index))
        return count

    def _count_folder_payload_nodes(self, items):
        if not isinstance(items, list):
            return 0

        count = 0

        def _walk(node):
            nonlocal count
            if not isinstance(node, dict):
                return
            count += 1
            for child in list(node.get("children", []) or []):
                _walk(child)

        for item in items:
            _walk(item)
        return count

    def _fast_expand_all_loaded_tree(self, panel_key):
        tree = self.source_tree_widget if panel_key == "source" else self.destination_tree_widget
        if tree is None:
            return False

        tree.setUpdatesEnabled(False)
        tree.blockSignals(True)
        try:
            tree.expandAll()
        finally:
            tree.blockSignals(False)
            tree.setUpdatesEnabled(True)
            tree.viewport().update()

        log_info(
            "expand_all_fast_path",
            panel_key=panel_key,
            folder_count=self._count_expandable_tree_nodes(panel_key),
        )
        self._expand_all_pending[panel_key] = False
        self._reset_expand_all_progress(panel_key)
        self._expand_all_deferred_refresh[panel_key] = False
        intent_map = getattr(self, "_workspace_restore_expanded_all_intent", None)
        if isinstance(intent_map, dict):
            intent_map[panel_key] = False
        self._set_expand_all_button_label(panel_key, True)
        status = self.source_tree_status if panel_key == "source" else self.destination_tree_status
        if status is not None:
            self._set_tree_status_message(panel_key, "All branches expanded.", loading=False)
        if panel_key == "destination":
            # Defer indicator pass so expandAll() returns immediately; avoid full future-model bind here —
            # that rematerialization froze the UI for tens of seconds on large libraries. Users can
            # Refresh Cache if overlays need a full rebuild.
            QTimer.singleShot(0, self._refresh_destination_tree_indicators)
        self._persist_workspace_ui_state_safely()
        return True

    def _process_expand_all_queue(self, panel_key):
        if not self._expand_all_pending.get(panel_key):
            return

        tree = self.source_tree_widget if panel_key == "source" else self.destination_tree_widget
        if tree is None:
            self._expand_all_pending[panel_key] = False
            return

        processed = 0
        max_per_tick = max(1, int((self._expand_all_max_per_tick or {}).get(panel_key, 1)))
        tick_budget_ms = 9 if panel_key == "source" else 11
        tick_start = time.perf_counter()
        waiting_for_async_load = False

        while (
            self._expand_all_queue[panel_key]
            and processed < max_per_tick
            and ((time.perf_counter() - tick_start) * 1000.0) < tick_budget_ms
        ):
            item = self._expand_all_queue[panel_key].popleft()
            if item is None:
                continue
            node_data = item.data(0, Qt.UserRole) or {}
            if node_data.get("placeholder") or not node_data.get("is_folder"):
                continue

            max_inflight_loads = 1 if panel_key == "source" else 3
            if len(self.pending_folder_loads.get(panel_key, set())) >= max_inflight_loads:
                waiting_for_async_load = True
                self._expand_all_queue[panel_key].appendleft(item)
                break

            already_loaded = bool(node_data.get("children_loaded"))
            tree.expandItem(item)
            processed += 1
            processed_seen_map = getattr(self, "_expand_all_processed_seen", {}) or {}
            panel_processed_seen = processed_seen_map.get(panel_key, set())
            if not isinstance(panel_processed_seen, set):
                panel_processed_seen = set()
                processed_seen_map[panel_key] = panel_processed_seen
                self._expand_all_processed_seen = processed_seen_map
            item_key = id(item)
            if item_key not in panel_processed_seen:
                panel_processed_seen.add(item_key)
                self._expand_all_processed[panel_key] += 1

            if panel_key == "destination" and self._item_has_unresolved_lazy_placeholder_child(item):
                # Keep indicator + styling deterministic when only placeholders exist.
                self._refresh_destination_item_visibility(item)

            if already_loaded and self._item_has_unresolved_lazy_placeholder_child(item):
                # Some branches remain represented only by "Expand to load contents"
                # placeholders. Force a real load pass so expand-all does not
                # report completion prematurely.
                restart_data = item.data(0, Qt.UserRole) or {}
                if restart_data.get("id") and not bool(restart_data.get("load_failed")):
                    restart_data["children_loaded"] = False
                    item.setData(0, Qt.UserRole, restart_data)
                    load_started = self._ensure_tree_item_load_started(panel_key, item)
                    if load_started:
                        waiting_for_async_load = True
                        break
                latest_data = item.data(0, Qt.UserRole) or {}
                if not bool(latest_data.get("children_loaded")):
                    waiting_for_async_load = True
                    break

            if already_loaded:
                for index in range(item.childCount()):
                    self._queue_expand_all_item(panel_key, item.child(index))
                continue

            latest_data = item.data(0, Qt.UserRole) or {}
            if (
                panel_key == "destination"
                and self.node_is_planned_allocation(latest_data)
                and not bool(latest_data.get("children_loaded"))
                and not bool(latest_data.get("load_failed"))
            ):
                # Planned allocations must be expanded via projected descendant materialization,
                # not via folder load worker gating.
                ip = self._tree_item_path(latest_data)
                if ip:
                    QTimer.singleShot(0, lambda p=ip: self._deferred_load_destination_projected_descendants(p))
                else:
                    self._load_destination_projected_descendants(item)
                latest_data = item.data(0, Qt.UserRole) or {}
                if bool(latest_data.get("children_loaded")):
                    for index in range(item.childCount()):
                        self._queue_expand_all_item(panel_key, item.child(index))
                    continue
                if self.node_is_planned_allocation(latest_data) and not bool(latest_data.get("children_loaded")):
                    if self._requeue_expand_all_item(
                        panel_key,
                        item,
                        reason="planned_allocation_children_unresolved_after_projected_materialize",
                    ):
                        waiting_for_async_load = True
                        break

            load_started = self._ensure_tree_item_load_started(panel_key, item)
            if load_started:
                waiting_for_async_load = True
                break
            latest_data = item.data(0, Qt.UserRole) or {}
            if bool(latest_data.get("children_loaded")):
                for index in range(item.childCount()):
                    self._queue_expand_all_item(panel_key, item.child(index))
                continue
            if (
                panel_key == "destination"
                and bool(latest_data.get("is_folder"))
                and not self.node_is_planned_allocation(latest_data)
                and not bool(latest_data.get("children_loaded"))
                and not bool(latest_data.get("load_failed"))
            ):
                # Avoid silently dropping unresolved destination branches when
                # a load could not be started in this tick.
                if self._requeue_expand_all_item(
                    panel_key,
                    item,
                    reason="destination_folder_children_unresolved_load_not_started",
                ):
                    waiting_for_async_load = True
                    break
            if (
                panel_key == "destination"
                and self.node_is_planned_allocation(latest_data)
                and not bool(latest_data.get("children_loaded"))
                and not bool(latest_data.get("load_failed"))
            ):
                self._refresh_destination_item_visibility(item)
                # Planned allocation descendants may still be resolving against source projection.
                # Requeue deterministically (bounded retries) so destination expand-all revisits this branch.
                if self._requeue_expand_all_item(
                    panel_key,
                    item,
                    reason="planned_allocation_children_unresolved",
                ):
                    waiting_for_async_load = True
                    break

        if waiting_for_async_load:
            self._update_expand_all_status(panel_key, "Expanding branches...", loading=True)
            self._schedule_expand_all(panel_key, delay_ms=24 if panel_key == "source" else 10)
            return

        if self.pending_folder_loads.get(panel_key):
            if self._expand_all_queue[panel_key]:
                self._update_expand_all_status(panel_key, "Expanding branches...", loading=True)
                self._schedule_expand_all(panel_key, delay_ms=20 if panel_key == "source" else 8)
            return

        if self._expand_all_queue[panel_key]:
            self._update_expand_all_status(panel_key, "Expanding branches...", loading=True)
            self._schedule_expand_all(panel_key, delay_ms=10 if panel_key == "source" else 8)
            return

        if panel_key == "destination" and self._tree_has_unloaded_folder_nodes("destination"):
            # Do not declare destination expand-all complete while unresolved
            # lazy nodes still exist in the visible tree.
            self._expand_all_seen["destination"].clear()
            self._expand_all_processed_seen["destination"].clear()
            self._expand_all_processed["destination"] = 0
            for index in range(tree.topLevelItemCount()):
                self._queue_expand_all_item(panel_key, tree.topLevelItem(index))
            self._update_expand_all_status(panel_key, "Expanding branches...", loading=True)
            self._schedule_expand_all(panel_key, delay_ms=10)
            return

        self._expand_all_pending[panel_key] = False
        pending_selected_path = str(self._pending_workspace_post_expand_selection.get(panel_key, "") or "")
        if pending_selected_path:
            self._restore_selected_tree_path(panel_key, pending_selected_path)
            self._pending_workspace_post_expand_selection[panel_key] = ""
        if self._expand_all_deferred_refresh.get(panel_key):
            self._expand_all_deferred_refresh[panel_key] = False
            if panel_key == "source":
                self._refresh_source_projection("source_projection_expand_all_complete")
                if self._source_column_refresh_pending:
                    self._source_column_refresh_pending = False
                    self._refresh_tree_column_width("source")
                if (
                    getattr(self, "destination_tree_widget", None) is not None
                    and self.planned_moves
                    and not getattr(self, "_sharepoint_lazy_mode", False)
                ):
                    self._schedule_deferred_destination_materialization("source_expand_all_complete", delay_ms=120)
            else:
                if getattr(self, "_sharepoint_lazy_mode", False):
                    self._replay_unresolved_proposed_overlay("destination_expand_all_complete")
                    self._replay_unresolved_allocation_overlay("destination_expand_all_complete")
                    self._reconcile_destination_semantic_duplicates("destination_expand_all_complete")
                # Lazy mode previously stopped here, so planned/allocated branches could diverge
                # from a full future-state bind (same symptom as fast expand-all).
                self._schedule_deferred_destination_materialization("destination_expand_all_complete", delay_ms=120)
                self._schedule_destination_restore_materialization_queue("expand_all_complete")
                if getattr(self, "destination_tree_widget", None) is not None:
                    self.destination_tree_widget.viewport().update()
        if panel_key == "source" and self._source_restore_materialization_queue:
            # Restart source restore materialization after expand-all finishes.
            self._schedule_source_restore_materialization_queue(
                "source_expand_all_complete",
                delay_ms=0,
            )
        if panel_key == "source":
            if self._pending_snapshot_branch_refresh.get("source"):
                self._schedule_snapshot_branch_refresh("source", delay_ms=0)
            if self._source_projection_refresh_paths:
                phase_name, trigger_path = self._source_projection_refresh_context
                self._schedule_source_projection_refresh_for_paths(
                    set(self._source_projection_refresh_paths),
                    phase_name,
                    trigger_path,
                    delay_ms=0,
                )
            elif self._source_projection_refresh_context:
                phase_name, trigger_path = self._source_projection_refresh_context
                self._schedule_source_projection_refresh(
                    phase_name,
                    trigger_path,
                    delay_ms=0,
                )
            self._try_flush_destination_future_model_after_source_restore("source_expand_all_complete")
        intent_map = getattr(self, "_workspace_restore_expanded_all_intent", None)
        if isinstance(intent_map, dict):
            intent_map[panel_key] = False
        self._sync_expand_all_button_from_tree(panel_key, fallback_expanded=True)
        self._update_expand_all_status(panel_key, "All loaded branches expanded.", loading=False)
        self._refresh_runtime_tree_snapshot(panel_key)
        self._workspace_ui_snapshot_dirty_panels.discard(panel_key)
        self._persist_workspace_ui_state_safely()

    def _continue_expand_all(self, panel_key, item=None):
        if not self._expand_all_pending.get(panel_key):
            return

        tree = self.source_tree_widget if panel_key == "source" else self.destination_tree_widget
        if tree is None:
            self._expand_all_pending[panel_key] = False
            return

        if item is not None:
            for index in range(item.childCount()):
                self._queue_expand_all_item(panel_key, item.child(index))
        elif not self._expand_all_queue[panel_key]:
            if panel_key == "source" and self._source_tree_uses_model_view():
                pass
            else:
                for index in range(tree.topLevelItemCount()):
                    self._queue_expand_all_item(panel_key, tree.topLevelItem(index))

        self._update_expand_all_status(panel_key, "Expanding branches...", loading=True)
        self._schedule_expand_all(panel_key, delay_ms=8 if panel_key == "source" else 10)

    def _collapse_loaded_branches_for_panel(self, panel_key, *, status_message="Loaded branches collapsed."):
        """Stop expand-all and restore-driven branch walks for this panel; collapse to top level immediately."""
        tree = self.source_tree_widget if panel_key == "source" else self.destination_tree_widget
        if tree is None:
            return
        self._cancel_expand_all(panel_key)
        if panel_key == "destination":
            self._destination_expand_all_after_full_tree = False
        intent = getattr(self, "_workspace_restore_expanded_all_intent", None)
        if isinstance(intent, dict):
            intent[panel_key] = False
        if panel_key == "source":
            self._source_restore_materialization_queue = []
            self._source_restore_materialization_seen = set()
            self._reset_source_background_preload_state()
        else:
            self._destination_restore_materialization_queue = []
            self._destination_restore_materialization_seen = set()
            self._destination_restore_materialization_user_paused = True
        pending_refresh = getattr(self, "_pending_snapshot_branch_refresh", None)
        if isinstance(pending_refresh, dict) and panel_key in pending_refresh:
            pending_refresh[panel_key] = set()
        tree.setUpdatesEnabled(False)
        tree.blockSignals(True)
        try:
            tree.collapseAll()
            if panel_key == "source" and self._source_tree_uses_model_view():
                model = getattr(self, "source_sharepoint_model", None)
                if model is not None:
                    for r in range(model.rowCount(QModelIndex())):
                        ix = model.index(r, 0, QModelIndex())
                        tree.expand(ix)
            else:
                for index in range(tree.topLevelItemCount()):
                    top_item = tree.topLevelItem(index)
                    if top_item is not None:
                        tree.expandItem(top_item)
        finally:
            tree.blockSignals(False)
            tree.setUpdatesEnabled(True)
            tree.viewport().update()
        self._set_expand_all_button_label(panel_key, False)
        last_ms = getattr(self, "_expand_all_status_last_update_ms", None)
        if isinstance(last_ms, dict):
            last_ms[panel_key] = 0
        self._set_tree_status_message(panel_key, status_message, loading=False)
        self._refresh_runtime_tree_snapshot(panel_key)
        self._workspace_ui_snapshot_dirty_panels.discard(panel_key)
        self._persist_workspace_ui_state_safely()

    def _source_model_expand_all_key(self, index):
        pl = index.data(Qt.UserRole) or {}
        if pl.get("placeholder") or not pl.get("is_folder"):
            return None
        drive_id = self._resolve_tree_item_drive_id("source", pl)
        item_id = pl.get("id", "")
        if not drive_id or not item_id:
            return None
        return f"{drive_id}:{item_id}"

    def _begin_source_model_expand_all(self):
        panel_key = "source"
        tree = self.source_tree_widget
        model = getattr(self, "source_sharepoint_model", None)
        if tree is None or model is None:
            return
        self._expand_all_pending[panel_key] = True
        self._reset_expand_all_progress(panel_key)
        self._expand_all_max_per_tick[panel_key] = 1
        self._set_expand_all_button_label(panel_key, True)
        self._update_expand_all_status(panel_key, "Expanding branches...", loading=True)
        root = QModelIndex()
        for r in range(model.rowCount(root)):
            ix = model.index(r, 0, root)
            pl = ix.data(Qt.UserRole) or {}
            if pl.get("placeholder") or not pl.get("is_folder"):
                continue
            key = self._source_model_expand_all_key(ix)
            if key:
                self._expand_all_seen[panel_key].add(key)
            self._expand_all_source_model_queue.append(QPersistentModelIndex(ix))
        self._source_model_expand_all_tick()

    def _source_model_expand_all_tick(self):
        if not self._expand_all_pending.get("source"):
            return
        tree = self.source_tree_widget
        model = getattr(self, "source_sharepoint_model", None)
        if tree is None or model is None:
            self._finish_source_model_expand_all()
            return

        processed = 0
        max_per_tick = max(1, int((self._expand_all_max_per_tick or {}).get("source", 1)))
        tick_budget_ms = 24
        tick_start = time.perf_counter()

        while (
            self._expand_all_source_model_queue
            and processed < max_per_tick
            and ((time.perf_counter() - tick_start) * 1000.0) < tick_budget_ms
        ):
            pending_count = len(self.pending_folder_loads.get("source", set()))
            max_inflight_loads = 3
            if pending_count >= max_inflight_loads:
                break

            pmi = self._expand_all_source_model_queue.popleft()
            if not pmi.isValid():
                continue
            index = QModelIndex(pmi)
            pl = index.data(Qt.UserRole) or {}
            if pl.get("placeholder") or not pl.get("is_folder"):
                continue

            tree.expand(index)
            processed += 1
            key = self._source_model_expand_all_key(index)
            if key:
                panel_ps = self._expand_all_processed_seen["source"]
                if key not in panel_ps:
                    panel_ps.add(key)
                    self._expand_all_processed["source"] += 1

            pl = index.data(Qt.UserRole) or {}
            if pl.get("children_loaded") or pl.get("load_failed"):
                for r in range(model.rowCount(index)):
                    cix = model.index(r, 0, index)
                    cpl = cix.data(Qt.UserRole) or {}
                    if cpl.get("placeholder") or not cpl.get("is_folder"):
                        continue
                    ck = self._source_model_expand_all_key(cix)
                    if ck:
                        self._expand_all_seen["source"].add(ck)
                    self._expand_all_source_model_queue.append(QPersistentModelIndex(cix))
                continue

            route = self._source_model_request_folder_children_load(index, triggered_by_user_expand=False)
            if route == "throttled":
                self._expand_all_source_model_queue.appendleft(pmi)
                break
            if route == "started":
                self._expand_all_source_model_queue.append(QPersistentModelIndex(index))
                break

            pl2 = index.data(Qt.UserRole) or {}
            if pl2.get("children_loaded") or pl2.get("load_failed"):
                for r in range(model.rowCount(index)):
                    cix = model.index(r, 0, index)
                    cpl = cix.data(Qt.UserRole) or {}
                    if cpl.get("placeholder") or not cpl.get("is_folder"):
                        continue
                    ck = self._source_model_expand_all_key(cix)
                    if ck:
                        self._expand_all_seen["source"].add(ck)
                    self._expand_all_source_model_queue.append(QPersistentModelIndex(cix))
                continue

            drive_id = self._resolve_tree_item_drive_id("source", pl2)
            item_id = pl2.get("id", "")
            pending_key = f"{drive_id}:{item_id}" if drive_id and item_id else ""
            if pending_key and pending_key in self.pending_folder_loads.get("source", set()):
                self._expand_all_source_model_queue.append(QPersistentModelIndex(index))
                break

        if self._expand_all_source_model_queue or self.pending_folder_loads.get("source"):
            self._update_expand_all_status("source", "Expanding branches...", loading=True)
            QTimer.singleShot(
                20,
                lambda: self._safe_invoke(
                    "source_model_expand_all_tick",
                    self._source_model_expand_all_tick,
                ),
            )
            return

        if self._source_model_tree_has_unloaded_folder_nodes():
            self._update_expand_all_status("source", "Expanding branches...", loading=True)
            QTimer.singleShot(
                50,
                lambda: self._safe_invoke(
                    "source_model_expand_all_reconcile",
                    self._source_model_expand_all_tick,
                ),
            )
            return

        self._finish_source_model_expand_all()

    def _finish_source_model_expand_all(self):
        panel_key = "source"
        tree = self.source_tree_widget
        if tree is not None:
            tree.setUpdatesEnabled(False)
            tree.blockSignals(True)
            try:
                tree.expandAll()
            finally:
                tree.blockSignals(False)
                tree.setUpdatesEnabled(True)
                tree.viewport().update()
        log_info(
            "expand_all_source_model_complete",
            folder_count=self._count_expandable_tree_nodes(panel_key),
        )
        self._expand_all_pending[panel_key] = False
        self._reset_expand_all_progress(panel_key)
        self._expand_all_deferred_refresh[panel_key] = True
        pending_selected_path = str(self._pending_workspace_post_expand_selection.get(panel_key, "") or "")
        if pending_selected_path:
            self._restore_selected_tree_path(panel_key, pending_selected_path)
            self._pending_workspace_post_expand_selection[panel_key] = ""
        if self._expand_all_deferred_refresh.get(panel_key):
            self._expand_all_deferred_refresh[panel_key] = False
            self._refresh_source_projection("source_projection_expand_all_complete")
            if self._source_column_refresh_pending:
                self._source_column_refresh_pending = False
                self._refresh_tree_column_width("source")
            if (
                getattr(self, "destination_tree_widget", None) is not None
                and self.planned_moves
                and not getattr(self, "_sharepoint_lazy_mode", False)
            ):
                self._schedule_deferred_destination_materialization("source_expand_all_complete", delay_ms=120)
        if self._source_restore_materialization_queue:
            self._schedule_source_restore_materialization_queue(
                "source_expand_all_complete",
                delay_ms=0,
            )
        if self._pending_snapshot_branch_refresh.get("source"):
            self._schedule_snapshot_branch_refresh("source", delay_ms=0)
        if self._source_projection_refresh_paths:
            phase_name, trigger_path = self._source_projection_refresh_context
            self._schedule_source_projection_refresh_for_paths(
                set(self._source_projection_refresh_paths),
                phase_name,
                trigger_path,
                delay_ms=0,
            )
        elif self._source_projection_refresh_context:
            phase_name, trigger_path = self._source_projection_refresh_context
            self._schedule_source_projection_refresh(
                phase_name,
                trigger_path,
                delay_ms=0,
            )
        self._try_flush_destination_future_model_after_source_restore("source_expand_all_complete")
        intent_map = getattr(self, "_workspace_restore_expanded_all_intent", None)
        if isinstance(intent_map, dict):
            intent_map[panel_key] = False
        self._sync_expand_all_button_from_tree(panel_key, fallback_expanded=True)
        self._update_expand_all_status(panel_key, "All loaded branches expanded.", loading=False)
        self._refresh_runtime_tree_snapshot(panel_key)
        self._workspace_ui_snapshot_dirty_panels.discard(panel_key)
        self._schedule_progress_summary_refresh()
        self._persist_workspace_ui_state_safely()

    def handle_expand_all(self, panel_key):
        tree = self.source_tree_widget if panel_key == "source" else self.destination_tree_widget
        if tree is None:
            return
        if panel_key == "source" and self._source_tree_uses_model_view():
            if self._expand_all_pending.get("source"):
                self._collapse_loaded_branches_for_panel(
                    "source",
                    status_message="Expand cancelled; branches collapsed.",
                )
                return
            expand_button = self._expand_all_button_for_panel("source")
            if expand_button is not None and expand_button.text() == "Collapse All":
                self._collapse_loaded_branches_for_panel("source", status_message="Loaded branches collapsed.")
                return
            if self._can_fast_bulk_expand("source"):
                self._fast_expand_all_loaded_tree("source")
                return
            self._begin_source_model_expand_all()
            return
        if self._expand_all_pending.get(panel_key):
            self._collapse_loaded_branches_for_panel(
                panel_key,
                status_message="Expand cancelled; branches collapsed.",
            )
            return

        if panel_key == "destination" and getattr(self, "_destination_root_prime_pending", False):
            status = self.destination_tree_status
            if status is not None:
                self._set_tree_status_message(
                    "destination",
                    "Please wait until top-level destination folders finish loading.",
                    loading=True,
                )
            return

        expand_button = self._expand_all_button_for_panel(panel_key)
        if expand_button is not None and expand_button.text() == "Collapse All":
            self._collapse_loaded_branches_for_panel(panel_key, status_message="Loaded branches collapsed.")
            return

        status = self.source_tree_status if panel_key == "source" else self.destination_tree_status

        if panel_key == "destination":
            self._destination_restore_materialization_user_paused = False
            drive_id = self._current_selected_destination_drive_id() or self.pending_root_drive_ids.get("destination", "")
            if drive_id and not self._destination_full_tree_ready():
                # Do not block expand-all while the full-tree snapshot is fetched.
                # Expand currently loaded branches immediately and hydrate additional
                # branches when full-tree materialization completes.
                # When the full-tree snapshot finishes, on_destination_full_tree_success resumes expand-all.
                self._destination_expand_all_after_full_tree = True
                self._update_expand_all_status(panel_key, "Loading full destination tree in background; expanding loaded branches...", loading=True)
                self.start_destination_full_tree_worker(drive_id)

        # When the tree has no lazy placeholders (_can_fast_bulk_expand), expandAll() only
        # opens rows that are already in memory — safe for destination after materialization.
        if self._can_fast_bulk_expand(panel_key):
            self._fast_expand_all_loaded_tree(panel_key)
            return

        self._expand_all_pending[panel_key] = True
        expandable_count = self._count_expandable_tree_nodes(panel_key)
        if panel_key == "source":
            # Keep source interactive first; expand speed is secondary.
            self._expand_all_max_per_tick[panel_key] = 1
        else:
            # Destination expand can process slightly more nodes per tick while
            # staying responsive; this reduces long-running 1-at-a-time loops.
            self._expand_all_max_per_tick[panel_key] = 3 if getattr(self, "_sharepoint_lazy_mode", False) else 2
        self._reset_expand_all_progress(panel_key)
        self._set_expand_all_button_label(panel_key, True)
        self._update_expand_all_status(panel_key, "Expanding branches...", loading=True)
        self._continue_expand_all(panel_key)

    def _narrow_source_projection_paths_for_move(self, move):
        paths = set()
        if not isinstance(move, dict):
            return paths
        p = self._canonical_source_projection_path(move.get("source_path", ""))
        if p:
            paths.add(p)
        return paths

    def _persist_planning_change(self, reason, *, notify_saved=False, source_projection_paths=None):
        try:
            spaths = (
                self._collect_current_source_projection_paths()
                if source_projection_paths is None
                else set(source_projection_paths)
            )
            self._queue_deferred_planning_refresh(
                reason,
                source_projection_paths=spaths,
                delay_ms=120,
                notify_saved=notify_saved,
            )
            if getattr(self, "source_tree_widget", None) is not None:
                self.source_tree_widget.viewport().update()
            if getattr(self, "destination_tree_widget", None) is not None:
                self.destination_tree_widget.viewport().update()
            self.update_progress_summaries()
        except Exception as exc:
            self._log_restore_exception("persist_planning_change.deferred_queue", exc)

        def _deferred_save():
            try:
                self._save_draft_shell(force=True)
            except Exception as exc:
                self._log_restore_exception("persist_planning_change.save", exc)

        # Yield a few frames so the planned-moves table and trees can repaint before disk I/O.
        QTimer.singleShot(50, _deferred_save)

    def _is_move_submitted(self, move):
        return str((move or {}).get("status", "")).strip().lower() == "submitted"

    def _is_proposed_folder_submitted(self, proposed_folder):
        return str(getattr(proposed_folder, "Status", "")).strip().lower() == "submitted"

    def _ensure_move_request_id(self, move, index_hint=0):
        request_id = str((move or {}).get("request_id", "")).strip()
        if request_id:
            return request_id
        request_id = f"REQ-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{index_hint + 1:03d}"
        move["request_id"] = request_id
        return request_id

    def _ensure_proposed_destination_id(self, proposed_folder, index_hint=0):
        destination_id = str(getattr(proposed_folder, "DestinationId", "")).strip()
        if destination_id:
            return destination_id
        destination_id = f"PROP-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{index_hint + 1:03d}"
        proposed_folder.DestinationId = destination_id
        return destination_id

    def _build_allocation_row_for_move(self, move, index_hint=0):
        source_node = move.get("source", {})
        destination_node = move.get("destination", {})
        source_path = move.get("source_path") or source_node.get("item_path") or source_node.get("display_path") or ""
        destination_path = (
            move.get("destination_path")
            or destination_node.get("item_path")
            or destination_node.get("display_path")
            or ""
        )
        return AllocationRow(
            RequestId=self._ensure_move_request_id(move, index_hint),
            SourceItemName=move.get("source_name") or source_node.get("name", "Unnamed Item"),
            SourcePath=source_path,
            SourceType="Folder" if source_node.get("is_folder", True) else "File",
            RequestedDestinationPath=destination_path,
            AllocationMethod=move.get("allocation_method", "Manual - Recursive"),
            RequestedBy=move.get("requested_by", self.current_session_context.get("operator_display_name", "")),
            RequestedDate=move.get("requested_date", datetime.utcnow().strftime("%Y-%m-%d %H:%M")),
            Status=move.get("status", "Pending"),
        )

    def _find_submitted_move_by_source_node(self, source_node):
        for move in self.planned_moves:
            if self._is_move_submitted(move) and self.node_keys_match(move.get("source", {}), source_node, "source"):
                return move
        return None

    def _find_submitted_move_by_destination_node(self, destination_node):
        for move in self.planned_moves:
            if self._is_move_submitted(move) and self.node_keys_match(move.get("destination", {}), destination_node, "destination"):
                return move
        return None

    def _find_proposed_folder_record_by_path(self, path):
        target_path = self.normalize_memory_path(path)
        for proposed_folder in self.proposed_folders:
            if self._proposed_destination_path(proposed_folder) == target_path:
                return proposed_folder
        return None

    def _show_submitted_item_locked_message(self, title, item_label, batch_id=""):
        suffix = f"\n\nSubmitted batch: {batch_id}" if batch_id else ""
        QMessageBox.information(
            self,
            title,
            f"{item_label} has already been submitted to Ozlink IT and is now locked.{suffix}",
        )

    def _rebuild_submission_visual_cache(self):
        cache = {
            "source_keys": {},
            "source_paths": {},
            "source_ancestors": [],
            "destination_keys": {},
            "destination_paths": {},
            "proposed_paths": {},
        }

        for move in self.planned_moves:
            if not self._is_move_submitted(move):
                continue

            batch_id = self._submitted_batch_id_for_move(move)
            source_node = move.get("source", {}) or {}
            destination_node = move.get("destination", {}) or {}

            source_key = self.build_node_key(source_node, "source")
            if source_key:
                cache["source_keys"][source_key] = batch_id

            source_path = self._canonical_source_projection_path(move.get("source_path", ""))
            if source_path:
                cache["source_paths"][source_path] = batch_id
                cache["source_ancestors"].append((source_path, batch_id))

            destination_key = self.build_node_key(destination_node, "destination")
            if destination_key:
                cache["destination_keys"][destination_key] = batch_id

            destination_path = self._canonical_destination_projection_path(move.get("destination_path", ""))
            if destination_path:
                cache["destination_paths"][destination_path] = batch_id

        cache["source_ancestors"].sort(key=lambda item: len(item[0]), reverse=True)

        for proposed_folder in self.proposed_folders:
            if not self._is_proposed_folder_submitted(proposed_folder):
                continue
            proposed_path = self._proposed_destination_path(proposed_folder)
            if proposed_path:
                cache["proposed_paths"][proposed_path] = self._submitted_batch_id_for_proposed_folder(proposed_folder)

        self._submitted_visual_cache = cache

    def _submitted_visual_state_for_node(self, node_data):
        empty_state = {"submitted": False, "batch_id": "", "kind": ""}
        if not node_data or node_data.get("placeholder"):
            return empty_state

        cache = getattr(self, "_submitted_visual_cache", None) or {}
        role = str(node_data.get("tree_role", "")).strip().lower()
        if role == "source":
            source_key = self.build_node_key(node_data, "source")
            batch_id = cache.get("source_keys", {}).get(source_key, "") if source_key else ""
            if batch_id:
                return {
                    "submitted": True,
                    "batch_id": batch_id,
                    "kind": "source_direct",
                }

            source_path = self._canonical_source_projection_path(self._tree_item_path(node_data))
            if source_path:
                batch_id = cache.get("source_paths", {}).get(source_path, "")
                if batch_id:
                    return {
                        "submitted": True,
                        "batch_id": batch_id,
                        "kind": "source_direct",
                    }
                for ancestor_path, ancestor_batch_id in cache.get("source_ancestors", []):
                    if self._path_is_descendant(source_path, ancestor_path, "source"):
                        return {
                            "submitted": True,
                            "batch_id": ancestor_batch_id,
                            "kind": "source_inherited",
                        }
            return empty_state

        if role == "destination":
            if self.node_is_proposed(node_data):
                proposed_path = self.normalize_memory_path(self._tree_item_path(node_data))
                batch_id = cache.get("proposed_paths", {}).get(proposed_path, "")
                if batch_id:
                    return {
                        "submitted": True,
                        "batch_id": batch_id,
                        "kind": "destination_proposed",
                    }

            if self.node_is_planned_allocation(node_data):
                destination_key = self.build_node_key(node_data, "destination")
                batch_id = cache.get("destination_keys", {}).get(destination_key, "") if destination_key else ""
                if not batch_id:
                    destination_path = self._canonical_destination_projection_path(self._tree_item_path(node_data))
                    batch_id = cache.get("destination_paths", {}).get(destination_path, "") if destination_path else ""
                if batch_id:
                    return {
                        "submitted": True,
                        "batch_id": batch_id,
                        "kind": "destination_allocation",
                    }

            source_path = self.normalize_memory_path(node_data.get("source_path", ""))
            if source_path:
                canonical_source_path = self._canonical_source_projection_path(source_path)
                batch_id = cache.get("source_paths", {}).get(canonical_source_path, "") if canonical_source_path else ""
                if batch_id:
                    return {
                        "submitted": True,
                        "batch_id": batch_id,
                        "kind": "destination_source_exact",
                    }
                if canonical_source_path:
                    for ancestor_path, ancestor_batch_id in cache.get("source_ancestors", []):
                        if self._path_is_descendant(canonical_source_path, ancestor_path, "source"):
                            return {
                                "submitted": True,
                                "batch_id": ancestor_batch_id,
                                "kind": "destination_source_inherited",
                            }

        return empty_state

    def _draft_submission_items(self):
        draft_moves = [move for move in self.planned_moves if not self._is_move_submitted(move)]
        draft_proposed = [folder for folder in self.proposed_folders if not self._is_proposed_folder_submitted(folder)]
        return draft_moves, draft_proposed

    def _validate_submission_readiness(self):
        issues = []
        warnings = []
        source_site = self.planning_inputs.get("Source Site").currentData() if hasattr(self, "planning_inputs") else None
        source_library = self.planning_inputs.get("Source Library").currentData() if hasattr(self, "planning_inputs") else None
        destination_site = self.planning_inputs.get("Destination Site").currentData() if hasattr(self, "planning_inputs") else None
        destination_library = self.planning_inputs.get("Destination Library").currentData() if hasattr(self, "planning_inputs") else None

        if not isinstance(source_site, dict):
            issues.append("Select a source site.")
        if not isinstance(source_library, dict):
            issues.append("Select a source library.")
        if not isinstance(destination_site, dict):
            issues.append("Select a destination site.")
        if not isinstance(destination_library, dict):
            issues.append("Select a destination library.")

        draft_moves, draft_proposed = self._draft_submission_items()
        if not draft_moves and not draft_proposed:
            issues.append("There are no new draft items to submit.")

        if self._workflow_needs_review_rows:
            warnings.append(f"{len(self._workflow_needs_review_rows)} item(s) still appear in Needs Review.")

        return issues, warnings, draft_moves, draft_proposed

    def _next_submission_batch_id(self):
        return f"SUB-{datetime.utcnow().strftime('%Y%m%d-%H%M%S-%f')}"

    def _mark_items_submitted(self, batch_id, submitted_utc, draft_moves, draft_proposed):
        for move in draft_moves:
            move["status"] = "Submitted"
            move["submitted_batch_id"] = batch_id
            move["submitted_utc"] = submitted_utc
            move.setdefault("allocation_method", "Manual - Recursive")

        for proposed_folder in draft_proposed:
            proposed_folder.Status = "Submitted"
            proposed_folder.RequestedBy = proposed_folder.RequestedBy or self.current_session_context.get("operator_display_name", "")
            proposed_folder.RequestedDate = proposed_folder.RequestedDate or submitted_utc
            setattr(proposed_folder, "SubmittedBatchId", batch_id)

    def _submitted_batch_id_for_move(self, move):
        return str((move or {}).get("submitted_batch_id", "")).strip()

    def _submitted_batch_id_for_proposed_folder(self, proposed_folder):
        return str(getattr(proposed_folder, "SubmittedBatchId", "") or "").strip()

    def _destination_target_path_for_node(self, node_data):
        if not isinstance(node_data, dict):
            return ""
        raw_path = (
            node_data.get("destination_path")
            or node_data.get("display_path")
            or node_data.get("item_path")
            or ""
        )
        return self._canonical_destination_projection_path(raw_path) or self.normalize_memory_path(raw_path)

    def _move_destination_target_path(self, move):
        if not isinstance(move, dict):
            return ""
        destination_node = move.get("destination", {})
        raw_path = (
            move.get("destination_path")
            or destination_node.get("destination_path")
            or destination_node.get("display_path")
            or destination_node.get("item_path")
            or ""
        )
        return self._canonical_destination_projection_path(raw_path) or self.normalize_memory_path(raw_path)

    def _confirm_assignment_override(self, source_name, current_destination_path, new_destination_path, inherited_from=""):
        inherited_text = ""
        if inherited_from:
            inherited_text = f"\n\nThis item is currently inheriting its mapping via:\n{inherited_from}"

        dialog = QMessageBox(self)
        dialog.setIcon(QMessageBox.Warning)
        dialog.setWindowTitle("Existing Assignment Detected")
        dialog.setText(
            f"'{source_name}' is already assigned to:\n"
            f"{current_destination_path or 'Not available'}\n\n"
            f"You are now assigning it to:\n"
            f"{new_destination_path or 'Not available'}"
            f"{inherited_text}\n\n"
            "Choose Yes to create an override and replace the current assignment, "
            "or No to keep the existing assignment."
        )
        dialog.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
        dialog.setDefaultButton(QMessageBox.No)
        return dialog.exec() == QMessageBox.Yes

    def handle_assign(self):
        source_node = self.get_selected_tree_node_data("source")
        destination_node = self.get_selected_tree_node_data("destination")

        if source_node is None:
            QMessageBox.information(self, "Assign", "Select one source item before assigning.")
            return

        if destination_node is None:
            QMessageBox.information(self, "Assign", "Select one destination item before assigning.")
            return

        if not self.node_is_valid_destination_target(destination_node):
            QMessageBox.information(self, "Assign", "Select a valid destination folder before assigning.")
            return

        submitted_move = self._find_submitted_move_by_source_node(source_node)
        if submitted_move is not None:
            self._show_submitted_item_locked_message(
                "Assign",
                f"'{source_node.get('name', 'This item')}'",
                self._submitted_batch_id_for_move(submitted_move),
            )
            return

        planning_state = self._resolve_selection_planning_state("source", source_node)
        existing_index = self.find_planned_move_index_by_source(source_node)
        if existing_index is not None:
            existing_move = self.planned_moves[existing_index]
            existing_destination = existing_move.get("destination", {})
            current_destination_path = self._move_destination_target_path(existing_move)
            new_destination_path = self._destination_target_path_for_node(destination_node)

            same_destination = bool(
                current_destination_path
                and new_destination_path
                and current_destination_path.lower() == new_destination_path.lower()
            )
            if not same_destination:
                same_destination = self.node_keys_match(existing_destination, destination_node, "destination")

            if same_destination:
                QMessageBox.information(self, "Assign", "This source item is already assigned to that destination.")
                return

            source_name = source_node.get("name", "This item")
            if not self._confirm_assignment_override(source_name, current_destination_path, new_destination_path):
                return

            self._mark_allocation_resolved(existing_move)
            replacement_move = self.build_planned_move_record(source_node, destination_node)
            replacement_move["allocation_method"] = "Manual - Override"
            self.planned_moves[existing_index] = replacement_move
            self.refresh_planned_moves_table()
            self.planned_moves_status.setText("Planned move override applied.")
            self._defer_destination_projection_if_quick_apply_failed(
                self._register_and_project_planned_move(replacement_move, reason="planned_move_override"),
                reconcile_reason="assign_override_tree_reconcile",
            )
            self._persist_planning_change(
                "planned_move_override_added",
                notify_saved=False,
                source_projection_paths=self._narrow_source_projection_paths_for_move(replacement_move),
            )
            return

        inherited_move = planning_state.get("move") if planning_state.get("mode") == "inherited" else None
        if inherited_move is not None:
            inherited_destination_path = self._move_destination_target_path(inherited_move)
            new_destination_path = self._destination_target_path_for_node(destination_node)
            same_destination = bool(
                inherited_destination_path
                and new_destination_path
                and inherited_destination_path.lower() == new_destination_path.lower()
            )
            if same_destination:
                QMessageBox.information(
                    self,
                    "Assign",
                    "This source item already inherits its assignment to that destination.",
                )
                return

            source_name = source_node.get("name", "This item")
            if not self._confirm_assignment_override(
                source_name,
                inherited_destination_path,
                new_destination_path,
                inherited_from=inherited_move.get("source_name", ""),
            ):
                return

            override_move = self.build_planned_move_record(source_node, destination_node)
            override_move["allocation_method"] = "Manual - Override"
            self.planned_moves.append(override_move)
            self.refresh_planned_moves_table()
            self.planned_moves_status.setText("Planned move override applied.")
            self._defer_destination_projection_if_quick_apply_failed(
                self._register_and_project_planned_move(override_move, reason="planned_move_override"),
                reconcile_reason="assign_override_tree_reconcile",
            )
            self._persist_planning_change(
                "planned_move_override_added",
                notify_saved=False,
                source_projection_paths=self._narrow_source_projection_paths_for_move(override_move),
            )
            return

        new_move = self.build_planned_move_record(source_node, destination_node)
        if source_node.get("is_folder"):
            new_move["allocation_method"] = "Manual - Recursive"
        else:
            new_move["allocation_method"] = "Manual"
        self.planned_moves.append(new_move)
        self.refresh_planned_moves_table()
        self.planned_moves_status.setText("Planned move added.")
        self._defer_destination_projection_if_quick_apply_failed(
            self._register_and_project_planned_move(new_move, reason="planned_move_added"),
            reconcile_reason="assign_tree_reconcile",
        )
        self._persist_planning_change(
            "planned_move_added",
            notify_saved=False,
            source_projection_paths=self._narrow_source_projection_paths_for_move(new_move),
        )

    def handle_unassign(self):
        selected_ranges = self.planned_moves_table.selectedRanges()
        if selected_ranges:
            row_index = selected_ranges[0].topRow()
            if 0 <= row_index < len(self.planned_moves):
                target_move = self.planned_moves[row_index]
                if self._is_move_submitted(target_move):
                    self._show_submitted_item_locked_message(
                        "Unassign",
                        f"'{target_move.get('source_name', 'This item')}'",
                        self._submitted_batch_id_for_move(target_move),
                    )
                    return
                removed_visually = self._quick_remove_planned_move_from_destination_tree(target_move) > 0
                self.planned_moves.pop(row_index)
                self._reset_unresolved_allocation_queue()
                self.refresh_planned_moves_table()
                self.planned_moves_status.setText("Planned move removed.")
                if not removed_visually:
                    self._schedule_deferred_destination_materialization("unassign_tree_reconcile", delay_ms=220)
                self._persist_planning_change(
                    "planned_move_removed",
                    source_projection_paths=self._narrow_source_projection_paths_for_move(target_move),
                )
                return

        source_node = self.get_selected_tree_node_data("source")
        if source_node is None:
            QMessageBox.information(self, "Unassign", "Select a planned move or select a source item with a planned move.")
            return

        for index, existing_move in enumerate(self.planned_moves):
            existing_source = existing_move.get("source", {})
            if self.node_keys_match(existing_source, source_node, "source"):
                if self._is_move_submitted(existing_move):
                    self._show_submitted_item_locked_message(
                        "Unassign",
                        f"'{existing_move.get('source_name', 'This item')}'",
                        self._submitted_batch_id_for_move(existing_move),
                    )
                    return
                removed_visually = self._quick_remove_planned_move_from_destination_tree(existing_move) > 0
                self.planned_moves.pop(index)
                self._reset_unresolved_allocation_queue()
                self.refresh_planned_moves_table()
                self.planned_moves_status.setText("Planned move removed.")
                if not removed_visually:
                    self._schedule_deferred_destination_materialization("unassign_tree_reconcile", delay_ms=220)
                self._persist_planning_change(
                    "planned_move_removed",
                    source_projection_paths=self._narrow_source_projection_paths_for_move(existing_move),
                )
                return

        QMessageBox.information(self, "Unassign", "No planned move exists for the selected source item.")

    def try_restore_main_window(self):
        preserve_maximized = self._was_maximized_before_login or self.isMaximized()
        print(
            "[window-login] "
            f"try_restore_main_window preserve_maximized={preserve_maximized} "
            f"before_state={self._window_state_repr()} "
            f"before_geometry={self.geometry().getRect()}"
        )

        if preserve_maximized:
            self.setWindowState((self.windowState() & ~Qt.WindowMinimized) | Qt.WindowMaximized | Qt.WindowActive)
            self.showMaximized()
        else:
            self.showNormal()
            self.setWindowState((self.windowState() & ~Qt.WindowMinimized) | Qt.WindowActive)
        self.raise_()
        self.activateWindow()

        self._schedule_safe_timer(100, "login_force_window_front_100ms", self._force_window_to_front_win32)
        self._schedule_safe_timer(300, "login_force_window_front_300ms", self._force_window_to_front_win32)
        self._schedule_safe_timer(750, "login_force_window_front_750ms", self._force_window_to_front_win32)
        self._schedule_safe_timer(1500, "login_force_window_front_1500ms", self._force_window_to_front_win32)
        self._schedule_safe_timer(2500, "login_force_window_front_2500ms", self._force_window_to_front_win32)
        self._schedule_safe_timer(0, "login_post_restore_window_log", self._log_post_login_window_state)

    def _force_window_to_front_win32(self):
        print(
            "[window-login] "
            f"_force_window_to_front_win32 before maximized={self.isMaximized()} "
            f"state={self._window_state_repr()} "
            f"geometry={self.geometry().getRect()}"
        )
        if not sys.platform.startswith("win"):
            self.raise_()
            self.activateWindow()
            self._log_post_login_window_state(prefix="[window-login] _force_window_to_front_win32 after")
            return

        try:
            hwnd = int(self.winId())
            user32 = ctypes.windll.user32

            SW_RESTORE = 9
            SW_SHOWMAXIMIZED = 3
            HWND_TOPMOST = -1
            HWND_NOTOPMOST = -2
            SWP_NOMOVE = 0x0002
            SWP_NOSIZE = 0x0001
            SWP_SHOWWINDOW = 0x0040

            show_code = SW_SHOWMAXIMIZED if (self._was_maximized_before_login or self.isMaximized()) else SW_RESTORE
            user32.ShowWindow(hwnd, show_code)

            user32.SetWindowPos(
                hwnd,
                HWND_TOPMOST,
                0, 0, 0, 0,
                SWP_NOMOVE | SWP_NOSIZE | SWP_SHOWWINDOW
            )

            user32.SetWindowPos(
                hwnd,
                HWND_NOTOPMOST,
                0, 0, 0, 0,
                SWP_NOMOVE | SWP_NOSIZE | SWP_SHOWWINDOW
            )

            user32.BringWindowToTop(hwnd)
            user32.SetForegroundWindow(hwnd)

        except Exception:
            self.raise_()
            self.activateWindow()

        self._log_post_login_window_state(prefix="[window-login] _force_window_to_front_win32 after")

    def flash_taskbar(self):
        if not sys.platform.startswith("win"):
            return

        try:
            hwnd = int(self.winId())

            class FLASHWINFO(ctypes.Structure):
                _fields_ = [
                    ("cbSize", ctypes.c_uint),
                    ("hwnd", ctypes.c_void_p),
                    ("dwFlags", ctypes.c_uint),
                    ("uCount", ctypes.c_uint),
                    ("dwTimeout", ctypes.c_uint),
                ]

            FLASHW_TRAY = 0x00000002
            FLASHW_TIMERNOFG = 0x0000000C

            info = FLASHWINFO(
                ctypes.sizeof(FLASHWINFO),
                hwnd,
                FLASHW_TRAY | FLASHW_TIMERNOFG,
                5,
                0
            )

            ctypes.windll.user32.FlashWindowEx(ctypes.byref(info))
        except Exception:
            pass
