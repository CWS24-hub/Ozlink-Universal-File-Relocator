import json
import logging
import os
import tempfile
import unittest
from collections import deque
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt, QTimer
from PySide6.QtWidgets import QApplication, QMessageBox, QTreeView, QTreeWidget, QTreeWidgetItem

import ozlink_console.logger as logger_module
from ozlink_console.logger import JsonLineFormatter
from ozlink_console.main_window import MainWindow
from ozlink_console.memory import MemoryManager
from ozlink_console.tree_models.sharepoint_source_model import SharePointSourceTreeModel
from ozlink_console.graph import GraphClient
from ozlink_console.models import ProposedFolder, SessionState


class _ViewportStub:
    def update(self):
        return None


class _TreeStub:
    def viewport(self):
        return _ViewportStub()


class _LabelStub:
    def __init__(self):
        self.text = ""
        self.properties = {}
        self.stylesheet = ""
        self.tooltip = ""

    def setText(self, value):
        self.text = value

    def setProperty(self, name, value):
        self.properties[name] = value

    def setStyleSheet(self, value):
        self.stylesheet = value

    def setToolTip(self, value):
        self.tooltip = str(value or "")

    def property(self, name):
        return self.properties.get(name)


class _ButtonStub:
    def __init__(self, text="Expand All"):
        self._text = text
        self.enabled = True

    def text(self):
        return self._text

    def setText(self, value):
        self._text = value

    def setEnabled(self, value):
        self.enabled = bool(value)


class _ExpandTreeStub:
    def __init__(self):
        self.collapsed = False
        self.expanded_all = False
        self.updates_enabled = True
        self.signals_blocked = False
        self._top_level_items = []
        self.current_item = None

    def setUpdatesEnabled(self, value):
        self.updates_enabled = bool(value)

    def blockSignals(self, value):
        self.signals_blocked = bool(value)

    def collapseAll(self):
        self.collapsed = True

    def expandAll(self):
        self.expanded_all = True

    def topLevelItemCount(self):
        return len(self._top_level_items)

    def topLevelItem(self, index):
        return self._top_level_items[index]

    def expandItem(self, _item):
        return None

    def addTopLevelItem(self, item):
        self._top_level_items.append(item)

    def takeTopLevelItem(self, index):
        return self._top_level_items.pop(index)

    def indexOfTopLevelItem(self, item):
        try:
            return self._top_level_items.index(item)
        except ValueError:
            return -1

    def setCurrentItem(self, item):
        self.current_item = item

    def viewport(self):
        return _ViewportStub()


def _main_window_stub_for_source_model_expand_all(tree, model):
    """Attach minimal state so MainWindow.__new__ can run model-view expand-all paths."""
    window = MainWindow.__new__(MainWindow)
    window._source_tree_model_view = True
    window.source_tree_widget = tree
    window.source_sharepoint_model = model
    window.pending_root_drive_ids = {"source": "", "destination": ""}
    window._expand_all_source_model_queue = deque()
    window._expand_all_pending = {"source": False, "destination": False}
    window._expand_all_queue = {"source": deque(), "destination": deque()}
    window._expand_all_seen = {"source": set(), "destination": set()}
    window._expand_all_processed_seen = {"source": set(), "destination": set()}
    window._expand_all_processed = {"source": 0, "destination": 0}
    window._expand_all_max_per_tick = {"source": 1, "destination": 2}
    window._expand_all_deferred_refresh = {"source": False, "destination": False}
    window._expand_all_requeue_attempts = {}
    window._expand_all_status_last_update_ms = {"source": 0, "destination": 0}
    window.pending_folder_loads = {"source": set(), "destination": set()}
    window._pending_workspace_post_expand_selection = {"source": "", "destination": ""}
    window._workspace_restore_expanded_all_intent = {"source": False, "destination": False}
    window._workspace_ui_snapshot_dirty_panels = set()
    window._source_column_refresh_pending = False
    window._source_restore_materialization_queue = []
    window._pending_snapshot_branch_refresh = {"source": set(), "destination": set()}
    window._source_projection_refresh_paths = None
    window._source_projection_refresh_context = None
    window.planned_moves = []
    window.destination_tree_widget = None
    window._set_expand_all_button_label = lambda *a, **k: None
    window._set_tree_status_message = lambda *a, **k: None
    window._update_expand_all_status = lambda *a, **k: None
    window._refresh_source_projection = lambda *a, **k: None
    window._refresh_tree_column_width = lambda *a, **k: None
    window._schedule_deferred_destination_materialization = lambda *a, **k: None
    window._schedule_source_restore_materialization_queue = lambda *a, **k: None
    window._schedule_snapshot_branch_refresh = lambda *a, **k: None
    window._schedule_source_projection_refresh_for_paths = lambda *a, **k: None
    window._schedule_source_projection_refresh = lambda *a, **k: None
    window._try_flush_destination_future_model_after_source_restore = lambda *a, **k: None
    window._sync_expand_all_button_from_tree = lambda *a, **k: None
    window._refresh_runtime_tree_snapshot = lambda *a, **k: None
    window._persist_workspace_ui_state_safely = lambda *a, **k: None
    window._schedule_progress_summary_refresh = lambda *a, **k: None
    return window


class _DeletedTreeItemStub:
    def childCount(self):
        raise RuntimeError("Internal C++ object (PySide6.QtWidgets.QTreeWidgetItem) already deleted.")

    def data(self, *_args, **_kwargs):
        raise RuntimeError("Internal C++ object (PySide6.QtWidgets.QTreeWidgetItem) already deleted.")


class _TimerStub:
    def __init__(self):
        self.active = False
        self.started = 0
        self.started_with = []
        self.stopped = 0
        self._interval = 900000

    def isActive(self):
        return self.active

    def start(self, value=None):
        self.active = True
        self.started += 1
        if value is not None:
            self.started_with.append(int(value))

    def stop(self):
        self.active = False
        self.stopped += 1

    def interval(self):
        return self._interval


class LoggerRegressionTests(unittest.TestCase):
    def test_json_formatter_handles_recursive_payload(self):
        payload = {}
        payload["self"] = payload

        record = logging.LogRecord(
            name="ozlink_console",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="Recursive payload test.",
            args=(),
            exc_info=None,
        )
        record.data = payload

        formatted = JsonLineFormatter().format(record)
        decoded = json.loads(formatted)

        self.assertEqual(decoded["message"], "Recursive payload test.")
        self.assertEqual(decoded["data"]["self"], "<recursive>")


class MemoryImportRegressionTests(unittest.TestCase):
    def tearDown(self):
        logger = logger_module._LOGGER
        if logger is not None:
            for handler in list(logger.handlers):
                handler.close()
                logger.removeHandler(handler)
        logger_module._LOGGER = None

    def test_import_bundle_normalizes_legacy_destination_paths(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            localappdata = Path(temp_dir) / "LocalAppData"
            localappdata.mkdir(parents=True, exist_ok=True)
            bundle_root = Path(temp_dir) / "bundle"
            bundle_root.mkdir(parents=True, exist_ok=True)

            session_payload = {
                "DraftId": "RECOVERED-TEST",
                "DraftName": "Recovered Draft",
                "DestinationSelectedPath": "Documents\\Root\\Finance\\Follow up",
                "DestinationExpandedPaths": ["Documents\\Root\\Projects", "Documents\\Root\\HR"],
            }
            allocations_payload = [
                {
                    "RequestId": "REC-001",
                    "SourceItemName": "Contractor Resumes",
                    "SourcePath": "Files to be Migrated\\FTBMRoot\\Contractor Resumes",
                    "SourceType": "Folder",
                    "RequestedDestinationPath": "Documents\\Root\\HR\\Employee Files\\Contractor Resumes",
                    "AllocationMethod": "Recovered from Log - Manual",
                    "RequestedBy": "Gary",
                    "RequestedDate": "2026-03-24 15:03",
                    "Status": "Draft",
                }
            ]
            proposed_payload = [
                {
                    "FolderName": "Completed Projects",
                    "DestinationPath": "",
                    "DestinationId": "",
                    "ParentPath": "Documents\\Root\\Projects",
                    "IsSelectable": True,
                    "IsProposed": True,
                    "Status": "Draft",
                }
            ]

            (bundle_root / "Draft-SessionState.json").write_text(json.dumps(session_payload, indent=2), encoding="utf-8")
            (bundle_root / "Draft-AllocationQueue.json").write_text(json.dumps(allocations_payload, indent=2), encoding="utf-8")
            (bundle_root / "Draft-ProposedFolders.json").write_text(json.dumps(proposed_payload, indent=2), encoding="utf-8")

            with patch.dict(os.environ, {"LOCALAPPDATA": str(localappdata)}):
                manager = MemoryManager(tenant_domain="aquaticcs.com.au", operator_upn="gary@aquaticcs.com.au")
                manager.import_bundle(bundle_root)

                imported_session = json.loads(manager.paths["session"].read_text(encoding="utf-8"))
                imported_allocations = json.loads(manager.paths["allocations"].read_text(encoding="utf-8"))
                imported_proposed = json.loads(manager.paths["proposed"].read_text(encoding="utf-8"))

                self.assertEqual(imported_session["DestinationSelectedPath"], "Root\\Finance\\Follow up")
                self.assertEqual(imported_session["DestinationExpandedPaths"], ["Root\\Projects", "Root\\HR"])
                self.assertEqual(
                    imported_allocations[0]["RequestedDestinationPath"],
                    "Root\\HR\\Employee Files\\Contractor Resumes",
                )
                self.assertEqual(imported_proposed[0]["ParentPath"], "Root\\Projects")
                self.assertTrue(manager.paths["session_recovery"].exists())
                self.assertTrue(manager.paths["allocations_recovery"].exists())
                self.assertTrue(manager.paths["proposed_recovery"].exists())

                logger = logger_module._LOGGER
                if logger is not None:
                    for handler in list(logger.handlers):
                        handler.close()
                        logger.removeHandler(handler)
                logger_module._LOGGER = None


class GraphClientRegressionTests(unittest.TestCase):
    def test_request_retries_once_after_401_with_silent_token_refresh(self):
        client = GraphClient()
        client.token = "expired-token"

        class _AppStub:
            def get_accounts(self):
                return [{"home_account_id": "account-1"}]

            def acquire_token_silent(self, scopes, account=None, force_refresh=False):
                if force_refresh:
                    return {"access_token": "renewed-token"}
                return None

        class _ResponseStub:
            def __init__(self, status_code, payload):
                self.status_code = status_code
                self._payload = payload

            def raise_for_status(self):
                if self.status_code >= 400:
                    raise RuntimeError(f"http {self.status_code}")

            def json(self):
                return self._payload

        client.app = _AppStub()

        with patch(
            "ozlink_console.graph.requests.request",
            side_effect=[
                _ResponseStub(401, {"error": {"code": "InvalidAuthenticationToken"}}),
                _ResponseStub(200, {"value": [{"id": "item-1"}]}),
            ],
        ) as request_mock:
            result = client.get_paged("https://graph.microsoft.com/v1.0/drives/drive/root/children")

        self.assertEqual(result, [{"id": "item-1"}])
        self.assertEqual(client.token, "renewed-token")
        self.assertEqual(request_mock.call_count, 2)


class DestinationReplayRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = QApplication.instance() or QApplication([])

    def test_lazy_background_load_preloads_destination_and_only_counts_source(self):
        window = MainWindow.__new__(MainWindow)
        window.planned_moves = []
        window._sharepoint_lazy_mode = True
        window._deferred_background_load_targets = {}
        destination_started = []
        source_counts_started = []
        source_preload_started = []
        window.start_destination_full_tree_worker = lambda drive_id: destination_started.append(drive_id)
        window.start_full_count_worker = lambda drive_id: source_counts_started.append(drive_id)
        window._start_source_background_preload = lambda: source_preload_started.append(True)

        window._schedule_deferred_background_load("destination", "drive-123")
        window._schedule_deferred_background_load("source", "drive-source")

        self.assertEqual(window._deferred_background_load_targets["destination"], "drive-123")
        self.assertEqual(window._deferred_background_load_targets["source"], "drive-source")
        self.assertEqual(destination_started, ["drive-123"])
        self.assertEqual(source_counts_started, ["drive-source"])
        self.assertEqual(source_preload_started, [])

    def test_session_state_round_trips_tree_snapshots(self):
        state = SessionState(
            DraftId="DRAFT-001",
            SourceTreeSnapshot=[{"text": "Folder: Source", "children": []}],
            DestinationTreeSnapshot=[{"text": "Folder: Destination", "children": []}],
        )

        decoded = SessionState.from_dict(state.to_dict())

        self.assertEqual(decoded.SourceTreeSnapshot[0]["text"], "Folder: Source")
        self.assertEqual(decoded.DestinationTreeSnapshot[0]["text"], "Folder: Destination")

    def test_begin_session_workspace_restore_uses_persisted_tree_snapshots(self):
        window = MainWindow.__new__(MainWindow)
        window._draft_shell_state = SessionState(
            SelectedSourceLibrary="Files to be Migrated",
            SelectedDestinationLibrary="Documents",
            SourceExpandedPaths=["FTBMRoot\\Public"],
            DestinationExpandedPaths=["Root\\Finance"],
            SourceExpandedAll=True,
            DestinationExpandedAll=True,
            PlanningHeaderCollapsed=True,
            WorkspacePanelCollapsed=True,
            SourceTreeSnapshot=[{
                "text": "Folder: Source Snapshot",
                "expanded": True,
                "data": {"item_path": "FTBMRoot\\Public", "is_folder": True},
                "children": [],
            }],
            DestinationTreeSnapshot=[{
                "text": "Folder: Destination Snapshot",
                "expanded": True,
                "data": {"item_path": "Root\\Finance", "is_folder": True},
                "children": [],
            }],
        )
        window._pending_snapshot_branch_refresh = {"source": set(), "destination": set()}
        window._snapshot_branch_refresh_scheduled = {"source": False, "destination": False}
        panel_states = []
        window._apply_planning_header_collapsed_state = lambda collapsed: panel_states.append(("header", collapsed))
        window._apply_workspace_tabs_collapsed_state = lambda collapsed: panel_states.append(("workspace", collapsed))

        window._begin_session_workspace_ui_restore()

        self.assertEqual(window._pending_session_workspace_restore_panels, {"source", "destination"})
        self.assertEqual(len(window._runtime_session_tree_snapshots["source"]), 1)
        self.assertEqual(len(window._runtime_session_tree_snapshots["destination"]), 1)
        self.assertIn("source", window._pending_session_tree_snapshots)
        self.assertIn("destination", window._pending_session_tree_snapshots)
        self.assertEqual(panel_states, [("header", True), ("workspace", True)])
        src_pub = MainWindow._canonical_source_projection_path(window, "FTBMRoot\\Public")
        dst_fin = MainWindow._canonical_destination_projection_path(window, "Root\\Finance")
        self.assertIn(src_pub, window._pending_snapshot_branch_refresh["source"])
        self.assertIn(dst_fin, window._pending_snapshot_branch_refresh["destination"])

    def test_restore_tree_items_snapshot_updates_runtime_cache(self):
        window = MainWindow.__new__(MainWindow)
        window.source_tree_widget = QTreeWidget()
        window.destination_tree_widget = QTreeWidget()
        window.source_tree_status = _LabelStub()
        window.destination_tree_status = _LabelStub()
        window._destination_root_prime_pending = True
        window._runtime_session_tree_snapshots = {"source": [], "destination": []}
        snapshots = [{
            "text": "Folder: Public",
            "expanded": True,
            "data": {"item_path": "FTBMRoot\\Public", "is_folder": True},
            "children": [],
        }]

        restored = window._restore_tree_items_snapshot(
            "source",
            snapshots,
            "Loaded source tree from local snapshot. Refreshing live content...",
        )

        self.assertTrue(restored)
        self.assertEqual(window._runtime_session_tree_snapshots["source"], snapshots)

    def test_schedule_workspace_ui_persist_refreshes_runtime_snapshots_and_starts_timer(self):
        window = MainWindow.__new__(MainWindow)
        source_tree = QTreeWidget()
        source_root = QTreeWidgetItem(["Folder: Public"])
        source_root.setData(0, Qt.UserRole, {"item_path": "FTBMRoot\\Public", "is_folder": True})
        source_tree.addTopLevelItem(source_root)
        destination_tree = QTreeWidget()
        destination_root = QTreeWidgetItem(["Folder: Root"])
        destination_root.setData(0, Qt.UserRole, {"item_path": "Root", "is_folder": True})
        destination_tree.addTopLevelItem(destination_root)
        window.source_tree_widget = source_tree
        window.destination_tree_widget = destination_tree
        window.source_tree_status = _LabelStub()
        window.destination_tree_status = _LabelStub()
        window._runtime_session_tree_snapshots = {"source": [], "destination": []}
        window._workspace_ui_snapshot_dirty_panels = set()
        window._expand_all_pending = {"source": False, "destination": False}
        window._planning_workspace_is_busy = lambda: False
        window._workspace_ui_persist_timer = _TimerStub()
        exceptions = []
        window._log_restore_exception = lambda phase, exc: exceptions.append((phase, str(exc)))
        window._persist_workspace_ui_state_safely = lambda: None

        window._schedule_workspace_ui_persist(delay_ms=1500)
        window._on_workspace_ui_persist_timer()

        self.assertEqual(window._workspace_ui_persist_timer.started_with, [1500])
        self.assertEqual(
            window._runtime_session_tree_snapshots["source"][0]["data"]["item_path"],
            "FTBMRoot\\Public",
        )
        self.assertEqual(
            window._runtime_session_tree_snapshots["destination"][0]["data"]["item_path"],
            "Root",
        )
        self.assertEqual(exceptions, [])

    def test_process_snapshot_branch_refresh_starts_visible_expanded_branch_loads(self):
        window = MainWindow.__new__(MainWindow)
        window._draft_shell_state = SessionState()
        source_tree = QTreeWidget()
        root = QTreeWidgetItem(["Folder: Public"])
        root.setData(
            0,
            Qt.UserRole,
            {
                "name": "Public",
                "item_path": "FTBMRoot\\Public",
                "is_folder": True,
                "children_loaded": False,
                "load_failed": False,
            },
        )
        source_tree.addTopLevelItem(root)
        window.source_tree_widget = source_tree
        window.destination_tree_widget = QTreeWidget()
        window.source_tree_status = _LabelStub()
        window.destination_tree_status = _LabelStub()
        window.pending_folder_loads = {"source": set(), "destination": set()}
        window.root_load_workers = {}
        window._pending_snapshot_branch_refresh = {"source": {"FTBMRoot\\Public"}, "destination": set()}
        window._snapshot_branch_refresh_scheduled = {"source": False, "destination": False}
        window._expand_all_pending = {"source": False, "destination": False}
        started = []
        scheduled = []
        status_updates = []
        window._ensure_tree_item_load_started = lambda panel_key, item: started.append(
            (panel_key, item.data(0, Qt.UserRole).get("item_path"))
        ) or True
        window._schedule_snapshot_branch_refresh = lambda panel_key, delay_ms=0: scheduled.append((panel_key, delay_ms))
        window._set_tree_status_message = lambda panel_key, message, loading=False: status_updates.append(
            (panel_key, message, loading)
        )

        window._process_snapshot_branch_refresh("source")

        self.assertEqual(started, [("source", "FTBMRoot\\Public")])
        self.assertEqual(window._pending_snapshot_branch_refresh["source"], {"FTBMRoot\\Public"})
        self.assertEqual(scheduled, [("source", 150)])
        self.assertEqual(status_updates[-1], ("source", "Refreshing saved branches... (0/1)", True))

    def test_process_snapshot_branch_refresh_limits_started_loads_per_tick(self):
        window = MainWindow.__new__(MainWindow)
        window._draft_shell_state = SessionState()
        source_tree = QTreeWidget()
        for path in ("FTBMRoot\\Public", "FTBMRoot\\Photos", "FTBMRoot\\Finance"):
            item = QTreeWidgetItem([path])
            item.setData(
                0,
                Qt.UserRole,
                {
                    "name": path.rsplit("\\", 1)[-1],
                    "item_path": path,
                    "is_folder": True,
                    "children_loaded": False,
                    "load_failed": False,
                },
            )
            source_tree.addTopLevelItem(item)
        window.source_tree_widget = source_tree
        window.destination_tree_widget = QTreeWidget()
        window.source_tree_status = _LabelStub()
        window.destination_tree_status = _LabelStub()
        window.pending_folder_loads = {"source": set(), "destination": set()}
        window.root_load_workers = {}
        window._pending_snapshot_branch_refresh = {
            "source": {"FTBMRoot\\Public", "FTBMRoot\\Photos", "FTBMRoot\\Finance"},
            "destination": set(),
        }
        window._snapshot_branch_refresh_scheduled = {"source": False, "destination": False}
        window._expand_all_pending = {"source": False, "destination": False}
        started = []
        window._ensure_tree_item_load_started = lambda panel_key, item: started.append(
            (panel_key, item.data(0, Qt.UserRole).get("item_path"))
        ) or True
        window._schedule_snapshot_branch_refresh = lambda panel_key, delay_ms=0: None
        window._set_tree_status_message = lambda panel_key, message, loading=False: None

        window._process_snapshot_branch_refresh("source")

        self.assertEqual(len(started), 2)
        started_paths = {path for _panel, path in started}
        self.assertTrue(started_paths.issubset({"FTBMRoot\\Public", "FTBMRoot\\Photos", "FTBMRoot\\Finance"}))
        self.assertEqual(len(window._pending_snapshot_branch_refresh["source"]), 2)
        self.assertEqual(window._pending_snapshot_branch_refresh["source"], started_paths)

    def test_restore_panel_expanded_all_state_uses_snapshot_without_restart_crawl(self):
        window = MainWindow.__new__(MainWindow)
        window._pending_session_tree_snapshots = {"source": [{"text": "Folder: Public"}], "destination": []}
        window._runtime_session_tree_snapshots = {"source": [], "destination": []}
        window._pending_snapshot_branch_refresh = {"source": {"FTBMRoot\\Public"}, "destination": set()}
        synced = []
        statuses = []
        window._panel_is_expanded_all = lambda panel_key: False
        window._sync_expand_all_button_from_tree = lambda panel_key, fallback_expanded=False: synced.append((panel_key, fallback_expanded))
        window._set_tree_status_message = lambda panel_key, message, loading=False: statuses.append((panel_key, message, loading))
        window._can_fast_bulk_expand = lambda panel_key: False
        window._count_expandable_tree_nodes = lambda panel_key: 0
        window._count_tree_snapshot_nodes = lambda snapshots: 1
        window._tree_snapshot_node_count_gt = lambda snapshots, threshold: True
        restored = []
        window._restore_tree_items_snapshot = lambda panel_key, snapshots, status_message: restored.append(
            (panel_key, snapshots, status_message)
        ) or True
        window._fast_expand_all_loaded_tree = lambda panel_key: (_ for _ in ()).throw(AssertionError("should not bulk expand"))
        window._continue_expand_all = lambda panel_key: (_ for _ in ()).throw(AssertionError("should not restart expand-all"))

        window._restore_panel_expanded_all_state("source")

        self.assertEqual(restored, [("source", [{"text": "Folder: Public"}], "Expanded from local snapshot. Refreshing live content...")])
        self.assertEqual(synced, [("source", False)])
        self.assertEqual(
            statuses,
            [("source", "Expanded from local snapshot. Refreshing live content...", True)],
        )

    def test_restore_panel_expanded_all_state_can_use_runtime_snapshot_cache(self):
        window = MainWindow.__new__(MainWindow)
        window._pending_session_tree_snapshots = {"source": [], "destination": []}
        window._runtime_session_tree_snapshots = {"source": [{"text": "Folder: Public"}], "destination": []}
        window._pending_snapshot_branch_refresh = {"source": {"FTBMRoot\\Public"}, "destination": set()}
        restored = []
        statuses = []
        synced = []
        window._panel_is_expanded_all = lambda panel_key: False
        window._count_expandable_tree_nodes = lambda panel_key: 1
        window._count_tree_snapshot_nodes = lambda snapshots: 6
        window._tree_snapshot_node_count_gt = lambda snapshots, threshold: True
        window._restore_tree_items_snapshot = lambda panel_key, snapshots, status_message: restored.append(
            (panel_key, snapshots, status_message)
        ) or True
        window._sync_expand_all_button_from_tree = lambda panel_key, fallback_expanded=False: synced.append((panel_key, fallback_expanded))
        window._set_tree_status_message = lambda panel_key, message, loading=False: statuses.append((panel_key, message, loading))
        window._can_fast_bulk_expand = lambda panel_key: False
        window._fast_expand_all_loaded_tree = lambda panel_key: (_ for _ in ()).throw(AssertionError("should not bulk expand"))
        window._continue_expand_all = lambda panel_key: (_ for _ in ()).throw(AssertionError("should not restart expand-all"))

        window._restore_panel_expanded_all_state("source")

        self.assertEqual(restored, [("source", [{"text": "Folder: Public"}], "Expanded from local snapshot. Refreshing live content...")])
        self.assertEqual(synced, [("source", False)])
        self.assertEqual(statuses[-1], ("source", "Expanded from local snapshot. Refreshing live content...", True))

    def test_panel_is_expanded_all_uses_tree_state_over_stale_button_label(self):
        window = MainWindow.__new__(MainWindow)
        tree = QTreeWidget()
        root = QTreeWidgetItem(["Folder: Root"])
        root.setData(0, Qt.UserRole, {"item_path": "Root", "is_folder": True})
        root.setExpanded(True)
        child = QTreeWidgetItem(["Folder: Child"])
        child.setData(0, Qt.UserRole, {"item_path": "Root\\Child", "is_folder": True})
        child.setExpanded(False)
        grandchild = QTreeWidgetItem(["File: Example"])
        grandchild.setData(0, Qt.UserRole, {"item_path": "Root\\Child\\Example.txt", "is_folder": False})
        child.addChild(grandchild)
        root.addChild(child)
        tree.addTopLevelItem(root)

        window.source_tree_widget = tree
        window.destination_tree_widget = QTreeWidget()
        window.source_expand_all_button = _ButtonStub("Collapse All")
        window.destination_expand_all_button = _ButtonStub("Expand All")
        window._expand_all_pending = {"source": False, "destination": False}

        self.assertFalse(window._panel_is_expanded_all("source"))

    def test_find_visible_destination_item_by_path_skips_verbose_logs_by_default(self):
        window = MainWindow.__new__(MainWindow)
        tree = QTreeWidget()
        root = QTreeWidgetItem(["Folder: Root"])
        root.setData(
            0,
            Qt.UserRole,
            {
                "name": "Root",
                "item_path": "Root\\Finance",
                "display_path": "Root\\Finance",
                "is_folder": True,
            },
        )
        tree.addTopLevelItem(root)
        window.destination_tree_widget = tree
        window._memory_restore_in_progress = False
        window._canonical_destination_projection_path = lambda path: path
        window.normalize_memory_path = lambda path: path
        window._tree_item_path = lambda data: data.get("item_path", "")
        window._iter_tree_items = lambda item: [item]
        window._select_canonical_destination_item = lambda matches: matches[0] if matches else None
        window._log_restore_phase = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected verbose logging"))

        match = window._find_visible_destination_item_by_path("Root\\Finance")

        self.assertIs(match, root)

    def test_restore_tree_items_snapshot_clears_destination_root_prime_when_snapshot_is_rich(self):
        window = MainWindow.__new__(MainWindow)
        window.source_tree_widget = QTreeWidget()
        window.destination_tree_widget = QTreeWidget()
        window.source_tree_status = _LabelStub()
        window.destination_tree_status = _LabelStub()
        window._destination_root_prime_pending = True

        restored = window._restore_tree_items_snapshot(
            "destination",
            [{
                "text": "Folder: Root",
                "expanded": True,
                "data": {"item_path": "Root", "is_folder": True},
                "children": [{
                    "text": "Folder: Finance",
                    "expanded": True,
                    "data": {"item_path": "Root\\Finance", "is_folder": True},
                    "children": [],
                }],
            }],
            "Expanded from local snapshot. Refreshing live content...",
        )

        self.assertTrue(restored)
        self.assertFalse(window._destination_root_prime_pending)

    def test_maybe_restore_runtime_snapshot_after_root_bind_uses_richer_saved_tree(self):
        window = MainWindow.__new__(MainWindow)
        window._draft_shell_state = SessionState(
            DestinationExpandedAll=True,
            DestinationSelectedPath="Root\\Finance",
        )
        window._runtime_session_tree_snapshots = {
            "source": [],
            "destination": [{
                "text": "Folder: Root",
                "expanded": True,
                "data": {"item_path": "Root", "is_folder": True},
                "children": [{
                    "text": "Folder: Finance",
                    "expanded": True,
                    "data": {"item_path": "Root\\Finance", "is_folder": True},
                    "children": [],
                }],
            }],
        }
        restored = []
        synced = []
        selections = []
        window._restore_tree_items_snapshot = lambda panel_key, snapshots, status_message: restored.append(
            (panel_key, snapshots, status_message)
        ) or True
        window._sync_expand_all_button_from_tree = lambda panel_key, fallback_expanded=False: synced.append((panel_key, fallback_expanded))
        window._restore_selected_tree_path = lambda panel_key, path: selections.append((panel_key, path))
        window._count_expandable_tree_nodes = lambda panel_key: 1
        window._count_tree_snapshot_nodes = lambda snapshots: 5

        reused = window._maybe_restore_runtime_snapshot_after_root_bind("destination")

        self.assertTrue(reused)
        self.assertEqual(restored[0][0], "destination")
        self.assertEqual(synced, [("destination", False)])
        self.assertEqual(selections, [("destination", "Root\\Finance")])

    def test_destination_shallow_root_payload_does_not_overwrite_richer_visible_tree(self):
        window = MainWindow.__new__(MainWindow)
        window.root_load_workers = {"destination": {"id": "root-1", "request_signature": {"panel_key": "destination"}}}
        window.pending_root_drive_ids = {"destination": "drive-1"}
        window._memory_restore_in_progress = False
        window._log_restore_phase = lambda *args, **kwargs: None
        window._log_worker_lifecycle = lambda *args, **kwargs: None
        window._count_root_payload_nodes = MainWindow._count_root_payload_nodes.__get__(window, MainWindow)
        window._count_expandable_tree_nodes = lambda panel_key: 12
        window._count_visible_destination_future_state_nodes = lambda: 4
        window._reset_source_background_preload_state = lambda: None
        applied = []
        window._apply_root_payload_to_tree = lambda panel_key, items: applied.append((panel_key, items))

        payload = {
            "panel_key": "destination",
            "drive_id": "drive-1",
            "items": [{
                "name": "Root",
                "children": [],
            }],
        }

        window.on_root_load_success(payload, "root-1")

        self.assertEqual(applied, [])

    def test_source_shallow_root_payload_does_not_overwrite_richer_visible_tree(self):
        window = MainWindow.__new__(MainWindow)
        window.root_load_workers = {"source": {"id": "root-1", "request_signature": {"panel_key": "source"}}}
        window.pending_root_drive_ids = {"source": "drive-1"}
        window._memory_restore_in_progress = False
        window._log_restore_phase = lambda *args, **kwargs: None
        window._log_worker_lifecycle = lambda *args, **kwargs: None
        window._count_root_payload_nodes = MainWindow._count_root_payload_nodes.__get__(window, MainWindow)
        window._count_expandable_tree_nodes = lambda panel_key: 12
        window._reset_source_background_preload_state = lambda: None
        applied = []
        window._apply_root_payload_to_tree = lambda panel_key, items: applied.append((panel_key, items))

        payload = {
            "panel_key": "source",
            "drive_id": "drive-1",
            "items": [{
                "name": "FTBMRoot",
                "children": [],
            }],
        }

        window.on_root_load_success(payload, "root-1")

        self.assertEqual(applied, [])

    def test_destination_shallow_folder_payload_does_not_overwrite_richer_visible_branch(self):
        app = QApplication.instance() or QApplication([])
        window = MainWindow.__new__(MainWindow)
        item = QTreeWidgetItem(["Folder: Finance"])
        item.setData(0, Qt.UserRole, {"item_path": "Root\\Finance", "is_folder": True, "children_loaded": True})
        child = QTreeWidgetItem(["Folder: Payroll"])
        child.setData(0, Qt.UserRole, {"item_path": "Root\\Finance\\Payroll", "is_folder": True, "children_loaded": True})
        item.addChild(child)

        window.pending_folder_loads = {"destination": set()}
        window.folder_load_workers = {"destination:item-1": {"id": "folder-1", "item": item}}
        window._memory_restore_in_progress = False
        window._snapshot_branch_refresh_baseline_by_worker = {}
        window._destination_preserved_children_by_worker = {"destination:item-1": []}
        window._log_worker_lifecycle = lambda *args, **kwargs: None
        window._log_restore_phase = lambda *args, **kwargs: None
        window.normalize_memory_path = MainWindow.normalize_memory_path.__get__(window, MainWindow)
        window._count_visible_subtree_nodes = MainWindow._count_visible_subtree_nodes.__get__(window, MainWindow)
        window._count_folder_payload_nodes = MainWindow._count_folder_payload_nodes.__get__(window, MainWindow)

        payload = {
            "panel_key": "destination",
            "drive_id": "drive-1",
            "item_id": "item-1",
            "items": [],
        }

        window.on_folder_load_success(payload, "folder-1")

        self.assertEqual(item.childCount(), 1)
        self.assertEqual((item.child(0).data(0, Qt.UserRole) or {}).get("item_path"), "Root\\Finance\\Payroll")

    def test_folder_worker_success_skips_deleted_tree_item(self):
        window = MainWindow.__new__(MainWindow)
        worker_key = "destination:item-1"
        window.pending_folder_loads = {"destination": {"drive-1:item-1"}}
        window.folder_load_workers = {worker_key: {"id": "folder-1", "item": _DeletedTreeItemStub()}}
        window._snapshot_branch_refresh_baseline_by_worker = {worker_key: {"Root\\Finance\\Payroll"}}
        window._destination_preserved_children_by_worker = {worker_key: ["kept"]}
        lifecycle = []
        window._log_worker_lifecycle = lambda *args, **kwargs: lifecycle.append((args, kwargs))
        window._tree_item_is_alive = MainWindow._tree_item_is_alive.__get__(window, MainWindow)

        payload = {
            "panel_key": "destination",
            "drive_id": "drive-1",
            "item_id": "item-1",
            "items": [],
        }

        window.on_folder_load_success(payload, "folder-1")

        self.assertEqual(window.pending_folder_loads["destination"], set())
        self.assertNotIn(worker_key, window._snapshot_branch_refresh_baseline_by_worker)
        self.assertNotIn(worker_key, window._destination_preserved_children_by_worker)
        self.assertTrue(any(args[:3] == ("stale_deleted_item_success_skipped", "folder", "folder-1") for args, _kwargs in lifecycle))

    def test_destination_branch_visual_color_uses_family_for_top_level_and_children(self):
        window = MainWindow.__new__(MainWindow)
        window.normalize_memory_path = MainWindow.normalize_memory_path.__get__(window, MainWindow)
        window._path_segments = MainWindow._path_segments.__get__(window, MainWindow)
        window._tree_item_path = MainWindow._tree_item_path.__get__(window, MainWindow)
        window._canonical_destination_projection_path = lambda path: MainWindow.normalize_memory_path(window, path)
        window._destination_branch_visual_context = MainWindow._destination_branch_visual_context.__get__(window, MainWindow)
        window._destination_branch_color_map = MainWindow._destination_branch_color_map.__get__(window, MainWindow)
        window._blend_colors = MainWindow._blend_colors.__get__(window, MainWindow)
        window._destination_branch_visual_color = MainWindow._destination_branch_visual_color.__get__(window, MainWindow)

        top_level_color, top_level_depth = window._destination_branch_visual_color({"destination_path": "Root\\Finance"})
        child_color, child_depth = window._destination_branch_visual_color({"destination_path": "Root\\Finance\\Payroll"})

        self.assertEqual(top_level_depth, 1)
        self.assertEqual(child_depth, 2)
        self.assertEqual(top_level_color.name().lower(), "#7fd36b")
        self.assertNotEqual(child_color.name().lower(), top_level_color.name().lower())

    def test_destination_visual_state_keeps_semantic_foreground_and_branch_background(self):
        app = QApplication.instance() or QApplication([])
        window = MainWindow.__new__(MainWindow)
        window.normalize_memory_path = MainWindow.normalize_memory_path.__get__(window, MainWindow)
        window._path_segments = MainWindow._path_segments.__get__(window, MainWindow)
        window._tree_item_path = MainWindow._tree_item_path.__get__(window, MainWindow)
        window._canonical_destination_projection_path = lambda path: MainWindow.normalize_memory_path(window, path)
        window._destination_branch_visual_context = MainWindow._destination_branch_visual_context.__get__(window, MainWindow)
        window._destination_branch_color_map = MainWindow._destination_branch_color_map.__get__(window, MainWindow)
        window._blend_colors = MainWindow._blend_colors.__get__(window, MainWindow)
        window._destination_branch_visual_color = MainWindow._destination_branch_visual_color.__get__(window, MainWindow)
        window._destination_branch_background_color = MainWindow._destination_branch_background_color.__get__(window, MainWindow)
        window._submitted_visual_state_for_node = lambda node_data: {"submitted": False, "batch_id": ""}
        window.node_is_proposed = lambda node_data: bool(node_data.get("is_proposed"))
        window.node_is_planned_allocation = lambda node_data: bool(node_data.get("is_planned_allocation"))

        item = QTreeWidgetItem(["Folder: Contractor Resumes"])
        node_data = {
            "tree_role": "destination",
            "destination_path": "Root\\HR\\Employee Resumes\\Contractor Resumes",
            "is_planned_allocation": True,
            "base_display_label": "Folder: Contractor Resumes",
        }

        window._apply_tree_item_visual_state = MainWindow._apply_tree_item_visual_state.__get__(window, MainWindow)
        window._apply_tree_item_visual_state(item, node_data)

        self.assertEqual(item.foreground(0).color().name().lower(), "#51e3f6")
        self.assertNotEqual(item.background(0).color().name().lower(), "#000000")

    def test_refresh_tree_ui_after_root_bind_skips_source_materialization_when_snapshot_restored(self):
        window = MainWindow.__new__(MainWindow)
        window._pending_snapshot_branch_refresh = {"source": {"FTBMRoot\\Public"}, "destination": set()}
        window.planned_moves = []
        window.proposed_folders = []
        synced = []
        statuses = []
        window._sync_expand_all_button_from_tree = lambda panel_key, fallback_expanded=False: synced.append((panel_key, fallback_expanded))
        window._set_tree_status_message = lambda panel_key, message, loading=False: statuses.append((panel_key, message, loading))
        window._refresh_source_projection = lambda reason: (_ for _ in ()).throw(AssertionError("should not refresh shallow source projection"))
        window._start_source_restore_materialization = lambda: (_ for _ in ()).throw(AssertionError("should not start source restore materialization"))
        window._schedule_progress_summary_refresh = lambda delay_ms=180: None
        window._log_root_success_step = lambda *args, **kwargs: None

        window._refresh_tree_ui_after_root_bind("source", restored_runtime_snapshot=True)

        self.assertEqual(synced, [("source", False)])
        self.assertEqual(statuses[-1], ("source", "Expanded from local snapshot. Refreshing live content...", True))

    def test_fast_expand_all_persists_workspace_ui_state(self):
        window = MainWindow.__new__(MainWindow)
        window.source_tree_widget = _ExpandTreeStub()
        window.destination_tree_widget = _ExpandTreeStub()
        window._expand_all_pending = {"source": False, "destination": False}
        window._expand_all_deferred_refresh = {"source": False, "destination": False}
        window._reset_expand_all_progress = lambda panel_key: None
        window._set_expand_all_button_label = lambda panel_key, expanded: None
        window.source_tree_status = _LabelStub()
        window.destination_tree_status = _LabelStub()
        window._set_tree_status_message = lambda panel_key, message, loading=False: None
        persisted = []
        window._persist_workspace_ui_state_safely = lambda: persisted.append(True)

        result = window._fast_expand_all_loaded_tree("source")

        self.assertTrue(result)
        self.assertEqual(persisted, [True])

    def test_destination_live_refresh_waits_until_overlay_work_is_finished(self):
        window = MainWindow.__new__(MainWindow)
        window._memory_restore_in_progress = False
        window._restore_destination_overlay_pending = True
        window._destination_restore_materialization_queue = []
        window._destination_idle_materialize_pending_reason = ""
        window._destination_idle_materialize_timer = _TimerStub()
        window._unresolved_proposed_queue_size = lambda: 0
        window._unresolved_allocation_queue_size = lambda: 0

        self.assertTrue(window._destination_live_refresh_still_blocked())

        window._restore_destination_overlay_pending = False
        self.assertFalse(window._destination_live_refresh_still_blocked())

    def test_snapshot_refresh_targets_from_source_snapshot_limits_depth(self):
        window = MainWindow.__new__(MainWindow)
        window._draft_shell_state = SessionState()

        targets = window._snapshot_refresh_targets_from_snapshot(
            "source",
            [{
                "expanded": True,
                "data": {"item_path": "FTBMRoot", "is_folder": True},
                "children": [{
                    "expanded": True,
                    "data": {"item_path": "FTBMRoot\\Public", "is_folder": True},
                    "children": [{
                        "expanded": True,
                        "data": {"item_path": "FTBMRoot\\Public\\Contracts", "is_folder": True},
                        "children": [{
                            "expanded": True,
                            "data": {"item_path": "FTBMRoot\\Public\\Contracts\\Northcote", "is_folder": True},
                            "children": [],
                        }],
                    }],
                }],
            }],
        )

        self.assertEqual(targets, {"FTBMRoot", "FTBMRoot\\Public", "FTBMRoot\\Public\\Contracts"})

    def test_capture_child_path_set_ignores_placeholders(self):
        window = MainWindow.__new__(MainWindow)
        parent = QTreeWidgetItem(["Folder: Parent"])
        real_child = QTreeWidgetItem(["Folder: Real"])
        real_child.setData(0, Qt.UserRole, {"item_path": "Root\\Real"})
        placeholder = QTreeWidgetItem(["Loading"])
        placeholder.setData(0, Qt.UserRole, {"placeholder": True, "item_path": "Root\\Placeholder"})
        parent.addChild(real_child)
        parent.addChild(placeholder)

        self.assertEqual(window._capture_child_path_set(parent), {"Root\\Real"})

    def test_materialize_destination_future_model_defers_for_large_tree_during_expand_all(self):
        window = MainWindow.__new__(MainWindow)
        window.destination_tree_widget = _ExpandTreeStub()
        window._destination_root_prime_pending = False
        window._destination_full_tree_worker = None
        window.pending_root_drive_ids = {"source": "", "destination": ""}
        window._current_selected_destination_drive_id = lambda: ""
        window._destination_full_tree_snapshot = []
        window._destination_full_tree_completed_drive_id = ""
        window._expand_all_pending = {"source": False, "destination": True}
        window.pending_folder_loads = {"source": set(), "destination": set()}
        window._root_tree_bind_in_progress = False
        window._count_expandable_tree_nodes = lambda panel_key: 200
        scheduled = []
        window._schedule_deferred_destination_materialization = lambda reason, delay_ms=180: scheduled.append((reason, delay_ms))
        window._log_restore_phase = lambda *args, **kwargs: None

        applied = window._materialize_destination_future_model("destination_expand_all_complete")

        self.assertEqual(applied, 0)
        self.assertEqual(scheduled, [("destination_expand_all_complete", 180)])

    def test_restore_workspace_tree_panel_state_reapplies_expanded_all(self):
        window = MainWindow.__new__(MainWindow)
        window.destination_tree_widget = QTreeWidget()
        window.destination_expand_all_button = _ButtonStub()
        window._expand_all_pending = {"source": False, "destination": False}
        restored = []
        window._restore_expanded_tree_paths = lambda panel_key, expanded_paths: restored.append(("paths", panel_key, set(expanded_paths)))
        window._restore_selected_tree_path = lambda panel_key, selected_path: restored.append(("selected", panel_key, selected_path))
        window._restore_panel_expanded_all_state = lambda panel_key: restored.append(("expanded_all", panel_key))

        window._restore_workspace_tree_panel_state(
            "destination",
            {
                "destination_expanded_paths": {"Root\\Finance"},
                "destination_selected_path": "Root\\Finance",
                "destination_expanded_all": True,
            },
        )

        self.assertEqual(
            restored,
            [
                ("paths", "destination", {"Root\\Finance"}),
                ("selected", "destination", "Root\\Finance"),
                ("expanded_all", "destination"),
            ],
        )

    def test_handle_expand_all_fast_path_for_loaded_source_tree(self):
        window = MainWindow.__new__(MainWindow)
        tree = _ExpandTreeStub()
        root = QTreeWidgetItem(["Folder: Root"])
        root.setData(
            0,
            Qt.UserRole,
            {
                "name": "Root",
                "is_folder": True,
                "children_loaded": True,
                "load_failed": False,
            },
        )
        tree.addTopLevelItem(root)

        window.source_tree_widget = tree
        window.destination_tree_widget = _ExpandTreeStub()
        window.source_tree_status = _LabelStub()
        window.destination_tree_status = _LabelStub()
        window._sharepoint_lazy_mode = True
        window._source_background_preload_pending = False
        window.pending_folder_loads = {"source": set(), "destination": set()}
        window._expand_all_pending = {"source": False, "destination": False}
        window._expand_all_queue = {"source": [], "destination": []}
        window._expand_all_seen = {"source": set(), "destination": set()}
        window._expand_all_deferred_refresh = {"source": False, "destination": False}
        window._set_expand_all_button_label = lambda panel_key, expanded: setattr(window, "_expand_label", (panel_key, expanded))
        window._set_tree_status_message = lambda panel_key, message, loading=False: setattr(window, "_expand_status", (panel_key, message, loading))

        window.handle_expand_all("source")

        self.assertTrue(tree.expanded_all)
        self.assertEqual(window._expand_label, ("source", True))
        self.assertEqual(window._expand_status, ("source", "All branches expanded.", False))

    def test_begin_source_model_expand_all_seeds_queue(self):
        model = SharePointSourceTreeModel()
        model.reset_root_payloads(
            [
                {
                    "name": "Lib",
                    "is_folder": True,
                    "id": "item-root",
                    "library_id": "drive-1",
                    "children_loaded": False,
                    "base_display_label": "Folder: Lib",
                    "tree_role": "source",
                }
            ]
        )
        tree = QTreeView()
        tree.setModel(model)
        window = _main_window_stub_for_source_model_expand_all(tree, model)
        tick_calls = []
        window._source_model_expand_all_tick = lambda: tick_calls.append(1)

        window._begin_source_model_expand_all()

        self.assertTrue(window._expand_all_pending["source"])
        self.assertEqual(tick_calls, [1])
        self.assertEqual(len(window._expand_all_source_model_queue), 1)
        self.assertTrue(window._expand_all_source_model_queue[0].isValid())
        self.assertIn("drive-1:item-root", window._expand_all_seen["source"])

    def test_source_model_expand_all_finishes_without_graph_for_loaded_subtree(self):
        model = SharePointSourceTreeModel()
        model.reset_root_payloads(
            [
                {
                    "name": "Root",
                    "is_folder": True,
                    "id": "r1",
                    "library_id": "d1",
                    "children_loaded": True,
                    "base_display_label": "Folder: Root",
                    "tree_role": "source",
                }
            ]
        )
        root_ix = model.index(0, 0, QModelIndex())
        model.replace_all_children(
            root_ix,
            [
                {
                    "name": "Sub",
                    "is_folder": True,
                    "id": "s1",
                    "library_id": "d1",
                    "children_loaded": True,
                    "base_display_label": "Folder: Sub",
                    "tree_role": "source",
                }
            ],
        )
        sub_ix = model.index(0, 0, root_ix)
        model.replace_all_children(sub_ix, [])
        model.update_payload_for_index(sub_ix, lambda p: p.update({"children_loaded": True}))

        tree = QTreeView()
        tree.setModel(model)
        window = _main_window_stub_for_source_model_expand_all(tree, model)
        graph_load_attempts = []
        window._source_model_request_folder_children_load = (
            lambda *a, **k: graph_load_attempts.append(1) or "noop"
        )

        with patch.object(QTimer, "singleShot", lambda _ms, _cb: None):
            window._begin_source_model_expand_all()

        self.assertEqual(len(window._expand_all_source_model_queue), 1)
        self.assertTrue(window._expand_all_pending["source"])

        window._source_model_expand_all_tick()

        self.assertFalse(window._expand_all_pending["source"])
        self.assertEqual(len(window._expand_all_source_model_queue), 0)
        self.assertEqual(graph_load_attempts, [])
        self.assertTrue(tree.isExpanded(root_ix))
        self.assertTrue(tree.isExpanded(sub_ix))

    def test_process_expand_all_queue_starts_async_source_loads(self):
        window = MainWindow.__new__(MainWindow)
        tree = _ExpandTreeStub()
        root = QTreeWidgetItem(["Folder: Root"])
        root.setData(
            0,
            Qt.UserRole,
            {
                "name": "Root",
                "item_path": "FTBMRoot",
                "is_folder": True,
                "children_loaded": False,
                "load_failed": False,
            },
        )
        tree.addTopLevelItem(root)

        window.source_tree_widget = tree
        window.destination_tree_widget = _ExpandTreeStub()
        window.source_tree_status = _LabelStub()
        window.destination_tree_status = _LabelStub()
        window.pending_folder_loads = {"source": set(), "destination": set()}
        window._expand_all_pending = {"source": True, "destination": False}
        window._expand_all_queue = {"source": deque([root]), "destination": deque()}
        window._expand_all_processed = {"source": 0, "destination": 0}
        window._expand_all_deferred_refresh = {"source": False, "destination": False}
        window._expand_all_max_per_tick = {"source": 1, "destination": 2}
        scheduled = []
        started = []
        window._count_expandable_tree_nodes = lambda panel_key: 10
        window._ensure_tree_item_load_started = lambda panel_key, item: started.append(
            (panel_key, item.data(0, Qt.UserRole).get("item_path"))
        ) or True
        window._update_expand_all_status = lambda panel_key, message, loading=False: scheduled.append((panel_key, message, loading))

        window._process_expand_all_queue("source")

        self.assertEqual(started, [("source", "FTBMRoot")])
        self.assertTrue(window._expand_all_pending["source"])
        self.assertEqual(scheduled[-1], ("source", "Expanding branches...", True))

    def test_fast_expand_path_skips_folders_with_lazy_placeholder_children(self):
        window = MainWindow.__new__(MainWindow)
        tree = _ExpandTreeStub()
        root = QTreeWidgetItem(["Folder: Root"])
        root.setData(
            0,
            Qt.UserRole,
            {
                "name": "Root",
                "is_folder": True,
                "children_loaded": True,
                "load_failed": False,
                "id": "root-id",
            },
        )
        placeholder = QTreeWidgetItem(["Expand to load contents"])
        placeholder.setData(0, Qt.UserRole, {"placeholder": True})
        root.addChild(placeholder)
        tree.addTopLevelItem(root)

        window.source_tree_widget = tree
        window.destination_tree_widget = _ExpandTreeStub()
        window.pending_root_drive_ids = {"source": "drive-1", "destination": ""}

        self.assertTrue(window._tree_has_unloaded_folder_nodes("source"))

    def test_expand_all_status_shows_progress_counts(self):
        window = MainWindow.__new__(MainWindow)
        window._expand_all_processed = {"source": 3, "destination": 0}
        window._expand_all_seen = {"source": {1, 2, 3, 4, 5}, "destination": set()}

        self.assertEqual(
            window._expand_all_progress_text("source", "Expanding branches..."),
            "Expanding branches... (3/5 folders)",
        )

    def test_projected_descendant_folder_node_is_preloaded(self):
        window = MainWindow.__new__(MainWindow)
        source_node_data = {
            "name": "Hawthorn",
            "real_name": "Hawthorn",
            "is_folder": True,
            "item_path": "FTBMRoot\\Migrated Unstructured Data\\Documents\\YMCA\\Hawthorn",
        }
        parent_data = {
            "drive_id": "drive",
            "site_id": "site",
            "site_name": "Aquatic Cleaning Solutions",
            "library_id": "lib",
            "library_name": "Documents",
            "web_url": "https://example.invalid",
        }

        node_data = window._build_destination_allocation_descendant_node_data(
            source_node_data,
            "Root\\Projects Completed\\YMCA\\Hawthorn",
            parent_data,
        )

        self.assertTrue(node_data["planned_allocation_descendant"])
        self.assertTrue(node_data["children_loaded"])
        self.assertEqual(node_data["node_origin"], "ProjectedAllocationDescendant")

    def test_projected_descendant_lazy_reload_is_skipped(self):
        window = MainWindow.__new__(MainWindow)
        node_data = {
            "name": "Hawthorn",
            "is_folder": True,
            "children_loaded": False,
            "planned_allocation_descendant": True,
            "item_path": "Root\\Projects Completed\\YMCA\\Hawthorn",
        }
        item = QTreeWidgetItem(["Folder: Hawthorn"])
        item.setData(0, Qt.UserRole, node_data)
        item.addChild(QTreeWidgetItem(["File: child.txt"]))

        log_calls = []
        window.destination_tree_widget = _TreeStub()
        window.proposed_folders = []
        window.planned_moves = []
        window._log_restore_phase = lambda phase, **data: log_calls.append((phase, data))
        window._find_planned_move_for_destination_node = lambda _: (_ for _ in ()).throw(AssertionError("should not resolve move"))

        window._load_destination_projected_descendants(item)

        updated = item.data(0, Qt.UserRole)
        self.assertTrue(updated["children_loaded"])
        self.assertEqual(log_calls[0][0], "destination_projected_descendant_lazy_load_skipped")

    def test_rewrite_proposed_branch_runtime_paths_moves_branch_and_allocations(self):
        window = MainWindow.__new__(MainWindow)
        window.proposed_folders = [
            ProposedFolder(
                FolderName="Follow Up",
                DestinationPath="Root\\Sales\\Follow Up",
                ParentPath="Root\\Sales",
            ),
            ProposedFolder(
                FolderName="Nested",
                DestinationPath="Root\\Sales\\Follow Up\\Nested",
                ParentPath="Root\\Sales\\Follow Up",
            ),
        ]
        window.planned_moves = [
            {
                "destination_path": "Root\\Sales\\Follow Up\\Salary Increases.xlsx",
                "destination": {
                    "display_path": "Root\\Sales\\Follow Up\\Salary Increases.xlsx",
                    "item_path": "Root\\Sales\\Follow Up\\Salary Increases.xlsx",
                    "destination_path": "Root\\Sales\\Follow Up\\Salary Increases.xlsx",
                },
            }
        ]

        window._rewrite_proposed_branch_runtime_paths(
            "Root\\Sales\\Follow Up",
            "Root\\Management\\Follow Up",
        )

        self.assertEqual(window.proposed_folders[0].DestinationPath, "Root\\Management\\Follow Up")
        self.assertEqual(window.proposed_folders[0].ParentPath, "Root\\Management")
        self.assertEqual(window.proposed_folders[1].DestinationPath, "Root\\Management\\Follow Up\\Nested")
        self.assertEqual(window.proposed_folders[1].ParentPath, "Root\\Management\\Follow Up")
        self.assertEqual(
            window.planned_moves[0]["destination_path"],
            "Root\\Management\\Follow Up\\Salary Increases.xlsx",
        )

    def test_destination_branch_move_to_top_level_folder_is_not_treated_as_same_path(self):
        window = MainWindow.__new__(MainWindow)
        window.proposed_folders = [
            ProposedFolder(
                FolderName="Test",
                DestinationPath="Root\\Test",
                ParentPath="Root",
            )
        ]
        window.planned_moves = []
        window.destination_tree_status = _LabelStub()
        window.destination_tree_widget = _ExpandTreeStub()
        window._proposed_branch_contains_submitted_items = lambda path: False
        window._find_proposed_folder_record_by_path = lambda path: None
        window._show_submitted_item_locked_message = lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unexpected lock")
        )
        window._persist_planning_change_lightweight = lambda: setattr(window, "_persisted", True)
        window.on_tree_selection_changed = lambda panel_key: setattr(window, "_selection_panel", panel_key)

        source_item = QTreeWidgetItem(["Folder: Test"])
        source_item.setData(
            0,
            Qt.UserRole,
            {
                "name": "Test",
                "real_name": "Test",
                "display_path": "Root\\Test",
                "item_path": "Root\\Test",
                "destination_path": "Root\\Test",
                "tree_role": "destination",
                "is_folder": True,
                "proposed": True,
                "node_origin": "Proposed",
            },
        )
        target_item = QTreeWidgetItem(["Folder: Management"])
        target_item.setData(
            0,
            Qt.UserRole,
            {
                "name": "Management",
                "real_name": "Management",
                "display_path": "Root\\Management",
                "item_path": "Root\\Management",
                "destination_path": "Root\\Management",
                "tree_role": "destination",
                "is_folder": True,
                "node_origin": "Real",
            },
        )
        window.destination_tree_widget.addTopLevelItem(source_item)
        window.destination_tree_widget.addTopLevelItem(target_item)

        window.handle_destination_draft_move(source_item, target_item)

        self.assertEqual(window.proposed_folders[0].DestinationPath, "Root\\Management\\Test")
        self.assertEqual(window.proposed_folders[0].ParentPath, "Root\\Management")
        self.assertEqual(target_item.childCount(), 1)
        self.assertEqual(target_item.child(0).data(0, Qt.UserRole).get("display_path"), "Root\\Management\\Test")
        self.assertTrue(window._persisted)

    def test_move_planned_destination_node_updates_parent_path(self):
        window = MainWindow.__new__(MainWindow)
        move = {
            "source_name": "Salary Increases.xlsx",
            "target_name": "Salary Increases.xlsx",
            "source_path": "FTBMRoot\\Follow Up\\Salary Increases.xlsx",
            "destination_path": "Root\\Sales\\Follow Up",
            "destination_id": "dest-1",
            "destination_name": "Follow Up",
            "destination": {
                "id": "dest-1",
                "name": "Follow Up",
                "display_path": "Root\\Sales\\Follow Up",
                "item_path": "Root\\Sales\\Follow Up",
                "destination_path": "Root\\Sales\\Follow Up",
            },
            "status": "Draft",
        }
        window.planned_moves = [move]
        window.destination_tree_status = _LabelStub()
        window.planned_moves_status = _LabelStub()
        window._resolve_planned_move_for_destination_node = lambda node: (0, move, None)
        window._is_move_submitted = lambda current_move: False
        window._find_visible_destination_item_by_path = lambda path: None
        window._persist_planning_change = lambda reason: setattr(window, "_persist_reason", reason)

        moved = window._move_planned_destination_node(
            {"name": "Salary Increases.xlsx", "display_path": "Root\\Sales\\Follow Up\\Salary Increases.xlsx"},
            {"name": "Management", "display_path": "Root\\Management", "item_path": "Root\\Management", "is_folder": True},
        )

        self.assertTrue(moved)
        self.assertEqual(move["destination_path"], "Root\\Management")
        self.assertEqual(move["destination"]["display_path"], "Root\\Management")
        self.assertEqual(window._persist_reason, "planned_item_moved")

    def test_persist_planning_change_lightweight_queues_deferred_refresh(self):
        window = MainWindow.__new__(MainWindow)
        window.destination_tree_widget = _TreeStub()
        window.source_tree_widget = _TreeStub()
        window._save_draft_shell = lambda force=False: setattr(window, "_saved_force", force)
        window._rebuild_submission_visual_cache = lambda: setattr(window, "_cache_rebuilt", True)
        window._collect_current_source_projection_paths = lambda: {"FTBMRoot", "FTBMRoot\\Contracts"}
        window._queue_deferred_planning_refresh = (
            lambda reason, source_projection_paths=None, delay_ms=None: setattr(
                window,
                "_queued_refresh",
                (reason, set(source_projection_paths or set()), delay_ms),
            )
        )
        window.update_progress_summaries = lambda: setattr(window, "_progress_updated", True)
        window._log_restore_exception = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected exception"))

        window._persist_planning_change_lightweight()

        self.assertTrue(window._saved_force)
        self.assertTrue(window._cache_rebuilt)
        self.assertEqual(
            window._queued_refresh,
            ("planning_change_lightweight", {"FTBMRoot", "FTBMRoot\\Contracts"}, None),
        )

    def test_flush_deferred_planning_refresh_materializes_and_clears_title_status(self):
        window = MainWindow.__new__(MainWindow)
        window.destination_tree_widget = _TreeStub()
        window.source_tree_widget = _TreeStub()
        window._deferred_planning_refresh_pending = True
        window._deferred_planning_refresh_reasons = ["move_folder"]
        window._deferred_source_projection_paths = {"FTBMRoot\\Contracts"}
        window._set_window_title_status = lambda status_text="": setattr(window, "_title_status", status_text)
        window._materialize_destination_future_model = lambda reason: setattr(window, "_destination_materialize_reason", reason)
        window._schedule_source_projection_refresh_for_paths = (
            lambda paths, reason, delay_ms=250, trigger_path="": setattr(
                window,
                "_scheduled_source_refresh",
                (set(paths), reason, delay_ms),
            )
        )
        window.update_progress_summaries = lambda: setattr(window, "_progress_updated", True)
        window._log_restore_exception = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected exception"))

        window._run_deferred_planning_refresh()

        self.assertFalse(window._deferred_planning_refresh_pending)
        self.assertEqual(window._destination_materialize_reason, "deferred_move_folder")
        self.assertEqual(
            window._scheduled_source_refresh,
            ({"FTBMRoot\\Contracts"}, "source_projection_deferred_move_folder", 50),
        )
        self.assertEqual(window._title_status, "")

    def test_finalize_cache_refresh_workspace_restore_reapplies_destination_overlay(self):
        window = MainWindow.__new__(MainWindow)
        window.destination_tree_widget = _TreeStub()
        window.source_tree_widget = _TreeStub()
        window._pending_cache_refresh_ui_state = {
            "source_expanded_paths": {"FTBMRoot"},
            "destination_expanded_paths": {"Root\\Management"},
            "source_selected_path": "FTBMRoot",
            "destination_selected_path": "Root\\Management",
        }
        window._pending_cache_refresh_panels = {"destination"}
        window._cache_refresh_restore_active = True
        window._cache_refresh_skip_expanded_restore_panels = set()
        window._restore_workspace_tree_state = lambda ui_state: setattr(window, "_restored_ui_state", ui_state)
        window._materialize_destination_future_model = lambda reason: setattr(window, "_cache_refresh_materialize_reason", reason)
        window._start_destination_restore_materialization = lambda: setattr(window, "_destination_materialization_started", True)
        window._refresh_source_projection = lambda reason: setattr(window, "_source_projection_reason", reason)
        window._schedule_progress_summary_refresh = lambda delay_ms=180: setattr(window, "_progress_refresh_scheduled", delay_ms)
        window._log_restore_exception = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected exception"))

        window._finalize_cache_refresh_workspace_restore()

        self.assertEqual(window._cache_refresh_materialize_reason, "cache_refresh_restore_complete")
        self.assertTrue(window._destination_materialization_started)
        self.assertEqual(window._source_projection_reason, "source_projection_cache_refresh_restore")
        self.assertFalse(window._cache_refresh_restore_active)
        self.assertIsNone(window._pending_cache_refresh_ui_state)
        self.assertEqual(window._pending_cache_refresh_panels, set())

    def test_destination_root_folder_load_error_falls_back_to_future_model(self):
        window = MainWindow.__new__(MainWindow)
        root_item = QTreeWidgetItem(["Folder: Root"])
        root_item.setData(
            0,
            Qt.UserRole,
            {
                "name": "Root",
                "display_path": "Root",
                "item_path": "Root",
                "destination_path": "Root",
                "tree_role": "destination",
                "is_folder": True,
            },
        )
        worker_state = {"id": 3, "item": root_item}
        window.folder_load_workers = {"destination:item-1": worker_state}
        window.pending_folder_loads = {"destination": {"drive-1:item-1"}, "source": set()}
        window._destination_preserved_children_by_worker = {}
        window._snapshot_branch_refresh_baseline_by_worker = {}
        window._destination_semantic_path = lambda node_data: str(node_data.get("item_path", ""))
        window._destination_root_prime_pending = True
        window._pending_snapshot_branch_refresh = {"source": set(), "destination": set()}
        window._set_tree_status_message = lambda panel_key, message, loading=False: setattr(
            window,
            "_last_tree_status",
            (panel_key, message, loading),
        )
        window._materialize_destination_future_model = lambda reason: setattr(window, "_fallback_materialize_reason", reason)
        window._start_destination_restore_materialization = lambda: setattr(window, "_fallback_restore_started", True)
        window.destination_tree_widget = _TreeStub()
        window._schedule_progress_summary_refresh = lambda delay_ms=180: setattr(window, "_progress_refresh_delay", delay_ms)
        window._log_restore_exception = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected exception"))
        window._log_worker_lifecycle = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected lifecycle"))

        window.on_folder_load_error(
            {"panel_key": "destination", "drive_id": "drive-1", "item_id": "item-1"},
            3,
        )

        updated = root_item.data(0, Qt.UserRole)
        self.assertFalse(window._destination_root_prime_pending)
        self.assertEqual(window._last_tree_status[0], "destination")
        self.assertEqual(window._fallback_materialize_reason, "destination_root_error_fallback")
        self.assertTrue(window._fallback_restore_started)
        self.assertTrue(updated["load_failed"])

    def test_cache_refresh_root_error_restores_last_visible_tree_snapshot(self):
        window = MainWindow.__new__(MainWindow)
        window.source_tree_widget = QTreeWidget()
        window.destination_tree_widget = QTreeWidget()
        window.source_tree_status = _LabelStub()
        window.destination_tree_status = _LabelStub()

        existing_item = QTreeWidgetItem(["Folder: Existing Source"])
        existing_item.setData(
            0,
            Qt.UserRole,
            {
                "name": "Existing Source",
                "display_path": "FTBMRoot",
                "item_path": "FTBMRoot",
                "tree_role": "source",
                "is_folder": True,
            },
        )
        window.source_tree_widget.addTopLevelItem(existing_item)

        worker_state = {"id": 9}
        window.root_load_workers = {"source": worker_state}
        window.pending_root_drive_ids = {"source": "drive-source", "destination": ""}
        window._cache_refresh_restore_active = True
        window._pending_cache_refresh_panels = {"source"}
        window._pending_cache_refresh_ui_state = {"source_expanded_paths": {"FTBMRoot"}}
        window._pending_cache_refresh_tree_snapshots = {
            "source": window._capture_tree_items_snapshot("source"),
        }
        window._reset_full_count_state = lambda: setattr(window, "_full_count_reset", True)
        window._refresh_planning_loading_banner = lambda: setattr(window, "_banner_refreshed", True)
        window._log_restore_phase = lambda *args, **kwargs: None
        window._log_restore_exception = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected exception"))
        window._log_worker_lifecycle = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected lifecycle"))

        window.on_root_load_error({"panel_key": "source", "drive_id": "drive-source"}, 9)

        self.assertEqual(window.source_tree_widget.topLevelItemCount(), 1)
        self.assertEqual(window.source_tree_widget.topLevelItem(0).text(0), "Folder: Existing Source")
        self.assertIn("last loaded source content", window.source_tree_status.text)
        self.assertFalse(window._cache_refresh_restore_active)
        self.assertEqual(window._pending_cache_refresh_tree_snapshots, {})

    def test_real_destination_refresh_preserves_explicit_proposed_node_identity(self):
        window = MainWindow.__new__(MainWindow)
        window.proposed_folders = [
            ProposedFolder(
                FolderName="Projects Completed",
                DestinationPath="Root\\Projects\\Projects Completed",
                ParentPath="Root\\Projects",
                Status="Draft",
            )
        ]

        node = window._upsert_destination_model_node(
            {},
            "Root\\Projects\\Projects Completed",
            name="Projects Completed",
            node_state="real",
            data={
                "name": "Projects Completed",
                "real_name": "Projects Completed",
                "display_path": "Root\\Projects\\Projects Completed",
                "item_path": "Root\\Projects\\Projects Completed",
                "destination_path": "Root\\Projects\\Projects Completed",
                "tree_role": "destination",
                "is_folder": True,
                "children_loaded": True,
            },
            parent_semantic_path="Root\\Projects",
        )

        self.assertEqual(node["node_state"], "proposed")
        self.assertTrue(node["data"]["proposed"])
        self.assertEqual(node["data"]["node_origin"], "Proposed")

    def test_handle_unlock_draft_clears_submitted_flags(self):
        window = MainWindow.__new__(MainWindow)
        move = {
            "status": "Submitted",
            "submitted_batch_id": "SUB-001",
            "submitted_utc": "2026-03-25T10:00:00",
        }
        proposed = ProposedFolder(
            FolderName="Recovered",
            DestinationPath="Root\\Management\\Recovered",
            ParentPath="Root\\Management",
            Status="Submitted",
        )
        setattr(proposed, "SubmittedBatchId", "SUB-001")

        window.planned_moves = [move]
        window.proposed_folders = [proposed]
        window.planned_moves_status = _LabelStub()
        window._rebuild_submission_visual_cache = lambda: setattr(window, "_cache_rebuilt", True)
        window.refresh_planned_moves_table = lambda: setattr(window, "_table_refreshed", True)
        window._persist_planning_change = lambda reason: setattr(window, "_persist_reason", reason)

        with patch("ozlink_console.main_window.QMessageBox.question", return_value=QMessageBox.Yes), patch(
            "ozlink_console.main_window.QMessageBox.information"
        ) as info_mock:
            window._handle_unlock_draft()

        self.assertEqual(move["status"], "Draft")
        self.assertNotIn("submitted_batch_id", move)
        self.assertNotIn("submitted_utc", move)
        self.assertEqual(proposed.Status, "Draft")
        self.assertFalse(hasattr(proposed, "SubmittedBatchId"))
        self.assertTrue(window._cache_rebuilt)
        self.assertTrue(window._table_refreshed)
        self.assertEqual(window._persist_reason, "draft_unlocked")
        self.assertIn("Unlocked 1 move(s) and 1 proposed folder(s)", window.planned_moves_status.text)
        self.assertEqual(info_mock.call_count, 1)

    def test_session_keepalive_starts_and_stops_with_session_state(self):
        window = MainWindow.__new__(MainWindow)
        timer = _TimerStub()
        window._session_keepalive_timer = timer
        window.session_keepalive_worker = None
        window.current_session_context = {"connected": True}
        window._log_restore_exception = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected exception"))

        window._start_session_keepalive()
        self.assertTrue(timer.isActive())
        self.assertEqual(timer.started, 1)

        window.current_session_context = {"connected": False}
        window._stop_session_keepalive()
        self.assertFalse(timer.isActive())
        self.assertEqual(timer.stopped, 1)

    def test_destination_expand_all_remains_immediately_cancellable_while_preloading(self):
        window = MainWindow.__new__(MainWindow)
        window.source_tree_widget = _ExpandTreeStub()
        window.destination_tree_widget = _ExpandTreeStub()
        window.source_tree_status = _LabelStub()
        window.destination_tree_status = _LabelStub()
        window.destination_expand_all_button = _ButtonStub()
        window._expand_all_pending = {"source": False, "destination": False}
        window._expand_all_queue = {"source": deque(), "destination": deque()}
        window._expand_all_seen = {"source": set(), "destination": set()}
        window._expand_all_deferred_refresh = {"source": False, "destination": False}
        window._expand_all_max_per_tick = {"source": 1, "destination": 2}
        window._expand_all_status_last_update_ms = {"source": 0, "destination": 0}
        window._expand_all_timers = {}
        window._destination_expand_all_after_full_tree = False
        window._destination_root_prime_pending = False
        window.pending_root_drive_ids = {"destination": ""}
        window.planned_moves = []
        window._sharepoint_lazy_mode = False
        window._current_selected_destination_drive_id = lambda: "drive-123"
        window._destination_full_tree_ready = lambda: False
        window.start_destination_full_tree_worker = lambda drive_id: setattr(window, "_started_drive_id", drive_id)
        window._set_tree_status_message = lambda panel_key, message, loading=False: setattr(
            window,
            "_last_status",
            (panel_key, message, loading),
        )
        window._can_fast_bulk_expand = lambda panel_key: False
        window._set_expand_all_button_label = lambda panel_key, expanded: window.destination_expand_all_button.setText(
            "Collapse All" if expanded else "Expand All"
        )
        window._persist_workspace_ui_state_safely = lambda: None
        window._refresh_runtime_tree_snapshot = lambda *args, **kwargs: None
        window._workspace_ui_snapshot_dirty_panels = set()
        window.node_is_planned_allocation = lambda node_data: False

        window.handle_expand_all("destination")

        self.assertTrue(window._expand_all_pending["destination"])
        self.assertTrue(window._destination_expand_all_after_full_tree)
        self.assertEqual(window.destination_expand_all_button.text(), "Collapse All")
        self.assertTrue(window.destination_expand_all_button.enabled)
        self.assertEqual(window._started_drive_id, "drive-123")

        window.handle_expand_all("destination")

        self.assertFalse(window._expand_all_pending["destination"])
        self.assertFalse(window._destination_expand_all_after_full_tree)
        self.assertEqual(window.destination_expand_all_button.text(), "Expand All")
        self.assertTrue(window.destination_tree_widget.collapsed)
        self.assertEqual(window._last_status, ("destination", "Expand cancelled; branches collapsed.", False))


if __name__ == "__main__":
    unittest.main()
