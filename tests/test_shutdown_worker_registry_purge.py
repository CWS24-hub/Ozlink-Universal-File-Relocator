"""Shutdown: worker registry purge + orphan QThread retention (lifecycle contract tests)."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from ozlink_console import main_window as mwmod
from ozlink_console.main_window import MainWindow


class ShutdownWorkerRegistryPurgeTests(unittest.TestCase):
    def test_purge_removes_root_and_folder_worker_refs(self):
        w = MainWindow.__new__(MainWindow)
        a = object()
        b = object()
        w.root_load_workers = {"source": {"id": 1, "worker": a, "panel_key": "source"}}
        w.folder_load_workers = {"source:x": {"id": 2, "worker": b, "worker_key": "source:x"}}
        self.assertEqual(MainWindow._purge_worker_from_shutdown_registries(w, a), 1)
        self.assertEqual(MainWindow._purge_worker_from_shutdown_registries(w, b), 1)
        self.assertEqual(w.root_load_workers, {})
        self.assertEqual(w.folder_load_workers, {})

    def test_orphan_finalize_removes_from_module_hold_list(self):
        holder: list = []
        worker = MagicMock()
        with patch.object(mwmod, "_SHUTDOWN_ORPHAN_QTHREADS", holder):
            holder.append(worker)
            mwmod._shutdown_orphan_qthread_finalize(worker)
            self.assertEqual(holder, [])
            worker.deleteLater.assert_called_once()

    def test_shutdown_workers_orphan_path_disconnects_and_retains(self):
        w = MainWindow.__new__(MainWindow)
        thr = MagicMock()
        thr.isRunning.return_value = True
        thr.disconnect = MagicMock()
        thr.finished = MagicMock()
        thr.finished.connect = MagicMock(return_value=None)
        w.root_load_workers = {"destination": {"id": 9, "worker": thr, "panel_key": "destination"}}
        w.folder_load_workers = {}
        w.root_load_retired_workers = {}
        w.folder_load_retired_workers = {}
        w._drive_delta_workers = {}
        w._retired_preview_workers = {}
        w._retired_destination_full_tree_workers = {}
        w._retired_full_count_workers = {}
        holder: list = []
        with patch.object(mwmod, "_SHUTDOWN_ORPHAN_QTHREADS", holder):
            with patch.object(MainWindow, "_purge_worker_from_shutdown_registries", return_value=1):
                with patch.object(MainWindow, "_shutdown_worker_safe_join"):
                    with patch("ozlink_console.main_window._shiboken_is_valid", return_value=True):
                        MainWindow._shutdown_running_workers_for_close(w)
            self.assertIn(thr, holder)
        thr.disconnect.assert_called()
        thr.finished.connect.assert_called()


if __name__ == "__main__":
    unittest.main()
