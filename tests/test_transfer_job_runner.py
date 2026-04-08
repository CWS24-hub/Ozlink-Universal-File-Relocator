import json
import unittest
from pathlib import Path

import requests

from ozlink_console.transfer_job_runner import (
    is_absolute_local_path,
    load_manifest_json,
    manifest_execution_summary,
    run_manifest_local_filesystem,
    validate_manifest,
)


class TransferJobRunnerTests(unittest.TestCase):
    def test_is_absolute_local_path(self):
        self.assertTrue(is_absolute_local_path(r"C:\temp\a.txt"))
        self.assertTrue(is_absolute_local_path(r"\\srv\share\a"))
        self.assertTrue(is_absolute_local_path("/tmp/a"))
        self.assertFalse(is_absolute_local_path(r"Lib\Root\a.txt"))
        self.assertFalse(is_absolute_local_path(""))

    def test_validate_manifest_version(self):
        errs = validate_manifest({"manifest_version": 3, "transfer_steps": []})
        self.assertTrue(any("Unsupported" in e for e in errs))

    def test_validate_manifest_ok(self):
        self.assertEqual(
            validate_manifest({"manifest_version": 1, "transfer_steps": [], "proposed_folder_steps": []}),
            [],
        )

    def test_run_copy_file_dry_run(self):
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            src = td / "a.txt"
            src.write_text("x", encoding="utf-8")
            dst = td / "b.txt"
            manifest = {
                "manifest_version": 1,
                "kind": "simulation",
                "transfer_steps": [
                    {
                        "index": 0,
                        "operation": "copy",
                        "source_path": str(src),
                        "destination_path": str(dst),
                        "source_name": "a.txt",
                        "destination_name": "b.txt",
                        "is_source_folder": False,
                        "request_id": "",
                        "status": "Draft",
                        "allocation_method": "",
                    }
                ],
                "proposed_folder_steps": [],
            }
            r = run_manifest_local_filesystem(manifest, dry_run=True)
            self.assertTrue(any(x.status == "dry_run" for x in r.records))

    def test_pilot_filter_by_step_index(self):
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            src = td / "a.txt"
            src.write_text("x", encoding="utf-8")
            dst = td / "b.txt"
            manifest = {
                "manifest_version": 1,
                "kind": "simulation",
                "transfer_steps": [
                    {
                        "index": 0,
                        "operation": "copy",
                        "source_path": str(src),
                        "destination_path": str(dst),
                        "source_name": "a.txt",
                        "destination_name": "b.txt",
                        "is_source_folder": False,
                        "request_id": "",
                        "status": "Draft",
                        "allocation_method": "",
                        "step_uid": "REQ::0",
                    }
                ],
                "proposed_folder_steps": [],
                "execution_options": {"pilot_transfer_step_indices": [999]},
            }
            r = run_manifest_local_filesystem(manifest, dry_run=False)
            self.assertTrue(any("pilot_transfer_step_indices filter" in x.detail for x in r.records))
            self.assertTrue(any("planned move 0 not selected" in x.detail for x in r.records))

    def test_pilot_filter_by_step_uid(self):
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            src = td / "a.txt"
            src.write_text("x", encoding="utf-8")
            dst = td / "b.txt"
            manifest = {
                "manifest_version": 1,
                "kind": "simulation",
                "transfer_steps": [
                    {
                        "index": 0,
                        "operation": "copy",
                        "source_path": str(src),
                        "destination_path": str(dst),
                        "source_name": "a.txt",
                        "destination_name": "b.txt",
                        "is_source_folder": False,
                        "request_id": "",
                        "status": "Draft",
                        "allocation_method": "",
                        "step_uid": "REQ::0",
                    }
                ],
                "proposed_folder_steps": [],
                "execution_options": {"pilot_transfer_step_uids": ["REQ::1"]},
            }
            r = run_manifest_local_filesystem(manifest, dry_run=False)
            self.assertTrue(any("pilot_transfer_step_uids filter" in x.detail for x in r.records))
            self.assertFalse(dst.exists())

    def test_run_copy_file_execute(self):
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            src = td / "a.txt"
            src.write_text("hello", encoding="utf-8")
            dst = td / "out" / "b.txt"
            manifest = {
                "manifest_version": 1,
                "transfer_steps": [
                    {
                        "index": 0,
                        "operation": "copy",
                        "source_path": str(src),
                        "destination_path": str(dst),
                        "source_name": "a.txt",
                        "destination_name": "b.txt",
                        "is_source_folder": False,
                        "request_id": "",
                        "status": "Draft",
                        "allocation_method": "",
                    }
                ],
                "proposed_folder_steps": [],
            }
            logf = td / "run.log"
            r = run_manifest_local_filesystem(manifest, dry_run=False, log_file=logf)
            self.assertTrue(dst.is_file())
            self.assertEqual(dst.read_text(encoding="utf-8"), "hello")
            self.assertTrue(logf.is_file())
            self.assertEqual(sum(1 for x in r.records if x.status == "ok"), 1)
            tr = [x for x in r.records if x.phase == "transfer"][0]
            self.assertTrue(tr.integrity_verified)
            self.assertEqual(len(tr.source_sha256), 64)
            self.assertEqual(tr.source_sha256, tr.dest_sha256)
            self.assertTrue(r.job_id)
            audit = logf.with_suffix(".audit.jsonl")
            self.assertTrue(audit.is_file())
            rep = logf.parent / f"{logf.stem}_report.json"
            self.assertTrue(rep.is_file())

    def test_run_copy_file_execute_without_integrity_check(self):
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            src = td / "a.txt"
            src.write_text("hello", encoding="utf-8")
            dst = td / "out" / "b.txt"
            manifest = {
                "manifest_version": 1,
                "execution_options": {"verify_integrity": False},
                "transfer_steps": [
                    {
                        "index": 0,
                        "operation": "copy",
                        "source_path": str(src),
                        "destination_path": str(dst),
                        "source_name": "a.txt",
                        "destination_name": "b.txt",
                        "is_source_folder": False,
                        "request_id": "",
                        "status": "Draft",
                        "allocation_method": "",
                    }
                ],
                "proposed_folder_steps": [],
            }
            r = run_manifest_local_filesystem(manifest, dry_run=False)
            self.assertTrue(dst.is_file())
            tr = [x for x in r.records if x.phase == "transfer"][0]
            self.assertIsNone(tr.integrity_verified)

    def test_skip_sharepoint_style_paths(self):
        manifest = {
            "manifest_version": 1,
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "FTBMRoot\\Public\\f.txt",
                    "destination_path": "Root\\HR\\f.txt",
                    "source_name": "f.txt",
                    "destination_name": "f.txt",
                    "is_source_folder": False,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                }
            ],
            "proposed_folder_steps": [],
        }
        r = run_manifest_local_filesystem(manifest, dry_run=False)
        self.assertTrue(any("SharePoint-style paths" in x.detail or "Graph ids" in x.detail for x in r.records))

    def test_graph_ids_skip_without_signed_in_client(self):
        manifest = {
            "manifest_version": 1,
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\a",
                    "destination_path": "Root\\b",
                    "source_name": "a",
                    "destination_name": "b",
                    "is_source_folder": False,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "i1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent-folder-id",
                }
            ],
            "proposed_folder_steps": [],
        }
        r = run_manifest_local_filesystem(manifest, dry_run=False, graph_client=None)
        self.assertTrue(
            any("sign-in" in x.detail.lower() or "graph client" in x.detail.lower() for x in r.records)
        )

    def test_graph_copy_ok_when_monitor_poll_401_but_destination_verified_by_path(self):
        class _GraphMonitor401:
            def start_drive_item_copy(self, **kwargs):
                return "https://graph.microsoft.com/v1.0/monitor/fake"

            def wait_graph_async_operation(self, monitor, timeout_sec=600.0):
                r = requests.Response()
                r.status_code = 401
                err = requests.HTTPError("401 Client Error: Unauthorized", response=r)
                raise err

            def get_drive_item_by_path(self, drive_id, relative_path):
                p = str(relative_path or "").replace("\\", "/")
                if "GOPR0307.JPG" in p:
                    return {"id": "dest-item-1", "name": "GOPR0307.JPG"}
                return None

        manifest = {
            "manifest_version": 2,
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "is_source_folder": False,
                    "source_name": "GOPR0307.JPG",
                    "destination_name": "GOPR0307.JPG",
                    "source_path": "",
                    "destination_path": "Root/Personal/100GOPRO/GOPR0307.JPG",
                    "source_drive_id": "src-drive",
                    "source_item_id": "src-item",
                    "destination_drive_id": "dst-drive",
                    "destination_item_id": "parent-folder-id",
                }
            ],
            "proposed_folder_steps": [],
        }
        r = run_manifest_local_filesystem(manifest, dry_run=False, graph_client=_GraphMonitor401())
        tr = [x for x in r.records if x.phase == "transfer"][0]
        self.assertEqual(tr.status, "ok")
        self.assertIn("401", tr.detail)
        self.assertIn("verified", tr.detail.lower())

    def test_graph_copy_401_does_not_recover_when_destination_name_mismatches(self):
        """REGRESSION: recovery requires resolved item name to match expected leaf (no false ok on wrong file)."""

        class _Graph401WrongName:
            def start_drive_item_copy(self, **kwargs):
                return "https://graph.microsoft.com/v1.0/monitor/fake"

            def wait_graph_async_operation(self, monitor, timeout_sec=600.0):
                r = requests.Response()
                r.status_code = 401
                raise requests.HTTPError("401", response=r)

            def get_drive_item_by_path(self, drive_id, relative_path):
                if "GOPR0307.JPG" in str(relative_path).upper():
                    return {"id": "x", "name": "OTHER.JPG"}
                return None

        manifest = {
            "manifest_version": 2,
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "is_source_folder": False,
                    "source_name": "GOPR0307.JPG",
                    "destination_name": "GOPR0307.JPG",
                    "source_path": "",
                    "destination_path": "Root/Personal/100GOPRO/GOPR0307.JPG",
                    "source_drive_id": "src-drive",
                    "source_item_id": "src-item",
                    "destination_drive_id": "dst-drive",
                    "destination_item_id": "parent-folder-id",
                }
            ],
            "proposed_folder_steps": [],
        }
        r = run_manifest_local_filesystem(manifest, dry_run=False, graph_client=_Graph401WrongName())
        tr = [x for x in r.records if x.phase == "transfer"][0]
        self.assertEqual(tr.status, "failed")

    def test_graph_copy_still_fails_when_monitor_401_and_destination_missing(self):
        class _GraphMonitor401NoDest:
            def start_drive_item_copy(self, **kwargs):
                return "https://graph.microsoft.com/v1.0/monitor/fake"

            def wait_graph_async_operation(self, monitor, timeout_sec=600.0):
                r = requests.Response()
                r.status_code = 401
                raise requests.HTTPError("401", response=r)

            def get_drive_item_by_path(self, drive_id, relative_path):
                return None

        manifest = {
            "manifest_version": 2,
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "is_source_folder": False,
                    "source_name": "missing.txt",
                    "destination_name": "missing.txt",
                    "source_path": "",
                    "destination_path": "Root/Personal/missing.txt",
                    "source_drive_id": "src-drive",
                    "source_item_id": "src-item",
                    "destination_drive_id": "dst-drive",
                    "destination_item_id": "parent-folder-id",
                }
            ],
            "proposed_folder_steps": [],
        }
        r = run_manifest_local_filesystem(manifest, dry_run=False, graph_client=_GraphMonitor401NoDest())
        tr = [x for x in r.records if x.phase == "transfer"][0]
        self.assertEqual(tr.status, "failed")

    def test_pilot_max_graph_operations_caps_live_graph_steps(self):
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        mock_g.start_drive_item_copy.return_value = "https://monitor"
        mock_g.wait_graph_async_operation.return_value = None
        mock_g.create_child_folder.return_value = None

        manifest = {
            "manifest_version": 1,
            "execution_options": {"pilot_max_graph_operations": 2},
            "proposed_folder_steps": [
                {
                    "index": 0,
                    "operation": "ensure_folder",
                    "folder_name": "A",
                    "destination_path": "Site / Lib / A",
                    "destination_drive_id": "d1",
                    "destination_parent_item_id": "p1",
                },
                {
                    "index": 1,
                    "operation": "ensure_folder",
                    "folder_name": "B",
                    "destination_path": "Site / Lib / B",
                    "destination_drive_id": "d1",
                    "destination_parent_item_id": "p1",
                },
            ],
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\a",
                    "destination_path": "Root\\b",
                    "source_name": "a",
                    "destination_name": "b",
                    "is_source_folder": False,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "i1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                },
                {
                    "index": 1,
                    "operation": "copy",
                    "source_path": "Lib\\c",
                    "destination_path": "Root\\d",
                    "source_name": "c",
                    "destination_name": "d",
                    "is_source_folder": False,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "i2",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent2",
                },
            ],
        }
        r = run_manifest_local_filesystem(manifest, dry_run=False, graph_client=mock_g)
        self.assertEqual(mock_g.create_child_folder.call_count, 2)
        self.assertEqual(mock_g.start_drive_item_copy.call_count, 0)
        skipped = [x for x in r.records if x.status == "skipped" and "pilot_max" in x.detail]
        self.assertEqual(len(skipped), 2)

    def test_pilot_max_spans_proposed_then_transfer(self):
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        mock_g.start_drive_item_copy.return_value = "https://monitor"
        mock_g.wait_graph_async_operation.return_value = None
        mock_g.create_child_folder.return_value = None

        manifest = {
            "manifest_version": 1,
            "execution_options": {"pilot_max_graph_operations": 2},
            "proposed_folder_steps": [
                {
                    "index": 0,
                    "operation": "ensure_folder",
                    "folder_name": "A",
                    "destination_path": "Site / Lib / A",
                    "destination_drive_id": "d1",
                    "destination_parent_item_id": "p1",
                },
            ],
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\a",
                    "destination_path": "Root\\b",
                    "source_name": "a",
                    "destination_name": "b",
                    "is_source_folder": False,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "i1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                },
                {
                    "index": 1,
                    "operation": "copy",
                    "source_path": "Lib\\c",
                    "destination_path": "Root\\d",
                    "source_name": "c",
                    "destination_name": "d",
                    "is_source_folder": False,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "i2",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent2",
                },
            ],
        }
        r = run_manifest_local_filesystem(manifest, dry_run=False, graph_client=mock_g)
        self.assertEqual(mock_g.create_child_folder.call_count, 1)
        self.assertEqual(mock_g.start_drive_item_copy.call_count, 1)
        skipped = [x for x in r.records if x.status == "skipped" and "pilot_max" in x.detail]
        self.assertEqual(len(skipped), 1)

    def test_pilot_limit_ignored_for_dry_run(self):
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        manifest = {
            "manifest_version": 1,
            "execution_options": {"pilot_max_graph_operations": 1},
            "proposed_folder_steps": [
                {
                    "index": 0,
                    "operation": "ensure_folder",
                    "folder_name": "A",
                    "destination_path": "Site / Lib / A",
                    "destination_drive_id": "d1",
                    "destination_parent_item_id": "p1",
                },
            ],
            "transfer_steps": [],
        }
        r = run_manifest_local_filesystem(manifest, dry_run=True, graph_client=mock_g)
        mock_g.create_child_folder.assert_not_called()
        self.assertTrue(any(x.status == "dry_run" for x in r.records))

    def test_pilot_caps_even_when_graph_copy_fails(self):
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        mock_g.start_drive_item_copy.side_effect = Exception("boom")
        mock_g.wait_graph_async_operation.return_value = None
        mock_g.create_child_folder.return_value = None

        manifest = {
            "manifest_version": 1,
            "execution_options": {"pilot_max_graph_operations": 1},
            "proposed_folder_steps": [],
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\a",
                    "destination_path": "Root\\b",
                    "source_name": "a",
                    "destination_name": "b",
                    "is_source_folder": False,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "i1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                },
                {
                    "index": 1,
                    "operation": "copy",
                    "source_path": "Lib\\c",
                    "destination_path": "Root\\d",
                    "source_name": "c",
                    "destination_name": "d",
                    "is_source_folder": False,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "i2",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent2",
                },
            ],
        }

        r = run_manifest_local_filesystem(manifest, dry_run=False, graph_client=mock_g)
        self.assertEqual(mock_g.start_drive_item_copy.call_count, 1)

        failed = [x for x in r.records if x.phase == "transfer" and x.status == "failed"]
        self.assertEqual(len(failed), 1)

        skipped = [x for x in r.records if x.phase == "transfer" and x.status == "skipped"]
        self.assertEqual(len(skipped), 1)
        self.assertIn("pilot_max_graph_operations reached", skipped[0].detail)

    def test_pilot_proposed_folder_name_filter(self):
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        mock_g.start_drive_item_copy.return_value = "https://monitor"
        mock_g.wait_graph_async_operation.return_value = None
        mock_g.create_child_folder.return_value = None

        manifest = {
            "manifest_version": 1,
            "execution_options": {"pilot_max_graph_operations": 2, "pilot_proposed_folder_name": "Active Clients"},
            "proposed_folder_steps": [
                {
                    "index": 0,
                    "operation": "ensure_folder",
                    "folder_name": "Employee Resumes",
                    "destination_path": "Site / Lib / Employee Resumes",
                    "destination_drive_id": "d1",
                    "destination_parent_item_id": "p1",
                },
                {
                    "index": 1,
                    "operation": "ensure_folder",
                    "folder_name": "Active Clients",
                    "destination_path": "Site / Lib / Active Clients",
                    "destination_drive_id": "d1",
                    "destination_parent_item_id": "p1",
                },
                {
                    "index": 2,
                    "operation": "ensure_folder",
                    "folder_name": "Stationery",
                    "destination_path": "Site / Lib / Stationery",
                    "destination_drive_id": "d1",
                    "destination_parent_item_id": "p1",
                },
            ],
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\a",
                    "destination_path": "Root\\b",
                    "source_name": "a",
                    "destination_name": "b",
                    "is_source_folder": False,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "i1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                }
            ],
        }

        r = run_manifest_local_filesystem(manifest, dry_run=False, graph_client=mock_g)
        self.assertEqual(mock_g.create_child_folder.call_count, 1)
        self.assertEqual(mock_g.create_child_folder.call_args[0][2], "Active Clients")
        self.assertEqual(mock_g.start_drive_item_copy.call_count, 1)
        self.assertTrue(any(x.status == "skipped" for x in r.records if x.phase == "proposed_folder"))

    def test_graph_copy_dry_run_does_not_call_graph(self):
        from unittest.mock import MagicMock

        manifest = {
            "manifest_version": 1,
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\a",
                    "destination_path": "Root\\b",
                    "source_name": "a",
                    "destination_name": "b.txt",
                    "is_source_folder": False,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "i1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                }
            ],
            "proposed_folder_steps": [],
        }
        mock_g = MagicMock()
        r = run_manifest_local_filesystem(manifest, dry_run=True, graph_client=mock_g)
        mock_g.start_drive_item_copy.assert_not_called()
        tr = [x for x in r.records if x.phase == "transfer"][0]
        self.assertEqual(tr.status, "dry_run")

    def test_graph_folder_skipped_when_embedded_unsafe_index(self):
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        mock_g.start_drive_item_copy.return_value = "https://monitor"
        mock_g.wait_graph_async_operation.return_value = None

        manifest = {
            "manifest_version": 2,
            "proposed_folder_steps": [],
            "execution_options": {"graph_unsafe_folder_step_indices": [0]},
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\Folder",
                    "destination_path": "Root\\Folder",
                    "source_name": "Folder",
                    "destination_name": "Folder",
                    "is_source_folder": True,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "fold1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                },
            ],
        }
        r = run_manifest_local_filesystem(manifest, dry_run=False, graph_client=mock_g)
        mock_g.start_drive_item_copy.assert_not_called()
        skipped = [x for x in r.records if x.phase == "transfer" and x.status == "skipped"]
        self.assertEqual(len(skipped), 1)
        self.assertIn("Graph folder copy blocked", skipped[0].detail)

    def test_graph_folder_runs_when_embedded_empty_list(self):
        """Embedded [] means the manifest was built with precise dirty detection: no unsafe folder steps."""
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        mock_g.start_drive_item_copy.return_value = "https://monitor"
        mock_g.wait_graph_async_operation.return_value = None

        manifest = {
            "manifest_version": 2,
            "proposed_folder_steps": [],
            "execution_options": {"graph_unsafe_folder_step_indices": []},
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\Folder",
                    "destination_path": "Root\\Folder",
                    "source_name": "Folder",
                    "destination_name": "Folder",
                    "is_source_folder": True,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "fold1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                },
            ],
        }
        run_manifest_local_filesystem(manifest, dry_run=False, graph_client=mock_g)
        mock_g.start_drive_item_copy.assert_called_once()

    def test_graph_folder_skipped_fallback_when_file_step_under_folder(self):
        """Without embedded indices, conservative fallback blocks Graph folder copy if a file step nests under it."""
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        mock_g.start_drive_item_copy.return_value = "https://monitor"
        mock_g.wait_graph_async_operation.return_value = None

        manifest = {
            "manifest_version": 2,
            "proposed_folder_steps": [],
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\Folder",
                    "destination_path": "Root\\Folder",
                    "source_name": "Folder",
                    "destination_name": "Folder",
                    "is_source_folder": True,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "fold1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                },
                {
                    "index": 1,
                    "operation": "copy",
                    "source_path": "Lib\\Folder\\a.txt",
                    "destination_path": "Root\\Folder",
                    "source_name": "a.txt",
                    "destination_name": "a.txt",
                    "is_source_folder": False,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "file1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent2",
                },
            ],
        }
        r = run_manifest_local_filesystem(manifest, dry_run=False, graph_client=mock_g)
        self.assertEqual(mock_g.start_drive_item_copy.call_count, 1)
        folder_rec = next(x for x in r.records if x.phase == "transfer" and x.step_index == 0)
        self.assertEqual(folder_rec.status, "skipped")
        self.assertIn("Graph folder copy blocked", folder_rec.detail)

    def test_graph_expanded_transfer_steps_runs_copy_without_folder_block(self):
        """Expanded list runs Graph copies; unsafe folder row in transfer_steps does not duplicate Graph."""
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        mock_g.start_drive_item_copy.return_value = "https://monitor"
        mock_g.wait_graph_async_operation.return_value = None

        manifest = {
            "manifest_version": 2,
            "proposed_folder_steps": [],
            "execution_options": {
                "graph_unsafe_folder_step_indices": [0],
                "graph_expanded_transfer_steps": [
                    {
                        "index": 0,
                        "operation": "copy",
                        "source_path": "Lib\\Folder\\a.txt",
                        "destination_path": "Root\\Dest",
                        "source_name": "a.txt",
                        "destination_name": "a.txt",
                        "is_source_folder": False,
                        "request_id": "REQ",
                        "status": "Draft",
                        "allocation_method": "",
                        "step_uid": "REQ::1",
                        "planned_move_index": 1,
                        "source_drive_id": "d1",
                        "source_item_id": "i1",
                        "destination_drive_id": "d2",
                        "destination_item_id": "parent1",
                    }
                ],
            },
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\Folder",
                    "destination_path": "Root\\Folder",
                    "source_name": "Folder",
                    "destination_name": "Folder",
                    "is_source_folder": True,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "fold1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                },
            ],
        }
        run_manifest_local_filesystem(manifest, dry_run=False, graph_client=mock_g)
        mock_g.start_drive_item_copy.assert_called_once()

    def test_pilot_indices_expanded_run_all_rows_for_same_planned_move(self):
        """Selecting planned move 0 runs every expanded step derived from it (not only row index 0)."""
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        mock_g.start_drive_item_copy.return_value = "https://monitor"
        mock_g.wait_graph_async_operation.return_value = None

        manifest = {
            "manifest_version": 2,
            "proposed_folder_steps": [],
            "execution_options": {
                "pilot_transfer_step_indices": [0],
                "graph_expanded_transfer_steps": [
                    {
                        "index": 0,
                        "operation": "copy",
                        "source_path": "Lib\\Folder\\a.txt",
                        "destination_path": "Root\\Dest",
                        "source_name": "a.txt",
                        "destination_name": "a.txt",
                        "is_source_folder": False,
                        "request_id": "REQ",
                        "status": "Draft",
                        "allocation_method": "",
                        "planned_move_index": 0,
                        "source_drive_id": "d1",
                        "source_item_id": "i1",
                        "destination_drive_id": "d2",
                        "destination_item_id": "p1",
                    },
                    {
                        "index": 1,
                        "operation": "copy",
                        "source_path": "Lib\\Folder\\b.txt",
                        "destination_path": "Root\\Dest",
                        "source_name": "b.txt",
                        "destination_name": "b.txt",
                        "is_source_folder": False,
                        "request_id": "REQ",
                        "status": "Draft",
                        "allocation_method": "",
                        "planned_move_index": 0,
                        "source_drive_id": "d1",
                        "source_item_id": "i2",
                        "destination_drive_id": "d2",
                        "destination_item_id": "p1",
                    },
                ],
            },
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\Folder",
                    "destination_path": "Root\\Folder",
                    "source_name": "Folder",
                    "destination_name": "Folder",
                    "is_source_folder": True,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "fold1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                },
            ],
        }
        run_manifest_local_filesystem(manifest, dry_run=False, graph_client=mock_g)
        self.assertEqual(mock_g.start_drive_item_copy.call_count, 2)

    def test_pilot_indices_expanded_skip_when_planned_move_not_selected(self):
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        mock_g.start_drive_item_copy.return_value = "https://monitor"
        mock_g.wait_graph_async_operation.return_value = None

        manifest = {
            "manifest_version": 2,
            "proposed_folder_steps": [],
            "execution_options": {
                "pilot_transfer_step_indices": [999],
                "graph_expanded_transfer_steps": [
                    {
                        "index": 0,
                        "operation": "copy",
                        "source_path": "Lib\\Folder\\a.txt",
                        "destination_path": "Root\\Dest",
                        "source_name": "a.txt",
                        "destination_name": "a.txt",
                        "is_source_folder": False,
                        "request_id": "REQ",
                        "status": "Draft",
                        "allocation_method": "",
                        "planned_move_index": 0,
                        "source_drive_id": "d1",
                        "source_item_id": "i1",
                        "destination_drive_id": "d2",
                        "destination_item_id": "p1",
                    },
                ],
            },
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\Folder",
                    "destination_path": "Root\\Folder",
                    "source_name": "Folder",
                    "destination_name": "Folder",
                    "is_source_folder": True,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "fold1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                },
            ],
        }
        r = run_manifest_local_filesystem(manifest, dry_run=False, graph_client=mock_g)
        mock_g.start_drive_item_copy.assert_not_called()
        self.assertTrue(any("planned move 0 not selected" in x.detail for x in r.records))

    def test_pilot_mixed_expanded_and_legacy_local_respects_planned_move_key(self):
        """Expanded steps use planned_move_index; legacy rows without it still use step index."""
        import tempfile
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        mock_g.start_drive_item_copy.return_value = "https://monitor"
        mock_g.wait_graph_async_operation.return_value = None

        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            src = td / "local.txt"
            src.write_text("z", encoding="utf-8")
            dst = td / "out" / "local.txt"

            manifest = {
                "manifest_version": 2,
                "proposed_folder_steps": [],
                "execution_options": {
                    "pilot_transfer_step_indices": [0],
                    "graph_expanded_transfer_steps": [
                        {
                            "index": 0,
                            "operation": "copy",
                            "source_path": "Lib\\Folder\\a.txt",
                            "destination_path": "Root\\Dest",
                            "source_name": "a.txt",
                            "destination_name": "a.txt",
                            "is_source_folder": False,
                            "request_id": "REQ",
                            "status": "Draft",
                            "allocation_method": "",
                            "planned_move_index": 0,
                            "source_drive_id": "d1",
                            "source_item_id": "i1",
                            "destination_drive_id": "d2",
                            "destination_item_id": "p1",
                        },
                    ],
                },
                "transfer_steps": [
                    {
                        "index": 0,
                        "operation": "copy",
                        "source_path": "Lib\\Folder",
                        "destination_path": "Root\\Folder",
                        "source_name": "Folder",
                        "destination_name": "Folder",
                        "is_source_folder": True,
                        "request_id": "",
                        "status": "Draft",
                        "allocation_method": "",
                        "source_drive_id": "d1",
                        "source_item_id": "fold1",
                        "destination_drive_id": "d2",
                        "destination_item_id": "parent1",
                    },
                    {
                        "index": 1,
                        "operation": "copy",
                        "source_path": str(src),
                        "destination_path": str(dst),
                        "source_name": "local.txt",
                        "destination_name": "local.txt",
                        "is_source_folder": False,
                        "request_id": "",
                        "status": "Draft",
                        "allocation_method": "",
                    },
                ],
            }
            r = run_manifest_local_filesystem(manifest, dry_run=False, graph_client=mock_g)
            mock_g.start_drive_item_copy.assert_called_once()
            self.assertFalse(dst.exists())
            self.assertTrue(any("planned move 1 not selected" in x.detail for x in r.records))

    def test_pilot_uids_expanded_shared_uid_runs_all_derived_steps(self):
        """Multiple expanded rows share the same step_uid (planned move); pilot UID selects all."""
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        mock_g.start_drive_item_copy.return_value = "https://monitor"
        mock_g.wait_graph_async_operation.return_value = None

        manifest = {
            "manifest_version": 2,
            "proposed_folder_steps": [],
            "execution_options": {
                "pilot_transfer_step_uids": ["REQ::0"],
                "graph_expanded_transfer_steps": [
                    {
                        "index": 0,
                        "operation": "copy",
                        "source_path": "Lib\\a.txt",
                        "destination_path": "Root\\D",
                        "source_name": "a.txt",
                        "destination_name": "a.txt",
                        "is_source_folder": False,
                        "request_id": "REQ",
                        "status": "Draft",
                        "allocation_method": "",
                        "step_uid": "REQ::0",
                        "planned_move_index": 0,
                        "source_drive_id": "d1",
                        "source_item_id": "i1",
                        "destination_drive_id": "d2",
                        "destination_item_id": "p1",
                    },
                    {
                        "index": 1,
                        "operation": "copy",
                        "source_path": "Lib\\b.txt",
                        "destination_path": "Root\\D",
                        "source_name": "b.txt",
                        "destination_name": "b.txt",
                        "is_source_folder": False,
                        "request_id": "REQ",
                        "status": "Draft",
                        "allocation_method": "",
                        "step_uid": "REQ::0",
                        "planned_move_index": 0,
                        "source_drive_id": "d1",
                        "source_item_id": "i2",
                        "destination_drive_id": "d2",
                        "destination_item_id": "p1",
                    },
                ],
            },
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\Folder",
                    "destination_path": "Root\\Folder",
                    "source_name": "Folder",
                    "destination_name": "Folder",
                    "is_source_folder": True,
                    "request_id": "REQ",
                    "status": "Draft",
                    "allocation_method": "",
                    "step_uid": "REQ::0",
                    "source_drive_id": "d1",
                    "source_item_id": "fold1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                },
            ],
        }
        run_manifest_local_filesystem(manifest, dry_run=False, graph_client=mock_g)
        self.assertEqual(mock_g.start_drive_item_copy.call_count, 2)

    def test_summary_counts(self):
        m = {
            "manifest_version": 1,
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": r"C:\a",
                    "destination_path": r"D:\b",
                    "source_drive_id": "",
                    "source_item_id": "",
                    "destination_drive_id": "",
                }
            ],
            "proposed_folder_steps": [{"operation": "ensure_folder", "destination_path": r"E:\nf"}],
        }
        s = manifest_execution_summary(m)
        self.assertEqual(s["local_filesystem_transfer"], 1)
        self.assertEqual(s["local_mkdir"], 1)
        self.assertEqual(s.get("graph_folder_create", 0), 0)

    def test_summary_counts_graph_proposed_folder(self):
        m = {
            "manifest_version": 1,
            "transfer_steps": [],
            "proposed_folder_steps": [
                {
                    "index": 0,
                    "operation": "ensure_folder",
                    "folder_name": "New",
                    "destination_path": "Site / Lib / A / New",
                    "parent_path": "",
                    "status": "Proposed",
                    "destination_drive_id": "drive-1",
                    "destination_parent_item_id": "parent-1",
                }
            ],
        }
        s = manifest_execution_summary(m)
        self.assertEqual(s["graph_folder_create"], 1)
        self.assertEqual(s["local_mkdir"], 0)
        self.assertEqual(s["proposed_skipped_non_local"], 0)

    def test_copytree_respects_plan_leaf_exclusions_and_direct_file_steps(self):
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            src_f = td / "src" / "F"
            src_f.mkdir(parents=True)
            (src_f / "keep.txt").write_text("keep", encoding="utf-8")
            (src_f / "ex.txt").write_text("bad", encoding="utf-8")
            (src_f / "direct.txt").write_text("direct", encoding="utf-8")
            dst_f = td / "dst" / "F"
            dst_alt = td / "dst" / "alt"
            dst_alt.mkdir(parents=True)

            manifest = {
                "manifest_version": 2,
                "kind": "simulation",
                "transfer_steps": [
                    {
                        "index": 0,
                        "operation": "copy",
                        "source_path": str(src_f),
                        "destination_path": str(dst_f),
                        "source_name": "F",
                        "destination_name": "F",
                        "is_source_folder": True,
                        "request_id": "R0",
                        "status": "Draft",
                        "allocation_method": "",
                        "step_uid": "R0::0",
                    },
                    {
                        "index": 1,
                        "operation": "copy",
                        "source_path": str(src_f / "direct.txt"),
                        "destination_path": str(dst_alt),
                        "source_name": "direct.txt",
                        "destination_name": "direct.txt",
                        "is_source_folder": False,
                        "request_id": "R1",
                        "status": "Draft",
                        "allocation_method": "",
                        "step_uid": "R1::1",
                    },
                ],
                "proposed_folder_steps": [],
                "execution_options": {
                    "verify_integrity": False,
                    "plan_leaf_exclusions": [r"F\ex.txt"],
                },
            }
            r = run_manifest_local_filesystem(manifest, dry_run=False, verify_integrity=False)
            self.assertTrue(any(x.status == "ok" for x in r.records))
            self.assertTrue((dst_f / "keep.txt").exists())
            self.assertFalse((dst_f / "ex.txt").exists())
            self.assertTrue((dst_alt / "direct.txt").exists())
            self.assertEqual((dst_alt / "direct.txt").read_text(encoding="utf-8"), "direct")

    def test_graph_expansion_ignored_when_subset_scoped(self):
        """graph_expanded_transfer_steps must not run under snapshot subset (stale vs scoped manifest)."""
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        mock_g.start_drive_item_copy.return_value = "https://monitor"
        mock_g.wait_graph_async_operation.return_value = None

        manifest = {
            "manifest_version": 2,
            "proposed_folder_steps": [],
            "execution_options": {
                "snapshot_scoped_request_ids": ["REQ-1"],
                "graph_unsafe_folder_step_indices": [0],
                "graph_expanded_transfer_steps": [
                    {
                        "index": 0,
                        "operation": "copy",
                        "source_path": "Lib\\Folder\\a.txt",
                        "destination_path": "Root\\Dest",
                        "source_name": "a.txt",
                        "destination_name": "a.txt",
                        "is_source_folder": False,
                        "request_id": "REQ",
                        "status": "Draft",
                        "allocation_method": "",
                        "step_uid": "REQ::1",
                        "planned_move_index": 1,
                        "source_drive_id": "d1",
                        "source_item_id": "i1",
                        "destination_drive_id": "d2",
                        "destination_item_id": "parent1",
                    }
                ],
            },
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\Folder",
                    "destination_path": "Root\\Folder",
                    "source_name": "Folder",
                    "destination_name": "Folder",
                    "is_source_folder": True,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "fold1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                },
            ],
        }
        r = run_manifest_local_filesystem(manifest, dry_run=False, graph_client=mock_g)
        mock_g.start_drive_item_copy.assert_not_called()
        folder_rec = next(x for x in r.records if x.phase == "transfer" and x.step_index == 0)
        self.assertEqual(folder_rec.status, "skipped")
        self.assertIn("Graph folder copy blocked", folder_rec.detail)

    def test_full_static_plan_still_runs_graph_expanded_transfer_steps(self):
        """Embedded expansion runs when execution context is full static (no subset/recursive/browse/recsub)."""
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        mock_g.start_drive_item_copy.return_value = "https://monitor"
        mock_g.wait_graph_async_operation.return_value = None

        manifest = {
            "manifest_version": 2,
            "proposed_folder_steps": [],
            "execution_options": {
                "graph_unsafe_folder_step_indices": [0],
                "graph_expanded_transfer_steps": [
                    {
                        "index": 0,
                        "operation": "copy",
                        "source_path": "Lib\\Folder\\a.txt",
                        "destination_path": "Root\\Dest",
                        "source_name": "a.txt",
                        "destination_name": "a.txt",
                        "is_source_folder": False,
                        "request_id": "REQ",
                        "status": "Draft",
                        "allocation_method": "",
                        "step_uid": "REQ::1",
                        "planned_move_index": 1,
                        "source_drive_id": "d1",
                        "source_item_id": "i1",
                        "destination_drive_id": "d2",
                        "destination_item_id": "parent1",
                    }
                ],
            },
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\Folder",
                    "destination_path": "Root\\Folder",
                    "source_name": "Folder",
                    "destination_name": "Folder",
                    "is_source_folder": True,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "fold1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                },
            ],
        }
        eo = manifest["execution_options"]
        self.assertNotIn("snapshot_scoped_request_ids", eo)
        self.assertNotIn("snapshot_recursive_subtree", eo)
        self.assertNotIn("snapshot_browsed_recursive", eo)

        run_manifest_local_filesystem(manifest, dry_run=False, graph_client=mock_g)
        mock_g.start_drive_item_copy.assert_called_once()

    def test_graph_expansion_ignored_when_snapshot_recursive_subtree(self):
        """graph_expanded_transfer_steps must not run when snapshot_recursive_subtree is set."""
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        mock_g.start_drive_item_copy.return_value = "https://monitor"
        mock_g.wait_graph_async_operation.return_value = None

        manifest = {
            "manifest_version": 2,
            "proposed_folder_steps": [],
            "execution_options": {
                "snapshot_recursive_subtree": True,
                "graph_unsafe_folder_step_indices": [0],
                "graph_expanded_transfer_steps": [
                    {
                        "index": 0,
                        "operation": "copy",
                        "source_path": "Lib\\Folder\\a.txt",
                        "destination_path": "Root\\Dest",
                        "source_name": "a.txt",
                        "destination_name": "a.txt",
                        "is_source_folder": False,
                        "request_id": "REQ",
                        "status": "Draft",
                        "allocation_method": "",
                        "step_uid": "REQ::1",
                        "planned_move_index": 1,
                        "source_drive_id": "d1",
                        "source_item_id": "i1",
                        "destination_drive_id": "d2",
                        "destination_item_id": "parent1",
                    }
                ],
            },
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\Folder",
                    "destination_path": "Root\\Folder",
                    "source_name": "Folder",
                    "destination_name": "Folder",
                    "is_source_folder": True,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "fold1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                },
            ],
        }
        r = run_manifest_local_filesystem(manifest, dry_run=False, graph_client=mock_g)
        mock_g.start_drive_item_copy.assert_not_called()
        folder_rec = next(x for x in r.records if x.phase == "transfer" and x.step_index == 0)
        self.assertEqual(folder_rec.status, "skipped")
        self.assertIn("Graph folder copy blocked", folder_rec.detail)

    def test_graph_expansion_ignored_when_snapshot_browsed_recursive(self):
        """graph_expanded_transfer_steps must not run when snapshot_browsed_recursive is set."""
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        mock_g.start_drive_item_copy.return_value = "https://monitor"
        mock_g.wait_graph_async_operation.return_value = None

        manifest = {
            "manifest_version": 2,
            "proposed_folder_steps": [],
            "execution_options": {
                "snapshot_browsed_recursive": True,
                "graph_unsafe_folder_step_indices": [0],
                "graph_expanded_transfer_steps": [
                    {
                        "index": 0,
                        "operation": "copy",
                        "source_path": "Lib\\Folder\\a.txt",
                        "destination_path": "Root\\Dest",
                        "source_name": "a.txt",
                        "destination_name": "a.txt",
                        "is_source_folder": False,
                        "request_id": "REQ",
                        "status": "Draft",
                        "allocation_method": "",
                        "step_uid": "REQ::1",
                        "planned_move_index": 1,
                        "source_drive_id": "d1",
                        "source_item_id": "i1",
                        "destination_drive_id": "d2",
                        "destination_item_id": "parent1",
                    }
                ],
            },
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\Folder",
                    "destination_path": "Root\\Folder",
                    "source_name": "Folder",
                    "destination_name": "Folder",
                    "is_source_folder": True,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "fold1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                },
            ],
        }
        r = run_manifest_local_filesystem(manifest, dry_run=False, graph_client=mock_g)
        mock_g.start_drive_item_copy.assert_not_called()
        folder_rec = next(x for x in r.records if x.phase == "transfer" and x.step_index == 0)
        self.assertEqual(folder_rec.status, "skipped")
        self.assertIn("Graph folder copy blocked", folder_rec.detail)

    def test_expansion_disabled_legacy_fallback_blocks_folder_without_unsafe_indices(self):
        """When expansion is off, runner_should_block_graph_folder_copy_fallback still blocks dirty folders."""
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        mock_g.start_drive_item_copy.return_value = "https://monitor"
        mock_g.wait_graph_async_operation.return_value = None

        manifest = {
            "manifest_version": 2,
            "proposed_folder_steps": [],
            "execution_options": {
                "snapshot_recursive_subtree": True,
                "graph_expanded_transfer_steps": [
                    {
                        "index": 0,
                        "operation": "copy",
                        "source_path": "Lib\\Folder\\a.txt",
                        "destination_path": "Root\\Dest",
                        "source_name": "a.txt",
                        "destination_name": "a.txt",
                        "is_source_folder": False,
                        "request_id": "REQ",
                        "status": "Draft",
                        "allocation_method": "",
                        "step_uid": "REQ::1",
                        "planned_move_index": 1,
                        "source_drive_id": "d1",
                        "source_item_id": "i1",
                        "destination_drive_id": "d2",
                        "destination_item_id": "parent1",
                    }
                ],
            },
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\Folder",
                    "destination_path": "Root\\Folder",
                    "source_name": "Folder",
                    "destination_name": "Folder",
                    "is_source_folder": True,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "fold1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                },
                {
                    "index": 1,
                    "operation": "copy",
                    "source_path": "Lib\\Folder\\a.txt",
                    "destination_path": "Root\\Folder",
                    "source_name": "a.txt",
                    "destination_name": "a.txt",
                    "is_source_folder": False,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "file1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent2",
                },
            ],
        }
        self.assertNotIn("graph_unsafe_folder_step_indices", manifest["execution_options"])

        r = run_manifest_local_filesystem(manifest, dry_run=False, graph_client=mock_g)
        mock_g.start_drive_item_copy.assert_called_once()
        folder_rec = next(x for x in r.records if x.phase == "transfer" and x.step_index == 0)
        self.assertEqual(folder_rec.status, "skipped")
        self.assertIn("Graph folder copy blocked", folder_rec.detail)
        file_rec = next(x for x in r.records if x.phase == "transfer" and x.step_index == 1)
        self.assertEqual(file_rec.status, "ok")

    def test_graph_expansion_ignored_when_recsub_runtime_step_present(self):
        """recsub-* request_id rows imply manifest was extended after static expansion was built."""
        from unittest.mock import MagicMock

        mock_g = MagicMock()
        mock_g.start_drive_item_copy.return_value = "https://monitor"
        mock_g.wait_graph_async_operation.return_value = None

        manifest = {
            "manifest_version": 2,
            "proposed_folder_steps": [],
            "execution_options": {
                "graph_unsafe_folder_step_indices": [0],
                "graph_expanded_transfer_steps": [
                    {
                        "index": 0,
                        "operation": "copy",
                        "source_path": "Lib\\Folder\\a.txt",
                        "destination_path": "Root\\Dest",
                        "source_name": "a.txt",
                        "destination_name": "a.txt",
                        "is_source_folder": False,
                        "request_id": "REQ",
                        "status": "Draft",
                        "allocation_method": "",
                        "step_uid": "REQ::1",
                        "planned_move_index": 1,
                        "source_drive_id": "d1",
                        "source_item_id": "i1",
                        "destination_drive_id": "d2",
                        "destination_item_id": "parent1",
                    }
                ],
            },
            "transfer_steps": [
                {
                    "index": 0,
                    "operation": "copy",
                    "source_path": "Lib\\Folder",
                    "destination_path": "Root\\Folder",
                    "source_name": "Folder",
                    "destination_name": "Folder",
                    "is_source_folder": True,
                    "request_id": "",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "fold1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                },
                {
                    "index": 1,
                    "operation": "copy",
                    "source_path": "Lib\\Folder\\from-graph.txt",
                    "destination_path": "Root\\Dest",
                    "source_name": "from-graph.txt",
                    "destination_name": "from-graph.txt",
                    "is_source_folder": False,
                    "request_id": "recsub-deadbeef",
                    "status": "Draft",
                    "allocation_method": "",
                    "source_drive_id": "d1",
                    "source_item_id": "g1",
                    "destination_drive_id": "d2",
                    "destination_item_id": "parent1",
                },
            ],
        }
        r = run_manifest_local_filesystem(manifest, dry_run=False, graph_client=mock_g)
        self.assertEqual(mock_g.start_drive_item_copy.call_count, 1)
        calls = [x for x in r.records if x.phase == "transfer" and x.status == "ok" and x.step_index == 1]
        self.assertTrue(calls)

    def test_round_trip_load_json(self):
        import tempfile

        doc = {"manifest_version": 1, "transfer_steps": [], "proposed_folder_steps": []}
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "m.json"
            p.write_text(json.dumps(doc), encoding="utf-8")
            self.assertEqual(load_manifest_json(p)["manifest_version"], 1)
