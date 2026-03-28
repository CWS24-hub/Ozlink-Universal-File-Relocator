import json
import unittest
from pathlib import Path

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
        errs = validate_manifest({"manifest_version": 2, "transfer_steps": []})
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
        self.assertTrue(any("not a local absolute path" in x.detail for x in r.records))

    def test_graph_ids_skip(self):
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
                    "destination_item_id": "",
                }
            ],
            "proposed_folder_steps": [],
        }
        r = run_manifest_local_filesystem(manifest, dry_run=False)
        self.assertTrue(any("Graph" in x.detail for x in r.records))

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

    def test_round_trip_load_json(self):
        import tempfile

        doc = {"manifest_version": 1, "transfer_steps": [], "proposed_folder_steps": []}
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "m.json"
            p.write_text(json.dumps(doc), encoding="utf-8")
            self.assertEqual(load_manifest_json(p)["manifest_version"], 1)
