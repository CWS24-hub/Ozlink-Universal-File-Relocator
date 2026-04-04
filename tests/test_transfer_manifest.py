import json
import tempfile
import unittest
from pathlib import Path

from ozlink_console.models import ProposedFolder
from ozlink_console.transfer_manifest import (
    build_simulation_manifest,
    upconvert_manifest_v1_to_v2,
    write_manifest_json,
)


class TransferManifestTests(unittest.TestCase):
    def test_empty_manifest(self):
        m = build_simulation_manifest(planned_moves=[], proposed_folders=[], draft_id="D1")
        self.assertEqual(m["manifest_version"], 1)
        self.assertEqual(m["kind"], "simulation")
        self.assertEqual(m["draft_id"], "D1")
        self.assertEqual(m["transfer_steps"], [])
        self.assertEqual(m["proposed_folder_steps"], [])
        self.assertIn("execution_options", m)
        self.assertTrue(m["execution_options"].get("verify_integrity"))
        self.assertNotIn("plan_leaf_exclusions", m["execution_options"])
        self.assertNotIn("graph_unsafe_folder_step_indices", m["execution_options"])

    def test_manifest_embeds_graph_unsafe_folder_step_indices_when_passed(self):
        m = build_simulation_manifest(
            planned_moves=[],
            proposed_folders=[],
            draft_id="D1",
            graph_unsafe_folder_step_indices=[2, 0, 2],
        )
        self.assertEqual(m["execution_options"]["graph_unsafe_folder_step_indices"], [0, 2])

    def test_manifest_embeds_empty_graph_unsafe_list_when_explicit(self):
        m = build_simulation_manifest(
            planned_moves=[],
            proposed_folders=[],
            draft_id="D1",
            graph_unsafe_folder_step_indices=[],
        )
        self.assertEqual(m["execution_options"]["graph_unsafe_folder_step_indices"], [])

    def test_manifest_includes_plan_leaf_exclusions_when_present(self):
        m = build_simulation_manifest(
            planned_moves=[],
            proposed_folders=[],
            draft_id="D1",
            plan_leaf_exclusions=[r"lib\a\x.bin", r"lib\b\y.txt"],
        )
        self.assertEqual(
            m["execution_options"]["plan_leaf_exclusions"],
            [r"lib\a\x.bin", r"lib\b\y.txt"],
        )

    def test_planned_moves_and_proposed_round_trip_file(self):
        moves = [
            {
                "request_id": "R1",
                "status": "Draft",
                "source_path": "Lib\\A\\f.txt",
                "destination_path": "Root\\B\\f.txt",
                "source_name": "f.txt",
                "destination_name": "f.txt",
                "allocation_method": "manual",
                "source": {"name": "f.txt", "is_folder": False},
                "destination": {"name": "f.txt"},
            }
        ]
        proposed = [
            ProposedFolder(FolderName="New", DestinationPath="Root\\New", ParentPath="Root"),
        ]
        doc = build_simulation_manifest(
            planned_moves=moves,
            proposed_folders=proposed,
            draft_id="DRAFT-99",
            tenant_hint="contoso",
            notes="dry run",
        )
        self.assertEqual(len(doc["transfer_steps"]), 1)
        self.assertEqual(doc["transfer_steps"][0]["operation"], "copy")
        self.assertEqual(doc["transfer_steps"][0]["is_source_folder"], False)
        self.assertTrue(str(doc["transfer_steps"][0].get("step_uid", "")).strip())
        self.assertEqual(len(doc["proposed_folder_steps"]), 1)
        self.assertEqual(doc["proposed_folder_steps"][0]["operation"], "ensure_folder")
        self.assertIn("destination_drive_id", doc["proposed_folder_steps"][0])
        self.assertIn("destination_parent_item_id", doc["proposed_folder_steps"][0])

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "m.json"
            write_manifest_json(out, doc)
            loaded = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(loaded["transfer_steps"], doc["transfer_steps"])

    def test_manifest_uses_target_name_as_destination_leaf_for_file_moves(self):
        moves = [
            {
                "request_id": "R2",
                "status": "Draft",
                "source_path": "Lib\\Personal\\IMG_0001.JPG",
                "destination_path": "Root\\Personal\\100GOPRO",
                "source_name": "IMG_0001.JPG",
                # destination_name may still hold the selected destination folder leaf.
                "destination_name": "100GOPRO",
                "target_name": "IMG_0001.JPG",
                "allocation_method": "Manual - Individual batch",
                "source": {"name": "IMG_0001.JPG", "is_folder": False},
                "destination": {"name": "100GOPRO"},
            }
        ]
        doc = build_simulation_manifest(planned_moves=moves, proposed_folders=[], draft_id="D2")
        self.assertEqual(len(doc["transfer_steps"]), 1)
        step = doc["transfer_steps"][0]
        self.assertEqual(step["source_name"], "IMG_0001.JPG")
        self.assertEqual(step["destination_name"], "IMG_0001.JPG")

    def test_upconvert_v1_to_v2_adds_step_uid(self):
        manifest_v1 = {
            "manifest_version": 1,
            "kind": "simulation",
            "transfer_steps": [
                {
                    "index": 3,
                    "operation": "copy",
                    "source_path": "A\\x.txt",
                    "destination_path": "Root\\B\\x.txt",
                    "source_name": "x.txt",
                    "destination_name": "x.txt",
                    "is_source_folder": False,
                    "request_id": "REQ-ABC",
                    "status": "Draft",
                }
            ],
            "proposed_folder_steps": [],
        }
        upgraded, changed = upconvert_manifest_v1_to_v2(manifest_v1)
        self.assertTrue(changed)
        self.assertEqual(upgraded["manifest_version"], 2)
        self.assertEqual(upgraded["transfer_steps"][0]["step_uid"], "REQ-ABC::3")
