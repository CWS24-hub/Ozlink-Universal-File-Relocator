import json
import tempfile
import unittest
from pathlib import Path

from ozlink_console.models import ProposedFolder
from ozlink_console.transfer_manifest import (
    build_simulation_manifest,
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
        self.assertEqual(len(doc["proposed_folder_steps"]), 1)
        self.assertEqual(doc["proposed_folder_steps"][0]["operation"], "ensure_folder")

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "m.json"
            write_manifest_json(out, doc)
            loaded = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(loaded["transfer_steps"], doc["transfer_steps"])
