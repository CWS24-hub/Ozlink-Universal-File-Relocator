"""Path-level dedupe must participate in session vs sidecar snapshot richness (hybrid)."""

import unittest
from unittest.mock import patch

from ozlink_console.destination_startup_snapshot_roots import (
    DestinationStartupSnapshotRootContext,
    select_validated_destination_startup_snapshot,
)


def _dup_forest(name: str, path_key: str, n_dup: int) -> list:
    """N sibling snapshot nodes with the same data path to simulate duplicate-path inflation."""
    d = {"name": name, "item_path": path_key, "tree_role": "destination", "drive_id": "d1", "is_folder": True}
    d["verification_state"] = "planned_only"
    d["row_kind"] = "planned_folder"
    children: list = []
    for i in range(n_dup):
        children.append(
            {
                "text": f"{name}-{i}",
                "data": dict(d),
                "children": [],
            }
        )
    return [
        {
            "text": "R",
            "data": {
                "name": "R",
                "item_path": "R",
                "tree_role": "destination",
                "drive_id": "d1",
                "is_folder": True,
            },
            "children": children,
        }
    ]


def _single_planned_subtree() -> list:
    d = {
        "name": "A",
        "item_path": "R\\A",
        "tree_role": "destination",
        "drive_id": "d1",
        "is_folder": True,
        "verification_state": "planned_only",
        "row_kind": "planned_folder",
    }
    return [
        {
            "text": "R",
            "data": {
                "name": "R",
                "item_path": "R",
                "tree_role": "destination",
                "drive_id": "d1",
                "is_folder": True,
            },
            "children": [
                {
                    "text": "A",
                    "data": dict(d),
                    "children": [],
                }
            ],
        }
    ]


class PathDedupeStartupSelectionTests(unittest.TestCase):
    def test_tie_after_dedup_prefers_sidecar_when_session_duplicate_inflated(self):
        """
        When both have the same *deduped* size but the session tree is duplicate-inflated pre-dedup,
        prefer the cleaner sidecar (not a larger pre-dedup count from JSON alone).
        """
        ctx = DestinationStartupSnapshotRootContext(
            browse_mode="sharepoint",
            destination_drive_id="d1",
            source_drive_id="src",
        )
        # Session: 8 duplicate "same path" children under one parent -> heavy inflation.
        session = _dup_forest("X", "R\\X", 8)
        # Sidecar: one clean planned row (same effective structure after path dedupe).
        side = _single_planned_subtree()

        with patch("ozlink_console.destination_startup_snapshot_roots.log_info", lambda *a, **k: None):
            chosen, label, n_sess, n_side, meta = select_validated_destination_startup_snapshot(
                session,
                side,
                ctx,
                session_envelope_drive_id="d1",
                session_envelope_library_id="d1",
                sidecar_envelope_drive_id="d1",
                sidecar_envelope_library_id="d1",
                intended_drive_id="d1",
            )
        self.assertEqual(n_sess, n_side)
        self.assertIn("WorkspaceSnapshot", label)
        self.assertEqual(chosen, side)
        self.assertEqual(meta.get("hydration_richness_decision_reason"), "sanitized_tie_prefer_cleaner_path_dedup_sidecar")


if __name__ == "__main__":
    unittest.main()
