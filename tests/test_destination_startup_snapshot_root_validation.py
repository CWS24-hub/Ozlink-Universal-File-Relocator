"""Destination startup snapshot root validation, selection, merge, and promoted-path scoping."""

import unittest

from PySide6.QtCore import QModelIndex, Qt

from ozlink_console.destination_startup_snapshot_roots import (
    DestinationStartupSnapshotRootContext,
    allowed_semantic_root_segments_cf_from_snapshot,
    apply_destination_snapshot_identity_gate,
    filter_promoted_semantic_paths_for_destination_roots,
    sanitize_destination_startup_snapshot_top_level,
    select_validated_destination_startup_snapshot,
    snapshot_node_count_recursive,
)
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _root_snap(text: str, *, drive_id: str = "", tree_role: str = "destination", children=None, **extra):
    pl = {"name": text, "tree_role": tree_role, "drive_id": drive_id, **extra}
    return {"text": text, "data": pl, "children": list(children or [])}


class DestinationStartupSnapshotValidationTests(unittest.TestCase):
    def test_prune_foreign_drive_root(self):
        ctx = DestinationStartupSnapshotRootContext(
            browse_mode="sharepoint",
            destination_drive_id="dest-drive-1",
            source_drive_id="src-drive-9",
        )
        good = _root_snap("Root3", drive_id="dest-drive-1")
        bad = _root_snap("OtherLibRoot", drive_id="src-drive-9")
        out, st = sanitize_destination_startup_snapshot_top_level([good, bad], ctx, selection_tag="t")
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["text"], "Root3")
        self.assertGreaterEqual(st.pruned_top_level, 1)

    def test_tree_role_source_pruned(self):
        ctx = DestinationStartupSnapshotRootContext(
            browse_mode="sharepoint",
            destination_drive_id="d1",
            source_drive_id="s1",
        )
        bad = _root_snap("X", drive_id="d1", tree_role="source")
        out, _ = sanitize_destination_startup_snapshot_top_level([bad], ctx, selection_tag="t")
        self.assertEqual(out, [])

    def test_selection_validity_beats_richness(self):
        ctx = DestinationStartupSnapshotRootContext(
            browse_mode="sharepoint",
            destination_drive_id="dest1",
            source_drive_id="src1",
        )
        session = [
            _root_snap("Good", drive_id="dest1"),
            _root_snap("Bad", drive_id="src1"),
            _root_snap("Bad2", drive_id="src1"),
        ]
        sidecar = [
            _root_snap(
                "OnlyGood",
                drive_id="dest1",
                children=[
                    {
                        "text": "child",
                        "data": {"name": "child", "drive_id": "dest1", "tree_role": "destination"},
                        "children": [],
                    }
                ],
            )
        ]
        chosen, label, _, _, meta = select_validated_destination_startup_snapshot(
            session,
            sidecar,
            ctx,
            session_envelope_drive_id="dest1",
            session_envelope_library_id="dest1",
            sidecar_envelope_drive_id="dest1",
            sidecar_envelope_library_id="dest1",
            intended_drive_id="dest1",
        )
        self.assertIn("Workspace", label)
        self.assertEqual(chosen[0]["text"], "OnlyGood")
        # Raw session had more nodes than raw sidecar, but sidecar is richer after both are sanitized.
        self.assertGreaterEqual(int(meta.get("session_raw_nodes", 0)), 3)
        self.assertEqual(int(meta.get("sidecar_raw_nodes", 0)), 2)
        self.assertGreater(int(meta.get("sidecar_sanitized_nodes", 0)), int(meta.get("session_sanitized_nodes", 0)))

    def test_sanitized_tie_prefers_sidecar_when_raw_side_is_richer(self):
        """When sanitized counts match, the workspace sidecar wins if its raw (pre-gate) node count is higher."""
        ctx = DestinationStartupSnapshotRootContext(
            browse_mode="sharepoint",
            destination_drive_id="dest1",
            source_drive_id="src1",
        )
        # Two good roots in session (2 raw, 2 sanitized) vs sidecar with 3 raw nodes
        # (1 root, 1 good child, 1 pruned by nested wrong-drive) → 2 sanitized, 3 raw: tie on
        # sanitized, win sidecar on raw, then on tie-breaker preferring workspace.
        session = [
            _root_snap("S1", drive_id="dest1"),
            _root_snap("S2", drive_id="dest1"),
        ]
        sidecar = [
            _root_snap(
                "B",
                drive_id="dest1",
                children=[
                    {
                        "text": "child",
                        "data": {
                            "name": "child",
                            "drive_id": "dest1",
                            "tree_role": "destination",
                        },
                        "children": [],
                    },
                    {
                        "text": "bad_nested",
                        "data": {
                            "name": "bad_nested",
                            "drive_id": "other-drive-99",
                            "tree_role": "destination",
                        },
                        "children": [],
                    },
                ],
            )
        ]
        chosen, label, _, _, meta = select_validated_destination_startup_snapshot(
            session,
            sidecar,
            ctx,
            session_envelope_drive_id="dest1",
            session_envelope_library_id="dest1",
            sidecar_envelope_drive_id="dest1",
            sidecar_envelope_library_id="dest1",
            intended_drive_id="dest1",
        )
        self.assertIn("Workspace", label)
        self.assertEqual(chosen[0]["text"], "B")
        self.assertEqual(int(meta.get("session_sanitized_nodes", 0)), int(meta.get("sidecar_sanitized_nodes", 0)))
        self.assertGreater(int(meta.get("sidecar_raw_nodes", 0)), int(meta.get("session_raw_nodes", 0)))
        self.assertIn("sanitized_tie", str(meta.get("hydration_richness_decision_reason", "")))

    def test_promoted_paths_filtered_by_allowed_segments(self):
        allowed = {"root3".casefold()}
        paths = ["Root3\\A", "Foreign\\B"]
        f = filter_promoted_semantic_paths_for_destination_roots(paths, allowed)
        self.assertEqual(f, ["Root3\\A"])

    def test_allowed_segments_from_snapshot(self):
        snaps = [{"text": "Root3", "data": {"item_path": "Root3\\x", "name": "Root3"}, "children": []}]
        s = allowed_semantic_root_segments_cf_from_snapshot(snaps)
        self.assertIn("root3", s)

    def test_valid_single_root_snapshot_unchanged(self):
        ctx = DestinationStartupSnapshotRootContext(
            browse_mode="sharepoint",
            destination_drive_id="dest1",
            source_drive_id="src1",
        )
        snaps = [_root_snap("Root3", drive_id="dest1")]
        out, st = sanitize_destination_startup_snapshot_top_level(snaps, ctx, selection_tag="t")
        self.assertEqual(len(out), 1)
        self.assertEqual(st.pruned_top_level, 0)

    def test_strict_prune_missing_row_drive_when_destination_known(self):
        ctx = DestinationStartupSnapshotRootContext(
            browse_mode="sharepoint",
            destination_drive_id="dest1",
            source_drive_id="src1",
            strict_missing_row_drive=True,
        )
        snap = _root_snap("NoRowDrive", drive_id="")
        out, st = sanitize_destination_startup_snapshot_top_level([snap], ctx, selection_tag="t")
        self.assertEqual(out, [])
        self.assertGreaterEqual(st.pruned_top_level, 1)

    def test_identity_gate_mismatch_returns_empty(self):
        snaps = [_root_snap("R", drive_id="a")]
        out, tag = apply_destination_snapshot_identity_gate(
            snaps,
            intended_drive_id="b",
            snapshot_stored_drive_id="x",
            snapshot_stored_library_id="x",
            source="session",
        )
        self.assertEqual(out, [])
        self.assertEqual(tag, "rejected_envelope_mismatch")

    def test_identity_gate_ok(self):
        snaps = [_root_snap("R", drive_id="dest1")]
        out, tag = apply_destination_snapshot_identity_gate(
            snaps,
            intended_drive_id="dest1",
            snapshot_stored_drive_id="dest1",
            snapshot_stored_library_id="dest1",
            source="session",
        )
        self.assertEqual(len(out), 1)
        self.assertEqual(tag, "identity_ok")


class DestinationPlanningModelRootMergeTests(unittest.TestCase):
    def test_enrich_only_prunes_foreign_drive_root(self):
        m = DestinationPlanningTreeModel()
        bogus = {
            "name": "ForeignRoot",
            "id": "id-foreign",
            "drive_id": "drive-src",
            "is_folder": True,
            "tree_role": "destination",
            "workspace_row_state": "live_confirmed",
        }
        m.reset_nested([(bogus, [])])
        self.assertEqual(m.rowCount(QModelIndex()), 1)
        incoming = [
            {
                "name": "Root3",
                "id": "id-dest",
                "drive_id": "drive-dest",
                "is_folder": True,
            }
        ]
        stats = m.merge_sharepoint_library_root_graph_children(incoming, enrich_only=True)
        self.assertGreaterEqual(stats.get("removed", 0), 1)
        self.assertEqual(m.rowCount(QModelIndex()), 1)
        ix = m.index(0, 0, QModelIndex())
        pl = ix.data(Qt.UserRole) or {}
        self.assertEqual(str(pl.get("name") or ""), "Root3")

    def test_enrich_only_keeps_cached_provisional_same_drive(self):
        m = DestinationPlanningTreeModel()
        shell = {
            "name": "PlannedShell",
            "id": "",
            "drive_id": "drive-dest",
            "is_folder": True,
            "workspace_row_state": "cached_provisional",
            "tree_role": "destination",
        }
        m.reset_nested([(shell, [])])
        incoming = [{"name": "Root3", "id": "g1", "drive_id": "drive-dest", "is_folder": True}]
        stats = m.merge_sharepoint_library_root_graph_children(incoming, enrich_only=True)
        self.assertEqual(stats.get("removed", 0), 0)
        self.assertGreaterEqual(m.rowCount(QModelIndex()), 1)
