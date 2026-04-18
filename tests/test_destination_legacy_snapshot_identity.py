"""Legacy destination snapshot identity inference (startup migration)."""

import unittest

from ozlink_console.destination_legacy_snapshot_identity import (
    DestinationLibraryCandidate,
    InferenceConfidence,
    infer_destination_snapshot_identity_from_legacy_snapshot,
)
from ozlink_console.destination_startup_snapshot_roots import (
    DestinationStartupSnapshotRootContext,
    apply_destination_snapshot_identity_gate_with_legacy,
    select_validated_destination_startup_snapshot,
)
from ozlink_console.models import SessionState


def _root(text: str, **data):
    pl = {"name": text, "tree_role": "destination", **data}
    return {"text": text, "data": pl, "children": []}


def _snap_many_row_votes(drive: str, n: int):
    """n descendant payloads sharing drive_id (plus root)."""
    ch = []
    for i in range(n):
        ch.append(
            {
                "text": f"n{i}",
                "data": {"name": f"n{i}", "drive_id": drive, "tree_role": "destination"},
                "children": [],
            }
        )
    return {
        "text": "root",
        "data": {"name": "root", "drive_id": drive, "tree_role": "destination"},
        "children": ch,
    }


class LegacySnapshotIdentityInferenceTests(unittest.TestCase):
    def test_explicit_envelope_skips_legacy_path(self):
        snaps = [_root("R", drive_id="d1")]
        gated, tag, stamp = apply_destination_snapshot_identity_gate_with_legacy(
            snaps,
            intended_drive_id="d1",
            snapshot_stored_drive_id="d1",
            snapshot_stored_library_id="d1",
            source="session",
            legacy_library_candidates=[
                DestinationLibraryCandidate(drive_id="other", display_name="Other"),
            ],
        )
        self.assertIsNone(stamp)
        self.assertEqual(tag, "identity_ok")
        self.assertEqual(len(gated), 1)

    def test_strong_row_drive_consensus_high_confidence(self):
        snap = _snap_many_row_votes("drive-consensus", 5)
        cands = [
            DestinationLibraryCandidate(drive_id="drive-consensus", display_name="Target Lib"),
            DestinationLibraryCandidate(drive_id="other-drive", display_name="Other"),
        ]
        r = infer_destination_snapshot_identity_from_legacy_snapshot([snap], cands)
        self.assertEqual(r.matched_drive_id, "drive-consensus")
        self.assertEqual(r.confidence, InferenceConfidence.HIGH)
        self.assertTrue(r.auto_apply_safe)

    def test_distinctive_shallow_root_fingerprint(self):
        rseg = frozenset({"alpharoot".casefold()})
        snap = _root("AlphaRoot", item_path="AlphaRoot\\Sub", drive_id="")
        cands = [
            DestinationLibraryCandidate(
                drive_id="d-alpha",
                display_name="Alpha Library",
                graph_shallow_root_names_cf=rseg,
            ),
            DestinationLibraryCandidate(drive_id="d-beta", display_name="Beta Library"),
        ]
        r = infer_destination_snapshot_identity_from_legacy_snapshot([snap], cands)
        self.assertEqual(r.matched_drive_id, "d-alpha")
        self.assertGreaterEqual(r.score, 38.0)

    def test_ambiguous_stays_low_or_medium(self):
        snap = _root("Documents", item_path="Documents\\A", drive_id="")
        cands = [
            DestinationLibraryCandidate(drive_id="a", display_name="Lib"),
            DestinationLibraryCandidate(drive_id="b", display_name="Lib"),
        ]
        r = infer_destination_snapshot_identity_from_legacy_snapshot([snap], cands)
        self.assertIn(r.confidence, (InferenceConfidence.LOW, InferenceConfidence.MEDIUM))
        self.assertFalse(r.auto_apply_safe)

    def test_gate_mismatch_intended_blocks_high_inference(self):
        snap = _snap_many_row_votes("drive-target", 5)
        gated, tag, inf_meta = apply_destination_snapshot_identity_gate_with_legacy(
            [snap],
            intended_drive_id="combo-wrong",
            snapshot_stored_drive_id="",
            snapshot_stored_library_id="",
            source="session",
            legacy_library_candidates=[
                DestinationLibraryCandidate(drive_id="drive-target"),
                DestinationLibraryCandidate(drive_id="combo-wrong"),
            ],
        )
        self.assertEqual(gated, [])
        self.assertEqual(tag, "legacy_identity_high_intended_mismatch")
        self.assertIsInstance(inf_meta, dict)
        self.assertIn("inference", inf_meta)

    def test_no_candidates_blocked(self):
        gated, tag, _ = apply_destination_snapshot_identity_gate_with_legacy(
            [_root("x")],
            intended_drive_id="d1",
            snapshot_stored_drive_id="",
            snapshot_stored_library_id="",
            source="session",
            legacy_library_candidates=[],
        )
        self.assertEqual(gated, [])
        self.assertEqual(tag, "blocked_legacy_no_candidates")

    def test_select_validated_with_legacy_stamps_session_meta(self):
        snap = _snap_many_row_votes("drive-x", 5)
        ctx = DestinationStartupSnapshotRootContext(
            browse_mode="sharepoint",
            destination_drive_id="drive-x",
            source_drive_id="src",
        )
        cands = [
            DestinationLibraryCandidate(drive_id="drive-x", display_name="L"),
            DestinationLibraryCandidate(drive_id="other", display_name="O"),
        ]
        chosen, label, _, _, meta = select_validated_destination_startup_snapshot(
            [snap],
            [],
            ctx,
            session_envelope_drive_id="",
            session_envelope_library_id="",
            sidecar_envelope_drive_id="",
            sidecar_envelope_library_id="",
            intended_drive_id="drive-x",
            legacy_library_candidates=cands,
        )
        self.assertGreater(len(chosen), 0)
        self.assertIn("SessionState", label)
        st = meta.get("session_legacy_identity_stamp")
        self.assertIsInstance(st, dict)
        self.assertTrue(st.get("destination_snapshot_identity_inferred_from_legacy"))

    def test_session_state_roundtrip_inferred_flag(self):
        s = SessionState(
            DestinationTreeSnapshotIdentityDriveId="d1",
            DestinationTreeSnapshotIdentityInferredFromLegacy=True,
        )
        d = s.to_dict()
        self.assertTrue(d.get("DestinationTreeSnapshotIdentityInferredFromLegacy"))
        s2 = SessionState.from_dict(d)
        self.assertTrue(s2.DestinationTreeSnapshotIdentityInferredFromLegacy)


if __name__ == "__main__":
    unittest.main()
