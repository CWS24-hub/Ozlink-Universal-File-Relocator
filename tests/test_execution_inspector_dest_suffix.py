"""Pure tests for execution duplicate inspector destination suffix suggestions (planning-only)."""

from __future__ import annotations

import unittest

from ozlink_console.plan_execution_duplicate_validation import (
    build_execution_inspector_chained_dest_suffix_preview_rows,
    build_execution_inspector_dest_suffix_preview_rows_for_moves,
    duplicate_transfer_step_groups_by_source,
    leaf_base_ext_for_execution_inspector_suffix,
    norm_execution_key,
)


def _dest_key(m: dict) -> str | None:
    if m.get("source", {}).get("is_folder"):
        return None
    dp = str(m.get("destination_path", "")).replace("/", "\\").rstrip("\\")
    leaf = str(m.get("target_name", "")).strip()
    if not dp or not leaf:
        return None
    parts = [p for p in dp.split("\\") if p]
    if parts and parts[-1].lower() == leaf.lower():
        return dp
    return f"{dp}\\{leaf}"


def _target(m: dict) -> str:
    return str(m.get("target_name", "") or "").strip()


class ExecutionInspectorDestSuffixTests(unittest.TestCase):
    def test_leaf_base_ext_preserves_multi_dot_extension(self) -> None:
        stem, ext = leaf_base_ext_for_execution_inspector_suffix("archive.tar.gz")
        self.assertEqual(stem, "archive.tar")
        self.assertEqual(ext, ".gz")

    def test_one_duplicate_destination_group_two_files(self) -> None:
        moves = [
            {"source_path": r"S:\a\1.txt", "source": {"is_folder": False}, "destination_path": r"D:\out", "target_name": "doc.txt"},
            {"source_path": r"S:\b\2.txt", "source": {"is_folder": False}, "destination_path": r"D:\out", "target_name": "doc.txt"},
        ]
        rows = build_execution_inspector_dest_suffix_preview_rows_for_moves(
            moves, [1, 0],
            destination_file_key=_dest_key,
            move_target_name=_target,
        )
        self.assertIsNotNone(rows)
        assert rows is not None
        by_idx = {int(r["index"]): r["suggested_leaf"] for r in rows}
        self.assertEqual(by_idx[0], "doc.txt")
        self.assertEqual(by_idx[1], "doc (2).txt")

    def test_uniqueness_against_planned_row_outside_group(self) -> None:
        moves = [
            {"source_path": r"S:\a\1.txt", "source": {"is_folder": False}, "destination_path": r"D:\out", "target_name": "doc.txt"},
            {"source_path": r"S:\b\2.txt", "source": {"is_folder": False}, "destination_path": r"D:\out", "target_name": "doc.txt"},
            {"source_path": r"S:\c\3.txt", "source": {"is_folder": False}, "destination_path": r"D:\out", "target_name": "doc (2).txt"},
        ]
        rows = build_execution_inspector_dest_suffix_preview_rows_for_moves(
            moves, [0, 1],
            destination_file_key=_dest_key,
            move_target_name=_target,
        )
        self.assertIsNotNone(rows)
        assert rows is not None
        by_idx = {int(r["index"]): r["suggested_leaf"] for r in rows}
        self.assertEqual(by_idx[0], "doc.txt")
        self.assertEqual(by_idx[1], "doc (3).txt")

    def test_chained_two_groups_independent_folders(self) -> None:
        moves = [
            {"source_path": r"S:\a", "source": {"is_folder": False}, "destination_path": r"D:\one", "target_name": "f.txt"},
            {"source_path": r"S:\b", "source": {"is_folder": False}, "destination_path": r"D:\one", "target_name": "f.txt"},
            {"source_path": r"S:\c", "source": {"is_folder": False}, "destination_path": r"D:\two", "target_name": "f.txt"},
            {"source_path": r"S:\d", "source": {"is_folder": False}, "destination_path": r"D:\two", "target_name": "f.txt"},
        ]
        rows = build_execution_inspector_chained_dest_suffix_preview_rows(
            moves,
            [[0, 1], [2, 3]],
            destination_file_key=_dest_key,
            move_target_name=_target,
        )
        self.assertIsNotNone(rows)
        assert rows is not None
        self.assertEqual(len(rows), 4)
        by_idx = {int(r["index"]): r["suggested_leaf"] for r in rows}
        self.assertEqual(by_idx[0], "f.txt")
        self.assertEqual(by_idx[1], "f (2).txt")
        self.assertEqual(by_idx[2], "f.txt")
        self.assertEqual(by_idx[3], "f (2).txt")

    def test_chained_second_group_sees_first_group_occupancy_same_folder(self) -> None:
        moves = [
            {"source_path": r"S:\a", "source": {"is_folder": False}, "destination_path": r"D:\out", "target_name": "x.txt"},
            {"source_path": r"S:\b", "source": {"is_folder": False}, "destination_path": r"D:\out", "target_name": "x.txt"},
            {"source_path": r"S:\c", "source": {"is_folder": False}, "destination_path": r"D:\out", "target_name": "y.txt"},
            {"source_path": r"S:\d", "source": {"is_folder": False}, "destination_path": r"D:\out", "target_name": "y.txt"},
        ]
        rows = build_execution_inspector_chained_dest_suffix_preview_rows(
            moves,
            [[0, 1], [2, 3]],
            destination_file_key=_dest_key,
            move_target_name=_target,
        )
        self.assertIsNotNone(rows)
        assert rows is not None
        by_idx = {int(r["index"]): r["suggested_leaf"] for r in rows}
        self.assertEqual(by_idx[0], "x.txt")
        self.assertEqual(by_idx[1], "x (2).txt")
        self.assertEqual(by_idx[2], "y.txt")
        self.assertEqual(by_idx[3], "y (2).txt")

    def test_less_than_two_indices_returns_none(self) -> None:
        moves = [
            {"source_path": r"S:\a", "source": {"is_folder": False}, "destination_path": r"D:\o", "target_name": "a.txt"},
        ]
        self.assertIsNone(
            build_execution_inspector_dest_suffix_preview_rows_for_moves(
                moves, [0], destination_file_key=_dest_key, move_target_name=_target
            )
        )

    def test_folder_move_in_group_returns_none(self) -> None:
        moves = [
            {"source_path": r"S:\a", "source": {"is_folder": True}, "destination_path": r"D:\o", "target_name": "fold"},
            {"source_path": r"S:\b", "source": {"is_folder": False}, "destination_path": r"D:\o", "target_name": "fold"},
        ]
        self.assertIsNone(
            build_execution_inspector_dest_suffix_preview_rows_for_moves(
                moves, [0, 1], destination_file_key=_dest_key, move_target_name=_target
            )
        )

    def test_source_duplicate_groups_are_separate_concern_no_suffix_api(self) -> None:
        """Suffix helpers only apply to destination-file collisions; source dupes use different UI paths."""
        steps = [
            {"operation": "copy", "source_path": r"C:\same.txt", "destination_path": r"D:\a", "destination_name": "a.txt", "is_source_folder": False},
            {"operation": "copy", "source_path": r"c:/same.txt", "destination_path": r"D:\b", "destination_name": "b.txt", "is_source_folder": False},
        ]
        src_groups = duplicate_transfer_step_groups_by_source(steps)
        self.assertEqual(len(src_groups), 1)
        # No transfer-step dest dupes in this fixture — suffix builder is never used for src_groups.
        self.assertNotEqual(norm_execution_key(r"C:\same.txt"), norm_execution_key(r"D:\a\a.txt"))


if __name__ == "__main__":
    unittest.main()
