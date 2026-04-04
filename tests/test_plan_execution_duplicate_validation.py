"""Duplicate collision validation for execution/export."""

from __future__ import annotations

import unittest

from ozlink_console.plan_execution_duplicate_validation import (
    duplicate_collision_report_from_moves,
    duplicate_collision_report_from_transfer_steps,
    format_execution_duplicate_message,
)


class PlanExecutionDuplicateValidationTests(unittest.TestCase):
    def test_transfer_steps_duplicate_source(self):
        steps = [
            {"operation": "copy", "source_path": r"C:\a\x.txt", "destination_path": r"D:\t", "destination_name": "x.txt", "is_source_folder": False},
            {"operation": "copy", "source_path": r"C:/a/x.txt", "destination_path": r"D:\u", "destination_name": "x.txt", "is_source_folder": False},
        ]
        ds, dd = duplicate_collision_report_from_transfer_steps(steps)
        self.assertEqual(len(ds), 1)
        self.assertEqual(len(dd), 0)

    def test_transfer_steps_duplicate_dest_file(self):
        steps = [
            {"operation": "copy", "source_path": r"C:\a\1.txt", "destination_path": r"D:\out", "destination_name": "x.txt", "is_source_folder": False},
            {"operation": "copy", "source_path": r"C:\b\2.txt", "destination_path": r"D:\out\x.txt", "destination_name": "x.txt", "is_source_folder": False},
        ]
        ds, dd = duplicate_collision_report_from_transfer_steps(steps)
        self.assertEqual(len(ds), 0)
        self.assertEqual(len(dd), 1)

    def test_planned_moves_duplicate_source_callback(self):
        moves = [
            {"source_path": r"lib\a\f.txt", "source": {"is_folder": False}, "destination_path": "x"},
            {"source_path": r"lib/a/f.txt", "source": {"is_folder": False}, "destination_path": "y"},
        ]
        ds, dd = duplicate_collision_report_from_moves(
            moves,
            canonical_source=lambda m: str(m.get("source_path", "") or "").replace("/", "\\").strip(),
            destination_file_key=lambda m: None,
        )
        self.assertEqual(len(ds), 1)
        self.assertEqual(len(dd), 0)

    def test_format_message_empty(self):
        self.assertEqual(format_execution_duplicate_message([], []), "")


if __name__ == "__main__":
    unittest.main()
