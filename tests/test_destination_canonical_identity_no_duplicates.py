"""Destination tree reuse uses one effective canonical path (no duplicate structural rows)."""

from __future__ import annotations

import os
import unittest
from unittest.mock import patch

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication

from ozlink_console.main_window import MainWindow, _DESTINATION_STRUCTURAL_INSERT_AMBIGUOUS
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _qapp():
    return QApplication.instance() or QApplication([])


def _bare_mw():
    mw = MainWindow.__new__(MainWindow)
    mw.pending_root_drive_ids = {"destination": "drive-dest"}
    mw._destination_tree_model_view = True
    return mw


class DestinationCanonicalIdentityTests(unittest.TestCase):
    def test_payload_index_raw_mem_matches_tree_item_path_precedence(self):
        mw = _bare_mw()
        d = {
            "source_path": r"Source\Only",
            "destination_path": r"Dest\Only",
            "tree_role": "destination",
        }
        self.assertEqual(mw._destination_payload_index_raw_mem(d), mw._tree_item_path(d))

    def test_child_under_real_root_folder_resolves_by_full_spo_path(self):
        _qapp()
        mw = _bare_mw()
        mw._current_destination_context_segments = lambda: []
        model = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
        mw.destination_planning_model = model
        model.reset_root_payloads([])
        parent = QModelIndex()
        real = {
            "name": "Invoices",
            "real_name": "Invoices",
            "is_folder": True,
            "display_path": r"Root\Finance\Invoices",
            "item_path": r"Root\Finance\Invoices",
            "tree_role": "destination",
        }
        model.append_child_payloads(parent, [real])
        child_ix = model.index(0, 0, parent)
        self.assertTrue(child_ix.isValid())
        hit = mw._find_destination_child_by_path(parent, r"Root\Finance\Invoices")
        self.assertIsNotNone(hit)
        self.assertEqual(hit.row(), child_ix.row())

    def test_file_child_reused_by_effective_path_not_item_path_first_mismatch(self):
        _qapp()
        mw = _bare_mw()
        mw._current_destination_context_segments = lambda: []
        model = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
        mw.destination_planning_model = model
        model.reset_root_payloads([])
        parent = QModelIndex()
        file_row = {
            "name": "a.pdf",
            "real_name": "a.pdf",
            "is_folder": False,
            "display_path": r"Shared\a.pdf",
            "item_path": r"wrong\item\a.pdf",
            "tree_role": "destination",
        }
        model.append_child_payloads(parent, [file_row])
        child_ix = model.index(0, 0, parent)
        self.assertTrue(child_ix.isValid())
        hit = mw._find_destination_child_by_path(parent, r"Shared\a.pdf")
        self.assertIsNotNone(hit)
        self.assertEqual(hit.row(), child_ix.row())

    def test_proposed_folder_exists_uses_tree_item_path_not_item_path_only(self):
        _qapp()
        mw = _bare_mw()
        mw._current_destination_context_segments = lambda: []
        model = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
        mw.destination_planning_model = model
        model.reset_root_payloads([])
        root = QModelIndex()
        dept = {
            "name": "Dept",
            "is_folder": True,
            "display_path": "Dept",
            "item_path": "Dept",
            "tree_role": "destination",
        }
        model.append_child_payloads(root, [dept])
        dept_ix = model.index(0, 0, root)
        folder = {
            "name": "Reports",
            "is_folder": True,
            "display_path": r"Dept\Reports",
            "item_path": r"legacy\wrong\Reports",
            "tree_role": "destination",
        }
        model.append_child_payloads(dept_ix, [folder])
        self.assertTrue(
            mw._proposed_folder_exists_under(dept_ix, r"Dept\Reports"),
            "expected display_path-first identity to match proposed destination",
        )

    def test_two_folder_siblings_same_last_segment_name_is_ambiguous_no_fallback(self):
        _qapp()
        mw = _bare_mw()
        mw._current_destination_context_segments = lambda: []
        model = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
        mw.destination_planning_model = model
        model.reset_root_payloads([])
        root = QModelIndex()
        hub = {
            "name": "Hub",
            "real_name": "Hub",
            "is_folder": True,
            "display_path": "Hub",
            "item_path": "Hub",
            "tree_role": "destination",
        }
        model.append_child_payloads(root, [hub])
        hub_ix = model.index(0, 0, root)
        self.assertTrue(hub_ix.isValid())
        model.append_child_payloads(
            hub_ix,
            [
                {
                    "name": "Foo",
                    "real_name": "Foo",
                    "is_folder": True,
                    "display_path": r"Hub\Foo",
                    "item_path": r"Hub\Foo",
                    "tree_role": "destination",
                },
                {
                    "name": "Foo",
                    "real_name": "Foo",
                    "is_folder": True,
                    "display_path": r"Hub\Bar\Foo",
                    "item_path": r"Hub\Bar\Foo",
                    "tree_role": "destination",
                },
            ],
        )
        tail = mw._destination_find_child_by_last_segment_folder_name(hub_ix, r"Hub\Other\Foo")
        self.assertIsNone(tail)

    def test_precheck_blocks_ambiguous_canonical_duplicates(self):
        _qapp()
        mw = _bare_mw()
        mw._current_destination_context_segments = lambda: []
        model = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
        mw.destination_planning_model = model
        model.reset_root_payloads([])
        parent = QModelIndex()
        holder = {
            "name": "Holder",
            "real_name": "Holder",
            "is_folder": True,
            "display_path": "Holder",
            "item_path": "Holder",
            "tree_role": "destination",
        }
        model.append_child_payloads(parent, [holder])
        holder_ix = model.index(0, 0, parent)
        dup_path = r"Holder\Leaf"
        for _ in range(2):
            model.append_child_payloads(
                holder_ix,
                [
                    {
                        "name": "Leaf",
                        "is_folder": True,
                        "display_path": dup_path,
                        "item_path": dup_path,
                        "tree_role": "destination",
                    }
                ],
            )
        with patch.object(mw, "_find_destination_child_by_path", return_value=None):
            out = mw._destination_precheck_before_projected_insert(
                holder_ix, dup_path, reason="test_ambiguous", assume_primary_find_miss=True
            )
        self.assertIs(out, _DESTINATION_STRUCTURAL_INSERT_AMBIGUOUS)


if __name__ == "__main__":
    unittest.main()
