"""Tests for graph_folder_execution_expansion.expand_graph_transfer_steps."""

import unittest

from ozlink_console.graph_folder_execution_expansion import expand_graph_transfer_steps


def _helpers():
    def canonical_source(m):
        return str(m.get("source_path", "")).replace("/", "\\").rstrip("\\")

    def path_is_descendant(child_path, parent_path):
        cl = child_path.lower()
        pl = parent_path.lower()
        return cl != pl and cl.startswith(pl + "\\")

    def allocation_projection_path(m):
        parent = str(m.get("destination_path", "") or "").replace("/", "\\").rstrip("\\")
        name = str(m.get("target_name") or m.get("source_name", "") or "").strip()
        if not parent or not name:
            return ""
        return f"{parent}\\{name}"

    def normalize_memory_path(p):
        return str(p).replace("/", "\\")

    def canonical_destination_projection_path(p):
        return str(p).replace("/", "\\").rstrip("\\")

    def paths_equivalent(a, b, tree_role):
        return (
            str(a).replace("/", "\\").rstrip("\\").lower()
            == str(b).replace("/", "\\").rstrip("\\").lower()
        )

    return {
        "canonical_source": canonical_source,
        "path_is_descendant": path_is_descendant,
        "allocation_projection_path": allocation_projection_path,
        "normalize_memory_path": normalize_memory_path,
        "canonical_destination_projection_path": canonical_destination_projection_path,
        "paths_equivalent": paths_equivalent,
    }


class ExpandGraphTransferStepsTests(unittest.TestCase):
    def test_clean_folder_single_step(self):
        h = _helpers()
        moves = [
            {
                "source_path": "src\\A",
                "destination_path": "dst\\Dest",
                "target_name": "A",
                "source": {"is_folder": True, "name": "A"},
                "destination": {},
            }
        ]
        out = expand_graph_transfer_steps(moves, set(), **h)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].kind, "folder")
        self.assertEqual(out[0].planned_move_index, 0)
        self.assertTrue(out[0].is_source_folder)

    def test_dirty_folder_with_exclusion_omits_excluded_file(self):
        h = _helpers()
        moves = [
            {
                "source_path": "src\\A",
                "destination_path": "dst\\Dest",
                "target_name": "A",
                "source": {"is_folder": True},
                "destination": {},
            },
            {
                "source_path": "src\\A\\e.txt",
                "destination_path": "dst\\Dest\\A",
                "target_name": "e.txt",
                "source": {"is_folder": False},
                "destination": {},
            },
        ]
        excl = {"src\\A\\e.txt"}
        out = expand_graph_transfer_steps(moves, excl, **h)
        self.assertEqual(out, [])

    def test_dirty_folder_with_override_emits_file_only(self):
        h = _helpers()
        moves = [
            {
                "source_path": "src\\A",
                "destination_path": "dst\\Dest",
                "target_name": "A",
                "source": {"is_folder": True},
                "destination": {},
            },
            {
                "source_path": "src\\A\\o.txt",
                "destination_path": "dst\\Other",
                "target_name": "o.txt",
                "source": {"is_folder": False},
                "destination": {},
            },
        ]
        out = expand_graph_transfer_steps(moves, set(), **h)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].kind, "file")
        self.assertEqual(out[0].planned_move_index, 1)
        self.assertIn("o.txt", out[0].source_path)

    def test_mixed_clean_subfolder_and_override_sibling(self):
        h = _helpers()
        moves = [
            {
                "source_path": "src\\A",
                "destination_path": "dst\\Dest",
                "target_name": "A",
                "source": {"is_folder": True},
                "destination": {},
            },
            {
                "source_path": "src\\A\\sub",
                "destination_path": "dst\\Dest\\A",
                "target_name": "sub",
                "source": {"is_folder": True},
                "destination": {},
            },
            {
                "source_path": "src\\A\\o.txt",
                "destination_path": "dst\\Other",
                "target_name": "o.txt",
                "source": {"is_folder": False},
                "destination": {},
            },
        ]
        out = expand_graph_transfer_steps(moves, set(), **h)
        kinds = [x.kind for x in out]
        self.assertEqual(kinds, ["folder", "file"])
        self.assertEqual(out[0].planned_move_index, 1)
        self.assertEqual(out[1].planned_move_index, 2)

    def test_nested_clean_folders_only_outermost_folder_step_emitted(self):
        """Two clean folder moves under the same dirty parent: one Graph folder copy (ancestor)."""
        h = _helpers()
        moves = [
            {
                "source_path": "src\\A",
                "destination_path": "dst\\Dest",
                "target_name": "A",
                "source": {"is_folder": True},
                "destination": {},
            },
            {
                "source_path": "src\\A\\sub",
                "destination_path": "dst\\Dest\\A",
                "target_name": "sub",
                "source": {"is_folder": True},
                "destination": {},
            },
            {
                "source_path": "src\\A\\sub\\nested",
                "destination_path": "dst\\Dest\\A\\sub",
                "target_name": "nested",
                "source": {"is_folder": True},
                "destination": {},
            },
        ]
        excl = {"src\\A\\e.txt"}
        out = expand_graph_transfer_steps(moves, excl, **h)
        folder_steps = [x for x in out if x.kind == "folder"]
        self.assertEqual(len(folder_steps), 1)
        self.assertEqual(folder_steps[0].planned_move_index, 1)
        self.assertIn("sub", folder_steps[0].source_path.lower())

    def test_dirty_parent_nested_clean_emits_shallow_folder_only(self):
        """Dirty F with two nested clean folder rows: only the shallow clean folder is a folder step."""
        h = _helpers()
        moves = [
            {
                "source_path": "lib\\Root",
                "destination_path": "dst\\R",
                "target_name": "Root",
                "source": {"is_folder": True},
                "destination": {},
            },
            {
                "source_path": "lib\\Root\\inner",
                "destination_path": "dst\\R\\Root",
                "target_name": "inner",
                "source": {"is_folder": True},
                "destination": {},
            },
            {
                "source_path": "lib\\Root\\inner\\deep",
                "destination_path": "dst\\R\\Root\\inner",
                "target_name": "deep",
                "source": {"is_folder": True},
                "destination": {},
            },
        ]
        excl = {"lib\\Root\\x.bin"}
        out = expand_graph_transfer_steps(moves, excl, **h)
        folder_ix = {x.planned_move_index for x in out if x.kind == "folder"}
        self.assertEqual(folder_ix, {1})

    def test_nested_dirty_folders_no_duplicate_emitted_steps(self):
        """Stacked dirty subfolders: each index handled once; no overlapping folder Graph steps."""
        h = _helpers()
        moves = [
            {
                "source_path": "src\\A",
                "destination_path": "dst\\Dest",
                "target_name": "A",
                "source": {"is_folder": True},
                "destination": {},
            },
            {
                "source_path": "src\\A\\sub",
                "destination_path": "dst\\Dest\\A",
                "target_name": "sub",
                "source": {"is_folder": True},
                "destination": {},
            },
            {
                "source_path": "src\\A\\sub\\nested",
                "destination_path": "dst\\Dest\\A\\sub",
                "target_name": "nested",
                "source": {"is_folder": True},
                "destination": {},
            },
        ]
        excl = {"src\\A\\e.txt", "src\\A\\sub\\e2.txt", "src\\A\\sub\\nested\\e3.txt"}
        out = expand_graph_transfer_steps(moves, excl, **h)
        self.assertEqual(out, [])

    def test_override_under_nested_folder_no_clean_folder_step_for_nested(self):
        """Nested folder move is dirty due to override file; expansion emits file step only for override."""
        h = _helpers()
        moves = [
            {
                "source_path": "src\\A",
                "destination_path": "dst\\Dest",
                "target_name": "A",
                "source": {"is_folder": True},
                "destination": {},
            },
            {
                "source_path": "src\\A\\nested",
                "destination_path": "dst\\Dest\\A",
                "target_name": "nested",
                "source": {"is_folder": True},
                "destination": {},
            },
            {
                "source_path": "src\\A\\nested\\o.txt",
                "destination_path": "dst\\Other",
                "target_name": "o.txt",
                "source": {"is_folder": False},
                "destination": {},
            },
        ]
        excl = {"src\\A\\skip.txt"}
        out = expand_graph_transfer_steps(moves, excl, **h)
        folder_ix = [x.planned_move_index for x in out if x.kind == "folder"]
        self.assertEqual(folder_ix, [])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].kind, "file")
        self.assertEqual(out[0].planned_move_index, 2)

    def test_duplicate_destination_raises(self):
        h = _helpers()
        moves = [
            {
                "source_path": "src\\A\\x.txt",
                "destination_path": "dst\\Dest",
                "target_name": "t.txt",
                "source_name": "x.txt",
                "source": {"is_folder": False},
                "destination": {},
            },
            {
                "source_path": "src\\A\\y.txt",
                "destination_path": "dst\\Dest",
                "target_name": "t.txt",
                "source_name": "y.txt",
                "source": {"is_folder": False},
                "destination": {},
            },
        ]
        with self.assertRaises(ValueError) as ctx:
            expand_graph_transfer_steps(moves, set(), **h)
        self.assertIn("Duplicate", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
