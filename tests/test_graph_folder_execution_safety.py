"""Unit tests for Graph folder copy safety (dirty folder detection)."""

import unittest

from ozlink_console.graph_folder_execution_safety import (
    compute_graph_unsafe_folder_step_indices,
    folder_planned_move_is_graph_dirty,
)


def _cb():
    """Minimal callbacks matching MainWindow-style path semantics."""

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

    return (
        canonical_source,
        path_is_descendant,
        allocation_projection_path,
        normalize_memory_path,
        canonical_destination_projection_path,
        paths_equivalent,
    )


class GraphFolderExecutionSafetyTests(unittest.TestCase):
    def test_clean_folder_with_matching_file_moves(self):
        folder = {
            "source_path": "src\\A",
            "destination_path": "dst\\Dest",
            "target_name": "A",
            "source": {"is_folder": True},
        }
        file_move = {
            "source_path": "src\\A\\f.txt",
            "destination_path": "dst\\Dest\\A",
            "target_name": "f.txt",
            "source": {"is_folder": False},
        }
        cs, pid, ap, nm, cdp, peq = _cb()
        self.assertFalse(
            folder_planned_move_is_graph_dirty(
                folder,
                [folder, file_move],
                set(),
                canonical_source=cs,
                path_is_descendant=pid,
                allocation_projection_path=ap,
                normalize_memory_path=nm,
                canonical_destination_projection_path=cdp,
                paths_equivalent=peq,
            )
        )

    def test_dirty_when_file_destination_overrides_inheritance(self):
        folder = {
            "source_path": "src\\A",
            "destination_path": "dst\\Dest",
            "target_name": "A",
            "source": {"is_folder": True},
        }
        file_move = {
            "source_path": "src\\A\\f.txt",
            "destination_path": "dst\\Other",
            "target_name": "f.txt",
            "source": {"is_folder": False},
        }
        cs, pid, ap, nm, cdp, peq = _cb()
        self.assertTrue(
            folder_planned_move_is_graph_dirty(
                folder,
                [folder, file_move],
                set(),
                canonical_source=cs,
                path_is_descendant=pid,
                allocation_projection_path=ap,
                normalize_memory_path=nm,
                canonical_destination_projection_path=cdp,
                paths_equivalent=peq,
            )
        )

    def test_dirty_when_excluded_leaf_under_folder(self):
        folder = {
            "source_path": "src\\A",
            "destination_path": "dst\\Dest",
            "target_name": "A",
            "source": {"is_folder": True},
        }
        cs, pid, ap, nm, cdp, peq = _cb()
        self.assertTrue(
            folder_planned_move_is_graph_dirty(
                folder,
                [folder],
                {"src\\A\\x.txt"},
                canonical_source=cs,
                path_is_descendant=pid,
                allocation_projection_path=ap,
                normalize_memory_path=nm,
                canonical_destination_projection_path=cdp,
                paths_equivalent=peq,
            )
        )

    def test_compute_indices_sorts_folder_steps(self):
        f0 = {
            "source_path": "src\\A",
            "destination_path": "dst\\Dest",
            "target_name": "A",
            "source": {"is_folder": True},
        }
        f1 = {
            "source_path": "src\\B",
            "destination_path": "dst\\Dest",
            "target_name": "B",
            "source": {"is_folder": True},
        }
        override = {
            "source_path": "src\\A\\f.txt",
            "destination_path": "dst\\Other",
            "target_name": "f.txt",
            "source": {"is_folder": False},
        }
        cs, pid, ap, nm, cdp, peq = _cb()
        idx = compute_graph_unsafe_folder_step_indices(
            [f0, f1, override],
            set(),
            canonical_source=cs,
            path_is_descendant=pid,
            allocation_projection_path=ap,
            normalize_memory_path=nm,
            canonical_destination_projection_path=cdp,
            paths_equivalent=peq,
        )
        self.assertEqual(idx, [0])


if __name__ == "__main__":
    unittest.main()
