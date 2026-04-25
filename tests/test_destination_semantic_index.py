import unittest

from ozlink_console.destination_semantic_index import (
    compute_incremental_merge_entry_roots,
    group_paths_by_parent_semantic,
    path_segments,
)


def _node(parent_sem: str, name: str, state: str = "projected_descendant", is_folder: bool = True):
    return {
        "parent_semantic_path": parent_sem,
        "name": name,
        "node_state": state,
        "data": {"is_folder": is_folder, "name": name},
        "children": [],
    }


class DestinationSemanticIndexTests(unittest.TestCase):
    def test_entry_roots_excludes_inner_new_paths(self):
        model_nodes = {
            "Root\\A": _node("Root", "A"),
            "Root\\A\\f1": _node("Root\\A", "f1", is_folder=False),
            "Root\\A\\f2": _node("Root\\A", "f2", is_folder=False),
        }
        new_paths = {"Root\\A\\f1", "Root\\A\\f2"}
        roots = compute_incremental_merge_entry_roots(model_nodes, new_paths)
        self.assertEqual(set(roots), {"Root\\A\\f1", "Root\\A\\f2"})

    def test_entry_roots_skips_parent_when_parent_also_new(self):
        model_nodes = {
            "Root\\P": _node("Root", "P"),
            "Root\\P\\c": _node("Root\\P", "c", is_folder=False),
        }
        new_paths = {"Root\\P", "Root\\P\\c"}
        roots = compute_incremental_merge_entry_roots(model_nodes, new_paths)
        self.assertEqual(roots, ["Root\\P"])

    def test_group_by_parent_sorts_children(self):
        model_nodes = {
            "Root\\X": _node("Root", "X", is_folder=False),
            "Root\\A": _node("Root", "a", is_folder=False),
        }
        paths = ["Root\\X", "Root\\A"]
        grouped = group_paths_by_parent_semantic(paths, model_nodes)
        self.assertEqual(list(grouped.keys()), ["Root"])
        # Sorted by future_model_sort_key: files first (is_folder False), then name lower
        self.assertEqual(grouped["Root"], ["Root\\A", "Root\\X"])

    def test_path_segments(self):
        self.assertEqual(path_segments("a/b\\c"), ["a", "b", "c"])


if __name__ == "__main__":
    unittest.main()
