"""Regression: indexed planned-move lookup matches linear scan."""

import unittest


class PlannedMoveDestinationLookupTests(unittest.TestCase):
    def _make_window_stub(self):
        from ozlink_console.main_window import MainWindow

        w = MainWindow.__new__(MainWindow)
        w.planned_moves = []
        w._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\").strip()
        w._allocation_projection_path = lambda m: m.get("_alloc", "")
        w._tree_item_path = lambda d: d.get("item_path", "") or d.get("display_path", "")
        w._canonical_source_projection_path = lambda p: str(p or "").replace("/", "\\").strip()
        w._paths_equivalent = lambda a, b, role: a == b
        w._path_is_descendant = lambda child, parent, role: (
            bool(child) and bool(parent) and child != parent and child.startswith(parent + "\\")
        )
        return w

    def test_indexed_matches_linear_exact_allocation(self):
        w = self._make_window_stub()
        w.planned_moves = [
            {"source_path": "S\\A", "_alloc": "D\\X", "destination_path": "D\\X"},
            {"source_path": "S\\B", "_alloc": "D\\Y", "destination_path": "D\\Y"},
        ]
        node = {"destination_path": "D\\Y", "item_path": "D\\Y", "source_path": ""}
        lookup = w._build_planned_move_destination_lookup()
        a = w._find_planned_move_for_destination_node(node)
        b = w._find_planned_move_for_destination_node_indexed(node, lookup)
        self.assertIs(a, w.planned_moves[1])
        self.assertEqual(a, b)

    def test_indexed_matches_linear_longest_source_ancestor(self):
        w = self._make_window_stub()
        w.planned_moves = [
            {"source_path": "Lib\\Root", "_alloc": "D\\R", "destination_path": "D\\R"},
            {"source_path": "Lib\\Root\\Sub", "_alloc": "D\\R\\Sub", "destination_path": "D\\R\\Sub"},
        ]
        node = {
            "destination_path": "D\\R\\Sub\\Leaf",
            "item_path": "D\\R\\Sub\\Leaf",
            "source_path": "Lib\\Root\\Sub\\Leaf",
        }
        lookup = w._build_planned_move_destination_lookup()
        a = w._find_planned_move_for_destination_node(node)
        b = w._find_planned_move_for_destination_node_indexed(node, lookup)
        self.assertEqual(a, b)
        self.assertIs(a, w.planned_moves[1])


if __name__ == "__main__":
    unittest.main()
