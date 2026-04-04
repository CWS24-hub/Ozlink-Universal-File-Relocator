"""Integration: expansion + transfer_manifest JSON + duplicate checks (no UI)."""

import unittest

from ozlink_console.graph_folder_execution_expansion import expand_graph_transfer_steps
from ozlink_console.transfer_manifest import (
    build_simulation_manifest,
    expanded_graph_steps_to_transfer_step_json_dicts,
)


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


class ManifestGraphExpansionIntegrationTests(unittest.TestCase):
    def test_clean_folder_expansion_one_row_matches_planned_move(self):
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
        expanded = expand_graph_transfer_steps(moves, set(), **h)
        rows = expanded_graph_steps_to_transfer_step_json_dicts(moves, expanded)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["planned_move_index"], 0)
        self.assertTrue(rows[0]["is_source_folder"])

    def test_dirty_nested_clean_subfolder_single_folder_step_in_json(self):
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
        expanded = expand_graph_transfer_steps(moves, excl, **h)
        rows = expanded_graph_steps_to_transfer_step_json_dicts(moves, expanded)
        folder_rows = [r for r in rows if r.get("is_source_folder")]
        self.assertEqual(len(folder_rows), 1)
        self.assertEqual(folder_rows[0]["planned_move_index"], 1)

    def test_override_and_exclusion_build_simulation_manifest_embeds_expanded(self):
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
        excl = {"src\\A\\skip.txt"}
        expanded = expand_graph_transfer_steps(moves, excl, **h)
        rows = expanded_graph_steps_to_transfer_step_json_dicts(moves, expanded)
        m = build_simulation_manifest(
            planned_moves=moves,
            proposed_folders=[],
            draft_id="D1",
            manifest_version=2,
            plan_leaf_exclusions=list(excl),
            graph_unsafe_folder_step_indices=[0],
            graph_expanded_transfer_steps=rows,
        )
        self.assertEqual(len(m["transfer_steps"]), 2)
        self.assertEqual(len(m["execution_options"]["graph_expanded_transfer_steps"]), 1)
        self.assertEqual(m["execution_options"]["graph_expanded_transfer_steps"][0]["planned_move_index"], 1)


if __name__ == "__main__":
    unittest.main()
