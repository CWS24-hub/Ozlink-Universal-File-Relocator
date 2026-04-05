"""Execution-safe planning precedence: direct move > exclusion > inherited ancestor."""

from __future__ import annotations

import unittest

from ozlink_console.planning_resolution import (
    find_inherited_planned_move_for_source_path_raw,
    path_is_descendant_canonical_paths,
    resolve_effective_planning,
)


def _canon_move_source(move: dict) -> str:
    return str(move.get("source_path") or "").replace("/", "\\").strip()


class PlanningResolutionExecutionTests(unittest.TestCase):
    def test_excluded_leaf_blocks_inherited(self):
        planned = [
            {
                "source_path": r"p\folder",
                "source": {"is_folder": True},
                "destination_path": r"d\dest",
            }
        ]
        excl = {r"p\folder\leaf.txt"}
        r = resolve_effective_planning(
            r"p\folder\leaf.txt",
            planned,
            excl,
            canonicalize_move_source=_canon_move_source,
            path_is_descendant=path_is_descendant_canonical_paths,
        )
        self.assertEqual(r.kind, "excluded")
        self.assertIsNone(r.direct_move)
        self.assertIsNone(r.inherited_move)

        raw = find_inherited_planned_move_for_source_path_raw(
            r"p\folder\leaf.txt",
            planned,
            _canon_move_source,
            path_is_descendant_canonical_paths,
        )
        self.assertIsNotNone(raw)

    def test_direct_exact_move_wins_over_exclusion_list(self):
        planned = [
            {
                "source_path": r"p\folder",
                "source": {"is_folder": True},
                "destination_path": r"d\dest",
            },
            {
                "source_path": r"p\folder\special.txt",
                "source": {"is_folder": False},
                "destination_path": r"d\other\special.txt",
            },
        ]
        excl = {r"p\folder\special.txt"}
        r = resolve_effective_planning(
            r"p\folder\special.txt",
            planned,
            excl,
            canonicalize_move_source=_canon_move_source,
            path_is_descendant=path_is_descendant_canonical_paths,
        )
        self.assertEqual(r.kind, "direct")
        self.assertEqual(r.direct_move["destination_path"], r"d\other\special.txt")

    def test_inherited_non_excluded_sibling(self):
        planned = [
            {
                "source_path": r"p\folder",
                "source": {"is_folder": True},
                "destination_path": r"d\dest",
            }
        ]
        excl = {r"p\folder\excluded.bin"}
        r = resolve_effective_planning(
            r"p\folder\keep.bin",
            planned,
            excl,
            canonicalize_move_source=_canon_move_source,
            path_is_descendant=path_is_descendant_canonical_paths,
        )
        self.assertEqual(r.kind, "inherited")
        self.assertIsNotNone(r.inherited_move)
        self.assertEqual(r.inherited_move["source_path"], r"p\folder")

    def test_mixed_folder_direct_exclusion_and_inherited_children(self):
        planned = [
            {
                "source_path": r"lib\batch",
                "source": {"is_folder": True},
                "destination_path": r"dst\batch",
            },
            {
                "source_path": r"lib\batch\one.txt",
                "source": {"is_folder": False},
                "destination_path": r"dst\override\one.txt",
            },
        ]
        excl = {r"lib\batch\skip.dat"}
        r_one = resolve_effective_planning(
            r"lib\batch\one.txt",
            planned,
            excl,
            canonicalize_move_source=_canon_move_source,
            path_is_descendant=path_is_descendant_canonical_paths,
        )
        self.assertEqual(r_one.kind, "direct")

        r_skip = resolve_effective_planning(
            r"lib\batch\skip.dat",
            planned,
            excl,
            canonicalize_move_source=_canon_move_source,
            path_is_descendant=path_is_descendant_canonical_paths,
        )
        self.assertEqual(r_skip.kind, "excluded")

        r_two = resolve_effective_planning(
            r"lib\batch\two.txt",
            planned,
            excl,
            canonicalize_move_source=_canon_move_source,
            path_is_descendant=path_is_descendant_canonical_paths,
        )
        self.assertEqual(r_two.kind, "inherited")
        self.assertEqual(r_two.inherited_move["source_path"], r"lib\batch")


if __name__ == "__main__":
    unittest.main()
