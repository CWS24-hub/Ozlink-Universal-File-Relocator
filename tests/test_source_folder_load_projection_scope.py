from __future__ import annotations

import types

from ozlink_console.main_window import MainWindow


class _FolderLoadHost:
    def __init__(self) -> None:
        self.planned_moves: list[dict] = [
            {
                "source_path": "OtherLib\\Unrelated\\file.txt",
            }
        ]
        self.proposed_folders: list = []
        self._source_path_is_strict_descendant_of_folder = types.MethodType(
            MainWindow._source_path_is_strict_descendant_of_folder, self
        )

    def _canonical_source_projection_path(self, p: str) -> str:
        return str(p or "").replace("/", "\\").strip().strip()


def test_planning_intersection_empty_when_moves_elsewhere() -> None:
    h = _FolderLoadHost()
    r = MainWindow._source_folder_load_planning_paths_intersecting(  # type: ignore[misc]
        h,
        "MyLib\\BigFolder",
    )
    assert r == set()


def test_planning_intersection_finds_planned_descendant() -> None:
    h = _FolderLoadHost()
    h.planned_moves = [
        {
            "source_path": "MyLib\\BigFolder\\Child\\doc.pdf",
        }
    ]
    r = MainWindow._source_folder_load_planning_paths_intersecting(  # type: ignore[misc]
        h,
        "MyLib\\BigFolder",
    )
    assert "MyLib\\BigFolder\\Child\\doc.pdf" in r


def test_path_under_folder_parent_equal() -> None:
    h = _FolderLoadHost()
    assert MainWindow._source_path_is_strict_descendant_of_folder(  # type: ignore[misc]
        h,
        "A\\B",
        "A\\B",
    )
    assert MainWindow._source_path_is_strict_descendant_of_folder(  # type: ignore[misc]
        h,
        "A\\B",
        "A\\B\\C",
    )
    assert not MainWindow._source_path_is_strict_descendant_of_folder(  # type: ignore[misc]
        h,
        "A\\B",
        "Z\\Other",
    )


def test_merge_subtree_scopes() -> None:
    assert (
        MainWindow._merge_source_projection_subtree_scopes(  # type: ignore[misc]
            "roots_only", "explicit_paths"
        )
        == "explicit_paths"
    )
    assert MainWindow._merge_source_projection_subtree_scopes("full", "roots_only") == "full"  # type: ignore[misc]
    assert MainWindow._merge_source_projection_subtree_scopes("roots_only", "roots_only") == "roots_only"  # type: ignore[misc]


def test_proposed_parent_intersects() -> None:
    from types import SimpleNamespace

    h = _FolderLoadHost()
    h.planned_moves = []
    h.proposed_folders = [
        SimpleNamespace(
            ParentPath="Site\\Lib\\Sub\\proposed",
            FolderName="x",
            DestinationPath="D",
        ),
    ]
    r = MainWindow._source_folder_load_planning_paths_intersecting(h, "Site\\Lib\\Sub")
    assert "Site\\Lib\\Sub\\proposed" in r
