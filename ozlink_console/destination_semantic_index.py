"""
Destination future-model topology helpers (Phase 3 incremental projection).

Pure functions over the in-memory ``model_nodes`` dict used by
``_build_destination_future_model`` / incremental merge. Keeps sort and
entry-root rules aligned with MainWindow without Qt dependencies.
"""

from __future__ import annotations

from typing import Any

# Mirrors MainWindow._destination_future_state_rank for sort keys.
_STATE_RANK = {
    "projected": 0,
    "proposed": 1,
    "allocated": 2,
}


def path_segments(path: str) -> list[str]:
    return [p for p in str(path or "").replace("/", "\\").split("\\") if p]


def _future_state_rank(node_state: str) -> int:
    return _STATE_RANK.get(str(node_state or ""), 3)


def future_model_sort_key(model_nodes: dict[str, Any], semantic_path: str) -> tuple:
    """Same ordering as MainWindow._destination_model_sort_key (tuple sort)."""
    node = model_nodes[semantic_path]
    data = node["data"]
    return (
        not bool(data.get("is_folder", False)),
        _future_state_rank(node["node_state"]),
        str(node["name"]).lower(),
    )


def compute_incremental_merge_entry_roots(model_nodes: dict[str, Any], new_paths: set[str]) -> list[str]:
    """
    Entry roots: new paths whose parent is not also new (attach each subtree once).
    Sort matches MainWindow._incremental_merge_destination_future_projection.
    """
    entry_roots = [
        p
        for p in new_paths
        if str(model_nodes.get(p, {}).get("parent_semantic_path", "") or "") not in new_paths
    ]
    entry_roots.sort(
        key=lambda path: (
            len(path_segments(path)),
            [s.lower() for s in path_segments(path)],
        )
    )
    return entry_roots


def group_paths_by_parent_semantic(
    paths: list[str],
    model_nodes: dict[str, Any],
) -> dict[str, list[str]]:
    """Group semantic paths by parent_semantic_path; sort each group for stable merge order."""
    out: dict[str, list[str]] = {}
    for p in paths:
        parent = str(model_nodes.get(p, {}).get("parent_semantic_path", "") or "")
        out.setdefault(parent, []).append(p)
    for parent in out:
        out[parent].sort(key=lambda child: future_model_sort_key(model_nodes, child))
    return out
