"""
Classification for unresolved destination overlay queue (proposed / allocation) lifecycle.

Entries may leave ``unresolved_*_by_parent_path`` only with a logged reason and a verified
visible-tree outcome (unless explicitly exempted, e.g. user retarget or queue rebuild).
"""

from __future__ import annotations

from enum import StrEnum


class UnresolvedQueueRemovalReason(StrEnum):
    """Why an entry was removed from an unresolved overlay queue."""

    bound_planned_chain_visible = "bound_planned_chain_visible"
    attached_to_existing_live_row = "attached_to_existing_live_row"
    merged_to_equivalent_visible_planned_row = "merged_to_equivalent_visible_planned_row"
    merged_to_equivalent_live_row = "merged_to_equivalent_live_row"
    deferred_parent_missing = "deferred_parent_missing"
    deferred_parent_not_loaded = "deferred_parent_not_loaded"
    deferred_suffix_mismatch = "deferred_suffix_mismatch"
    illegal_resolution_without_visible_outcome = "illegal_resolution_without_visible_outcome"
    planning_memory_queue_rebuilt = "planning_memory_queue_rebuilt"
    user_planned_move_retargeted = "user_planned_move_retargeted"
    removed_from_tree_user_action = "removed_from_tree_user_action"


def legal_overlay_resolution_reasons() -> frozenset[UnresolvedQueueRemovalReason]:
    return frozenset(
        {
            UnresolvedQueueRemovalReason.bound_planned_chain_visible,
            UnresolvedQueueRemovalReason.attached_to_existing_live_row,
            UnresolvedQueueRemovalReason.merged_to_equivalent_visible_planned_row,
            UnresolvedQueueRemovalReason.merged_to_equivalent_live_row,
            UnresolvedQueueRemovalReason.planning_memory_queue_rebuilt,
            UnresolvedQueueRemovalReason.user_planned_move_retargeted,
            UnresolvedQueueRemovalReason.removed_from_tree_user_action,
        }
    )
