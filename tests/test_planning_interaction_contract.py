from ozlink_console.planning_interaction_contract import (
    is_local_first_deferred_materialize_reason,
    is_narrow_planned_item_move_overlay_reason,
)


def test_planned_item_moved_is_local_first_followup():
    assert is_local_first_deferred_materialize_reason("planned_item_moved") is True
    assert is_local_first_deferred_materialize_reason("planned_item_moved_manual_drag") is True
    assert is_local_first_deferred_materialize_reason("deferred_planned_item_moved_manual_drag") is True
    assert is_local_first_deferred_materialize_reason("local_first_edit_rename") is True
    assert is_local_first_deferred_materialize_reason("local_first_edit_assign") is True
    assert is_local_first_deferred_materialize_reason("deferred_reconcile_folder_worker_success") is False
    assert is_local_first_deferred_materialize_reason("") is False


def test_manual_drag_narrow_overlay_reason():
    assert is_narrow_planned_item_move_overlay_reason("planned_item_moved_manual_drag") is True
    assert is_narrow_planned_item_move_overlay_reason("deferred_planned_item_moved_manual_drag") is True
    assert is_narrow_planned_item_move_overlay_reason("planned_item_moved") is True
    assert is_narrow_planned_item_move_overlay_reason("folder_worker_success") is False
