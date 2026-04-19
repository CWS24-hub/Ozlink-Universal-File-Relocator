from ozlink_console.planning_interaction_contract import is_local_first_deferred_materialize_reason


def test_planned_item_moved_is_local_first_followup():
    assert is_local_first_deferred_materialize_reason("planned_item_moved") is True
    assert is_local_first_deferred_materialize_reason("deferred_reconcile_folder_worker_success") is False
    assert is_local_first_deferred_materialize_reason("") is False
