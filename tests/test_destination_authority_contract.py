"""Pure unit tests for SharePoint destination structural authority contract."""

from __future__ import annotations

from ozlink_console import destination_authority_contract as dac


class _Host:
    def __init__(self, *, dest_mode: str = "local", has_model: bool = True) -> None:
        self._dest_mode = dest_mode
        self.destination_planning_model = object() if has_model else None

    def _planning_browse_mode(self, key: str) -> str:
        return self._dest_mode if key == "destination" else "local"


def test_graph_owns_structure_when_sharepoint_and_model_present():
    h = _Host(dest_mode="sharepoint", has_model=True)
    assert dac.sharepoint_planning_tree_active(h) is True
    assert dac.graph_owns_visible_real_destination_structure(h) is True
    assert dac.future_model_bind_may_insert_visible_real_rows(h) is False
    assert dac.full_tree_snapshot_may_author_visible_real_rows(h) is False


def test_local_destination_future_bind_may_insert_real():
    h = _Host(dest_mode="local", has_model=True)
    assert dac.graph_owns_visible_real_destination_structure(h) is False
    assert dac.future_model_bind_may_insert_visible_real_rows(h) is True


def test_sharepoint_without_planning_model_not_graph_owned():
    h = _Host(dest_mode="sharepoint", has_model=False)
    assert dac.sharepoint_planning_tree_active(h) is False
    assert dac.graph_owns_visible_real_destination_structure(h) is False
