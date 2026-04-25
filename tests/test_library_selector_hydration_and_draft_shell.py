from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ozlink_console.models import SessionState
from ozlink_console.main_window import MainWindow


def _minimal_shell_with_libraries(source_drive: str, dest_drive: str) -> SessionState:
    s = SessionState()
    s.SelectedSourceSite = "S"
    s.SelectedSourceSiteKey = "site-key"
    s.SelectedSourceLibrary = "SrcLib"
    s.SelectedSourceLibraryId = source_drive
    s.SelectedDestinationSite = "D"
    s.SelectedDestinationSiteKey = "site-key-d"
    s.SelectedDestinationLibrary = "Documents"
    s.SelectedDestinationLibraryId = dest_drive
    return s


@pytest.fixture
def mw() -> MainWindow:
    m = MainWindow.__new__(MainWindow)
    m._draft_shell_state = _minimal_shell_with_libraries("drive-src-111", "drive-dst-documents-222")
    m._saved_library_selector_hydration_attempt = {"source": 0, "destination": 0}
    m.planning_inputs = {"Source Site": MagicMock(), "Source Library": MagicMock(), "Destination Site": MagicMock(), "Destination Library": MagicMock()}
    return m


def test_merge_preserves_destination_id_when_capture_empty(mw: MainWindow):
    existing = mw._draft_shell_state
    sel = MagicMock()
    sel.count.return_value = 3
    sel.currentData.return_value = None
    sel.currentIndex.return_value = -1
    sel.itemData.return_value = None
    name, did = mw._merge_persisted_library_fields_for_draft_shell(
        group="destination",
        existing=existing,
        site_key_new=str(getattr(existing, "SelectedDestinationSiteKey", "") or ""),
        library_selector=sel,
        name_new="",
        id_new="",
    )
    assert did == "drive-dst-documents-222"
    assert name == "Documents"


def test_merge_preserves_source_id_when_capture_empty(mw: MainWindow):
    existing = mw._draft_shell_state
    sel = MagicMock()
    sel.count.return_value = 2
    sel.currentData.return_value = None
    sel.currentIndex.return_value = -1
    name, did = mw._merge_persisted_library_fields_for_draft_shell(
        group="source",
        existing=existing,
        site_key_new=str(getattr(existing, "SelectedSourceSiteKey", "") or ""),
        library_selector=sel,
        name_new="",
        id_new="",
    )
    assert did == "drive-src-111"


def test_planning_defer_when_saved_drive_not_in_combo_items(mw: MainWindow):
    lib = MagicMock()
    lib.count.return_value = 1
    lib.itemData.side_effect = lambda i: {"id": "wrong-other", "name": "FTBM"} if i == 0 else None
    sel_lib = {"id": "wrong-other", "name": "FTBM"}
    r = mw._planning_should_defer_or_block_wrong_library_bind(
        "destination", selected_library=sel_lib, library_selector=lib
    )
    assert r == "deferred"


def test_planning_block_when_wrong_selection_but_saved_in_combo(mw: MainWindow):
    lib = MagicMock()
    lib.count.return_value = 2
    lib.itemData.side_effect = lambda i: (
        {"id": "wrong", "name": "A"}
        if i == 0
        else {"id": "drive-dst-documents-222", "name": "Documents"}
        if i == 1
        else None
    )
    sel_lib = {"id": "wrong", "name": "A"}
    mw._draft_shell_state.SelectedDestinationLibraryId = "drive-dst-documents-222"
    r = mw._planning_should_defer_or_block_wrong_library_bind(
        "destination", selected_library=sel_lib, library_selector=lib
    )
    assert r == "blocked"


def test_merge_preserves_names_when_combo_text_is_loading_placeholder(mw: MainWindow):
    """Hydration shows Loading SharePoint... — do not overwrite saved library display names."""
    existing = mw._draft_shell_state
    sel = MagicMock()
    sel.count.return_value = 1
    sel.currentText.return_value = "Loading SharePoint..."
    sel.currentData.return_value = None
    sel.currentIndex.return_value = -1
    name, did = mw._merge_persisted_library_fields_for_draft_shell(
        group="source",
        existing=existing,
        site_key_new=str(getattr(existing, "SelectedSourceSiteKey", "") or ""),
        library_selector=sel,
        name_new="Loading SharePoint...",
        id_new="",
    )
    assert did == "drive-src-111"
    assert name == "SrcLib"


def test_sharepoint_site_resolved_requires_graph_site_id(mw: MainWindow):
    site = MagicMock()
    site.currentData.return_value = {"libraries": [], "name": "X"}
    assert mw._planning_sharepoint_site_selector_resolved_for_libraries(site) is False
    site.currentData.return_value = {"id": "guid-1", "libraries": []}
    assert mw._planning_sharepoint_site_selector_resolved_for_libraries(site) is True


def test_restore_persistence_simulator_reopen_merge(mw: MainWindow):
    """Simulate: save during hydration (blank capture) then after reopen merge — IDs and names stable."""
    shell = mw._draft_shell_state
    sel = MagicMock()
    sel.count.return_value = 0
    sel.currentData.return_value = None
    sel.currentText.return_value = "Not selected"
    sel.currentIndex.return_value = -1
    for group, key_a, key_i, sk in (
        ("source", "SelectedSourceLibrary", "SelectedSourceLibraryId", "SelectedSourceSiteKey"),
        ("destination", "SelectedDestinationLibrary", "SelectedDestinationLibraryId", "SelectedDestinationSiteKey"),
    ):
        nm, did = mw._merge_persisted_library_fields_for_draft_shell(
            group=group,
            existing=shell,
            site_key_new=str(getattr(shell, sk, "") or ""),
            library_selector=sel,
            name_new="",
            id_new="",
        )
        assert did == str(getattr(shell, key_i, "") or "")
        assert nm == str(getattr(shell, key_a, "") or "")
