"""Planning workspace: dedicated Preview tab and explicit tree Preview actions."""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock

from PySide6.QtWidgets import QApplication

from ozlink_console.main_window import MainWindow


def _qapp():
    return QApplication.instance() or QApplication([])


def _minimal_context(panel_key="source"):
    return {
        "panel_key": panel_key,
        "node_data": {"name": "a.txt", "id": "1"},
        "traceability": {},
        "planning_state": {},
        "metadata": {
            "item_name": "a.txt",
            "item_path": "p",
            "item_type": "File",
            "item_area": panel_key,
            "node_origin": "",
            "planning_state": "",
            "destination_path": "",
            "source_path": "",
            "item_size": "",
            "item_modified": "",
            "library_context": "",
            "item_link": "",
        },
        "notes_preview": {"notes_text": "", "body_text": "", "preview_text": "summary"},
        "actions": {},
    }


def test_preview_action_in_source_and_destination_context_menu_sources():
    src = inspect.getsource(MainWindow.show_source_context_menu)
    assert 'menu.addAction("Preview")' in src
    assert "_handle_context_menu_preview" in src and '"source"' in src

    dst = inspect.getsource(MainWindow.show_destination_context_menu)
    assert 'menu.addAction("Preview")' in dst
    assert "_handle_context_menu_preview" in dst and '"destination"' in dst


def test_open_file_actions_still_present_in_context_menus():
    src = inspect.getsource(MainWindow.show_source_context_menu)
    assert 'menu.addAction("Open File")' in src
    assert "handle_open_source_item" in src

    dst = inspect.getsource(MainWindow.show_destination_context_menu)
    assert 'menu.addAction("Open File")' in dst
    assert "handle_open_selected_file" in dst


def test_handle_context_menu_preview_switches_tab_and_starts_load():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._workspace_tabs_collapsed = False
    mw.preview_tab_box = MagicMock()
    mw.workspace_tabs = MagicMock()
    ctx = _minimal_context("source")
    mw._resolve_selected_item_context = MagicMock(return_value=ctx)
    mw._update_selection_details = MagicMock()
    mw._start_preview_tab_load = MagicMock()
    mw._apply_workspace_tabs_collapsed_state = MagicMock()

    MainWindow._handle_context_menu_preview(mw, "source", {"name": "a.txt"})

    mw._resolve_selected_item_context.assert_called_once()
    mw._update_selection_details.assert_called_once_with(ctx, invalidate_preview_on_selection=False)
    mw.workspace_tabs.setCurrentWidget.assert_called_once_with(mw.preview_tab_box)
    mw._start_preview_tab_load.assert_called_once_with(ctx)
    mw._apply_workspace_tabs_collapsed_state.assert_not_called()


def test_handle_context_menu_preview_expands_bottom_panel_when_collapsed():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._workspace_tabs_collapsed = True
    mw._apply_workspace_tabs_collapsed_state = MagicMock()
    mw.preview_tab_box = MagicMock()
    mw.workspace_tabs = MagicMock()
    ctx = _minimal_context("source")
    mw._resolve_selected_item_context = MagicMock(return_value=ctx)
    mw._update_selection_details = MagicMock()
    mw._start_preview_tab_load = MagicMock()

    MainWindow._handle_context_menu_preview(mw, "source", {"name": "a.txt"})

    mw._apply_workspace_tabs_collapsed_state.assert_called_once_with(False)


def test_preview_tab_panel_stack_states():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    # Keep the returned root widget alive; otherwise the stacked widget is destroyed when `box` is GC'd.
    mw._preview_tab_root = MainWindow.build_preview_tab_panel(mw)
    assert mw.preview_stack.count() == 4

    mw._preview_tab_set_loading()
    assert mw.preview_stack.currentIndex() == mw._preview_stack_loading

    mw._preview_tab_set_unsupported("Cannot preview this item.")
    assert mw.preview_stack.currentIndex() == mw._preview_stack_unsupported

    mw._preview_tab_set_content("Title", "/path", "body")
    assert mw.preview_stack.currentIndex() == mw._preview_stack_content
    assert mw.preview_body_text.toPlainText() == "body"
    assert mw.preview_body_stack.currentIndex() == 0

    mw._preview_tab_set_empty_state("Custom empty.")
    assert mw.preview_stack.currentIndex() == mw._preview_stack_empty
    assert "Custom empty." in mw.preview_empty_label.text()


def test_on_preview_success_image_shows_image_stack_not_text_fallback():
    _qapp()
    from PySide6.QtCore import QByteArray, QBuffer, QIODevice
    from PySide6.QtGui import QColor, QPixmap

    mw = MainWindow.__new__(MainWindow)
    mw._preview_text_cache = {}
    mw._preview_image_bytes_cache = {}
    mw._preview_tab_root = MainWindow.build_preview_tab_panel(mw)
    mw._active_preview_request_id = 1

    pm = QPixmap(12, 10)
    pm.fill(QColor("red"))
    ba = QByteArray()
    buf = QBuffer(ba)
    buf.open(QIODevice.OpenModeFlag.WriteOnly)
    pm.save(buf, "PNG")
    png_bytes = bytes(ba.data())

    ctx = _minimal_context()
    ctx["metadata"]["item_name"] = "photo.png"
    MainWindow.on_preview_success(
        mw,
        {"drive_id": "d1", "item_id": "i1", "item_name": "photo.png", "content": png_bytes},
        1,
        ctx,
    )
    assert mw.preview_stack.currentIndex() == mw._preview_stack_content
    assert mw.preview_body_stack.currentIndex() == 1
    assert mw.preview_image_label.pixmap() is not None
    assert not mw.preview_image_label.pixmap().isNull()
    assert ("preview summary" not in mw.preview_body_text.toPlainText().lower()) or mw.preview_body_text.toPlainText() == ""


def test_on_preview_success_unsupported_type_still_uses_text_fallback():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._preview_text_cache = {}
    mw._preview_image_bytes_cache = {}
    mw._preview_tab_root = MainWindow.build_preview_tab_panel(mw)
    mw._active_preview_request_id = 1
    ctx = _minimal_context()
    ctx["metadata"]["item_name"] = "blob.bin"
    MainWindow.on_preview_success(
        mw,
        {"drive_id": "d1", "item_id": "i1", "item_name": "blob.bin", "content": b"\x00\x01\x02\xff"},
        1,
        ctx,
    )
    assert mw.preview_body_stack.currentIndex() == 0
    body = mw.preview_body_text.toPlainText()
    assert "preview summary" in body.lower() or "not available" in body.lower()


def test_update_selection_details_invalidates_preview_by_default():
    mw = MainWindow.__new__(MainWindow)
    mw._display_detail_value = MainWindow._display_detail_value.__get__(mw, MainWindow)
    mw._invalidate_preview_on_selection_change = MagicMock()
    mw.details_metadata_summary = MagicMock()
    mw.details_notes = MagicMock()
    ctx = _minimal_context()

    MainWindow._update_selection_details(mw, ctx)

    mw._invalidate_preview_on_selection_change.assert_called_once()


def test_update_selection_details_can_skip_preview_invalidate_for_explicit_preview():
    mw = MainWindow.__new__(MainWindow)
    mw._display_detail_value = MainWindow._display_detail_value.__get__(mw, MainWindow)
    mw._invalidate_preview_on_selection_change = MagicMock()
    mw.details_metadata_summary = MagicMock()
    mw.details_notes = MagicMock()
    ctx = _minimal_context()

    MainWindow._update_selection_details(mw, ctx, invalidate_preview_on_selection=False)

    mw._invalidate_preview_on_selection_change.assert_not_called()
