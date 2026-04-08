"""Left-nav access: guest (signed out) vs user vs admin."""

from __future__ import annotations

from ozlink_console.main_window import MainWindow


def _minimal_main_window():
    mw = MainWindow.__new__(MainWindow)
    mw.nav_allowed_by_role = {
        "guest": ["Dashboard"],
        "user": ["Dashboard", "Planning Workspace", "Audit"],
        "admin": [
            "Dashboard",
            "Planning Workspace",
            "Settings",
            "Audit",
            "Execution",
            "Requests",
        ],
    }
    return mw


def test_navigation_role_guest_when_not_connected():
    mw = _minimal_main_window()
    mw.current_session_context = {"connected": False, "user_role": "admin"}
    assert mw._navigation_role() == "guest"


def test_navigation_role_respects_user_role_when_connected():
    mw = _minimal_main_window()
    mw.current_session_context = {"connected": True, "user_role": "user"}
    assert mw._navigation_role() == "user"
    mw.current_session_context["user_role"] = "admin"
    assert mw._navigation_role() == "admin"


def test_navigation_role_unknown_connected_defaults_to_user():
    mw = _minimal_main_window()
    mw.current_session_context = {"connected": True, "user_role": "bogus"}
    assert mw._navigation_role() == "user"


def test_is_page_allowed_matches_navigation_role():
    mw = _minimal_main_window()
    mw.current_session_context = {"connected": False, "user_role": "user"}
    assert mw._is_page_allowed("Dashboard")
    assert not mw._is_page_allowed("Audit")
    mw.current_session_context["connected"] = True
    assert mw._is_page_allowed("Audit")
    assert not mw._is_page_allowed("Settings")
    mw.current_session_context["user_role"] = "admin"
    assert mw._is_page_allowed("Settings")
