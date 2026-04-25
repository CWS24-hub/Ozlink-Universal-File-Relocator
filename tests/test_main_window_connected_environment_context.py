"""Regression: _build_connected_environment_context must not assume pending_root_site_ids exists."""

from __future__ import annotations

from ozlink_console.main_window import MainWindow


class _HostWithoutSiteIds:
    """Minimal host: planning context + session only (no pending_root_site_ids)."""

    current_session_context = {"tenant_id": "t1", "tenant_domain": "example.com", "client_key": "ck"}

    def _graph_resolve_planning_context(self):
        return {
            "source_drive_id": "d-src",
            "dest_drive_id": "d-dst",
            "source_site_name": "S",
            "source_library_name": "L1",
            "dest_site_name": "D",
            "dest_library_name": "L2",
        }


def test_build_connected_environment_context_without_pending_root_site_ids():
    host = _HostWithoutSiteIds()
    ctx = MainWindow._build_connected_environment_context(host)
    assert ctx["tenant_id"] == "t1"
    assert ctx["source_drive_id"] == "d-src"
    assert ctx["destination_drive_id"] == "d-dst"
    assert ctx["source_site_id"] == ""
    assert ctx["destination_site_id"] == ""
    assert ctx["source_site_name"] == "S"
    assert ctx["destination_site_name"] == "D"


def test_build_connected_environment_context_with_malformed_site_ids():
    class _Bad:
        current_session_context = {}
        pending_root_site_ids = "not-a-dict"  # type: ignore[assignment]

        def _graph_resolve_planning_context(self):
            return {}

    ctx = MainWindow._build_connected_environment_context(_Bad())
    assert ctx["source_site_id"] == ""
    assert ctx["destination_site_id"] == ""
