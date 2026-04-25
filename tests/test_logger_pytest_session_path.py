"""Session log directory must not pollute the primary Logs folder during pytest."""

from __future__ import annotations


def test_init_session_logging_routes_pytest_under_subdir():
    from ozlink_console import logger as lg

    assert lg.running_under_pytest() is True
    lg.reset_logging_for_tests()
    d = lg.init_session_logging()
    norm = str(d.resolve()).replace("\\", "/")
    assert "/_pytest_sessions/" in norm or "\\_pytest_sessions\\" in str(d.resolve())
