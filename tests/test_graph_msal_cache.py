"""MSAL persistent token cache (process restart / silent session)."""

from __future__ import annotations

from ozlink_console.graph import GraphClient


def test_disconnect_removes_msal_cache_file(tmp_path, monkeypatch):
    monkeypatch.setattr("ozlink_console.graph.msal_token_cache_path", lambda: tmp_path / "msal_token_cache.json")
    cache_path = tmp_path / "msal_token_cache.json"
    cache_path.write_text("{}", encoding="utf-8")
    GraphClient().disconnect()
    assert not cache_path.exists()


def test_msal_token_cache_path_under_cache_root():
    from ozlink_console.paths import cache_root, msal_token_cache_path

    p = msal_token_cache_path()
    assert p.parent == cache_root()
    assert p.name == "msal_token_cache.json"
