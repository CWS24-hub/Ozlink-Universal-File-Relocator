"""MSAL persistent token cache (process restart / silent session)."""

from __future__ import annotations

import msal

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


def test_msal_token_cache_not_loaded_from_disk_when_not_dev(tmp_path, monkeypatch):
    monkeypatch.setattr("ozlink_console.graph.msal_token_cache_path", lambda: tmp_path / "msal_token_cache.json")
    monkeypatch.setattr("ozlink_console.graph.is_dev_mode", lambda: False)
    deserialized = []

    def _capture_deserialize(self, blob):
        deserialized.append(blob)
        return msal.SerializableTokenCache.deserialize(self, blob)

    monkeypatch.setattr(msal.SerializableTokenCache, "deserialize", _capture_deserialize)
    cache_file = tmp_path / "msal_token_cache.json"
    cache_file.write_text('{"AccessToken": {}}', encoding="utf-8")

    GraphClient()._ensure_app()
    assert deserialized == []


def test_msal_token_cache_not_persisted_to_disk_when_not_dev(tmp_path, monkeypatch):
    monkeypatch.setattr("ozlink_console.graph.msal_token_cache_path", lambda: tmp_path / "msal_token_cache.json")
    monkeypatch.setattr("ozlink_console.graph.is_dev_mode", lambda: False)
    cache_file = tmp_path / "msal_token_cache.json"
    client = GraphClient()
    client._ensure_app()
    assert client._token_cache is not None
    client._token_cache.has_state_changed = True
    client._persist_msal_token_cache()
    assert not cache_file.exists()
    assert client._token_cache.has_state_changed is False
