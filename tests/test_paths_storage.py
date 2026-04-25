"""Storage layout: ensure directories exist."""

from __future__ import annotations

from ozlink_console.paths import (
    appdata_root,
    ensure_app_storage_directories,
    exports_root,
    graph_cache_root,
    legacy_memory_root,
    logs_root,
    memory_root,
    requests_root,
)


def test_ensure_app_storage_directories_is_idempotent():
    ensure_app_storage_directories()
    ensure_app_storage_directories()
    base = appdata_root()
    assert base.is_dir()
    assert logs_root().is_dir()
    assert memory_root().is_dir()
    assert exports_root().is_dir()
    assert requests_root().is_dir()
    assert graph_cache_root().is_dir()
    assert legacy_memory_root().is_dir()


def test_memory_manager_ensure_creates_user_scoped_tree(tmp_path):
    from ozlink_console.memory import MemoryManager

    mm = MemoryManager.__new__(MemoryManager)
    mm.storage_scope_root = tmp_path / "scope"
    mm.root = mm.storage_scope_root / "Memory"
    mm.backups = mm.root / "Backups"
    mm.quarantine = mm.root / "Quarantine"
    mm.exports = mm.storage_scope_root / "Exports"
    mm.legacy_root = tmp_path / "legacy" / "Memory"
    mm._ensure_memory_directories()
    assert mm.workspace_reset_backups_dir.is_dir()
    assert mm.exports.is_dir()
    assert mm.legacy_root.is_dir()
