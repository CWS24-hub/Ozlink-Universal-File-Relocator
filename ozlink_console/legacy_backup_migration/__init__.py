"""Legacy backup migration / preflight (sandboxed, non-destructive)."""

from __future__ import annotations

from .engine import migrate_legacy_backup_folder
from .errors import LegacyBackupDirectRestoreBlocked
from .shape import is_legacy_shaped_bundle
from .types import MigrationIdentityPreflight, MigrationResult

__all__ = [
    "LegacyBackupDirectRestoreBlocked",
    "MigrationIdentityPreflight",
    "MigrationResult",
    "is_legacy_shaped_bundle",
    "migrate_legacy_backup_folder",
]
