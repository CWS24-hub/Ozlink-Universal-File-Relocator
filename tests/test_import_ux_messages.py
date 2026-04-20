"""Tests for Import Draft / migration report user-facing messages (UX mapping only)."""

from __future__ import annotations

import pytest

from ozlink_console.draft_import.import_ux_messages import (
    user_message_for_import_blocked_exception,
    user_message_for_import_bundle_classification_error,
    user_message_for_migrated_validation_error,
)
from ozlink_console.legacy_backup_migration.errors import LegacyBackupDirectRestoreBlocked


def test_classification_missing_required_file() -> None:
    msg = user_message_for_import_bundle_classification_error("missing:Draft-SessionState.json")
    assert "Draft-SessionState.json" in msg
    assert "required" in msg.lower()


def test_classification_migration_report_read_error() -> None:
    msg = user_message_for_import_bundle_classification_error("migration_report:bad encoding")
    assert "migration report" in msg.lower()


def test_validation_rows_rejected_friendly() -> None:
    raw = "Migration report shows rows_rejected=3; confirm to import."
    assert "rejected" in user_message_for_migrated_validation_error(raw).lower()


def test_validation_offline_confirm() -> None:
    raw = "Migration was run with Graph resolution disabled; confirm to import."
    m = user_message_for_migrated_validation_error(raw)
    assert "graph" in m.lower() or "offline" in m.lower() or "resolution" in m.lower()


def test_validation_identity_placeholder_drives() -> None:
    raw = "Migration report has missing or invalid source/destination drive ids."
    m = user_message_for_migrated_validation_error(raw)
    assert "drive" in m.lower()


def test_blocked_exception_raw_legacy() -> None:
    msg = user_message_for_import_blocked_exception(LegacyBackupDirectRestoreBlocked("x", reasons=[]))
    assert msg is not None
    assert "migrat" in msg.lower()


def test_blocked_exception_unknown_returns_none() -> None:
    assert user_message_for_import_blocked_exception(RuntimeError("nope")) is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
