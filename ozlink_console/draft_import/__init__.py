"""Draft bundle import classification, validation, and migration conflict helpers."""

from __future__ import annotations

from ozlink_console.draft_import.classification import (
    INVALID_UNKNOWN,
    MIGRATED_LEGACY_PACKAGE,
    MODERN_EXPORT,
    RAW_LEGACY_EXPORT,
    classify_import_bundle,
)
from ozlink_console.draft_import.conflicts import load_migration_conflicts_for_review
from ozlink_console.draft_import.migrated_validation import (
    MigratedImportValidationResult,
    is_valid_migration_drive_id,
    validate_migrated_import_bundle,
)
from ozlink_console.draft_import.summary import build_migration_report_summary_lines, build_migration_report_summary_text
from ozlink_console.draft_import.import_ux_messages import (
    user_message_for_import_blocked_exception,
    user_message_for_import_bundle_classification_error,
    user_message_for_legacy_identity_incomplete,
    user_message_for_migrated_validation_error,
    user_message_for_placeholder_drive_ids,
)

__all__ = [
    "INVALID_UNKNOWN",
    "MIGRATED_LEGACY_PACKAGE",
    "MODERN_EXPORT",
    "RAW_LEGACY_EXPORT",
    "MigratedImportValidationResult",
    "is_valid_migration_drive_id",
    "build_migration_report_summary_lines",
    "build_migration_report_summary_text",
    "classify_import_bundle",
    "load_migration_conflicts_for_review",
    "validate_migrated_import_bundle",
    "user_message_for_import_blocked_exception",
    "user_message_for_import_bundle_classification_error",
    "user_message_for_legacy_identity_incomplete",
    "user_message_for_migrated_validation_error",
    "user_message_for_placeholder_drive_ids",
]
