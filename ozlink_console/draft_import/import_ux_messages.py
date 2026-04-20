"""User-facing strings for Import Draft / migration report UX (no validation logic)."""

from __future__ import annotations


def user_message_for_import_bundle_classification_error(error: str) -> str:
    """Map :func:`classify_import_bundle` ``meta['error']`` values to clear UI text."""
    if not error:
        return "This draft bundle is invalid or incomplete."
    if error.startswith("missing:"):
        name = error.split(":", 1)[1].strip()
        return (
            f"Required file missing from this bundle: {name}. "
            "A complete export must include Draft-SessionState.json, Draft-AllocationQueue.json, "
            "and Draft-ProposedFolders.json."
        )
    if error == "session_not_object":
        return "Draft-SessionState.json must be a single JSON object."
    if error == "allocations_not_list":
        return "Draft-AllocationQueue.json must be a JSON array."
    if error == "proposed_not_list":
        return "Draft-ProposedFolders.json must be a JSON array."
    if error.startswith("read_error:"):
        return f"Could not read the bundle JSON files ({error.split(':', 1)[1][:240]})."
    if error == "migration_report_not_object":
        return "LegacyMigrationReport.json must be a JSON object."
    if error.startswith("migration_report:"):
        return f"The migration report file could not be read: {error.split(':', 1)[1][:240]}."
    return f"This draft bundle could not be read: {error}"


def user_message_for_migrated_validation_error(error: str) -> str:
    """Map ``validate_migrated_import_bundle`` result strings without changing validation rules."""
    e = (error or "").strip()
    if not e:
        return "Migrated package validation failed. Check LegacyMigrationReport.json and try again."
    el = e.lower()
    if e == "missing_LegacyMigrationReport.json":
        return "This folder must include LegacyMigrationReport.json next to the Draft JSON files."
    if e.startswith("Invalid LegacyMigrationReport.json"):
        return f"Migrated package validation failed — report file is not valid JSON ({e})."
    if "must be a json object" in el and "legacy" in el:
        return "LegacyMigrationReport.json must contain a single JSON object."
    if "missing identity block" in el:
        return "Migration report is missing the identity block (source/destination site and drives)."
    if ("invalid source/destination drive" in el) or ("placeholder" in el and "drive" in el):
        return (
            "Migration report has missing or placeholder Graph drive ids. "
            "Re-run migration with resolved Source and Destination libraries, or fix LegacyMigrationReport.json."
        )
    if "missing counts" in el:
        return "Migration report is missing the counts section (allocations/proposed counts)."
    if e.startswith("Could not read Draft queue files"):
        return f"Required files could not be read: {e}"
    if "must be json arrays" in el:
        return "Draft-AllocationQueue.json and Draft-ProposedFolders.json must be JSON arrays."
    if "rows_rejected" in el and "confirm" in el:
        return (
            "The migration report lists rejected rows. Confirm only if you accept importing with those rejects."
        )
    if "allocation_parent_unresolved_after_planned_lookup" in el:
        return (
            "Migration left unresolved planned-parent allocations. "
            "Resolve or re-run migration before importing."
        )
    if "graph resolution disabled" in el or ("offline" in el and "confirm" in el):
        return (
            "This migration was run without Graph resolution (offline/skip mode). "
            "Confirm only if you intend to import an offline-migrated package."
        )
    if "allocation row has an invalid" in el or "proposed folder row has an invalid" in el:
        return (
            "One or more rows still have invalid or placeholder drive ids. "
            "Fix the Draft JSON or re-run migration."
        )
    if "count mismatch" in el:
        return e
    return e


def user_message_for_legacy_identity_incomplete() -> str:
    return (
        "Source and destination identity is incomplete for legacy migration. "
        "Select resolved Source and Destination document libraries in the planning header, then try again."
    )


def user_message_for_placeholder_drive_ids() -> str:
    return (
        "Migration requires real Microsoft Graph drive ids (not placeholders). "
        "Select real Source and Destination document libraries, then try again."
    )


def user_message_for_import_blocked_exception(exc: BaseException) -> str | None:
    """Return a specific Import Draft message, or None to fall back to a generic failure string."""
    from ozlink_console.legacy_backup_migration.errors import LegacyBackupDirectRestoreBlocked

    if isinstance(exc, LegacyBackupDirectRestoreBlocked):
        return (
            "This bundle is legacy-shaped and cannot be imported until it is migrated. "
            "Migrate the export first, then use Import Draft on the migrated folder that contains LegacyMigrationReport.json "
            "(or set OZLINK_ALLOW_LEGACY_DIRECT_RESTORE=1 for debugging only)."
        )
    return None
