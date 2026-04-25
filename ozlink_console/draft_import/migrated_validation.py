"""Validate migrated legacy packages (LegacyMigrationReport.json + Draft-* files) before Memory import."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ozlink_console.logger import log_info


def _looks_like_real_drive_id(s: str) -> bool:
    t = str(s or "").strip()
    return bool(t.startswith("b!") and len(t) > 20)


def is_valid_migration_drive_id(s: str) -> bool:
    """True when ``s`` looks like a real SharePoint Graph drive id (``b!…``)."""
    return _looks_like_real_drive_id(s)


@dataclass
class MigratedImportValidationResult:
    ok: bool
    error: str = ""
    report: dict[str, Any] | None = None


def validate_migrated_import_bundle(
    folder: Path,
    *,
    user_confirmed_rows_rejected: bool = False,
    user_confirmed_offline_migration: bool = False,
) -> MigratedImportValidationResult:
    """
    Safety checks before importing a folder that includes LegacyMigrationReport.json.

    Does not reject solely on legacy_shape_reasons or Draft_* status name strings.
    """
    folder = Path(folder)
    log_info("migrated_package_validation_started", folder=str(folder))
    rp = folder / "LegacyMigrationReport.json"
    if not rp.is_file():
        log_info("migrated_package_validation_failed", reason="missing_LegacyMigrationReport.json")
        return MigratedImportValidationResult(False, error="missing_LegacyMigrationReport.json")

    try:
        report: dict[str, Any] = json.loads(rp.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        log_info("migrated_package_validation_failed", reason=f"report_json:{exc}")
        return MigratedImportValidationResult(False, error=f"Invalid LegacyMigrationReport.json: {exc}")

    if not isinstance(report, dict):
        log_info("migrated_package_validation_failed", reason="report_not_object")
        return MigratedImportValidationResult(False, error="LegacyMigrationReport.json must be a JSON object.")

    ident = report.get("identity")
    if not isinstance(ident, dict):
        log_info("migrated_package_validation_failed", reason="missing_identity")
        return MigratedImportValidationResult(False, error="Migration report missing identity block.")

    src_d = str(ident.get("source_drive_id") or "").strip()
    dst_d = str(ident.get("destination_drive_id") or "").strip()
    if not _looks_like_real_drive_id(src_d) or not _looks_like_real_drive_id(dst_d):
        log_info("migrated_package_validation_failed", reason="placeholder_or_missing_drive_ids")
        return MigratedImportValidationResult(False, error="Migration report has missing or invalid source/destination drive ids.")

    counts = report.get("counts")
    if not isinstance(counts, dict):
        log_info("migrated_package_validation_failed", reason="missing_counts")
        return MigratedImportValidationResult(False, error="Migration report missing counts.")

    try:
        alloc_json = json.loads((folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
        prop_json = json.loads((folder / "Draft-ProposedFolders.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        log_info("migrated_package_validation_failed", reason=f"draft_read:{exc}")
        return MigratedImportValidationResult(False, error=f"Could not read Draft queue files: {exc}")

    if not isinstance(alloc_json, list) or not isinstance(prop_json, list):
        log_info("migrated_package_validation_failed", reason="draft_not_lists")
        return MigratedImportValidationResult(False, error="Draft-AllocationQueue / Draft-ProposedFolders must be JSON arrays.")

    n_alloc = len(alloc_json)
    n_prop = len(prop_json)
    ci = int(counts.get("allocations_input") or counts.get("migrated_allocations") or -1)
    cp = int(counts.get("proposed_input") or counts.get("migrated_proposed") or -1)

    if ci >= 0 and n_alloc != ci:
        log_info(
            "migrated_package_validation_failed",
            reason="allocation_count_mismatch",
            file_count=n_alloc,
            report_count=ci,
        )
        return MigratedImportValidationResult(
            False,
            error=f"Allocation count mismatch: JSON has {n_alloc}, report expects {ci}.",
        )
    if cp >= 0 and n_prop != cp:
        log_info(
            "migrated_package_validation_failed",
            reason="proposed_count_mismatch",
            file_count=n_prop,
            report_count=cp,
        )
        return MigratedImportValidationResult(
            False,
            error=f"Proposed folder count mismatch: JSON has {n_prop}, report expects {cp}.",
        )

    rows_rejected = int(counts.get("rows_rejected") or 0)
    if rows_rejected > 0 and not user_confirmed_rows_rejected:
        log_info("migrated_package_validation_failed", reason="rows_rejected_requires_confirmation", rows_rejected=rows_rejected)
        return MigratedImportValidationResult(
            False,
            error=f"Migration report shows rows_rejected={rows_rejected}; confirm to import.",
        )

    unresolved_planned = int(counts.get("allocation_parent_unresolved_after_planned_lookup") or 0)
    if unresolved_planned > 0:
        log_info(
            "migrated_package_validation_failed",
            reason="allocation_parent_unresolved_after_planned_lookup",
            count=unresolved_planned,
        )
        return MigratedImportValidationResult(
            False,
            error=f"Migration has allocation_parent_unresolved_after_planned_lookup={unresolved_planned}.",
        )

    if counts.get("skip_graph_resolution") is True and not user_confirmed_offline_migration:
        log_info("migrated_package_validation_failed", reason="offline_migration_requires_confirmation")
        return MigratedImportValidationResult(
            False,
            error="Migration was run with Graph resolution disabled; confirm to import.",
        )

    for row in alloc_json:
        if not isinstance(row, dict):
            continue
        sd = str(row.get("SourceDriveId") or "").strip()
        dd = str(row.get("DestinationDriveId") or "").strip()
        if sd and not _looks_like_real_drive_id(sd):
            log_info("migrated_package_validation_failed", reason="allocation_placeholder_source_drive")
            return MigratedImportValidationResult(False, error="An allocation row has an invalid SourceDriveId.")
        if dd and not _looks_like_real_drive_id(dd):
            log_info("migrated_package_validation_failed", reason="allocation_placeholder_dest_drive")
            return MigratedImportValidationResult(False, error="An allocation row has an invalid DestinationDriveId.")

    for row in prop_json:
        if not isinstance(row, dict):
            continue
        dd = str(row.get("DestinationDriveId") or "").strip()
        if dd and not _looks_like_real_drive_id(dd):
            log_info("migrated_package_validation_failed", reason="proposed_placeholder_dest_drive")
            return MigratedImportValidationResult(False, error="A proposed folder row has an invalid DestinationDriveId.")

    log_info("migrated_package_validation_passed", folder=str(folder))
    return MigratedImportValidationResult(True, report=report)
