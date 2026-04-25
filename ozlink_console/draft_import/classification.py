"""Classify draft import folders/zips before touching live Memory."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ozlink_console.logger import log_info
from ozlink_console.legacy_backup_migration.shape import is_legacy_shaped_bundle

# ImportPackageKind values (strings for logs/json)
MODERN_EXPORT = "modern_export"
MIGRATED_LEGACY_PACKAGE = "migrated_legacy_package"
RAW_LEGACY_EXPORT = "raw_legacy_export"
INVALID_UNKNOWN = "invalid_unknown"

REQUIRED_FILES = ("Draft-SessionState.json", "Draft-AllocationQueue.json", "Draft-ProposedFolders.json")


def _load_required_bundle(folder: Path) -> tuple[dict[str, Any] | None, list[dict[str, Any]], list[dict[str, Any]], str | None]:
    """Return (session, allocations, proposed, error_reason)."""
    for name in REQUIRED_FILES:
        p = folder / name
        if not p.is_file():
            return None, [], [], f"missing:{name}"
    try:
        raw_s = json.loads((folder / "Draft-SessionState.json").read_text(encoding="utf-8"))
        if not isinstance(raw_s, dict):
            return None, [], [], "session_not_object"
        raw_a = json.loads((folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
        raw_p = json.loads((folder / "Draft-ProposedFolders.json").read_text(encoding="utf-8"))
        if not isinstance(raw_a, list):
            return None, [], [], "allocations_not_list"
        if not isinstance(raw_p, list):
            return None, [], [], "proposed_not_list"
        allocations = [dict(x) for x in raw_a]
        proposed = [dict(x) for x in raw_p]
        return raw_s, allocations, proposed, None
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return None, [], [], f"read_error:{exc}"


def classify_import_bundle(folder: Path) -> tuple[str, dict[str, Any]]:
    """
    Classify a folder containing (at minimum) Draft-* JSON files.

    Returns (kind, meta) where meta includes error/reason keys for invalid inputs.
    """
    folder = Path(folder)
    meta: dict[str, Any] = {"folder": str(folder)}

    session, allocations, proposed, err = _load_required_bundle(folder)
    if err:
        meta["error"] = err
        log_info("import_package_invalid", reason=err, folder=str(folder))
        log_info("import_package_classified", kind=INVALID_UNKNOWN, reason=err, folder=str(folder))
        return INVALID_UNKNOWN, meta

    report_path = folder / "LegacyMigrationReport.json"
    has_report = report_path.is_file()
    if has_report:
        try:
            raw_rep = json.loads(report_path.read_text(encoding="utf-8"))
            if not isinstance(raw_rep, dict):
                meta["error"] = "migration_report_not_object"
                log_info("import_package_invalid", reason=meta["error"], folder=str(folder))
                log_info("import_package_classified", kind=INVALID_UNKNOWN, reason=meta["error"], folder=str(folder))
                return INVALID_UNKNOWN, meta
            meta["legacy_migration_report_present"] = True
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            meta["error"] = f"migration_report:{exc}"
            log_info("import_package_invalid", reason=str(exc)[:120], folder=str(folder))
            log_info("import_package_classified", kind=INVALID_UNKNOWN, reason=meta["error"], folder=str(folder))
            return INVALID_UNKNOWN, meta

    legacy, legacy_reasons = is_legacy_shaped_bundle(session, allocations, proposed)
    meta["legacy_shape_reasons"] = list(legacy_reasons)

    if has_report:
        log_info("import_package_migrated_legacy_detected", folder=str(folder))
        log_info("import_package_classified", kind=MIGRATED_LEGACY_PACKAGE, folder=str(folder))
        return MIGRATED_LEGACY_PACKAGE, meta

    if legacy:
        log_info("import_package_raw_legacy_detected", reasons=legacy_reasons, folder=str(folder))
        log_info("import_package_classified", kind=RAW_LEGACY_EXPORT, folder=str(folder))
        return RAW_LEGACY_EXPORT, meta

    log_info("import_package_modern_detected", folder=str(folder))
    log_info("import_package_classified", kind=MODERN_EXPORT, folder=str(folder))
    return MODERN_EXPORT, meta
