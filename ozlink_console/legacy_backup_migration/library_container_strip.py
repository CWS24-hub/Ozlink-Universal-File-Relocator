"""
Strip legacy document-library display names incorrectly stored as a leading path segment.

In older exports, paths were stored as e.g. ``Documents\\Root\\Finance`` even though
``Documents`` is the library container in Graph, not a folder. Library-relative paths
must be ``Root\\Finance``. Stripping runs **before** internal-root strip, re-anchor,
foreign-root heuristics, and Graph resolution.
"""

from __future__ import annotations

from ozlink_console.logger import log_info
from ozlink_console.paths import normalize_manifest_path

OUTCOME_STRIPPED = "stripped"
OUTCOME_NOOP = "noop"
OUTCOME_IDENTITY_MISSING = "identity_missing"
OUTCOME_AMBIGUOUS = "ambiguous"


def _single_library_segment(library_display_name: str) -> tuple[str | None, bool]:
    """Return (one segment, False) or (None, True) if multi-segment / invalid."""
    n = normalize_manifest_path(str(library_display_name or "").strip())
    if not n:
        return None, False
    parts = [p for p in n.split("\\") if p]
    if len(parts) > 1:
        return None, True
    return parts[0], False


def strip_legacy_library_container_segment(
    manifest_path: str,
    library_display_name: str | None,
    *,
    row_index: int,
    row_kind: str,
    role: str,
) -> tuple[str, str]:
    """
    If the first segment equals the selected library's display name (case-insensitive),
    remove it once. ``Documents\\Documents\\Policies`` → ``Documents\\Policies``.

    Returns ``(normalized_path, outcome)`` where outcome is one of
    ``OUTCOME_*``. When identity is missing or ambiguous, the path is returned normalized
    but **not** stripped.
    """
    raw_in = str(manifest_path or "").strip()
    raw = normalize_manifest_path(raw_in)
    if not raw:
        return "", OUTCOME_NOOP

    lib = str(library_display_name or "").strip()
    if not lib:
        if raw:
            log_info(
                "legacy_backup_migration_library_wrapper_missing_identity",
                row_index=row_index,
                row_kind=row_kind,
                role=role,
                path_excerpt=raw[:220],
            )
        return raw, OUTCOME_IDENTITY_MISSING

    seg, ambiguous = _single_library_segment(lib)
    if ambiguous:
        log_info(
            "legacy_backup_migration_library_wrapper_missing_identity",
            row_index=row_index,
            row_kind=row_kind,
            role=role,
            reason="ambiguous_multi_segment_library_name",
            library_excerpt=lib[:120],
            path_excerpt=raw[:220],
        )
        return raw, OUTCOME_AMBIGUOUS
    if seg is None:
        return raw, OUTCOME_NOOP

    parts = [p for p in raw.split("\\") if p]
    if not parts:
        return "", OUTCOME_NOOP
    if parts[0].casefold() != seg.casefold():
        return raw, OUTCOME_NOOP

    remainder = parts[1:]
    if not remainder:
        out = ""
    else:
        out = normalize_manifest_path("\\".join(remainder))

    log_info(
        "legacy_backup_migration_library_container_wrapper_stripped",
        row_index=row_index,
        row_kind=row_kind,
        role=role,
        library_segment=seg[:120],
        before_excerpt=raw[:220],
        after_excerpt=out[:220],
    )
    if role == "destination":
        log_info(
            "legacy_backup_migration_destination_library_wrapper_stripped",
            row_index=row_index,
            row_kind=row_kind,
            before_excerpt=raw[:220],
            after_excerpt=out[:220],
        )
    elif role == "source":
        log_info(
            "legacy_backup_migration_source_library_wrapper_stripped",
            row_index=row_index,
            row_kind=row_kind,
            before_excerpt=raw[:220],
            after_excerpt=out[:220],
        )

    return out, OUTCOME_STRIPPED
