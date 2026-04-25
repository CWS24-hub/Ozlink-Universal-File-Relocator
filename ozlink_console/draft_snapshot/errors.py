"""Errors for submitted snapshot import and validation."""

from __future__ import annotations


class SnapshotError(Exception):
    """Base class for draft snapshot pipeline errors."""


class SnapshotValidationError(SnapshotError):
    """Malformed snapshot payload or failed contract validation."""

    def __init__(self, message: str, *, details: list[str] | None = None) -> None:
        super().__init__(message)
        self.details = list(details or [])


class UnsupportedSnapshotVersionError(SnapshotError):
    """Snapshot schema or engine version is not supported."""

    def __init__(self, message: str, *, detected_version: str | None = None) -> None:
        super().__init__(message)
        self.detected_version = detected_version
