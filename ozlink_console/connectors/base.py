"""Protocol-style base for cloud / local relocation connectors."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol, runtime_checkable


class ConnectorKind(str, Enum):
    LOCAL_FILESYSTEM = "local_filesystem"
    MICROSOFT_GRAPH = "microsoft_graph"
    DROPBOX = "dropbox"
    GOOGLE_DRIVE = "google_drive"
    AMAZON_S3 = "amazon_s3"
    UNKNOWN = "unknown"


@dataclass
class ConnectorCapabilities:
    """What a connector can do in this product generation."""

    kind: ConnectorKind
    can_list_children: bool = False
    can_read_stream: bool = False
    can_write_stream: bool = False
    can_server_side_copy: bool = False
    supports_integrity_hash: bool = False


@runtime_checkable
class RelocatorConnector(Protocol):
    """
    Provider-facing operations for a future executor.

    MVP: no concrete cloud implementations here; local runs use transfer_job_runner + integrity.
    """

    @property
    def capabilities(self) -> ConnectorCapabilities: ...

    def normalize_path(self, raw: str) -> str: ...
    """Return provider-native or canonical path string for planning."""

    def describe(self) -> dict[str, Any]:
        """Safe metadata for audit (no secrets)."""
        ...
