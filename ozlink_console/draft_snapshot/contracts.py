"""Canonical submitted snapshot model (ozlink.submitted_snapshot/v1)."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from uuid import UUID
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

from ozlink_console.draft_snapshot.errors import SnapshotValidationError, UnsupportedSnapshotVersionError

SNAPSHOT_SCHEMA_V1 = "ozlink.submitted_snapshot/v1"
SNAPSHOT_ENGINE_VERSION_V1 = 1
MIN_SUPPORTED_ENGINE_VERSION = 1
MAX_SUPPORTED_ENGINE_VERSION = 1

ItemType = Literal["file", "folder"]
AssignmentMode = Literal[
    "move",
    "copy",
    "move_recursive",
    "copy_recursive",
    "unknown",
]
SourceRetentionPolicy = Literal[
    "retain",
    "cleanup_after_verified_transfer",
    "archive_source",
]


def new_snapshot_id() -> str:
    """Strict 32-character lowercase hex id for newly created canonical snapshots."""
    return uuid.uuid4().hex


def normalize_submitted_snapshot_id(raw: str) -> tuple[str, str]:
    """
    Map legacy / external ``snapshot_id`` values to the internal strict form (32 lowercase hex).

    Returns ``(normalized_id, snapshot_id_submitted)`` where ``snapshot_id_submitted`` is the
    on-wire value when it differs from ``normalized_id`` (audit / support); otherwise ``""``.

    Accepted inputs:

    - 32 hex characters (any case) — normalized to lowercase
    - Standard UUID strings (with dashes, optional braces/URN) — normalized to 32 hex without dashes
    - Any other non-empty string — deterministic ``sha256(utf-8)[:32]`` (migration-friendly)
    """
    s = str(raw or "").strip()
    if not s:
        raise SnapshotValidationError("snapshot_id is required", details=[f"snapshot_id={raw!r}"])
    if len(s) == 32 and re.fullmatch(r"[0-9a-fA-F]{32}", s):
        n = s.lower()
        return (n, "" if s == n else s)
    try:
        u = UUID(s)
    except ValueError:
        pass
    else:
        n = u.hex
        return (n, "" if s.lower() == n else s)
    digest = hashlib.sha256(s.encode("utf-8")).hexdigest()[:32]
    return (digest, s)


def _snapshot_hash_from_body_dict(body: dict[str, Any]) -> str:
    canonical = json.dumps(body, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def new_run_id() -> str:
    return uuid.uuid4().hex


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class TenantIdentitySnapshot:
    tenant_id: str = ""
    tenant_domain: str = ""
    tenant_label: str = ""
    client_key: str = ""


@dataclass
class SiteLibraryContextSnapshot:
    site_id: str = ""
    site_name: str = ""
    site_web_url: str = ""
    library_drive_id: str = ""
    library_name: str = ""


@dataclass
class SourceContextSnapshot:
    platform: str = "sharepoint"
    site_library: SiteLibraryContextSnapshot = field(default_factory=SiteLibraryContextSnapshot)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class DestinationContextSnapshot:
    platform: str = "sharepoint"
    site_library: SiteLibraryContextSnapshot = field(default_factory=SiteLibraryContextSnapshot)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class CanonicalMappingItem:
    mapping_id: str
    item_type: ItemType
    source_path: str
    """Path relative to the source document library root."""
    source_name: str
    source_graph_item_id: str | None = None
    source_graph_drive_id: str | None = None
    destination_path: str = ""
    """Folder path relative to the destination document library root."""
    destination_name: str = ""
    destination_graph_item_id: str | None = None
    destination_parent_graph_item_id: str | None = None
    destination_graph_drive_id: str | None = None
    assignment_mode: AssignmentMode = "unknown"
    """File: ``move`` | ``copy``. Folder: include ``move_recursive`` / ``copy_recursive`` when tree applies."""
    depth: int = 0
    source_node_uid: str = ""
    destination_node_uid: str = ""
    legacy_request_id: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProposedFolderMappingItem:
    proposed_id: str
    folder_name: str
    destination_path: str
    """Folder path relative to the destination document library root."""
    parent_path: str = ""
    """Optional parent path, library-relative (same rules as ``destination_path``)."""
    destination_drive_id: str | None = None
    destination_parent_item_id: str | None = None
    depth: int = 0
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class CanonicalSubmittedSnapshot:
    snapshot_id: str
    """Strict internal id (32 lowercase hex). See ``snapshot_id_submitted`` for on-wire legacy form."""
    draft_id: str
    draft_version: str
    submitted_at_utc: str
    submitted_by: str
    app_version: str
    tenant: TenantIdentitySnapshot
    source: SourceContextSnapshot
    destination: DestinationContextSnapshot
    mapping_items: list[CanonicalMappingItem] = field(default_factory=list)
    proposed_folder_items: list[ProposedFolderMappingItem] = field(default_factory=list)
    source_retention_policy: SourceRetentionPolicy = "retain"
    execution_options: dict[str, Any] = field(default_factory=dict)
    snapshot_hash: str = ""
    snapshot_schema: str = SNAPSHOT_SCHEMA_V1
    engine_version: int = SNAPSHOT_ENGINE_VERSION_V1
    adapter_source: str = ""
    snapshot_id_submitted: str = ""
    """Original submitted ``snapshot_id`` when it differed from normalized form (e.g. dashed UUID)."""

    def to_json_dict(self) -> dict[str, Any]:
        def _sl(s: SiteLibraryContextSnapshot) -> dict[str, Any]:
            return asdict(s)

        out: dict[str, Any] = {
            "snapshot_schema": self.snapshot_schema,
            "engine_version": self.engine_version,
            "snapshot_id": self.snapshot_id,
            "draft_id": self.draft_id,
            "draft_version": self.draft_version,
            "submitted_at_utc": self.submitted_at_utc,
            "submitted_by": self.submitted_by,
            "app_version": self.app_version,
            "tenant": asdict(self.tenant),
            "source": {
                "platform": self.source.platform,
                "site_library": _sl(self.source.site_library),
                "raw": dict(self.source.raw),
            },
            "destination": {
                "platform": self.destination.platform,
                "site_library": _sl(self.destination.site_library),
                "raw": dict(self.destination.raw),
            },
            "mapping_items": [asdict(m) for m in self.mapping_items],
            "proposed_folder_items": [asdict(p) for p in self.proposed_folder_items],
            "source_retention_policy": self.source_retention_policy,
            "execution_options": dict(self.execution_options),
            "snapshot_hash": self.snapshot_hash,
            "adapter_source": self.adapter_source,
        }
        if self.snapshot_id_submitted:
            out["snapshot_id_submitted"] = self.snapshot_id_submitted
        return out

    def compute_snapshot_hash(self) -> str:
        body = dict(self.to_json_dict())
        body.pop("snapshot_hash", None)
        body.pop("snapshot_id_submitted", None)
        canonical = json.dumps(body, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _nullable_str(v: Any) -> str | None:
    s = str(v or "").strip()
    return s if s else None


def _parse_assignment(v: Any) -> AssignmentMode:
    t = str(v or "unknown").strip().lower().replace("-", "_").replace(" ", "_")
    allowed: set[str] = {"move", "copy", "unknown", "move_recursive", "copy_recursive"}
    if t in allowed:
        return t  # type: ignore[return-value]
    if "recursive" in t:
        if "copy" in t:
            return "copy_recursive"
        if "move" in t:
            return "move_recursive"
    if t in ("recursivecopy", "recursive_copy"):
        return "copy_recursive"
    if t in ("recursivemove", "recursive_move"):
        return "move_recursive"
    return "unknown"


def _parse_source_retention_policy(v: Any) -> SourceRetentionPolicy:
    t = str(v or "retain").strip().lower()
    if t in ("retain", "cleanup_after_verified_transfer", "archive_source"):
        return t  # type: ignore[return-value]
    return "retain"


def _parse_context_raw(src_raw: dict[str, Any]) -> dict[str, Any]:
    """Rebuild ``SourceContextSnapshot.raw`` / ``DestinationContextSnapshot.raw`` from JSON."""
    extra = {k: v for k, v in src_raw.items() if k not in ("platform", "site_library", "raw")}
    inner = src_raw.get("raw")
    if isinstance(inner, dict):
        merged = dict(inner)
        merged.update(extra)
        return merged
    return dict(extra)


def _parse_site_library(obj: dict[str, Any]) -> SiteLibraryContextSnapshot:
    if not isinstance(obj, dict):
        return SiteLibraryContextSnapshot()
    return SiteLibraryContextSnapshot(
        site_id=str(obj.get("site_id", "") or ""),
        site_name=str(obj.get("site_name", "") or ""),
        site_web_url=str(obj.get("site_web_url", "") or ""),
        library_drive_id=str(obj.get("library_drive_id", "") or ""),
        library_name=str(obj.get("library_name", "") or ""),
    )


_MAPPING_ITEM_KEYS = frozenset(
    {
        "mapping_id",
        "request_id",
        "item_type",
        "source_path",
        "source_name",
        "source_graph_item_id",
        "source_graph_drive_id",
        "destination_path",
        "destination_name",
        "destination_graph_item_id",
        "destination_parent_graph_item_id",
        "destination_graph_drive_id",
        "assignment_mode",
        "depth",
        "source_node_uid",
        "destination_node_uid",
        "legacy_request_id",
        "raw",
    }
)


def _parse_mapping_item_dict(obj: dict[str, Any], *, index: int) -> CanonicalMappingItem:
    mid = str(obj.get("mapping_id") or obj.get("request_id") or "").strip()
    if not mid:
        mid = f"mapping-{index}"
    it = str(obj.get("item_type") or "").strip().lower()
    if it not in ("file", "folder"):
        raise SnapshotValidationError(
            f"mapping_items[{index}].item_type must be 'file' or 'folder'",
            details=[f"got={it!r}"],
        )
    return CanonicalMappingItem(
        mapping_id=mid,
        item_type=it,  # type: ignore[arg-type]
        source_path=str(obj.get("source_path", "") or ""),
        source_name=str(obj.get("source_name", "") or ""),
        source_graph_item_id=_nullable_str(obj.get("source_graph_item_id")),
        source_graph_drive_id=_nullable_str(obj.get("source_graph_drive_id")),
        destination_path=str(obj.get("destination_path", "") or ""),
        destination_name=str(obj.get("destination_name", "") or ""),
        destination_graph_item_id=_nullable_str(obj.get("destination_graph_item_id")),
        destination_parent_graph_item_id=_nullable_str(obj.get("destination_parent_graph_item_id")),
        destination_graph_drive_id=_nullable_str(obj.get("destination_graph_drive_id")),
        assignment_mode=_parse_assignment(obj.get("assignment_mode", "unknown")),
        depth=int(obj.get("depth", 0) or 0),
        source_node_uid=str(obj.get("source_node_uid", "") or ""),
        destination_node_uid=str(obj.get("destination_node_uid", "") or ""),
        legacy_request_id=str(obj.get("legacy_request_id", "") or ""),
        raw={k: v for k, v in obj.items() if k not in _MAPPING_ITEM_KEYS},
    )


_PROPOSED_KEYS = frozenset(
    {
        "proposed_id",
        "DestinationId",
        "folder_name",
        "FolderName",
        "destination_path",
        "DestinationPath",
        "parent_path",
        "ParentPath",
        "destination_drive_id",
        "DestinationDriveId",
        "destination_parent_item_id",
        "DestinationParentItemId",
        "depth",
        "raw",
    }
)


def _parse_proposed_folder_dict(obj: dict[str, Any], *, index: int) -> ProposedFolderMappingItem:
    pid = str(obj.get("proposed_id") or obj.get("DestinationId") or "").strip()
    if not pid:
        pid = f"proposed-{index}"
    return ProposedFolderMappingItem(
        proposed_id=pid,
        folder_name=str(obj.get("folder_name", obj.get("FolderName", "")) or ""),
        destination_path=str(obj.get("destination_path", obj.get("DestinationPath", "")) or ""),
        parent_path=str(obj.get("parent_path", obj.get("ParentPath", "")) or ""),
        destination_drive_id=_nullable_str(obj.get("destination_drive_id", obj.get("DestinationDriveId"))),
        destination_parent_item_id=_nullable_str(obj.get("destination_parent_item_id", obj.get("DestinationParentItemId"))),
        depth=int(obj.get("depth", 0) or 0),
        raw={k: v for k, v in obj.items() if isinstance(k, str) and k not in _PROPOSED_KEYS},
    )


def parse_canonical_submitted_snapshot_dict(raw: dict[str, Any]) -> CanonicalSubmittedSnapshot:
    """
    Parse and validate canonical JSON (schema ozlink.submitted_snapshot/v1).

    ``snapshot_id`` may be legacy (dashed UUID, arbitrary string, or non-lowercase hex); values are
    normalized to 32 lowercase hex on the model, with the on-wire form preserved in
    ``snapshot_id_submitted`` when it differs.

    ``snapshot_hash`` is computed over the JSON body excluding ``snapshot_hash`` and
    ``snapshot_id_submitted``. If the stored hash does not match the normalized-id body but matches
    a legacy body that used the original wire ``snapshot_id`` string, import still succeeds.
    """
    if not isinstance(raw, dict):
        raise SnapshotValidationError("snapshot root must be a JSON object")
    schema = str(raw.get("snapshot_schema") or raw.get("schema") or "").strip()
    if schema != SNAPSHOT_SCHEMA_V1:
        raise SnapshotValidationError(
            f"unsupported snapshot_schema {schema!r}; expected {SNAPSHOT_SCHEMA_V1!r}",
            details=[f"schema={schema!r}"],
        )
    eng = int(raw.get("engine_version", 0) or 0)
    if eng < MIN_SUPPORTED_ENGINE_VERSION or eng > MAX_SUPPORTED_ENGINE_VERSION:
        raise UnsupportedSnapshotVersionError(
            f"engine_version {eng} not in supported range "
            f"[{MIN_SUPPORTED_ENGINE_VERSION}, {MAX_SUPPORTED_ENGINE_VERSION}]",
            detected_version=str(eng),
        )
    sid_raw = str(raw.get("snapshot_id") or "").strip()
    sid_norm, sid_submitted = normalize_submitted_snapshot_id(sid_raw)
    explicit_submitted = str(raw.get("snapshot_id_submitted") or "").strip()
    if explicit_submitted:
        sid_submitted = explicit_submitted
    if sid_submitted == sid_norm:
        sid_submitted = ""

    mapping_raw = raw.get("mapping_items")
    if not isinstance(mapping_raw, list):
        raise SnapshotValidationError("mapping_items must be a list")
    proposed_raw = raw.get("proposed_folder_items")
    if proposed_raw is None:
        proposed_raw = []
    if not isinstance(proposed_raw, list):
        raise SnapshotValidationError("proposed_folder_items must be a list or omitted")

    tenant_raw = raw.get("tenant") if isinstance(raw.get("tenant"), dict) else {}
    src_raw = raw.get("source") if isinstance(raw.get("source"), dict) else {}
    dst_raw = raw.get("destination") if isinstance(raw.get("destination"), dict) else {}

    items: list[CanonicalMappingItem] = []
    for i, x in enumerate(mapping_raw):
        if not isinstance(x, dict):
            raise SnapshotValidationError(f"mapping_items[{i}] must be an object")
        items.append(_parse_mapping_item_dict(x, index=i))

    proposed_items: list[ProposedFolderMappingItem] = []
    for i, x in enumerate(proposed_raw):
        if not isinstance(x, dict):
            raise SnapshotValidationError(f"proposed_folder_items[{i}] must be an object")
        proposed_items.append(_parse_proposed_folder_dict(x, index=i))

    snap = CanonicalSubmittedSnapshot(
        snapshot_id=sid_norm,
        draft_id=str(raw.get("draft_id", "") or ""),
        draft_version=str(raw.get("draft_version", "") or ""),
        submitted_at_utc=str(raw.get("submitted_at_utc", "") or ""),
        submitted_by=str(raw.get("submitted_by", "") or ""),
        app_version=str(raw.get("app_version", "") or ""),
        tenant=TenantIdentitySnapshot(
            tenant_id=str(tenant_raw.get("tenant_id", "") or ""),
            tenant_domain=str(tenant_raw.get("tenant_domain", "") or "").lower(),
            tenant_label=str(tenant_raw.get("tenant_label", "") or ""),
            client_key=str(tenant_raw.get("client_key", "") or ""),
        ),
        source=SourceContextSnapshot(
            platform=str(src_raw.get("platform", "sharepoint") or "sharepoint"),
            site_library=_parse_site_library(src_raw["site_library"]) if isinstance(src_raw.get("site_library"), dict) else SiteLibraryContextSnapshot(),
            raw=_parse_context_raw(src_raw),
        ),
        destination=DestinationContextSnapshot(
            platform=str(dst_raw.get("platform", "sharepoint") or "sharepoint"),
            site_library=_parse_site_library(dst_raw["site_library"]) if isinstance(dst_raw.get("site_library"), dict) else SiteLibraryContextSnapshot(),
            raw=_parse_context_raw(dst_raw),
        ),
        mapping_items=items,
        proposed_folder_items=proposed_items,
        source_retention_policy=_parse_source_retention_policy(raw.get("source_retention_policy", "retain")),
        execution_options=dict(raw["execution_options"]) if isinstance(raw.get("execution_options"), dict) else {},
        snapshot_hash=str(raw.get("snapshot_hash", "") or ""),
        snapshot_schema=SNAPSHOT_SCHEMA_V1,
        engine_version=eng,
        adapter_source=str(raw.get("adapter_source", "canonical_json") or "canonical_json"),
        snapshot_id_submitted=sid_submitted,
    )
    computed = snap.compute_snapshot_hash()
    if snap.snapshot_hash and snap.snapshot_hash != computed:
        legacy_body = dict(snap.to_json_dict())
        legacy_body.pop("snapshot_hash", None)
        legacy_body["snapshot_id"] = sid_raw
        legacy_body.pop("snapshot_id_submitted", None)
        legacy_computed = _snapshot_hash_from_body_dict(legacy_body)
        if snap.snapshot_hash != legacy_computed:
            raise SnapshotValidationError(
                "snapshot_hash does not match payload",
                details=[f"computed={computed}", f"stored={snap.snapshot_hash}"],
            )
    snap.snapshot_hash = computed
    return snap


def validate_canonical_required_fields(
    snap: CanonicalSubmittedSnapshot, *, require_mapping_items: bool = False
) -> list[str]:
    """
    Hard validation errors (empty list means OK).
    Use after adapters build an in-memory snapshot (not only after strict JSON parse).
    """
    errors: list[str] = []
    if not str(snap.snapshot_id or "").strip():
        errors.append("snapshot_id is required")
    if not str(snap.snapshot_schema or "").strip():
        errors.append("snapshot_schema is required")
    if snap.engine_version < MIN_SUPPORTED_ENGINE_VERSION or snap.engine_version > MAX_SUPPORTED_ENGINE_VERSION:
        errors.append(
            f"engine_version must be in [{MIN_SUPPORTED_ENGINE_VERSION}, {MAX_SUPPORTED_ENGINE_VERSION}] "
            f"(got {snap.engine_version})"
        )
    if require_mapping_items and not snap.mapping_items and not snap.proposed_folder_items:
        errors.append("at least one mapping_items or proposed_folder_items entry is required")
    return errors


def validate_snapshot_minimal_ready(snap: CanonicalSubmittedSnapshot) -> list[str]:
    """
    Soft readiness checks (warnings), not strict schema errors.
    Returns human-readable warning strings.
    """
    warnings: list[str] = []
    if not snap.mapping_items and not snap.proposed_folder_items:
        warnings.append("snapshot has no mapping_items and no proposed_folder_items")
    for i, m in enumerate(snap.mapping_items):
        if not str(m.source_path or "").strip():
            warnings.append(f"mapping_items[{i}] ({m.mapping_id}) missing source_path")
        if not str(m.destination_path or "").strip() and m.item_type == "file":
            warnings.append(f"mapping_items[{i}] ({m.mapping_id}) missing destination_path")
    return warnings
