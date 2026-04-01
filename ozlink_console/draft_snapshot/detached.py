"""Load submitted drafts into an in-memory object without mutating live planning memory."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from ozlink_console.draft_snapshot.adapters import (
    from_bundle_folder,
    from_canonical_json_bytes,
    from_req_payload_dict,
)
from ozlink_console.draft_snapshot.contracts import (
    CanonicalSubmittedSnapshot,
    new_run_id,
    validate_canonical_required_fields,
    validate_snapshot_minimal_ready,
)

ImportKind = Literal["canonical_json", "req_json", "legacy_bundle"]


@dataclass
class DetachedSubmittedSnapshot:
    """
    Detached execution snapshot for inspection and environment validation only.
    Does not touch ``MemoryManager`` store or session files.
    """

    run_id: str
    snapshot: CanonicalSubmittedSnapshot
    import_kind: ImportKind
    import_warnings: list[str] = field(default_factory=list)
    normalization_notes: list[str] = field(default_factory=list)

    def required_field_errors(self, *, require_mapping_items: bool = False) -> list[str]:
        return validate_canonical_required_fields(self.snapshot, require_mapping_items=require_mapping_items)


def load_detached_from_canonical_json_bytes(
    data: bytes, *, run_id: str | None = None, normalization_notes: list[str] | None = None
) -> DetachedSubmittedSnapshot:
    rid = run_id or new_run_id()
    snap = from_canonical_json_bytes(data)
    notes = list(normalization_notes or [])
    warnings = validate_snapshot_minimal_ready(snap)
    return DetachedSubmittedSnapshot(
        run_id=rid,
        snapshot=snap,
        import_kind="canonical_json",
        import_warnings=warnings,
        normalization_notes=notes,
    )


def load_detached_from_canonical_json_path(path: Path, *, run_id: str | None = None) -> DetachedSubmittedSnapshot:
    return load_detached_from_canonical_json_bytes(Path(path).read_bytes(), run_id=run_id)


def load_detached_from_req_json_bytes(
    data: bytes, *, run_id: str | None = None, normalization_notes: list[str] | None = None
) -> DetachedSubmittedSnapshot:
    rid = run_id or new_run_id()
    try:
        obj = json.loads(data.decode("utf-8"))
    except UnicodeDecodeError as e:
        from ozlink_console.draft_snapshot.errors import SnapshotValidationError

        raise SnapshotValidationError("REQ JSON must be UTF-8") from e
    except json.JSONDecodeError as e:
        from ozlink_console.draft_snapshot.errors import SnapshotValidationError

        raise SnapshotValidationError("REQ JSON is not valid") from e
    if not isinstance(obj, dict):
        from ozlink_console.draft_snapshot.errors import SnapshotValidationError

        raise SnapshotValidationError("REQ JSON root must be an object")
    snap = from_req_payload_dict(obj)
    notes = list(normalization_notes or [])
    warnings = validate_snapshot_minimal_ready(snap)
    return DetachedSubmittedSnapshot(
        run_id=rid,
        snapshot=snap,
        import_kind="req_json",
        import_warnings=warnings,
        normalization_notes=notes,
    )


def load_detached_from_req_json_path(path: Path, *, run_id: str | None = None) -> DetachedSubmittedSnapshot:
    return load_detached_from_req_json_bytes(Path(path).read_bytes(), run_id=run_id)


def load_detached_from_bundle_folder(path: Path, *, run_id: str | None = None) -> DetachedSubmittedSnapshot:
    rid = run_id or new_run_id()
    snap = from_bundle_folder(Path(path))
    notes: list[str] = [
        "tenant identity not present in legacy bundle; environment checks may be inconclusive",
    ]
    warnings = validate_snapshot_minimal_ready(snap)
    return DetachedSubmittedSnapshot(
        run_id=rid,
        snapshot=snap,
        import_kind="legacy_bundle",
        import_warnings=warnings,
        normalization_notes=notes,
    )


def load_detached_auto_bytes(
    data: bytes,
    *,
    run_id: str | None = None,
    hint: ImportKind | None = None,
) -> DetachedSubmittedSnapshot:
    """
    Best-effort detection: canonical payloads include ``snapshot_schema``;
    REQ payloads typically include ``RequestId`` and ``PlannedMoves``.
    """
    if hint == "canonical_json":
        return load_detached_from_canonical_json_bytes(data, run_id=run_id)
    if hint == "req_json":
        return load_detached_from_req_json_bytes(data, run_id=run_id)
    try:
        obj = json.loads(data.decode("utf-8"))
    except Exception:
        obj = None
    if isinstance(obj, dict):
        if str(obj.get("snapshot_schema") or "") == "ozlink.submitted_snapshot/v1":
            return load_detached_from_canonical_json_bytes(data, run_id=run_id)
        if "PlannedMoves" in obj or "RequestId" in obj:
            return load_detached_from_req_json_bytes(data, run_id=run_id)
    raise ValueError("Could not detect REQ vs canonical JSON; pass explicit loader or hint=.")
