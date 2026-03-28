"""Build JSON manifests for future execution (simulation only — no I/O to cloud or disk paths)."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ozlink_console.models import ProposedFolder


@dataclass
class TransferStep:
    """One planned file or folder relocation as understood by the console (execution not implemented)."""

    index: int
    operation: str
    source_path: str
    destination_path: str
    source_name: str
    destination_name: str
    is_source_folder: bool
    request_id: str
    status: str
    allocation_method: str = ""

    def to_json_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ProposedFolderStep:
    """A folder the user proposed on the destination tree (structure only)."""

    index: int
    operation: str
    folder_name: str
    destination_path: str
    parent_path: str
    status: str

    def to_json_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SimulationManifest:
    """Full document written for dry-run review and future job runners."""

    manifest_version: int
    kind: str
    generated_at_utc: str
    draft_id: str
    tenant_hint: str
    transfer_steps: list[dict[str, Any]] = field(default_factory=list)
    proposed_folder_steps: list[dict[str, Any]] = field(default_factory=list)
    notes: str = ""

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "manifest_version": self.manifest_version,
            "kind": self.kind,
            "generated_at_utc": self.generated_at_utc,
            "draft_id": self.draft_id,
            "tenant_hint": self.tenant_hint,
            "transfer_steps": list(self.transfer_steps),
            "proposed_folder_steps": list(self.proposed_folder_steps),
            "notes": self.notes,
        }


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _planned_move_to_step(index: int, move: dict[str, Any]) -> TransferStep:
    src = move.get("source") or {}
    dest = move.get("destination") or {}
    return TransferStep(
        index=index,
        operation="copy",
        source_path=str(move.get("source_path", "") or ""),
        destination_path=str(move.get("destination_path", "") or ""),
        source_name=str(move.get("source_name", "") or src.get("name", "") or ""),
        destination_name=str(move.get("destination_name", "") or dest.get("name", "") or ""),
        is_source_folder=bool(src.get("is_folder", False)),
        request_id=str(move.get("request_id", "") or ""),
        status=str(move.get("status", "") or "Draft"),
        allocation_method=str(move.get("allocation_method", "") or ""),
    )


def _proposed_to_step(index: int, pf: ProposedFolder) -> ProposedFolderStep:
    return ProposedFolderStep(
        index=index,
        operation="ensure_folder",
        folder_name=str(pf.FolderName or ""),
        destination_path=str(pf.DestinationPath or ""),
        parent_path=str(pf.ParentPath or ""),
        status=str(pf.Status or "Proposed"),
    )


def build_simulation_manifest(
    *,
    planned_moves: list[dict[str, Any]],
    proposed_folders: list[ProposedFolder],
    draft_id: str = "",
    tenant_hint: str = "",
    notes: str = "",
) -> dict[str, Any]:
    """Return a JSON-serialisable manifest dict (simulation / handoff only)."""
    steps = [_planned_move_to_step(i, m).to_json_dict() for i, m in enumerate(planned_moves or [])]
    proposed = [
        _proposed_to_step(i, pf).to_json_dict() for i, pf in enumerate(proposed_folders or [])
    ]
    doc = SimulationManifest(
        manifest_version=1,
        kind="simulation",
        generated_at_utc=_utc_now_iso(),
        draft_id=str(draft_id or ""),
        tenant_hint=str(tenant_hint or ""),
        transfer_steps=steps,
        proposed_folder_steps=proposed,
        notes=str(notes or ""),
    )
    return doc.to_json_dict()


def write_manifest_json(path: str | Path, manifest: dict[str, Any]) -> None:
    """Write manifest with stable formatting for diffing and review."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(manifest, indent=2, ensure_ascii=False) + "\n"
    p.write_text(text, encoding="utf-8")
