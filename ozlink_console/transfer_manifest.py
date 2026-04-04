"""Build JSON manifests for handoff and execution. Local paths can be run via transfer_job_runner."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ozlink_console.models import ProposedFolder


@dataclass
class TransferStep:
    """One planned file or folder relocation as understood by the console."""

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
    source_drive_id: str = ""
    source_item_id: str = ""
    destination_drive_id: str = ""
    destination_item_id: str = ""
    step_uid: str = ""

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
    destination_drive_id: str = ""
    destination_parent_item_id: str = ""

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
    execution_options: dict[str, Any] = field(default_factory=dict)

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
            "execution_options": dict(self.execution_options),
        }


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _planned_move_to_step(index: int, move: dict[str, Any]) -> TransferStep:
    src = move.get("source") or {}
    dest = move.get("destination") or {}
    # Transfer runner addresses a copy step by destination parent + destination leaf.
    # For per-file moves, leaf must be the move target (usually filename), not the
    # selected destination folder name.
    destination_leaf = str(
        move.get("target_name", "")
        or move.get("destination_name", "")
        or move.get("source_name", "")
        or dest.get("name", "")
        or src.get("name", "")
        or ""
    )
    request_id = str(move.get("request_id", "") or "").strip()
    step_uid = f"{request_id or 'NOREQ'}::{index}"
    return TransferStep(
        index=index,
        operation="copy",
        source_path=str(move.get("source_path", "") or ""),
        destination_path=str(move.get("destination_path", "") or ""),
        source_name=str(move.get("source_name", "") or src.get("name", "") or ""),
        destination_name=destination_leaf,
        is_source_folder=bool(src.get("is_folder", False)),
        request_id=request_id,
        status=str(move.get("status", "") or "Draft"),
        allocation_method=str(move.get("allocation_method", "") or ""),
        source_drive_id=str(src.get("drive_id", "") or move.get("source_drive_id", "") or ""),
        source_item_id=str(src.get("id", "") or move.get("source_id", "") or ""),
        destination_drive_id=str(dest.get("drive_id", "") or move.get("destination_drive_id", "") or ""),
        destination_item_id=str(dest.get("id", "") or move.get("destination_id", "") or ""),
        step_uid=step_uid,
    )


def _proposed_to_step(index: int, pf: ProposedFolder) -> ProposedFolderStep:
    return ProposedFolderStep(
        index=index,
        operation="ensure_folder",
        folder_name=str(pf.FolderName or ""),
        destination_path=str(pf.DestinationPath or ""),
        parent_path=str(pf.ParentPath or ""),
        status=str(pf.Status or "Proposed"),
        destination_drive_id=str(getattr(pf, "DestinationDriveId", "") or ""),
        destination_parent_item_id=str(getattr(pf, "DestinationParentItemId", "") or ""),
    )


def expanded_graph_steps_to_transfer_step_json_dicts(
    planned_moves: list[dict[str, Any]],
    expanded: list[Any],
) -> list[dict[str, Any]]:
    """Build transfer_step JSON dicts from expansion output (sequential index, drive ids from planned move)."""
    out: list[dict[str, Any]] = []
    moves = list(planned_moves or [])
    for i, eg in enumerate(expanded):
        pm = int(getattr(eg, "planned_move_index", -1))
        if pm < 0 or pm >= len(moves):
            continue
        m = moves[pm]
        step = _planned_move_to_step(i, m).to_json_dict()
        step["planned_move_index"] = pm
        out.append(step)
    return out


def build_simulation_manifest(
    *,
    planned_moves: list[dict[str, Any]],
    proposed_folders: list[ProposedFolder],
    draft_id: str = "",
    tenant_hint: str = "",
    notes: str = "",
    manifest_version: int = 1,
    plan_leaf_exclusions: list[str] | None = None,
    graph_unsafe_folder_step_indices: list[int] | None = None,
    graph_expanded_transfer_steps: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return a JSON-serialisable manifest dict (simulation / handoff only)."""
    steps = [_planned_move_to_step(i, m).to_json_dict() for i, m in enumerate(planned_moves or [])]
    proposed = [
        _proposed_to_step(i, pf).to_json_dict() for i, pf in enumerate(proposed_folders or [])
    ]
    excl = [str(x) for x in (plan_leaf_exclusions or []) if str(x).strip()]
    execution_options: dict[str, Any] = {
        "governance_schema": "ozlink/v1",
        "verify_integrity": True,
    }
    if excl:
        execution_options["plan_leaf_exclusions"] = sorted(excl, key=lambda s: s.lower())
    if graph_unsafe_folder_step_indices is not None:
        execution_options["graph_unsafe_folder_step_indices"] = sorted(
            {int(x) for x in graph_unsafe_folder_step_indices if int(x) >= 0}
        )
    if graph_expanded_transfer_steps is not None:
        execution_options["graph_expanded_transfer_steps"] = list(graph_expanded_transfer_steps)
    doc = SimulationManifest(
        manifest_version=int(manifest_version or 1),
        kind="simulation",
        generated_at_utc=_utc_now_iso(),
        draft_id=str(draft_id or ""),
        tenant_hint=str(tenant_hint or ""),
        transfer_steps=steps,
        proposed_folder_steps=proposed,
        notes=str(notes or ""),
        execution_options=execution_options,
    )
    return doc.to_json_dict()


def write_manifest_json(path: str | Path, manifest: dict[str, Any]) -> None:
    """Write manifest with stable formatting for diffing and review."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(manifest, indent=2, ensure_ascii=False) + "\n"
    p.write_text(text, encoding="utf-8")


def upconvert_manifest_v1_to_v2(manifest: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """
    Return (manifest_copy, changed) with v2-compatible transfer step uids.

    - Leaves unknown fields untouched.
    - Adds `step_uid` to transfer rows when missing.
    - Updates `manifest_version` to 2 when source is v1.
    """
    if not isinstance(manifest, dict):
        return (manifest, False)
    out = json.loads(json.dumps(manifest))
    changed = False
    version = int(out.get("manifest_version", 1) or 1)
    transfer_steps = list(out.get("transfer_steps") or [])
    for i, step in enumerate(transfer_steps):
        if not isinstance(step, dict):
            continue
        if str(step.get("step_uid", "") or "").strip():
            continue
        req = str(step.get("request_id", "") or "").strip() or "NOREQ"
        idx = int(step.get("index", i))
        step["step_uid"] = f"{req}::{idx}"
        changed = True
    if version <= 1:
        out["manifest_version"] = 2
        changed = True
    return (out, changed)
