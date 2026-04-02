"""ResolveIds service/wrapper for canonical submitted snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from ozlink_console.draft_snapshot.contracts import CanonicalMappingItem, CanonicalSubmittedSnapshot, new_run_id
from ozlink_console.draft_snapshot.resolution_contracts import (
    ItemResolutionResult,
    ResolvedSnapshot,
    summarize_resolution,
)
from ozlink_console.draft_snapshot.run_log import log_resolution_item_state
from ozlink_console.models import ProposedFolder
from ozlink_console.planned_move_graph_resolve import (
    enrich_proposed_folder_record,
    enrich_single_planned_move,
    is_internal_proposed_destination_item_id,
)
from ozlink_console.transfer_job_runner import is_absolute_local_path


def _norm_rel_path_segments(p: str) -> str:
    s = str(p or "").replace("\\", "/").strip()
    while "//" in s:
        s = s.replace("//", "/")
    return s.strip("/")


def _library_relative_parent_of_file_destination(destination_path: str) -> str:
    """Folder path under the library root for a file destination; empty if the file sits at library root."""
    n = _norm_rel_path_segments(destination_path)
    if not n or "/" not in n:
        return ""
    return n.rsplit("/", 1)[0]


def _mapping_dest_parent_deferrable_to_mkdir_chain(item: CanonicalMappingItem) -> bool:
    """
    Destination parent folder is not on Graph yet, but plan materialization (with graph_client)
    can still emit destination-chain mkdir steps from mapping destination_path prefixes.
    """
    if item.item_type != "file":
        return False
    dp = str(item.destination_path or "").strip()
    if not dp or is_absolute_local_path(dp):
        return False
    return bool(_library_relative_parent_of_file_destination(dp))


class GraphResolutionClient(Protocol):
    def get_drive_item_by_path(self, drive_id: str, relative_path: str) -> dict[str, Any] | None: ...

    def get_drive_root_item(self, drive_id: str) -> dict[str, Any] | None: ...


class ResolveIdsService(Protocol):
    def resolve(self, snapshot: CanonicalSubmittedSnapshot, *, run_id: str = "") -> ResolvedSnapshot: ...


@dataclass
class GraphResolveIdsService:
    """Adapter-style resolver that reuses existing planned_move Graph enrichment helpers."""

    graph_client: GraphResolutionClient
    resolver_name: str = "graph_resolve_ids_v1"

    def resolve(self, snapshot: CanonicalSubmittedSnapshot, *, run_id: str = "") -> ResolvedSnapshot:
        rid = run_id or new_run_id()
        mapping_results: list[ItemResolutionResult] = []
        proposed_results: list[ItemResolutionResult] = []

        src_drive = str(snapshot.source.site_library.library_drive_id or "").strip()
        dst_drive = str(snapshot.destination.site_library.library_drive_id or "").strip()
        src_lib = str(snapshot.source.site_library.library_name or "").strip()
        dst_lib = str(snapshot.destination.site_library.library_name or "").strip()
        src_site = str(snapshot.source.site_library.site_name or "").strip()
        dst_site = str(snapshot.destination.site_library.site_name or "").strip()

        for idx, item in enumerate(snapshot.mapping_items):
            result = self._resolve_mapping_item(
                snapshot,
                item,
                move_index=idx,
                source_drive_id=src_drive,
                source_library_name=src_lib,
                source_site_name=src_site,
                dest_drive_id=dst_drive,
                dest_library_name=dst_lib,
                dest_site_name=dst_site,
                run_id=rid,
            )
            mapping_results.append(result)

        for idx, pf in enumerate(snapshot.proposed_folder_items):
            result = self._resolve_proposed_folder(
                snapshot,
                pf,
                proposed_index=idx,
                dest_drive_id=dst_drive,
                dest_library_name=dst_lib,
                dest_site_name=dst_site,
                run_id=rid,
            )
            proposed_results.append(result)

        summary = summarize_resolution(mapping_results, proposed_results)
        return ResolvedSnapshot(
            snapshot=snapshot,
            run_id=rid,
            resolver_name=self.resolver_name,
            summary=summary,
            mapping_results=mapping_results,
            proposed_folder_results=proposed_results,
        )

    def _resolve_mapping_item(
        self,
        snapshot: CanonicalSubmittedSnapshot,
        item: CanonicalMappingItem,
        *,
        move_index: int,
        source_drive_id: str,
        source_library_name: str,
        source_site_name: str,
        dest_drive_id: str,
        dest_library_name: str,
        dest_site_name: str,
        run_id: str,
    ) -> ItemResolutionResult:
        existing_source_id = str(item.source_graph_item_id or "").strip()
        existing_dest_parent_id = str(item.destination_parent_graph_item_id or "").strip()
        if (
            existing_source_id
            and existing_dest_parent_id
            and not is_internal_proposed_destination_item_id(existing_dest_parent_id)
        ):
            result = ItemResolutionResult(
                item_kind="mapping_item",
                mapping_id=item.mapping_id,
                item_type=item.item_type,
                status="skipped",
                message="already_has_graph_ids",
                source_drive_id=str(item.source_graph_drive_id or source_drive_id),
                source_item_id=existing_source_id,
                destination_drive_id=str(item.destination_graph_drive_id or dest_drive_id),
                destination_parent_item_id=existing_dest_parent_id,
                destination_name=item.destination_name,
            )
            log_resolution_item_state(snapshot_id=snapshot.snapshot_id, run_id=run_id, result=result)
            return result

        move: dict[str, Any] = {
            "source_name": item.source_name,
            "source_path": item.source_path,
            "source_id": existing_source_id,
            "source_drive_id": str(item.source_graph_drive_id or source_drive_id),
            "destination_name": item.destination_name,
            "destination_path": item.destination_path,
            "destination_id": existing_dest_parent_id,
            "destination_drive_id": str(item.destination_graph_drive_id or dest_drive_id),
            "source": {
                "id": existing_source_id,
                "drive_id": str(item.source_graph_drive_id or source_drive_id),
                "name": item.source_name,
            },
            "destination": {
                "id": existing_dest_parent_id,
                "drive_id": str(item.destination_graph_drive_id or dest_drive_id),
                "name": item.destination_name,
            },
        }
        enrich_single_planned_move(
            move,
            get_item_by_path=self.graph_client.get_drive_item_by_path,
            get_root_item=self.graph_client.get_drive_root_item,
            source_drive_id=str(item.source_graph_drive_id or source_drive_id),
            source_library_name=source_library_name,
            dest_drive_id=str(item.destination_graph_drive_id or dest_drive_id),
            dest_library_name=dest_library_name,
            source_site_name=source_site_name,
            dest_site_name=dest_site_name,
            move_index=move_index,
            request_id=item.mapping_id,
        )
        src_obj = move.get("source", {}) if isinstance(move.get("source"), dict) else {}
        dst_obj = move.get("destination", {}) if isinstance(move.get("destination"), dict) else {}
        resolved_source_id = str(src_obj.get("id") or move.get("source_id") or "").strip()
        resolved_dest_parent = str(dst_obj.get("id") or move.get("destination_id") or "").strip()
        unresolved_reasons: list[str] = []
        if not resolved_source_id:
            unresolved_reasons.append("missing_source_item_id")
        if not resolved_dest_parent:
            unresolved_reasons.append("missing_destination_parent_item_id")
        status = "resolved" if not unresolved_reasons else "unresolved"
        message = "resolved_by_graph_lookup" if status == "resolved" else "resolution_incomplete"
        raw_payload: dict[str, Any] = {"move_index": move_index, "assignment_mode": item.assignment_mode}
        if (
            status == "unresolved"
            and unresolved_reasons == ["missing_destination_parent_item_id"]
            and resolved_source_id
            and _mapping_dest_parent_deferrable_to_mkdir_chain(item)
        ):
            # Parent path not on Graph yet (e.g. new subtree under a proposed root). Plan materialization
            # builds create_folder chain from destination_path; bridge resolves parent_step_id outputs.
            status = "resolved"
            unresolved_reasons = []
            resolved_dest_parent = ""
            message = "resolved_dest_parent_deferred_to_materialization_chain"
            raw_payload["deferred_destination_parent_to_mkdir_chain"] = True

        result = ItemResolutionResult(
            item_kind="mapping_item",
            mapping_id=item.mapping_id,
            item_type=item.item_type,
            status=status,
            message=message,
            source_drive_id=str(src_obj.get("drive_id") or move.get("source_drive_id") or ""),
            source_item_id=resolved_source_id,
            destination_drive_id=str(dst_obj.get("drive_id") or move.get("destination_drive_id") or ""),
            destination_parent_item_id=resolved_dest_parent,
            destination_name=str(dst_obj.get("name") or move.get("destination_name") or item.destination_name),
            unresolved_reasons=unresolved_reasons,
            raw=raw_payload,
        )
        log_resolution_item_state(snapshot_id=snapshot.snapshot_id, run_id=run_id, result=result)
        return result

    def _resolve_proposed_folder(
        self,
        snapshot: CanonicalSubmittedSnapshot,
        pf: Any,
        *,
        proposed_index: int,
        dest_drive_id: str,
        dest_library_name: str,
        dest_site_name: str,
        run_id: str,
    ) -> ItemResolutionResult:
        current_drive = str(pf.destination_drive_id or "").strip()
        current_parent = str(pf.destination_parent_item_id or "").strip()
        mapping_id = str(pf.proposed_id or f"proposed-{proposed_index}")
        if current_drive and current_parent and not is_internal_proposed_destination_item_id(current_parent):
            result = ItemResolutionResult(
                item_kind="proposed_folder",
                mapping_id=mapping_id,
                item_type="folder",
                status="skipped",
                message="already_has_graph_parent_ids",
                destination_drive_id=current_drive,
                destination_parent_item_id=current_parent,
                destination_name=str(pf.folder_name or ""),
            )
            log_resolution_item_state(snapshot_id=snapshot.snapshot_id, run_id=run_id, result=result)
            return result

        model = ProposedFolder(
            FolderName=str(pf.folder_name or ""),
            DestinationPath=str(pf.destination_path or ""),
            DestinationId=str(pf.proposed_id or ""),
            DestinationDriveId=current_drive,
            DestinationParentItemId=current_parent,
            ParentPath=str(pf.parent_path or ""),
        )
        enrich_proposed_folder_record(
            model,
            get_item_by_path=self.graph_client.get_drive_item_by_path,
            dest_drive_id=dest_drive_id,
            dest_library_name=dest_library_name,
            dest_site_name=dest_site_name,
            proposed_index=proposed_index,
        )
        unresolved_reasons: list[str] = []
        if not str(model.DestinationDriveId or "").strip():
            unresolved_reasons.append("missing_destination_drive_id")
        if not str(model.DestinationParentItemId or "").strip():
            unresolved_reasons.append("missing_destination_parent_item_id")
        status = "resolved" if not unresolved_reasons else "unresolved"
        result = ItemResolutionResult(
            item_kind="proposed_folder",
            mapping_id=mapping_id,
            item_type="folder",
            status=status,
            message="resolved_proposed_parent" if status == "resolved" else "resolution_incomplete",
            destination_drive_id=str(model.DestinationDriveId or ""),
            destination_parent_item_id=str(model.DestinationParentItemId or ""),
            destination_name=str(model.FolderName or ""),
            unresolved_reasons=unresolved_reasons,
            raw={"proposed_index": proposed_index, "destination_path": model.DestinationPath},
        )
        log_resolution_item_state(snapshot_id=snapshot.snapshot_id, run_id=run_id, result=result)
        return result

