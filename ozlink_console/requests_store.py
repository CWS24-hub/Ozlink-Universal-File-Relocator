from __future__ import annotations

import json
import shutil
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

from .logger import log_info, log_trace
from .models import SubmissionBatch
from .paths import requests_root, test_requests_root


class RequestStore:
    def __init__(self) -> None:
        self.root = requests_root()
        self.test_root = test_requests_root()

    def _write_json(self, path: Path, payload: Any) -> None:
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _read_json(self, path: Path, fallback: Any) -> Any:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return fallback

    def create_submission_batch(
        self,
        batch: SubmissionBatch,
        allocations_payload: list[dict[str, Any]],
        proposed_payload: list[dict[str, Any]],
        *,
        test_mode: bool = False,
    ) -> Path:
        base_root = self.test_root if test_mode else self.root
        batch_dir = base_root / batch.BatchId
        if batch_dir.exists():
            raise FileExistsError(f"Submission batch already exists: {batch.BatchId}")

        batch_dir.mkdir(parents=True, exist_ok=False)
        self._write_json(batch_dir / "request.json", batch.to_dict())
        self._write_json(batch_dir / "allocations.json", allocations_payload)
        self._write_json(batch_dir / "proposed_folders.json", proposed_payload)
        self._write_json(
            batch_dir / "submission_manifest.json",
            {
                "BatchId": batch.BatchId,
                "CreatedUtc": batch.SubmittedUtc or datetime.utcnow().isoformat(),
                "Status": batch.Status,
                "Files": [
                    "request.json",
                    "allocations.json",
                    "proposed_folders.json",
                ],
            },
        )
        log_info(
            "Submission batch stored.",
            batch_id=batch.BatchId,
            destination=str(batch_dir),
            planned_move_count=batch.PlannedMoveCount,
            proposed_folder_count=batch.ProposedFolderCount,
            test_mode=test_mode,
        )
        return batch_dir

    def list_submission_batches(self) -> list[dict[str, Any]]:
        batches: list[dict[str, Any]] = []
        for root, is_test in ((self.root, False), (self.test_root, True)):
            if not root.exists():
                continue
            for batch_dir in sorted(root.iterdir(), reverse=True):
                if not batch_dir.is_dir():
                    continue
                request_path = batch_dir / "request.json"
                if not request_path.exists():
                    continue
                request_payload = self._read_json(request_path, {})
                if not isinstance(request_payload, dict):
                    continue
                batch = SubmissionBatch.from_dict(request_payload)
                batches.append(
                    {
                        "batch_id": batch.BatchId,
                        "status": batch.Status,
                        "submitted_utc": batch.SubmittedUtc,
                        "submitted_by": batch.SubmittedBy,
                        "tenant_domain": batch.TenantDomain,
                        "draft_id": batch.DraftId,
                        "draft_name": batch.DraftName,
                        "source_site": batch.SourceSite,
                        "source_library": batch.SourceLibrary,
                        "destination_site": batch.DestinationSite,
                        "destination_library": batch.DestinationLibrary,
                        "planned_move_count": batch.PlannedMoveCount,
                        "proposed_folder_count": batch.ProposedFolderCount,
                        "needs_review_count": batch.NeedsReviewCount,
                        "validation_warnings": list(batch.ValidationWarnings),
                        "is_test": is_test,
                        "path": str(batch_dir),
                    }
                )
        batches.sort(key=lambda row: (row.get("submitted_utc", ""), row.get("batch_id", "")), reverse=True)
        return batches

    def load_submission_batch(self, batch_id: str, *, test_mode: bool = False) -> dict[str, Any]:
        root = self.test_root if test_mode else self.root
        batch_dir = root / batch_id
        payload = {
            "request": self._read_json(batch_dir / "request.json", {}),
            "allocations": self._read_json(batch_dir / "allocations.json", []),
            "proposed_folders": self._read_json(batch_dir / "proposed_folders.json", []),
            "manifest": self._read_json(batch_dir / "submission_manifest.json", {}),
            "path": str(batch_dir),
            "is_test": test_mode,
        }
        log_trace(
            "requests",
            "load_submission_batch",
            batch_id=batch_id,
            test_mode=test_mode,
            allocations_count=len(payload.get("allocations") or []),
            proposed_count=len(payload.get("proposed_folders") or []),
        )
        return payload

    def delete_submission_batch(self, batch_id: str, *, test_mode: bool = False) -> Path:
        root = self.test_root if test_mode else self.root
        batch_dir = root / batch_id
        if not batch_dir.exists():
            raise FileNotFoundError(f"Submission batch not found: {batch_id}")
        shutil.rmtree(batch_dir)
        log_info(
            "Submission batch deleted.",
            batch_id=batch_id,
            path=str(batch_dir),
            test_mode=test_mode,
        )
        log_trace("requests", "delete_submission_batch", batch_id=batch_id, test_mode=test_mode)
        return batch_dir

    def export_submission_batch_zip(
        self,
        batch_id: str,
        *,
        test_mode: bool = False,
        destination_zip: Path | None = None,
    ) -> Path:
        root = self.test_root if test_mode else self.root
        batch_dir = root / batch_id
        if not batch_dir.exists():
            raise FileNotFoundError(f"Submission batch not found: {batch_id}")

        if destination_zip is None:
            destination_zip = batch_dir.with_suffix(".zip")
        destination_zip = Path(destination_zip)
        destination_zip.parent.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(destination_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for file_path in sorted(batch_dir.rglob("*")):
                if not file_path.is_file():
                    continue
                archive.write(file_path, arcname=file_path.relative_to(batch_dir))

        log_info(
            "Submission batch zip exported.",
            batch_id=batch_id,
            source=str(batch_dir),
            destination=str(destination_zip),
            test_mode=test_mode,
        )
        log_trace(
            "requests",
            "export_submission_batch_zip",
            batch_id=batch_id,
            destination_excerpt=str(destination_zip)[-100:],
            test_mode=test_mode,
        )
        return destination_zip
