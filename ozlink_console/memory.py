from __future__ import annotations

import json
import os
import shutil
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .logger import log_info, log_trace, log_warn
from .version_info import APP_VERSION
from .models import AllocationRow, ProposedFolder, SessionState, MemoryManifest

# Optional sidecar written with exports / full workspace saves; older bundles omit this file.
WORKSPACE_SNAPSHOT_SCHEMA_VERSION = 2
from .paths import (
    ensure_app_storage_directories,
    memory_root,
    legacy_memory_root,
    backups_root,
    quarantine_root,
    python_primary_storage_root,
    legacy_compatibility_root,
    user_scoped_storage_root,
)


# Reject promoting a backup session older than the live primary by more than this unless scores tie-break.
_RESTORE_BACKUP_STALENESS_SOFT_SEC = float(45 * 86400)


def _persist_ctx_for_log(ctx: dict[str, Any] | None) -> dict[str, Any]:
    """Drop keys duplicated as explicit ``log_info`` kwargs (``save_reason`` is passed separately)."""
    out = dict(ctx or {})
    out.pop("save_reason", None)
    return out


def _restore_candidate_preference_score(name: str) -> int:
    """Higher score wins when sorting fallback candidates (non-authoritative live paths)."""
    n = str(name or "")
    order = (
        "python_global_primary",
        "python_global_recovery",
        "python_backup_latest",
        "python_global_backup_latest",
        "legacy_live_primary",
        "legacy_live_recovery",
        "legacy_backup_latest",
    )
    try:
        idx = order.index(n)
    except ValueError:
        return 0
    return len(order) - idx


class MemoryManager:
    def __init__(self, *, tenant_domain: str = "", operator_upn: str = "") -> None:
        self.tenant_domain = str(tenant_domain or "").strip().lower()
        self.operator_upn = str(operator_upn or "").strip().lower()
        self.expected_fingerprint = f"{self.operator_upn}|{self.tenant_domain}".strip("|")
        self.storage_scope_root = (
            user_scoped_storage_root(self.tenant_domain, self.operator_upn)
            if self.expected_fingerprint
            else python_primary_storage_root()
        )
        self.root = self.storage_scope_root / "Memory"
        self.legacy_root = legacy_memory_root()
        self.backups = self.root / "Backups"
        self.quarantine = self.root / "Quarantine"
        self.exports = self.storage_scope_root / "Exports"
        self.python_primary_storage_root = python_primary_storage_root()
        self.legacy_compatibility_root = legacy_compatibility_root()
        self.current_restore_source = "python"
        self.current_write_root = self.root
        # After Memory bundle import: block empty persisted planning until runtime confirms (see save_*).
        self._import_restore_empty_guard: tuple[int, int] | None = None
        self._import_restore_runtime_confirmed: bool = False

        self.paths = {
            "allocations": self.root / "Draft-AllocationQueue.json",
            "allocations_recovery": self.root / "Draft-AllocationQueue.recovery.json",
            "proposed": self.root / "Draft-ProposedFolders.json",
            "proposed_recovery": self.root / "Draft-ProposedFolders.recovery.json",
            "session": self.root / "Draft-SessionState.json",
            "session_recovery": self.root / "Draft-SessionState.recovery.json",
            "manifest": self.root / "MemoryManifest.json",
            "workspace_snapshot": self.root / "WorkspaceSnapshot.json",
        }
        self.legacy_paths = {
            "allocations": self.legacy_root / "Draft-AllocationQueue.json",
            "allocations_recovery": self.legacy_root / "Draft-AllocationQueue.recovery.json",
            "proposed": self.legacy_root / "Draft-ProposedFolders.json",
            "proposed_recovery": self.legacy_root / "Draft-ProposedFolders.recovery.json",
            "session": self.legacy_root / "Draft-SessionState.json",
            "session_recovery": self.legacy_root / "Draft-SessionState.recovery.json",
            "manifest": self.legacy_root / "MemoryManifest.json",
        }
        ensure_app_storage_directories()
        self._ensure_memory_directories()
        self.initialize_store()
        log_info(
            "Memory roots configured.",
            python_primary_storage_root=str(self.python_primary_storage_root),
            legacy_compatibility_root=str(self.legacy_compatibility_root),
            memory_write_root=str(self.root),
            memory_scope_root=str(self.storage_scope_root),
            expected_fingerprint=self.expected_fingerprint,
        )

    @property
    def workspace_reset_backups_dir(self) -> Path:
        """Folder for ``DraftReset_*.json`` (Backup && Reset Workspace). Kept separate from manifest rotation files in ``Backups``."""
        return self.backups / "WorkspaceReset"

    def _ensure_memory_directories(self) -> None:
        """Ensure this manager's scope (user/tenant Memory, backups, exports, legacy read path) exists on disk."""
        try:
            self.storage_scope_root.mkdir(parents=True, exist_ok=True)
            self.root.mkdir(parents=True, exist_ok=True)
            self.backups.mkdir(parents=True, exist_ok=True)
            self.workspace_reset_backups_dir.mkdir(parents=True, exist_ok=True)
            self.quarantine.mkdir(parents=True, exist_ok=True)
            self.exports.mkdir(parents=True, exist_ok=True)
            self.legacy_root.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            log_warn(
                "Memory directory ensure failed.",
                error=str(exc),
                storage_scope_root=str(self.storage_scope_root),
                memory_root=str(self.root),
            )
            raise

    def _read_json_path(self, path: Path, fallback: Any) -> Any:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return fallback

    def _normalize_imported_memory_path(self, value: Any) -> str:
        text = str(value or "").strip().replace("/", "\\")
        if not text:
            return ""
        while "\\\\" in text:
            text = text.replace("\\\\", "\\")
        lowered = text.lower()
        if lowered.startswith("documents\\root\\"):
            return text[len("Documents\\") :]
        if lowered == "documents\\root":
            return "Root"
        return text

    def _normalize_imported_allocations_payload(self, payload: Any) -> Any:
        if not isinstance(payload, list):
            return payload
        normalized_payload = []
        for row in payload:
            if isinstance(row, dict):
                normalized_row = dict(row)
                normalized_row["RequestedDestinationPath"] = self._normalize_imported_memory_path(
                    normalized_row.get("RequestedDestinationPath", "")
                )
                normalized_payload.append(normalized_row)
            else:
                normalized_payload.append(row)
        return normalized_payload

    def _normalize_imported_proposed_payload(self, payload: Any) -> Any:
        if not isinstance(payload, list):
            return payload
        normalized_payload = []
        for row in payload:
            if isinstance(row, dict):
                normalized_row = dict(row)
                normalized_row["ParentPath"] = self._normalize_imported_memory_path(normalized_row.get("ParentPath", ""))
                normalized_row["DestinationPath"] = self._normalize_imported_memory_path(
                    normalized_row.get("DestinationPath", "")
                )
                normalized_payload.append(normalized_row)
            else:
                normalized_payload.append(row)
        return normalized_payload

    def _normalize_imported_session_payload(self, payload: Any) -> Any:
        if not isinstance(payload, dict):
            return payload
        normalized_payload = dict(payload)
        normalized_payload["DestinationSelectedPath"] = self._normalize_imported_memory_path(
            normalized_payload.get("DestinationSelectedPath", "")
        )
        expanded_paths = normalized_payload.get("DestinationExpandedPaths", [])
        if isinstance(expanded_paths, list):
            normalized_payload["DestinationExpandedPaths"] = [
                self._normalize_imported_memory_path(path) for path in expanded_paths
            ]
        return normalized_payload

    def _list_count(self, payload: Any) -> int:
        return len(payload) if isinstance(payload, list) else 0

    def _normalize_candidate_timestamp(self, timestamp: datetime | None) -> tuple[datetime, str]:
        if timestamp is None:
            return datetime.min.replace(tzinfo=timezone.utc), "missing"
        if timestamp.tzinfo is None or timestamp.tzinfo.utcoffset(timestamp) is None:
            normalized = timestamp.replace(tzinfo=timezone.utc)
            return normalized, "naive_assumed_utc"
        return timestamp.astimezone(timezone.utc), "aware_normalized_utc"

    def _parse_candidate_timestamp(self, value: str) -> tuple[datetime | None, str]:
        text = str(value or "").strip()
        if not text:
            return None, "missing"

        normalized = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
            timestamp, kind = self._normalize_candidate_timestamp(parsed)
            return timestamp, f"iso_{kind}"
        except Exception:
            pass

        for fmt in ("%m/%d/%Y %I:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(text, fmt)
                timestamp, kind = self._normalize_candidate_timestamp(parsed)
                return timestamp, f"strptime_{fmt}_{kind}"
            except Exception:
                continue

        return None, "invalid"

    def _latest_backup_file(self, backup_root: Path, prefixes: tuple[str, ...], *, require_populated: bool = False) -> Path | None:
        candidates: list[Path] = []
        for prefix in prefixes:
            candidates.extend(backup_root.glob(f"{prefix}_*.json"))
        # Backup file names are timestamped (`<prefix>_YYYYMMDD-HHMMSS-fff.json`), so name sort
        # is enough and avoids thousands of expensive stat() calls on slow/cloud-backed folders.
        candidates.sort(key=lambda item: item.name, reverse=True)

        for path in candidates:
            if not require_populated:
                return path
            payload = self._read_json_path(path, [])
            if self._list_count(payload) > 0:
                return path

        return None

    def _inspect_candidate(self, name: str, session_path: Path, allocations_path: Path, proposed_path: Path) -> dict[str, Any]:
        session_raw = self._read_json_path(session_path, {})
        allocations_raw = self._read_json_path(allocations_path, [])
        proposed_raw = self._read_json_path(proposed_path, [])
        session_state = SessionState.from_dict(session_raw if isinstance(session_raw, dict) else {})
        allocation_count = self._list_count(allocations_raw)
        proposed_count = self._list_count(proposed_raw)
        timestamp = None
        timestamp_kind = "missing"
        if isinstance(session_raw, dict):
            timestamp, timestamp_kind = self._parse_candidate_timestamp(session_raw.get("LastSavedUtc", ""))
            if timestamp is None:
                timestamp, timestamp_kind = self._parse_candidate_timestamp(session_raw.get("CreatedUtc", ""))
        if timestamp is None:
            try:
                raw_timestamp = datetime.fromtimestamp(max(
                    session_path.stat().st_mtime if session_path.exists() else 0,
                    allocations_path.stat().st_mtime if allocations_path.exists() else 0,
                    proposed_path.stat().st_mtime if proposed_path.exists() else 0,
                ))
                timestamp, normalized_kind = self._normalize_candidate_timestamp(raw_timestamp)
                timestamp_kind = f"filesystem_fallback_{normalized_kind}"
            except Exception:
                timestamp, normalized_kind = self._normalize_candidate_timestamp(None)
                timestamp_kind = f"datetime_min_fallback_{normalized_kind}"

        populated = allocation_count > 0 or proposed_count > 0
        valid = isinstance(session_raw, dict) and isinstance(allocations_raw, list) and isinstance(proposed_raw, list)
        timestamp_sort_value = timestamp.timestamp() if timestamp != datetime.min.replace(tzinfo=timezone.utc) else float("-inf")
        log_info(
            "Memory candidate timestamp normalized.",
            candidate=name,
            parsed_timestamp_kind=timestamp_kind,
            normalized_comparison_value=timestamp_sort_value,
            timestamp_iso=timestamp.isoformat(),
        )
        return {
            "name": name,
            "storage_root": str(session_path.parent.parent) if session_path.parent.name.lower() == "memory" else str(session_path.parent),
            "session_path": session_path,
            "allocations_path": allocations_path,
            "proposed_path": proposed_path,
            "session_state": session_state,
            "session_raw": session_raw if isinstance(session_raw, dict) else {},
            "allocations_raw": allocations_raw if isinstance(allocations_raw, list) else [],
            "proposed_raw": proposed_raw if isinstance(proposed_raw, list) else [],
            "draft_id": session_state.DraftId or str((session_raw or {}).get("ActiveDraftId", "") or ""),
            "fingerprint": session_state.SessionFingerprint,
            "allocation_count": allocation_count,
            "proposed_count": proposed_count,
            "populated": populated,
            "valid": valid,
            "timestamp": timestamp,
            "timestamp_kind": timestamp_kind,
            "timestamp_sort_value": timestamp_sort_value,
        }

    def _tree_snapshot_list_recursive_count(self, nodes: Any) -> int:
        n = 0
        if not isinstance(nodes, list):
            return 0
        for node in nodes:
            if isinstance(node, dict):
                n += 1
                n += self._tree_snapshot_list_recursive_count(node.get("children") or [])
        return n

    def _tree_snapshot_node_counts_from_raw(self, raw: dict[str, Any]) -> tuple[int, int]:
        if not isinstance(raw, dict):
            return 0, 0
        s_raw = raw.get("SourceTreeSnapshot") or []
        d_raw = raw.get("DestinationTreeSnapshot") or []
        s_list = s_raw if isinstance(s_raw, list) else []
        d_list = d_raw if isinstance(d_raw, list) else []
        return (
            self._tree_snapshot_list_recursive_count(s_list),
            self._tree_snapshot_list_recursive_count(d_list),
        )

    def restore_candidate_field_metrics(self, candidate: dict[str, Any]) -> dict[str, Any]:
        """Selector + snapshot richness used for restore scoring and sparse-primary detection."""
        ss = candidate.get("session_state")
        raw = candidate.get("session_raw") if isinstance(candidate.get("session_raw"), dict) else {}
        if not isinstance(ss, SessionState):
            ss = SessionState()
        s_nodes, d_nodes = self._tree_snapshot_node_counts_from_raw(raw)

        def _g(attr: str, raw_key: str) -> str:
            try:
                v = getattr(ss, attr, "") if ss is not None else ""
            except Exception:
                v = ""
            if str(v or "").strip():
                return str(v).strip()
            return str(raw.get(raw_key, "") or "").strip()

        has_src_site = bool(
            _g("SelectedSourceSiteKey", "SelectedSourceSiteKey")
            or _g("SelectedSourceSite", "SelectedSourceSite")
        )
        has_dst_site = bool(
            _g("SelectedDestinationSiteKey", "SelectedDestinationSiteKey")
            or _g("SelectedDestinationSite", "SelectedDestinationSite")
        )
        has_src_lib = bool(
            _g("SelectedSourceLibraryId", "SelectedSourceLibraryId")
            or _g("SelectedSourceLibrary", "SelectedSourceLibrary")
        )
        has_dst_lib = bool(
            _g("SelectedDestinationLibraryId", "SelectedDestinationLibraryId")
            or _g("SelectedDestinationLibrary", "SelectedDestinationLibrary")
        )
        return {
            "has_source_selector": bool(has_src_site or has_src_lib),
            "has_destination_selector": bool(has_dst_site or has_dst_lib),
            "has_source_library_hint": bool(has_src_lib),
            "has_destination_library_hint": bool(has_dst_lib),
            "source_snapshot_nodes": int(s_nodes),
            "destination_snapshot_nodes": int(d_nodes),
        }

    def restore_candidate_population_score(self, candidate: dict[str, Any]) -> int:
        m = self.restore_candidate_field_metrics(candidate)
        ac = int(candidate.get("allocation_count", 0) or 0)
        pc = int(candidate.get("proposed_count", 0) or 0)
        snap = int(m["source_snapshot_nodes"]) + int(m["destination_snapshot_nodes"])
        sel = (3 if m.get("has_source_library_hint") else 0) + (3 if m.get("has_destination_library_hint") else 0)
        return ac * 1_000_000 + pc * 10_000 + snap * 100 + sel

    def _restore_primary_is_sparse_for_promotion(self, primary: dict[str, Any]) -> bool:
        """True when live primary has no workload and no *library* hints or tree snapshots.

        Site-only strings do not count as selector material — a same-draft backup with libraries
        or allocations can still be promoted.
        """
        if not primary.get("valid"):
            return False
        ac = int(primary.get("allocation_count", 0) or 0)
        pc = int(primary.get("proposed_count", 0) or 0)
        if ac > 0 or pc > 0 or primary.get("populated"):
            return False
        m = self.restore_candidate_field_metrics(primary)
        if m.get("has_source_library_hint") or m.get("has_destination_library_hint"):
            return False
        if m["source_snapshot_nodes"] + m["destination_snapshot_nodes"] > 0:
            return False
        return True

    def _restore_draft_ids_match(self, a: dict[str, Any], b: dict[str, Any]) -> bool:
        da = str(a.get("draft_id") or "").strip()
        db = str(b.get("draft_id") or "").strip()
        if not da or not db:
            return False
        return da.casefold() == db.casefold()

    def _restore_fingerprints_compatible(self, a: dict[str, Any], b: dict[str, Any]) -> bool:
        fa = str(a.get("fingerprint") or "").strip().lower()
        fb = str(b.get("fingerprint") or "").strip().lower()
        if not fa or not fb:
            return True
        return fa == fb

    def _restore_backup_too_stale_vs_primary(self, primary: dict[str, Any], backup: dict[str, Any]) -> bool:
        """True when backup is far older than primary *and* not strictly richer (anti-stale promotion)."""
        pb = float(primary.get("timestamp_sort_value") or float("-inf"))
        bb = float(backup.get("timestamp_sort_value") or float("-inf"))
        if pb == float("-inf") or bb == float("-inf"):
            return False
        if bb >= pb - 120.0:
            return False
        if bb >= pb - _RESTORE_BACKUP_STALENESS_SOFT_SEC:
            return False
        sp = self.restore_candidate_population_score(primary)
        sb = self.restore_candidate_population_score(backup)
        return sb <= sp

    def _log_restore_candidate_population_scores(
        self,
        primary: dict[str, Any] | None,
        backup: dict[str, Any] | None,
    ) -> None:
        for label, cand in (("python_live_primary", primary), ("python_backup_latest", backup)):
            if cand is None:
                continue
            m = self.restore_candidate_field_metrics(cand)
            log_info(
                "restore_candidate_population_score",
                candidate_kind=str(cand.get("name", label)),
                draft_id=str(cand.get("draft_id", "") or "")[:120],
                populated=bool(cand.get("populated")),
                allocation_count=int(cand.get("allocation_count", 0) or 0),
                proposed_count=int(cand.get("proposed_count", 0) or 0),
                has_source_selector=bool(m.get("has_source_selector")),
                has_destination_selector=bool(m.get("has_destination_selector")),
                has_source_library_hint=bool(m.get("has_source_library_hint")),
                has_destination_library_hint=bool(m.get("has_destination_library_hint")),
                source_snapshot_nodes=int(m.get("source_snapshot_nodes", 0)),
                destination_snapshot_nodes=int(m.get("destination_snapshot_nodes", 0)),
                score=int(self.restore_candidate_population_score(cand)),
            )

    def discover_restore_candidates(self) -> list[dict[str, Any]]:
        candidates = [
            self._inspect_candidate(
                "python_live_primary",
                self.paths["session"],
                self.paths["allocations"],
                self.paths["proposed"],
            ),
            self._inspect_candidate(
                "python_live_recovery",
                self.paths["session_recovery"],
                self.paths["allocations_recovery"],
                self.paths["proposed_recovery"],
            ),
        ]

        backup_session = self._latest_backup_file(self.backups, ("Draft-SessionState",), require_populated=False)
        backup_allocations = self._latest_backup_file(self.backups, ("Draft-AllocationQueue", "AllocationQueue"), require_populated=True)
        backup_proposed = self._latest_backup_file(self.backups, ("Draft-ProposedFolders", "ProposedFolders"), require_populated=False)
        if backup_session and backup_allocations and backup_proposed:
            candidates.append(self._inspect_candidate(
                "python_backup_latest",
                backup_session,
                backup_allocations,
                backup_proposed,
            ))

        # Startup fast-path: if active Python-scope memory already has a populated candidate,
        # skip global/legacy scans. Those scans are only needed for migration/fallback and can
        # be very slow on large OneDrive-backed backup folders.
        has_local_populated = any(
            bool(c.get("populated")) and str(c.get("name", "")).startswith("python_live_")
            for c in candidates
        )
        if has_local_populated:
            log_trace(
                "memory",
                "discover_restore_candidates_fast_path",
                candidate_count=len(candidates),
                candidate_names=[str(c.get("name", "")) for c in candidates],
            )
            return candidates

        global_root = memory_root()
        if self.root != global_root:
            global_paths = {
                "session": global_root / "Draft-SessionState.json",
                "allocations": global_root / "Draft-AllocationQueue.json",
                "proposed": global_root / "Draft-ProposedFolders.json",
                "session_recovery": global_root / "Draft-SessionState.recovery.json",
                "allocations_recovery": global_root / "Draft-AllocationQueue.recovery.json",
                "proposed_recovery": global_root / "Draft-ProposedFolders.recovery.json",
            }
            candidates.extend([
                self._inspect_candidate(
                    "python_global_primary",
                    global_paths["session"],
                    global_paths["allocations"],
                    global_paths["proposed"],
                ),
                self._inspect_candidate(
                    "python_global_recovery",
                    global_paths["session_recovery"],
                    global_paths["allocations_recovery"],
                    global_paths["proposed_recovery"],
                ),
            ])
            global_backups = global_root / "Backups"
            backup_session = self._latest_backup_file(global_backups, ("Draft-SessionState",), require_populated=False) if global_backups.exists() else None
            backup_allocations = self._latest_backup_file(global_backups, ("Draft-AllocationQueue", "AllocationQueue"), require_populated=True) if global_backups.exists() else None
            backup_proposed = self._latest_backup_file(global_backups, ("Draft-ProposedFolders", "ProposedFolders"), require_populated=False) if global_backups.exists() else None
            if backup_session and backup_allocations and backup_proposed:
                candidates.append(self._inspect_candidate(
                    "python_global_backup_latest",
                    backup_session,
                    backup_allocations,
                    backup_proposed,
                ))

        candidates.extend([
            self._inspect_candidate(
                "legacy_live_primary",
                self.legacy_paths["session"],
                self.legacy_paths["allocations"],
                self.legacy_paths["proposed"],
            ),
            self._inspect_candidate(
                "legacy_live_recovery",
                self.legacy_paths["session_recovery"],
                self.legacy_paths["allocations_recovery"],
                self.legacy_paths["proposed_recovery"],
            ),
        ])

        legacy_backup_root = self.legacy_root / "Backups"
        backup_session = self._latest_backup_file(legacy_backup_root, ("Draft-SessionState",), require_populated=False) if legacy_backup_root.exists() else None
        backup_allocations = self._latest_backup_file(legacy_backup_root, ("Draft-AllocationQueue", "AllocationQueue"), require_populated=True) if legacy_backup_root.exists() else None
        backup_proposed = self._latest_backup_file(legacy_backup_root, ("Draft-ProposedFolders", "ProposedFolders"), require_populated=False) if legacy_backup_root.exists() else None
        if backup_session and backup_allocations and backup_proposed:
            candidates.append(self._inspect_candidate(
                "legacy_backup_latest",
                backup_session,
                backup_allocations,
                backup_proposed,
            ))

        log_trace(
            "memory",
            "discover_restore_candidates",
            candidate_count=len(candidates),
            candidate_names=[str(c.get("name", "")) for c in candidates],
        )
        return candidates

    def select_restore_candidate(self, candidates: list[dict[str, Any]] | None = None) -> tuple[dict[str, Any] | None, str]:
        inspected = candidates or self.discover_restore_candidates()
        if not inspected:
            log_trace("memory", "select_restore_candidate", selected_name=None, reason="no candidates")
            return None, "no candidates"

        if self.expected_fingerprint:
            matching = [
                candidate for candidate in inspected
                if str(candidate.get("fingerprint", "")).strip().lower() == self.expected_fingerprint
            ]
            if matching:
                inspected = matching
            else:
                current_scope_root = str(self.root).lower()
                inspected = [
                    candidate for candidate in inspected
                    if str(candidate.get("storage_root", "")).lower().startswith(current_scope_root)
                ]
                if not inspected:
                    log_trace(
                        "memory",
                        "select_restore_candidate",
                        selected_name=None,
                        reason="no_user_scoped_candidates",
                        expected_fingerprint_excerpt=str(self.expected_fingerprint)[:80],
                    )
                    return None, f"no user-scoped memory candidates for {self.expected_fingerprint}"

        primary = next((c for c in inspected if str(c.get("name", "")) == "python_live_primary"), None)
        recovery = next((c for c in inspected if str(c.get("name", "")) == "python_live_recovery"), None)
        backup_latest = next((c for c in inspected if str(c.get("name", "")) == "python_backup_latest"), None)

        if primary and primary.get("valid"):
            self._log_restore_candidate_population_scores(primary, backup_latest)
            promote_backup = False
            promote_note = ""
            if backup_latest and backup_latest.get("valid"):
                sparse = self._restore_primary_is_sparse_for_promotion(primary)
                same_draft = self._restore_draft_ids_match(primary, backup_latest)
                fp_ok = self._restore_fingerprints_compatible(primary, backup_latest)
                stale = self._restore_backup_too_stale_vs_primary(primary, backup_latest)
                richer = self.restore_candidate_population_score(backup_latest) > self.restore_candidate_population_score(
                    primary
                )
                if sparse and same_draft and fp_ok and (not stale) and richer:
                    promote_backup = True
                    promote_note = (
                        "python_backup_latest promoted: sparse_primary "
                        f"draft_match={same_draft} fingerprint_ok={fp_ok} richer={richer}"
                    )
                elif sparse and same_draft and backup_latest and not richer:
                    log_info(
                        "restore_candidate_empty_primary_rejected",
                        note="backup_not_strictly_richer",
                        primary_draft=str(primary.get("draft_id", "") or "")[:80],
                    )
                elif sparse and backup_latest and not same_draft:
                    log_info(
                        "restore_candidate_empty_primary_rejected",
                        note="draft_id_mismatch_cannot_promote_backup",
                        primary_draft=str(primary.get("draft_id", "") or "")[:80],
                        backup_draft=str(backup_latest.get("draft_id", "") or "")[:80],
                    )
                elif sparse and backup_latest and not fp_ok:
                    log_info(
                        "restore_candidate_empty_primary_rejected",
                        note="fingerprint_mismatch_cannot_promote_backup",
                    )
                elif sparse and backup_latest and stale:
                    log_info(
                        "restore_candidate_empty_primary_rejected",
                        note="backup_too_stale_vs_primary",
                    )

            if promote_backup and backup_latest is not None:
                selected = backup_latest
                reason = (
                    "authoritative_python_backup_promoted_over_sparse_primary "
                    f"valid={selected.get('valid')} populated={selected.get('populated')} "
                    f"allocations={selected.get('allocation_count', 0)} "
                    f"proposed={selected.get('proposed_count', 0)} "
                    f"timestamp_sort_value={selected.get('timestamp_sort_value', float('-inf'))} "
                    f"detail={promote_note}"
                )
                log_info(
                    "restore_candidate_backup_promoted",
                    selected_name=str(selected.get("name", "")),
                    draft_id=str(selected.get("draft_id", "") or "")[:120],
                    allocation_count=int(selected.get("allocation_count", 0) or 0),
                    proposed_count=int(selected.get("proposed_count", 0) or 0),
                    populated=bool(selected.get("populated")),
                )
                log_info(
                    "Restore candidate selected (backup promoted over sparse live primary).",
                    selected_name=str(selected.get("name", "")),
                    allocation_count=int(selected.get("allocation_count", 0) or 0),
                    proposed_count=int(selected.get("proposed_count", 0) or 0),
                    populated=bool(selected.get("populated")),
                    stale_backup_skipped=False,
                )
                log_trace("memory", "select_restore_candidate", selected_name="python_backup_latest", reason_excerpt=reason[:400])
                return selected, reason

            selected = primary
            reason = (
                "authoritative_python_live_primary "
                f"valid={selected.get('valid')} populated={selected.get('populated')} "
                f"allocations={selected.get('allocation_count', 0)} "
                f"proposed={selected.get('proposed_count', 0)} "
                f"timestamp_sort_value={selected.get('timestamp_sort_value', float('-inf'))}"
            )
            log_info(
                "Restore candidate selected (live primary is authoritative).",
                selected_name=str(selected.get("name", "")),
                allocation_count=int(selected.get("allocation_count", 0) or 0),
                proposed_count=int(selected.get("proposed_count", 0) or 0),
                populated=bool(selected.get("populated")),
                stale_backup_skipped=True,
            )
            log_trace("memory", "select_restore_candidate", selected_name="python_live_primary", reason_excerpt=reason[:400])
            return selected, reason

        if recovery and recovery.get("valid"):
            selected = recovery
            reason = (
                "authoritative_python_live_recovery "
                f"valid={selected.get('valid')} populated={selected.get('populated')} "
                f"allocations={selected.get('allocation_count', 0)} "
                f"proposed={selected.get('proposed_count', 0)} "
                f"timestamp_sort_value={selected.get('timestamp_sort_value', float('-inf'))}"
            )
            log_info(
                "Restore candidate selected (live recovery is authoritative).",
                selected_name=str(selected.get("name", "")),
                allocation_count=int(selected.get("allocation_count", 0) or 0),
                proposed_count=int(selected.get("proposed_count", 0) or 0),
                populated=bool(selected.get("populated")),
                stale_backup_skipped=True,
            )
            log_trace("memory", "select_restore_candidate", selected_name="python_live_recovery", reason_excerpt=reason[:400])
            return selected, reason

        ranked = sorted(
            inspected,
            key=lambda candidate: (
                1 if candidate.get("valid") else 0,
                1 if candidate.get("populated") else 0,
                _restore_candidate_preference_score(str(candidate.get("name", ""))),
                candidate.get("timestamp_sort_value", float("-inf")),
            ),
            reverse=True,
        )
        selected = ranked[0]
        reason = (
            f"fallback_selected={selected.get('name')} valid={selected.get('valid')} "
            f"populated={selected.get('populated')} "
            f"allocations={selected.get('allocation_count', 0)} "
            f"proposed={selected.get('proposed_count', 0)} "
            f"draft_id={selected.get('draft_id', '') or 'none'} "
            f"timestamp={selected.get('timestamp')} "
            f"timestamp_kind={selected.get('timestamp_kind', 'unknown')} "
            f"timestamp_sort_value={selected.get('timestamp_sort_value', float('-inf'))}"
        )
        log_info(
            "Restore candidate selected (fallback; live paths invalid or missing).",
            selected_name=str(selected.get("name", "")),
            allocation_count=int(selected.get("allocation_count", 0) or 0),
            proposed_count=int(selected.get("proposed_count", 0) or 0),
            populated=bool(selected.get("populated")),
            stale_backup_skipped=False,
        )
        log_trace(
            "memory",
            "select_restore_candidate",
            selected_name=str(selected.get("name", "")),
            reason_excerpt=reason[:400],
        )
        return selected, reason

    def _python_store_has_populated_draft(self, candidates: list[dict[str, Any]]) -> bool:
        return any(
            candidate.get("name", "").startswith("python_") and candidate.get("populated")
            for candidate in candidates
        )

    def _copy_candidate_into_current_root(self, candidate: dict[str, Any]) -> bool:
        try:
            shutil.copy2(candidate["session_path"], self.paths["session"])
            shutil.copy2(candidate["allocations_path"], self.paths["allocations"])
            shutil.copy2(candidate["proposed_path"], self.paths["proposed"])
            shutil.copy2(self.paths["session"], self.paths["session_recovery"])
            shutil.copy2(self.paths["allocations"], self.paths["allocations_recovery"])
            shutil.copy2(self.paths["proposed"], self.paths["proposed_recovery"])
            self.refresh_manifest(
                draft_id=str(candidate.get("draft_id", "") or ""),
                fingerprint=str(candidate.get("fingerprint", "") or ""),
                status="Healthy",
            )
            log_info(
                "Draft copied into active memory root.",
                source_candidate=candidate.get("name"),
                source_root=candidate.get("storage_root"),
                write_root=str(self.root),
            )
            return True
        except Exception as exc:
            log_warn(
                "Draft copy into active memory root failed.",
                error=str(exc),
                source_candidate=candidate.get("name"),
                source_root=candidate.get("storage_root"),
                write_root=str(self.root),
            )
            return False

    def prepare_selected_candidate_for_runtime(self, candidate: dict[str, Any] | None, candidates: list[dict[str, Any]] | None = None) -> dict[str, Any] | None:
        if not candidate:
            return candidate

        selected_name = str(candidate.get("name", ""))
        self.current_restore_source = "legacy" if selected_name.startswith("legacy_") else "python"
        log_info(
            "Memory restore source selected.",
            restore_source=self.current_restore_source,
            selected_candidate=selected_name,
            selected_storage_root=candidate.get("storage_root", ""),
            write_root=str(self.root),
        )
        log_trace(
            "memory",
            "prepare_selected_candidate_for_runtime",
            selected_name=selected_name,
            restore_source=self.current_restore_source,
        )

        inspected = candidates or []
        selected_storage_root = str(candidate.get("storage_root", ""))
        current_storage_root = str(self.root)
        should_copy_to_current_root = (
            selected_storage_root
            and Path(selected_storage_root) != self.root
            and (
                self.current_restore_source == "legacy"
                or not self._python_store_has_populated_draft(inspected)
                or str(candidate.get("name", "")).startswith("python_global_")
            )
        )
        if should_copy_to_current_root:
            copied = self._copy_candidate_into_current_root(candidate)
            if copied:
                python_candidate = self._inspect_candidate(
                    "python_live_primary",
                    self.paths["session"],
                    self.paths["allocations"],
                    self.paths["proposed"],
                )
                self.current_restore_source = "python"
                log_info(
                    "Legacy draft copied and switched to Python root.",
                    selected_candidate=python_candidate.get("name"),
                    selected_storage_root=python_candidate.get("storage_root", ""),
                    write_root=str(self.root),
                )
                return python_candidate

        return candidate

    def load_candidate_payload(
        self,
        candidate: dict[str, Any],
    ) -> tuple[SessionState, list[AllocationRow], list[ProposedFolder], dict[str, Any]]:
        """Load session/allocations/proposed from the candidate's on-disk paths (not stale inspect caches)."""
        session_raw: dict[str, Any] = {}
        session_path = candidate.get("session_path")
        try:
            if session_path is not None:
                sp = Path(session_path)
                if sp.is_file():
                    session_raw = self._read_json_path(sp, {})
        except Exception:
            session_raw = {}
        if not isinstance(session_raw, dict) or not session_raw:
            embedded_raw = candidate.get("session_raw")
            if isinstance(embedded_raw, dict):
                session_raw = dict(embedded_raw)
        session_state = SessionState.from_dict(session_raw if isinstance(session_raw, dict) else {})

        allocations_raw = candidate.get("allocations_raw", [])
        alloc_path = candidate.get("allocations_path")
        try:
            if alloc_path is not None:
                ap = Path(alloc_path)
                if ap.is_file():
                    ar = self._read_json_path(ap, [])
                    if isinstance(ar, list):
                        allocations_raw = ar
        except Exception:
            pass
        allocations = [AllocationRow.from_dict(item) for item in allocations_raw if isinstance(item, dict)]

        proposed_raw = candidate.get("proposed_raw", [])
        proposed_path = candidate.get("proposed_path")
        try:
            if proposed_path is not None:
                pp = Path(proposed_path)
                if pp.is_file():
                    pr = self._read_json_path(pp, [])
                    if isinstance(pr, list):
                        proposed_raw = pr
        except Exception:
            pass
        proposed = [ProposedFolder.from_dict(item) for item in proposed_raw if isinstance(item, dict)]

        return session_state, allocations, proposed, session_raw if isinstance(session_raw, dict) else {}

    def initialize_store(self) -> None:
        defaults = {
            "allocations": [],
            "allocations_recovery": [],
            "proposed": [],
            "proposed_recovery": [],
        }
        for key, default_value in defaults.items():
            self._ensure_json_file(self.paths[key], default_value)
        for key in ("session", "session_recovery"):
            self._ensure_json_file(self.paths[key], SessionState().to_dict())
        if not self.paths["manifest"].exists():
            self.save_manifest(MemoryManifest(
                AllocationQueuePath=str(self.paths["allocations"]),
                ProposedFoldersPath=str(self.paths["proposed"]),
                SessionStatePath=str(self.paths["session"]),
            ))

    def _ensure_json_file(self, path: Path, default_obj: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            self._atomic_write_text(path, json.dumps(default_obj, indent=2))
            return
        try:
            raw = path.read_text(encoding="utf-8").strip()
            if not raw:
                raise ValueError("empty")
            json.loads(raw)
        except Exception:
            stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            quarantine_path = self.quarantine / f"{path.stem}.corrupt-{stamp}{path.suffix}"
            try:
                shutil.move(str(path), str(quarantine_path))
            except Exception:
                pass
            self._atomic_write_text(path, json.dumps(default_obj, indent=2))
            log_warn("Memory file re-initialized after corruption detection.", file=str(path), quarantine=str(quarantine_path))

    def _read_json(self, path: Path, fallback: Any) -> Any:
        return self._read_json_path(path, fallback)

    def _json_count(self, path: Path) -> int:
        data = self._read_json(path, [])
        return len(data) if isinstance(data, list) else 0

    def _backup_file(self, path: Path, prefix: str) -> None:
        if not path.exists():
            return
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:-3]
        backup_path = self.backups / f"{prefix}_{stamp}.json"
        shutil.copy2(path, backup_path)

    def _atomic_write_text(self, target: Path, text: str) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(
            prefix=f"{target.stem}.",
            suffix=f"{target.suffix}.tmp",
            dir=str(target.parent),
        )
        temp_path = Path(temp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
            temp_path.replace(target)
            try:
                dir_fd = os.open(str(target.parent), os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except Exception:
                pass
        finally:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass

    def _write_json_safely(self, target: Path, recovery: Path | None, payload: Any) -> None:
        serialized = json.dumps(payload, indent=2, ensure_ascii=False)
        json.loads(serialized)

        self._backup_file(target, target.stem)
        self._atomic_write_text(target, serialized)

        if recovery is not None:
            self._atomic_write_text(recovery, serialized)

    def load_allocations(self) -> list[AllocationRow]:
        data = self._read_json(self.paths["allocations"], [])
        rows = [AllocationRow.from_dict(x) for x in data]
        log_trace("memory", "load_allocations", row_count=len(rows), path_excerpt=str(self.paths["allocations"])[-80:])
        return rows

    def _maybe_log_planning_recovery_candidate(
        self,
        *,
        planning_file: str,
        glob_pattern: str,
        live_primary_count: int,
        save_reason: str,
        persist_context: dict[str, Any] | None,
    ) -> None:
        """When the live primary file is empty on disk, log if a rotated backup still has rows (recovery hint)."""
        try:
            if live_primary_count > 0 or not self.backups.is_dir():
                return
            candidates = sorted(
                self.backups.glob(glob_pattern),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            for bp in candidates[:40]:
                bc = self._json_count(bp)
                if bc > 0:
                    log_info(
                        "planning_memory_recovery_candidate_found",
                        planning_file=str(planning_file),
                        backup_path=str(bp),
                        backup_row_count=int(bc),
                        live_primary_count=int(live_primary_count),
                        save_reason=str(save_reason or "")[:240],
                        **_persist_ctx_for_log(persist_context),
                    )
                    return
        except Exception:
            return

    def save_allocations(
        self,
        rows: list[AllocationRow],
        *,
        allow_empty: bool | None = None,
        allow_empty_planning_persist: bool = False,
        save_reason: str = "",
        persist_context: dict[str, Any] | None = None,
    ) -> None:
        """Persist allocation queue. Empty overwrite of a non-empty file requires allow_empty_planning_persist."""
        if allow_empty is not None:
            allow_empty_planning_persist = bool(allow_empty_planning_persist or allow_empty)

        payload = [r.to_dict() for r in rows]
        new_count = len(payload)
        target = self.paths["allocations"]
        existing_count = self._json_count(target)
        ctx = dict(persist_context or {})
        ctx_log = _persist_ctx_for_log(ctx)

        if new_count == 0 and existing_count == 0:
            self._maybe_log_planning_recovery_candidate(
                planning_file="Draft-AllocationQueue.json",
                glob_pattern="Draft-AllocationQueue*.json",
                live_primary_count=0,
                save_reason=save_reason,
                persist_context=ctx,
            )

        if new_count == 0 and existing_count > 0:
            # Post-import: do not allow even "explicit" empty persist until runtime has hydrated matching counts.
            if (
                allow_empty_planning_persist
                and self._import_restore_empty_guard is not None
                and not self._import_restore_runtime_confirmed
            ):
                log_info(
                    "import_restore_empty_write_blocked",
                    channel="allocations",
                    previous_count=int(existing_count),
                    new_count=int(new_count),
                    save_reason=str(save_reason or "")[:240],
                    guard_expected_alloc=int(self._import_restore_empty_guard[0]),
                    guard_expected_proposed=int(self._import_restore_empty_guard[1]),
                )
                return
            log_info(
                "allocation_queue_empty_write_attempt",
                previous_count=int(existing_count),
                new_count=int(new_count),
                save_reason=str(save_reason or "")[:240],
                allow_empty_planning_persist=bool(allow_empty_planning_persist),
                **ctx_log,
            )
            if not allow_empty_planning_persist:
                log_info(
                    "allocation_queue_empty_write_blocked",
                    previous_count=int(existing_count),
                    new_count=int(new_count),
                    save_reason=str(save_reason or "")[:240],
                    **ctx_log,
                )
                return
            log_info(
                "allocation_queue_empty_write_allowed_explicit_clear",
                previous_count=int(existing_count),
                new_count=int(new_count),
                save_reason=str(save_reason or "")[:240],
                **ctx_log,
            )

        self._write_json_safely(
            self.paths["allocations"],
            self.paths["allocations_recovery"],
            payload,
        )
        log_trace(
            "memory",
            "save_allocations",
            row_count=len(payload),
            allow_empty_planning_persist=allow_empty_planning_persist,
        )

    def load_proposed(self) -> list[ProposedFolder]:
        data = self._read_json(self.paths["proposed"], [])
        rows = [ProposedFolder.from_dict(x) for x in data]
        log_trace("memory", "load_proposed", row_count=len(rows))
        return rows

    def save_proposed(
        self,
        rows: list[ProposedFolder],
        *,
        allow_empty: bool | None = None,
        allow_empty_planning_persist: bool = False,
        save_reason: str = "",
        persist_context: dict[str, Any] | None = None,
    ) -> None:
        if allow_empty is not None:
            allow_empty_planning_persist = bool(allow_empty_planning_persist or allow_empty)

        payload = [r.to_dict() for r in rows]
        new_count = len(payload)
        existing_count = self._json_count(self.paths["proposed"])
        ctx = dict(persist_context or {})
        ctx_log = _persist_ctx_for_log(ctx)

        if new_count == 0 and existing_count == 0:
            self._maybe_log_planning_recovery_candidate(
                planning_file="Draft-ProposedFolders.json",
                glob_pattern="Draft-ProposedFolders*.json",
                live_primary_count=0,
                save_reason=save_reason,
                persist_context=ctx,
            )

        if new_count == 0 and existing_count > 0:
            if (
                allow_empty_planning_persist
                and self._import_restore_empty_guard is not None
                and not self._import_restore_runtime_confirmed
            ):
                log_info(
                    "import_restore_empty_write_blocked",
                    channel="proposed",
                    previous_count=int(existing_count),
                    new_count=int(new_count),
                    save_reason=str(save_reason or "")[:240],
                    guard_expected_alloc=int(self._import_restore_empty_guard[0]),
                    guard_expected_proposed=int(self._import_restore_empty_guard[1]),
                )
                return
            log_info(
                "proposed_folders_empty_write_attempt",
                previous_count=int(existing_count),
                new_count=int(new_count),
                save_reason=str(save_reason or "")[:240],
                allow_empty_planning_persist=bool(allow_empty_planning_persist),
                **ctx_log,
            )
            if not allow_empty_planning_persist:
                log_info(
                    "proposed_folders_empty_write_blocked",
                    previous_count=int(existing_count),
                    new_count=int(new_count),
                    save_reason=str(save_reason or "")[:240],
                    **ctx_log,
                )
                return
            log_info(
                "proposed_folders_empty_write_allowed_explicit_clear",
                previous_count=int(existing_count),
                new_count=int(new_count),
                save_reason=str(save_reason or "")[:240],
                **ctx_log,
            )

        self._write_json_safely(
            self.paths["proposed"],
            self.paths["proposed_recovery"],
            payload,
        )
        log_trace(
            "memory",
            "save_proposed",
            row_count=len(payload),
            allow_empty_planning_persist=allow_empty_planning_persist,
        )

    def confirm_import_restore_runtime_loaded(self) -> None:
        """Call after runtime planned_moves/proposed_folders match imported disk rows so empty saves may proceed."""
        self._import_restore_runtime_confirmed = True
        self._import_restore_empty_guard = None
        log_info("import_restore_runtime_load_confirmed")

    def clear_import_restore_empty_guard(self) -> None:
        """Clear import guard without confirming (e.g. failed load path)."""
        self._import_restore_empty_guard = None
        self._import_restore_runtime_confirmed = False

    def log_planning_recovery_hint_if_primary_empty_after_import(self) -> None:
        """Read-only diagnostic: if primary queues are empty, log best quarantine ImportBefore snapshot counts."""
        try:
            pa = self._json_count(self.paths["allocations"])
            pp = self._json_count(self.paths["proposed"])
            if pa > 0 or pp > 0:
                return
            best: tuple[int, int, str] | None = None
            if not self.quarantine.is_dir():
                return
            for sub in sorted(self.quarantine.iterdir(), reverse=True):
                if not sub.is_dir() or not sub.name.startswith("ImportBefore_"):
                    continue
                ap = sub / "Draft-AllocationQueue.json"
                ppth = sub / "Draft-ProposedFolders.json"
                if not ap.is_file():
                    continue
                ac = self._json_count(ap)
                pc = self._json_count(ppth) if ppth.is_file() else 0
                if ac + pc == 0:
                    continue
                cand = (ac, pc, str(sub))
                if best is None or (ac + pc) > (best[0] + best[1]):
                    best = cand
            if best is not None:
                log_info(
                    "planning_memory_recovery_candidate_found_after_import",
                    active_primary_count=int(pa),
                    backup_folder=best[2],
                    backup_allocation_count=int(best[0]),
                    backup_proposed_count=int(best[1]),
                    memory_write_root=str(self.root),
                )
        except Exception:
            return

    def load_session_raw_and_allocations_proposed(self) -> tuple[dict[str, Any], list[AllocationRow], list[ProposedFolder]]:
        """Load session JSON plus allocation/proposed rows directly from primary paths (no restore-candidate selection)."""
        session_raw = self._read_json_path(self.paths["session"], {})
        if not isinstance(session_raw, dict):
            session_raw = {}
        allocations = self.load_allocations()
        proposed = self.load_proposed()
        return session_raw, allocations, proposed

    def load_session(self) -> SessionState:
        state = SessionState.from_dict(self._read_json(self.paths["session"], {}))
        log_trace(
            "memory",
            "load_session",
            draft_id_excerpt=str(getattr(state, "DraftId", "") or "")[:40],
        )
        return state

    def save_session(self, state: SessionState) -> None:
        payload = state.to_dict()

        self._write_json_safely(
            self.paths["session"],
            self.paths["session_recovery"],
            payload,
        )
        log_trace("memory", "save_session", draft_id_excerpt=str(getattr(state, "DraftId", "") or "")[:40])

    def write_workspace_snapshot(
        self,
        payload: dict[str, Any],
        *,
        allow_strip_allocation_graph: bool = False,
        save_reason: str = "",
    ) -> None:
        """Replace ``WorkspaceSnapshot.json`` entirely (no merge with any prior JSON on disk).

        Strips ``allocation_graph_identity`` only when allow_strip_allocation_graph is True; otherwise
        previous non-empty graph rows are merged forward to avoid silent loss during partial saves.
        """
        if not isinstance(payload, dict):
            raise TypeError("workspace snapshot payload must be a dict")
        data = dict(payload)
        data.setdefault("schema_version", WORKSPACE_SNAPSHOT_SCHEMA_VERSION)

        prev = self.read_workspace_snapshot_optional()
        prev_n = 0
        if isinstance(prev, dict):
            pag = prev.get("allocation_graph_identity")
            prev_n = len(pag) if isinstance(pag, list) else 0
        agi_new = data.get("allocation_graph_identity")
        new_n = len(agi_new) if isinstance(agi_new, list) else 0
        if prev_n > 0 and new_n == 0 and not allow_strip_allocation_graph:
            data["allocation_graph_identity"] = [dict(x) for x in (prev.get("allocation_graph_identity") or []) if isinstance(x, dict)]
            log_info(
                "workspace_snapshot_allocation_graph_preserved_from_existing",
                previous_graph_rows=int(prev_n),
                new_graph_rows_before_merge=int(new_n),
                save_reason=str(save_reason or "")[:240],
            )

        text = json.dumps(data, indent=2, ensure_ascii=False)
        json.loads(text)
        path = self.paths["workspace_snapshot"]
        path.parent.mkdir(parents=True, exist_ok=True)
        self._atomic_write_text(path, text)
        log_trace("memory", "write_workspace_snapshot", schema_version=data.get("schema_version"))

    def read_workspace_snapshot_optional(self) -> dict[str, Any] | None:
        path = self.paths["workspace_snapshot"]
        if not path.is_file():
            return None
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        return raw if isinstance(raw, dict) else None

    def save_manifest(self, manifest: MemoryManifest) -> None:
        self._backup_file(self.paths["manifest"], self.paths["manifest"].stem)
        self._atomic_write_text(self.paths["manifest"], json.dumps(manifest.to_dict(), indent=2))
        log_trace(
            "memory",
            "save_manifest",
            draft_id_excerpt=str(getattr(manifest, "DraftId", "") or "")[:40],
            save_status=str(getattr(manifest, "SaveStatus", "") or ""),
        )

    def refresh_manifest(self, draft_id: str = "", fingerprint: str = "", status: str = "Healthy") -> None:
        manifest = MemoryManifest(
            DraftId=draft_id,
            SessionFingerprint=fingerprint,
            AllocationQueueCount=self._json_count(self.paths["allocations"]),
            ProposedFolderCount=self._json_count(self.paths["proposed"]),
            LastGoodSaveUtc=datetime.utcnow().isoformat(),
            SaveStatus=status,
            AllocationQueuePath=str(self.paths["allocations"]),
            ProposedFoldersPath=str(self.paths["proposed"]),
            SessionStatePath=str(self.paths["session"]),
        )
        self.save_manifest(manifest)

    def save_draft_reset_backup(self, payload: dict[str, Any]) -> Path:
        """Persist a single verified JSON snapshot under ``Memory/Backups/WorkspaceReset`` (used before draft reset).

        Stored outside ``Backups`` root so automatic ``MemoryManifest_*.json`` rotation does not bury these files.

        Raises if serialization, write, or post-read verification fails so callers can abort reset.
        """
        if not isinstance(payload, dict):
            raise TypeError("draft reset backup payload must be a dict")
        payload = dict(payload)
        payload.setdefault("schema_version", 1)
        text = json.dumps(payload, indent=2, ensure_ascii=False)
        json.loads(text)
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:-3]
        dest_dir = self.workspace_reset_backups_dir
        dest_dir.mkdir(parents=True, exist_ok=True)
        backup_path = dest_dir / f"DraftReset_{stamp}.json"
        self._atomic_write_text(backup_path, text)
        if not backup_path.is_file() or backup_path.stat().st_size <= 0:
            raise OSError(f"Draft reset backup missing or empty: {backup_path}")
        roundtrip = json.loads(backup_path.read_text(encoding="utf-8"))
        if int(roundtrip.get("schema_version", 0) or 0) != int(payload.get("schema_version", 1) or 1):
            raise ValueError("Draft reset backup verification failed (schema_version mismatch)")
        log_trace(
            "memory",
            "draft_reset_backup_written",
            path=str(backup_path),
            byte_size=backup_path.stat().st_size,
        )
        return backup_path

    def apply_draft_reset_backup(
        self, source_path: Path
    ) -> tuple[SessionState, list[AllocationRow], list[ProposedFolder], dict[str, Any], list[dict[str, Any]] | None]:
        """Load a ``DraftReset_*.json`` written by :meth:`save_draft_reset_backup` and persist to active memory files.

        Returns runtime-ready objects plus optional ``planned_moves`` snapshots when the backup lists them
        (preferred over rows-only reconstruction for UI fidelity).
        """
        source_path = Path(source_path)
        if not source_path.is_file():
            raise FileNotFoundError(f"Draft reset backup not found: {source_path}")
        raw_text = source_path.read_text(encoding="utf-8")
        payload = json.loads(raw_text)
        if not isinstance(payload, dict):
            raise ValueError("Draft reset backup must be a JSON object")
        if str(payload.get("kind", "")) != "draft_reset_backup":
            raise ValueError(
                f"Not a workspace reset backup (expected kind 'draft_reset_backup', got {payload.get('kind')!r})"
            )
        if int(payload.get("schema_version", 0) or 0) != 1:
            raise ValueError(f"Unsupported draft reset backup schema_version: {payload.get('schema_version')!r}")

        session_payload = payload.get("session")
        if not isinstance(session_payload, dict):
            raise ValueError("Draft reset backup missing 'session' object")

        session_dict = dict(self._normalize_imported_session_payload(session_payload))
        if self.expected_fingerprint:
            session_dict["SessionFingerprint"] = self.expected_fingerprint
        if not str(session_dict.get("LastSavedUtc", "") or "").strip():
            session_dict["LastSavedUtc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        session_state = SessionState.from_dict(session_dict)

        alloc_raw = self._normalize_imported_allocations_payload(payload.get("allocations") or [])
        if not isinstance(alloc_raw, list):
            alloc_raw = []
        allocations = [AllocationRow.from_dict(x) for x in alloc_raw if isinstance(x, dict)]

        proposed_raw = self._normalize_imported_proposed_payload(payload.get("proposed_folders") or [])
        if not isinstance(proposed_raw, list):
            proposed_raw = []
        proposed = [ProposedFolder.from_dict(x) for x in proposed_raw if isinstance(x, dict)]

        self.save_session(session_state)
        self.save_allocations(allocations, allow_empty_planning_persist=True, save_reason="apply_draft_reset_backup")
        self.save_proposed(proposed, allow_empty_planning_persist=True, save_reason="apply_draft_reset_backup")
        fp = str(session_state.SessionFingerprint or self.expected_fingerprint or "")
        self.refresh_manifest(draft_id=str(session_state.DraftId or ""), fingerprint=fp, status="Healthy")

        pm = payload.get("planned_moves")
        planned_moves_out: list[dict[str, Any]] | None = None
        if isinstance(pm, list) and pm:
            planned_moves_out = [x for x in pm if isinstance(x, dict)]

        log_info(
            "Memory draft reset backup applied to active store.",
            source=str(source_path),
            draft_id=str(session_state.DraftId or ""),
            allocation_count=len(allocations),
            proposed_count=len(proposed),
            planned_moves_snapshot_count=len(planned_moves_out or []),
        )
        log_trace("memory", "apply_draft_reset_backup", source_excerpt=str(source_path)[-100:])
        return session_state, allocations, proposed, session_dict, planned_moves_out

    def export_bundle(self, reason: str = "Manual", destination: Path | None = None) -> Path:
        if destination is None:
            destination = self.exports / ("Export_" + datetime.now().strftime("%Y%m%d-%H%M%S"))
        destination.mkdir(parents=True, exist_ok=True)
        for path in self.paths.values():
            if path.exists():
                shutil.copy2(path, destination / path.name)
        meta = {
            "ExportedUtc": datetime.utcnow().isoformat(),
            "Reason": reason,
            "MachineName": __import__("platform").node(),
        }
        (destination / "ExportMetadata.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
        log_info("Memory bundle exported.", destination=str(destination), reason=reason)
        log_trace("memory", "export_bundle", reason=reason, destination_excerpt=str(destination)[-100:])
        return destination

    def export_bundle_zip(self, bundle_folder: Path, destination_zip: Path | None = None) -> Path:
        bundle_folder = Path(bundle_folder)
        if destination_zip is None:
            destination_zip = bundle_folder.with_suffix(".zip")
        destination_zip.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(destination_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(bundle_folder.iterdir()):
                if path.is_file():
                    archive.write(path, arcname=path.name)
        log_info("Memory bundle zip exported.", bundle=str(bundle_folder), destination=str(destination_zip))
        return destination_zip

    def import_bundle(self, source_folder: Path) -> None:
        source_folder = Path(source_folder)
        global_mem = memory_root()
        log_info(
            "import_memory_write_root_resolved",
            memory_write_root=str(self.root),
            active_tenant=self.tenant_domain,
            active_upn=self.operator_upn,
            expected_fingerprint=str(self.expected_fingerprint or "")[:120],
            destination_memory_folder=str(self.root),
            global_memory_root=str(global_mem),
            scope_is_user_scoped=bool(self.expected_fingerprint),
            import_targets_global_root_only=not bool(self.expected_fingerprint),
        )
        if not self.expected_fingerprint:
            log_info(
                "import_migrated_bundle_wrong_scope_detected",
                note="no_tenant_upn_fingerprint_Memory_writes_global_root_not_per_user",
                memory_write_root=str(self.root),
            )
        required = ["Draft-SessionState.json", "Draft-AllocationQueue.json", "Draft-ProposedFolders.json"]
        session_payload_preview: dict[str, Any] | None = None
        allocations_preview: list[Any] = []
        proposed_preview: list[Any] = []
        for name in required:
            if not (source_folder / name).exists():
                log_info("import_migrated_bundle_missing_expected_file", filename=name, source_folder=str(source_folder))
                raise FileNotFoundError(f"Import bundle missing required file: {name}")
            raw = json.loads((source_folder / name).read_text(encoding="utf-8"))
            if name == "Draft-SessionState.json" and isinstance(raw, dict):
                session_payload_preview = raw
            elif name == "Draft-AllocationQueue.json" and isinstance(raw, list):
                allocations_preview = raw
            elif name == "Draft-ProposedFolders.json" and isinstance(raw, list):
                proposed_preview = raw

        src_alloc_n = self._list_count(allocations_preview)
        src_prop_n = self._list_count(proposed_preview)
        log_info(
            "import_migrated_bundle_source_counts",
            import_source_folder=str(source_folder),
            import_source_allocation_count=int(src_alloc_n),
            import_source_proposed_count=int(src_prop_n),
        )

        if session_payload_preview is not None:
            from .legacy_backup_migration.shape import is_legacy_shaped_bundle
            from .legacy_backup_migration.errors import LegacyBackupDirectRestoreBlocked

            legacy, lb_reasons = is_legacy_shaped_bundle(
                session_payload_preview, allocations_preview, proposed_preview
            )
            migrated_marker = (Path(source_folder) / "LegacyMigrationReport.json").is_file()
            if migrated_marker:
                log_info("migrated_backup_restore_started", source=str(source_folder))
            allow_legacy_direct = os.environ.get("OZLINK_ALLOW_LEGACY_DIRECT_RESTORE", "").strip().lower() in (
                "1",
                "true",
                "yes",
            )
            if legacy and not migrated_marker and not allow_legacy_direct:
                log_info(
                    "legacy_backup_direct_restore_blocked_requires_migration",
                    reasons=lb_reasons,
                    source=str(source_folder),
                )
                raise LegacyBackupDirectRestoreBlocked(
                    "Legacy-shaped memory bundle requires migration before import (or set "
                    "OZLINK_ALLOW_LEGACY_DIRECT_RESTORE=1 for debug).",
                    reasons=lb_reasons,
                )

        stamp = "ImportBefore_" + datetime.now().strftime("%Y%m%d-%H%M%S")
        qdir = self.quarantine / stamp
        qdir.mkdir(parents=True, exist_ok=True)
        n_q = 0
        for path in self.paths.values():
            if path.exists():
                shutil.copy2(path, qdir / path.name)
                n_q += 1
        log_info("import_memory_quarantine_created", quarantine_dir=str(qdir))
        log_info("import_memory_quarantine_file_count", count=int(n_q), quarantine_dir=str(qdir))

        mapping = {
            "Draft-SessionState.json": self.paths["session"],
            "Draft-AllocationQueue.json": self.paths["allocations"],
            "Draft-ProposedFolders.json": self.paths["proposed"],
            "MemoryManifest.json": self.paths["manifest"],
        }
        imported_names: list[str] = []
        for name, target in mapping.items():
            src = source_folder / name
            if src.exists():
                payload = json.loads(src.read_text(encoding="utf-8"))
                if name == "Draft-SessionState.json":
                    payload = self._normalize_imported_session_payload(payload)
                elif name == "Draft-AllocationQueue.json":
                    payload = self._normalize_imported_allocations_payload(payload)
                elif name == "Draft-ProposedFolders.json":
                    payload = self._normalize_imported_proposed_payload(payload)
                self._atomic_write_text(target, json.dumps(payload, indent=2))
                imported_names.append(name)

        for extra in ("LegacyMigrationReport.json", "LegacyMigrationReport.md"):
            src = source_folder / extra
            if src.is_file():
                try:
                    shutil.copy2(src, self.root / extra)
                    imported_names.append(extra)
                    log_info("import_bundle_sidecar_copied", filename=extra, dest=str(self.root / extra))
                except OSError as exc:
                    log_warn("import_bundle_sidecar_copy_failed", filename=extra, error=str(exc))

        ws_src = source_folder / "WorkspaceSnapshot.json"
        if ws_src.is_file():
            try:
                ws_payload = json.loads(ws_src.read_text(encoding="utf-8"))
                if isinstance(ws_payload, dict):
                    self._atomic_write_text(
                        self.paths["workspace_snapshot"],
                        json.dumps(ws_payload, indent=2, ensure_ascii=False),
                    )
                    imported_names.append("WorkspaceSnapshot.json")
                    log_info("WorkspaceSnapshot.json imported with bundle.", source=str(ws_src))
            except Exception as exc:
                log_warn("WorkspaceSnapshot.json import skipped.", error=str(exc))

        session_payload = self._read_json_path(self.paths["session"], {})
        if isinstance(session_payload, dict):
            if self.expected_fingerprint:
                session_payload["SessionFingerprint"] = self.expected_fingerprint
            if not session_payload.get("LastSavedUtc"):
                session_payload["LastSavedUtc"] = datetime.utcnow().isoformat()
            self.paths["session"].write_text(json.dumps(session_payload, indent=2), encoding="utf-8")

        shutil.copy2(self.paths["session"], self.paths["session_recovery"])
        shutil.copy2(self.paths["allocations"], self.paths["allocations_recovery"])
        shutil.copy2(self.paths["proposed"], self.paths["proposed_recovery"])
        self.refresh_manifest(
            draft_id=str(session_payload.get("DraftId", "")) if isinstance(session_payload, dict) else "",
            fingerprint=self.expected_fingerprint,
            status="Healthy",
        )

        post_a = self._json_count(self.paths["allocations"])
        post_p = self._json_count(self.paths["proposed"])
        post_session = self.paths["session"].is_file()
        post_rep = (self.root / "LegacyMigrationReport.json").is_file()
        log_info(
            "import_migrated_bundle_post_copy_counts",
            post_import_allocation_count=int(post_a),
            post_import_proposed_count=int(post_p),
            post_import_session_exists=bool(post_session),
            post_import_report_exists=bool(post_rep),
            imported_files_list=imported_names[:80],
        )
        log_info(
            "migrated_package_restore_confirmed",
            import_source_folder=str(source_folder),
            post_import_allocation_count=int(post_a),
            post_import_proposed_count=int(post_p),
            memory_write_root=str(self.root),
        )
        if src_alloc_n != post_a or src_prop_n != post_p:
            log_warn(
                "import_migrated_bundle_post_copy_mismatch",
                source_alloc=int(src_alloc_n),
                source_prop=int(src_prop_n),
                post_alloc=int(post_a),
                post_prop=int(post_p),
            )

        self.clear_import_restore_empty_guard()
        self._import_restore_runtime_confirmed = False
        if post_a > 0 or post_p > 0:
            self._import_restore_empty_guard = (int(post_a), int(post_p))
            log_info(
                "import_restore_expected_counts",
                expected_allocations=int(post_a),
                expected_proposed=int(post_p),
            )

        log_info("Memory bundle imported.", source=str(source_folder))
        log_trace("memory", "import_bundle", source_excerpt=str(source_folder)[-100:])

    def import_bundle_zip(self, source_zip: Path) -> None:
        source_zip = Path(source_zip)
        if not source_zip.exists():
            raise FileNotFoundError(f"Import bundle zip not found: {source_zip}")

        with tempfile.TemporaryDirectory(prefix="ozlink_draft_import_") as temp_dir:
            extract_root = Path(temp_dir) / "bundle"
            extract_root.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(source_zip, "r") as archive:
                archive.extractall(extract_root)
            self.import_bundle(extract_root)
        log_info("Memory bundle zip imported.", source=str(source_zip))
        log_trace("memory", "import_bundle_zip", source_excerpt=str(source_zip)[-100:])

    def submit_request_package(self, user_display: str, user_email: str, tenant_id: str, tenant_label: str,
                               source_context: dict[str, Any], destination_context: dict[str, Any]) -> Path:
        request_id = "REQ-" + datetime.now().strftime("%Y%m%d-%H%M%S")
        payload = {
            "RequestId": request_id,
            "Status": "Submitted",
            "SubmittedBy": {
                "DisplayName": user_display,
                "WorkEmail": user_email,
                "UserPrincipalName": user_email,
            },
            "Tenant": {
                "TenantId": tenant_id,
                "TenantLabel": tenant_label,
            },
            "SourceContext": source_context,
            "DestinationContext": destination_context,
            "PlannedMoves": [x.to_dict() for x in self.load_allocations()],
            "ProposedFolders": [x.to_dict() for x in self.load_proposed()],
            "NeedsReview": [],
            "CreatedOn": datetime.now().isoformat(timespec="seconds"),
            "LastUpdatedOn": datetime.now().isoformat(timespec="seconds"),
            "Version": "Python-PySide6-v1",
            "AppVersion": APP_VERSION,
        }
        from .paths import requests_root
        out = requests_root() / f"{request_id}.json"
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        log_info("Request package saved.", path=str(out), request_id=request_id)
        return out
