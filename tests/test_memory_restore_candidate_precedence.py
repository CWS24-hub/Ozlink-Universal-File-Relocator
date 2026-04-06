"""Restore candidate selection: live Memory files must beat rotated Backups/ copies."""

from __future__ import annotations

from datetime import datetime, timezone

from ozlink_console.memory import MemoryManager


def _base_candidate(
    name: str,
    *,
    valid: bool,
    populated: bool,
    allocation_count: int,
    proposed_count: int = 0,
    ts: float = 1.0,
) -> dict:
    return {
        "name": name,
        "valid": valid,
        "populated": populated,
        "allocation_count": allocation_count,
        "proposed_count": proposed_count,
        "draft_id": "",
        "fingerprint": "",
        "timestamp_sort_value": ts,
        "session_path": None,
        "allocations_path": None,
        "proposed_path": None,
        "session_state": None,
        "session_raw": {},
        "allocations_raw": [],
        "proposed_raw": [],
        "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc),
        "timestamp_kind": "test",
    }


def test_select_restore_candidate_prefers_valid_empty_live_primary_over_populated_backup():
    mm = MemoryManager()
    backup_ts = datetime(2025, 6, 1, tzinfo=timezone.utc).timestamp()
    live_ts = datetime(2025, 5, 1, tzinfo=timezone.utc).timestamp()
    candidates = [
        _base_candidate(
            "python_live_primary",
            valid=True,
            populated=False,
            allocation_count=0,
            ts=live_ts,
        ),
        _base_candidate(
            "python_backup_latest",
            valid=True,
            populated=True,
            allocation_count=76,
            ts=backup_ts,
        ),
    ]
    selected, reason = mm.select_restore_candidate(candidates)
    assert selected is not None
    assert selected["name"] == "python_live_primary"
    assert "authoritative_python_live_primary" in reason


def test_select_restore_candidate_prefers_live_primary_with_fewer_rows_than_backup():
    mm = MemoryManager()
    candidates = [
        _base_candidate("python_live_primary", valid=True, populated=True, allocation_count=14, ts=100.0),
        _base_candidate("python_backup_latest", valid=True, populated=True, allocation_count=76, ts=200.0),
    ]
    selected, _reason = mm.select_restore_candidate(candidates)
    assert selected is not None
    assert selected["name"] == "python_live_primary"
    assert selected["allocation_count"] == 14


def test_select_restore_candidate_uses_recovery_when_primary_invalid():
    mm = MemoryManager()
    candidates = [
        _base_candidate("python_live_primary", valid=False, populated=True, allocation_count=99, ts=300.0),
        _base_candidate("python_live_recovery", valid=True, populated=True, allocation_count=3, ts=100.0),
        _base_candidate("python_backup_latest", valid=True, populated=True, allocation_count=76, ts=400.0),
    ]
    selected, reason = mm.select_restore_candidate(candidates)
    assert selected is not None
    assert selected["name"] == "python_live_recovery"
    assert "authoritative_python_live_recovery" in reason


def test_select_restore_candidate_fallback_to_backup_when_live_invalid():
    mm = MemoryManager()
    candidates = [
        _base_candidate("python_live_primary", valid=False, populated=False, allocation_count=0, ts=1.0),
        _base_candidate("python_live_recovery", valid=False, populated=False, allocation_count=0, ts=2.0),
        _base_candidate("python_backup_latest", valid=True, populated=True, allocation_count=12, ts=3.0),
    ]
    selected, reason = mm.select_restore_candidate(candidates)
    assert selected is not None
    assert selected["name"] == "python_backup_latest"
    assert "fallback_selected" in reason
