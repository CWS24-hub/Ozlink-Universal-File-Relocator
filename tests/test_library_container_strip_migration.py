"""Library display name incorrectly stored as first path segment (legacy exports)."""

from __future__ import annotations

import json
from pathlib import Path

from ozlink_console.legacy_backup_migration import migrate_legacy_backup_folder
from ozlink_console.legacy_backup_migration.library_container_strip import (
    OUTCOME_AMBIGUOUS,
    OUTCOME_IDENTITY_MISSING,
    OUTCOME_NOOP,
    OUTCOME_STRIPPED,
    strip_legacy_library_container_segment,
)
from tests.test_live_skeleton_reanchor_migration import GraphStub, _identity, _write_min_bundle


def test_unit_strip_destination_documents_root_hr():
    """A. Documents\\Root\\HR -> Root\\HR."""
    out, o = strip_legacy_library_container_segment(
        r"Documents\Root\HR",
        "Documents",
        row_index=0,
        row_kind="test",
        role="destination",
    )
    assert o == OUTCOME_STRIPPED
    assert out.replace("/", "\\") == r"Root\HR"


def test_unit_strip_source_library_prefix():
    """B. Source: Files to be Migrated\\Folder\\File.pdf -> Folder\\File.pdf."""
    out, o = strip_legacy_library_container_segment(
        r"Files to be Migrated\Folder\File.pdf",
        "Files to be Migrated",
        row_index=0,
        row_kind="test",
        role="source",
    )
    assert o == OUTCOME_STRIPPED
    assert out.replace("/", "\\") == r"Folder\File.pdf"


def test_unit_double_documents_preserves_inner_documents():
    """C. Documents\\Documents\\Policies -> Documents\\Policies."""
    out, o = strip_legacy_library_container_segment(
        r"Documents\Documents\Policies",
        "Documents",
        row_index=0,
        row_kind="test",
        role="destination",
    )
    assert o == OUTCOME_STRIPPED
    assert out.replace("/", "\\") == r"Documents\Policies"


def test_unit_missing_library_identity_no_strip():
    """D. Missing selected library name -> no strip, flagged outcome."""
    out, o = strip_legacy_library_container_segment(
        r"Documents\Root\X",
        "",
        row_index=0,
        row_kind="test",
        role="destination",
    )
    assert o == OUTCOME_IDENTITY_MISSING
    assert "Root" in out


def test_unit_root_finance_unchanged_without_documents_prefix():
    out, o = strip_legacy_library_container_segment(
        r"Root\Finance",
        "Documents",
        row_index=0,
        row_kind="test",
        role="destination",
    )
    assert o == OUTCOME_NOOP
    assert out == r"Root\Finance"


def test_unit_multisegment_library_name_ambiguous():
    out, o = strip_legacy_library_container_segment(
        r"Seg\More",
        "Bad\\Lib",
        row_index=0,
        row_kind="test",
        role="destination",
    )
    assert o == OUTCOME_AMBIGUOUS


def test_e_after_strip_reanchor_root3_integration(tmp_path):
    """E. Strip Documents then live re-anchor Root -> Root3."""
    src = tmp_path / "in"
    _write_min_bundle(src, r"Documents\Root\HR")
    session = json.loads((src / "Draft-SessionState.json").read_text(encoding="utf-8"))
    session["SelectedDestinationLibrary"] = "Documents"
    session["SelectedSourceLibrary"] = "Files to be Migrated"
    (src / "Draft-SessionState.json").write_text(json.dumps(session), encoding="utf-8")

    g = GraphStub(
        root_names=["Root3"],
        resolve_paths={"Root3/HR"},
    )
    res = migrate_legacy_backup_folder(
        src,
        tmp_path,
        identity=_identity("Root3"),
        graph=g,
        skip_graph_resolution=True,
    )
    assert res.ok
    out = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    assert out[0]["RequestedDestinationPath"].replace("/", "\\") == r"Root3\HR"
