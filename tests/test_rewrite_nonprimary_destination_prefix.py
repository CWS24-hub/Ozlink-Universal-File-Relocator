"""Planning: retarget rewrite must not stamp one sibling's leaf onto all file rows."""

from __future__ import annotations

from ozlink_console.main_window import MainWindow


def test_exact_projection_match_preserves_each_file_leaf_when_primary_is_file():
    """When several file rows wrongly share the moved file's old projection, each keeps its target_name."""
    mw = MainWindow.__new__(MainWindow)
    mw.normalize_memory_path = lambda p: str(p or "").replace("/", "\\").strip()
    mw._canonical_destination_projection_path = lambda p: mw.normalize_memory_path(p)
    mw._is_move_submitted = lambda _m: False

    primary = {
        "request_id": "p1",
        "source": {"is_folder": False, "name": "ACS - Alcohol and Drug Policy.docx"},
        "source_path": r"S\A\ACS - Alcohol and Drug Policy.docx",
        "source_name": "ACS - Alcohol and Drug Policy.docx",
        "destination_path": r"D\New\ACS - Alcohol and Drug Policy.docx",
        "target_name": "ACS - Alcohol and Drug Policy.docx",
        "destination_name": "ACS - Alcohol and Drug Policy.docx",
    }
    decl = {
        "request_id": "d1",
        "source": {"is_folder": False, "name": "ACS - Declaration.docx"},
        "source_path": r"S\A\ACS - Declaration.docx",
        "source_name": "ACS - Declaration.docx",
        "destination_path": r"D\Old\X",
        "target_name": "ACS - Declaration.docx",
        "destination_name": "ACS - Declaration.docx",
    }
    hazard = {
        "request_id": "h1",
        "source": {"is_folder": False, "name": "ACS - Hazard Control Plan.docx"},
        "source_path": r"S\A\ACS - Hazard Control Plan.docx",
        "source_name": "ACS - Hazard Control Plan.docx",
        "destination_path": r"D\Old\Y",
        "target_name": "ACS - Hazard Control Plan.docx",
        "destination_name": "ACS - Hazard Control Plan.docx",
    }
    mw.planned_moves = [primary, decl, hazard]

    norm_old = r"D\Old\ACS - Alcohol and Drug Policy.docx"
    norm_new = r"D\New\ACS - Alcohol and Drug Policy.docx"

    real_alloc = MainWindow._allocation_projection_path.__get__(mw, MainWindow)

    def patched_alloc(m):
        if m is decl or m is hazard:
            return norm_old
        return real_alloc(m)

    mw._allocation_projection_path = patched_alloc

    MainWindow._rewrite_nonprimary_planned_moves_destination_prefix(
        mw, norm_old, norm_new, primary_move=primary
    )

    assert decl["target_name"] == "ACS - Declaration.docx"
    assert hazard["target_name"] == "ACS - Hazard Control Plan.docx"
    assert decl["destination_path"] == r"D\New"
    assert hazard["destination_path"] == r"D\New"


def test_exact_match_folder_primary_appends_child_leaf_under_new_root():
    mw = MainWindow.__new__(MainWindow)
    mw.normalize_memory_path = lambda p: str(p or "").replace("/", "\\").strip()
    mw._canonical_destination_projection_path = lambda p: mw.normalize_memory_path(p)
    mw._is_move_submitted = lambda _m: False

    folder_primary = {
        "request_id": "fold",
        "source": {"is_folder": True, "name": "ACS"},
        "source_path": r"S\ACS",
        "source_name": "ACS",
        "destination_path": r"D\NewRoot\ACS",
        "target_name": "ACS",
        "destination_name": "ACS",
    }
    child_file = {
        "request_id": "c1",
        "source": {"is_folder": False, "name": "ACS - Declaration.docx"},
        "source_path": r"S\ACS\ACS - Declaration.docx",
        "source_name": "ACS - Declaration.docx",
        "destination_path": r"D\OldRoot\ACS",
        "target_name": "ACS - Declaration.docx",
        "destination_name": "ACS - Declaration.docx",
    }
    mw.planned_moves = [folder_primary, child_file]

    norm_old = r"D\OldRoot\ACS"
    norm_new = r"D\NewRoot\ACS"

    real_alloc = MainWindow._allocation_projection_path.__get__(mw, MainWindow)

    def patched_alloc(m):
        if m is child_file:
            return norm_old
        return real_alloc(m)

    mw._allocation_projection_path = patched_alloc

    MainWindow._rewrite_nonprimary_planned_moves_destination_prefix(
        mw, norm_old, norm_new, primary_move=folder_primary
    )

    assert child_file["target_name"] == "ACS - Declaration.docx"
    assert child_file["destination_path"] == r"D\NewRoot\ACS"
    assert (
        MainWindow._allocation_projection_path.__get__(mw, MainWindow)(child_file)
        == r"D\NewRoot\ACS\ACS - Declaration.docx"
    )
