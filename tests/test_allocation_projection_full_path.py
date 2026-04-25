"""Planning: file moves with full destination_path must not double-append the leaf."""

from __future__ import annotations

from ozlink_console.main_window import MainWindow


def _stub_window():
    mw = MainWindow.__new__(MainWindow)
    mw.normalize_memory_path = lambda p: str(p or "").replace("/", "\\").strip()
    mw._canonical_destination_projection_path = lambda p: mw.normalize_memory_path(p)
    return mw


def test_file_move_full_destination_path_projection_is_not_doubled():
    mw = _stub_window()
    alloc = MainWindow._allocation_projection_path.__get__(mw, MainWindow)
    parent_path_fn = MainWindow._allocation_parent_path.__get__(mw, MainWindow)
    m = {
        "source_path": r"S\HR\ACS - Declaration.docx",
        "source_name": "ACS - Declaration.docx",
        "destination_path": r"Root\HR\Templates\ACS - Declaration.docx",
        "source": {"is_folder": False, "name": "ACS - Declaration.docx"},
    }
    assert alloc(m) == r"Root\HR\Templates\ACS - Declaration.docx"
    assert parent_path_fn(m) == r"Root\HR\Templates"


def test_distinct_file_moves_keep_distinct_projections():
    mw = _stub_window()
    alloc = MainWindow._allocation_projection_path.__get__(mw, MainWindow)
    a = {
        "source_path": r"S\ACS - Alcohol and Drug Policy.docx",
        "source_name": "ACS - Alcohol and Drug Policy.docx",
        "destination_path": r"Root\HR\Templates\ACS - Alcohol and Drug Policy.docx",
        "source": {"is_folder": False, "name": "ACS - Alcohol and Drug Policy.docx"},
    }
    b = {
        "source_path": r"S\ACS - Declaration.docx",
        "source_name": "ACS - Declaration.docx",
        "destination_path": r"Root\HR\Templates\ACS - Declaration.docx",
        "source": {"is_folder": False, "name": "ACS - Declaration.docx"},
    }
    assert alloc(a) != alloc(b)


def test_legacy_parent_plus_leaf_shape_unchanged():
    mw = _stub_window()
    alloc = MainWindow._allocation_projection_path.__get__(mw, MainWindow)
    m = {
        "source_path": r"S\file.docx",
        "source_name": "file.docx",
        "destination_path": r"Root\HR\Templates",
        "source": {"is_folder": False, "name": "file.docx"},
    }
    assert alloc(m) == r"Root\HR\Templates\file.docx"
