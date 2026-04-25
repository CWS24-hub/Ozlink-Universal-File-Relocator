"""Byte-level verification for local copies (SHA-256). Used for governance / non-repudiation of successful transfers."""

from __future__ import annotations

import hashlib
from pathlib import Path


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Return lowercase hex SHA-256 of file contents."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def verify_copied_file(source: Path, destination: Path) -> tuple[bool, str, str, str]:
    """
    Compare SHA-256 of two files. Returns (ok, message, source_hash, dest_hash).
    """
    if not source.is_file():
        return False, "source is not a file", "", ""
    if not destination.is_file():
        return False, "destination is not a file", "", ""
    hs = sha256_file(source)
    hd = sha256_file(destination)
    if hs == hd:
        return True, "sha256_match", hs, hd
    return False, "sha256_mismatch", hs, hd


def _iter_rel_files(root: Path) -> dict[str, str]:
    """Map relative path (posix) -> sha256 for all files under root."""
    out: dict[str, str] = {}
    for p in sorted(root.rglob("*")):
        if p.is_file():
            rel = p.relative_to(root).as_posix()
            out[rel] = sha256_file(p)
    return out


def verify_copied_tree(source_root: Path, destination_root: Path) -> tuple[bool, str]:
    """
    After copytree, verify every file under source_root exists under destination_root
    with the same relative path and SHA-256.
    """
    if not source_root.is_dir() or not destination_root.is_dir():
        return False, "source or destination root is not a directory"
    src_map = _iter_rel_files(source_root)
    dst_map = _iter_rel_files(destination_root)
    if src_map.keys() != dst_map.keys():
        missing = src_map.keys() - dst_map.keys()
        extra = dst_map.keys() - src_map.keys()
        return False, f"file_set_mismatch missing_in_dst={len(missing)} extra_in_dst={len(extra)}"
    for rel, h_src in src_map.items():
        if dst_map.get(rel) != h_src:
            return False, f"sha256_mismatch rel={rel}"
    return True, "tree_sha256_match"
