"""Library-root-relative path normalization for canonical snapshots."""

from __future__ import annotations


def to_library_relative_path(path: str, *library_names: str) -> str:
    """
    Strip a leading document-library segment so paths are relative to the library root.

    Accepts mixed ``/`` and ``\\``; output uses forward slashes. Repeatedly removes a leading
    segment that matches any non-empty ``library_names`` (case-insensitive), e.g.
    ``Archive Lib/Projects`` → ``Projects`` when ``library_names`` contains ``Archive Lib``.
    """
    p = str(path or "").strip().replace("\\", "/")
    while "//" in p:
        p = p.replace("//", "/")
    p = p.lstrip("/")
    names = [str(n).strip() for n in library_names if str(n).strip()]
    changed = True
    while changed and names:
        changed = False
        lower = p.lower()
        for name in names:
            nl = name.lower()
            if lower == nl:
                p = ""
                changed = True
                break
            prefix = nl + "/"
            if lower.startswith(prefix):
                p = p[len(name) + 1 :]
                changed = True
                break
    return p
