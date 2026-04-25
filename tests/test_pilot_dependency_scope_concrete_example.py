"""
Concrete example for pilot dependency-complete merge (mirrors main_window dialog logic).

Shows derived values matching a log line from pilot_dependency_scope when:
- operator leaves Proposed combobox at shallow "Personal"
- allocated / file intent is under Personal\\100GOPRO
"""

from __future__ import annotations


def _norm_identity_dir(path: str) -> str:
    return str(path or "").replace("/", "\\").strip().rstrip("\\")


def _path_is_within(child: str, root: str) -> bool:
    """Subset of MainWindow pilot _path_is_within sufficient for this scenario."""
    if not child or not root:
        return False
    c = _norm_identity_dir(child).lower()
    r = _norm_identity_dir(root).lower()
    if c == r:
        return True
    if c.startswith(r + "\\"):
        return True
    child_leaf = c.split("\\")[-1] if "\\" in c else c
    root_leaf = r.split("\\")[-1] if "\\" in r else r
    return bool(child_leaf and root_leaf and child_leaf == root_leaf)


def _infer_proposed_destination_scope_for_identity(identity: str, proposed_steps: list[dict]) -> str:
    ident = _norm_identity_dir(identity)
    if not ident:
        return ""
    candidates: list[str] = []
    for s in proposed_steps:
        dpath = str((s or {}).get("destination_path", "") or "").strip()
        if not dpath:
            continue
        pident = _norm_identity_dir(dpath)
        if not pident:
            continue
        if _path_is_within(ident, pident):
            candidates.append(dpath)
    if not candidates:
        return ""
    candidates.sort(
        key=lambda p: len([seg for seg in _norm_identity_dir(p).split("\\") if seg]),
        reverse=True,
    )
    return str(candidates[0] or "")


def _proposed_ancestor_chain_paths(seed_path: str, proposed_steps: list[dict]) -> list[str]:
    seed = _norm_identity_dir(seed_path)
    if not seed:
        return []
    chain: list[tuple[int, str]] = []
    seen: set[str] = set()
    for s in proposed_steps:
        dpath = str((s or {}).get("destination_path", "") or "").strip()
        if not dpath:
            continue
        ident = _norm_identity_dir(dpath)
        if not ident:
            continue
        if not _path_is_within(seed, ident):
            continue
        if ident.lower() in seen:
            continue
        seen.add(ident.lower())
        depth = len([seg for seg in ident.split("\\") if seg])
        chain.append((depth, dpath))
    chain.sort(key=lambda x: x[0])
    return [p for _d, p in chain]


def _merge_shallow_and_expanded(shallow: list[str], expanded: list[str]) -> list[str]:
    merge_seen: set[str] = set()
    merged: list[str] = []
    for src_list in (shallow, expanded):
        for p in src_list:
            ps = str(p or "").strip()
            if not ps:
                continue
            lk = ps.lower()
            if lk in merge_seen:
                continue
            merge_seen.add(lk)
            merged.append(ps)
    merged.sort(key=lambda path: len([x for x in _norm_identity_dir(path).split("\\") if x]))
    return merged


def test_concrete_personal_100gopro_derived_values_match_pilot_log_contract():
    """Log-shaped example: shallow Personal + deepest Personal\\100GOPRO -> merged mkdir whitelist."""
    proposed_steps = [
        {"destination_path": r"Personal", "folder_name": "Personal"},
        {"destination_path": r"Personal\100GOPRO", "folder_name": "100GOPRO"},
    ]
    selected_proposed_combo_initial = r"Personal"
    proposed_dest_path_effective = r"Personal"
    proposed_dest_paths_shallow = _proposed_ancestor_chain_paths(proposed_dest_path_effective, proposed_steps)
    assert proposed_dest_paths_shallow == [r"Personal"]

    allocated_effective_folder = r"Personal\100GOPRO"
    deepest_scope = _infer_proposed_destination_scope_for_identity(allocated_effective_folder, proposed_steps)
    assert deepest_scope == r"Personal\100GOPRO"

    expanded_chain = _proposed_ancestor_chain_paths(deepest_scope, proposed_steps)
    assert expanded_chain == [r"Personal", r"Personal\100GOPRO"]

    derived_proposed_folder_paths = _merge_shallow_and_expanded(proposed_dest_paths_shallow, expanded_chain)
    assert derived_proposed_folder_paths == [r"Personal", r"Personal\100GOPRO"]

    log_payload = {
        "selected_proposed_combo_initial": selected_proposed_combo_initial or None,
        "proposed_dest_path_effective": proposed_dest_path_effective or None,
        "inferred_deepest_scope": deepest_scope or None,
        "derived_proposed_folder_paths": list(derived_proposed_folder_paths),
        "derived_transfer_keys": ["Z:\\dst\\Personal\\100GOPRO|||GOPR0307.JPG"],
    }
    assert log_payload["selected_proposed_combo_initial"] == "Personal"
    assert log_payload["inferred_deepest_scope"] == r"Personal\100GOPRO"
    assert log_payload["derived_proposed_folder_paths"] == [r"Personal", r"Personal\100GOPRO"]
