"""Branch-level Graph /children union under a destination folder (no replace-all, no reset_nested)."""

from __future__ import annotations

from PySide6.QtCore import QModelIndex, Qt

from ozlink_console.paths import normalize_manifest_path
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _key(pl: dict) -> str:
    k = pl.get("semantic_path") or pl.get("item_path")
    if k:
        return str(normalize_manifest_path(str(k)))
    return str(pl.get("name") or "")


def _folder_payload(name: str, path: str, **extra) -> dict:
    return {
        "name": name,
        "is_folder": True,
        "tree_label": "Folder",
        "tree_role": "destination",
        "semantic_path": path,
        "item_path": path,
        "destination_path": path,
        "id": f"id-{name}",
        "drive_id": "driveX",
        **extra,
    }


def test_root3_union_inserts_graph_only_and_merges_common() -> None:
    m = DestinationPlanningTreeModel(destination_index_key_fn=_key)
    root3 = _folder_payload("Root3", "Root3", id="r3", workspace_row_state="cached_provisional")
    sales = _folder_payload("Sales", "Root3\\Sales", workspace_row_state="planned_only", row_kind="planned_folder")
    mgmt = _folder_payload(
        "Management",
        "Root3\\Management",
        workspace_row_state="planned_only",
        row_kind="planned_folder",
    )
    pic = _folder_payload("Pictures", "Root3\\Sales\\Pictures", workspace_row_state="planned_only", is_folder=True)
    em = _folder_payload(
        "Email attachments", "Root3\\Management\\Email attachments", workspace_row_state="planned_only", is_folder=False
    )
    m.append_child_payloads(QModelIndex(), [root3])
    r3_ix = m.index(0, 0, QModelIndex())
    m.append_child_payloads(r3_ix, [sales, mgmt])
    s_ix = m.index(0, 0, r3_ix)  # Sales, Management order by sort in append? order [sales, mgmt] -> row0 Sales
    m_ix = m.index(1, 0, r3_ix)
    # re-find by path
    hits = m.find_indices_for_canonical_destination_path("Root3\\Sales")
    s_ix = hits[0] if hits else s_ix
    m.append_child_payloads(s_ix, [pic])
    h2 = m.find_indices_for_canonical_destination_path("Root3\\Management")
    m_ix = h2[0] if h2 else m_ix
    m.append_child_payloads(m_ix, [em])

    graph = [
        {"name": "IT", "id": "g-it", "is_folder": True, "item_path": "Root3/IT", "drive_id": "driveX"},
        {"name": "Marketing", "id": "g-mk", "is_folder": True, "item_path": "Root3/Marketing", "drive_id": "driveX"},
        {"name": "Sales", "id": "g-sales2", "is_folder": True, "item_path": "Root3/Sales", "drive_id": "driveX"},
        {"name": "Management", "id": "g-mgt2", "is_folder": True, "item_path": "Root3/Management", "drive_id": "driveX"},
    ]
    cps: list[dict] = []
    for c in graph:
        pl = {
            "name": c["name"],
            "id": c["id"],
            "is_folder": c.get("is_folder", True),
            "item_path": c.get("item_path", ""),
            "tree_role": "destination",
            "drive_id": c.get("drive_id", "driveX"),
        }
        pl["semantic_path"] = normalize_manifest_path(
            "Root3\\" + c["name"] if not str(c.get("item_path", "")).startswith("Root3") else str(c.get("item_path", ""))
        )
        cps.append(
            {
                "name": pl["name"],
                "id": pl["id"],
                "is_folder": pl["is_folder"],
                "item_path": pl["semantic_path"],
                "semantic_path": pl["semantic_path"],
                "destination_path": pl["semantic_path"],
                "tree_role": "destination",
                "tree_label": "Folder",
                "base_display_label": pl["name"],
                "drive_id": "driveX",
            }
        )

    st = m.merge_graph_branch_union_at_parent(
        m.index(0, 0, QModelIndex()).siblingAtColumn(0), cps, parent_canonical_path="Root3"
    )
    assert st["upgraded"] >= 2
    assert st["inserted"] == 2

    keys = [m._path_key_for_payload(m.index(i, 0, r3_ix).data(Qt.UserRole) or {}) for i in range(m.rowCount(r3_ix))]  # type: ignore[union-attr, unused-ignore]
    lp = "root3"
    cfp = {str(k or "").casefold() for k in keys}
    need = {normalize_manifest_path("Root3\\" + s).casefold() for s in ("IT", "Marketing", "Sales", "Management")}
    assert need.issubset(cfp)

    sp = m.find_indices_for_canonical_destination_path("Root3\\Sales\\Pictures")
    assert sp and (sp[0].data(Qt.UserRole) or {}).get("name") == "Pictures"
    m_h = m.find_indices_for_canonical_destination_path("Root3\\Management\\Email attachments")
    assert m_h


def test_planned_orphan_preserved_if_not_in_graph() -> None:
    m = DestinationPlanningTreeModel(destination_index_key_fn=_key)
    f = _folder_payload("X", "Root3\\X", id="x1", row_kind="planned_folder", workspace_row_state="planned_only", verification_state="planned_only")
    p = _folder_payload("Y", "Root3\\X\\Y", row_kind="planned_file", workspace_row_state="planned_only", is_folder=False, verification_state="planned_only")
    r3 = _folder_payload("Root3", "Root3", id="r3a")
    m.append_child_payloads(QModelIndex(), [r3])
    r3i = m.index(0, 0, QModelIndex())
    m.append_child_payloads(r3i, [f])
    xi = m.find_indices_for_canonical_destination_path("Root3\\X")[0]
    m.append_child_payloads(xi, [p])

    m.merge_graph_branch_union_at_parent(
        r3i, [{"name": "IT", "id": "g2", "is_folder": True, "drive_id": "d"}], parent_canonical_path="Root3"
    )
    assert m.find_indices_for_canonical_destination_path("Root3\\X")
    assert m.find_indices_for_canonical_destination_path("Root3\\X\\Y")


def test_client_root_name_folder_union_no_literal_root3_assumption() -> None:
    """Folder-root: hub folder name is tenant-specific; union uses path/payload, not a product name."""
    m = DestinationPlanningTreeModel(destination_index_key_fn=_key)
    hub = _folder_payload("ClientRoot", "ClientRoot", id="h1", workspace_row_state="cached_provisional")
    sales = _folder_payload("Sales", "ClientRoot\\Sales", workspace_row_state="planned_only", row_kind="planned_folder")
    m.append_child_payloads(QModelIndex(), [hub])
    h_ix = m.index(0, 0, QModelIndex())
    m.append_child_payloads(h_ix, [sales])
    graph = [
        {"name": "IT", "id": "g-it", "is_folder": True, "item_path": "ClientRoot/IT", "drive_id": "driveX"},
        {"name": "Sales", "id": "g-s", "is_folder": True, "item_path": "ClientRoot/Sales", "drive_id": "driveX"},
    ]
    cps: list[dict] = []
    for c in graph:
        cps.append(
            {
                "name": c["name"],
                "id": c["id"],
                "is_folder": True,
                "item_path": f"ClientRoot\\{c['name']}",
                "semantic_path": f"ClientRoot\\{c['name']}",
                "destination_path": f"ClientRoot\\{c['name']}",
                "tree_role": "destination",
                "drive_id": "driveX",
                "base_display_label": c["name"],
            }
        )
    st = m.merge_graph_branch_union_at_parent(h_ix, cps, parent_canonical_path="ClientRoot")
    assert st.get("inserted", 0) == 1
    assert m.find_indices_for_canonical_destination_path("ClientRoot\\IT")


def test_library_root_merge_inserts_siblings_at_model_root() -> None:
    """Library-root: top-level children live under invalid QModelIndex() = drive/library root in the model."""
    m = DestinationPlanningTreeModel(destination_index_key_fn=_key)
    sales = _folder_payload("Sales", "Sales", workspace_row_state="planned_only", row_kind="planned_folder")
    mgt = _folder_payload("Management", "Management", workspace_row_state="planned_only", row_kind="planned_folder")
    m.append_child_payloads(QModelIndex(), [sales, mgt])
    cps: list[dict] = [
        {
            "name": "Finance",
            "id": "gf1",
            "is_folder": True,
            "item_path": "Finance",
            "semantic_path": "Finance",
            "destination_path": "Finance",
            "tree_role": "destination",
            "drive_id": "driveX",
            "base_display_label": "Finance",
        },
        {
            "name": "Sales",
            "id": "g-sales",
            "is_folder": True,
            "item_path": "Sales",
            "semantic_path": "Sales",
            "destination_path": "Sales",
            "tree_role": "destination",
            "drive_id": "driveX",
            "base_display_label": "Sales",
        },
    ]
    m.merge_graph_branch_union_at_parent(QModelIndex(), cps, parent_canonical_path="")
    assert m.find_indices_for_canonical_destination_path("Finance")
    assert m.find_indices_for_canonical_destination_path("Sales")
    assert m.rowCount(QModelIndex()) >= 3
