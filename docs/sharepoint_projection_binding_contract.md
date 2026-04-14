# SharePoint projection binding — planned workspace rows

## Graph authority (unchanged)

- **Live-confirmed** rows have a real Microsoft Graph `driveItem` id. Only Graph root bind and folder-load success may insert or replace those structural rows (`replace_all_children` from Graph payloads).
- **No fake `Root`** segment and no future-model tree as the visible structure source.
- **Full-library snapshot** remains assist-only (trust, validation, reconcile hints); it does not author visible real rows.

## Planned workspace model

Workspace-only rows represent **user intent** before SharePoint contains the path.

| Field | Meaning |
|--------|---------|
| `row_kind` | `planned_folder` \| `planned_file` (folders used first) |
| `verification_state` | `planned_only` until merged with Graph |
| `planning_uuid` | Stable identity for tracing / reconcile |
| `allocation_id` | Optional link to planning request |
| `id` | **Empty** — not a live Graph item |

Predicate: `destination_payload_is_planned_workspace_row` in `sharepoint_destination_overlay_attach.py`.

**Not live:** `destination_payload_is_live_graph_row` remains **false** for planned rows (`non_graph_structural_authority` is set via overlay marker).

## Binding behaviour

1. **Deepest anchor** — Walk library-relative segments; each step may resolve a **live** or **already-planned** folder (`_destination_row_allows_sharepoint_projection_traversal`).
2. **Missing suffix** — Under Graph authority, remaining segments are created as **planned folders** via `_sharepoint_bind_planned_segment_chain` (logging `sharepoint_planned_workspace_bind`, `projection_bind_mode=planned_bind`).
3. **Proposed / allocation replay** — If no live child matches, bind the planned chain under the resolved parent, apply overlay metadata, and **still** queue `_request_graph_destination_children_load` so Graph can confirm later.
4. **Allocation descendants** — If the live path chain breaks, bind the relative path as planned rows, then apply descendant overlay on the leaf (`_decorate_destination_graph_subtree_for_allocation_move`).

## Reconciliation (minimal)

After a destination `FolderLoadWorker` replaces children with Graph results:

- Snapshot **planned-only** direct children before `replace_all_children`.
- For each snapshot, if a **live** child matches by canonical path (preferred) or by **name**, merge: set `verification_state=live_confirmed`, `row_kind=live_folder` / `live_file`, preserve `planning_uuid` / `allocation_id`, log `sharepoint_planned_workspace_reconcile` with `transition=planned_to_live`.
- Otherwise **re-append** the planned payload (`transition=reappended_planned`).

## Forbidden

- Treating planned rows as Graph-backed for structural authority decisions.
- Inventing a synthetic **internal** `Root\` planning namespace for the visible tree.
- Replacing Graph loads entirely with planned rows (loads remain queued).

## Examples

- **Proposed** `Root3\Finance\NewDept` when `NewDept` is missing: under resolved `Finance`, append planned segments for `NewDept`, mark proposed overlay, queue Graph load on `Finance`.
- **Allocation descendant** when intermediate folder missing: planned chain under allocation root, then descendant overlay on the leaf planned row.

## Related

- Narrow overlay-visibility notes: `docs/sharepoint_live_projection_visibility_contract.md`.
