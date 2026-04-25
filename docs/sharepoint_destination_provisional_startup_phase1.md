# SharePoint destination — provisional cached startup (Option 3 Phase 1)

## Intent

After login, when the destination panel is in **SharePoint** mode, the UI may apply the last persisted **destination workspace tree snapshot** to `DestinationPlanningTreeModel` **before** the live Microsoft Graph root bind completes. Those rows are stamped `workspace_row_state=cached_provisional` (or `planned_only` for planned workspace rows) and **must not** satisfy `destination_payload_is_live_graph_row`.

## Authority

- **Live SharePoint structure** remains exclusively `workspace_row_state=live_confirmed` from Graph payloads.
- Phase 1 does **not** add synthetic library parents, internal `Root` wrappers, or future-model structural revival.
- When Graph root children arrive, matching root `id`s are **merged in place** into live-confirmed payloads (minimal transition); subtree identity uses existing `replace_all_children(..., graph_child_bind=True)` from Phase 0.

## Opt-out

Set `OZLINK_PROVISIONAL_DESTINATION_STARTUP=0` (or `false` / `no` / `off`) to skip immediate snapshot paint.

## Phase 2+ (explicitly out of scope)

Full rename reconciliation, aggressive stale pruning, deep ID migration, and broad background convergence gates are reserved for later phases.
