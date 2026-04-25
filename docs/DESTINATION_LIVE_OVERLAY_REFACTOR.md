# Destination live skeleton + overlay refactor

## Phase A — Current pipeline (audit)

### 1. Pipeline map (SharePoint destination)

| Stage | Primary symbols | Role |
|--------|------------------|------|
| Root fetch | `RootLoadWorker`, `graph.list_drive_root_items_normalized`, `load_library_root` | Live Graph `drives/{id}/root/children` |
| Root bind UI | `_apply_root_payload_to_destination_model_view` | `DestinationPlanningTreeModel.reset_root_payloads` |
| Non-auth shell (legacy) | Placeholder `destination_authority_pending` row | Blocked “real” top-level until full walk |
| Full library walk | `DestinationFullTreeWorker`, `on_destination_full_tree_success` | Builds `_destination_full_tree_snapshot` |
| Real + overlay merge | `_build_destination_future_model`, `_refresh_destination_real_tree_snapshot` | Imports snapshot + proposed + allocations into one `model_nodes` graph |
| Bind to view | `_materialize_destination_future_model`, `_bind_destination_future_model_sync`, chunked paths | `DestinationPlanningTreeModel` rows |
| Per-folder Graph | `FolderLoadWorker`, `on_folder_load_success` (`destination`) | `replace_all_children` on planning model |
| Restore / session | `_memory_restore_in_progress`, snapshot restore, `_destination_restore_*` | Can defer materialize / expand; historically gated full-tree worker |
| Caching | `graph._drive_children_cache`, persistent children cache | Speed; invalidated on authority paths |

### 2. Where `Root\...` is assumed

- `_build_destination_future_model`: explicit `Root` node; overlays use `_destination_projection_prefixes`, `_canonical_destination_projection_path`.
- Full-tree snapshot rows: `semantic_path` / `parent_semantic_path` under `Root`.
- Expand/restore: `_restore_expanded_destination_paths_body` seeds `"Root"`; prefix hydration walks segment chains.
- Planned moves / memory: `destination_path` fields normalized to library-relative `Root\...` (see `planned_move_graph_resolve`, manifest).

### 3. Where restore / snapshot / planning affect visible hierarchy

- `_materialize_destination_future_model` and deferred materialize timers.
- `_destination_future_model_blocked_by_source_restore`, `_try_flush_destination_future_model_after_source_restore`.
- `on_folder_load_success` destination: skips `replace_all_children` when `_memory_restore_in_progress` and visible subtree larger than incoming (keeps replayed structure).
- Chunked bind / scoped rebuild after restore.
- `_destination_real_tree_snapshot` merge when full tree not ready: `_ensure_visible_destination_root_children_in_model`.

### 4. Where overlays mix with structural authority

- Single `model_nodes` dict with `node_state` (`real` / `projected` / `proposed` / `allocated`).
- Same `DestinationPlanningTreeModel` holds Graph rows and overlay rows.
- Full-tree snapshot import and overlay fingerprint cache share one payload.

### 5. Full-tree scheduling gates (prior to this refactor)

- `_ensure_sharepoint_destination_full_tree_worker_scheduled` / `start_destination_full_tree_worker`: deferred on `_memory_restore_in_progress` unless “authority shell” waiting.
- `_destination_future_bind_sync_active`: timer retry.
- Snapshot reuse when trust TTL valid and no authority shell.

---

## Proposed replacement architecture

### Layer 1 — Live skeleton (target)

- **Authority**: Microsoft Graph parent/child only for **real** rows.
- **Mechanism**: Same primitives as source — `reset_root_payloads` + `replace_all_children` + lazy folder workers (already on `DestinationPlanningTreeModel`).
- **Visible**: No synthetic `Root` row; no “reconciling” placeholder row as a structural parent (status bar / flag only for “full enumerate in progress”).
- **Model**: Either keep `DestinationPlanningTreeModel` as the **explorer** surface for real rows only, or introduce `SharePointDestinationExplorerModel` (clone of source model) and attach overlays separately (Phase B+).

### Layer 2 — Overlay engine (target)

- **Inputs**: `planned_moves`, `proposed_folders`, projection rules.
- **Output**: Rows marked `overlay_kind` / existing `proposed` / `planned_allocation` / `placeholder_role` distinct from Graph `id` authority.
- **Rules**: Overlays never remove or replace a real Graph row; reconcile when Graph gains a matching folder (merge/dedupe).
- **Deep paths**: Materialize prefix chain as projected nodes under real parents (existing `_append_one_planned_move_overlay_*` logic, refactored to run against live skeleton indices).

### Path contract (Phase C)

- **Internal**: Keep `Root\...` in memory, JSON, execution.
- **Bridge**: `destination_path_bridge` — map canonical planning path ↔ Graph `item_path` segments / drive item identity.
- **Visible**: Tree identity for real rows = Graph `id` + `drive_id`; display from Graph `name`.

---

## Modules / functions to change (rolling)

| Area | Files / symbols |
|------|------------------|
| Root bind | `MainWindow._apply_root_payload_to_destination_model_view` |
| Authority pending detection | `MainWindow._destination_tree_shows_authority_pending_shell` |
| Full-tree schedule | `MainWindow._ensure_sharepoint_destination_full_tree_worker_scheduled`, `start_destination_full_tree_worker` |
| Full-tree completion | `MainWindow.on_destination_full_tree_success`, `on_destination_full_tree_error` |
| Prepare load | `MainWindow._destination_prepare_live_sharepoint_root_load` (reset new flags) |
| Overlay split (later) | `_build_destination_future_model` → split `import_live_snapshot` vs `apply_overlays` |
| Path bridge | `ozlink_console/destination_path_bridge.py` |
| Tests | `tests/test_destination_path_bridge.py`, `tests/test_destination_full_tree_schedule_contract.py` (new) |

---

## Invariants to preserve

- Planned move execution and Graph resolve must keep accepting canonical `Root\...` paths.
- Local FS destination mode unchanged.
- Source pane and `SharePointSourceTreeModel` unchanged.
- Destination manual drag (`DestinationPlanningTreeView`) unchanged.
- Trust / digest / open-validation hooks for SPO remain available for background authority.

---

## Migration risks

- **Regression**: Removing shell row changes timing of `_destination_materialize_authoritative_if_shell_still_showing` — mitigated by `_destination_full_library_reconcile_pending` flag.
- **Restore**: Memory restore may still prefer larger subtree vs shallow Graph; review `skip_destination_child_replace` separately.
- **Full cutover**: Swapping models requires retargeting ~100 `destination_planning_model` references — staged.

---

## This iteration (implemented)

1. **Visible root** = live Graph children only (no `destination_authority_pending` placeholder row).
2. **Reconcile pending** = explicit flag until full-tree success/error, wired into shell detection for scheduling/materialize.
3. **Full-tree worker** = not deferred on `_memory_restore_in_progress` when destination root Graph bind has recorded the active `drive_id`.
4. **Path bridge** module + unit tests for internal `Root\...` mapping.
5. Architecture doc (this file) for Phase B+ overlay engine extraction.

---

## Validation checklist (manual)

- [ ] After library bind, top rows are Graph root children (names match SPO).
- [ ] Status still indicates full reconcile while walk runs; no fake folder row.
- [ ] Full-tree worker starts during memory restore after destination root bind.
- [ ] Overlays still appear after authoritative materialize (existing pipeline until overlay split).

---

## Implementation status (this branch)

### Done

- **Visible root**: SharePoint destination root bind uses **only** live Graph root children (`reset_root_payloads(payloads)`); removed the structural `destination_authority_pending` placeholder row.
- **Reconcile state**: `_destination_full_library_reconcile_pending` set on root bind, cleared on full-tree success/error; `_destination_tree_shows_authority_pending_shell()` treats this as authority-pending for gates (no reliance on a fake tree row).
- **Full-tree vs restore**: `_destination_full_tree_memory_restore_may_block_worker` + bound drive id `_destination_sharepoint_root_graph_bound_drive_id` so `_memory_restore_in_progress` does not defer the worker once the same drive has completed Graph root bind.
- **Path bridge**: `ozlink_console/destination_path_bridge.py` + unit tests.
- **Overlay placeholder module**: `ozlink_console/destination_overlay_layer.py` (marker helper only; merge logic still in MainWindow until Phase B).
- **Tests**: `test_destination_path_bridge.py`, `test_destination_full_tree_memory_restore_gate.py`, `test_destination_reconcile_pending_shell.py`.

### Not done (explicit follow-up / Phase B+)

- Split `_build_destination_future_model` so **real** rows are never imported from snapshot into the visible model; overlays only after lazy Graph skeleton (or keep snapshot for digest only).
- Dedicated `SharePointDestinationExplorerModel` vs continuing to use `DestinationPlanningTreeModel` as the explorer surface.
- Overlay attach pass keyed by Graph `id` + canonical path bridge.
- Tests 3–12 in the original brief (rename/delete UI, overlay reconcile, expansion restore with lazy load) — require larger UI/integration harness.

### Removed from visible structural authority

- The **reconciling placeholder folder row** inserted before Graph children at the destination root.

### Still overlay / secondary authority (unchanged in this iteration)

- `_materialize_destination_future_model` still merges snapshot + proposed + allocations for bind after full walk.
- Full-tree snapshot still drives **non-visible** digest/trust and **current** real import in future model.
