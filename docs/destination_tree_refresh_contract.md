# Destination tree refresh — contract and consolidation plan

This note is the **single reference** for how the destination panel should get its data, what may touch **structure** vs **overlay**, and how we converge `main_window.py` toward **read → load → repaint**.

---

## DESTINATION READ INVARIANT

The destination tree must read **exactly** the contents of the selected SharePoint document library.

- **No filtering**
- **No reinterpretation**
- **No logical root substitution**
- **No scope transformation**

The **visible structure** must match the structure returned by SharePoint for that library.

**Nothing less, nothing more.**

*Overlay* (planning, allocations, proposed chrome) applies **on top of** that skeleton; it must not replace this read rule. Reconcile is governed separately by **[RECONCILE CONSTRAINT](#reconcile-constraint)**.

---

## VISIBLE STRUCTURE UNIQUENESS

The destination tree must **not** contain multiple visible representations of the **same logical location** derived from different internal paths.

If a location **exists in the SharePoint structure**, it must be represented **exactly once** in the visible tree.

The system **must not** render:

- **parallel hierarchies**
- **duplicate rows** derived from different internal sources
- **multiple nodes** that resolve to the same SharePoint path

Together with the **DESTINATION READ INVARIANT**, this means: show what SharePoint provides, **once per logical item**—no second tree and no duplicate rows for the same SPO-backed path.

---

## RECONCILE CONSTRAINT

Reconcile may **only** remove or adjust rows that were **introduced by the application** and are **not** part of the SharePoint source-of-truth structure.

Reconcile **must never**:

- remove or hide rows that exist in SharePoint  
- change the scope of the SharePoint result  
- reinterpret the SharePoint hierarchy  

Reconcile is **strictly limited** to correcting **client-side structural inconsistencies**.

**Root-child top-level leak pass** (`_reconcile_destination_root_child_top_level_leaks_planning_model` / `_reconcile_destination_root_child_top_level_leaks_widget`): removes a library top-level row `X` only when `Root\X` already exists under top-level `Root` **and** the two rows are treated as the **same** Graph item—**both** `id` / `item_id` values empty, or **equal** if both set. If both ids are non-empty and **differ**, the pass **does nothing** for that pair (same rule as `_destination_deduplicate_root_child_top_level_leaks` on the future-model graph).

---

## Scope boundary (what this effort changes — and what it does not)

**In scope**

- **How** the destination reads structure from the SharePoint document library (Graph scope, initial load, lazy children).
- **How** planning / future-state is **overlaid** on that structure (payload repaint, unresolved-anchor logging, lazy overlay contract).

**Explicitly out of scope unless a change is unavoidable**

- **Cut, paste, drag-and-drop, move**, and other **interaction** features: must keep current behavior; no refactors “while we’re here.”
- **Source tree**, **planned moves table**, **memory/autosave format**, **auth**, and unrelated panels — unchanged unless a destination read/orchestration fix **requires** a minimal touch (document why in the PR).

**Backward compatibility (do not regress)**

- **Legacy app / Gary’s workflow**: sessions created before Graph id capture was reliable must keep working.
- **Graph id (and related) resolution paths** already implemented for backward compatibility are **frozen for behavior**: new destination work must **not** weaken import, restore, or rebind of older drafts. Add tests when touching adjacent code.
- If a read/orchestration change risks session fidelity, **add a regression test** or feature flag rather than silently changing resolution.

---

## Empty document library (initial read)

When the destination **first** reads the selected document library and Graph returns **no items** at the chosen root scope:

- The tree should **not** pretend there is structure.
- Show a clear, user-facing message, e.g.  
  **“This document library is empty. Propose a folder to start planning.”**  
  (Final wording can be tuned for product voice; intent: empty library + call to action for proposed folders.)
- **Overlay** still applies: user may create **proposed** structure per existing product rules; that remains **overlay/planning**, not fake Graph rows.

Implement this in the **load** phase of the orchestrator (or equivalent single path) so empty libraries behave consistently across restore, library switch, and lazy mode.

---

## First step (do this before more code churn)

**Lock the contract in writing (this file) + complete a short inventory.**

1. **Agree on product rules** (one paragraph each):
   - What is **structural** (rows that exist because SharePoint/Graph returned them for the chosen scope).
   - What is **overlay** (labels, allocation/proposed chrome, flags — no new “phantom” folders/files unless explicitly product-approved).
   - What the **first visible state** is after library select (loading shell vs first wrong paint).
   - **Empty library**: message + propose-folder CTA (see [Empty document library](#empty-document-library-initial-read)).
   - **Visible uniqueness**: one visible node per SharePoint-backed location — see [VISIBLE STRUCTURE UNIQUENESS](#visible-structure-uniqueness).

2. **Inventory callers** (partial list below; extend as you touch code):
   - Every path that calls `DestinationPlanningTreeModel.reset_nested`, `append_nested_child`, `remove_node_at`, `replace_all_children`, or equivalent widget-tree mutations.
   - Mark each: **structural load** | **overlay** | **reconcile / fix-up** | **legacy / to fold**.

3. **Orchestrator name (Graph library-root → tree):**  
   `MainWindow._orchestrate_root_tree_bind_from_graph(panel_key, items, reason=...)`  
   — logs destination read + loads skeleton for source or destination.  
   `_destination_refresh_from_graph` is a **backward-compatible alias** (same implementation).

Until (1)–(3) are stable, parallel “bind” and “materialize” changes will keep fighting each other.

---

## Target pipeline (subscription-grade simplicity)

| Phase | Meaning | Allowed side effects |
|-------|---------|----------------------|
| **Read** | Graph calls for the **selected document library**; results are used **as returned** — see [DESTINATION READ INVARIANT](#destination-read-invariant). | None on the planning model. |
| **Load** | Commit **skeleton rows** only from those read results + lazy children as each folder is expanded (still **exactly** what Graph returns for each request). | Structure only; ids/paths from SPO. |
| **Repaint** | Merge draft/planning **onto existing indices** (payload/display), or log **unresolved** anchors. | Payload/roles/labels; **no** competing hierarchy. |

**`Root`** is a **normal folder name** in the customer’s library when present — not a platform abstraction. Paths like `Root\Finance` are **app semantic paths** for that layout, not a special SharePoint concept.

---

## First-paint policy (destination)

Until the **first Load commit** for the active destination library root request (real Graph children **or** the empty-library placeholder row), the UI must **not** show **structural rows** from a **previous** library or stale read.

- **Allowed before skeleton**: empty destination model (QTreeView) or non-structural loading copy + loading affordance — implemented as `DESTINATION_STRUCTURAL_LOADING_STATUS_MESSAGE` and `MainWindow._destination_enter_structural_loading_shell`.
- **Not allowed**: keeping prior top-level folders/files visible while a new root Graph read is in flight or **deferred** behind future bind (e.g. `_queue_deferred_destination_library_root` must enter the same loading shell immediately).
- **After skeleton**: normal **Repaint** (overlay) rules apply; lazy folder expands still commit structure only from Graph per folder request.

### Decision (product)

**Chosen behavior:** **empty / loading shell until the first committed skeleton** for the active library root request (Graph children or empty-library placeholder). **No partial** “old library + new overlay” structural mix during an in-flight root read. Deferred root bind must still enter the loading shell immediately so users never see stale top-level structure from a prior selection.

**Rationale:** matches the **DESTINATION READ INVARIANT** and avoids ambiguous paths during library switches and restore.

---

## Structural vs overlay (enforcement intent)

- **Structural**
  - Initial and expanded **real** folder/file rows from Graph for the active destination library, per the **DESTINATION READ INVARIANT**.
  - **Reconcile** only as allowed by **[RECONCILE CONSTRAINT](#reconcile-constraint)** (app-introduced rows only; never hide or reinterpret SharePoint truth).
  - User actions that repoint **real** rows (e.g. move proposed folder) where product allows.

- **Overlay**
  - `update_payload_for_index` / `_destination_merge_future_overlay_into_real_index` style updates.
  - Planned allocation / proposed **presentation** on real anchors.
  - Indicators, sort order refresh, expansion restore — **without** inventing a parallel tree.

- **Illegal for lazy overlay contract** (current direction)
  - Building a **second** visible tree from the future-state graph (`append_nested_child` for projected-only branches when overlay-only mode is on) — violates **[VISIBLE STRUCTURE UNIQUENESS](#visible-structure-uniqueness)**.

---

## Current entry points (inventory seed — `main_window.py`)

These are **not** the final orchestrator; they are **today’s** hotspots to fold into one flow:

| Area | Examples | Typical role |
|------|----------|----------------|
| Future model → UI | `_materialize_destination_future_model`, `_materialize_destination_future_model_body`, `_bind_destination_future_model_sync`, `_bind_destination_tree_from_future_state_model` | Mixed: was full rebind; moving toward overlay-on-skeleton in lazy mode |
| Chunked / async bind | Destination chunked bind `build` phase, `_destination_bind_run_incremental_append_model`, `_destination_bind_drain_append_stack_tick` | Structural append / reset (non–overlay-contract paths) |
| Graph root | `_orchestrate_root_tree_bind_from_graph` from root worker success / `populate_root_tree` → `_apply_root_payload_to_tree` | **Read** + **Load** for library root; destination must satisfy **DESTINATION READ INVARIANT** |
| Folder workers | `_destination_folder_worker_success` paths, `replace_all_children`, preserved nested replay | Structural children under lazy expand |
| Reconcile | `_reconcile_destination_root_child_top_level_leaks_planning_model`, `_reconcile_destination_semantic_duplicates_index`, sibling collision passes | **[RECONCILE CONSTRAINT](#reconcile-constraint)** — app-introduced inconsistencies only |
| Projection / restore | `_restore_destination_future_state_children_model`, `_merge_nested_spec_into_parent_index`, `_incremental_merge_destination_future_projection` | Must be **overlay-only** or blocked under overlay contract |
| Model primitives | `DestinationPlanningTreeModel.reset_nested` / `append_nested_child` / `remove_node_at` (`tree_models/destination_planning_model.py`) | Low-level structural API |

**Low-level model** (`ozlink_console/tree_models/destination_planning_model.py`): `reset_nested` is the nuclear structural reset; prefer routing **full** resets through the future orchestrator only.

---

## Destination structure mutation inventory (`main_window.py`)

Classification for **destination** `DestinationPlanningTreeModel` / widget mutations (extend as you touch code).

| Classification | Model / UI API | Representative callers or notes |
|----------------|----------------|--------------------------------|
| **Structural (Graph)** | `replace_all_children` | `_destination_model_view_apply_folder_load_structural_and_pipeline` — lazy folder expand from Graph |
| **Structural (Graph root)** | `reset_root_payloads` | `_apply_root_payload_to_destination_model_view` — live library root bind (via `_orchestrate_root_tree_bind_from_graph`; alias `_destination_refresh_from_graph`) |
| **Structural (legacy / snapshot)** | `reset_root_payloads`, `set_empty_library_message` | **`_destination_structural_commit_snapshot_top_level`** — session snapshot restore (chunked + non-chunked model-view paths) |
| **Structural (future-model bind)** | `reset_nested`, `append_nested_child`, `dm.reset_nested([])` | Chunked bind build, incremental append, `_destination_bind_add_top_level_item_widget`, `_merge_nested_spec_into_parent_index` |
| **Overlay** | `update_payload_for_index`, `_destination_merge_future_overlay_into_real_index` | Future-state / allocation / proposed chrome on real rows |
| **Reconcile / fix-up** | `remove_node_at`, duplicate collapse | Semantic duplicate reconcile, root-child leak coalesce |
| **Loading shell (non-SPO structure)** | `model.clear()`, `set_tree_placeholder` | `_destination_enter_structural_loading_shell`, `set_tree_placeholder` before root worker |

**Routing cut (this pass):** snapshot top-level structural restore for the planning model is centralized in **`MainWindow._destination_structural_commit_snapshot_top_level`** (logs `destination_refresh_pipeline` steps `structural_snapshot_top_level_*`).

---

## Proposed single entry point (target)

**Implemented for library-root Graph results:** `_orchestrate_root_tree_bind_from_graph` performs **Read** logging (destination) + **Load** into the tree model/widget. Callers still run **Repaint** via `_refresh_tree_ui_after_root_bind` / `populate_root_tree` as today.

**Still to converge** (incremental refactors — do not big-bang):

1. **Read** per **DESTINATION READ INVARIANT** for non-root scopes (lazy folder workers already have their own structural pipeline).
2. **Load** skeleton everywhere from those reads without duplicate bind paths.
3. **Repaint** from cached future-state / overlay fingerprint only (chunked async bind, `_materialize_destination_future_model`, etc. → overlay-on-skeleton under lazy contract).
4. Schedule lazy work **without** alternate full-tree bind paths that bypass the above.

All existing call sites should eventually classify as:

- “enqueue refresh” → orchestrator  
- or “local overlay only” → payload helpers only.

---

## Next steps after this doc

1. ~~Fix **crashers** on reconcile paths (`None` index / `isValid`) so restore never enters **abort** for routine folder completion.~~ *(Hardened: sibling-folder finalize drain, widget semantic duplicate reconcile, `append_nested_child` result handling.)*
2. ~~Decide **first-paint policy**~~ — see [Decision (product)](#decision-product) under **First-paint policy**.
3. ~~Implement **empty library** UX~~ — model-view Graph root and snapshot paths use `EMPTY_LIBRARY_MESSAGE`; legacy QTreeWidget destination root bind aligned to the same copy and pipeline log.
4. **Tests** — contract tests cover skeleton count, overlay-on-skeleton, correlation continuity; extend when adding new structural entry points.
5. ~~**Correlation id** per destination refresh~~ — `_destination_refresh_correlation_id` on pipeline logs; new id when entering the loading shell; preserved through `_destination_refresh_begin_graph_root` for the same cycle; fresh id for snapshot structural top-level commit.
6. **Ongoing:** route chunked / incremental future-model **structural** work through the same invariants (overlay-only where the lazy contract applies); extend tests per new entry points.

**Local `baseline/` folder:** Power Platform solution exports are **gitignored** at repo root (`/baseline/`). Remove the ignore rule if you intend to version those assets here.

**Chunked async bind:** When `_schedule_chunked_destination_future_bind` runs, logs use a **child** `correlation_id` (`chunk_bind_correlation_id`) and, if set, **`parent_refresh_correlation_id`** pointing at the last `_destination_refresh_correlation_id` from Graph root load—filter `destination_refresh_pipeline` on `bind_kind=chunked_async` to stitch multi-timer work.

---

## Revision

| Date | Change |
|------|--------|
| 2026-04-10 | Initial contract + inventory seed |
| 2026-04-10 | Scope boundary, empty library UX, backward compatibility (legacy / Graph ids), non-goals for interactions |
| 2026-04-10 | **DESTINATION READ INVARIANT** (exact library contents; no filtering / reinterpretation / substitution / scope transform) |
| 2026-04-10 | **RECONCILE CONSTRAINT** (only app-introduced rows; never hide/reinterpret SharePoint) |
| 2026-04-10 | **VISIBLE STRUCTURE UNIQUENESS** (one visible row per SPO location; no parallel hierarchies) |
| 2026-04-10 | **Orchestrator entry** (initial name `MainWindow._destination_refresh_from_graph`) + `destination_refresh_pipeline` logs (`read` / `load` / `repaint`) with per-refresh `correlation_id` |
| 2026-04-10 | **Pass 3:** same `correlation_id` carried on lazy folder load (`lazy_folder_skeleton_committed`, `folder_worker_post_load_repaint`) and incremental bind (`incremental_bind_*`) pipeline logs |
| 2026-04-10 | **First-paint policy:** `_destination_enter_structural_loading_shell` + deferred root queue clears stale structure; expand-all status filter allows `Loading root content` |
| 2026-04-10 | **Chunked async bind:** `chunk_bind_correlation_id` + `parent_refresh_correlation_id` on `_destination_chunked_bind_state`; `destination_refresh_pipeline` with `bind_kind=chunked_async` at schedule, build enter, collect_flat, allocation, indicators, restore_expand, complete / abort |
| 2026-04-10 | **Structure inventory + routing:** table [Destination structure mutation inventory](#destination-structure-mutation-inventory-main_windowpy); `_destination_structural_commit_snapshot_top_level` for snapshot root bind |
| 2026-04-10 | **RECONCILE CONSTRAINT — root-child leak:** skip coalesce when top vs `Root\X` have conflicting non-empty Graph ids (model + widget); doc + regression tests |
| 2026-04-10 | **Contract tests:** graph skeleton count/order/no-placeholder; id-poor root bind rows; overlay repaint does not add library-top rows for unanchored future nodes |
| 2026-04-10 | **First-paint decision** recorded; reconcile hardening + widget empty-library UX + correlation continuity (loading shell → graph read; snapshot commit gets fresh id) |
| 2026-04-10 | **Graph root orchestrator:** `_orchestrate_root_tree_bind_from_graph` (alias `_destination_refresh_from_graph`); root worker + `populate_root_tree` call the canonical name; `/baseline/` gitignored |
| 2026-04-10 | **Overlay contract helper:** `destination_overlay_only_visible_structure_contract` in `destination_tree_contract.py` (mirrors `MainWindow._destination_overlay_only_visible_structure_contract`); regression test: lazy incremental bind skips `reset_nested` |
