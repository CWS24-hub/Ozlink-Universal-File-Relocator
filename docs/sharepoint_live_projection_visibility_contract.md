# SharePoint live Graph — projection / overlay visibility contract

## Purpose of this document

This contract describes how **planning projections and overlays** must behave for **SharePoint destinations** under the **live Microsoft Graph** structural authority model. It exists to fix and validate **overlay visibility correctness** (truthful skips, path attachment, proactive loading, queue draining) **without** introducing a provisional cached-workspace architecture or reviving deleted future-model pipelines.

## Live Graph authority (current)

- **Visible destination folders and files** come only from **Graph-backed loads** (root bind, per-folder `FolderLoadWorker` success replacing children on real rows).
- There is **no fake `Root`** row in the UI and **no synthetic parent** rows created to satisfy overlays.
- There is **no projection-built hierarchy**, no **future-model tree bind**, and no **“projection pending” placeholder structure** as parent rows.
- **Full-tree / snapshot data** may assist trust, validation, and reconcile logic but **must not** become the source of new visible structural rows in Graph authority mode.

## What future projections / overlays may do

- **Attach metadata** to **existing** visible Graph rows (e.g. proposed folder markers, allocation chrome, labels) via model payload updates on real indices.
- **Queue Graph child loads** when a target path’s parent chain is not yet present (`_ensure_destination_projection_path_sharepoint_graph_only`, `_request_graph_destination_children_load`).
- **Replay unresolved** proposed/allocation entries when the matching parent row becomes available.

## What they must not do

- Reintroduce or approximate deleted pipelines: `_build_destination_future_model`, `_incremental_merge_destination_future_projection`, `_destination_projection_prefixes`, `_append_one_planned_move_overlay_to_destination_future_model`, `_bind_destination_future_model_sync`, `_schedule_chunked_destination_future_bind`, `_run_destination_chunked_bind_tick`.
- Insert **synthetic** structural rows or **fake** library hierarchy for visibility.
- Treat **fingerprint match alone** as proof that overlays are visible when **evidence on real rows is still zero**.

## Truthful skip requirements

- **`_try_skip_redundant_destination_future_model_materialize`** may only skip redundant materialize when it is **safe** for Graph authority:
  - If **`unresolved_proposed` + `unresolved_allocation` > 0**, fingerprint-based skip must **not** short-circuit overlay work.
  - If **planning memory** exists (`planned_moves` or `proposed_folders`) but **visible overlay evidence** (e.g. counted proposed/allocation/projection-tagged rows) is **still zero**, skip must **not** claim “already materialized”.
- **Proactive parent-chain scheduling** (`_schedule_proactive_graph_parent_chains_for_unresolved_overlays`) must run **before** redundant-skip short-circuit in `_apply_destination_planning_overlays_body`, and when **`deferred_reconcile_folder_worker_success`** bails out early after `_try_skip`, so **unresolved work is not starved** by defer/skip guards that return `0`.

## Proactive parent-chain load requirements

- On Graph authority, unresolved overlay target paths must trigger **`_ensure_destination_projection_path_sharepoint_graph_only`** so missing segments request **real** Graph loads.
- Requests must be **deduped** (e.g. `_destination_overlay_proactive_chain_seen`) and **bounded** (batch size caps) to avoid storms.
- Logging: `destination_overlay_proactive_parent_chain_batch` should remain **concise** (targets, scheduled count, optional parent-chain preview).

## Path lookup expectations

- **`_find_visible_destination_item_by_path`** resolves rows using **canonical keys** aligned with the planning model index (`_destination_payload_index_key`).
- Under a **single top-level library hub**, **re-anchor** legacy `Root\…` paths under the visible hub when needed, including **canonical equality** match when strict segment match differs.
- Misses should be classified where possible: **not loaded**, **canonical mismatch**, **empty model** — without inventing rows.

## Forbidden legacy behaviors

- Any “bind the whole future model into the tree” or **incremental projection merge** as the **visible** structure source.
- **Placeholder hierarchy** rows whose role is only to stand in for unloaded Graph parents.

## Validation checklist (manual / logs)

1. Destination **Graph root** loads; library hub row(s) appear without a fake `Root` wrapper.
2. With unresolved overlays, logs show **`destination_overlay_proactive_parent_chain_batch`** and Graph folder workers queued, not only idle deferral.
3. **`destination_overlay_skip_decision`**: when planning memory exists and visible evidence is **0**, **`final_decision`** is **`apply`**, not skip with `already_materialized_same_overlay`.
4. **`destination_overlay_unresolved_drain_tick`**: after folder success, **before/after** unresolved counts trend **down** over time when paths resolve.
5. **`destination_visible_path_lookup`**: misses explain **not_loaded** vs mismatch; **found** includes re-anchor when applicable.
6. No duplicate ghost parents; tree matches Graph.

## Relation to future “provisional cache” work

This contract is **narrow**: it does **not** specify optimistic rendering from a full local structural cache or diff-merge UI. That remains a **separate** architecture decision; this pass only secures **correct overlay visibility** on the **current** Graph-first model.
