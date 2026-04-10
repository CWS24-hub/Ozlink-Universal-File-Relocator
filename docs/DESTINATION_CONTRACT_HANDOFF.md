# Destination tree contract — release / handoff (checkpoint)

**Canonical product + technical rules:** [destination_tree_refresh_contract.md](destination_tree_refresh_contract.md) — treat that file as the source of truth for this effort.

---

## What changed (this checkpoint)

- **Pipeline framing:** Destination library-root work is ordered as **Read → Load → Repaint** (logged via `destination_refresh_pipeline` and related steps). Graph root apply goes through **`_orchestrate_root_tree_bind_from_graph`** (alias `_destination_refresh_from_graph`).
- **Read invariant:** Structural skeleton for the selected document library is intended to match **SharePoint/Graph** for that scope (no filtering, substitution, or parallel “second tree” for real rows).
- **Overlay:** Under lazy mode + planning model view, **`destination_overlay_only_visible_structure_contract`** (see `ozlink_console/destination_tree_contract.py`) encodes **overlay-only** on top of the Graph skeleton; incremental bind regression test ensures **`reset_nested` is not** used on that path.
- **First-paint:** Loading shell / empty-until-skeleton behavior is documented and implemented for library root transitions; **empty library** UX uses shared **`EMPTY_LIBRARY_MESSAGE`** (model view + legacy widget destination path).
- **Reconcile / robustness:** Extra guards on some reconcile and merge paths; correlation ids stitch loading shell → graph read; snapshot structural top-level gets its own refresh id.
- **Tests:** Contract and regression tests under `tests/test_destination_tree_contract.py`, `test_destination_post_graph_merge_guard.py`, `test_destination_real_root_bind.py`, `test_destination_root_child_top_level_leak.py`.
- **Repo:** **`/baseline/`** (Power Platform exports) is **gitignored** unless you deliberately remove that rule.

---

## What was intentionally **not** changed

- **Cut, paste, drag-and-drop, move, assign / unassign**, and other **interaction** flows were **not** refactored for their own sake; only touches that fell out of destination read/orchestration fixes.
- **Source tree**, planned-moves table format, auth, and unrelated panels — unchanged except where a destination fix required a minimal adjacent touch (see contract scope section).

---

## What remains (incremental only — no giant rewrite)

- **Convergence:** Leftover **hotspots** in `main_window.py` (chunked bind, some materialize paths, widget vs model paths) should be tightened **one at a time** with small PRs, each pointing back to [destination_tree_refresh_contract.md](destination_tree_refresh_contract.md).
- **Examples of follow-ups:** a remaining full-tree bind under lazy model-view; a widget clear/rebuild that should be overlay-only; adding a regression test where behavior is already correct.

---

## Pre-merge manual smoke (real tenant)

Run through **library switch**, **empty library**, **lazy expand**, **restore session**. Confirm:

- Visible structure matches the **selected** document library.
- Overlay does **not** invent competing **structure** (duplicate rows for the same SPO-backed path should not appear).
- **Drag / cut / paste / assign / unassign** still behave as before.

If something fails narrowly and is destination-scoped, fix in **one** patch before merge. If the failure is broad, keep the PR open and scope the next fix.

---

## Suggested PR summary (copy-paste)

**Title:** Destination tree: contract-aligned Read → Load → Repaint checkpoint

**Summary:**

Primary reference: **[docs/destination_tree_refresh_contract.md](docs/destination_tree_refresh_contract.md)** (destination read invariant, overlay rules, first-paint, reconcile constraints).

- Destination structure is aligned with **Read → Load → Repaint**; library-root Graph results go through **`_orchestrate_root_tree_bind_from_graph`**.
- **Visible structure** is intended to come from **SharePoint/Graph** for the selected library scope only.
- **Overlay** (planning, allocations, proposed chrome) is **overlay-only** under the lazy + model-view contract; shared helper + regression test guard the incremental bind path.
- **Interaction** features (drag/cut/paste/assign/unassign, etc.) were **not** intentionally changed.
- **Remaining work** is **incremental convergence** of leftover hotspots (small follow-up PRs), not another large destination rewrite.

Handoff detail: **[docs/DESTINATION_CONTRACT_HANDOFF.md](docs/DESTINATION_CONTRACT_HANDOFF.md)**

---

*Checkpoint date: 2026-04-10*
