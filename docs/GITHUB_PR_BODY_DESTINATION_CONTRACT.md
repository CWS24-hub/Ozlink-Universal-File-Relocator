## Primary reference (contract)

**[docs/destination_tree_refresh_contract.md](docs/destination_tree_refresh_contract.md)** — read this first: DESTINATION READ INVARIANT, overlay vs structure, first-paint, reconcile constraints, inventory, and what remains incremental.

**Handoff / release note:** [docs/DESTINATION_CONTRACT_HANDOFF.md](docs/DESTINATION_CONTRACT_HANDOFF.md)

---

## Summary

- Destination structure is aligned with **Read → Load → Repaint**; library-root Graph results go through **`_orchestrate_root_tree_bind_from_graph`** (alias `_destination_refresh_from_graph`).
- **Visible structure** is intended to come from **SharePoint/Graph** for the selected document library only.
- **Overlay** (planning, allocations, proposed chrome) is **overlay-only** under the lazy + planning-model-view contract (`destination_overlay_only_visible_structure_contract` in `ozlink_console/destination_tree_contract.py`).
- **Interaction** features (drag, cut, paste, assign, unassign, etc.) were **not** intentionally changed.
- **Remaining work** is **incremental convergence** of leftover hotspots in `main_window.py` (small follow-up PRs), not another large destination rewrite.

## Pre-merge

Real-tenant smoke **before merge**: library switch, empty library, lazy expand, restore session — confirm visible tree matches the selected library, overlay does not create competing structure, no duplicate visible rows for the same SPO location, interactions still normal. See handoff doc.

## baseline/

**`/baseline/`** remains **gitignored** unless those Power Platform exports are deliberately committed in a separate PR.
