# Ozlink Browse vs Plan vs Execute Engineering Contract

## Purpose

This contract defines the runtime boundaries for Ozlink so the app behaves like a file browser by default, a planning tool only when the user edits planning state, and a heavy truth-checking engine only when the user validates or executes.

The visible tree is a **preview surface**, not a live reconciliation engine.

This contract is intended to be enforceable in code and tests. It is not guidance. It is the architecture rule set.

---

## Product Promise

### Client-facing behavior

The client should be able to:

* open the app
* see the source and destination structure they have created so far
* understand proposed folders, exclusions, planning state, and review state
* browse folders smoothly
* execute later so the real transfer matches the structure they are seeing

### Engineering translation

This means:

* **Browse** must remain cheap
* **Plan** must be scoped to impacted rows only
* **Execute** is where heavy truth-checking belongs

---

## Non-negotiable architecture principle

### The tree is a preview of intent

The source and destination trees shown in the UI must be treated as:

* a browser of live/provider children where needed
* a renderer of already-known planning state

They must **not** be treated as the place where the system continuously rediscovers, revalidates, or repairs global planning truth during ordinary viewing.

### Heavy truth-making belongs in execution

Operations such as:

* Graph ID resolution for unresolved rows
* missing destination parent resolution
* move manifest construction
* collision / duplicate execution validation
* retry planning
* final live-truth validation

belong to **Execute lane** or an explicit **Validate/Prepare for Execute** action, not to ordinary browse or paint operations.

---

## The Three Lanes

# Lane 1 — Browse / View Lane

## Intent

This is the default lane. It should feel like turning the lights on in a store.

## Entry triggers

The following events enter Browse lane:

* source tree expand/collapse
* destination tree expand/collapse
* scrolling
* paint / redraw
* lazy visible-child load for a visible node
* navigate to path for viewing only
* selection change for viewing only
* startup rendering of the visible tree shell
* restore-time shell presentation of previously known structure

## Allowed operations

Browse lane may do only the following:

* fetch or read children for the visible node
* bind visible rows
* update minimal visible-row metadata required for display
* update icons, labels, simple badges, and children_loaded state
* maintain minimal path/index/cache coherence required for visible-row correctness
* attach already-known planning chrome to impacted visible rows if that data is already available cheaply
* lightweight visibility or selection state changes
* cheap sort/stability work local to the inserted sibling set only

## Forbidden operations

Browse lane must never directly or indirectly trigger any of the following unless explicitly promoted into Plan or Execute lane:

* deep projection
* subtree projection walks
* `_refresh_source_projection_for_paths(..., subtree_scope="full")`
* broad impacted-row discovery by walking entire visible subtrees
* `_apply_destination_planning_overlays(...)` from a browse-triggered load
* `_schedule_coalesced_overlay_invariant(...)` from a browse-triggered load when there is no planning mutation
* destination invariant repair or destination-wide reconcile
* full-tree walks on source or destination
* Graph linkage batch resolution
* broad Graph ID refresh
* missing-parent resolution
* duplicate/collision recomputation beyond the touched row(s)
* execution manifest rebuild
* global readiness recomputation
* broad startup-time “catch-up” work piggybacking on paint/expand/scroll

## Output rules

Browse lane may change only:

* visible child rows for the browsed node
* local display properties for those rows
* minimal cache/index state needed to make those rows resolvable

Browse lane must not:

* mutate unrelated source rows
* mutate destination planning state
* create or repair broad destination structure
* broaden startup scope

## Escalation rule

Browse lane may escalate only if one of the following is explicitly true:

1. the user performs a planning mutation action
2. the user explicitly requests validate/prepare/execute
3. a narrowly-scoped impacted-row check proves the browsed node intersects existing planning state

Even when intersection exists, Browse lane may only request a **scoped Plan-lane refresh for impacted rows**, not a full subtree or global pass.

## Acceptance examples

### Valid

* expand a folder with 2,500 children and no planning intersection → show children, roots-only or no-op planning refresh, no destination overlay pass
* scroll through visible rows → no source/destination-wide computation

### Invalid

* expand a folder and trigger full subtree projection because the session has planned moves somewhere else
* browse a source folder and trigger destination overlay repair without a planning mutation
* paint a destination shell and trigger Graph linkage batch resolution across unresolved rows

---

# Lane 2 — Plan Lane

## Intent

Plan lane runs only when the user changes planning state or when Browse lane proves a browsed node intersects planning state and a small targeted refresh is needed to keep the preview honest.

## Entry triggers

The following events enter Plan lane:

* assign folder / assign destination
* propose folder
* exclude leaf
* include leaf again
* retarget destination
* remove mapping
* confirm review item
* change duplicate-resolution choice
* explicit refresh of planning state for selected rows
* browse event with proven planning intersection, but only for impacted rows

## Allowed operations

Plan lane may do the following:

* recompute planning state for impacted rows only
* recompute direct ancestors/descendants only when required for consistency
* update destination preview only where truly impacted by the planning mutation
* update review/readiness state for impacted rows only
* update planning chrome/badges for impacted visible rows
* perform narrow duplicate/review calculations for affected items
* schedule bounded follow-up work tied to the mutation

## Forbidden operations

Plan lane must not:

* treat a browse gesture as a planning mutation
* do full-tree projection because it is easier than impacted scoping
* do destination-wide repair/reconcile unless the mutation itself changes destination planning structure broadly and this is explicitly justified
* perform broad Graph linkage batch resolution as a side effect of planning UI edits
* enumerate live provider trees beyond what the impacted mutation requires
* block the UI thread with monolithic subtree or global passes when an impacted-only update is sufficient

## Output rules

Plan lane may change:

* impacted source rows
* impacted destination preview rows
* local readiness/review states for impacted rows
* lightweight persisted planning state

Plan lane must preserve the rule that the preview remains cheap to browse after the mutation is applied.

## Escalation rule

Plan lane may escalate to Execute lane only on:

* explicit Validate/Prepare action
* explicit Execute action

Plan lane must not automatically do Execute-lane truth-making just because a row is planning-relevant.

## Acceptance examples

### Valid

* user retargets one mapped folder → only affected source rows, destination preview rows, and direct related readiness states update
* user excludes one file → only that file and directly affected aggregates refresh

### Invalid

* user changes one mapping and the app walks the entire destination tree to “be safe”
* user proposes one folder and the app re-runs global Graph ID resolution

---

# Lane 3 — Execute Lane

## Intent

Execute lane is where the system makes real provider truth match the preview the client has approved.

## Entry triggers

The following events enter Execute lane:

* explicit Validate / Prepare to Execute
* explicit Execute / Transfer
* explicit retry of failed execution items

## Allowed operations

Execute lane may do the following:

* resolve Graph/provider IDs needed for execution
* resolve or create missing destination parents where product rules allow
* construct move/copy manifests
* perform duplicate/collision execution validation
* run retry logic
* perform final truth validation required to execute safely
* record execution status and outcomes

## Forbidden operations

Execute lane must not:

* change the user’s plan silently without surfacing it
* mutate the preview structure in a way that contradicts what the user approved unless surfaced as an execution issue or explicit remediation
* use Browse lane as an excuse to preload broad execution truth during ordinary viewing

## Output rules

Execute lane may change:

* live provider state through moves/copies/creates as allowed
* execution status fields in the preview
* readiness/error state based on execution truth

---

## Preview vs Truth Contract

### Preview

The visible UI tree is allowed to show:

* saved planning state
* proposed structure
* exclusions
* review state
* execution readiness state if already known

Preview does **not** require all live provider identities to be resolved at browse time.

### Truth

Provider truth required for safe execution includes:

* stable provider IDs
* live destination parent existence or creation path
* collision checks
* manifest correctness

Truth belongs to Execute lane or explicit Validate/Prepare, not Browse lane.

---

## Backward Compatibility Contract with Gary’s Legacy App

## Goal

Backward compatibility is required for Gary’s work in the legacy app.

## Rule

Compatibility is provided through a **versioned adapter contract**, not by preserving current lane leakage.

## Required compatibility behavior

The new implementation must:

* continue to import legacy planning/draft/session data produced by Gary’s app
* preserve existing plan semantics where they are part of user-visible behavior
* avoid breaking legacy export/import workflows
* treat legacy snapshot/tree-heavy data as import material or preview hints, not as authority for forcing full live startup computation

## Required architecture rule

Gary compatibility must be implemented through:

* versioned JSON/schema adapters
* explicit import pipelines
* golden fixtures/tests for legacy files

Gary compatibility must **not** require:

* restoring full broad destination/source trees as live computational models on startup
* reintroducing browse-triggered deep projection or destination repair
* reintroducing replay storms, synthetic structure, or synchronous global refresh behavior

## Acceptance rule

A legacy session/draft produced by Gary’s app must be loadable so the user can see their structured work, but loading it must still obey Browse/Plan/Execute lane boundaries.

---

## Current Known Lane Violations to Eliminate

The current codebase has known or recent examples of lane leakage. These must be treated as defects to remove, not behaviors to preserve.

Examples include:

* source folder load triggering broad source projection refresh when the folder is not planning-relevant
* source folder load triggering destination overlay or invariant work without a planning mutation
* startup shell presentation triggering large materialize/bind/reconcile phases beyond what is needed to show the shell
* browse/restore lifecycle points repeatedly invoking Graph linkage audits/resolution attempts for unresolved rows
* binding small child batches causing multi-second main-thread work due to hidden full-model or broad-scope follow-up operations

This list is illustrative, not exhaustive. Cursor must identify and remove equivalent violations.

---

## Enforcement Rules for Implementation

## Hard rule 1

A browse-triggered source or destination folder load must never directly or indirectly call broad planning or execution routines unless promoted by explicit lane transition.

## Hard rule 2

The presence of `planned_moves` somewhere in the session is not sufficient justification for subtree-wide or global work on every browse event.

## Hard rule 3

Any work triggered by browsing must be proportional to:

* the visible folder being loaded
* the visible row set
* the impacted planning intersection for that folder only

It must not be proportional to the total visible subtree, total session plan size, or total destination tree size unless the user explicitly invoked Plan or Execute work.

## Hard rule 4

If a row or folder has no planning intersection and no execution truth requirement, it must remain mostly inert.

## Hard rule 5

UI-thread operations must remain bounded and local. If a batch of a few rows causes seconds of work, hidden lane leakage or non-local recomputation must be assumed and removed.

---

## Acceptance Test Matrix

### Browse-only

1. Expand a large folder with no planning intersection.

   * Children appear.
   * No full subtree projection.
   * No destination overlay/invariant pass.
   * No broad Graph ID resolution.

2. Scroll a large visible tree.

   * No broad planning or execution work is scheduled.

3. Load a legacy session with many saved nodes.

   * The shell appears.
   * The user can browse their structure.
   * Heavy truth-making does not begin just because the tree is visible.

### Plan

4. Change one mapping.

   * Only impacted rows and direct related preview rows refresh.
   * No global destination walk unless explicitly justified and tested.

5. Exclude one leaf.

   * Only impacted readiness/review states change.

### Execute

6. Click Validate/Prepare.

   * Graph/provider IDs may resolve.
   * Missing parents may be checked.
   * Manifest/collision logic may run.

7. Click Execute.

   * Real provider operations run.
   * Execution status feeds back into preview.

### Gary compatibility

8. Import a golden legacy fixture from Gary’s app.

   * Plan semantics remain visible.
   * Browse remains cheap.
   * No restore-time global computational storm.

---

## Repository Strategy Recommendation

## Immediate recommendation

Do **not** continue trying to force the current monolithic runtime to evolve without a boundary.

Choose one of these two paths:

### Preferred

Create a **new top-level package or new repo** for the Browse/Plan/Execute architecture.

Why:

* it creates a hard boundary against old lane leakage
* it lets Gary continue on the legacy app
* it allows legacy compatibility through adapters instead of shared runtime behavior
* it makes forbidden-import and ownership rules enforceable

### Acceptable fallback

Create a **new branch** with a strict constitution and forbidden-import rules, but only if the team is disciplined enough to stop reusing legacy browse-time computation patterns.

## Recommended shape

* legacy app remains stable for Gary
* new architecture uses adapter-based import from legacy JSON/session data
* shared compatibility surface is schema/tests, not shared browse-time runtime behavior

---

## Migration Rule

The goal is to preserve what users have already done while changing runtime behavior.

Therefore:

* preserve user-visible planning semantics
* preserve import/export compatibility
* preserve execution outcomes where intended
* do **not** preserve current browse-time computation storms, broad startup live walks, or lane leakage just because they exist today

---

## Implementation Directive to Cursor

When changing code, Cursor must:

1. classify the triggering event into Browse, Plan, or Execute lane before modifying behavior
2. document which lane a changed path belongs to
3. remove any direct or indirect calls that violate lane rules
4. add tests that prove browse events do not trigger Plan/Execute work without explicit promotion
5. preserve Gary compatibility through adapters and fixtures, not by keeping legacy runtime coupling alive

If a piece of code appears to require violating this contract for correctness, Cursor must stop and explain why the preview model or adapter boundary is insufficient, instead of silently reintroducing the old behavior.

---

## Hybrid Destination Preview Contract

## Core product rule

The destination tree is a **merged preview**:

> saved plan preview first + live SharePoint overlay only when needed

It is not purely memory-only and not purely live-Graph-only.

The user must be able to see:

* folders that already exist in SharePoint
* folders/files planned under those existing folders
* proposed folders that do not exist yet
* rows not refreshed in this session
* rows that have execution/validation issues only after explicit validation proves the issue

---

## Destination Row State Badges

Every destination row must be able to show one or more clear row states.

### Required badge states

| Badge                | Meaning                                                                                           |
| -------------------- | ------------------------------------------------------------------------------------------------- |
| `[Live]`             | Confirmed to exist in SharePoint in the current or a trusted previous live refresh.               |
| `[Planned]`          | Exists in the saved relocation plan / allocation preview. May or may not exist in SharePoint yet. |
| `[Proposed]`         | User-created planned destination folder. Expected to be created during execution if required.     |
| `[Not refreshed]`    | Displayed from saved preview/cache; not checked against SharePoint in this session.               |
| `[Live + Planned]`   | Exists in SharePoint and also has planning semantics attached.                                    |
| `[Pending creation]` | Does not need to exist yet; expected to be created during Validate/Execute.                       |
| `[Issue]`            | Only after explicit Validate/Prepare/Execute or explicit branch refresh proves a real problem.    |

### Example required display behavior

If SharePoint contains:

```text
Root3
  Finance
```

and the saved plan contains:

```text
Root3 / Finance / Payroll / 2026
```

then startup may show:

```text
Root3             [Live or Not refreshed]
  Finance         [Live or Not refreshed]
    Payroll       [Planned or Not refreshed]
      2026        [Planned]
```

After the user expands or refreshes `Finance`, if SharePoint confirms `Payroll` exists, the tree must merge the live row with the planned row and show:

```text
Root3             [Live]
  Finance         [Live]
    Payroll       [Live + Planned]
      2026        [Planned or Pending creation]
```

If SharePoint does not contain `Payroll`, that is not automatically an error during Browse lane. It remains:

```text
Root3             [Live]
  Finance         [Live]
    Payroll       [Planned or Pending creation]
      2026        [Planned]
```

Only explicit Validate/Prepare/Execute may promote that state to `[Issue]`, and only if product execution rules say it cannot be created or resolved.

---

## Merge Rules for Destination Rows

Rows must merge by canonical destination path and provider identity when available.

| Source of row                                  | Display result                                     |
| ---------------------------------------------- | -------------------------------------------------- |
| SharePoint only                                | `[Live]`                                           |
| Plan only                                      | `[Planned]`, `[Proposed]`, or `[Pending creation]` |
| SharePoint + Plan                              | `[Live + Planned]`                                 |
| Saved preview but not checked this session     | `[Not refreshed]` plus any saved plan badge        |
| Validated missing/unresolvable live dependency | `[Issue]`                                          |

### Important rule

A live SharePoint folder can be an anchor for planned work under it.

Planned descendants under a live parent must remain visible even if those descendants are not live-confirmed yet.

The app must never drop planned/proposed children just because a live branch refresh returns only currently existing SharePoint children.

---

## Destination Startup Behavior

At startup, the destination panel must:

* render the saved destination preview immediately
* preserve Gary/legacy draft planned rows and proposed folders
* show row badges based on saved state and known live state
* avoid broad live SharePoint rehydration
* avoid broad Graph linkage resolution
* avoid broad destination materialize/reconcile
* avoid fan-out Graph workers under the root just because saved preview rows are visible

Startup may show `[Not refreshed]` where current live state has not been checked.

Startup must not treat planned/proposed rows as broken merely because they do not yet exist in SharePoint.

---

## Destination Manual Expand / Refresh Behavior

When the user expands a destination folder or explicitly refreshes a branch:

* query SharePoint only for that folder’s immediate children
* merge returned live children into the saved preview under that branch
* preserve planned/proposed children under the same branch
* upgrade matching rows to `[Live + Planned]` when both live and planned state exist
* mark newly discovered SharePoint-only children as `[Live]`
* avoid full-tree overlay/reconcile/materialize
* avoid resolving unrelated planned moves

This allows new folders added directly in SharePoint to appear when relevant without forcing a full-library startup crawl.

---

## Validate / Execute Behavior for Destination

Validate/Prepare/Execute may perform heavy truth-checking:

* resolve Graph/provider IDs
* check missing live parents
* create required proposed/planned folders where allowed
* detect collisions and duplicates
* build execution manifests
* report real issues

Only this lane may turn planned/pending rows into `[Issue]` based on execution truth.

---

## Gary Legacy Draft Compatibility for Hybrid Destination Preview

Gary’s legacy app may provide drafts/session data containing:

* planned moves
* proposed folders
* destination paths
* path-only identities
* memory snapshots
* preview rows that may not exist in SharePoint yet

The new/hybrid behavior must treat those drafts as **valid preview input**.

### Required compatibility rules

* Legacy planned/proposed rows must be imported and displayed in the destination preview.
* Path-only rows from Gary’s draft must not be treated as startup linkage failures if they are planned/proposed or pending creation.
* Legacy full-tree snapshots may be used as preview hints, but must not force broad live Graph rehydration on startup.
* Importing Gary’s draft must preserve the client-visible structure he built.
* Live SharePoint overlay must merge into Gary’s planned structure without deleting planned-only rows.
* Execution/Validate must still perform full truth-checking before transfer.

### Golden fixture requirement

Maintain at least one sanitized Gary legacy draft/session fixture and test that:

* the planned destination structure loads
* badges are assigned correctly
* planned-only rows remain visible
* live overlay can upgrade matching rows to `[Live + Planned]`
* startup does not run broad live verification just because the legacy draft contains many rows

---

## Final Standard

Ozlink must behave like:

* a **store with the lights on** in Browse lane
* a **planner’s workbench** in Plan lane
* a **warehouse/factory** only in Execute lane

The client should see a structured preview of what they have done so far.
Execution should make the real destination match that preview.

For destination specifically, the standard is:

> saved plan preview first, live SharePoint overlay on demand, clear badges for what is live vs planned vs pending.

That is the standard this contract enforces.
