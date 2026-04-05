# Execution Architecture Playbook

Execution-cutover dossier focused only on replacing the live manifest execution path safely.
This document is code-grounded and limited to execution-relevant subsystems.

**Controlled internal cutover posture (current):**

- **Primary internal path:** snapshot / execution-plan pipeline (`draft_pipeline`): bundle export, harness, bridge-backed execution.
- **Fallback / recovery path:** legacy manifest worker (`TransferJobRunner` / manifest orchestration). Enabled via Settings (internal admin): **Legacy manifest execution only**, or by setting `ENABLE_DRAFT_PIPELINE_EXECUTION=false`.
- **Legacy pilot cap UI** (Graph operation limits and scoped folders): **deprecated for routine internal validation** when the snapshot path is active; it remains only on the legacy fallback path for recovery debugging.
- **Rollback:** unset env + uncheck legacy fallback restores snapshot primary; `ENABLE_DRAFT_PIPELINE_EXECUTION=0|1` still overrides the in-app checkbox for emergencies.

### Scoped snapshot subset modes (Phase 2)

When **Run selected planned moves only** is enabled on the draft snapshot execution path, two explicit modes are available (default: **strict** until the user chooses otherwise):

- **`strict`:** `subset_execution_plan_by_mapping_ids` — only steps whose `mapping_id` is in the selected seed set; no automatic parent-chain or destination-path expansion.
- **`dependency_closure`:** `subset_execution_plan_by_dependency_closure` — selected seeds plus structural ancestors via `parent_step_id`, plus path-based `create_folder` steps needed for seeded file copy/move destinations; for each normalized directory prefix, **`dst_chain:*` steps are preferred** over proposed-folder steps when both cover the same path.

**Harness:** `DraftPipelineHarnessRequest.snapshot_scoped_mode` (`strict` | `dependency_closure`) and `scoped_seed_mapping_ids`. After a full plan is built, the harness applies the chosen subset; **full-plan execution is unchanged** when scoped mode is empty or seeds are not supplied. Invalid mode, empty seeds, or no matching steps stops at plan build with stable codes (`scoped_execution_invalid_mode`, `scoped_execution_empty_seeds`, `scoped_execution_no_matching_steps`).

**Orchestrator:** `run_snapshot` passes `snapshot_scoped_mode` and `scoped_seed_mapping_ids` into `run_pipeline_from_bundle_folder`; manifest export path behavior is unchanged.

**UI:** Execution page — checkbox plus radio pair (**Strict allowlist only** / **Include required folder/path steps**); selection source remains the Planned Moves table. Legacy manifest execution strips these options.

---

## 1. Cutover Scope

Target cutover question:

- Can live execution be moved from the current manifest path to the new draft snapshot pipeline safely?

Execution-critical files in scope:

1. `ozlink_console/main_window.py`
2. `ozlink_console/transfer_manifest.py`
3. `ozlink_console/transfer_job_runner.py`
4. `ozlink_console/graph.py`
5. `ozlink_console/planned_move_graph_resolve.py`
6. `ozlink_console/draft_snapshot/resolver_service.py`
7. `ozlink_console/draft_snapshot/plan_materialization.py`
8. `ozlink_console/draft_snapshot/execution_bridge.py`
9. `ozlink_console/draft_snapshot/pipeline_harness.py`
10. `ozlink_console/draft_snapshot/run_log.py` (execution-critical telemetry)

---

## 2. Subsystem Cutover Assessment (A–I)

### 2.1 `main_window.py`

**A. Exact current role in execution**
- Current live execution orchestrator and user-facing entrypoint for simulate/run manifest workflows.

**B. Key entry points / functions / classes**
- `ManifestRunWorker`
- `_on_simulate_run_save_manifest`
- `_load_execution_manifest_from_path`
- `_start_manifest_run_worker`
- `_run_transfer_manifest_from_path_with_dialogs`

**C. Expected / emitted data shapes**
- Expects UI planning state (`self.planned_moves`, `self.proposed_folders`) and manifest JSON.
- Emits manifest run requests, run options, and status signals.

**D. Classification**
- **Dual-path internal:** snapshot pipeline primary; legacy manifest fallback.

**E. What would break if cutover happened today**
- Forcing removal of the legacy worker would remove rollback/recovery for snapshot failures and block manifest-only workflows.
- Misconfigured env/toggle could still route operators to the wrong path if runbook steps are outdated.

**F. What must change before cutover**
- ~~Add controlled alternate run path~~ *(done: internal default is snapshot pipeline; legacy is opt-in fallback.)*
- Preserve rollback (`ENABLE_DRAFT_PIPELINE_EXECUTION` and internal legacy checkbox).
- Continue tightening telemetry and boundary vocabulary for snapshot runs.

**G. What can stay as-is**
- Planning UI and manifest save/load for inspection and legacy recovery.
- Legacy manifest run path as explicit fallback (not the default internal validation path).

**H. Recommended cutover strategy**
- **Active:** internal default = snapshot pipeline; legacy manifest = fallback only; no broader external rollout.

**I. Risk level**
- **High** (execution surface area), mitigated by preserved legacy path and env rollback.

**Implemented Today vs Planned / Future**
- Implemented: live snapshot pipeline entry from `main_window.py` with legacy fallback and env overrides; scoped snapshot run UI (checkbox + strict vs dependency-closure radios) and lightweight preflight copy for seed count and mode.
- Planned: production rollout policy, further manifest decoupling, unified external UX (out of scope for this internal phase).

---

### 2.2 `transfer_manifest.py`

**A. Exact current role in execution**
- Converts live planning state into runner-consumable manifest rows.

**B. Key entry points / functions / classes**
- `TransferStep`, `ProposedFolderStep`, `SimulationManifest`
- `build_simulation_manifest`
- `upconvert_manifest_v1_to_v2`

**C. Expected / emitted data shapes**
- Input: planned move dicts + `ProposedFolder` list.
- Output: manifest dict with `transfer_steps`, `proposed_folder_steps`, `execution_options`.

**D. Classification**
- **Legacy-live**

**E. What would break if cutover happened today**
- Nothing directly if retained as fallback.
- If removed prematurely, current live execution path fails.

**F. What must change before cutover**
- None required for initial controlled cutover if fallback retained.
- Eventually reduce dependence once plan-native path is primary.

**G. What can stay as-is**
- Keep for backward compatibility and rollback.

**H. Recommended cutover strategy**
- Keep stable and frozen during initial cutover phases.

**I. Risk level**
- **Medium**

**Implemented Today vs Planned / Future**
- Implemented: active live contract.
- Planned: transitional/fallback role after cutover.

---

### 2.3 `transfer_job_runner.py`

**A. Exact current role in execution**
- Mutation backend executing local and Graph operations from manifest-style payloads.

**B. Key entry points / functions / classes**
- `run_manifest_local_filesystem`
- `manifest_execution_summary`
- `RunManifestResult`, `StepRunRecord`

**C. Expected / emitted data shapes**
- Input: manifest dict with transfer/proposed steps and options.
- Output: `RunManifestResult`, step records, log file, audit events, governance report.

**D. Classification**
- **Legacy-live (also backend used by bridge)**

**E. What would break if cutover happened today**
- Bridge already depends on this backend; no immediate break.
- Semantic mismatch remains for policy-native behavior (runner is manifest-centric, not plan-native).

**F. What must change before cutover**
- Not strictly required for first internal cutover (bridge adapts).
- Required before full production cutover: tighten semantic parity (move/retention lifecycle, policy-native outcomes).

**G. What can stay as-is**
- Core execution mechanics can stay for initial cutover phases.

**H. Recommended cutover strategy**
- Use unchanged backend behind bridge first; defer deep backend refactor until after internal validation.

**I. Risk level**
- **High**

**Implemented Today vs Planned / Future**
- Implemented: single mutation engine.
- Planned: gradual semantics alignment with plan contracts.

---

### 2.4 `graph.py`

**A. Exact current role in execution**
- Provides Graph API capabilities used for resolution and execution (path lookup, copy, create folder, async monitoring).

**B. Key entry points / functions / classes**
- `get_drive_item_by_path`
- `start_drive_item_copy`
- `wait_graph_async_operation`
- `create_child_folder`

**C. Expected / emitted data shapes**
- Input: drive/item ids, relative paths, conflict behaviors.
- Output: Graph payloads and monitor results.

**D. Classification**
- **Shared**

**E. What would break if cutover happened today**
- Cutover depends on Graph client features required by resolver and bridge preflight logic.
- Missing/unstable Graph methods would break resolution and compatibility handling.

**F. What must change before cutover**
- No architectural changes required, but operational hardening and regression coverage for required Graph methods is needed.

**G. What can stay as-is**
- Existing methods and signatures used by both old/new paths.

**H. Recommended cutover strategy**
- Treat Graph interface as a stable contract; gate cutover by Graph-path scenario test pass rates.

**I. Risk level**
- **High**

**Implemented Today vs Planned / Future**
- Implemented: shared operational dependency for both architectures.
- Planned: keep stable while reducing higher-layer adapter drift.

---

### 2.5 `planned_move_graph_resolve.py`

**A. Exact current role in execution**
- Enrichment helper layer that resolves/mutates legacy move/proposed records with Graph IDs.

**B. Key entry points / functions / classes**
- `enrich_single_planned_move`
- `enrich_proposed_folder_record`
- path candidate helpers

**C. Expected / emitted data shapes**
- Input: legacy move dict / `ProposedFolder` + callback functions.
- Output: in-place enriched IDs and names.

**D. Classification**
- **Shared (legacy-native helper reused by new resolver)**

**E. What would break if cutover happened today**
- New resolver (`GraphResolveIdsService`) depends on this module via adapter translation.

**F. What must change before cutover**
- None mandatory for initial cutover.
- Desirable: canonical-native resolver implementation to reduce translation-layer fragility.

**G. What can stay as-is**
- Existing enrichment functions during transition.

**H. Recommended cutover strategy**
- Keep and wrap; replace only after canonical-native resolver parity is proven.

**I. Risk level**
- **Medium**

**Implemented Today vs Planned / Future**
- Implemented: active shared dependency.
- Planned: reduce adapter dependence.

---

### 2.6 `draft_snapshot/resolver_service.py`

**A. Exact current role in execution**
- Converts canonical mapping/proposed records into structured resolution results (`ResolvedSnapshot`).

**B. Key entry points / functions / classes**
- `GraphResolveIdsService.resolve`
- `ResolveIdsService` protocol

**C. Expected / emitted data shapes**
- Input: `CanonicalSubmittedSnapshot`, graph client adapter callbacks.
- Output: `ResolvedSnapshot` with `ResolutionSummary`, per-item statuses.

**D. Classification**
- **New-non-live**

**E. What would break if cutover happened today**
- Resolver works, but relies on legacy-shape adapter flow; drift risk remains.

**F. What must change before cutover**
- Strengthen coverage for mixed/partial resolution edge cases tied to live data quality.

**G. What can stay as-is**
- Current service contract and summary model.

**H. Recommended cutover strategy**
- Use as-is in initial internal cutover, with strict telemetry and gating on unresolved rates.

**I. Risk level**
- **Medium**

**Implemented Today vs Planned / Future**
- Implemented: functional resolver path.
- Planned: canonical-native internalization over time.

---

### 2.7 `draft_snapshot/plan_materialization.py`

**A. Exact current role in execution**
- Builds deterministic `ExecutionPlan` from `ResolvedSnapshot`, including destination chain handling and plan hash.

**B. Key entry points / functions / classes**
- `ResolvedSnapshotExecutionPlanBuilder.build_plan`
- `build_execution_plan_from_resolved`
- `build_destination_chain_steps`
- `subset_execution_plan_by_mapping_ids` (strict scoped subset)
- `subset_execution_plan_by_dependency_closure` (dependency-aware scoped subset)

**C. Expected / emitted data shapes**
- Input: `ResolvedSnapshot`, policy, optional graph client.
- Output: `ExecutionPlan` (`steps`, `summary`, `materialization`, `plan_hash`, metadata).

**D. Classification**
- **New-non-live**

**E. What would break if cutover happened today**
- Plan generation itself is functional; primary cutover risk is downstream semantic parity in runtime execution.

**F. What must change before cutover**
- Ensure all cutover-critical scenarios produce expected plan shapes consistently from live-like inputs.

**G. What can stay as-is**
- Deterministic ordering/hash/materialization summary model.

**H. Recommended cutover strategy**
- Keep implementation stable; gate cutover with scenario matrix and plan/result diffing.

**I. Risk level**
- **Medium**

**Implemented Today vs Planned / Future**
- Implemented: deterministic planner with audit-ready metadata.
- Planned: further simplification once bridge/backend become more plan-native.

---

### 2.8 `draft_snapshot/execution_bridge.py`

**A. Exact current role in execution**
- Executes `ExecutionPlan` by adapting each step to manifest-style backend calls.
- Applies compatibility logic (skip existing, merge existing, move blocking).

**B. Key entry points / functions / classes**
- `ExecutionPlanBridge.execute_plan`
- `_resolve_destination_parent`
- `_preflight_existing_destination`
- `_compatibility_gate`
- `BridgeStepState`, `ExecutionBridgeRuntimeState`

**C. Expected / emitted data shapes**
- Input: `ExecutionPlan`, graph client, dry-run flag.
- Output: runtime step states with `status`, `outcome`, `compatibility_decision`, dependency resolution details.

**D. Classification**
- **Bridge (transitional)**

**E. What would break if cutover happened today**
- Move operations are blocked by default; this may conflict with user expectations where current live path allows manifest “copy-style move intent”.
- Backend skip semantics not fully native; some outcomes rely on bridge preflight interception.

**F. What must change before cutover**
- Decide and implement definitive move lifecycle strategy for live usage (block with UX guardrails or real move semantics).
- Tighten outcome mapping consistency for all backend skip reasons.

**G. What can stay as-is**
- Parent-step dependency resolution approach and structured outcome model.

**H. Recommended cutover strategy**
- Internal-only toggle first; require explicit telemetry review for compatibility outcomes.

**I. Risk level**
- **High**

**Implemented Today vs Planned / Future**
- Implemented: bridge runtime with explicit compatibility outcomes.
- Planned: reduced compatibility shims and fuller semantic parity.

---

### 2.9 `draft_snapshot/pipeline_harness.py`

**A. Exact current role in execution**
- Non-live orchestrator for full new pipeline and run-record production.

**B. Key entry points / functions / classes**
- `run_draft_snapshot_pipeline`
- `run_pipeline_from_canonical_json_bytes`
- `run_pipeline_from_req_json_bytes`
- `run_pipeline_from_bundle_folder`
- `DraftPipelineHarnessRequest`, `DraftPipelineRunResult` (includes `snapshot_scoped_mode`, `scoped_seed_mapping_ids` for Phase 2 scoped runs)

**C. Expected / emitted data shapes**
- Input: detached snapshot + connected environment context + graph client.
- Output: structured run result containing environment/resolution/plan/bridge summaries and step outcomes.

**D. Classification**
- **New-non-live**

**E. What would break if cutover happened today**
- No live entrypoint integration; harness is service-level orchestration only.

**F. What must change before cutover**
- Promote orchestration responsibilities into a production callable path used by live UI/workers.

**G. What can stay as-is**
- Structured run-result model and phase orchestration logic.

**H. Recommended cutover strategy**
- Reuse harness orchestration as the basis for live internal mode first.

**I. Risk level**
- **Medium**

**Implemented Today vs Planned / Future**
- Implemented: full non-live orchestration; post–plan-build scoped subset routing (`strict` vs `dependency_closure`) with harness logging subphase `plan_scoped_subset`.
- Planned: integrate into live run trigger path.

---

### 2.10 `draft_snapshot/run_log.py` (execution telemetry layer)

**A. Exact current role in execution**
- Structured correlated logging for new pipeline phases and bridge events.

**B. Key entry points / functions / classes**
- `SnapshotPipelineEvent`
- `log_resolution_item_state`
- `log_plan_build_phase`
- `log_execution_bridge_step`
- `log_harness_phase`

**C. Expected / emitted data shapes**
- Input: phase metadata and correlated IDs.
- Output: structured log records with `snapshot_id`, `run_id`, optional `plan_id`, and step context.

**D. Classification**
- **New-non-live**

**E. What would break if cutover happened today**
- Cutover observability would be insufficient if these logs are not connected to live run controls/report surfaces.

**F. What must change before cutover**
- Ensure live run path emits equivalent correlated events when routed through new pipeline.

**G. What can stay as-is**
- Current event schema and phase taxonomy.

**H. Recommended cutover strategy**
- Adopt as required telemetry contract for internal cutover gate.

**I. Risk level**
- **Medium**

**Implemented Today vs Planned / Future**
- Implemented: complete new-path logging model.
- Planned: full operational integration and dashboard/report alignment.

---

## 3. Cutover Blockers

Exact code-grounded blockers preventing safe full replacement today:

1. **Snapshot path operational risk**
   - Internal default is now the snapshot pipeline; residual gaps are semantic (e.g. move policy), observability, and operator training—not absence of wiring.
2. **Move semantics unresolved for live behavior**
   - `ExecutionPlanBridge` blocks `move_item` by default.
3. **Bridge remains manifest-adapter over legacy backend**
   - Full plan-native runtime semantics are not yet backend-native.
4. **Dual protocol coupling in UI and runner**
   - UI/runner conventions around destination naming/selection logic still require careful alignment.
5. **Operational telemetry not yet unified at live cutover boundary**
   - New pipeline logs exist, but live orchestration/reporting integration is not complete.

---

## 4. Cutover Prerequisites (Dependency-Ordered)

1. **Controlled live execution switch in `main_window.py`** *(implemented)*
   - Internal default: snapshot pipeline when env is unset and the internal **Legacy manifest execution only** checkbox is **unchecked**.
   - `ENABLE_DRAFT_PIPELINE_EXECUTION=true|false` overrides the checkbox for rollback.
   - Optional automatic fallback to legacy exists in draft-pipeline finish helpers where wired; primary operator rollback remains env + legacy checkbox.
2. **Define and implement live move-policy strategy**
   - Block with explicit UX contract or implement real move lifecycle.
3. **Integrate new pipeline telemetry into live run lifecycle**
   - Ensure correlated IDs and phase events are emitted and reviewable in live runs.
4. **Run full cutover test matrix in internal mode (non-production)**
   - Validate scenario parity and rollback behavior.
5. **Operational rollout guardrails**
   - Add mode controls, success/failure thresholds, and fallback decision logic.

---

## 5. Cutover Strategy Options

### Option A: Soft parallel rollout
- **Internal variant in use:** snapshot pipeline is default; legacy is opt-in fallback (not the routine validation path).

### Option B: Hidden internal-only execution toggle
- Superseded for *direction* by Option A: env + admin checkbox still provide hidden rollback.

### Option C: Full replacement
- Not in scope; legacy manifest path remains for recovery.

### Recommended strategy
- **Internal cutover (current):** default snapshot execution for internal builds; legacy manifest + pilot UI only when explicitly recovering or debugging the old runner.

---

## 6. Test Matrix for Cutover

Minimum scenarios that must pass while snapshot pipeline is the internal default (legacy fallback remains available):

1. Canonical snapshot happy path
2. Legacy bundle input path
3. REQ input path
4. Tenant mismatch blocked before execution
5. Missing IDs / unresolved items remain non-executable and visible
6. Existing destination file -> `skipped_existing`
7. Existing destination folder -> merge/no-op success
8. `parent_step_id` dependency chain resolution
9. `move_item` compatibility behavior (blocked by default or chosen strategy)
10. Logging/audit correlation validation (`snapshot_id`/`run_id`/`plan_id`/`step_id`)
11. Failure and rollback handling (fallback to legacy path)
12. Graph/network transient behavior under retry and timeout constraints

---

## 7. End-to-End Data Model Map (Execution-Only)

Execution-relevant shape chain:

1. **UI planning dicts** (`main_window.py`)
   - Legacy/current source for live execution.
2. **Memory JSON files** (`memory.py`)
   - Current persisted planning state.
3. **Bundle / REQ payloads** (`memory.py`, `requests_store.py`)
   - Submission/transport forms.
4. **Manifest rows** (`transfer_manifest.py`)
   - Legacy live execution contract.
5. **Canonical submitted snapshot** (`draft_snapshot/contracts.py`)
   - New execution source contract.
6. **Resolved snapshot** (`draft_snapshot/resolution_contracts.py`)
   - Execution eligibility and ID-state contract.
7. **Execution plan** (`draft_snapshot/execution_plan_contracts.py`)
   - Deterministic execution description.
8. **Bridge runtime state** (`draft_snapshot/execution_bridge.py`)
   - Transitional runtime outcomes.
9. **Audit/report outputs** (`audit_log.py`, `governance_report.py`, harness run result)
   - Operational evidence and review artifacts.

Lifecycle status labels:
- Legacy/current: (1)–(4)
- Transitional/current: (5)–(9)
- Target-state direction: plan-first orchestration with reduced manifest coupling.

---

## 8. End-to-End Execution Path Comparison

### Primary internal path (snapshot pipeline)

- Source of truth: draft bundle → canonical snapshot / execution plan.
- Validation: environment + resolution gates in harness path.
- Materialization: deterministic execution plan → bridge → runner.
- Backend: bridge adapter to current runner.
- Logging/audit: phase logs + bridge step logs + harness run summary + `execution_path_selected` → `draft_pipeline`.
- **Live pilot cap dialog:** not used on this path (legacy pilot workflow is demoted).

### Fallback / recovery path (legacy manifest)

- Source of truth: UI planning + manifest JSON.
- Validation: manifest load/validate + optional preflight enrichment.
- Resolution: pre-run enrichment in UI path.
- Materialization: manifest rows.
- Backend: legacy manifest worker / `run_manifest_local_filesystem` path.
- Logging/audit: logger + audit JSONL + governance report; `execution_path_selected` → `legacy_manifest`.
- **Legacy pilot cap:** still available only when this path is selected (internal fallback toggle or `ENABLE_DRAFT_PIPELINE_EXECUTION=false`).

### Known gaps

- Move strategy not finalized for live behavior.
- Backend semantics still mediated by bridge compatibility layer for the snapshot path.

---

## 9. Final Readiness Verdict

### Verdict
- **In progress:** **controlled internal cutover** — snapshot pipeline is the default internal execution path; legacy remains for fallback.
- **Not ready** for full production / external rollout.

### Why

- Snapshot path is wired for live internal use; legacy path preserves rollback.
- Critical blockers remain at move/policy semantics and external rollout boundary.
- Continue internal validation on the snapshot path; use legacy only for recovery or parity checks.

---

## 10. Appendix: Implemented Today vs Planned / Future

### Implemented Today

- Snapshot pipeline is the **default internal** live execution path in `main_window.py` when env is unset and the internal legacy fallback checkbox is off.
- Legacy manifest execution remains available (internal admin checkbox or `ENABLE_DRAFT_PIPELINE_EXECUTION=false`).
- Structured snapshot telemetry and compatibility outcomes are present.

### Planned / Future

- External rollout policy and non-admin execution UX.
- Move semantics and retention-policy lifecycle completion.
- Unified execution telemetry/reporting across old/new paths.

---

## 11. Internal Validation Runbook (Toggle-Based Live Entry)

This runbook is for **controlled internal validation only**. External/production rollout defaults are unchanged; internally, the **snapshot pipeline is the default** execution path.

### Paths (terminology)

- **Snapshot primary:** `execution_path_selected` → `draft_pipeline`; no legacy pilot-cap dialog on Execution tab live run.
- **Legacy fallback:** `execution_path_selected` → `legacy_manifest`; legacy pilot / scoped Graph cap UI may appear on live runs.
- **Legacy pilot path:** **deprecated for routine internal validation** — use only when the legacy fallback is intentionally enabled for recovery or parity debugging.

### Enable/disable / rollback

- **Default internal (snapshot primary):** `ENABLE_DRAFT_PIPELINE_EXECUTION` **unset**, and Settings → **Legacy manifest execution only (fallback / recovery)** **unchecked** (admin-only control).
- **Force snapshot everywhere (emergency override):**
  - `$env:ENABLE_DRAFT_PIPELINE_EXECUTION = "true"`
- **Force legacy manifest everywhere (rollback):**
  - `$env:ENABLE_DRAFT_PIPELINE_EXECUTION = "false"`
- Truthy values accepted: `1`, `true`, `yes`, `on` (case-insensitive). False-y: `0`, `false`, `no`, `off`.
- **In-app internal checkbox (admin-only):** Settings → **Execution path (internal)** → **Legacy manifest execution only (fallback / recovery)**. Checked = legacy manifest path; unchecked = snapshot primary (when env is unset).

### Precedence rule (env vs in-app checkbox)

1. If `ENABLE_DRAFT_PIPELINE_EXECUTION` is explicitly set to true/false, it wins.
2. Otherwise, the admin-only **Legacy manifest execution only** checkbox applies: checked → legacy; unchecked → snapshot.
3. If env is unset and the checkbox is unchecked, **snapshot pipeline is the internal default**.

### Exact internal test sequence (Phase 3 observability)

Run the rows in order on the **same** draft scenario (or as close as your scenario allows). After **each** run, capture the fields in **“Capture after each run”** before changing mode or scenario.

| Step | Mode | Action | Expected outcome |
|------|------|--------|------------------|
| A1 | Snapshot primary (env unset, legacy checkbox **off**) | Execute (dry run or live per your test plan) | Snapshot path: `execution_path_selected` → `draft_pipeline`; bundle export; `execution_run_started` then terminal event; dialog lists `run_id`, `snapshot_id`, `plan_id`, `status=completed` on success. No legacy pilot dialog on Execution live run. |
| A2 | Legacy fallback (legacy checkbox **on**, env unset) | Same execute options | Legacy manifest path: `execution_path_selected` → `legacy_manifest`; background manifest worker; legacy pilot dialog may appear on live run; completion dialog with log/audit/report paths. |
| A3 | Snapshot primary | Induce env mismatch (e.g. wrong tenant in session vs snapshot) or run unsigned (no Graph client) | `execution_run_stopped` **or** `failure_boundary=environment` (see §15); `boundary_detail` in `environment_validation` / `graph_client_required`; **stop** if unexpected on a routine scenario. |
| A4 | Snapshot primary | Induce unresolved mappings (e.g. broken bundle / missing ids) | `execution_run_stopped`, `failure_boundary=resolution`, `boundary_detail=unresolved_or_ambiguous`. |
| A5 | Snapshot primary | Induce plan materialization failure (integration-only) | `execution_run_stopped`, `failure_boundary=plan_build`, `boundary_detail=plan_build_exception` (harness still records a structured error line; not only raw text at top level). |
| A6 | Snapshot primary | Scenario with `move_item` or compatibility block | `execution_run_failed`, `failure_boundary=bridge`, `boundary_detail` in `bridge_step_failed` / `bridge_compatibility_blocked`. |
| A7 | Snapshot primary | Rare: exception inside harness before structured return | `execution_run_failed` + `snapshot_run_internal_summary` with `final_status=runner_failed`, `failure_boundary=runner`, `boundary_detail=runner_uncaught_exception`, **`exception_type=<class name>`** (separate field). |

**Stop conditions (halt snapshot-primary validation and roll back to legacy):**

- Set `$env:ENABLE_DRAFT_PIPELINE_EXECUTION = "false"` **or** enable the internal **Legacy manifest execution only** checkbox.
- Also stop the sequence if: Step A1 (snapshot primary) does not emit `snapshot_run_internal_summary` with the same `run_id` as `execution_run_started`; any snapshot step shows `failure_boundary=runner` on a routine copy-only dry run that used to pass; correlation IDs (`run_id`, `snapshot_id`, `plan_id`) are missing from logs or UI on snapshot runs; or Step A2 (legacy fallback) regresses vs known legacy baseline.

**Capture after each run (minimum):**

- **Legacy fallback (A2):** `transfer_manifest_run_started` / completion dialog: `log_path`, job summary line, `job_id` if present, audit JSONL path, governance report path.
- **Snapshot (A1, A3–A7):** One line from `snapshot_run_internal_summary` (or its structured fields): `run_id`, `snapshot_id`, `plan_id`, `final_status`, `failure_boundary`, `boundary_detail`, `stopped_at`, `stop_reason`, and `internal_comparison` JSON (stable comparison shape, §15).
- **Both:** `execution_path_selected` line (`path`, `mode_source`).

### Validation results recording (snapshot path, Phase 5)

Use a **stable JSON record** per snapshot run so internal sessions stay comparable (see §17 for schema and module API).

**Optional JSONL file (no effect unless set):**

- Set **`OZLINK_VALIDATION_CAPTURE_JSONL`** to an absolute or relative path (e.g. `%TEMP%\ozlink_validation.jsonl`). After each **`run_snapshot`** completion (including **`runner_failed`**), one JSON object is **appended** as a single line. Failures to write log **`internal_validation_capture_write_failed`** and do **not** change execution outcomes.

**Operator fields (env, optional):**

| Variable | Purpose |
|----------|---------|
| `OZLINK_VALIDATION_SCENARIO_NAME` | Short label (e.g. `A2-baseline-copy-dry`) |
| `OZLINK_VALIDATION_OPERATOR_NOTES` | Free text (length-capped) |
| `OZLINK_VALIDATION_MATCHED_INTENT` | `yes` / `no` / truthy-falsy synonyms, or omit → `null` in JSON |
| `OZLINK_VALIDATION_DIFFERED_FROM_LEGACY` | Same as above |

**Consistency:** Set scenario name and yes/no fields **before** each run; re-set or clear between scenarios. For legacy fallback runs (checkbox **on**), keep capturing manifest artifacts as above; the JSONL hook applies only to **snapshot** orchestration.

### Controlled internal test flow (summary)

1. Start from a stable draft/planning state with known expected outcomes.
2. Run once in **snapshot primary** mode (env unset, legacy checkbox **off**) — Step A1.
3. Run the same scenario with **legacy fallback** enabled (checkbox **on**) — Step A2 — when parity or recovery testing is required.
4. Optionally run induced-failure steps A3–A7 under snapshot primary for boundary validation.
5. Compare outcomes using **internal comparison** shape (§15): same keys for snapshot vs manually filled legacy record.

### Priority scenarios (must test first)

1. Snapshot primary -> snapshot pipeline starts and returns success (see §14); no legacy pilot dialog on Execution live run.
2. Legacy fallback -> legacy path starts and completes; pilot dialog may appear on live run.
3. Snapshot primary + induced failure -> categorize via **`failure_boundary`** / **`snapshot_run_internal_summary`** (§15); do not assume automatic legacy fallback for routine validation.
4. Existing destination file -> `skipped_existing` behavior remains visible in snapshot path.
5. Existing destination folder -> merge/no-op behavior remains visible in snapshot path.
6. `move_item` remains blocked by compatibility guardrail.

### What to inspect

- Path selection logs:
  - `execution_path_selected` (`legacy_manifest` or `draft_pipeline`)
  - `execution_path_fallback` (from draft to legacy)
- New-path lifecycle logs:
  - Top-level: `execution_run_started` / `execution_run_completed` / `execution_run_stopped` / `execution_run_failed` (see §14), each with **`failure_boundary`**, **`boundary_detail`**, **`final_status`** when applicable.
  - **`snapshot_run_internal_summary`**: single structured rollup for internal validation (includes **`internal_comparison`** dict).
  - Harness: `draft_snapshot.harness.*` (summary lines include **`failure_boundary`** / **`boundary_detail`** on stop/fail).
  - Legacy draft-fallback helpers (`draft_pipeline_live_entry_*`) apply only if that code path is used; **`SnapshotPipelineRunWorker`** does not call them.
- Correlation IDs when new path is used:
  - `snapshot_id`, `run_id`, `plan_id`
- Existing legacy logs/audit/report outputs should remain unchanged.

### Pass/fail criteria

- **Pass:**
  - Legacy checkbox **on** always uses legacy manifest path.
  - Snapshot primary (env unset, checkbox **off**) successful runs complete on the snapshot pipeline with correlation IDs present (`run_id`, `snapshot_id`, `plan_id` in logs/UI).
  - Failures are categorized by **`failure_boundary`** (environment, resolution, plan_build, bridge, runner), not only by raw exception text at the top level.
- **Fail:**
  - Legacy path behavior regresses when the legacy fallback is explicitly enabled.
  - Snapshot primary failure is silent or loses correlation IDs.
  - Missing/unclear path-selection logs prevent diagnosis.

### Immediate fallback conditions

Roll back to legacy (`ENABLE_DRAFT_PIPELINE_EXECUTION=false` and/or enable **Legacy manifest execution only**) when any of the following occurs:

- unexpected compatibility blocks in routine copy-only scenarios on the snapshot path,
- repeated snapshot-path exceptions for the same workload,
- missing correlation IDs or broken run traceability on snapshot runs,
- any suspected divergence from legacy execution outcomes that blocks internal sign-off.

---

## 12. Startup Restore Stabilization (Implemented)

Scope: startup reliability only (not execution-cutover behavior).

- Added restore fail-fast mode in `main_window.py` (`restore_abort_mode`), entered automatically when restore-phase exceptions occur.
- In abort mode, the app stops cascading restore work (destination replay/materialization, deferred refresh, graph post-root refresh hooks, delta sync application).
- Autosave remains suppressed in abort mode so partial/unstable startup state does not overwrite draft memory state.
- Added explicit restore stop logging:
  - `restore_abort_mode_entered`
  - `restore_finalization_skipped`
  - `deferred_planning_refresh_skipped`
  - `root_worker_success_skipped`
  - `drive_delta_sync_skipped` / `drive_delta_sync_success_ignored`
- Fixed immediate `QTreeView`/`QTreeWidget` API mismatch hotspots in startup/delta paths by using model-safe iteration/count logic instead of direct `topLevelItemCount()` assumptions.
- Follow-up regression fix: corrected expand/collapse restore-state path (`_panel_loaded_branch_state`) to be tree-safe for model-view trees, and fixed abort-mode logging argument collision (`phase` duplicate) by logging `source_phase` metadata instead.

---

## 13. Execution Orchestrator — Phase 1 Shell (Implemented)

- **Package:** `ozlink_console/execution/` — `RunContext`, `ExecutionStopReason` (enum), `ExecutionOrchestrator`.
- **Integration:** `MainWindow._start_manifest_run_worker` after preflight success, before worker dispatch.
- **Legacy manifest mode (fallback path):** `run_manifest` creates `RunContext`, logs **`execution_run_started`** (`execution_event`, `run_id`, `execution_kind`, `source_of_truth`, …), then calls existing `_start_legacy_manifest_worker` unchanged.
- **Phase 1 snapshot stub (superseded by §14):** Previously `run_snapshot` logged **`execution_run_stopped`** with `snapshot_pipeline_not_implemented` only.

---

## 14. Execution Orchestrator — Phase 2 Snapshot Wiring (Implemented)

**Implemented**

- **`ExecutionOrchestrator.run_snapshot`** delegates to **`run_pipeline_from_bundle_folder`** (same harness as tests/integration); no duplicated import → resolve → plan → bridge logic inside the orchestrator.
- **Run alignment:** Orchestrator allocates **`run_id`** first; passes **`run_id=ctx.run_id`** into the harness so detached **`run_id`** and all `draft_snapshot` harness logs match the top-level run. **`orchestration_context`** on the harness request carries **`execution_kind`**, **`source_of_truth`**, **`started_at_iso`**, **`correlation`** (manifest path + bundle folder), and **`dry_run`** into **`draft_snapshot.harness.start`** logs.
- **Environment dict:** UI may include orchestration-only keys (e.g. `mode_source`); the orchestrator maps **`ConnectedEnvironmentContext`** from known fields only.
- **Hard stops (structured `DraftPipelineRunResult`, no silent fall-through):**
  - Environment validation failure (unchanged harness behavior).
  - **`block_on_resolution_gaps=True`** (orchestrated snapshot path only): stop at **`stopped_at=resolution`** when unresolved/ambiguous counts are non-zero (default harness/tests keep **`block_on_resolution_gaps=False`**).
  - Plan materialization exception: **`stopped_at=plan_build`** with error payload (harness catch).
- **Top-level logs (snapshot path):** **`execution_run_started`** → then one of **`execution_run_completed`**, **`execution_run_stopped`** (gated stops + mapped **`ExecutionStopReason`**), **`execution_run_failed`** (bridge outcome failure or harness exception after **`execution_run_failed`** log + re-raise). **`execution_kind`** and **`source_of_truth`** are present on started and terminal events. **Manifest path** logging and **`run_manifest`** behavior are unchanged.
- **MainWindow:** When **snapshot primary** mode is active (internal default: env unset, legacy fallback checkbox off), the app **exports a draft bundle**, starts **`SnapshotPipelineRunWorker`**, which calls **`run_snapshot`** off the UI thread. **No automatic fallback to the legacy manifest runner** on snapshot failure in Phase 2 (the separate **`_on_draft_pipeline_run_finished`** fallback helpers remain for tests / any future reuse but are not used by this path).

**Still pending (not Phase 2)**

- Runner refactor, execution bridge redesign, manifest removal, external production default switch, move-semantics changes, and broader non-admin UI for execution path selection.

---

## 15. Phase 3 — Snapshot observability & internal comparison (Implemented)

**Implemented**

- **`DraftPipelineRunResult`**: **`failure_boundary`** (`environment`, `resolution`, `plan_build`, `bridge`, or empty on success) and **`boundary_detail`** (stable tokens, e.g. `graph_client_required`, `plan_build_exception`, `bridge_compatibility_blocked`). Harness summary logs include these fields.
- **`graph_client_required`** is grouped under **`failure_boundary=environment`** with **`boundary_detail=graph_client_required`** (session / Graph client not available).
- **Uncaught exceptions** escaping the harness are **`failure_boundary=runner`** at the orchestrator layer; **`boundary_detail=runner_uncaught_exception`** with **`exception_type`** holding the **exception class name** (Phase 4; not a full traceback).
- **`SnapshotRunResultSummary`** (`ozlink_console/execution/snapshot_summary.py`): **`build_snapshot_run_result_summary`**, **`as_log_dict()`**, **`one_line_internal()`** for status labels / compact dialogs.
- **Logging:** every snapshot run that returns from the orchestrator emits **`snapshot_run_internal_summary`** with **`internal_comparison`** = **`snapshot_internal_comparison_record(summary)`** (stable JSON-serializable dict). Terminal **`execution_run_*`** events also include **`failure_boundary`**, **`boundary_detail`**, **`final_status`**.
- **MainWindow:** snapshot completion dialog includes **`run_id`**, **`snapshot_id`**, **`plan_id`**, **`status`**, and on failure **`failure_boundary`**, **`boundary_detail`**, **`stopped_at`**, **`stop_reason`**.

**Internal comparison (no diff engine)**

- Snapshot side: use the **`internal_comparison`** object from **`snapshot_run_internal_summary`** (or rebuild via **`snapshot_internal_comparison_record`** from code).
- Legacy manifest side: same **logical keys** for a manual row in notes — `execution_path`, `run_id`, `snapshot_id`, `plan_id`, `final_status`, `failure_boundary`, `boundary_detail`, `stopped_at`, `stop_reason` — with `execution_path=legacy_manifest` and `run_id`/`plan_id` often absent; fill **`job_id`**, **`log_path`**, audit/report paths from the manifest completion dialog and logs (see §11 capture list).

**Still pending (not Phase 3)**

- Automated side-by-side diff of legacy vs snapshot outcomes, production rollout defaults, runner/bridge refactors.

---

## 16. Phase 4 — Semantics hardening after internal validation (Implemented)

**Validation inputs**

- No proprietary internal run logs are in-repo; **automated harness/bridge/orchestrator tests** continued to pass. This phase applies **code-review** hardening (ambiguous tokens, bridge vs runner clarity) rather than scenario-specific execution fixes.

**Implemented**

- **`ozlink_console/execution/boundary_vocabulary.py`**: canonical **`boundary_detail`** string constants, **`CANONICAL_BOUNDARY_DETAILS`**, **`normalize_boundary_detail()`** (aliases e.g. legacy `bridge_execution` → **`bridge_outcome_unspecified`**), and **`internal_outcome_phrase()`** for operator-readable status text.
- **Harness** sets **`boundary_detail`** only via vocabulary constants (single source of truth with snapshot summary).
- **Bridge failures:** distinct **`ExecutionStopReason`** values **`bridge_step_failed`** vs **`bridge_compatibility_blocked`**; **`execution_run_failed`** / summaries use the mapped **`stop_reason`** (generic **`bridge_execution_failed`** remains for unspecified bridge outcomes).
- **Runner failures:** **`boundary_detail=runner_uncaught_exception`** plus **`exception_type`** on logs and **`internal_comparison`** (exception type is no longer overloaded into **`boundary_detail`**).
- **`one_line_internal()`** appends a short human phrase plus canonical **`detail=`** for non-success paths.

**Canonical `boundary_detail` tokens (grep reference)**

- `environment_validation`, `graph_client_required`, `plan_override_mismatch`, `unresolved_or_ambiguous`, `plan_build_exception`, `bridge_step_failed`, `bridge_compatibility_blocked`, `runner_uncaught_exception`, `bridge_outcome_unspecified`

**Still pending (not Phase 4)**

- Deeper fixes in folder/file/legacy-bundle/parent-chain/move paths **if** future internal runs surface regressions not covered by current tests; production rollout unchanged.

---

## 17. Phase 5 — Internal validation capture (Implemented)

**Purpose**

- Structured **internal validation run records** for the **snapshot** execution path only. Does **not** change pipeline behavior, defaults, or manifest routing.

**Record shape (`schema_version` = `"1"`)**

- `captured_at_utc` — UTC timestamp (ISO-8601 `Z`)
- `scenario_name`, `execution_path` (`snapshot_pipeline`), `run_id`, `snapshot_id`, `plan_id`, `final_status`, `failure_boundary`, `boundary_detail`, `stopped_at`, `stop_reason`, `exception_type`
- `operator_notes`, `matched_intent` (`yes` / `no` / JSON `null`), `differed_from_legacy` (`yes` / `no` / JSON `null`)

**Code**

- **`ozlink_console/execution/validation_capture.py`**: `InternalValidationRunRecord`, `build_internal_validation_run_record_from_snapshot_summary`, `append_internal_validation_jsonl`, `maybe_append_snapshot_validation_capture` (env-gated).
- **`ExecutionOrchestrator.run_snapshot`** calls **`maybe_append_snapshot_validation_capture`** after **`snapshot_run_internal_summary`** (success and exception paths).

**Still pending (not Phase 5)**

- Automatic capture for the **legacy manifest** path, reporting UI, or production rollout tooling.
