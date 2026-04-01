# MVP: governance, integrity, and SaaS direction

This MVP adds **verifiable local execution** and **audit/report artifacts** on top of the existing desktop console. It does **not** move bytes through Ozlink infrastructure.

## What shipped (code)

- **`ozlink_console/integrity.py`** — SHA-256 per file; full tree compare after `copytree`.
- **`ozlink_console/audit_log.py`** — append-only **JSONL** events (`job_started`, `step`, `job_finished`).
- **`ozlink_console/governance_report.py`** — machine-readable **`ozlink.job_report/v1`** JSON (per-step status + hashes when verified).
- **`transfer_job_runner.run_manifest_local_filesystem`** — optional integrity verification (default **on**); reads `manifest["execution_options"]` (`verify_integrity`, optional `audit_jsonl_path`, `job_report_path`, `job_id`).
- **`transfer_manifest.build_simulation_manifest`** — includes default `execution_options` with `verify_integrity: true`.
- **`ozlink_console/connectors/`** — `RelocatorConnector` protocol + `ConnectorKind` enum for future Dropbox / Graph / S3 executors (stubs only).

## Execution UI

When you run or dry-run a manifest from **Execution**, the app writes next to the transfer `.log` file:

- `*.audit.jsonl` — governance audit trail  
- `*_report.json` — structured job summary  

The completion dialog lists **job id**, log, audit, and report paths.

## Future SaaS (zero customer payload hosting)

- **Control plane**: tenants, SSO, policies, job definitions, **ingest of `*_report.json` / audit streams**, dashboards.  
- **Data plane**: transfers remain **client-side or provider-to-provider**; Ozlink stores **metadata and governance artifacts**, not file contents.

## Integrity semantics

- **Files**: after `copy2`, source and destination must have the **same SHA-256**.  
- **Folders**: after `copytree`, every relative file path and hash must match under source and destination roots.  
- Mismatch → step status **`failed`** with `integrity_failed` in the detail line.

Turn off verification for a manifest with:

```json
"execution_options": { "verify_integrity": false }
```
