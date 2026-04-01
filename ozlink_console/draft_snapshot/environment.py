"""Strict environment validation: expected snapshot context vs connected session (no execution)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from ozlink_console.draft_snapshot.contracts import CanonicalSubmittedSnapshot

Severity = Literal["error", "warning", "info"]
CheckId = Literal[
    "canonical_connected_tenant_id",
    "tenant_id",
    "tenant_domain",
    "client_key",
    "source_drive_id",
    "destination_drive_id",
    "source_site_id",
    "destination_site_id",
    "source_site_name",
    "destination_site_name",
    "source_library_name",
    "destination_library_name",
]


def _norm_domain(s: str) -> str:
    return str(s or "").strip().lower()


def _norm_id(s: str) -> str:
    return str(s or "").strip()


@dataclass
class EnvironmentCheckResult:
    check_id: CheckId | str
    passed: bool
    severity: Severity
    message: str
    expected: str = ""
    actual: str = ""
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class EnvironmentValidationReport:
    """Structured pass/fail for pre-execution gates (contract only in this phase)."""

    passed: bool
    checks: list[EnvironmentCheckResult] = field(default_factory=list)
    snapshot_id: str = ""
    run_id: str = ""

    def errors(self) -> list[EnvironmentCheckResult]:
        return [c for c in self.checks if not c.passed and c.severity == "error"]

    def warnings(self) -> list[EnvironmentCheckResult]:
        return [c for c in self.checks if not c.passed and c.severity == "warning"]


@dataclass
class ConnectedEnvironmentContext:
    """Actual connected tenant / site-library context supplied by the caller."""

    tenant_id: str = ""
    tenant_domain: str = ""
    client_key: str = ""
    source_drive_id: str = ""
    destination_drive_id: str = ""
    source_site_id: str = ""
    destination_site_id: str = ""
    source_site_name: str = ""
    destination_site_name: str = ""
    source_library_name: str = ""
    destination_library_name: str = ""


def _compare(
    check_id: CheckId,
    expected: str,
    actual: str,
    *,
    severity: Severity = "error",
    label: str | None = None,
) -> EnvironmentCheckResult:
    exp = _norm_id(expected)
    act = _norm_id(actual)
    if not exp:
        return EnvironmentCheckResult(
            check_id=check_id,
            passed=True,
            severity="info",
            message=f"{label or check_id}: snapshot did not specify a value; skipped",
            expected="",
            actual=act,
        )
    ok = exp == act
    if check_id == "tenant_domain":
        ok = _norm_domain(expected) == _norm_domain(actual)
    return EnvironmentCheckResult(
        check_id=check_id,
        passed=ok,
        severity=severity if not ok else "info",
        message=(f"{label or check_id} matches" if ok else f"{label or check_id} mismatch"),
        expected=exp,
        actual=act,
    )


LEGACY_SNAPSHOT_ADAPTER_SOURCES = frozenset({"legacy_bundle"})


def validate_environment_against_snapshot(
    snap: CanonicalSubmittedSnapshot,
    connected: ConnectedEnvironmentContext,
    *,
    run_id: str = "",
    strict_canonical_tenant: bool = True,
) -> EnvironmentValidationReport:
    """
    Compare non-empty expected fields on the snapshot to the connected context.
    Empty expected fields do not fail (informational skip).

    When ``strict_canonical_tenant`` is True (default), a **non-legacy** snapshot that includes
    ``tenant.tenant_id`` requires the connected session to supply ``tenant_id`` (hard gate for
    canonical / REQ-style submissions). Legacy bundle imports keep prior skip-if-empty behavior
    on the snapshot side.
    """
    checks: list[EnvironmentCheckResult] = []
    is_legacy = snap.adapter_source in LEGACY_SNAPSHOT_ADAPTER_SOURCES
    snap_tid = _norm_id(snap.tenant.tenant_id)
    conn_tid = _norm_id(connected.tenant_id)

    if strict_canonical_tenant and not is_legacy and snap_tid:
        if not conn_tid:
            checks.append(
                EnvironmentCheckResult(
                    check_id="canonical_connected_tenant_id",
                    passed=False,
                    severity="error",
                    message="Canonical snapshot specifies tenant_id; connected session must report tenant_id",
                    expected=snap_tid,
                    actual="",
                )
            )
        else:
            checks.append(_compare("tenant_id", snap_tid, conn_tid, label="Tenant id"))
    else:
        checks.append(_compare("tenant_id", snap.tenant.tenant_id, connected.tenant_id, label="Tenant id"))
    checks.append(_compare("tenant_domain", snap.tenant.tenant_domain, connected.tenant_domain, label="Tenant domain"))
    checks.append(_compare("client_key", snap.tenant.client_key, connected.client_key, label="Client key"))
    checks.append(
        _compare(
            "source_drive_id",
            snap.source.site_library.library_drive_id,
            connected.source_drive_id,
            label="Source library drive id",
        )
    )
    checks.append(
        _compare(
            "destination_drive_id",
            snap.destination.site_library.library_drive_id,
            connected.destination_drive_id,
            label="Destination library drive id",
        )
    )
    checks.append(
        _compare(
            "source_site_id",
            snap.source.site_library.site_id,
            connected.source_site_id,
            label="Source site id",
            severity="warning",
        )
    )
    checks.append(
        _compare(
            "destination_site_id",
            snap.destination.site_library.site_id,
            connected.destination_site_id,
            label="Destination site id",
            severity="warning",
        )
    )
    checks.append(
        _compare(
            "source_site_name",
            snap.source.site_library.site_name,
            connected.source_site_name,
            label="Source site name",
            severity="warning",
        )
    )
    checks.append(
        _compare(
            "destination_site_name",
            snap.destination.site_library.site_name,
            connected.destination_site_name,
            label="Destination site name",
            severity="warning",
        )
    )
    checks.append(
        _compare(
            "source_library_name",
            snap.source.site_library.library_name,
            connected.source_library_name,
            label="Source library name",
            severity="warning",
        )
    )
    checks.append(
        _compare(
            "destination_library_name",
            snap.destination.site_library.library_name,
            connected.destination_library_name,
            label="Destination library name",
            severity="warning",
        )
    )
    failed_error = any(not c.passed and c.severity == "error" for c in checks)
    return EnvironmentValidationReport(
        passed=not failed_error,
        checks=checks,
        snapshot_id=snap.snapshot_id,
        run_id=run_id,
    )
