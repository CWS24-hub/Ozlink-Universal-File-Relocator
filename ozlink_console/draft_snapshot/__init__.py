"""Submitted snapshot pipeline foundation (models, adapters, detached load, env validation, logging)."""

from ozlink_console.draft_snapshot.adapters import (
    apply_library_relative_paths,
    from_bundle_folder,
    from_canonical_json_bytes,
    from_canonical_json_dict,
    from_req_payload_dict,
)
from ozlink_console.draft_snapshot.contracts import (
    SNAPSHOT_ENGINE_VERSION_V1,
    SNAPSHOT_SCHEMA_V1,
    CanonicalMappingItem,
    CanonicalSubmittedSnapshot,
    ProposedFolderMappingItem,
    SiteLibraryContextSnapshot,
    new_run_id,
    new_snapshot_id,
    normalize_submitted_snapshot_id,
    parse_canonical_submitted_snapshot_dict,
    validate_canonical_required_fields,
    validate_snapshot_minimal_ready,
)
from ozlink_console.draft_snapshot.detached import (
    DetachedSubmittedSnapshot,
    load_detached_auto_bytes,
    load_detached_from_bundle_folder,
    load_detached_from_canonical_json_bytes,
    load_detached_from_canonical_json_path,
    load_detached_from_req_json_bytes,
    load_detached_from_req_json_path,
)
from ozlink_console.draft_snapshot.environment import (
    ConnectedEnvironmentContext,
    EnvironmentCheckResult,
    EnvironmentValidationReport,
    validate_environment_against_snapshot,
)
from ozlink_console.draft_snapshot.path_normalization import to_library_relative_path
from ozlink_console.draft_snapshot.errors import (
    SnapshotError,
    SnapshotValidationError,
    UnsupportedSnapshotVersionError,
)
from ozlink_console.draft_snapshot.run_log import (
    SnapshotPipelineEvent,
    event_from_detached_import,
    log_environment_validation_summary,
    log_pipeline_info,
    log_pipeline_warn,
    validation_report_as_dict,
)

__all__ = [
    "LEGACY_SNAPSHOT_ADAPTER_SOURCES",
    "SNAPSHOT_ENGINE_VERSION_V1",
    "SNAPSHOT_SCHEMA_V1",
    "CanonicalMappingItem",
    "CanonicalSubmittedSnapshot",
    "ConnectedEnvironmentContext",
    "DetachedSubmittedSnapshot",
    "EnvironmentCheckResult",
    "EnvironmentValidationReport",
    "ProposedFolderMappingItem",
    "apply_library_relative_paths",
    "SiteLibraryContextSnapshot",
    "SnapshotError",
    "SnapshotPipelineEvent",
    "SnapshotValidationError",
    "UnsupportedSnapshotVersionError",
    "event_from_detached_import",
    "from_bundle_folder",
    "from_canonical_json_bytes",
    "from_canonical_json_dict",
    "from_req_payload_dict",
    "load_detached_auto_bytes",
    "load_detached_from_bundle_folder",
    "load_detached_from_canonical_json_bytes",
    "load_detached_from_canonical_json_path",
    "load_detached_from_req_json_bytes",
    "load_detached_from_req_json_path",
    "log_environment_validation_summary",
    "log_pipeline_info",
    "log_pipeline_warn",
    "new_run_id",
    "new_snapshot_id",
    "normalize_submitted_snapshot_id",
    "parse_canonical_submitted_snapshot_dict",
    "to_library_relative_path",
    "validate_canonical_required_fields",
    "validate_environment_against_snapshot",
    "validate_snapshot_minimal_ready",
    "validation_report_as_dict",
]
