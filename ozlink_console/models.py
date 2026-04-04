from __future__ import annotations

from dataclasses import MISSING, dataclass, asdict, field
from typing import Any

@dataclass
class AllocationRow:
    RequestId: str
    SourceItemName: str
    SourcePath: str
    SourceType: str
    RequestedDestinationPath: str
    AllocationMethod: str
    RequestedBy: str
    RequestedDate: str
    Status: str = "Pending"
    SourceDriveId: str = ""
    SourceItemId: str = ""
    DestinationDriveId: str = ""
    DestinationParentItemId: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AllocationRow":
        return cls(
            RequestId=str(data.get("RequestId", "")),
            SourceItemName=str(data.get("SourceItemName", "")),
            SourcePath=str(data.get("SourcePath", "")),
            SourceType=str(data.get("SourceType", "")),
            RequestedDestinationPath=str(data.get("RequestedDestinationPath", "")),
            AllocationMethod=str(data.get("AllocationMethod", "")),
            RequestedBy=str(data.get("RequestedBy", "")),
            RequestedDate=str(data.get("RequestedDate", "")),
            Status=str(data.get("Status", "Pending")),
            SourceDriveId=str(data.get("SourceDriveId", "")),
            SourceItemId=str(data.get("SourceItemId", "")),
            DestinationDriveId=str(data.get("DestinationDriveId", "")),
            DestinationParentItemId=str(data.get("DestinationParentItemId", "")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def is_recursive(self) -> bool:
        return "recursive" in self.AllocationMethod.lower()

@dataclass
class ProposedFolder:
    FolderName: str
    DestinationPath: str
    DestinationId: str = ""
    DestinationDriveId: str = ""
    DestinationParentItemId: str = ""
    ParentPath: str = ""
    IsSelectable: bool = True
    IsProposed: bool = True
    Status: str = "Proposed"
    RequestedBy: str = ""
    RequestedDate: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ProposedFolder":
        return cls(
            DestinationId=str(data.get("DestinationId", "")),
            FolderName=str(data.get("FolderName", "")),
            DestinationPath=str(data.get("DestinationPath", "")),
            DestinationDriveId=str(data.get("DestinationDriveId", "")),
            DestinationParentItemId=str(data.get("DestinationParentItemId", "")),
            ParentPath=str(data.get("ParentPath", "")),
            IsSelectable=bool(data.get("IsSelectable", True)),
            IsProposed=bool(data.get("IsProposed", True)),
            Status=str(data.get("Status", "Proposed")),
            RequestedBy=str(data.get("RequestedBy", "")),
            RequestedDate=str(data.get("RequestedDate", "")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

@dataclass
class SessionState:
    DraftId: str = ""
    DraftName: str = ""
    IsActiveDraft: bool = True
    CreatedUtc: str = ""
    LastWorkspace: str = "Allocation"
    LastSavedUtc: str = ""
    EnvironmentMode: str = "Client"
    SelectedSourceSite: str = ""
    SelectedSourceSiteKey: str = ""
    SelectedSourceLibrary: str = ""
    SelectedDestinationSite: str = ""
    SelectedDestinationSiteKey: str = ""
    SelectedDestinationLibrary: str = ""
    SessionFingerprint: str = ""
    SourceExpandedPaths: list[str] = field(default_factory=list)
    DestinationExpandedPaths: list[str] = field(default_factory=list)
    SourceSelectedPath: str = ""
    DestinationSelectedPath: str = ""
    SourceExpandedAll: bool = False
    DestinationExpandedAll: bool = False
    PlanningHeaderCollapsed: bool = False
    WorkspacePanelCollapsed: bool = False
    SourceTreeSnapshot: list[dict[str, Any]] = field(default_factory=list)
    DestinationTreeSnapshot: list[dict[str, Any]] = field(default_factory=list)
    # Canonical source paths (files) excluded from inherited folder allocations; persisted with draft session.
    PlanLeafExclusions: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SessionState":
        kwargs: dict[str, Any] = {}
        for field_name, field_def in cls.__dataclass_fields__.items():  # type: ignore[attr-defined]
            if field_name in data:
                kwargs[field_name] = data[field_name]
            elif field_def.default_factory is not MISSING:
                kwargs[field_name] = field_def.default_factory()
            elif field_def.default is not MISSING:
                kwargs[field_name] = field_def.default
            else:
                kwargs[field_name] = None
        if not isinstance(kwargs.get("PlanLeafExclusions"), list):
            kwargs["PlanLeafExclusions"] = []
        return cls(**kwargs)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

@dataclass
class MemoryManifest:
    DraftId: str = ""
    SessionFingerprint: str = ""
    AllocationQueueCount: int = 0
    ProposedFolderCount: int = 0
    LastGoodSaveUtc: str = ""
    SaveStatus: str = "Initialized"
    AllocationQueuePath: str = ""
    ProposedFoldersPath: str = ""
    SessionStatePath: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

@dataclass
class SubmissionBatch:
    BatchId: str = ""
    DraftId: str = ""
    DraftName: str = ""
    SubmittedUtc: str = ""
    SubmittedBy: str = ""
    SubmittedByUpn: str = ""
    TenantDomain: str = ""
    Status: str = "Submitted"
    SourceSite: str = ""
    SourceLibrary: str = ""
    DestinationSite: str = ""
    DestinationLibrary: str = ""
    PlannedMoveCount: int = 0
    ProposedFolderCount: int = 0
    NeedsReviewCount: int = 0
    ValidationWarnings: list[str] = field(default_factory=list)
    AllocationRequestIds: list[str] = field(default_factory=list)
    ProposedDestinationIds: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SubmissionBatch":
        kwargs = {}
        for field_name, field_def in cls.__dataclass_fields__.items():  # type: ignore[attr-defined]
            if field_def.default is not MISSING:
                default_value = field_def.default
            elif field_def.default_factory is not MISSING:
                default_value = field_def.default_factory()
            else:
                default_value = None
            kwargs[field_name] = data.get(field_name, default_value)
        return cls(**kwargs)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

@dataclass
class SiteCandidate:
    site_id: str
    site_name: str
    web_url: str
    site_key: str
    libraries: list[dict[str, Any]] = field(default_factory=list)
    source: str = "Site"

@dataclass
class TreeNodeData:
    item_id: str
    path: str
    name: str
    node_type: str  # file/folder
    drive_id: str
    library_name: str
    site_name: str
    has_unloaded_children: bool = False
    lazy_loaded: bool = False
    node_origin: str = "Live"
    overlay_state: str = ""
    overlay_relation_text: str = ""
    overlay_destination_path: str = ""
    overlay_via_source_path: str = ""
    proposed: bool = False
    real_name: str = ""

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        if not data["real_name"]:
            data["real_name"] = self.name
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TreeNodeData":
        return cls(
            item_id=str(data.get("item_id", data.get("ItemId", ""))),
            path=str(data.get("path", data.get("SourcePath", data.get("DestinationPath", "")))),
            name=str(data.get("name", data.get("Name", data.get("FolderName", "")))),
            node_type=str(data.get("node_type", "folder")),
            drive_id=str(data.get("drive_id", data.get("DriveId", ""))),
            library_name=str(data.get("library_name", data.get("LibraryName", ""))),
            site_name=str(data.get("site_name", data.get("SiteName", ""))),
            has_unloaded_children=bool(data.get("has_unloaded_children", data.get("HasUnloadedChildren", False))),
            lazy_loaded=bool(data.get("lazy_loaded", data.get("LazyLoaded", False))),
            node_origin=str(data.get("node_origin", data.get("NodeOrigin", "Live"))),
            overlay_state=str(data.get("overlay_state", data.get("OverlayState", ""))),
            overlay_relation_text=str(data.get("overlay_relation_text", data.get("OverlayRelationText", ""))),
            overlay_destination_path=str(data.get("overlay_destination_path", data.get("OverlayDestinationPath", ""))),
            overlay_via_source_path=str(data.get("overlay_via_source_path", data.get("OverlayViaSourcePath", ""))),
            proposed=bool(data.get("proposed", False)),
            real_name=str(data.get("real_name", data.get("RealName", ""))),
        )
