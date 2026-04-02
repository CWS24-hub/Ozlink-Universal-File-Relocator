from __future__ import annotations

import os
import re
from pathlib import Path

APP_VENDOR = "OzlinkIT"
PYTHON_APP_NAME = "OzlinkITSharePointRelocationConsole"
LEGACY_APP_NAME = "SharePointRelocationConsole"

def vendor_root() -> Path:
    return Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local"))) / APP_VENDOR

def python_primary_storage_root() -> Path:
    return vendor_root() / PYTHON_APP_NAME

def _safe_storage_segment(value: str, fallback: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return fallback
    text = re.sub(r"[^a-z0-9._@-]+", "_", text)
    text = text.strip("._-")
    return text or fallback

def user_scoped_storage_root(tenant_domain: str = "", operator_upn: str = "") -> Path:
    tenant_segment = _safe_storage_segment(tenant_domain, "unknown_tenant")
    user_segment = _safe_storage_segment(operator_upn, "anonymous")
    return python_primary_storage_root() / "Users" / tenant_segment / user_segment

def legacy_compatibility_root() -> Path:
    return vendor_root() / LEGACY_APP_NAME

def appdata_root() -> Path:
    return python_primary_storage_root()

def logs_root() -> Path:
    path = appdata_root() / "Logs"
    path.mkdir(parents=True, exist_ok=True)
    return path

def memory_root() -> Path:
    path = appdata_root() / "Memory"
    path.mkdir(parents=True, exist_ok=True)
    return path

def legacy_memory_root() -> Path:
    return legacy_compatibility_root() / "Memory"

def backups_root() -> Path:
    path = memory_root() / "Backups"
    path.mkdir(parents=True, exist_ok=True)
    return path

def quarantine_root() -> Path:
    path = memory_root() / "Quarantine"
    path.mkdir(parents=True, exist_ok=True)
    return path

def exports_root() -> Path:
    path = appdata_root() / "Exports"
    path.mkdir(parents=True, exist_ok=True)
    return path

def requests_root() -> Path:
    path = appdata_root() / "Requests"
    path.mkdir(parents=True, exist_ok=True)
    return path

def test_requests_root() -> Path:
    path = requests_root() / "Test"
    path.mkdir(parents=True, exist_ok=True)
    return path

def cache_root() -> Path:
    path = appdata_root() / "Cache"
    path.mkdir(parents=True, exist_ok=True)
    return path

def graph_cache_root() -> Path:
    path = cache_root() / "Graph"
    path.mkdir(parents=True, exist_ok=True)
    return path


def msal_token_cache_path() -> Path:
    """MSAL delegated-token cache (refresh tokens). Stored under the app cache dir (user-local)."""
    return cache_root() / "msal_token_cache.json"


def normalize_manifest_path(path: str | None) -> str:
    """Normalize a stored path string the same way as ``MainWindow.normalize_memory_path``."""
    text = str(path or "").strip().replace("/", "\\")
    text = re.sub(r"\s*\\\s*", "\\\\", text)
    text = re.sub(r"\\{2,}", "\\\\", text)
    segments = [segment.strip() for segment in text.split("\\") if segment.strip()]
    return "\\".join(segments)


def manifest_folder_copy_logical_path(destination_path: str, leaf_name: str) -> str:
    """
    Build the logical destination path for a folder-copy manifest row.

    Graph folder rows often repeat the leaf: ``destination_path`` may already be
    ``Root\\Personal`` while ``leaf_name`` is ``Personal``. Blind concatenation
    yields ``Root\\Personal\\Personal``, which breaks pilot identity matching.
    """
    dst_norm = normalize_manifest_path(destination_path).rstrip("\\")
    if not dst_norm:
        return ""
    nm = str(leaf_name or "").strip()
    if nm:
        parts = [p for p in dst_norm.split("\\") if p]
        if parts and parts[-1].lower() == nm.lower():
            return dst_norm
        return f"{dst_norm}\\{nm}"
    return dst_norm
