import hashlib
import json
import os
import webbrowser
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import msal
import pyperclip
import requests

from .logger import log_info, log_trace, log_warn
from .paths import graph_cache_root


def _graph_url_excerpt(url: str, max_len: int = 200) -> str:
    s = str(url or "")
    if "graph.microsoft.com" in s:
        s = s.split("graph.microsoft.com", 1)[-1]
    if "?" in s:
        s = s.split("?", 1)[0]
    return s[:max_len]


AUTH_CONFIG = {
    "client_id": "2202c796-0d9d-4e3d-af02-640a3aed518a",
    "tenant_id": "62f50435-6db9-4e94-8739-d930d6d945a7",
    "authority": "https://login.microsoftonline.com/62f50435-6db9-4e94-8739-d930d6d945a7",
    "scope": [
        "User.Read",
        "Sites.Read.All",
        "Files.Read.All",
        "Group.Read.All",
        "Directory.Read.All",
    ],
    "graph_base": "https://graph.microsoft.com/v1.0",
}


ADMIN_DIRECTORY_ROLE_NAMES = {
    "global administrator",
    "sharepoint administrator",
    "exchange administrator",
    "user administrator",
    "application administrator",
    "cloud application administrator",
    "privileged role administrator",
    "security administrator",
    "teams administrator",
    "billing administrator",
}


class GraphClient:
    GRAPH_CACHE_TTL_HOURS = 24

    def _persistent_cache_ttl(self) -> timedelta:
        raw = os.environ.get("OZLINK_GRAPH_CACHE_TTL_HOURS", "").strip()
        if raw:
            try:
                hours = float(raw)
                if hours > 0:
                    return timedelta(hours=hours)
            except ValueError:
                pass
        return timedelta(hours=float(self.GRAPH_CACHE_TTL_HOURS))

    def __init__(self):
        self.token: Optional[str] = None
        self.device_flow: Optional[Dict[str, Any]] = None
        self.profile: Optional[Dict[str, Any]] = None
        self._drive_children_cache: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
        self._graph_cache_root: Path = graph_cache_root()
        self.session_context: Dict[str, Any] = {
            "connected": False,
            "user_role": "user",
            "operator_display_name": "",
            "operator_upn": "",
            "tenant_domain": "",
        }

        self.app = None

    def _ensure_app(self):
        if self.app is None:
            self.app = msal.PublicClientApplication(
                AUTH_CONFIG["client_id"],
                authority=AUTH_CONFIG["authority"],
            )
        return self.app

    # -------------------------------------------------------------------------
    # Authentication
    # -------------------------------------------------------------------------
    def connect_device_flow(self) -> Dict[str, str]:
        flow = self._ensure_app().initiate_device_flow(scopes=AUTH_CONFIG["scope"])

        if "user_code" not in flow:
            raise RuntimeError("Failed to start Microsoft 365 device sign-in flow.")

        self.device_flow = flow

        code = flow["user_code"]
        url = flow["verification_uri"]
        message = flow.get("message", "")

        pyperclip.copy(code)

        log_trace(
            "graph_auth",
            "device_flow_ready",
            verification_host_excerpt=str(url or "")[:120],
            has_user_code=bool(code),
        )
        return {
            "code": code,
            "url": url,
            "message": message,
        }

    def open_device_login_page(self) -> None:
        if not self.device_flow:
            raise RuntimeError("Device flow has not been initialized.")
        webbrowser.open(self.device_flow["verification_uri"])

    def acquire_token(self) -> Dict[str, Any]:
        if not self.device_flow:
            raise RuntimeError("No active device flow. Start device login first.")

        result = self._ensure_app().acquire_token_by_device_flow(self.device_flow)

        if "access_token" not in result:
            raise RuntimeError(result.get("error_description", "Authentication failed."))

        self.token = result["access_token"]
        self.session_context["connected"] = True
        log_trace("graph_auth", "token_acquired_device_flow", connected=True)
        return result

    def _get_cached_account(self) -> Optional[Dict[str, Any]]:
        try:
            accounts = self._ensure_app().get_accounts()
        except Exception:
            return None
        if not accounts:
            return None
        return accounts[0]

    def _try_acquire_token_silent(self, *, force_refresh: bool = False) -> bool:
        account = self._get_cached_account()
        if not account:
            return False
        try:
            result = self._ensure_app().acquire_token_silent(
                AUTH_CONFIG["scope"],
                account=account,
                force_refresh=force_refresh,
            )
        except TypeError:
            result = self._ensure_app().acquire_token_silent(
                AUTH_CONFIG["scope"],
                account=account,
            )
        except Exception:
            return False

        if not isinstance(result, dict) or "access_token" not in result:
            return False

        self.token = result["access_token"]
        self.session_context["connected"] = True
        return True

    def refresh_access_token_silently(self, *, force_refresh: bool = False) -> bool:
        return self._try_acquire_token_silent(force_refresh=force_refresh)

    def disconnect(self) -> None:
        log_trace("graph_auth", "disconnect")
        self.token = None
        self.device_flow = None
        self.profile = None
        self._drive_children_cache.clear()
        self.session_context = {
            "connected": False,
            "user_role": "user",
            "operator_display_name": "",
            "operator_upn": "",
            "tenant_domain": "",
        }

    # -------------------------------------------------------------------------
    # Core HTTP helpers
    # -------------------------------------------------------------------------
    def get_headers(self) -> Dict[str, str]:
        if not self.token and not self._try_acquire_token_silent():
            raise RuntimeError("No access token is available. Connect to Microsoft 365 first.")
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        timeout: int = 60,
        stream: bool = False,
    ) -> requests.Response:
        retried_401 = False
        response = requests.request(
            method,
            url,
            headers=self.get_headers(),
            params=params,
            json=json_body,
            timeout=timeout,
            stream=stream,
        )
        if response.status_code == 401 and self._try_acquire_token_silent(force_refresh=True):
            retried_401 = True
            response = requests.request(
                method,
                url,
                headers=self.get_headers(),
                params=params,
                json=json_body,
                timeout=timeout,
                stream=stream,
            )
        response.raise_for_status()
        log_trace(
            "graph_http",
            "response_ok",
            method=method,
            path_excerpt=_graph_url_excerpt(url),
            status_code=response.status_code,
            retried_after_401=retried_401,
            stream=stream,
        )
        return response

    def get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        response = self._request("GET", url, params=params, timeout=60)
        return response.json()

    def get_bytes(self, url: str, *, params: Optional[Dict[str, Any]] = None, max_bytes: int = 262144) -> bytes:
        response = self._request("GET", url, params=params, timeout=60, stream=True)
        chunks: List[bytes] = []
        total = 0
        for chunk in response.iter_content(chunk_size=16384):
            if not chunk:
                continue
            remaining = max_bytes - total
            if remaining <= 0:
                break
            if len(chunk) > remaining:
                chunk = chunk[:remaining]
            chunks.append(chunk)
            total += len(chunk)
            if total >= max_bytes:
                break
        return b"".join(chunks)

    def get_paged(self, url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        next_url = url
        next_params = params

        while next_url:
            response = self._request("GET", next_url, params=next_params, timeout=60)
            payload = response.json()

            items.extend(payload.get("value", []))
            next_url = payload.get("@odata.nextLink")
            next_params = None

        return items

    def _graph_children_cache_path(self, drive_id: str, item_id: str) -> Path:
        cache_name = hashlib.sha1(f"{drive_id}:{item_id}".encode("utf-8")).hexdigest()
        return self._graph_cache_root / f"{cache_name}.json"

    def _load_persistent_children_cache(self, drive_id: str, item_id: str) -> Optional[List[Dict[str, Any]]]:
        cache_path = self._graph_children_cache_path(drive_id, item_id)
        if not cache_path.exists():
            return None

        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            return None

        saved_utc = str(payload.get("saved_utc", "")).strip()
        if not saved_utc:
            return None

        try:
            saved_at = datetime.fromisoformat(saved_utc.replace("Z", "+00:00"))
            if saved_at.tzinfo is None or saved_at.tzinfo.utcoffset(saved_at) is None:
                saved_at = saved_at.replace(tzinfo=timezone.utc)
            else:
                saved_at = saved_at.astimezone(timezone.utc)
        except Exception:
            return None

        if datetime.now(timezone.utc) - saved_at > self._persistent_cache_ttl():
            return None

        items = payload.get("items", [])
        if not isinstance(items, list):
            return None
        return items

    def _save_persistent_children_cache(self, drive_id: str, item_id: str, items: List[Dict[str, Any]]) -> None:
        cache_path = self._graph_children_cache_path(drive_id, item_id)
        payload = {
            "drive_id": drive_id,
            "item_id": item_id,
            "saved_utc": datetime.now(timezone.utc).isoformat(),
            "items": items,
        }
        try:
            cache_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
        except Exception:
            return

    def clear_drive_children_cache(self, drive_id: str) -> None:
        drive_id = str(drive_id or "").strip()
        if not drive_id:
            return

        stale_keys = [cache_key for cache_key in self._drive_children_cache if cache_key[0] == drive_id]
        for cache_key in stale_keys:
            self._drive_children_cache.pop(cache_key, None)

        disk_removed = 0
        for cache_path in self._graph_cache_root.glob("*.json"):
            try:
                if cache_path.name.startswith("drive_delta_"):
                    continue
                payload = json.loads(cache_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if str(payload.get("drive_id", "")).strip() != drive_id:
                continue
            try:
                cache_path.unlink()
                disk_removed += 1
            except Exception:
                continue
        log_trace(
            "graph_cache",
            "clear_drive_children_cache",
            drive_id_suffix=drive_id[-16:] if len(drive_id) > 16 else drive_id,
            in_memory_keys_cleared=len(stale_keys),
            disk_cache_files_removed=disk_removed,
        )

    def clear_all_children_cache(self) -> None:
        mem_count = len(self._drive_children_cache)
        self._drive_children_cache.clear()
        disk_removed = 0
        for cache_path in self._graph_cache_root.glob("*.json"):
            try:
                if cache_path.name.startswith("drive_delta_"):
                    continue
                cache_path.unlink()
                disk_removed += 1
            except Exception:
                continue
        log_trace(
            "graph_cache",
            "clear_all_children_cache",
            prior_in_memory_keys=mem_count,
            disk_cache_files_removed=disk_removed,
        )

    def _drive_delta_state_path(self, drive_id: str) -> Path:
        digest = hashlib.sha1(f"{drive_id}".encode("utf-8")).hexdigest()
        return self._graph_cache_root / f"drive_delta_{digest}.json"

    def invalidate_drive_folder_children_cache(self, drive_id: str, item_id: str) -> None:
        drive_id = str(drive_id or "").strip()
        item_id = str(item_id or "").strip()
        if not drive_id or not item_id:
            return
        self._drive_children_cache.pop((drive_id, item_id), None)
        path = self._graph_children_cache_path(drive_id, item_id)
        try:
            if path.exists():
                path.unlink()
        except OSError:
            pass

    def sync_drive_children_delta(
        self,
        drive_id: str,
        *,
        allow_initial_bootstrap: bool = True,
    ) -> Dict[str, Any]:
        """Synchronize local per-folder child caches with Microsoft Graph drive delta.

        First successful run pages ``/drives/{{id}}/root/delta`` until Graph returns ``@odata.deltaLink``
        (bootstrap). That pass can be long on huge libraries; it does not invalidate caches.

        Later runs follow ``deltaLink`` and **invalidate** only folder cache entries that changed.

        ``allow_initial_bootstrap``: when False and no saved token, the call returns ``skipped`` (no network).

        Environment overrides:
        - ``OZLINK_GRAPH_DELTA_DISABLE_BOOTSTRAP=1`` — treated as ``allow_initial_bootstrap=False`` at call site.
        - ``OZLINK_GRAPH_DELTA_MAX_PAGES`` — optional safety cap on HTTP pages (0 or unset = unlimited).
        """
        drive_id = str(drive_id or "").strip()
        if not drive_id:
            return {"ok": False, "reason": "no_drive_id"}
        if not self.token:
            return {"ok": False, "reason": "not_connected"}

        state_path = self._drive_delta_state_path(drive_id)
        delta_link = ""
        if state_path.exists():
            try:
                st = json.loads(state_path.read_text(encoding="utf-8"))
                delta_link = str(st.get("delta_link") or "").strip()
            except Exception:
                delta_link = ""

        if not delta_link and not allow_initial_bootstrap:
            return {
                "ok": True,
                "skipped": True,
                "reason": "no_delta_token_bootstrap_disabled",
            }

        initial_run = not bool(delta_link)
        url = delta_link or f"{AUTH_CONFIG['graph_base']}/drives/{drive_id}/root/delta"
        params = None
        invalidated: set[tuple[str, str]] = set()
        total_items = 0
        pages = 0
        new_delta = None

        max_pages = 0
        raw_max = os.environ.get("OZLINK_GRAPH_DELTA_MAX_PAGES", "").strip()
        if raw_max.isdigit():
            max_pages = max(0, int(raw_max))

        try:
            while url:
                pages += 1
                if max_pages and pages > max_pages:
                    log_warn(
                        "graph_delta",
                        "sync_drive_children_delta_aborted_max_pages",
                        drive_id_suffix=drive_id[-16:] if len(drive_id) > 16 else drive_id,
                        max_pages=max_pages,
                    )
                    return {
                        "ok": False,
                        "reason": "max_pages_exceeded",
                        "pages": pages,
                        "items_seen": total_items,
                        "invalidated_folders": len(invalidated),
                        "invalidated_entries": [
                            {"drive_id": d, "item_id": i} for d, i in sorted(invalidated)
                        ],
                    }

                response = self._request("GET", url, params=params, timeout=120)
                payload = response.json()
                chunk = payload.get("value") or []
                if not isinstance(chunk, list):
                    chunk = []
                total_items += len(chunk)

                if not initial_run:
                    for item in chunk:
                        if not isinstance(item, dict):
                            continue
                        iid = str(item.get("id") or "").strip()
                        parent = item.get("parentReference") or {}
                        pid = str(parent.get("id") or "").strip()
                        if item.get("@removed"):
                            if iid:
                                self.invalidate_drive_folder_children_cache(drive_id, iid)
                                invalidated.add((drive_id, iid))
                            if pid:
                                self.invalidate_drive_folder_children_cache(drive_id, pid)
                                invalidated.add((drive_id, pid))
                        else:
                            if pid:
                                self.invalidate_drive_folder_children_cache(drive_id, pid)
                                invalidated.add((drive_id, pid))
                            if item.get("folder") and iid:
                                self.invalidate_drive_folder_children_cache(drive_id, iid)
                                invalidated.add((drive_id, iid))

                new_delta = payload.get("@odata.deltaLink") or new_delta
                url = payload.get("@odata.nextLink")
                params = None
        except requests.RequestException as exc:
            log_warn(
                "graph_delta",
                "sync_drive_children_delta_http_failed",
                drive_id_suffix=drive_id[-16:] if len(drive_id) > 16 else drive_id,
                error=str(exc)[:500],
            )
            return {
                "ok": False,
                "reason": "request_failed",
                "error": str(exc),
                "pages": pages,
                "items_seen": total_items,
                "invalidated_folders": len(invalidated),
            }
        except Exception as exc:
            log_warn(
                "graph_delta",
                "sync_drive_children_delta_failed",
                drive_id_suffix=drive_id[-16:] if len(drive_id) > 16 else drive_id,
                error=str(exc)[:500],
            )
            return {
                "ok": False,
                "reason": "unexpected_error",
                "error": str(exc),
                "pages": pages,
                "items_seen": total_items,
                "invalidated_folders": len(invalidated),
            }

        if new_delta:
            try:
                state_path.write_text(
                    json.dumps(
                        {
                            "drive_id": drive_id,
                            "delta_link": new_delta,
                            "saved_utc": datetime.now(timezone.utc).isoformat(),
                        },
                        ensure_ascii=True,
                    ),
                    encoding="utf-8",
                )
            except OSError:
                pass
        elif not initial_run:
            log_warn(
                "graph_delta",
                "sync_drive_children_delta_missing_delta_link",
                drive_id_suffix=drive_id[-16:] if len(drive_id) > 16 else drive_id,
                pages=pages,
            )

        log_trace(
            "graph_delta",
            "sync_drive_children_delta_done",
            drive_id_suffix=drive_id[-16:] if len(drive_id) > 16 else drive_id,
            pages=pages,
            items_seen=total_items,
            invalidated=len(invalidated),
            initial_token_run=initial_run,
        )
        if initial_run and new_delta:
            log_info(
                "graph_delta_bootstrap_complete",
                drive_id_suffix=drive_id[-16:] if len(drive_id) > 16 else drive_id,
                pages=pages,
                items_seen=total_items,
            )

        invalidated_entries = [{"drive_id": d, "item_id": i} for d, i in sorted(invalidated)]
        return {
            "ok": True,
            "skipped": False,
            "initial_token_run": initial_run,
            "pages": pages,
            "items_seen": total_items,
            "invalidated_folders": len(invalidated),
            "invalidated_entries": invalidated_entries,
        }

    def has_cached_drive_root_children(self, drive_id: str) -> bool:
        cache_key = (drive_id, "__root__")
        cached_items = self._drive_children_cache.get(cache_key)
        if cached_items is not None:
            return True
        return self._load_persistent_children_cache(drive_id, "__root__") is not None

    def has_cached_drive_item_children(self, drive_id: str, item_id: str) -> bool:
        cache_key = (drive_id, item_id)
        cached_items = self._drive_children_cache.get(cache_key)
        if cached_items is not None:
            return True
        return self._load_persistent_children_cache(drive_id, item_id) is not None

    # -------------------------------------------------------------------------
    # Identity / session context
    # -------------------------------------------------------------------------
    def get_profile(self) -> Dict[str, Any]:
        profile = self.get(f"{AUTH_CONFIG['graph_base']}/me")
        self.profile = profile

        display_name = profile.get("displayName", "")
        upn = profile.get("userPrincipalName", "")
        tenant_domain = upn.split("@", 1)[1] if "@" in upn else ""

        self.session_context["operator_display_name"] = display_name
        self.session_context["operator_upn"] = upn
        self.session_context["tenant_domain"] = tenant_domain

        return profile

    def get_my_member_groups(self) -> List[str]:
        url = f"{AUTH_CONFIG['graph_base']}/me/getMemberGroups"
        payload = {"securityEnabledOnly": False}
        response = self._request("POST", url, json_body=payload, timeout=60)
        return response.json().get("value", [])

    def get_my_directory_roles(self) -> List[Dict[str, Any]]:
        return self.get_paged(
            f"{AUTH_CONFIG['graph_base']}/me/memberOf/microsoft.graph.directoryRole",
            params={"$select": "id,displayName,roleTemplateId"},
        )

    def determine_user_role(self) -> str:
        """
        Resolve the signed-in operator role from Microsoft 365 directory role membership.
        Fail closed to standard user if role lookup is unavailable for the delegated token.
        """
        role = "user"

        try:
            directory_roles = self.get_my_directory_roles()
            normalized_role_names = {
                str(item.get("displayName", "")).strip().lower()
                for item in directory_roles
                if item.get("displayName")
            }

            if normalized_role_names & ADMIN_DIRECTORY_ROLE_NAMES:
                role = "admin"
            elif directory_roles:
                role = "admin"
        except requests.HTTPError:
            role = "user"

        self.session_context["user_role"] = role
        return role

    def build_session_context(self) -> Dict[str, Any]:
        profile = self.get_profile()
        role = self.determine_user_role()

        ctx = {
            "connected": True,
            "profile": profile,
            "operator_display_name": self.session_context.get("operator_display_name", ""),
            "operator_upn": self.session_context.get("operator_upn", ""),
            "tenant_domain": self.session_context.get("tenant_domain", ""),
            "user_role": role,
        }
        upn = ""
        if isinstance(profile, dict):
            upn = str(profile.get("userPrincipalName") or profile.get("mail") or "")[:120]
        log_trace("graph", "build_session_context", user_role=role, profile_upn_excerpt=upn)
        return ctx

    # -------------------------------------------------------------------------
    # SharePoint discovery - foundation methods for upcoming Planning Workspace parity
    # -------------------------------------------------------------------------
    def list_sites(self, search: str = "*") -> List[Dict[str, Any]]:
        params = {"search": search}
        return self.get_paged(f"{AUTH_CONFIG['graph_base']}/sites", params=params)

    def list_site_drives(self, site_id: str) -> List[Dict[str, Any]]:
        return self.get_paged(f"{AUTH_CONFIG['graph_base']}/sites/{site_id}/drives")

    def list_drive_root_children(self, drive_id: str) -> List[Dict[str, Any]]:
        cache_key = (drive_id, "__root__")
        if cache_key not in self._drive_children_cache:
            cached_items = self._load_persistent_children_cache(drive_id, "__root__")
            if cached_items is None:
                cached_items = self.get_paged(
                    f"{AUTH_CONFIG['graph_base']}/drives/{drive_id}/root/children"
                )
                self._save_persistent_children_cache(drive_id, "__root__", cached_items)
            self._drive_children_cache[cache_key] = cached_items
        return list(self._drive_children_cache[cache_key])

    def list_drive_root_children_cached_only(self, drive_id: str) -> List[Dict[str, Any]]:
        cache_key = (drive_id, "__root__")
        cached_items = self._drive_children_cache.get(cache_key)
        if cached_items is None:
            cached_items = self._load_persistent_children_cache(drive_id, "__root__")
            if cached_items is None:
                return []
            self._drive_children_cache[cache_key] = cached_items
        return list(cached_items)

    def list_drive_item_children(self, drive_id: str, item_id: str) -> List[Dict[str, Any]]:
        cache_key = (drive_id, item_id)
        if cache_key not in self._drive_children_cache:
            cached_items = self._load_persistent_children_cache(drive_id, item_id)
            if cached_items is None:
                cached_items = self.get_paged(
                    f"{AUTH_CONFIG['graph_base']}/drives/{drive_id}/items/{item_id}/children"
                )
                self._save_persistent_children_cache(drive_id, item_id, cached_items)
            self._drive_children_cache[cache_key] = cached_items
        return list(self._drive_children_cache[cache_key])

    def list_drive_item_children_cached_only(self, drive_id: str, item_id: str) -> List[Dict[str, Any]]:
        cache_key = (drive_id, item_id)
        cached_items = self._drive_children_cache.get(cache_key)
        if cached_items is None:
            cached_items = self._load_persistent_children_cache(drive_id, item_id)
            if cached_items is None:
                return []
            self._drive_children_cache[cache_key] = cached_items
        return list(cached_items)

    def get_drive_item(self, drive_id: str, item_id: str) -> Dict[str, Any]:
        return self.get(f"{AUTH_CONFIG['graph_base']}/drives/{drive_id}/items/{item_id}")

    def get_drive_item_by_path(self, drive_id: str, relative_path: str) -> Optional[Dict[str, Any]]:
        """
        Resolve a path relative to the document library root (e.g. FTBMRoot/Admin/Folder) to a driveItem.
        Returns None on 404. Used when allocation payloads store SourcePath but omit Graph ids.
        """
        drive_id = str(drive_id or "").strip()
        if not drive_id:
            return None
        normalized = str(relative_path or "").replace("\\", "/").strip("/")
        if not normalized:
            return None
        encoded = "/".join(quote(segment, safe="") for segment in normalized.split("/") if segment)
        url = f"{AUTH_CONFIG['graph_base']}/drives/{drive_id}/root:/{encoded}"
        try:
            payload = self.get(url)
            log_trace(
                "graph",
                "get_drive_item_by_path_ok",
                drive_id_suffix=drive_id[-16:] if len(drive_id) > 16 else drive_id,
                path_excerpt=encoded[:180],
            )
            return payload
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                log_trace(
                    "graph",
                    "get_drive_item_by_path_404",
                    drive_id_suffix=drive_id[-16:] if len(drive_id) > 16 else drive_id,
                    path_excerpt=encoded[:180],
                )
                return None
            raise

    def count_drive_items_recursive_split(self, drive_id: str) -> tuple[int, int]:
        """Return ``(file_count, folder_count)`` under the library root (recursive, paged).

        Each file row counts toward files; each folder row counts toward folders. The library root
        item itself is not included. ``file_count + folder_count`` matches ``count_drive_items_recursive``.
        """
        files = 0
        folders = 0
        stack: List[Optional[str]] = [None]

        while stack:
            item_id = stack.pop()
            children = (
                self.list_drive_root_children(drive_id)
                if item_id is None
                else self.list_drive_item_children(drive_id, item_id)
            )

            for child in children:
                if "folder" in child:
                    folders += 1
                    child_id = child.get("id")
                    if child_id:
                        stack.append(child_id)
                else:
                    files += 1

        return files, folders

    def count_drive_items_recursive(self, drive_id: str) -> int:
        """Total items (files + folders) under the library root; see ``count_drive_items_recursive_split``."""
        files, folders = self.count_drive_items_recursive_split(drive_id)
        return files + folders

    # -------------------------------------------------------------------------
    # Normalizers for UI parity
    # -------------------------------------------------------------------------
    @staticmethod
    def normalize_site(site: Dict[str, Any]) -> Dict[str, Any]:
        web_url = site.get("webUrl", "")
        display_name = site.get("displayName") or site.get("name") or web_url or "Unnamed Site"

        return {
            "id": site.get("id", ""),
            "name": display_name,
            "web_url": web_url,
            "raw": site,
        }

    @staticmethod
    def normalize_drive(drive: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": drive.get("id", ""),
            "name": drive.get("name", "Unnamed Library"),
            "web_url": drive.get("webUrl", ""),
            "drive_type": drive.get("driveType", ""),
            "raw": drive,
        }

    @staticmethod
    def is_usable_document_library(drive: Dict[str, Any]) -> bool:
        return bool(drive.get("id")) and drive.get("driveType") == "documentLibrary"

    @staticmethod
    def _extract_graph_parent_path(item: Dict[str, Any]) -> str:
        raw_path = item.get("parentReference", {}).get("path", "")
        if "root:" in raw_path:
            raw_path = raw_path.split("root:", 1)[1]

        raw_path = str(raw_path or "").strip()
        if not raw_path:
            return "/"

        if not raw_path.startswith("/"):
            raw_path = f"/{raw_path}"

        return raw_path.rstrip("/") or "/"

    @classmethod
    def build_item_path(
        cls,
        item: Dict[str, Any],
        parent_item_path: str = "",
    ) -> str:
        item_name = str(item.get("name", "")).strip()
        base_path = parent_item_path.strip() if parent_item_path else cls._extract_graph_parent_path(item)

        if not base_path:
            base_path = "/"
        if not base_path.startswith("/"):
            base_path = f"/{base_path}"

        base_path = base_path.rstrip("/") or "/"

        if not item_name:
            return base_path
        if base_path == "/":
            return f"/{item_name}"

        return f"{base_path}/{item_name}"

    @staticmethod
    def build_display_path(site_name: str, library_name: str, item_path: str) -> str:
        parts = [part for part in [site_name, library_name] if part]
        prefix = " / ".join(parts)
        if not prefix:
            return item_path or "/"
        if item_path in ("", "/"):
            return prefix
        return f"{prefix}{item_path}"

    @classmethod
    def normalize_drive_item(
        cls,
        item: Dict[str, Any],
        *,
        drive_id: str = "",
        site_id: str = "",
        site_name: str = "",
        library_id: str = "",
        library_name: str = "",
        tree_role: str = "",
        parent_item_id: str = "",
        parent_item_path: str = "",
    ) -> Dict[str, Any]:
        is_folder = "folder" in item
        child_count = item.get("folder", {}).get("childCount", 0) if is_folder else 0
        normalized_parent_item_id = parent_item_id or item.get("parentReference", {}).get("id", "")
        item_path = cls.build_item_path(item, parent_item_path=parent_item_path)
        display_path = cls.build_display_path(site_name, library_name, item_path)

        return {
            "id": item.get("id", ""),
            "name": item.get("name", "Unnamed Item"),
            "is_folder": is_folder,
            "child_count": child_count,
            "web_url": item.get("webUrl", ""),
            "drive_id": drive_id or item.get("parentReference", {}).get("driveId", ""),
            "site_id": site_id,
            "site_name": site_name,
            "library_id": library_id or drive_id or item.get("parentReference", {}).get("driveId", ""),
            "library_name": library_name,
            "tree_role": tree_role,
            "parent_item_id": normalized_parent_item_id,
            "item_path": item_path,
            "display_path": display_path,
            "size": item.get("size", 0),
            "raw": item,
        }

    # -------------------------------------------------------------------------
    # Composite helpers - these are what the UI should use next
    # -------------------------------------------------------------------------
    def discover_sites_with_libraries(self) -> List[Dict[str, Any]]:
        log_trace("graph", "discover_sites_with_libraries_start")
        sites = self.list_sites()
        results: List[Dict[str, Any]] = []

        for site in sites:
            normalized_site = self.normalize_site(site)
            site_id = normalized_site["id"]
            if not site_id:
                continue

            try:
                drives = self.list_site_drives(site_id)
            except Exception:
                continue

            normalized_drives = [
                self.normalize_drive(drive)
                for drive in drives
                if self.is_usable_document_library(drive)
            ]
            if not normalized_drives:
                continue

            normalized_site["libraries"] = normalized_drives
            results.append(normalized_site)

        log_trace(
            "graph",
            "discover_sites_with_libraries_done",
            raw_site_count=len(sites),
            sites_with_libraries=len(results),
        )
        return results

    def list_drive_root_items_normalized(
        self,
        drive_id: str,
        *,
        site_id: str = "",
        site_name: str = "",
        library_id: str = "",
        library_name: str = "",
        tree_role: str = "",
        cache_only: bool = False,
    ) -> List[Dict[str, Any]]:
        items = self.list_drive_root_children_cached_only(drive_id) if cache_only else self.list_drive_root_children(drive_id)
        return [
            self.normalize_drive_item(
                item,
                drive_id=drive_id,
                site_id=site_id,
                site_name=site_name,
                library_id=library_id or drive_id,
                library_name=library_name,
                tree_role=tree_role,
                parent_item_id="",
                parent_item_path="/",
            )
            for item in items
            if item.get("id")
        ]

    def list_drive_item_children_normalized(
        self,
        drive_id: str,
        item_id: str,
        *,
        site_id: str = "",
        site_name: str = "",
        library_id: str = "",
        library_name: str = "",
        tree_role: str = "",
        parent_item_path: str = "",
        cache_only: bool = False,
    ) -> List[Dict[str, Any]]:
        items = self.list_drive_item_children_cached_only(drive_id, item_id) if cache_only else self.list_drive_item_children(drive_id, item_id)
        return [
            self.normalize_drive_item(
                item,
                drive_id=drive_id,
                site_id=site_id,
                site_name=site_name,
                library_id=library_id or drive_id,
                library_name=library_name,
                tree_role=tree_role,
                parent_item_id=item_id,
                parent_item_path=parent_item_path,
            )
            for item in items
            if item.get("id")
        ]

    def list_drive_all_items_normalized(
        self,
        drive_id: str,
        *,
        site_id: str = "",
        site_name: str = "",
        library_id: str = "",
        library_name: str = "",
        tree_role: str = "",
    ) -> List[Dict[str, Any]]:
        normalized_items: List[Dict[str, Any]] = []
        stack: List[Dict[str, str]] = [{"item_id": "", "parent_item_path": "/"}]

        while stack:
            current = stack.pop()
            item_id = current.get("item_id", "")
            parent_item_path = current.get("parent_item_path", "/")
            raw_items = (
                self.list_drive_root_children(drive_id)
                if not item_id
                else self.list_drive_item_children(drive_id, item_id)
            )

            for item in raw_items:
                if not item.get("id"):
                    continue
                normalized = self.normalize_drive_item(
                    item,
                    drive_id=drive_id,
                    site_id=site_id,
                    site_name=site_name,
                    library_id=library_id or drive_id,
                    library_name=library_name,
                    tree_role=tree_role,
                    parent_item_id=item_id,
                    parent_item_path=parent_item_path,
                )
                normalized_items.append(normalized)
                if normalized.get("is_folder"):
                    stack.append({
                        "item_id": normalized.get("id", ""),
                        "parent_item_path": normalized.get("item_path", "/"),
                    })

        return normalized_items

    def list_drive_subtree_items_normalized(
        self,
        drive_id: str,
        item_id: str,
        *,
        site_id: str = "",
        site_name: str = "",
        library_id: str = "",
        library_name: str = "",
        tree_role: str = "",
        parent_item_path: str = "",
    ) -> List[Dict[str, Any]]:
        if not drive_id or not item_id:
            return []

        log_trace(
            "graph",
            "list_drive_subtree_start",
            drive_id_suffix=str(drive_id)[-16:],
            item_id_suffix=str(item_id)[-16:],
            tree_role=tree_role,
            parent_item_path_excerpt=str(parent_item_path or "")[:120],
        )
        normalized_items: List[Dict[str, Any]] = []
        stack: List[Dict[str, str]] = [{
            "item_id": item_id,
            "parent_item_path": parent_item_path or "/",
        }]

        while stack:
            current = stack.pop()
            current_item_id = current.get("item_id", "")
            current_parent_item_path = current.get("parent_item_path", "/")
            raw_items = self.list_drive_item_children(drive_id, current_item_id)

            for item in raw_items:
                if not item.get("id"):
                    continue
                normalized = self.normalize_drive_item(
                    item,
                    drive_id=drive_id,
                    site_id=site_id,
                    site_name=site_name,
                    library_id=library_id or drive_id,
                    library_name=library_name,
                    tree_role=tree_role,
                    parent_item_id=current_item_id,
                    parent_item_path=current_parent_item_path,
                )
                normalized_items.append(normalized)
                if normalized.get("is_folder"):
                    stack.append({
                        "item_id": normalized.get("id", ""),
                        "parent_item_path": normalized.get("item_path", "/"),
                    })

        log_trace("graph", "list_drive_subtree_done", normalized_descendant_count=len(normalized_items))
        return normalized_items

    def download_drive_item_content(self, drive_id: str, item_id: str, *, max_bytes: int = 262144) -> bytes:
        if not drive_id or not item_id:
            return b""
        url = f"{AUTH_CONFIG['graph_base']}/drives/{drive_id}/items/{item_id}/content"
        data = self.get_bytes(url, max_bytes=max_bytes)
        log_trace(
            "graph",
            "download_drive_item_content",
            drive_id_suffix=str(drive_id)[-16:],
            item_id_suffix=str(item_id)[-16:],
            bytes_returned=len(data),
            max_bytes=max_bytes,
        )
        return data
