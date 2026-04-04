import hashlib
import json
import os
import time
import webbrowser
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import msal
import pyperclip
import requests

from .logger import flush_logger, log_info, log_trace, log_warn
from .paths import graph_cache_root, msal_token_cache_path


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
        "Files.ReadWrite.All",
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
        self._token_cache: Optional[msal.SerializableTokenCache] = None

    def _load_msal_token_cache(self) -> msal.SerializableTokenCache:
        cache = msal.SerializableTokenCache()
        path = msal_token_cache_path()
        try:
            if path.is_file():
                raw = path.read_text(encoding="utf-8")
                if raw.strip():
                    cache.deserialize(raw)
        except (OSError, ValueError) as exc:
            log_warn("msal_token_cache_load_failed", error=str(exc)[:240])
        return cache

    def _persist_msal_token_cache(self) -> None:
        cache = self._token_cache
        if cache is None or not getattr(cache, "has_state_changed", False):
            return
        path = msal_token_cache_path()
        tmp = path.with_suffix(".json.tmp")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            data = cache.serialize()
            tmp.write_text(data, encoding="utf-8")
            tmp.replace(path)
        except OSError as exc:
            log_warn("msal_token_cache_persist_failed", error=str(exc)[:240])
            try:
                if tmp.is_file():
                    tmp.unlink()
            except OSError:
                pass
        finally:
            cache.has_state_changed = False

    def _clear_persistent_msal_token_cache_file(self) -> None:
        try:
            p = msal_token_cache_path()
            if p.is_file():
                p.unlink()
            tmp = p.with_suffix(".json.tmp")
            if tmp.is_file():
                tmp.unlink()
        except OSError as exc:
            log_warn("msal_token_cache_remove_failed", error=str(exc)[:200])

    def _ensure_app(self):
        if self.app is None:
            self._token_cache = self._load_msal_token_cache()
            self.app = msal.PublicClientApplication(
                AUTH_CONFIG["client_id"],
                authority=AUTH_CONFIG["authority"],
                token_cache=self._token_cache,
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

    def open_device_login_page(self) -> bool:
        if not self.device_flow:
            raise RuntimeError("Device flow has not been initialized.")
        uri = self.device_flow.get("verification_uri")
        if not uri:
            return False
        return bool(webbrowser.open(uri))

    def acquire_token(self) -> Dict[str, Any]:
        if not self.device_flow:
            raise RuntimeError("No active device flow. Start device login first.")

        result = self._ensure_app().acquire_token_by_device_flow(self.device_flow)

        if "access_token" not in result:
            raise RuntimeError(result.get("error_description", "Authentication failed."))

        self.token = result["access_token"]
        self.session_context["connected"] = True
        self._persist_msal_token_cache()
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
        self._persist_msal_token_cache()
        return True

    def refresh_access_token_silently(self, *, force_refresh: bool = False) -> bool:
        return self._try_acquire_token_silent(force_refresh=force_refresh)

    def disconnect(self) -> None:
        log_trace("graph_auth", "disconnect")
        self.token = None
        self.device_flow = None
        self.profile = None
        self._drive_children_cache.clear()
        self.app = None
        self._token_cache = None
        self._clear_persistent_msal_token_cache_file()
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

    @staticmethod
    def _graph_retry_after_seconds(response: Optional[requests.Response]) -> Optional[float]:
        if response is None:
            return None
        ra = response.headers.get("Retry-After")
        if ra is None or str(ra).strip() == "":
            return None
        try:
            return float(ra)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _graph_should_retry_request_failure(exc: BaseException) -> bool:
        if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
            return True
        if isinstance(exc, requests.exceptions.ChunkedEncodingError):
            return True
        if isinstance(exc, requests.HTTPError):
            r = exc.response
            if r is None:
                return False
            code = r.status_code
            if code in (401, 403, 404):
                return False
            if code in (408, 429, 500, 502, 503, 504):
                return True
        return False

    @staticmethod
    def _graph_transient_retry_delay_after_failure(
        failed_attempt_index: int,
        exc: BaseException,
        default_backoffs: Tuple[float, float],
    ) -> float:
        if isinstance(exc, requests.HTTPError) and exc.response is not None and exc.response.status_code == 429:
            ra = GraphClient._graph_retry_after_seconds(exc.response)
            if ra is not None:
                return min(max(ra, 0.0), 15.0)
        if failed_attempt_index >= len(default_backoffs):
            return default_backoffs[-1]
        return default_backoffs[failed_attempt_index]

    @staticmethod
    def _graph_transient_retry_allowed(method: str, url: str) -> bool:
        m = str(method or "").strip().upper()
        if m in ("GET", "HEAD", "OPTIONS"):
            return True
        if m == "POST":
            base = str(url or "").split("?", 1)[0].rstrip("/")
            if base.endswith("/me/getMemberGroups"):
                return True
        return False

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
        max_attempts = 3
        default_backoffs: Tuple[float, float] = (0.22, 0.45)
        excerpt = _graph_url_excerpt(url)
        for attempt in range(max_attempts):
            try:
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
                    path_excerpt=excerpt,
                    status_code=response.status_code,
                    retried_after_401=retried_401,
                    stream=stream,
                )
                return response
            except (
                requests.HTTPError,
                requests.ConnectionError,
                requests.Timeout,
                requests.exceptions.ChunkedEncodingError,
            ) as exc:
                if not self._graph_should_retry_request_failure(exc):
                    raise
                if not self._graph_transient_retry_allowed(method, url):
                    raise
                if attempt >= max_attempts - 1:
                    log_warn(
                        "graph_http_transient_exhausted",
                        method=method,
                        path_excerpt=excerpt,
                        attempts=max_attempts,
                        error=str(exc)[:300],
                    )
                    raise
                delay_s = self._graph_transient_retry_delay_after_failure(attempt, exc, default_backoffs)
                log_warn(
                    "graph_http_transient_retry",
                    method=method,
                    path_excerpt=excerpt,
                    attempt=attempt + 2,
                    max_attempts=max_attempts,
                    delay_s=delay_s,
                    error=str(exc)[:300],
                )
                time.sleep(delay_s)
        raise RuntimeError("graph_http_retry_internal_error")  # pragma: no cover

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

    def get_drive_item_optional(self, drive_id: str, item_id: str) -> Optional[Dict[str, Any]]:
        """Same as ``get_drive_item`` but returns None on 404 (deleted, wrong drive, or moved beyond this drive)."""
        drive_id = str(drive_id or "").strip()
        item_id = str(item_id or "").strip()
        if not drive_id or not item_id:
            return None
        try:
            return self.get_drive_item(drive_id, item_id)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            raise

    def get_drive_root_item(self, drive_id: str) -> Optional[Dict[str, Any]]:
        """Return the library root driveItem (used as parent when paths start at first-level folder)."""
        drive_id = str(drive_id or "").strip()
        if not drive_id:
            return None
        try:
            return self.get(f"{AUTH_CONFIG['graph_base']}/drives/{drive_id}/root")
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            raise

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

    # -------------------------------------------------------------------------
    # Drive mutations (copy / create folder) — requires Files.ReadWrite.All consent
    # -------------------------------------------------------------------------
    def start_drive_item_copy(
        self,
        *,
        source_drive_id: str,
        source_item_id: str,
        dest_drive_id: str,
        dest_parent_item_id: str,
        name: Optional[str] = None,
        conflict_behavior: str = "rename",
    ) -> str:
        """POST /copy; returns async monitor URL from the ``Location`` response header."""
        source_drive_id = str(source_drive_id or "").strip()
        source_item_id = str(source_item_id or "").strip()
        dest_drive_id = str(dest_drive_id or "").strip()
        dest_parent_item_id = str(dest_parent_item_id or "").strip()
        if not source_drive_id or not source_item_id or not dest_drive_id or not dest_parent_item_id:
            raise ValueError("start_drive_item_copy requires source/destination drive and item ids.")

        enc_drive = quote(source_drive_id, safe="")
        enc_item = quote(source_item_id, safe="")
        url = f"{AUTH_CONFIG['graph_base']}/drives/{enc_drive}/items/{enc_item}/copy"
        body: Dict[str, Any] = {
            "parentReference": {"driveId": dest_drive_id, "id": dest_parent_item_id},
            "@microsoft.graph.conflictBehavior": str(conflict_behavior or "rename"),
        }
        if name:
            body["name"] = str(name).strip()

        headers = dict(self.get_headers())
        headers["Content-Type"] = "application/json"

        def _post() -> requests.Response:
            return requests.post(url, headers=headers, json=body, timeout=120)

        response = _post()
        if response.status_code == 401 and self._try_acquire_token_silent(force_refresh=True):
            headers = dict(self.get_headers())
            headers["Content-Type"] = "application/json"
            response = requests.post(url, headers=headers, json=body, timeout=120)
        if response.status_code == 429:
            time.sleep(float(response.headers.get("Retry-After", "10")))
            headers = dict(self.get_headers())
            headers["Content-Type"] = "application/json"
            response = requests.post(url, headers=headers, json=body, timeout=120)

        if response.status_code != 202:
            response.raise_for_status()

        loc = response.headers.get("Location") or response.headers.get("location")
        if not loc:
            raise RuntimeError("Copy returned 202 but no Location header for progress polling.")
        log_trace(
            "graph",
            "drive_item_copy_started",
            source_drive_suffix=source_drive_id[-12:] if len(source_drive_id) > 12 else source_drive_id,
            dest_drive_suffix=dest_drive_id[-12:] if len(dest_drive_id) > 12 else dest_drive_id,
        )
        loc_s = str(loc).strip()
        log_info(
            "graph_async_copy_submitted",
            phase="copy_post_accepted",
            monitor_url_excerpt=loc_s[:220],
            source_item_id_suffix=source_item_id[-16:] if len(source_item_id) > 16 else source_item_id,
            dest_parent_item_id_suffix=dest_parent_item_id[-16:]
            if len(dest_parent_item_id) > 16
            else dest_parent_item_id,
            copy_target_name=str(name or "")[:200] or None,
        )
        flush_logger()
        return loc_s

    def wait_graph_async_operation(
        self,
        monitor_url: str,
        *,
        timeout_sec: float = 600.0,
        poll_interval_sec: float = 1.0,
    ) -> Dict[str, Any]:
        """Poll a Graph async monitor URL until completion or failure."""
        effective_timeout = max(30.0, float(timeout_sec))
        deadline = time.monotonic() + effective_timeout
        monitor_url = str(monitor_url or "").strip()
        if not monitor_url:
            raise ValueError("monitor_url is required")

        wait_started = time.monotonic()
        poll_count = 0
        log_info(
            "graph_async_wait_started",
            monitor_url_excerpt=monitor_url[:220],
            timeout_sec_config=float(timeout_sec),
            effective_timeout_sec=float(effective_timeout),
            poll_interval_sec=float(poll_interval_sec),
        )
        flush_logger()

        last_payload: Dict[str, Any] = {}
        while time.monotonic() < deadline:
            poll_count += 1
            headers = dict(self.get_headers())
            response = requests.get(monitor_url, headers=headers, timeout=120)
            retried_monitor_401 = False
            if response.status_code == 401 and self._try_acquire_token_silent(force_refresh=True):
                retried_monitor_401 = True
                headers = dict(self.get_headers())
                response = requests.get(monitor_url, headers=headers, timeout=120)
            if response.status_code == 401:
                log_info(
                    "graph_async_monitor_poll_401",
                    poll_count=poll_count,
                    elapsed_sec=round(time.monotonic() - wait_started, 3),
                    retried_token_refresh=bool(retried_monitor_401),
                    monitor_url_excerpt=monitor_url[:220],
                    http_status=401,
                )
                flush_logger()
            if response.status_code == 429:
                log_info(
                    "graph_async_wait_poll",
                    poll_count=poll_count,
                    elapsed_sec=round(time.monotonic() - wait_started, 3),
                    http_status=429,
                    note="retry_after_sleep",
                )
                flush_logger()
                time.sleep(float(response.headers.get("Retry-After", "5")))
                continue
            response.raise_for_status()
            try:
                last_payload = response.json()
            except Exception:
                last_payload = {}

            status = str(last_payload.get("status", "") or "").lower()
            op_id = str(last_payload.get("id", "") or "").strip()
            log_info(
                "graph_async_wait_poll",
                poll_count=poll_count,
                elapsed_sec=round(time.monotonic() - wait_started, 3),
                graph_status=status or None,
                operation_id=op_id or None,
                http_status=int(response.status_code),
            )
            flush_logger()
            if status in ("completed", "completedwithwarnings"):
                log_trace("graph", "async_operation_done", status=status)
                log_info(
                    "graph_async_wait_completed",
                    poll_count=poll_count,
                    elapsed_sec=round(time.monotonic() - wait_started, 3),
                    graph_status=status,
                    operation_id=op_id or None,
                )
                flush_logger()
                return last_payload
            if status in ("failed",):
                err = last_payload.get("error") or last_payload
                log_info(
                    "graph_async_wait_failed",
                    poll_count=poll_count,
                    elapsed_sec=round(time.monotonic() - wait_started, 3),
                    graph_status=status,
                    operation_id=op_id or None,
                    error_excerpt=repr(err)[:500],
                )
                flush_logger()
                raise RuntimeError(f"Graph async operation failed: {err!r}")
            time.sleep(max(0.2, float(poll_interval_sec)))

        elapsed = time.monotonic() - wait_started
        log_info(
            "graph_async_wait_timeout",
            poll_count=poll_count,
            elapsed_sec=round(elapsed, 3),
            effective_timeout_sec=float(effective_timeout),
            timeout_sec_config=float(timeout_sec),
            monitor_url_excerpt=monitor_url[:220],
            last_graph_status=str(last_payload.get("status", "") or "") or None,
            last_operation_id=str(last_payload.get("id", "") or "").strip() or None,
        )
        flush_logger()
        raise TimeoutError(f"Graph async operation timed out after {timeout_sec}s: {monitor_url[:120]!r}…")

    def create_child_folder(
        self,
        drive_id: str,
        parent_item_id: str,
        name: str,
        *,
        conflict_behavior: str = "fail",
    ) -> Dict[str, Any]:
        """Create a folder under a parent driveItem (synchronous)."""
        drive_id = str(drive_id or "").strip()
        parent_item_id = str(parent_item_id or "").strip()
        name = str(name or "").strip()
        if not drive_id or not parent_item_id or not name:
            raise ValueError("create_child_folder requires drive_id, parent_item_id, and name.")

        enc_drive = quote(drive_id, safe="")
        enc_parent = quote(parent_item_id, safe="")
        url = f"{AUTH_CONFIG['graph_base']}/drives/{enc_drive}/items/{enc_parent}/children"
        body: Dict[str, Any] = {
            "name": name,
            "folder": {},
            "@microsoft.graph.conflictBehavior": str(conflict_behavior or "fail"),
        }
        headers = dict(self.get_headers())
        headers["Content-Type"] = "application/json"

        response = requests.post(url, headers=headers, json=body, timeout=120)
        if response.status_code == 401 and self._try_acquire_token_silent(force_refresh=True):
            headers = dict(self.get_headers())
            headers["Content-Type"] = "application/json"
            response = requests.post(url, headers=headers, json=body, timeout=120)
        if response.status_code == 429:
            time.sleep(float(response.headers.get("Retry-After", "10")))
            headers = dict(self.get_headers())
            headers["Content-Type"] = "application/json"
            response = requests.post(url, headers=headers, json=body, timeout=120)

        response.raise_for_status()
        payload = response.json()
        log_trace(
            "graph",
            "create_child_folder_ok",
            drive_id_suffix=drive_id[-12:] if len(drive_id) > 12 else drive_id,
            name_excerpt=name[:80],
        )
        return payload

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
    def discover_sites_for_planning_workspace(self) -> List[Dict[str, Any]]:
        """Return sites the user can access. Libraries are loaded lazily per site (see MainWindow)."""
        log_trace("graph", "discover_sites_for_planning_workspace_start")
        sites = self.list_sites()
        results: List[Dict[str, Any]] = []
        for site in sites:
            normalized_site = self.normalize_site(site)
            site_id = normalized_site.get("id", "")
            if not site_id:
                continue
            normalized_site["libraries"] = []
            normalized_site["site_key"] = normalized_site.get("web_url") or site_id
            results.append(normalized_site)

        log_trace(
            "graph",
            "discover_sites_for_planning_workspace_done",
            raw_site_count=len(sites),
            site_row_count=len(results),
        )
        return results

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
            normalized_site.setdefault("site_key", normalized_site.get("web_url") or site_id)
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

    def list_drive_folder_descendant_files_normalized(
        self,
        drive_id: str,
        folder_item_id: str,
        *,
        site_id: str = "",
        site_name: str = "",
        library_id: str = "",
        library_name: str = "",
        tree_role: str = "",
        folder_item_path: str = "",
        max_files: int = 5000,
    ) -> List[Dict[str, Any]]:
        """
        Walk a folder tree via Graph children calls and return normalized **file** rows only.

        Used to turn one folder selection into many per-file planned moves (separate Graph copy steps).
        """
        drive_id = str(drive_id or "").strip()
        folder_item_id = str(folder_item_id or "").strip()
        if not drive_id or not folder_item_id:
            return []

        files: List[Dict[str, Any]] = []
        stack: List[tuple[str, str]] = [(folder_item_id, str(folder_item_path or "").strip() or "/")]
        seen_folders: set[str] = {folder_item_id}
        lib_id = library_id or drive_id

        while stack:
            cur_id, cur_path = stack.pop()
            children = self.list_drive_item_children_normalized(
                drive_id,
                cur_id,
                site_id=site_id,
                site_name=site_name,
                library_id=lib_id,
                library_name=library_name,
                tree_role=tree_role,
                parent_item_path=cur_path,
            )
            for ch in children:
                cid = str(ch.get("id") or "").strip()
                if not cid:
                    continue
                if ch.get("is_folder"):
                    if cid not in seen_folders:
                        seen_folders.add(cid)
                        stack.append((cid, str(ch.get("item_path") or "").strip() or "/"))
                else:
                    files.append(ch)
                    if len(files) >= max(1, int(max_files)):
                        log_info(
                            "graph_folder_descendant_files_cap",
                            cap=int(max_files),
                            drive_id_suffix=drive_id[-16:] if len(drive_id) > 16 else drive_id,
                        )
                        return files
        return files

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
