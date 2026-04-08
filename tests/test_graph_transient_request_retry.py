"""Bounded transient retries for GraphClient._request (idempotent methods + narrow POST allowlist)."""

from __future__ import annotations

from typing import Any, Dict, Optional
from unittest.mock import patch

import pytest
import requests

from ozlink_console.graph import GraphClient


def _response(status_code: int, headers: Optional[Dict[str, Any]] = None) -> requests.Response:
    r = requests.Response()
    r.status_code = status_code
    r.url = "https://graph.microsoft.com/v1.0/me/drive/root/children"
    r.headers = requests.structures.CaseInsensitiveDict(headers or {})
    return r


@pytest.fixture
def graph_client() -> GraphClient:
    c = GraphClient()
    c.token = "unit-test-token"
    return c


def test_retries_503_then_succeeds(graph_client, monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr("ozlink_console.graph.time.sleep", lambda s: sleeps.append(float(s)))
    a, b, ok = _response(503), _response(503), _response(200)
    with patch("ozlink_console.graph.requests.request", side_effect=[a, b, ok]) as rq:
        out = graph_client._request("GET", "https://graph.microsoft.com/v1.0/x")
    assert out.status_code == 200
    assert rq.call_count == 3
    assert len(sleeps) == 2
    assert sleeps[0] == pytest.approx(0.22)
    assert sleeps[1] == pytest.approx(0.45)


def test_retries_connection_error_then_succeeds(graph_client, monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr("ozlink_console.graph.time.sleep", lambda s: sleeps.append(float(s)))
    ok = _response(200)
    with patch(
        "ozlink_console.graph.requests.request",
        side_effect=[requests.ConnectionError("reset"), requests.ConnectionError("reset"), ok],
    ) as rq:
        out = graph_client._request("GET", "https://graph.microsoft.com/v1.0/y")
    assert out.status_code == 200
    assert rq.call_count == 3
    assert len(sleeps) == 2


def test_no_retry_on_404(graph_client, monkeypatch):
    monkeypatch.setattr("ozlink_console.graph.time.sleep", lambda s: pytest.fail("sleep should not run"))
    bad = _response(404)
    with patch("ozlink_console.graph.requests.request", return_value=bad) as rq:
        with pytest.raises(requests.HTTPError):
            graph_client._request("GET", "https://graph.microsoft.com/v1.0/missing")
    assert rq.call_count == 1


def test_exhausts_three_attempts_on_persistent_503(graph_client, monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr("ozlink_console.graph.time.sleep", lambda s: sleeps.append(float(s)))
    r = _response(503)
    with patch("ozlink_console.graph.requests.request", return_value=r) as rq:
        with pytest.raises(requests.HTTPError):
            graph_client._request("GET", "https://graph.microsoft.com/v1.0/z")
    assert rq.call_count == 3
    assert len(sleeps) == 2


def test_429_retry_after_overrides_backoff():
    r = _response(429, {"Retry-After": "0.55"})
    try:
        r.raise_for_status()
    except requests.HTTPError as exc:
        delay = GraphClient._graph_transient_retry_delay_after_failure(0, exc, (0.22, 0.45))
        assert delay == pytest.approx(0.55)


def test_should_not_retry_401():
    r = _response(401)
    try:
        r.raise_for_status()
    except requests.HTTPError as exc:
        assert GraphClient._graph_should_retry_request_failure(exc) is False


def test_retry_allowed_get_head_options():
    assert GraphClient._graph_transient_retry_allowed("get", "https://graph.microsoft.com/v1.0/x")
    assert GraphClient._graph_transient_retry_allowed("HEAD", "https://graph.microsoft.com/v1.0/x")
    assert GraphClient._graph_transient_retry_allowed("options", "https://graph.microsoft.com/v1.0/x")


def test_retry_allowed_post_only_get_member_groups():
    assert GraphClient._graph_transient_retry_allowed(
        "POST",
        "https://graph.microsoft.com/v1.0/me/getMemberGroups",
    )
    assert not GraphClient._graph_transient_retry_allowed(
        "POST",
        "https://graph.microsoft.com/v1.0/me/sendMail",
    )
    assert GraphClient._graph_transient_retry_allowed(
        "POST",
        "https://graph.microsoft.com/v1.0/me/getMemberGroups?x=1",
    )


def test_post_transient_503_no_retry_by_default(graph_client, monkeypatch):
    monkeypatch.setattr("ozlink_console.graph.time.sleep", lambda s: pytest.fail("sleep should not run"))
    url = "https://graph.microsoft.com/v1.0/me/sendMail"
    bad = _response(503)
    bad.url = url
    with patch("ozlink_console.graph.requests.request", return_value=bad) as rq:
        with pytest.raises(requests.HTTPError):
            graph_client._request("POST", url, json_body={})
    assert rq.call_count == 1


def test_post_get_member_groups_retries_transient_503(graph_client, monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr("ozlink_console.graph.time.sleep", lambda s: sleeps.append(float(s)))
    url = "https://graph.microsoft.com/v1.0/me/getMemberGroups"
    a, b, ok = _response(503), _response(503), _response(200)
    for resp in (a, b, ok):
        resp.url = url
    with patch("ozlink_console.graph.requests.request", side_effect=[a, b, ok]) as rq:
        out = graph_client._request("POST", url, json_body={"securityEnabledOnly": False})
    assert out.status_code == 200
    assert rq.call_count == 3
    assert len(sleeps) == 2
