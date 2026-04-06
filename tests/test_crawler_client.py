from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest

from crawler.cli import build_parser
from crawler.client import FlkClient


def make_response(status_code: int, content_type: str, body: str) -> httpx.Response:
    request = httpx.Request("GET", "https://flk.npc.gov.cn/test")
    return httpx.Response(
        status_code,
        headers={"content-type": content_type},
        text=body,
        request=request,
    )


@pytest.mark.anyio
async def test_client_retries_after_html_challenge(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FlkClient("https://flk.npc.gov.cn", retries=2)
    request_calls = 0
    warm_up_calls = 0

    async def fake_request(method: str, path: str, params=None, json=None, headers=None):
        nonlocal request_calls
        request_calls += 1
        if request_calls == 1:
            return make_response(200, "text/html; charset=utf-8", "<html>WZWS challenge</html>")
        return make_response(200, "application/json", '{"code":200,"data":{"ok":true}}')

    async def fake_warm_up_session() -> None:
        nonlocal warm_up_calls
        warm_up_calls += 1

    async def fake_reset_client() -> None:
        client._request_count = 1

    client._client = SimpleNamespace(request=fake_request)
    monkeypatch.setattr(client, "_warm_up_session", fake_warm_up_session)
    monkeypatch.setattr(client, "_reset_client", fake_reset_client)

    payload = await client.get_json("/law-search/search/list")

    assert payload == {"code": 200, "data": {"ok": True}}
    assert request_calls == 2
    assert warm_up_calls == 1


@pytest.mark.anyio
async def test_client_resets_session_on_first_request_challenge(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FlkClient("https://flk.npc.gov.cn", retries=2)
    reset_calls = 0
    warm_up_calls = 0
    request_calls = 0

    async def fake_request(method: str, path: str, params=None, json=None, headers=None):
        nonlocal request_calls
        request_calls += 1
        if request_calls == 1:
            return make_response(200, "text/html; charset=utf-8", "<html>WZWS challenge</html>")
        return make_response(200, "application/json", '{"code":200,"data":{"ok":true}}')

    async def fake_reset_client() -> None:
        nonlocal reset_calls
        reset_calls += 1

    async def fake_warm_up_session() -> None:
        nonlocal warm_up_calls
        warm_up_calls += 1

    client._client = SimpleNamespace(request=fake_request)
    monkeypatch.setattr(client, "_reset_client", fake_reset_client)
    monkeypatch.setattr(client, "_warm_up_session", fake_warm_up_session)

    payload = await client.get_json("/law-search/search/list")

    assert payload == {"code": 200, "data": {"ok": True}}
    assert reset_calls == 1
    assert warm_up_calls == 1


def test_cli_accepts_concurrency_option() -> None:
    parser = build_parser()

    args = parser.parse_args(["--category", "法律", "--metadata", "--concurrency", "2"])

    assert args.category == "法律"
    assert args.metadata is True
    assert args.concurrency == 2
