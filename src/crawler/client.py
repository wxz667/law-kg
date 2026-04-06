from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx


class FlkClientError(RuntimeError):
    pass


class FlkClient:
    def __init__(self, base_url: str, timeout: float = 20.0, retries: int = 4) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self._client: httpx.AsyncClient | None = None
        self._refresh_lock = asyncio.Lock()
        self._request_count = 0

    async def __aenter__(self) -> "FlkClient":
        self._client = self._build_client()
        await self.bootstrap()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def bootstrap(self) -> None:
        await self._warm_up_session()
        try:
            await self.get_json("/law-search/index/aggregateData")
        except FlkClientError:
            # `aggregateData` is only a warm-up request; crawling can proceed without it.
            return

    async def get_json(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        response = await self._request("GET", path, params=params)
        return self._parse_json(response)

    async def post_json(
        self,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        response = await self._request("POST", path, json=payload)
        return self._parse_json(response)

    async def get_text(self, path: str, *, params: dict[str, Any] | None = None) -> str:
        response = await self._request("GET", path, params=params, allow_html=True)
        return response.text

    async def get_bytes(self, path: str, *, params: dict[str, Any] | None = None) -> bytes:
        response = await self._request("GET", path, params=params)
        self._raise_for_challenge(response)
        return response.content

    async def get_external_bytes(self, url: str) -> bytes:
        if self._client is None:
            raise RuntimeError("Client is not initialized.")
        response = await self._request("GET", url, absolute=True)
        self._raise_for_challenge(response)
        return response.content

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        absolute: bool = False,
        allow_html: bool = False,
    ) -> httpx.Response:
        if self._client is None:
            raise RuntimeError("Client is not initialized.")

        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                headers = self._request_headers(allow_html=allow_html)
                response = await self._client.request(
                    method,
                    path if absolute else path,
                    params=params,
                    json=json,
                    headers=headers,
                )
                self._request_count += 1
                response.raise_for_status()
                if not allow_html:
                    self._raise_for_challenge(response)
                return response
            except Exception as exc:  # pragma: no cover - exercised through higher level tests
                last_error = exc
                if self._should_refresh_session(exc) and attempt < self.retries:
                    await self._refresh_session()
                if attempt >= self.retries:
                    break
                await asyncio.sleep(self._retry_delay(attempt, challenge=self._should_refresh_session(exc)))
        raise FlkClientError(str(last_error) if last_error else "Unknown FLK client error")

    def _parse_json(self, response: httpx.Response) -> Any:
        self._raise_for_challenge(response)
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise FlkClientError(f"Expected JSON response, got invalid payload: {exc}") from exc
        if isinstance(payload, dict) and payload.get("code") not in (None, 200):
            raise FlkClientError(f"FLK API returned code={payload.get('code')} msg={payload.get('msg')}")
        return payload

    def _raise_for_challenge(self, response: httpx.Response) -> None:
        content_type = response.headers.get("content-type", "")
        if "text/html" in content_type.lower():
            text = response.text[:500]
            if "WZWS" in text or "国家法律法规数据库" in text or "<html" in text.lower():
                raise FlkClientError("Received HTML challenge or non-API HTML response from FLK.")

    async def _refresh_session(self) -> None:
        async with self._refresh_lock:
            if self._client is None:
                return
            if self._request_count <= 1:
                await self._reset_client()
            await self._warm_up_session()

    def _should_refresh_session(self, exc: Exception) -> bool:
        if not isinstance(exc, FlkClientError):
            return False
        message = str(exc)
        return "HTML challenge" in message or "non-API HTML" in message

    async def _warm_up_session(self) -> None:
        if self._client is None:
            return
        warm_up_paths = ("/", "/search", "/advanceSearch")
        for path in warm_up_paths:
            try:
                response = await self._client.get(
                    path,
                    headers={
                        "Referer": f"{self.base_url}/search",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    },
                )
                response.raise_for_status()
            except Exception:
                continue
            await asyncio.sleep(0.2)

    def _retry_delay(self, attempt: int, *, challenge: bool) -> float:
        base = min(2 ** (attempt - 1), 8)
        if challenge:
            return base + 1.0
        return float(base)

    def _build_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Origin": self.base_url,
                "Referer": f"{self.base_url}/search",
            },
        )

    def _request_headers(self, *, allow_html: bool) -> dict[str, str]:
        headers = {
            "Referer": f"{self.base_url}/search",
            "Origin": self.base_url,
        }
        if not allow_html:
            headers["X-Requested-With"] = "XMLHttpRequest"
        return headers

    async def _reset_client(self) -> None:
        if self._client is not None and hasattr(self._client, "aclose"):
            await self._client.aclose()
        self._client = self._build_client()
        self._request_count = 0
