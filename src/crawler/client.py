from __future__ import annotations

import asyncio
import json
import random
import time
from typing import Any

import httpx


class FlkClientError(RuntimeError):
    pass


class FlkClient:
    def __init__(
        self,
        base_url: str,
        timeout: float = 20.0,
        retries: int = 4,
        *,
        request_delay: float = 0.0,
        request_jitter: float = 0.0,
        warmup_timeout: float = 5.0,
        bootstrap_api_probe: bool = False,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.request_delay = max(float(request_delay), 0.0)
        self.request_jitter = max(float(request_jitter), 0.0)
        self.warmup_timeout = max(float(warmup_timeout), 0.1)
        self.bootstrap_api_probe = bool(bootstrap_api_probe)
        self._client: httpx.AsyncClient | None = None
        self._refresh_lock = asyncio.Lock()
        self._throttle_lock = asyncio.Lock()
        self._request_count = 0
        self._last_request_at = 0.0

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
        if not self.bootstrap_api_probe:
            return
        try:
            response = await self._request(
                "GET",
                "/law-search/index/aggregateData",
                max_attempts=1,
                retry_on_challenge=False,
            )
            self._parse_json(response)
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
        max_attempts: int | None = None,
        retry_on_challenge: bool = True,
    ) -> httpx.Response:
        if self._client is None:
            raise RuntimeError("Client is not initialized.")

        last_error: Exception | None = None
        attempts = max(int(max_attempts if max_attempts is not None else self.retries), 1)
        for attempt in range(1, attempts + 1):
            try:
                await self._throttle()
                headers = self._request_headers(allow_html=allow_html)
                response = await self._client.request(
                    method,
                    path if absolute else path,
                    params=params,
                    json=json,
                    headers=headers,
                )
                self._request_count += 1
                if not allow_html:
                    self._raise_for_challenge(response)
                response.raise_for_status()
                return response
            except Exception as exc:  # pragma: no cover - exercised through higher level tests
                last_error = exc
                refresh = retry_on_challenge and self._should_refresh_session(exc)
                if refresh and attempt < attempts:
                    await self._refresh_session()
                if attempt >= attempts:
                    break
                await asyncio.sleep(self._retry_delay(attempt, challenge=refresh))
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
        content_type = response.headers.get("content-type", "").lower()
        text = response.content[:800].decode("utf-8", errors="ignore")
        normalized = text.lower()
        looks_like_html = (
            "text/html" in content_type
            or normalized.lstrip().startswith(("<!doctype html", "<html"))
            or "<script" in normalized
        )
        challenge_markers = (
            "wzws",
            "captcha",
            "challenge",
            "访问过于频繁",
            "安全验证",
            "国家法律法规数据库",
        )
        if looks_like_html and any(marker in normalized or marker in text for marker in challenge_markers):
            raise FlkClientError("Received HTML challenge or non-API HTML response from FLK.")
        try:
            request_path = response.request.url.path
        except RuntimeError:
            request_path = ""
        if looks_like_html and request_path.startswith("/law-search/"):
            raise FlkClientError("Received HTML challenge or non-API HTML response from FLK.")

    async def _refresh_session(self) -> None:
        async with self._refresh_lock:
            if self._client is None:
                return
            await self._reset_client()
            await self._warm_up_session()

    def _should_refresh_session(self, exc: Exception) -> bool:
        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code in {403, 429, 503}
        if not isinstance(exc, FlkClientError):
            return False
        message = str(exc)
        return "HTML challenge" in message or "non-API HTML" in message or "code=403" in message

    async def _warm_up_session(self) -> None:
        if self._client is None:
            return
        warm_up_paths = ("/", "/search", "/advanceSearch")
        for path in warm_up_paths:
            try:
                await self._throttle()
                response = await self._client.get(
                    path,
                    timeout=self.warmup_timeout,
                    headers={
                        "Referer": f"{self.base_url}/search",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Site": "same-origin",
                        "Sec-Fetch-User": "?1",
                    },
                )
                response.raise_for_status()
            except Exception:
                continue
            await asyncio.sleep(0.4 + random.uniform(0.0, 0.4))

    def _retry_delay(self, attempt: int, *, challenge: bool) -> float:
        base = min(2 ** (attempt - 1), 16)
        jitter = random.uniform(0.0, 1.5)
        if challenge:
            return base + 3.0 + jitter
        return float(base) + jitter

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
                "Accept-Encoding": "gzip, deflate",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Origin": self.base_url,
                "Referer": f"{self.base_url}/search",
                "Sec-CH-UA": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
                "Sec-CH-UA-Mobile": "?0",
                "Sec-CH-UA-Platform": '"Linux"',
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            },
        )

    def _request_headers(self, *, allow_html: bool) -> dict[str, str]:
        headers = {
            "Referer": f"{self.base_url}/search",
            "Origin": self.base_url,
            "Sec-Fetch-Dest": "document" if allow_html else "empty",
            "Sec-Fetch-Mode": "navigate" if allow_html else "cors",
            "Sec-Fetch-Site": "same-origin",
        }
        if not allow_html:
            headers["X-Requested-With"] = "XMLHttpRequest"
        return headers

    async def _throttle(self) -> None:
        if self.request_delay <= 0 and self.request_jitter <= 0:
            return
        async with self._throttle_lock:
            target_delay = self.request_delay + random.uniform(0.0, self.request_jitter)
            elapsed = time.monotonic() - self._last_request_at
            if elapsed < target_delay:
                await asyncio.sleep(target_delay - elapsed)
            self._last_request_at = time.monotonic()

    async def _reset_client(self) -> None:
        if self._client is not None and hasattr(self._client, "aclose"):
            await self._client.aclose()
        self._client = self._build_client()
        self._request_count = 0
