from __future__ import annotations

import json
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable

from .base import ProviderRequestConfig, ProviderResponseError


RATE_LIMIT_ERROR_MARKERS = (
    "rate limit",
    "too many requests",
    "429",
    "rpm",
    "requests per minute",
    "tpm",
    "tokens per minute",
    "resource exhausted",
    "quota exceeded",
)


@dataclass(frozen=True)
class RateLimitSettings:
    rpm: int = 0
    tpm: int = 0
    window_seconds: float = 60.0
    retry_count: int = 3
    backoff_seconds: float = 5.0


class SlidingWindowRateLimiter:
    def __init__(
        self,
        *,
        rpm: int = 0,
        tpm: int = 0,
        window_seconds: float = 60.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.rpm = max(int(rpm or 0), 0)
        self.tpm = max(int(tpm or 0), 0)
        self.window_seconds = max(float(window_seconds or 60.0), 0.001)
        self.clock = clock or time.monotonic
        self._events: deque[tuple[float, int]] = deque()
        self._lock = threading.Condition()

    def acquire(self, token_cost: int = 0) -> None:
        tokens = max(int(token_cost or 0), 0)
        if self.rpm <= 0 and self.tpm <= 0:
            return
        with self._lock:
            while True:
                now = self.clock()
                self._expire(now)
                wait_seconds = self._required_wait_seconds(now, tokens)
                if wait_seconds <= 0:
                    self._events.append((now, tokens))
                    self._lock.notify_all()
                    return
                self._lock.wait(timeout=wait_seconds)

    def _expire(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._events and self._events[0][0] <= cutoff:
            self._events.popleft()

    def _required_wait_seconds(self, now: float, tokens: int) -> float:
        waits: list[float] = []
        if self.rpm > 0 and len(self._events) >= self.rpm:
            oldest = self._events[0][0]
            waits.append(max((oldest + self.window_seconds) - now, 0.0))
        if self.tpm > 0:
            total_tokens = sum(value for _, value in self._events)
            if total_tokens + tokens > self.tpm:
                overflow = (total_tokens + tokens) - self.tpm
                released = 0
                for timestamp, token_count in self._events:
                    released += token_count
                    if released >= overflow:
                        waits.append(max((timestamp + self.window_seconds) - now, 0.0))
                        break
        return max(waits, default=0.0)


_LIMITERS: dict[tuple[str, str, int, int, float], SlidingWindowRateLimiter] = {}
_LIMITERS_LOCK = threading.Lock()


def rate_limit_settings_from_local(local: dict[str, Any]) -> RateLimitSettings:
    payload = dict((local or {}).get("rate_limit", {}) or {})
    return RateLimitSettings(
        rpm=max(int(payload.get("rpm", 0) or 0), 0),
        tpm=max(int(payload.get("tpm", 0) or 0), 0),
        window_seconds=max(float(payload.get("window_seconds", 60.0) or 60.0), 0.001),
        retry_count=max(int(payload.get("retry_count", 3) or 3), 1),
        backoff_seconds=max(float(payload.get("backoff_seconds", 5.0) or 5.0), 0.1),
    )


def estimate_message_tokens(messages: list[dict[str, str]], *, max_output_tokens: int = 0) -> int:
    try:
        payload = json.dumps(messages, ensure_ascii=False)
    except TypeError:
        payload = "".join(str(item) for item in messages)
    return max(len(payload), 0) + max(int(max_output_tokens or 0), 0)


def estimate_texts_tokens(texts: list[str]) -> int:
    return sum(max(len(str(text or "")), 0) for text in texts)


def run_with_rate_limit(
    request_config: ProviderRequestConfig,
    *,
    estimated_tokens: int,
    operation: Callable[[], Any],
    operation_name: str,
) -> Any:
    settings = rate_limit_settings_from_local(request_config.local)
    limiter = _get_limiter(request_config, settings)
    last_error: Exception | None = None
    attempts = max(settings.retry_count, 1)
    for attempt in range(1, attempts + 1):
        limiter.acquire(estimated_tokens)
        try:
            return operation()
        except Exception as exc:
            if not is_rate_limit_error(exc):
                raise
            last_error = exc
            if attempt >= attempts:
                break
            time.sleep(settings.backoff_seconds * attempt)
    raise ProviderResponseError(f"{operation_name} exceeded configured rate limits after {attempts} attempts: {last_error}")


def is_rate_limit_error(exc: Exception) -> bool:
    text = str(exc or "").lower()
    return any(marker in text for marker in RATE_LIMIT_ERROR_MARKERS)


def _get_limiter(
    request_config: ProviderRequestConfig,
    settings: RateLimitSettings,
) -> SlidingWindowRateLimiter:
    if settings.rpm <= 0 and settings.tpm <= 0:
        return SlidingWindowRateLimiter()
    key = (
        request_config.provider,
        request_config.model,
        settings.rpm,
        settings.tpm,
        settings.window_seconds,
    )
    with _LIMITERS_LOCK:
        limiter = _LIMITERS.get(key)
        if limiter is None:
            limiter = SlidingWindowRateLimiter(
                rpm=settings.rpm,
                tpm=settings.tpm,
                window_seconds=settings.window_seconds,
            )
            _LIMITERS[key] = limiter
        return limiter
