from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from typing import Any


@dataclass
class _CacheItem:
    expires_at: float
    value: Any


class RecommendationCache:
    def __init__(self, ttl_seconds: int = 300, max_size: int = 1000):
        self._ttl_seconds = int(ttl_seconds)
        self._max_size = int(max_size)
        self._store: dict[str, _CacheItem] = {}

    def _now(self) -> float:
        return time.time()

    def _make_key(self, content: str, case_type: str, current_paragraph: str | None, top_k: int) -> str:
        raw = f"{case_type}::{top_k}::{(current_paragraph or '')[:400]}::{content[:1200]}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def get(self, content: str, case_type: str, current_paragraph: str | None, top_k: int) -> Any | None:
        key = self._make_key(content, case_type, current_paragraph, top_k)
        item = self._store.get(key)
        if item is None:
            return None
        if item.expires_at <= self._now():
            self._store.pop(key, None)
            return None
        return item.value

    def set(self, content: str, case_type: str, current_paragraph: str | None, top_k: int, value: Any) -> None:
        key = self._make_key(content, case_type, current_paragraph, top_k)
        if len(self._store) >= self._max_size:
            oldest_key = None
            oldest_exp = None
            for k, v in self._store.items():
                if oldest_exp is None or v.expires_at < oldest_exp:
                    oldest_key = k
                    oldest_exp = v.expires_at
            if oldest_key is not None:
                self._store.pop(oldest_key, None)
        self._store[key] = _CacheItem(expires_at=self._now() + self._ttl_seconds, value=value)


_global_cache: RecommendationCache | None = None


def get_recommendation_cache(ttl_seconds: int = 300, max_size: int = 1000) -> RecommendationCache:
    global _global_cache
    if _global_cache is None:
        _global_cache = RecommendationCache(ttl_seconds=ttl_seconds, max_size=max_size)
    return _global_cache

