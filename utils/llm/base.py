from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


LOCAL_REQUEST_PARAM_KEYS = frozenset(
    {
        "timeout_seconds",
        "max_retries",
        "rate_limit",
        "rpm",
        "tpm",
        "rate_limit_window_seconds",
        "rate_limit_retries",
        "rate_limit_backoff_seconds",
    }
)


class ProviderResponseError(RuntimeError):
    """Provider-level error wrapper independent from any algorithm stage."""


@dataclass(frozen=True)
class ProviderRequestConfig:
    provider: str
    model: str
    params: dict[str, Any] = field(default_factory=dict)
    local: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        validate_provider_api_params(self.params)


def validate_provider_api_params(params: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(params or {})
    invalid_keys = sorted(set(payload).intersection(LOCAL_REQUEST_PARAM_KEYS))
    if invalid_keys:
        joined = ", ".join(invalid_keys)
        raise ValueError(f"provider request params must not contain local-only keys: {joined}")
    return payload


def build_provider_request_config(
    *,
    provider: str,
    model: str,
    params: dict[str, Any] | None = None,
    timeout_seconds: int | None = None,
    max_retries: int | None = None,
    rate_limit: dict[str, Any] | None = None,
) -> ProviderRequestConfig:
    local: dict[str, Any] = {}
    if timeout_seconds is not None:
        local["timeout_seconds"] = max(int(timeout_seconds), 1)
    if max_retries is not None:
        local["max_retries"] = max(int(max_retries), 1)
    if rate_limit:
        local["rate_limit"] = {
            "rpm": max(int(rate_limit.get("rpm", 0) or 0), 0),
            "tpm": max(int(rate_limit.get("tpm", 0) or 0), 0),
            "window_seconds": max(float(rate_limit.get("window_seconds", 60.0) or 60.0), 0.001),
            "retry_count": max(int(rate_limit.get("retry_count", 3) or 3), 1),
            "backoff_seconds": max(float(rate_limit.get("backoff_seconds", 5.0) or 5.0), 0.1),
        }
    return ProviderRequestConfig(
        provider=provider,
        model=model,
        params=validate_provider_api_params(params),
        local=local,
    )


class ProviderClient(Protocol):
    def generate_text(
        self,
        messages: list[dict[str, str]],
        model: str,
        **params: Any,
    ) -> str: ...

    def embed_texts(
        self,
        texts: list[str],
        model: str,
        **params: Any,
    ) -> list[list[float]]: ...
