from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


class ProviderResponseError(RuntimeError):
    """Provider-level error wrapper independent from any algorithm stage."""


@dataclass(frozen=True)
class ProviderRequestConfig:
    provider: str
    model: str
    params: dict[str, Any] = field(default_factory=dict)


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
