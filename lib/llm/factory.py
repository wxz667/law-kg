from __future__ import annotations

from .base import ProviderClient, ProviderRequestConfig, ProviderResponseError
from .bigmodel.client import BigModelClient
from .deepseek.client import DeepSeekClient


def build_provider_client(config: ProviderRequestConfig) -> ProviderClient:
    if config.provider == "deepseek":
        return DeepSeekClient(config)
    if config.provider == "bigmodel":
        return BigModelClient(config)
    raise ProviderResponseError(f"Unsupported provider: {config.provider}")
