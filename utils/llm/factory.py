from __future__ import annotations

from .base import ProviderClient, ProviderRequestConfig, ProviderResponseError


def build_provider_client(config: ProviderRequestConfig) -> ProviderClient:
    if config.provider == "deepseek":
        from .deepseek.client import DeepSeekClient

        return DeepSeekClient(config)
    if config.provider == "siliconflow":
        from .siliconflow.client import SiliconFlowClient

        return SiliconFlowClient(config)
    if config.provider == "bigmodel":
        from .bigmodel.client import BigModelClient

        return BigModelClient(config)
    raise ProviderResponseError(f"Unsupported provider: {config.provider}")
