from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from ..base import ProviderResponseError


@dataclass(frozen=True)
class DeepSeekConfig:
    api_key: str
    base_url: str


def load_deepseek_config() -> DeepSeekConfig:
    env_values = _load_env_values()
    api_key = env_values.get("DEEPSEEK_API_KEY", "").strip()
    base_url = env_values.get("DEEPSEEK_BASE_URL", "").strip()
    if not api_key:
        raise ProviderResponseError("Missing DEEPSEEK_API_KEY in .env.")
    if not base_url:
        raise ProviderResponseError("Missing DEEPSEEK_BASE_URL in .env.")
    return DeepSeekConfig(api_key=api_key, base_url=base_url)


def _load_env_values() -> dict[str, str]:
    merged = dict(os.environ)
    env_path = Path(__file__).resolve().parents[3] / ".env"
    if not env_path.exists():
        return merged
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in merged:
            merged[key] = value
    return merged
