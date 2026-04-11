from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


LABELS = ("false", "true")
DEFAULT_CONFIG_PATH = Path("configs/config.json")


@dataclass(frozen=True)
class InterpretsFilterConfig:
    dataset: dict[str, Any]
    distill: dict[str, Any]
    train: dict[str, Any]
    predict: dict[str, Any]
    hub: dict[str, Any]


@dataclass(frozen=True)
class DistillRuntimeConfig:
    provider: str
    model: str
    batch_size: int
    concurrent_requests: int
    request_timeout_seconds: int
    max_retries: int
    params: dict[str, Any]


def load_interprets_filter_config(config_path: Path | None = None) -> InterpretsFilterConfig:
    path = (config_path or DEFAULT_CONFIG_PATH).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    module_config = payload.get("interprets_filter")
    if not isinstance(module_config, dict):
        raise ValueError(f"{path.name} is missing top-level 'interprets_filter' configuration.")
    required = ("dataset", "distill", "train", "predict")
    missing = [key for key in required if key not in module_config or not isinstance(module_config[key], dict)]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"{path.name} is missing interprets_filter sections: {joined}")
    return InterpretsFilterConfig(
        dataset=dict(module_config["dataset"]),
        distill=dict(module_config["distill"]),
        train=dict(module_config["train"]),
        predict=dict(module_config["predict"]),
        hub=dict(module_config.get("hub", {})) if isinstance(module_config.get("hub", {}), dict) else {},
    )


def canonical_label(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    compact = str(value).strip().lower()
    if compact in {"true", "1", "yes", "y", "interprets"}:
        return "true"
    return "false"


def label_to_bool(value: Any) -> bool:
    return canonical_label(value) == "true"


def resolve_distill_runtime_config(distill: dict[str, Any]) -> DistillRuntimeConfig:
    provider = str(distill.get("provider", "")).strip()
    model = str(distill.get("model", "")).strip()
    if not provider or not model:
        raise ValueError("interprets_filter.distill must define non-empty provider and model.")

    batch_size = max(int(distill.get("batch_size", 1)), 1)
    concurrent_requests = max(int(distill.get("concurrent_requests", 1)), 1)
    request_timeout_seconds = max(int(distill.get("request_timeout_seconds", 60)), 1)
    max_retries = max(int(distill.get("max_retries", 2)), 1)
    params = dict(distill.get("params", {}))
    params.pop("timeout_seconds", None)
    params.pop("max_retries", None)
    params.setdefault("temperature", 0.0)
    params.setdefault("max_tokens", 512)
    max_tokens = max(int(params.get("max_tokens", 512)), 1)
    minimum_required_tokens = 32 + (batch_size * 56)
    if max_tokens < minimum_required_tokens:
        raise ValueError(
            "interprets_filter.distill params are inconsistent: "
            f"batch_size={batch_size} requires max_tokens>={minimum_required_tokens}, got {max_tokens}."
        )
    return DistillRuntimeConfig(
        provider=provider,
        model=model,
        batch_size=batch_size,
        concurrent_requests=concurrent_requests,
        request_timeout_seconds=request_timeout_seconds,
        max_retries=max_retries,
        params=params,
    )
