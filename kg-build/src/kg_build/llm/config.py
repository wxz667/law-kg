from __future__ import annotations

import os
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..common import project_root

LEGACY_MODEL_ENV_MAP = {
    "summarize": "OPENAI_MODEL_SUMMARIZE",
    "extract": "OPENAI_MODEL_EXTRACT",
    "aggr": "OPENAI_MODEL_AGGR",
    "conv": "OPENAI_MODEL_CONV",
    "embed": "OPENAI_EMBEDDING_MODEL",
    "dedup": "OPENAI_MODEL_DEDUP",
    "pred": "OPENAI_MODEL_PRED",
}


@dataclass(frozen=True)
class StageModelConfig:
    stage_name: str
    provider: str
    model: str
    purpose: str
    base_url: str
    api_key: str
    params: dict[str, Any] = field(default_factory=dict)
    base_url_env: str = ""
    api_key_env: str = ""
    model_env: str = ""

    def to_snapshot(self) -> dict[str, Any]:
        return {
            "stage_name": self.stage_name,
            "provider": self.provider,
            "model": self.model,
            "purpose": self.purpose,
            "base_url": self.base_url,
            "base_url_env": self.base_url_env,
            "api_key_env": self.api_key_env,
            "api_key_configured": bool(self.api_key),
            "params": self.params,
        }


def resolve_stage_model(stage_name: str, env: dict[str, str] | None = None) -> StageModelConfig:
    env_values = env or dict(os.environ)
    models = _load_models()
    if stage_name not in models:
        raise ValueError(f"Stage model config not found for stage: {stage_name}")
    payload = models[stage_name]
    provider = payload.get("provider", "openai_compatible")
    purpose = payload.get("purpose", stage_name)
    base_url_env = payload.get("base_url_env", "OPENAI_BASE_URL")
    api_key_env = payload.get("api_key_env", "OPENAI_API_KEY")
    model_env = payload.get("model_env", LEGACY_MODEL_ENV_MAP.get(stage_name, ""))

    configured_model = payload.get("model", "")
    model = (env_values.get(model_env, "") if model_env else "") or configured_model
    if not model:
        raise ValueError(
            f"Stage {stage_name} does not have a resolved model. "
            f"Configure models.json or set {model_env or 'a model env var'}."
        )

    base_url = env_values.get(base_url_env, "") or env_values.get("OPENAI_BASE_URL", "")
    api_key = env_values.get(api_key_env, "") or env_values.get("OPENAI_API_KEY", "")

    return StageModelConfig(
        stage_name=stage_name,
        provider=provider,
        model=model,
        purpose=purpose,
        base_url=base_url,
        api_key=api_key,
        params=dict(payload.get("params", {})),
        base_url_env=base_url_env,
        api_key_env=api_key_env,
        model_env=model_env,
    )


def resolve_all_stage_models(env: dict[str, str] | None = None) -> dict[str, dict[str, Any]]:
    models = _load_models()
    return {
        stage_name: resolve_stage_model(stage_name, env=env).to_snapshot()
        for stage_name in models
    }


def _load_models() -> dict[str, Any]:
    path = Path(project_root()) / "resources" / "models.json"
    return json.loads(path.read_text(encoding="utf-8"))
