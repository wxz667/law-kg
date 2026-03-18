from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..common import project_root


@dataclass(frozen=True)
class StageModelConfig:
    stage_name: str
    provider: str
    model: str
    purpose: str
    params: dict[str, Any] = field(default_factory=dict)

    def to_snapshot(self) -> dict[str, Any]:
        return {
            "stage_name": self.stage_name,
            "provider": self.provider,
            "model": self.model,
            "purpose": self.purpose,
            "params": self.params,
        }


def resolve_stage_model(stage_name: str) -> StageModelConfig:
    models = _load_models()
    stage_configs = models.get("stages", {})

    if stage_name not in stage_configs:
        raise ValueError(f"Stage model config not found for stage: {stage_name}")
    payload = stage_configs[stage_name]
    provider = payload.get("provider", "").strip()
    purpose = payload.get("purpose", "").strip()
    model = payload.get("model", "")
    if not provider:
        raise ValueError(f"Stage {stage_name} does not have a configured provider in models.json.")
    if not purpose:
        raise ValueError(f"Stage {stage_name} does not have a configured purpose in models.json.")
    if not model:
        raise ValueError(f"Stage {stage_name} does not have a configured model in models.json.")

    return StageModelConfig(
        stage_name=stage_name,
        provider=provider,
        model=model,
        purpose=purpose,
        params=dict(payload.get("params", {})),
    )


def resolve_all_stage_models() -> dict[str, dict[str, Any]]:
    models = _load_models()
    stage_configs = models.get("stages", {})
    return {
        stage_name: resolve_stage_model(stage_name).to_snapshot()
        for stage_name in stage_configs
    }


def _load_models() -> dict[str, Any]:
    path = Path(project_root()) / "resources" / "models.json"
    return json.loads(path.read_text(encoding="utf-8"))
