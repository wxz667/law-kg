from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_CONFIG_PATH = Path("configs/config.json")


@dataclass(frozen=True)
class ModelAssetSpec:
    name: str
    model_dir: Path
    dataset_dir: Path
    model_repo_id: str
    model_revision: str
    dataset_repo_id: str
    dataset_revision: str


def load_project_config(config_path: Path | None = None) -> dict[str, Any]:
    path = (config_path or DEFAULT_CONFIG_PATH).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path.name} must contain a JSON object at top level.")
    return payload


def list_supported_assets() -> list[str]:
    return ["interprets_filter"]


def resolve_asset_spec(asset_name: str, config_path: Path | None = None) -> ModelAssetSpec:
    normalized = str(asset_name).strip()
    if normalized != "interprets_filter":
        supported = ", ".join(list_supported_assets())
        raise ValueError(f"Unsupported model asset: {asset_name}. Supported assets: {supported}")
    config = load_project_config(config_path)
    return resolve_interprets_filter_spec(config)


def resolve_interprets_filter_spec(config: dict[str, Any]) -> ModelAssetSpec:
    module_config = dict(config.get("interprets_filter", {}))
    predict = dict(module_config.get("predict", {}))
    hub = dict(module_config.get("hub", {}))
    model_dir = Path(str(predict.get("default_model_dir", "models/interprets_filter")))
    dataset_dir = Path("data/train/interprets_filter")
    return ModelAssetSpec(
        name="interprets_filter",
        model_dir=model_dir,
        dataset_dir=dataset_dir,
        model_repo_id=str(hub.get("model_repo_id", "")).strip(),
        model_revision=str(hub.get("model_revision", "main")).strip() or "main",
        dataset_repo_id=str(hub.get("dataset_repo_id", "")).strip(),
        dataset_revision=str(hub.get("dataset_revision", "main")).strip() or "main",
    )
