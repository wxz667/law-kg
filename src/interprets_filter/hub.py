from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from huggingface_hub import snapshot_download

from .config import load_interprets_filter_config


@dataclass(frozen=True)
class HubAssetConfig:
    model_repo_id: str
    model_revision: str
    dataset_repo_id: str
    dataset_revision: str


def resolve_hub_asset_config(config_path: Path | None = None) -> HubAssetConfig:
    config = load_interprets_filter_config(config_path)
    hub = dict(config.hub)
    return HubAssetConfig(
        model_repo_id=str(hub.get("model_repo_id", "")).strip(),
        model_revision=str(hub.get("model_revision", "main")).strip() or "main",
        dataset_repo_id=str(hub.get("dataset_repo_id", "")).strip(),
        dataset_revision=str(hub.get("dataset_revision", "main")).strip() or "main",
    )


def download_assets(
    *,
    model_repo_id: str,
    dataset_repo_id: str,
    model_dir: Path,
    dataset_dir: Path,
    model_revision: str = "main",
    dataset_revision: str = "main",
    include_model: bool = True,
    include_dataset: bool = True,
) -> dict[str, str]:
    downloaded: dict[str, str] = {}
    if include_model:
        if not model_repo_id:
            raise ValueError("Missing model_repo_id. Set interprets_filter.hub.model_repo_id or pass --model-repo-id.")
        model_dir.mkdir(parents=True, exist_ok=True)
        downloaded["model"] = snapshot_download(
            repo_id=model_repo_id,
            local_dir=model_dir,
            revision=model_revision,
            ignore_patterns=[
                "checkpoints/**",
                "**/checkpoint-*",
                "optimizer.pt",
                "scheduler.pt",
                "rng_state.pth",
                "trainer_state.json",
                "training_args.bin",
            ],
        )
    if include_dataset:
        if not dataset_repo_id:
            raise ValueError(
                "Missing dataset_repo_id. Set interprets_filter.hub.dataset_repo_id or pass --dataset-repo-id."
            )
        dataset_dir.mkdir(parents=True, exist_ok=True)
        downloaded["dataset"] = snapshot_download(
            repo_id=dataset_repo_id,
            repo_type="dataset",
            local_dir=dataset_dir,
            revision=dataset_revision,
        )
    return downloaded


def build_download_summary(downloaded: dict[str, str]) -> dict[str, Any]:
    return {
        "downloaded": bool(downloaded),
        "artifacts": {key: str(value) for key, value in downloaded.items()},
    }
