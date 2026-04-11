from __future__ import annotations

from pathlib import Path
from typing import Any

from huggingface_hub import HfApi, snapshot_download

from .registry import ModelAssetSpec


MODEL_IGNORE_PATTERNS = [
    "checkpoints/**",
    "**/checkpoint-*",
    "optimizer.pt",
    "scheduler.pt",
    "rng_state.pth",
    "trainer_state.json",
    "training_args.bin",
]


def download_asset_bundle(
    spec: ModelAssetSpec,
    *,
    include_model: bool = True,
    include_dataset: bool = True,
) -> dict[str, str]:
    downloaded: dict[str, str] = {}
    if include_model:
        ensure_repo_id(spec.model_repo_id, kind="model", asset_name=spec.name)
        spec.model_dir.mkdir(parents=True, exist_ok=True)
        downloaded["model"] = snapshot_download(
            repo_id=spec.model_repo_id,
            local_dir=spec.model_dir,
            revision=spec.model_revision,
            ignore_patterns=list(MODEL_IGNORE_PATTERNS),
        )
    if include_dataset:
        ensure_repo_id(spec.dataset_repo_id, kind="dataset", asset_name=spec.name)
        spec.dataset_dir.mkdir(parents=True, exist_ok=True)
        downloaded["dataset"] = snapshot_download(
            repo_id=spec.dataset_repo_id,
            repo_type="dataset",
            local_dir=spec.dataset_dir,
            revision=spec.dataset_revision,
        )
    return downloaded


def publish_asset_bundle(
    spec: ModelAssetSpec,
    *,
    include_model: bool = True,
    include_dataset: bool = True,
) -> dict[str, str]:
    api = HfApi()
    published: dict[str, str] = {}
    if include_model:
        ensure_repo_id(spec.model_repo_id, kind="model", asset_name=spec.name)
        ensure_local_dir(spec.model_dir, kind="model", asset_name=spec.name)
        api.create_repo(repo_id=spec.model_repo_id, exist_ok=True)
        published["model"] = str(
            api.upload_folder(
                repo_id=spec.model_repo_id,
                folder_path=spec.model_dir,
                ignore_patterns=list(MODEL_IGNORE_PATTERNS),
            )
        )
    if include_dataset:
        ensure_repo_id(spec.dataset_repo_id, kind="dataset", asset_name=spec.name)
        ensure_local_dir(spec.dataset_dir, kind="dataset", asset_name=spec.name)
        api.create_repo(repo_id=spec.dataset_repo_id, repo_type="dataset", exist_ok=True)
        published["dataset"] = str(
            api.upload_folder(
                repo_id=spec.dataset_repo_id,
                repo_type="dataset",
                folder_path=spec.dataset_dir,
            )
        )
    return published


def build_asset_summary(
    *,
    action: str,
    asset_name: str,
    paths: dict[str, str],
) -> dict[str, Any]:
    return {
        "action": action,
        "asset": asset_name,
        "completed": bool(paths),
        "artifacts": {key: str(value) for key, value in paths.items()},
    }


def ensure_repo_id(repo_id: str, *, kind: str, asset_name: str) -> None:
    if str(repo_id).strip():
        return
    raise ValueError(f"Missing {kind}_repo_id for {asset_name}. Fill it in configs/config.json before running.")


def ensure_local_dir(path: Path, *, kind: str, asset_name: str) -> None:
    if path.exists():
        return
    raise FileNotFoundError(f"Missing local {kind} directory for {asset_name}: {path}")
