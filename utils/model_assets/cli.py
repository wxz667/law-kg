from __future__ import annotations

import argparse
import json
from dataclasses import replace
from pathlib import Path

from .hub import build_asset_summary, download_asset_bundle, publish_asset_bundle
from .registry import DEFAULT_CONFIG_PATH, resolve_asset_spec


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download or publish local model assets with config-driven settings.")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--download", metavar="MODEL_NAME", help="Download the named model asset from Hugging Face.")
    action.add_argument("--publish", metavar="MODEL_NAME", help="Publish the named model asset to Hugging Face.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to project config file.")
    parser.add_argument("--model", action="store_true", help="Only operate on the model artifact.")
    parser.add_argument("--dataset", action="store_true", help="Only operate on the dataset artifact.")
    parser.add_argument("--model-dir", help="Optional local model directory override.")
    parser.add_argument("--dataset-dir", help="Optional local dataset directory override.")
    parser.add_argument("--model-repo-id", help="Optional model repo_id override.")
    parser.add_argument("--dataset-repo-id", help="Optional dataset repo_id override.")
    parser.add_argument("--model-revision", help="Optional model revision override.")
    parser.add_argument("--dataset-revision", help="Optional dataset revision override.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    asset_name = str(args.download or args.publish).strip()
    spec = resolve_asset_spec(asset_name, Path(args.config))
    spec = apply_overrides(
        spec,
        model_dir=args.model_dir,
        dataset_dir=args.dataset_dir,
        model_repo_id=args.model_repo_id,
        dataset_repo_id=args.dataset_repo_id,
        model_revision=args.model_revision,
        dataset_revision=args.dataset_revision,
    )
    include_model, include_dataset = resolve_asset_selection(model=args.model, dataset=args.dataset)

    if args.download:
        artifacts = download_asset_bundle(spec, include_model=include_model, include_dataset=include_dataset)
        print(json.dumps(build_asset_summary(action="download", asset_name=spec.name, paths=artifacts), ensure_ascii=False, indent=2))
        return 0

    artifacts = publish_asset_bundle(spec, include_model=include_model, include_dataset=include_dataset)
    print(json.dumps(build_asset_summary(action="publish", asset_name=spec.name, paths=artifacts), ensure_ascii=False, indent=2))
    return 0


def apply_overrides(
    spec,
    *,
    model_dir: str | None,
    dataset_dir: str | None,
    model_repo_id: str | None,
    dataset_repo_id: str | None,
    model_revision: str | None,
    dataset_revision: str | None,
):
    return replace(
        spec,
        model_dir=Path(model_dir) if model_dir else spec.model_dir,
        dataset_dir=Path(dataset_dir) if dataset_dir else spec.dataset_dir,
        model_repo_id=str(model_repo_id).strip() if model_repo_id else spec.model_repo_id,
        dataset_repo_id=str(dataset_repo_id).strip() if dataset_repo_id else spec.dataset_repo_id,
        model_revision=str(model_revision).strip() if model_revision else spec.model_revision,
        dataset_revision=str(dataset_revision).strip() if dataset_revision else spec.dataset_revision,
    )


def resolve_asset_selection(*, model: bool, dataset: bool) -> tuple[bool, bool]:
    if not model and not dataset:
        return True, True
    return bool(model), bool(dataset)


if __name__ == "__main__":
    raise SystemExit(main())
