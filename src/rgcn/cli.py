from __future__ import annotations

import argparse
import json
from pathlib import Path

from .dataset import build_dataset
from .predict import predict
from .train import train


def main() -> int:
    parser = argparse.ArgumentParser(description="RGCN dataset/train/predict utilities.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    dataset_parser = subparsers.add_parser("build-dataset")
    dataset_parser.add_argument("--source", required=True)
    dataset_parser.add_argument("--output", required=True)

    train_parser = subparsers.add_parser("train")
    train_parser.add_argument("--dataset", required=True)
    train_parser.add_argument("--output-dir", required=True)

    predict_parser = subparsers.add_parser("predict")
    predict_parser.add_argument("--features", required=True)
    predict_parser.add_argument("--model-dir")

    args = parser.parse_args()
    if args.command == "build-dataset":
        stats = build_dataset(Path(args.source), Path(args.output))
        print(json.dumps(stats, ensure_ascii=False))
        return 0
    if args.command == "train":
        model_path = train(Path(args.dataset), Path(args.output_dir))
        print(model_path)
        return 0
    features = json.loads(Path(args.features).read_text(encoding="utf-8"))
    results = predict(features, Path(args.model_dir) if args.model_dir else None)
    print(json.dumps([result.__dict__ for result in results], ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
