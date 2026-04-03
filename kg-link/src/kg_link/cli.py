from __future__ import annotations

import argparse
import json
from pathlib import Path

from .api import predict_relations


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run relation-linking utilities for legal graph construction.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    predict_parser = subparsers.add_parser("predict", help="Predict graph relations from prepared samples.")
    predict_parser.add_argument("--samples", required=True, help="Path to relation_samples.jsonl")
    predict_parser.add_argument("--output", required=True, help="Path to relation_predictions.jsonl")
    predict_parser.add_argument("--model-name", default="placeholder", help="Model or adapter name")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "predict":
        predictions = predict_relations(
            samples_path=Path(args.samples),
            output_path=Path(args.output),
            model_name=args.model_name,
        )
        print(json.dumps({"prediction_count": len(predictions)}, ensure_ascii=False, indent=2))
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
