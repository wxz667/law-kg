from __future__ import annotations

import argparse
import json
from pathlib import Path

from .api import predict_relations
from .dataset import build_relation_testset
from .deepke_train import evaluate_pair_classifier, prepare_deepke_dataset, train_pair_classifier


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run relation-linking utilities for legal graph construction.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    predict_parser = subparsers.add_parser("predict", help="Predict graph relations from prepared samples.")
    predict_parser.add_argument("--samples", required=True, help="Path to relation_samples.jsonl")
    predict_parser.add_argument("--output", required=True, help="Path to relation_predictions.jsonl")
    predict_parser.add_argument("--model-name", default="placeholder", help="Model or adapter name")

    testset_parser = subparsers.add_parser(
        "build-testset",
        help="Build a relation testset from a segment bundle or directory.",
    )
    testset_parser.add_argument("--input", required=True, help="Path to a single .bundle-xxxx.json file or a directory containing them")
    testset_parser.add_argument("--output", required=True, help="Path to output testset jsonl file")
    testset_parser.add_argument("--refers", type=int, default=1500, help="Number of REFERS_TO samples")
    testset_parser.add_argument("--interprets", type=int, default=1500, help="Number of INTERPRETS samples")
    testset_parser.add_argument("--amends", type=int, default=200, help="Number of AMENDS samples")
    testset_parser.add_argument("--repeals", type=int, default=200, help="Number of REPEALS samples")
    testset_parser.add_argument("--none", type=int, default=600, help="Number of NONE samples")
    testset_parser.add_argument("--seed", type=int, default=7, help="Random seed for sampling")

    train_parser = subparsers.add_parser(
        "train-deepke",
        help="Prepare dataset and train a relation classifier (DeepKE-style text-pair).",
    )
    train_parser.add_argument("--input", required=True, help="Path to relation_pairs.jsonl")
    train_parser.add_argument("--workdir", required=True, help="Output directory for prepared dataset and model")
    train_parser.add_argument("--prepare-only", action="store_true", help="Only prepare train/dev/test files")
    train_parser.add_argument("--train-ratio", type=float, default=0.8)
    train_parser.add_argument("--val-ratio", type=float, default=0.1)
    train_parser.add_argument("--test-ratio", type=float, default=0.1)
    train_parser.add_argument("--no-group-by-document", action="store_true", help="Disable document-level split")
    train_parser.add_argument("--seed", type=int, default=7)
    train_parser.add_argument("--base-model", default="bert-base-chinese")
    train_parser.add_argument("--epochs", type=int, default=3)
    train_parser.add_argument("--max-steps", type=int, default=-1, help="Override max training steps (-1 disables)")
    train_parser.add_argument("--batch-size", type=int, default=8)
    train_parser.add_argument("--lr", type=float, default=2e-5)
    train_parser.add_argument("--max-length", type=int, default=256)
    train_parser.add_argument("--cpu", action="store_true")

    eval_parser = subparsers.add_parser(
        "eval-deepke",
        help="Evaluate a trained relation classifier on a labeled jsonl (relation_pairs.jsonl).",
    )
    eval_parser.add_argument("--model", required=True, help="Path to trained model directory")
    eval_parser.add_argument("--input", required=True, help="Path to labeled relation_pairs.jsonl")
    eval_parser.add_argument("--limit", type=int, default=0, help="Evaluate only first N samples (0 means all)")
    eval_parser.add_argument("--show", type=int, default=10, help="Show N wrong examples")
    eval_parser.add_argument("--batch-size", type=int, default=16)
    eval_parser.add_argument("--max-length", type=int, default=256)
    eval_parser.add_argument("--seed", type=int, default=7)
    eval_parser.add_argument("--cpu", action="store_true")
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

    if args.command == "build-testset":
        meta = build_relation_testset(
            input_path=Path(args.input),
            output_path=Path(args.output),
            refers_to_count=args.refers,
            interprets_count=args.interprets,
            amends_count=args.amends,
            repeals_count=args.repeals,
            none_count=args.none,
            seed=args.seed,
        )
        print(json.dumps(meta, ensure_ascii=False, indent=2))
        return 0

    if args.command == "train-deepke":
        workdir = Path(args.workdir)
        dataset_dir = workdir / "dataset"
        model_dir = workdir / "model"
        stats = prepare_deepke_dataset(
            input_jsonl=Path(args.input),
            output_dir=dataset_dir,
            seed=args.seed,
            train_ratio=args.train_ratio,
            val_ratio=args.val_ratio,
            test_ratio=args.test_ratio,
            group_by_document=not args.no_group_by_document,
        )
        if args.prepare_only:
            print(json.dumps(stats, ensure_ascii=False, indent=2))
            return 0
        meta = train_pair_classifier(
            dataset_dir=dataset_dir,
            output_dir=model_dir,
            base_model=args.base_model,
            seed=args.seed,
            epochs=args.epochs,
            max_steps=args.max_steps,
            batch_size=args.batch_size,
            lr=args.lr,
            max_length=args.max_length,
            use_cpu=bool(args.cpu),
        )
        print(json.dumps(meta, ensure_ascii=False, indent=2))
        return 0

    if args.command == "eval-deepke":
        report = evaluate_pair_classifier(
            model_dir=Path(args.model),
            input_jsonl=Path(args.input),
            limit=args.limit,
            show=args.show,
            seed=args.seed,
            batch_size=args.batch_size,
            max_length=args.max_length,
            use_cpu=bool(args.cpu),
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
