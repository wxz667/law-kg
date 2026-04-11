from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import torch
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from torch.utils.data import Dataset
from transformers import AutoModelForSequenceClassification, AutoTokenizer, Trainer, TrainingArguments, set_seed

from .config import LABELS, canonical_label, load_interprets_filter_config
from .io import read_jsonl, write_json


class InterpretDataset(Dataset):
    def __init__(self, rows: list[dict[str, Any]], tokenizer: Any, label_map: dict[str, int], max_length: int) -> None:
        self.rows = rows
        self.tokenizer = tokenizer
        self.label_map = label_map
        self.max_length = max_length

    def __len__(self) -> int:
        return len(self.rows)

    def __getitem__(self, index: int) -> dict[str, Any]:
        row = self.rows[index]
        encoded = self.tokenizer(
            str(row["text"]),
            truncation=True,
            max_length=self.max_length,
            padding="max_length",
        )
        encoded["labels"] = self.label_map[canonical_label(row["label"])]
        return encoded


def compute_metrics_from_probabilities(probabilities: np.ndarray, labels: np.ndarray, threshold: float) -> dict[str, float]:
    predictions = (probabilities >= threshold).astype(int)
    return {
        "accuracy": float(accuracy_score(labels, predictions)),
        "precision": float(precision_score(labels, predictions, zero_division=0)),
        "recall": float(recall_score(labels, predictions, zero_division=0)),
        "f1": float(f1_score(labels, predictions, zero_division=0)),
    }


def evaluate_thresholds(
    probabilities: np.ndarray,
    labels: np.ndarray,
    thresholds: list[float],
    *,
    selection_mode: str,
    target_recall: float,
    target_precision: float,
) -> tuple[float, dict[str, float], list[dict[str, float]]]:
    evaluations: list[dict[str, float]] = []
    for threshold in thresholds:
        metrics = compute_metrics_from_probabilities(probabilities, labels, threshold)
        evaluations.append({"threshold": threshold, **metrics})
    if selection_mode == "precision_first":
        eligible = [
            item
            for item in evaluations
            if item["precision"] >= target_precision and item["recall"] >= target_recall
        ]
        if not eligible:
            eligible = [item for item in evaluations if item["precision"] >= target_precision]
        ranking_pool = eligible or evaluations
        selected = max(ranking_pool, key=lambda item: (item["precision"], item["recall"], item["f1"], item["accuracy"], item["threshold"]))
    else:
        eligible = [item for item in evaluations if item["recall"] >= target_recall]
        ranking_pool = eligible or evaluations
        selected = max(ranking_pool, key=lambda item: (item["recall"], item["f1"], item["precision"], item["accuracy"], -item["threshold"]))
    return float(selected["threshold"]), {key: float(value) for key, value in selected.items() if key != "threshold"}, evaluations


def train(
    dataset_dir: Path,
    output_dir: Path,
    config_path: Path | None = None,
) -> Path:
    config = load_interprets_filter_config(config_path)
    train_config = config.train

    train_rows = read_jsonl(dataset_dir / "train.jsonl")
    dev_rows = read_jsonl(dataset_dir / "dev.jsonl")
    test_rows = read_jsonl(dataset_dir / "test.jsonl")
    if not train_rows:
        raise ValueError(f"No training rows found under {dataset_dir}")

    label_map = {label: index for index, label in enumerate(LABELS)}
    positive_index = label_map["true"]
    max_length = int(train_config.get("max_length", 512))
    backbone = str(train_config.get("backbone_model", "hfl/chinese-roberta-wwm-ext"))
    seed = int(train_config.get("seed", 42))
    device_preference = str(train_config.get("device_preference", "cuda"))
    use_cuda = device_preference == "cuda" and torch.cuda.is_available()
    device_name = "cuda" if use_cuda else "cpu"
    mixed_precision = str(train_config.get("mixed_precision", "auto"))
    pin_memory = bool(train_config.get("dataloader_pin_memory", use_cuda))
    dataloader_num_workers = int(train_config.get("dataloader_num_workers", 0))
    precision_flags = resolve_mixed_precision(use_cuda, mixed_precision)
    thresholds = [float(value) for value in train_config.get("threshold_grid", [0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6])]
    target_recall = float(train_config.get("target_recall", 0.9))
    target_precision = float(train_config.get("target_precision", 0.85))
    selection_mode = str(train_config.get("threshold_selection_mode", "precision_first"))
    set_seed(seed)

    tokenizer = AutoTokenizer.from_pretrained(backbone)
    model = AutoModelForSequenceClassification.from_pretrained(
        backbone,
        num_labels=len(LABELS),
        id2label={index: label for label, index in label_map.items()},
        label2id=label_map,
    )

    train_dataset = InterpretDataset(train_rows, tokenizer, label_map, max_length)
    dev_dataset = InterpretDataset(dev_rows, tokenizer, label_map, max_length) if dev_rows else None
    test_dataset = InterpretDataset(test_rows, tokenizer, label_map, max_length) if test_rows else None

    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoints_dir = output_dir / "checkpoints"
    checkpoints_dir.mkdir(parents=True, exist_ok=True)
    args = TrainingArguments(
        output_dir=str(checkpoints_dir),
        learning_rate=float(train_config.get("learning_rate", 2e-5)),
        per_device_train_batch_size=int(train_config.get("train_batch_size", 4)),
        per_device_eval_batch_size=int(train_config.get("eval_batch_size", 8)),
        num_train_epochs=float(train_config.get("num_train_epochs", 1)),
        weight_decay=float(train_config.get("weight_decay", 0.01)),
        warmup_ratio=float(train_config.get("warmup_ratio", 0.1)),
        logging_steps=int(train_config.get("logging_steps", 10)),
        save_total_limit=int(train_config.get("save_total_limit", 1)),
        eval_strategy="epoch" if dev_dataset is not None else "no",
        save_strategy="epoch" if dev_dataset is not None else "no",
        load_best_model_at_end=dev_dataset is not None,
        metric_for_best_model="eval_recall",
        greater_is_better=True,
        report_to=[],
        seed=seed,
        do_train=True,
        do_eval=dev_dataset is not None,
        use_cpu=not use_cuda,
        dataloader_pin_memory=pin_memory,
        dataloader_num_workers=dataloader_num_workers,
        fp16=precision_flags["fp16"],
        bf16=precision_flags["bf16"],
    )

    def compute_eval_metrics(eval_prediction: tuple[np.ndarray, np.ndarray]) -> dict[str, float]:
        logits, labels = eval_prediction
        probabilities = torch.softmax(torch.tensor(logits), dim=-1).numpy()[:, positive_index]
        return compute_metrics_from_probabilities(probabilities, np.asarray(labels), 0.5)

    trainer = Trainer(
        model=model,
        args=args,
        train_dataset=train_dataset,
        eval_dataset=dev_dataset,
        processing_class=tokenizer,
        compute_metrics=compute_eval_metrics if dev_dataset is not None else None,
    )
    trainer.train()
    trainer.save_model(output_dir)
    tokenizer.save_pretrained(output_dir)

    metrics: dict[str, Any] = {
        "task": "interprets_filter",
        "device": device_name,
        "torch_version": torch.__version__,
        "torch_cuda": torch.version.cuda,
        "mixed_precision": precision_flags["mode"],
    }
    if use_cuda:
        metrics["gpu"] = {
            "name": torch.cuda.get_device_name(0),
            "device_count": torch.cuda.device_count(),
            "capability": list(torch.cuda.get_device_capability(0)),
        }

    selected_threshold = float(train_config.get("default_threshold", 0.5))
    dev_threshold_metrics: dict[str, float] = {}
    threshold_report: list[dict[str, float]] = []
    if dev_dataset is not None:
        dev_prediction = trainer.predict(dev_dataset)
        dev_probabilities = torch.softmax(torch.tensor(dev_prediction.predictions), dim=-1).numpy()[:, positive_index]
        dev_labels = np.asarray(dev_prediction.label_ids)
        selected_threshold, dev_threshold_metrics, threshold_report = evaluate_thresholds(
            dev_probabilities,
            dev_labels,
            thresholds,
            selection_mode=selection_mode,
            target_recall=target_recall,
            target_precision=target_precision,
        )
        metrics["dev"] = dev_threshold_metrics
        metrics["dev_thresholds"] = threshold_report

    if test_dataset is not None:
        test_prediction = trainer.predict(test_dataset)
        test_probabilities = torch.softmax(torch.tensor(test_prediction.predictions), dim=-1).numpy()[:, positive_index]
        test_labels = np.asarray(test_prediction.label_ids)
        metrics["test"] = compute_metrics_from_probabilities(test_probabilities, test_labels, selected_threshold)

    metrics["selected_threshold"] = selected_threshold
    metrics["target_recall"] = target_recall
    metrics["target_precision"] = target_precision
    metrics["threshold_selection_mode"] = selection_mode

    write_json(output_dir / "label_map.json", label_map)
    write_json(output_dir / "metrics.json", metrics)
    write_json(output_dir / "training_args.json", train_config)
    write_json(
        output_dir / "model_card.json",
        {
            "task": "interprets_filter",
            "backbone_model": backbone,
            "device": device_name,
            "mixed_precision": precision_flags["mode"],
            "labels": [False, True],
            "selected_threshold": selected_threshold,
            "train_samples": len(train_rows),
            "dev_samples": len(dev_rows),
            "test_samples": len(test_rows),
        },
    )
    return output_dir


def resolve_mixed_precision(use_cuda: bool, mixed_precision: str) -> dict[str, bool | str]:
    if not use_cuda:
        return {"fp16": False, "bf16": False, "mode": "no"}
    if mixed_precision == "fp16":
        return {"fp16": True, "bf16": False, "mode": "fp16"}
    if mixed_precision == "bf16":
        return {"fp16": False, "bf16": True, "mode": "bf16"}
    if mixed_precision == "no":
        return {"fp16": False, "bf16": False, "mode": "no"}
    if torch.cuda.is_bf16_supported():
        return {"fp16": False, "bf16": True, "mode": "bf16"}
    return {"fp16": True, "bf16": False, "mode": "fp16"}
