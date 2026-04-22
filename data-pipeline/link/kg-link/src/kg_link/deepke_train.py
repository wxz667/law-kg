from __future__ import annotations

import json
import inspect
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator


@dataclass(frozen=True)
class PairExample:
    sample_id: str
    text_a: str
    text_b: str
    label: str
    document_node_id: str


def load_pair_examples(jsonl_path: Path) -> list[PairExample]:
    examples: list[PairExample] = []
    for line in jsonl_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        meta = row.get("metadata", {}) or {}
        text_b = str(row.get("target_text", "") or "").strip()
        if not text_b:
            text_b = str(meta.get("target_name", "") or "").strip()
        if not text_b:
            text_b = str(row.get("target_node_id", "") or "").strip()
        examples.append(
            PairExample(
                sample_id=str(row.get("sample_id", "")),
                text_a=str(row.get("source_text", "")),
                text_b=text_b,
                label=str(row.get("relation_type", "")),
                document_node_id=str(meta.get("document_node_id", "")),
            )
        )
    return examples


def split_examples(
    *,
    examples: list[PairExample],
    seed: int,
    train_ratio: float,
    val_ratio: float,
    test_ratio: float,
    group_by_document: bool,
) -> tuple[list[PairExample], list[PairExample], list[PairExample]]:
    if train_ratio <= 0 or val_ratio < 0 or test_ratio < 0:
        raise ValueError("Invalid split ratios.")
    total = train_ratio + val_ratio + test_ratio
    if total <= 0:
        raise ValueError("Invalid split ratios.")

    rng = random.Random(seed)
    if not group_by_document:
        shuffled = examples[:]
        rng.shuffle(shuffled)
        n = len(shuffled)
        n_train = int(n * (train_ratio / total))
        n_val = int(n * (val_ratio / total))
        train = shuffled[:n_train]
        val = shuffled[n_train : n_train + n_val]
        test = shuffled[n_train + n_val :]
        return train, val, test

    by_doc: dict[str, list[PairExample]] = {}
    for ex in examples:
        key = ex.document_node_id or "__unknown__"
        by_doc.setdefault(key, []).append(ex)

    doc_ids = list(by_doc.keys())
    rng.shuffle(doc_ids)

    train: list[PairExample] = []
    val: list[PairExample] = []
    test: list[PairExample] = []
    counts = {"train": 0, "val": 0, "test": 0}
    target_train = train_ratio / total
    target_val = val_ratio / total

    total_count = len(examples)
    for doc_id in doc_ids:
        chunk = by_doc[doc_id]
        frac_train = counts["train"] / total_count if total_count else 0.0
        frac_val = counts["val"] / total_count if total_count else 0.0

        if frac_train < target_train:
            train.extend(chunk)
            counts["train"] += len(chunk)
            continue
        if frac_val < target_val:
            val.extend(chunk)
            counts["val"] += len(chunk)
            continue
        test.extend(chunk)
        counts["test"] += len(chunk)

    return train, val, test


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(row, ensure_ascii=False) for row in rows]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        yield json.loads(line)


def _batched(items: list[Any], batch_size: int) -> Iterator[list[Any]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


def evaluate_pair_classifier(
    *,
    model_dir: Path,
    input_jsonl: Path,
    output_jsonl: Path | None = None,
    limit: int = 0,
    show: int = 10,
    seed: int = 7,
    batch_size: int = 16,
    max_length: int = 256,
    use_cpu: bool = False,
) -> dict[str, Any]:
    try:
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer
    except Exception as e:
        raise RuntimeError("Missing runtime dependencies. Install: pip install -e kg-link[train]") from e

    rows = list(iter_jsonl(input_jsonl))
    if limit and limit > 0:
        rows = rows[:limit]

    pairs: list[tuple[str, str]] = []
    gold: list[str] = []
    ids: list[str] = []

    for row in rows:
        meta = row.get("metadata", {}) or {}
        text_a = str(row.get("source_text", "") or "").strip()
        text_b = str(row.get("target_text", "") or "").strip()
        if not text_b:
            text_b = str(meta.get("target_name", "") or "").strip()
        if not text_b:
            text_b = str(row.get("target_node_id", "") or "").strip()
        label = str(row.get("relation_type", "") or "").strip()
        sample_id = str(row.get("sample_id", "") or "").strip()
        if not (text_a and text_b and label and sample_id):
            continue
        pairs.append((text_a, text_b))
        gold.append(label)
        ids.append(sample_id)

    tokenizer = AutoTokenizer.from_pretrained(str(model_dir), use_fast=True)
    model = AutoModelForSequenceClassification.from_pretrained(str(model_dir))
    device = torch.device("cpu") if use_cpu or not torch.cuda.is_available() else torch.device("cuda")
    model.to(device)
    model.eval()

    id2label = getattr(model.config, "id2label", None) or {}
    if not id2label:
        label_map = json.loads((model_dir / "labels.json").read_text(encoding="utf-8"))
        id2label = {v: k for k, v in label_map.items()}

    pred_labels: list[str] = []
    pred_scores: list[float] = []

    with torch.no_grad():
        for batch in _batched(pairs, batch_size=batch_size):
            a = [x[0] for x in batch]
            b = [x[1] for x in batch]
            enc = tokenizer(
                a,
                b,
                truncation=True,
                padding=True,
                max_length=max_length,
                return_tensors="pt",
            )
            enc = {k: v.to(device) for k, v in enc.items()}
            out = model(**enc)
            logits = out.logits
            probs = torch.softmax(logits, dim=-1)
            conf, idx = torch.max(probs, dim=-1)
            for i in range(idx.shape[0]):
                label_id = int(idx[i].item())
                pred_labels.append(str(id2label.get(label_id, label_id)))
                pred_scores.append(float(conf[i].item()))

    correct = sum(1 for g, p in zip(gold, pred_labels) if g == p)
    total = len(gold)
    accuracy = (correct / total) if total else 0.0

    labels = sorted(set(gold) | set(pred_labels))
    confusion: dict[str, dict[str, int]] = {g: {p: 0 for p in labels} for g in labels}
    for g, p in zip(gold, pred_labels):
        confusion[g][p] += 1

    examples = list(zip(ids, gold, pred_labels, pred_scores, pairs))
    rng = random.Random(seed)
    wrong = [e for e in examples if e[1] != e[2]]
    rng.shuffle(wrong)
    preview = wrong[: max(show, 0)] if show else []

    if output_jsonl is not None:
        out_rows: list[dict[str, Any]] = []
        for i in range(len(ids)):
            out_rows.append(
                {
                    "sample_id": ids[i],
                    "gold": gold[i],
                    "pred": pred_labels[i],
                    "score": pred_scores[i],
                }
            )
        write_jsonl(output_jsonl, out_rows)

    report = {
        "input": str(input_jsonl.resolve()),
        "model_dir": str(model_dir.resolve()),
        "counts": {"evaluated": total, "correct": correct},
        "accuracy": accuracy,
        "labels": labels,
        "confusion": confusion,
        "wrong_examples": [
            {
                "sample_id": sid,
                "gold": g,
                "pred": p,
                "score": s,
                "text_a": pair[0][:200],
                "text_b": pair[1][:200],
            }
            for sid, g, p, s, pair in preview
        ],
    }
    return report


def prepare_deepke_dataset(
    *,
    input_jsonl: Path,
    output_dir: Path,
    seed: int = 7,
    train_ratio: float = 0.8,
    val_ratio: float = 0.1,
    test_ratio: float = 0.1,
    group_by_document: bool = True,
    label_allowlist: set[str] | None = None,
) -> dict[str, Any]:
    examples = load_pair_examples(input_jsonl)
    examples = [ex for ex in examples if ex.sample_id and ex.text_a and ex.text_b and ex.label]
    if label_allowlist is not None:
        examples = [ex for ex in examples if ex.label in label_allowlist]

    train, val, test = split_examples(
        examples=examples,
        seed=seed,
        train_ratio=train_ratio,
        val_ratio=val_ratio,
        test_ratio=test_ratio,
        group_by_document=group_by_document,
    )

    labels = sorted({ex.label for ex in examples})
    label_to_id = {label: i for i, label in enumerate(labels)}

    def to_row(ex: PairExample) -> dict[str, Any]:
        return {
            "id": ex.sample_id,
            "text_a": ex.text_a,
            "text_b": ex.text_b,
            "label": ex.label,
            "label_id": label_to_id[ex.label],
            "document_node_id": ex.document_node_id,
        }

    output_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(output_dir / "train.jsonl", (to_row(ex) for ex in train))
    write_jsonl(output_dir / "dev.jsonl", (to_row(ex) for ex in val))
    write_jsonl(output_dir / "test.jsonl", (to_row(ex) for ex in test))
    (output_dir / "labels.json").write_text(json.dumps(label_to_id, ensure_ascii=False, indent=2), encoding="utf-8")

    stats = {
        "input": str(input_jsonl.resolve()),
        "output_dir": str(output_dir.resolve()),
        "seed": seed,
        "group_by_document": group_by_document,
        "counts": {"train": len(train), "dev": len(val), "test": len(test), "total": len(examples)},
        "labels": label_to_id,
    }
    (output_dir / "dataset.meta.json").write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    return stats


def train_pair_classifier(
    *,
    dataset_dir: Path,
    output_dir: Path,
    base_model: str = "bert-base-chinese",
    seed: int = 7,
    epochs: int = 3,
    max_steps: int = -1,
    batch_size: int = 8,
    lr: float = 2e-5,
    max_length: int = 256,
    use_cpu: bool = False,
) -> dict[str, Any]:
    try:
        import numpy as np
        import torch
        from datasets import Dataset
        from transformers import (
            AutoModelForSequenceClassification,
            AutoTokenizer,
            DataCollatorWithPadding,
            Trainer,
            TrainingArguments,
        )
    except Exception as e:
        raise RuntimeError(
            "Missing training dependencies. Install: pip install -e kg-link[train]"
        ) from e

    label_to_id = json.loads((dataset_dir / "labels.json").read_text(encoding="utf-8"))
    id_to_label = {v: k for k, v in label_to_id.items()}

    def read_rows(p: Path) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
        return rows

    train_rows = read_rows(dataset_dir / "train.jsonl")
    dev_rows = read_rows(dataset_dir / "dev.jsonl")

    tokenizer = AutoTokenizer.from_pretrained(base_model, use_fast=True)

    def tokenize(batch: dict[str, list[Any]]) -> dict[str, Any]:
        return tokenizer(
            batch["text_a"],
            batch["text_b"],
            truncation=True,
            max_length=max_length,
        )

    train_ds = Dataset.from_list(train_rows).map(tokenize, batched=True)
    dev_ds = Dataset.from_list(dev_rows).map(tokenize, batched=True)
    train_ds = train_ds.rename_column("label_id", "labels")
    dev_ds = dev_ds.rename_column("label_id", "labels")
    keep = {"input_ids", "token_type_ids", "attention_mask", "labels"}
    train_drop = [c for c in train_ds.column_names if c not in keep]
    dev_drop = [c for c in dev_ds.column_names if c not in keep]
    if train_drop:
        train_ds = train_ds.remove_columns(train_drop)
    if dev_drop:
        dev_ds = dev_ds.remove_columns(dev_drop)

    model = AutoModelForSequenceClassification.from_pretrained(
        base_model,
        num_labels=len(label_to_id),
        id2label=id_to_label,
        label2id=label_to_id,
    )

    data_collator = DataCollatorWithPadding(tokenizer=tokenizer)

    def compute_metrics(eval_pred: Any) -> dict[str, float]:
        logits = getattr(eval_pred, "predictions", eval_pred[0])
        labels = getattr(eval_pred, "label_ids", eval_pred[1])
        preds = np.argmax(logits, axis=-1)
        acc = float((preds == labels).mean())
        return {"accuracy": acc}

    sig = inspect.signature(TrainingArguments.__init__)
    params = sig.parameters
    eval_key = "eval_strategy" if "eval_strategy" in params else "evaluation_strategy"
    cpu_key = "use_cpu" if "use_cpu" in params else "no_cuda"

    training_kwargs: dict[str, Any] = {
        "output_dir": str(output_dir),
        "seed": seed,
        "num_train_epochs": epochs,
        "max_steps": max_steps,
        "per_device_train_batch_size": batch_size,
        "per_device_eval_batch_size": batch_size,
        "learning_rate": lr,
        eval_key: "epoch",
        "save_strategy": "epoch",
        "load_best_model_at_end": True,
        "metric_for_best_model": "accuracy",
        "report_to": "none",
        "fp16": bool(torch.cuda.is_available()) and not use_cpu,
        cpu_key: bool(use_cpu),
        "logging_steps": 50,
    }

    try:
        args = TrainingArguments(**training_kwargs)
    except ImportError as e:
        raise RuntimeError(
            "Missing training runtime dependencies. Install: pip install -e kg-link[train]"
        ) from e

    trainer_sig = inspect.signature(Trainer.__init__)
    trainer_params = trainer_sig.parameters
    processing_key = "processing_class" if "processing_class" in trainer_params else "tokenizer"

    trainer_kwargs: dict[str, Any] = {
        "model": model,
        "args": args,
        "train_dataset": train_ds,
        "eval_dataset": dev_ds,
        "data_collator": data_collator,
        "compute_metrics": compute_metrics,
        processing_key: tokenizer,
    }

    trainer = Trainer(**trainer_kwargs)

    trainer.train()
    trainer.save_model(str(output_dir))
    tokenizer.save_pretrained(str(output_dir))

    meta = {
        "dataset_dir": str(dataset_dir.resolve()),
        "output_dir": str(output_dir.resolve()),
        "base_model": base_model,
        "labels": label_to_id,
        "hyperparams": {
            "seed": seed,
            "epochs": epochs,
            "batch_size": batch_size,
            "lr": lr,
            "max_length": max_length,
            "use_cpu": use_cpu,
        },
    }
    (output_dir / "train.meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return meta
