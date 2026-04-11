from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from .config import canonical_label, label_to_bool, load_interprets_filter_config


@dataclass(frozen=True)
class InterpretFilterInput:
    text: str = ""


@dataclass(frozen=True)
class InterpretPrediction:
    is_interprets: bool
    score: float
    model: str


def predict_interprets(
    inputs: list[InterpretFilterInput | dict[str, Any] | str],
    model_dir: Path | None = None,
    config_path: Path | None = None,
) -> list[InterpretPrediction]:
    normalized = [normalize_input(item) for item in inputs]
    resolved_model_dir = resolve_model_dir(model_dir, config_path)
    if resolved_model_dir and (resolved_model_dir / "label_map.json").exists():
        return predict_with_transformer(normalized, resolved_model_dir, config_path)
    return [heuristic_predict(item) for item in normalized]


def normalize_input(value: InterpretFilterInput | dict[str, Any] | str) -> InterpretFilterInput:
    if isinstance(value, InterpretFilterInput):
        return InterpretFilterInput(text=str(value.text).strip())
    if isinstance(value, str):
        return InterpretFilterInput(text=value.strip())
    return InterpretFilterInput(text=str(value.get("text", "")).strip())


def resolve_model_dir(model_dir: Path | None, config_path: Path | None) -> Path | None:
    if model_dir is not None:
        return model_dir
    config = load_interprets_filter_config(config_path)
    return Path(str(config.predict.get("default_model_dir", "models/interprets_filter")))


def predict_with_transformer(
    samples: list[InterpretFilterInput],
    model_dir: Path,
    config_path: Path | None = None,
) -> list[InterpretPrediction]:
    config = load_interprets_filter_config(config_path)
    max_length = int(config.predict.get("max_length", 512))
    device_preference = str(config.train.get("device_preference", "cuda"))
    device = resolve_device(device_preference)
    tokenizer, model = load_transformer_bundle(model_dir, str(device))

    label_map = json.loads((model_dir / "label_map.json").read_text(encoding="utf-8"))
    index_to_label = {int(index): label for label, index in label_map.items()}
    positive_index = infer_positive_index(index_to_label)
    threshold = resolve_threshold(model_dir, config)

    encoded = tokenizer(
        [sample.text for sample in samples],
        truncation=True,
        max_length=max_length,
        padding=True,
        return_tensors="pt",
    )
    encoded = {key: value.to(device) for key, value in encoded.items()}

    with torch.no_grad():
        logits = model(**encoded).logits
        probabilities = torch.softmax(logits, dim=-1)

    predictions: list[InterpretPrediction] = []
    for probability in probabilities:
        positive_score = float(probability[positive_index].item())
        predictions.append(
            InterpretPrediction(
                is_interprets=positive_score >= threshold,
                score=positive_score,
                model=model_dir.name,
            )
        )
    return predictions


def infer_positive_index(index_to_label: dict[int, Any]) -> int:
    for index, label in index_to_label.items():
        if canonical_label(label) == "true":
            return index
    return max(index_to_label) if index_to_label else 1


def resolve_threshold(model_dir: Path, config: Any) -> float:
    metrics_path = model_dir / "metrics.json"
    if metrics_path.exists():
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
        if "selected_threshold" in metrics:
            return float(metrics["selected_threshold"])
    return float(config.predict.get("threshold", 0.5))


@lru_cache(maxsize=4)
def load_transformer_bundle(model_dir: Path, device_name: str) -> tuple[Any, Any]:
    tokenizer = AutoTokenizer.from_pretrained(model_dir)
    model = AutoModelForSequenceClassification.from_pretrained(model_dir).to(torch.device(device_name))
    model.eval()
    return tokenizer, model


def resolve_device(device_preference: str) -> torch.device:
    if device_preference == "cuda" and torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")


def heuristic_predict(sample: InterpretFilterInput) -> InterpretPrediction:
    text = sample.text
    if any(marker in text for marker in ("依照", "根据", "按照", "参照", "所列", "除外", "规定的情形", "规定办理")):
        return InterpretPrediction(False, 0.12, "rule-based")
    if any(marker in text for marker in ("是指", "所称", "系指", "含义", "解释如下", "批复如下", "答复如下")):
        return InterpretPrediction(True, 0.82, "rule-based")
    return InterpretPrediction(False, 0.18, "rule-based")


# Compatibility helper for a few call sites that may still import the old name during transition.
predict_relations = predict_interprets
