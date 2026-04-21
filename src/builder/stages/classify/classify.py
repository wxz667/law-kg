from __future__ import annotations

import threading
from typing import Any, Callable, Iterator

from ...pipeline.runtime import PipelineRuntime, resolve_builder_substage_config


def resolve_interprets_policy(runtime: PipelineRuntime) -> dict[str, float | bool | int]:
    raw = resolve_builder_substage_config(runtime, "classify", "model")
    return {
        "high_confidence_true_threshold": float(raw.get("high_confidence_true_threshold", 0.8)),
        "low_confidence_false_threshold": float(raw.get("low_confidence_false_threshold", 0.35)),
        "use_llm_for_uncertain": bool(raw.get("use_llm_for_uncertain", True)),
        "prediction_batch_size": int(raw.get("prediction_batch_size", 128)),
    }


def batched_predict_interprets(
    runtime: PipelineRuntime,
    marked_texts: list[str],
    *,
    batch_size: int,
    cancel_event: threading.Event | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> list[Any]:
    total = len(marked_texts)
    if progress_callback is not None:
        progress_callback(0, total)
    if not marked_texts:
        return []

    predictions: list[Any] = []
    step = max(1, batch_size)
    for start in range(0, total, step):
        if cancel_event is not None and cancel_event.is_set():
            raise KeyboardInterrupt
        batch_texts = marked_texts[start : start + step]
        predictions.extend(runtime.predict_interprets([{"text": text} for text in batch_texts]))
        if progress_callback is not None:
            progress_callback(min(start + len(batch_texts), total), total)
    return predictions


def iter_predict_interprets_batches(
    runtime: PipelineRuntime,
    marked_texts: list[str],
    *,
    batch_size: int,
    cancel_event: threading.Event | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> Iterator[tuple[int, list[Any]]]:
    total = len(marked_texts)
    if progress_callback is not None:
        progress_callback(0, total)
    if not marked_texts:
        return
    step = max(1, batch_size)
    for start in range(0, total, step):
        if cancel_event is not None and cancel_event.is_set():
            raise KeyboardInterrupt
        batch_texts = marked_texts[start : start + step]
        predictions = runtime.predict_interprets([{"text": text} for text in batch_texts])
        completed = min(start + len(batch_texts), total)
        if progress_callback is not None:
            progress_callback(completed, total)
        yield start, list(predictions)


def is_prediction_low_confidence(prediction: Any, *, policy: dict[str, float | bool | int]) -> bool:
    score = float(getattr(prediction, "score", 0.0))
    return float(policy["low_confidence_false_threshold"]) < score < float(policy["high_confidence_true_threshold"])


def build_llm_payload(
    *,
    sample_id: str,
    text: str,
    prediction: Any,
    source_category: str,
    target_categories: list[str],
    is_legislative_interpretation: bool,
) -> dict[str, object]:
    return {
        "sample_id": sample_id,
        "text": text,
        "model_score": float(getattr(prediction, "score", 0.0)),
        "model_is_interprets": bool(getattr(prediction, "is_interprets", False)),
        "source_category": source_category,
        "target_categories": list(target_categories),
        "is_legislative_interpretation": is_legislative_interpretation,
    }
