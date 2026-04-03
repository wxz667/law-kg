from __future__ import annotations

from pathlib import Path
from typing import Any

from .io import read_jsonl, write_jsonl

SUPPORTED_RELATIONS = {
    "REFERS_TO",
    "INTERPRETS",
    "AMENDS",
    "REPEALS",
}


def predict_relations(
    *,
    samples_path: Path,
    output_path: Path,
    model_name: str,
    config: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    _ = model_name
    _ = config or {}
    samples = read_jsonl(samples_path)
    predictions: list[dict[str, Any]] = []
    write_jsonl(output_path, predictions)
    return predictions
