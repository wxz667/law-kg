from __future__ import annotations

from pathlib import Path

from .api import RelationPrediction, predict_relations


def predict(sentences: list[str], model_dir: Path | None = None) -> list[RelationPrediction]:
    return predict_relations(sentences, model_dir=model_dir)
