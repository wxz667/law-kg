from __future__ import annotations

from pathlib import Path

from .api import ImplicitRelationPrediction, predict_implicit_relations


def predict(features: list[dict[str, object]], model_dir: Path | None = None) -> list[ImplicitRelationPrediction]:
    return predict_implicit_relations(features, model_dir=model_dir)
