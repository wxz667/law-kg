from __future__ import annotations

from pathlib import Path

from .api import EntityPrediction, predict_entities


def predict(text: str, model_dir: Path | None = None) -> list[EntityPrediction]:
    return predict_entities(text, model_dir=model_dir)
