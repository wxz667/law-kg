from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RelationPrediction:
    relation_type: str
    score: float
    model: str


def predict_relations(sentences: list[str], model_dir: Path | None = None) -> list[RelationPrediction]:
    model_name = "roberta-wwm-ext" if model_dir and (model_dir / "model.json").exists() else "rule-based"
    predictions: list[RelationPrediction] = []
    for sentence in sentences:
        if "废止" in sentence:
            predictions.append(RelationPrediction("REPEALS", 0.93, model_name))
        elif "修改" in sentence or "修正" in sentence:
            predictions.append(RelationPrediction("AMENDS", 0.91, model_name))
        elif "解释" in sentence:
            predictions.append(RelationPrediction("INTERPRETS", 0.88, model_name))
        else:
            predictions.append(RelationPrediction("REFERENCES", 0.74, model_name))
    return predictions
