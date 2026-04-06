from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ImplicitRelationPrediction:
    relation_type: str
    score: float
    model: str


def predict_implicit_relations(graph_features: list[dict[str, object]], model_dir: Path | None = None) -> list[ImplicitRelationPrediction]:
    model_name = "rgcn" if model_dir and (model_dir / "model.json").exists() else "heuristic-rgcn"
    predictions: list[ImplicitRelationPrediction] = []
    for feature in graph_features:
        overlap = float(feature.get("overlap_score", 0.0))
        if overlap >= 0.8:
            predictions.append(ImplicitRelationPrediction("INTERPRETS", overlap, model_name))
        else:
            predictions.append(ImplicitRelationPrediction("REFERENCES", max(overlap, 0.42), model_name))
    return predictions
