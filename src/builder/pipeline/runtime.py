from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

from ner.api import EntityPrediction, predict_entities
from relation_classifier.api import RelationPrediction, predict_relations
from rgcn.api import ImplicitRelationPrediction, predict_implicit_relations


@dataclass(frozen=True)
class AlignmentDecision:
    left_id: str
    right_id: str
    approved: bool
    score: float
    model: str


class PipelineRuntime:
    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root
        self.models_root = data_root / "models"

    def predict_relations(self, sentences: list[str]) -> list[RelationPrediction]:
        return predict_relations(sentences, model_dir=self.models_root / "relation_classifier")

    def predict_entities(self, text: str) -> list[EntityPrediction]:
        return predict_entities(text, model_dir=self.models_root / "ner")

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for text in texts:
            buckets = [0.0] * 16
            if not text:
                vectors.append(buckets)
                continue
            for char in text:
                buckets[ord(char) % len(buckets)] += 1.0
            norm = math.sqrt(sum(value * value for value in buckets)) or 1.0
            vectors.append([value / norm for value in buckets])
        return vectors

    def judge_alignment(self, candidate_pairs: list[dict[str, object]]) -> list[AlignmentDecision]:
        decisions: list[AlignmentDecision] = []
        for pair in candidate_pairs:
            left_text = str(pair.get("left_text", "")).strip()
            right_text = str(pair.get("right_text", "")).strip()
            similarity = float(pair.get("similarity", 0.0))
            approved = (
                left_text == right_text
                or (left_text and right_text and (left_text in right_text or right_text in left_text) and similarity >= 0.55)
                or similarity >= 0.84
            )
            score = 0.92 if approved and left_text == right_text else max(similarity, 0.35)
            decisions.append(
                AlignmentDecision(
                    left_id=str(pair.get("left_id", "")),
                    right_id=str(pair.get("right_id", "")),
                    approved=approved,
                    score=score,
                    model="builder-alignment-judge",
                )
            )
        return decisions

    def predict_implicit_relations(self, graph_features: list[dict[str, object]]) -> list[ImplicitRelationPrediction]:
        return predict_implicit_relations(graph_features, model_dir=self.models_root / "rgcn")
