from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class EntityPrediction:
    text: str
    label: str
    normalized_text: str
    start_offset: int = -1
    end_offset: int = -1
    model: str = "bilstm-crf"


LAW_NAME_RE = re.compile(r"《[^》]+》")
DEFINITION_RE = re.compile(r"^(?P<term>[\u4e00-\u9fffA-Za-z0-9]{2,20})(?:，|,)?是指")


def predict_entities(text: str, model_dir: Path | None = None) -> list[EntityPrediction]:
    model_name = "bilstm-crf" if model_dir and (model_dir / "model.json").exists() else "rule-ner"
    predictions: list[EntityPrediction] = []
    for match in LAW_NAME_RE.finditer(text):
        predictions.append(
            EntityPrediction(
                text=match.group(0),
                label="law",
                normalized_text=match.group(0).strip("《》"),
                start_offset=match.start(),
                end_offset=match.end(),
                model=model_name,
            )
        )
    definition_match = DEFINITION_RE.search(text.strip())
    if definition_match:
        term = definition_match.group("term")
        predictions.append(EntityPrediction(text=term, label="legal_concept", normalized_text=term, model=model_name))
    return predictions
