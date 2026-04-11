from __future__ import annotations

import math
import json
from pathlib import Path
from typing import Any

from interprets_filter.api import InterpretPrediction, predict_interprets
from utils.llm.base import ProviderRequestConfig
from utils.llm.factory import build_provider_client
from ner.api import EntityPrediction, predict_entities
from rgcn.api import ImplicitRelationPrediction, predict_implicit_relations

from ..utils.ids import project_root


class PipelineRuntime:
    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root
        root = project_root()
        self.models_root = root / "models"
        self.config_path = root / "configs" / "config.json"
        self._config_cache: dict[str, Any] | None = None

    def load_config(self) -> dict[str, Any]:
        if self._config_cache is None:
            self._config_cache = json.loads(self.config_path.read_text(encoding="utf-8"))
        return dict(self._config_cache)

    def builder_stage_config(self, stage_name: str) -> dict[str, Any]:
        payload = self.load_config()
        builder = payload.get("builder", {})
        config = builder.get(stage_name, {})
        return dict(config) if isinstance(config, dict) else {}

    def stage_checkpoint_every(self, stage_name: str, default: int = 0) -> int:
        config = self.builder_stage_config(stage_name)
        return max(int(config.get("checkpoint_every", default) or default), 0)

    def reference_filter_config(self) -> dict[str, Any]:
        return self.builder_stage_config("reference_filter")

    def relation_classify_config(self) -> dict[str, Any]:
        return self.builder_stage_config("relation_classify")

    def build_request_config(self, payload: dict[str, Any]) -> ProviderRequestConfig:
        return ProviderRequestConfig(
            provider=str(payload.get("provider", "")).strip(),
            model=str(payload.get("model", "")).strip(),
            params=dict(payload.get("params", {})),
        )

    def generate_text(self, messages: list[dict[str, str]], request_config: ProviderRequestConfig) -> str:
        client = build_provider_client(request_config)
        return client.generate_text(messages, model=request_config.model)

    def predict_interprets(self, inputs: list[Any]) -> list[InterpretPrediction]:
        return predict_interprets(inputs, model_dir=self.models_root / "interprets_filter", config_path=self.config_path)

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

    def predict_implicit_relations(self, graph_features: list[dict[str, object]]) -> list[ImplicitRelationPrediction]:
        return predict_implicit_relations(graph_features, model_dir=self.models_root / "rgcn")
