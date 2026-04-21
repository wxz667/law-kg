from __future__ import annotations

import json
from importlib import import_module
from pathlib import Path
from typing import Any

from utils.llm.base import ProviderRequestConfig, build_provider_request_config
from utils.llm.factory import build_provider_client

from ..utils.ids import project_root


SUBSTAGE_DRIVEN_STAGES = frozenset({"classify", "extract", "align", "infer"})

def resolve_builder_substage_config(runtime: Any, stage_name: str, substage_name: str) -> dict[str, Any]:
    if hasattr(runtime, "builder_substage_config"):
        resolver = getattr(runtime, "builder_substage_config")
        if callable(resolver):
            payload = resolver(stage_name, substage_name)
            return dict(payload) if isinstance(payload, dict) else {}
    stage_getter = getattr(runtime, f"{stage_name}_config", None)
    stage_config = stage_getter() if callable(stage_getter) else {}
    if not isinstance(stage_config, dict):
        return {}
    payload = stage_config.get(substage_name, {})
    return dict(payload) if isinstance(payload, dict) else {}


class PipelineRuntime:
    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root
        root = project_root()
        self.models_root = root / "models"
        self.config_path = root / "configs" / "config.json"
        self._config_cache: dict[str, Any] | None = None
        self._provider_clients: dict[str, Any] = {}

    def load_config(self) -> dict[str, Any]:
        if self._config_cache is None:
            self._config_cache = json.loads(self.config_path.read_text(encoding="utf-8"))
        return dict(self._config_cache)

    def builder_stage_config(self, stage_name: str) -> dict[str, Any]:
        payload = self.load_config()
        builder = payload.get("builder", {})
        config = builder.get(stage_name, {})
        return dict(config) if isinstance(config, dict) else {}

    def builder_substage_config(self, stage_name: str, substage_name: str) -> dict[str, Any]:
        stage_config = self.builder_stage_config(stage_name)
        payload = stage_config.get(substage_name, {})
        return dict(payload) if isinstance(payload, dict) else {}

    def stage_checkpoint_every(self, stage_name: str, default: int = 0) -> int:
        if stage_name in SUBSTAGE_DRIVEN_STAGES:
            return 0
        config = self.builder_stage_config(stage_name)
        return max(int(config.get("checkpoint_every", default) or default), 0)

    def substage_checkpoint_every(self, stage_name: str, substage_name: str, default: int = 0) -> int:
        substage = self.builder_substage_config(stage_name, substage_name)
        if "checkpoint_every" in substage:
            return max(int(substage.get("checkpoint_every", default) or default), 0)
        return max(int(default or 0), 0)

    def detect_config(self) -> dict[str, Any]:
        return self.builder_stage_config("detect")

    def classify_config(self) -> dict[str, Any]:
        return self.builder_stage_config("classify")

    def extract_config(self) -> dict[str, Any]:
        return self.builder_stage_config("extract")

    def aggregate_config(self) -> dict[str, Any]:
        return self.builder_stage_config("aggregate")

    def align_config(self) -> dict[str, Any]:
        return self.builder_stage_config("align")

    def infer_config(self) -> dict[str, Any]:
        return self.builder_stage_config("infer")

    def build_request_config(self, payload: dict[str, Any]) -> ProviderRequestConfig:
        return build_provider_request_config(
            provider=str(payload.get("provider", "")).strip(),
            model=str(payload.get("model", "")).strip(),
            params=dict(payload.get("params", {})),
            timeout_seconds=payload.get("request_timeout_seconds"),
            max_retries=payload.get("max_retries"),
            rate_limit=dict(payload.get("rate_limit", {})) if isinstance(payload.get("rate_limit", {}), dict) else None,
        )

    def generate_text(self, messages: list[dict[str, str]], request_config: ProviderRequestConfig) -> str:
        client = self._provider_client(request_config)
        return client.generate_text(messages, model=request_config.model)

    def embed_texts(self, texts: list[str], request_config: ProviderRequestConfig) -> list[list[float]]:
        client = self._provider_client(request_config)
        return client.embed_texts(texts, model=request_config.model)

    def predict_interprets(self, inputs: list[Any]) -> list[Any]:
        config = self.builder_substage_config("classify", "model")
        predictor_path = str(config.get("interprets_predictor", "")).strip()
        module_name, separator, function_name = predictor_path.partition(":")
        if not module_name or not separator or not function_name:
            raise ValueError("classify.model.interprets_predictor must use 'module:function' format.")
        predictor = getattr(import_module(module_name), function_name)
        model_dir = self.models_root / str(config.get("interprets_model_dir", "interprets_filter"))
        return predictor(inputs, model_dir=model_dir, config_path=self.config_path)

    def _provider_client(self, request_config: ProviderRequestConfig) -> Any:
        cache_key = json.dumps(
            {
                "provider": request_config.provider,
                "model": request_config.model,
                "params": request_config.params,
                "local": request_config.local,
            },
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )
        client = self._provider_clients.get(cache_key)
        if client is None:
            client = build_provider_client(request_config)
            self._provider_clients[cache_key] = client
        return client
