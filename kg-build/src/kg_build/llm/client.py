from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Any

from ..common import repo_root
from .config import StageModelConfig


class LLMError(RuntimeError):
    pass


@dataclass
class LLMClient:
    config: StageModelConfig

    def generate_text(
        self,
        prompt: str,
        system_prompt: str = "",
        **params: Any,
    ) -> str:
        messages: list[dict[str, str]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        try:
            return self._provider_client().generate_text(
                messages=messages,
                model=self.config.model,
                **self._merged_params(params),
            )
        except Exception as exc:
            raise LLMError(f"Text generation failed for stage {self.config.stage_name}: {exc}") from exc

    def embed_texts(self, texts: list[str], **params: Any) -> list[list[float]]:
        try:
            return self._provider_client().embed_texts(
                texts=texts,
                model=self.config.model,
                **self._merged_params(params),
            )
        except Exception as exc:
            raise LLMError(f"Embedding failed for stage {self.config.stage_name}: {exc}") from exc

    def _merged_params(self, runtime_params: dict[str, Any]) -> dict[str, Any]:
        merged = dict(self.config.params)
        merged.update(runtime_params)
        return merged

    def _provider_client(self) -> Any:
        repo_path = str(repo_root())
        if repo_path not in sys.path:
            sys.path.insert(0, repo_path)
        from infra.llm.base import ProviderRequestConfig
        from infra.llm.factory import build_provider_client

        config = ProviderRequestConfig(
            provider=self.config.provider,
            model=self.config.model,
            params=dict(self.config.params),
        )
        return build_provider_client(config)


def build_llm_client(config: StageModelConfig) -> LLMClient:
    return LLMClient(config=config)
