from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib import error, request

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
        payload = {
            "model": self.config.model,
            "messages": [],
        }
        if system_prompt:
            payload["messages"].append({"role": "system", "content": system_prompt})
        payload["messages"].append({"role": "user", "content": prompt})
        payload.update(self._merged_params(params))
        response = self._post_json("/chat/completions", payload)
        try:
            content = response["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise LLMError(f"Invalid chat completion response shape for stage {self.config.stage_name}") from exc
        if not isinstance(content, str) or not content.strip():
            raise LLMError(f"Empty model output for stage {self.config.stage_name}")
        return content.strip()

    def embed_texts(self, texts: list[str], **params: Any) -> list[list[float]]:
        payload = {
            "model": self.config.model,
            "input": texts,
        }
        payload.update(self._merged_params(params))
        response = self._post_json("/embeddings", payload)
        try:
            rows = response["data"]
            return [row["embedding"] for row in rows]
        except (KeyError, TypeError) as exc:
            raise LLMError(f"Invalid embedding response shape for stage {self.config.stage_name}") from exc

    def _merged_params(self, runtime_params: dict[str, Any]) -> dict[str, Any]:
        merged = dict(self.config.params)
        merged.update(runtime_params)
        merged.pop("timeout_seconds", None)
        merged.pop("max_retries", None)
        return merged

    def _post_json(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        if self.config.provider != "openai_compatible":
            raise LLMError(f"Unsupported provider: {self.config.provider}")
        if not self.config.base_url:
            raise LLMError(
                f"Stage {self.config.stage_name} is missing base_url. "
                f"Set {self.config.base_url_env or 'OPENAI_BASE_URL'}."
            )
        if not self.config.api_key:
            raise LLMError(
                f"Stage {self.config.stage_name} is missing api_key. "
                f"Set {self.config.api_key_env or 'OPENAI_API_KEY'}."
            )

        timeout_seconds = int(self.config.params.get("timeout_seconds", 60))
        max_retries = int(self.config.params.get("max_retries", 2))
        url = self.config.base_url.rstrip("/") + endpoint
        body = json.dumps(payload).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
        }
        req = request.Request(url=url, data=body, headers=headers, method="POST")

        last_error: Exception | None = None
        for _ in range(max_retries + 1):
            try:
                with request.urlopen(req, timeout=timeout_seconds) as response:
                    return json.loads(response.read().decode("utf-8"))
            except (error.HTTPError, error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_error = exc
        raise LLMError(
            f"Failed to call {endpoint} for stage {self.config.stage_name}: {last_error}"
        ) from last_error


def build_llm_client(config: StageModelConfig) -> LLMClient:
    return LLMClient(config=config)
