from __future__ import annotations

import json
from typing import Any
from urllib import error, request

from ..base import ProviderRequestConfig, ProviderResponseError
from .config import load_deepseek_config


class DeepSeekClient:
    def __init__(self, request_config: ProviderRequestConfig) -> None:
        self.request_config = request_config
        self.provider_config = load_deepseek_config()

    def generate_text(
        self,
        messages: list[dict[str, str]],
        model: str,
        **params: Any,
    ) -> str:
        payload = {
            "model": model,
            "messages": messages,
        }
        payload.update(self._merged_params(params))
        response = self._post_json("/chat/completions", payload)
        try:
            content = response["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise ProviderResponseError("Invalid DeepSeek chat completion response shape.") from exc
        if not isinstance(content, str) or not content.strip():
            raise ProviderResponseError("Empty model output.")
        return content.strip()

    def embed_texts(
        self,
        texts: list[str],
        model: str,
        **params: Any,
    ) -> list[list[float]]:
        payload = {
            "model": model,
            "input": texts,
        }
        payload.update(self._merged_params(params))
        response = self._post_json("/embeddings", payload)
        try:
            rows = response["data"]
            return [row["embedding"] for row in rows]
        except (KeyError, TypeError) as exc:
            raise ProviderResponseError("Invalid DeepSeek embedding response shape.") from exc

    def _merged_params(self, runtime_params: dict[str, Any]) -> dict[str, Any]:
        merged = dict(self.request_config.params)
        merged.update(runtime_params)
        merged.pop("timeout_seconds", None)
        merged.pop("max_retries", None)
        return merged

    def _post_json(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        timeout_seconds = int(self.request_config.params.get("timeout_seconds", 60))
        max_retries = int(self.request_config.params.get("max_retries", 2))
        url = self.provider_config.base_url.rstrip("/") + endpoint
        body = json.dumps(payload).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {self.provider_config.api_key}",
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
        raise ProviderResponseError(f"Failed to call DeepSeek endpoint {endpoint}: {last_error}") from last_error
