from __future__ import annotations

from typing import Any

from openai import OpenAI

from ..base import ProviderRequestConfig, ProviderResponseError
from .config import load_deepseek_config


class DeepSeekClient:
    def __init__(self, request_config: ProviderRequestConfig) -> None:
        self.request_config = request_config
        self.provider_config = load_deepseek_config()
        self.sdk_client = OpenAI(
            api_key=self.provider_config.api_key,
            base_url=self.provider_config.base_url,
            timeout=self.request_config.params.get("timeout_seconds"),
            max_retries=int(self.request_config.params.get("max_retries", 2)),
        )

    def generate_text(
        self,
        messages: list[dict[str, str]],
        model: str,
        **params: Any,
    ) -> str:
        try:
            response = self.sdk_client.chat.completions.create(
                model=model,
                messages=messages,
                stream=False,
                **self._merged_generation_params(params),
            )
            content = response.choices[0].message.content
        except (AttributeError, IndexError, KeyError) as exc:
            raise ProviderResponseError("Invalid DeepSeek chat completion response shape.") from exc
        except Exception as exc:
            raise ProviderResponseError(f"Failed to call DeepSeek chat completion: {exc}") from exc
        if not isinstance(content, str) or not content.strip():
            raise ProviderResponseError("Empty model output.")
        return content.strip()

    def embed_texts(
        self,
        texts: list[str],
        model: str,
        **params: Any,
    ) -> list[list[float]]:
        try:
            response = self.sdk_client.embeddings.create(
                model=model,
                input=texts,
                **self._merged_embedding_params(params),
            )
            return [item.embedding for item in response.data]
        except (AttributeError, KeyError) as exc:
            raise ProviderResponseError("Invalid DeepSeek embedding response shape.") from exc
        except Exception as exc:
            raise ProviderResponseError(f"Failed to call DeepSeek embeddings: {exc}") from exc

    def _merged_generation_params(self, runtime_params: dict[str, Any]) -> dict[str, Any]:
        merged = dict(self.request_config.params)
        merged.update(runtime_params)
        merged.pop("timeout_seconds", None)
        merged.pop("max_retries", None)
        return merged

    def _merged_embedding_params(self, runtime_params: dict[str, Any]) -> dict[str, Any]:
        merged = dict(self.request_config.params)
        merged.update(runtime_params)
        merged.pop("timeout_seconds", None)
        merged.pop("max_retries", None)
        merged.pop("temperature", None)
        merged.pop("top_p", None)
        merged.pop("do_sample", None)
        merged.pop("stop", None)
        return merged
