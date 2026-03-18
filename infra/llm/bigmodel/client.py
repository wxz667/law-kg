from __future__ import annotations

from typing import Any

from zai import ZhipuAiClient

from ..base import ProviderRequestConfig
from ..base import ProviderResponseError
from .config import load_bigmodel_config


class BigModelClient:
    def __init__(self, request_config: ProviderRequestConfig) -> None:
        self.request_config = request_config
        self.provider_config = load_bigmodel_config()
        self.sdk_client = ZhipuAiClient(
            api_key=self.provider_config.api_key,
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
                **self._merged_generation_params(params),
            )
            content = response.choices[0].message.content
        except (AttributeError, IndexError, KeyError, TypeError) as exc:
            raise ProviderResponseError("Invalid BigModel chat completion response shape.") from exc
        except Exception as exc:
            raise ProviderResponseError(f"Failed to call BigModel chat completion: {exc}") from exc
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
        except (AttributeError, KeyError, TypeError) as exc:
            raise ProviderResponseError("Invalid BigModel embedding response shape.") from exc
        except Exception as exc:
            raise ProviderResponseError(f"Failed to call BigModel embeddings: {exc}") from exc

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
        # Generation-only parameters should not leak into embedding calls.
        merged.pop("temperature", None)
        merged.pop("top_p", None)
        merged.pop("thinking", None)
        merged.pop("do_sample", None)
        merged.pop("stop", None)
        return merged
