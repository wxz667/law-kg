from __future__ import annotations

from typing import Any

from openai import OpenAI

from ..base import ProviderRequestConfig, ProviderResponseError
from ..rate_limit import estimate_message_tokens, estimate_texts_tokens, run_with_rate_limit
from .config import load_siliconflow_config


class SiliconFlowClient:
    def __init__(self, request_config: ProviderRequestConfig) -> None:
        self.request_config = request_config
        self.provider_config = load_siliconflow_config()
        self.sdk_client = OpenAI(
            api_key=self.provider_config.api_key,
            base_url=self.provider_config.base_url,
            timeout=self.request_config.local.get("timeout_seconds"),
            max_retries=int(self.request_config.local.get("max_retries", 2)),
        )

    def generate_text(
        self,
        messages: list[dict[str, str]],
        model: str,
        **params: Any,
    ) -> str:
        try:
            merged_params = self._merged_generation_params(params)
            response = run_with_rate_limit(
                self.request_config,
                estimated_tokens=estimate_message_tokens(
                    messages,
                    max_output_tokens=int(merged_params.get("max_tokens", 0) or 0),
                ),
                operation=lambda: self.sdk_client.chat.completions.create(
                    model=model,
                    messages=messages,
                    stream=False,
                    **merged_params,
                ),
                operation_name="SiliconFlow chat completion",
            )
            content = response.choices[0].message.content
        except (AttributeError, IndexError, KeyError) as exc:
            raise ProviderResponseError("Invalid SiliconFlow chat completion response shape.") from exc
        except ProviderResponseError:
            raise
        except Exception as exc:
            raise ProviderResponseError(f"Failed to call SiliconFlow chat completion: {exc}") from exc
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
            merged_params = self._merged_embedding_params(params)
            response = run_with_rate_limit(
                self.request_config,
                estimated_tokens=estimate_texts_tokens(texts),
                operation=lambda: self.sdk_client.embeddings.create(
                    model=model,
                    input=texts,
                    **merged_params,
                ),
                operation_name="SiliconFlow embeddings",
            )
            return [item.embedding for item in response.data]
        except (AttributeError, KeyError) as exc:
            raise ProviderResponseError("Invalid SiliconFlow embedding response shape.") from exc
        except ProviderResponseError:
            raise
        except Exception as exc:
            raise ProviderResponseError(f"Failed to call SiliconFlow embeddings: {exc}") from exc

    def _merged_generation_params(self, runtime_params: dict[str, Any]) -> dict[str, Any]:
        merged = dict(self.request_config.params)
        merged.update(runtime_params)
        return merged

    def _merged_embedding_params(self, runtime_params: dict[str, Any]) -> dict[str, Any]:
        merged = dict(self.request_config.params)
        merged.update(runtime_params)
        merged.pop("temperature", None)
        merged.pop("top_p", None)
        merged.pop("do_sample", None)
        merged.pop("stop", None)
        return merged
