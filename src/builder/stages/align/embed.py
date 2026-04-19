from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable

from utils.llm.base import ProviderResponseError

from ...contracts import AlignConceptRecord, ConceptVectorRecord


@dataclass(frozen=True)
class EmbedRuntimeConfig:
    provider: str
    model: str
    batch_size: int
    concurrent_requests: int
    request_timeout_seconds: int
    max_retries: int
    params: dict[str, Any]
    rate_limit: dict[str, Any]


@dataclass(frozen=True)
class EmbedBatchResult:
    vectors: list[ConceptVectorRecord]
    concept_ids: list[str]
    errors: list[dict[str, Any]]
    failed_concept_ids: list[str]
    request_count: int = 0
    retry_count: int = 0


def resolve_embed_runtime_config(runtime: Any) -> EmbedRuntimeConfig:
    payload = dict(runtime.align_config().get("embed", {}))
    provider = str(payload.get("provider", "")).strip()
    model = str(payload.get("model", "")).strip()
    if not provider or not model:
        raise ValueError("builder.align.embed must define non-empty provider and model.")
    return EmbedRuntimeConfig(
        provider=provider,
        model=model,
        batch_size=max(int(payload.get("batch_size", 20) or 20), 1),
        concurrent_requests=max(int(payload.get("concurrent_requests", 1) or 1), 1),
        request_timeout_seconds=max(int(payload.get("request_timeout_seconds", 90) or 90), 1),
        max_retries=max(int(payload.get("max_retries", 2) or 2), 1),
        params=dict(payload.get("params", {})),
        rate_limit=dict(payload.get("rate_limit", {})) if isinstance(payload.get("rate_limit", {}), dict) else {},
    )


def embed_concepts(
    concepts: list[AlignConceptRecord],
    runtime: Any,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
    checkpoint_every: int = 0,
    checkpoint_callback: Callable[[list[ConceptVectorRecord], dict[str, int], list[str], list[dict[str, Any]]], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> tuple[list[ConceptVectorRecord], dict[str, int], list[str], list[dict[str, Any]]]:
    total = len(concepts)
    if progress_callback is not None:
        progress_callback(0, max(total, 1))
    if not concepts:
        if progress_callback is not None:
            progress_callback(1, 1)
        return [], build_embed_stats([], 0, 0, []), [], []

    config = resolve_embed_runtime_config(runtime)
    request_config = runtime.build_request_config(
        {
            "provider": config.provider,
            "model": config.model,
            "params": config.params,
            "request_timeout_seconds": config.request_timeout_seconds,
            "max_retries": config.max_retries,
            "rate_limit": config.rate_limit,
        }
    )
    batches = [concepts[index : index + config.batch_size] for index in range(0, len(concepts), config.batch_size)]
    vectors_by_start: dict[int, list[ConceptVectorRecord]] = {}
    llm_errors: list[dict[str, Any]] = []
    processed_concept_ids: list[str] = []
    total_requests = 0
    retry_count = 0
    completed = 0
    next_checkpoint = max(int(checkpoint_every or 0), 0)
    max_workers = max(1, min(config.concurrent_requests, len(batches) or 1))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {
            executor.submit(embed_batch, runtime, request_config, batch, config.max_retries): (start, batch)
            for start, batch in enumerate(batches)
        }
        for future in as_completed(future_to_batch):
            if cancel_event is not None and cancel_event.is_set():
                raise KeyboardInterrupt
            start, batch = future_to_batch[future]
            batch_result = future.result()
            if batch_result.failed_concept_ids:
                raise ProviderResponseError(
                    f"Align embed failed for concepts: {', '.join(batch_result.failed_concept_ids[:5])}"
                )
            vectors_by_start[start] = list(batch_result.vectors)
            llm_errors.extend(batch_result.errors)
            total_requests += int(batch_result.request_count)
            retry_count += int(batch_result.retry_count)
            processed_concept_ids.extend(batch_result.concept_ids)
            completed += len(batch)
            if progress_callback is not None:
                progress_callback(completed, max(total, 1))
            if checkpoint_callback is not None and checkpoint_every > 0:
                if completed >= total:
                    checkpoint_callback(
                        materialize_vectors(vectors_by_start),
                        build_embed_stats(materialize_vectors(vectors_by_start), total_requests, retry_count, llm_errors),
                        sorted(dict.fromkeys(processed_concept_ids)),
                        summarize_errors(llm_errors),
                    )
                elif next_checkpoint > 0 and completed >= next_checkpoint:
                    snapshot = materialize_vectors(vectors_by_start)
                    checkpoint_callback(
                        snapshot,
                        build_embed_stats(snapshot, total_requests, retry_count, llm_errors),
                        sorted(dict.fromkeys(processed_concept_ids)),
                        summarize_errors(llm_errors),
                    )
                    while next_checkpoint > 0 and completed >= next_checkpoint:
                        next_checkpoint += checkpoint_every

    vectors = materialize_vectors(vectors_by_start)
    return (
        vectors,
        build_embed_stats(vectors, total_requests, retry_count, llm_errors),
        sorted(dict.fromkeys(processed_concept_ids)),
        summarize_errors(llm_errors),
    )


def embed_batch(
    runtime: Any,
    request_config: Any,
    batch: list[AlignConceptRecord],
    max_retries: int,
) -> EmbedBatchResult:
    errors: list[dict[str, Any]] = []
    concept_ids = [row.id for row in batch]
    texts = [build_embedding_text(row) for row in batch]
    for attempt in range(1, max_retries + 1):
        try:
            embeddings = runtime.embed_texts(texts, request_config)
            if len(embeddings) != len(batch):
                raise ValueError("Align embed response count does not match input count.")
            return EmbedBatchResult(
                vectors=[
                    ConceptVectorRecord(id=row.id, vector=vector)
                    for row, vector in zip(batch, embeddings)
                ],
                concept_ids=concept_ids,
                errors=[],
                failed_concept_ids=[],
                request_count=attempt,
                retry_count=max(attempt - 1, 0),
            )
        except (ProviderResponseError, ValueError) as exc:
            errors.append(
                {
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "attempt": attempt,
                    "concept_ids": concept_ids,
                }
            )
    return EmbedBatchResult(
        vectors=[],
        concept_ids=[],
        errors=errors,
        failed_concept_ids=concept_ids,
        request_count=max_retries,
        retry_count=max(max_retries - 1, 0),
    )


def build_embedding_text(row: AlignConceptRecord) -> str:
    return f"{row.name}\n{row.description}".strip()


def materialize_vectors(vectors_by_start: dict[int, list[ConceptVectorRecord]]) -> list[ConceptVectorRecord]:
    rows: list[ConceptVectorRecord] = []
    for start in sorted(vectors_by_start):
        rows.extend(vectors_by_start[start])
    return rows


def build_embed_stats(
    vectors: list[ConceptVectorRecord],
    total_requests: int,
    retry_count: int,
    llm_errors: list[dict[str, Any]],
) -> dict[str, int]:
    return {
        "input_count": len(vectors),
        "vector_count": len(vectors),
        "result_count": len(vectors),
        "llm_request_count": int(total_requests),
        "llm_error_count": len(llm_errors),
        "retry_count": int(retry_count),
    }


def summarize_errors(errors: list[dict[str, Any]], *, limit: int = 10) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in errors:
        key = (str(row.get("error_type", "")), str(row.get("message", "")))
        entry = deduped.get(key)
        if entry is None:
            entry = {
                "error_type": key[0],
                "message": key[1],
                "count": 0,
                "sample_concept_ids": [str(value) for value in row.get("concept_ids", [])[:3]],
            }
            deduped[key] = entry
        entry["count"] = int(entry.get("count", 0)) + 1
    return sorted(deduped.values(), key=lambda item: (-int(item.get("count", 0)), item["error_type"], item["message"]))[:limit]
