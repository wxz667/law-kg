from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable
from uuid import NAMESPACE_URL, uuid5

from ...contracts import ConceptVectorRecord, EmbeddedConceptRecord, ExtractConceptRecord
from ...utils.locator import source_id_from_node_id


@dataclass(frozen=True)
class EmbedRuntimeConfig:
    provider: str
    model: str
    batch_size: int
    request_timeout_seconds: int
    max_retries: int
    params: dict[str, Any]
    rate_limit: dict[str, Any]


@dataclass
class EmbedResult:
    concepts: list[EmbeddedConceptRecord] = field(default_factory=list)
    vectors: list[ConceptVectorRecord] = field(default_factory=list)
    processed_source_ids: list[str] = field(default_factory=list)
    processed_concept_ids: list[str] = field(default_factory=list)
    stats: dict[str, int] = field(default_factory=dict)


def resolve_embed_runtime_config(runtime: Any) -> EmbedRuntimeConfig:
    payload = dict(runtime.embed_config().get("embedding", {}))
    provider = str(payload.get("provider", "")).strip()
    model = str(payload.get("model", "")).strip()
    if not provider or not model:
        raise ValueError("builder.embed.embedding must define non-empty provider and model.")
    return EmbedRuntimeConfig(
        provider=provider,
        model=model,
        batch_size=max(int(payload.get("batch_size", 64) or 64), 1),
        request_timeout_seconds=max(int(payload.get("request_timeout_seconds", 90) or 90), 1),
        max_retries=max(int(payload.get("max_retries", 2) or 2), 1),
        params=dict(payload.get("params", {})),
        rate_limit=dict(payload.get("rate_limit", {})) if isinstance(payload.get("rate_limit", {}), dict) else {},
    )


def run(
    extract_concepts: list[ExtractConceptRecord],
    runtime: Any,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
    checkpoint_every: int = 0,
    checkpoint_callback: Callable[[list[EmbeddedConceptRecord], list[ConceptVectorRecord], dict[str, int], list[str], list[str]], None] | None = None,
) -> EmbedResult:
    concept_mentions = expand_extract_concepts(extract_concepts)
    processed_source_ids = sorted({source_id_from_node_id(row.id) for row in extract_concepts})
    total_mentions = max(len(concept_mentions), 1)
    if progress_callback is not None:
        progress_callback(0, total_mentions)
    if not concept_mentions:
        if progress_callback is not None:
            progress_callback(1, 1)
        runtime_config = resolve_embed_runtime_config(runtime)
        return EmbedResult(
            concepts=[],
            vectors=[],
            processed_source_ids=processed_source_ids,
            processed_concept_ids=[],
            stats=build_embed_stats(
                processed_source_ids,
                0,
                0,
                provider=runtime_config.provider,
                model=runtime_config.model,
                backend="provider",
                vector_dimension=0,
            ),
        )

    runtime_config = resolve_embed_runtime_config(runtime)
    request_config = runtime.build_request_config(
        {
            "provider": runtime_config.provider,
            "model": runtime_config.model,
            "params": runtime_config.params,
            "request_timeout_seconds": runtime_config.request_timeout_seconds,
            "max_retries": runtime_config.max_retries,
            "rate_limit": runtime_config.rate_limit,
        }
    )
    vectors: list[ConceptVectorRecord] = []
    processed = 0
    next_checkpoint = max(int(checkpoint_every or 0), 0)
    batch_size = max(runtime_config.batch_size, 1)
    for start in range(0, len(concept_mentions), batch_size):
        batch = concept_mentions[start : start + batch_size]
        batch_vectors = runtime.embed_texts([row.text for row in batch], request_config=request_config)
        vectors.extend(
            ConceptVectorRecord(id=row.id, vector=list(vector))
            for row, vector in zip(batch, batch_vectors)
        )
        processed += len(batch)
        if progress_callback is not None:
            progress_callback(processed, total_mentions)
        if checkpoint_callback is not None and checkpoint_every > 0:
            if processed >= len(concept_mentions):
                checkpoint_callback(
                    list(concept_mentions),
                    list(vectors),
                    build_embed_stats(
                        processed_source_ids,
                        len(concept_mentions),
                        processed,
                        provider=runtime_config.provider,
                        model=runtime_config.model,
                        backend="provider",
                        vector_dimension=len(vectors[0].vector) if vectors else 0,
                    ),
                    processed_source_ids,
                    [row.id for row in concept_mentions[:processed]],
                )
            elif next_checkpoint > 0 and processed >= next_checkpoint:
                checkpoint_callback(
                    list(concept_mentions),
                    list(vectors),
                    build_embed_stats(
                        processed_source_ids,
                        len(concept_mentions),
                        processed,
                        provider=runtime_config.provider,
                        model=runtime_config.model,
                        backend="provider",
                        vector_dimension=len(vectors[0].vector) if vectors else 0,
                    ),
                    processed_source_ids,
                    [row.id for row in concept_mentions[:processed]],
                )
                while next_checkpoint > 0 and processed >= next_checkpoint:
                    next_checkpoint += checkpoint_every

    return EmbedResult(
        concepts=concept_mentions,
        vectors=vectors,
        processed_source_ids=processed_source_ids,
        processed_concept_ids=[row.id for row in concept_mentions],
        stats=build_embed_stats(
            processed_source_ids,
            len(concept_mentions),
            len(vectors),
            provider=runtime_config.provider,
            model=runtime_config.model,
            backend="provider",
            vector_dimension=len(vectors[0].vector) if vectors else 0,
        ),
    )


def expand_extract_concepts(rows: list[ExtractConceptRecord]) -> list[EmbeddedConceptRecord]:
    expanded: list[EmbeddedConceptRecord] = []
    for row in rows:
        for index, text in enumerate(row.concepts, start=1):
            concept_text = str(text).strip()
            if not concept_text:
                continue
            concept_id = build_embedded_concept_id(row.id, concept_text, index)
            expanded.append(
                EmbeddedConceptRecord(
                    id=concept_id,
                    source_node_id=row.id,
                    text=concept_text,
                )
            )
    return expanded


def build_embedded_concept_id(source_node_id: str, text: str, ordinal: int) -> str:
    token = uuid5(NAMESPACE_URL, f"{source_node_id}\t{text}\t{ordinal}")
    return f"concept:{token}"


def build_embed_stats(
    source_ids: list[str],
    concept_count: int,
    vector_count: int,
    *,
    provider: str = "",
    model: str = "",
    backend: str = "",
    vector_dimension: int = 0,
) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "source_count": len(source_ids),
        "succeeded_sources": len(source_ids),
        "failed_sources": 0,
        "reused_sources": 0,
        "work_units_total": concept_count,
        "work_units_completed": vector_count,
        "work_units_failed": 0,
        "work_units_skipped": 0,
        "concept_count": concept_count,
        "vector_count": vector_count,
        "updated_nodes": 0,
        "updated_edges": 0,
    }
    if backend:
        stats["embedding_backend"] = backend
    if provider:
        stats["embedding_provider"] = provider
    if model:
        stats["embedding_model"] = model
    if vector_dimension > 0:
        stats["vector_dimension"] = vector_dimension
    return stats
