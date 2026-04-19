from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from ...contracts import (
    AlignConceptRecord,
    AlignPairRecord,
    AlignRelationRecord,
    ConceptVectorRecord,
    EquivalenceRecord,
    GraphBundle,
)
from .classify import AlignClassifyResult, classify_pairs
from .embed import build_embed_stats, embed_concepts
from .pair import build_pairs
from .resolve import ResolveResult, run as resolve_state


@dataclass
class AlignResult:
    concepts: list[EquivalenceRecord] = field(default_factory=list)
    vectors: list[ConceptVectorRecord] = field(default_factory=list)
    pairs: list[AlignPairRecord] = field(default_factory=list)
    relations: list[AlignRelationRecord] = field(default_factory=list)
    graph_bundle: GraphBundle = field(default_factory=GraphBundle)
    stats: dict[str, int] = field(default_factory=dict)
    embed_stats: dict[str, int] = field(default_factory=dict)
    llm_errors: list[dict[str, Any]] = field(default_factory=list)
    classify_result: AlignClassifyResult | None = None
    resolve_result: ResolveResult | None = None


def run(
    base_graph: GraphBundle,
    runtime: Any,
    *,
    all_concepts: list[AlignConceptRecord],
    retained_vectors: list[ConceptVectorRecord],
    retained_pairs: list[AlignPairRecord],
    retained_concepts: list[EquivalenceRecord],
    scoped_concepts: list[AlignConceptRecord],
    scoped_embed_concepts: list[AlignConceptRecord] | None = None,
    scoped_recall_concepts: list[AlignConceptRecord] | None = None,
    embed_progress_callback: Callable[[int, int], None] | None = None,
    recall_progress_callback: Callable[[int, int], None] | None = None,
    judge_progress_callback: Callable[[int, int], None] | None = None,
    embed_checkpoint_every: int = 0,
    recall_checkpoint_every: int = 0,
    judge_checkpoint_every: int = 0,
    embed_checkpoint_callback: Callable[[list[ConceptVectorRecord], dict[str, int], list[str], list[dict[str, Any]]], None] | None = None,
    recall_checkpoint_callback: Callable[[list[AlignPairRecord], dict[str, int], list[str]], None] | None = None,
    judge_checkpoint_callback: Callable[[list[AlignPairRecord], dict[str, int], list[str], list[dict[str, Any]]], None] | None = None,
    cancel_event=None,
) -> AlignResult:
    embed_targets = list(scoped_concepts if scoped_embed_concepts is None else scoped_embed_concepts)
    if embed_targets:
        embedded_vectors, embed_stats, _, embed_errors = embed_concepts(
            embed_targets,
            runtime,
            progress_callback=embed_progress_callback,
            checkpoint_every=embed_checkpoint_every,
            checkpoint_callback=embed_checkpoint_callback,
            cancel_event=cancel_event,
        )
    else:
        embedded_vectors = []
        embed_stats = build_embed_stats([], 0, 0, [])
        embed_errors = []
        if embed_progress_callback is not None:
            embed_progress_callback(0, 1)
    all_vectors = list(retained_vectors) + list(embedded_vectors)
    recall_targets = list(scoped_concepts if scoped_recall_concepts is None else scoped_recall_concepts)
    recalled_pairs = build_pairs(
        recall_targets,
        all_concepts,
        all_vectors,
        retained_concepts,
        runtime,
        progress_callback=recall_progress_callback,
        checkpoint_every=recall_checkpoint_every,
        checkpoint_callback=recall_checkpoint_callback,
    )
    all_pairs = list(retained_pairs) + list(recalled_pairs)
    classify_result = classify_pairs(
        all_pairs,
        all_concepts,
        retained_concepts,
        runtime,
        progress_callback=judge_progress_callback,
        checkpoint_every=judge_checkpoint_every,
        checkpoint_callback=judge_checkpoint_callback,
        cancel_event=cancel_event,
    )
    resolve_result = resolve_state(base_graph, all_concepts, classify_result.pairs, retained_concepts)
    return AlignResult(
        concepts=resolve_result.equivalence,
        vectors=all_vectors,
        pairs=resolve_result.pairs,
        relations=resolve_result.relations,
        graph_bundle=resolve_result.graph_bundle,
        stats=build_align_stats(
            concepts=resolve_result.equivalence,
            vectors=all_vectors,
            pairs=resolve_result.pairs,
            relations=resolve_result.relations,
            embed_stats=embed_stats,
            judge_stats=classify_result.stats,
            resolve_stats=resolve_result.stats,
        ),
        embed_stats=embed_stats,
        llm_errors=embed_errors + list(classify_result.llm_errors),
        classify_result=classify_result,
        resolve_result=resolve_result,
    )


def build_align_stats(
    *,
    concepts: list[EquivalenceRecord],
    vectors: list[ConceptVectorRecord],
    pairs: list[AlignPairRecord],
    relations: list[AlignRelationRecord],
    embed_stats: dict[str, int],
    judge_stats: dict[str, int],
    resolve_stats: dict[str, int],
) -> dict[str, int]:
    return {
        "concept_count": len(concepts),
        "vector_count": len(vectors),
        "pair_count": len(pairs),
        "relation_count": len(relations),
        "llm_request_count": int(embed_stats.get("llm_request_count", 0)) + int(judge_stats.get("llm_request_count", 0)),
        "llm_error_count": int(embed_stats.get("llm_error_count", 0)) + int(judge_stats.get("llm_error_count", 0)),
        "retry_count": int(embed_stats.get("retry_count", 0)) + int(judge_stats.get("retry_count", 0)),
        "updated_nodes": int(resolve_stats.get("updated_nodes", 0)),
        "updated_edges": int(resolve_stats.get("updated_edges", 0)),
    }
