from __future__ import annotations

from dataclasses import dataclass, field

from ...contracts import AlignPairRecord, ConceptVectorRecord, EmbeddedConceptRecord, GraphBundle
from .classify import AlignClassifyResult, classify_pairs
from .pair import build_pairs
from .resolve import ResolveResult, run as resolve_pairs


@dataclass
class AlignResult:
    pairs: list[AlignPairRecord] = field(default_factory=list)
    graph_bundle: GraphBundle = field(default_factory=GraphBundle)
    classify_result: AlignClassifyResult | None = None
    resolve_result: ResolveResult | None = None


def run(
    base_graph: GraphBundle,
    embedded_concepts: list[EmbeddedConceptRecord],
    concept_vectors: list[ConceptVectorRecord],
    runtime,
) -> AlignResult:
    pairs = build_pairs(embedded_concepts, concept_vectors, runtime)
    classify_result = classify_pairs(pairs, runtime)
    resolve_result = resolve_pairs(base_graph, embedded_concepts, classify_result.pairs)
    return AlignResult(
        pairs=classify_result.pairs,
        graph_bundle=resolve_result.graph_bundle,
        classify_result=classify_result,
        resolve_result=resolve_result,
    )
