from __future__ import annotations

from dataclasses import dataclass, field
from ...contracts import AlignPairRecord, EmbeddedConceptRecord, GraphBundle, deduplicate_graph
from .merge import UnionFind, build_canonical_nodes, build_mentions_edges, build_related_edges


@dataclass
class ResolveResult:
    graph_bundle: GraphBundle
    stats: dict[str, int] = field(default_factory=dict)


def run(
    base_graph: GraphBundle,
    embedded_concepts: list[EmbeddedConceptRecord],
    pairs: list[AlignPairRecord],
) -> ResolveResult:
    union_find = UnionFind([row.id for row in embedded_concepts])
    for pair in pairs:
        if pair.relation == "equivalent":
            union_find.union(pair.left_id, pair.right_id)

    canonical_nodes, canonical_by_member = build_canonical_nodes(embedded_concepts, union_find)
    mention_edges = build_mentions_edges(embedded_concepts, canonical_by_member)
    related_edges = build_related_edges(
        [
            (canonical_by_member[pair.left_id], canonical_by_member[pair.right_id])
            for pair in pairs
            if pair.relation == "related"
            and pair.left_id in canonical_by_member
            and pair.right_id in canonical_by_member
        ]
    )
    graph_bundle = GraphBundle(
        nodes=list(base_graph.nodes) + canonical_nodes,
        edges=list(base_graph.edges) + mention_edges + related_edges,
    )
    graph_bundle = deduplicate_graph(graph_bundle)
    return ResolveResult(
        graph_bundle=graph_bundle,
        stats=build_resolve_stats(
            embedded_concepts=embedded_concepts,
            canonical_node_count=len(canonical_nodes),
            mention_edge_count=len(mention_edges),
            related_edge_count=len(related_edges),
        ),
    )


def build_resolve_stats(
    *,
    embedded_concepts: list[EmbeddedConceptRecord],
    canonical_node_count: int,
    mention_edge_count: int,
    related_edge_count: int,
) -> dict[str, int]:
    return {
        "embedded_concept_count": len(embedded_concepts),
        "canonical_concept_count": canonical_node_count,
        "mention_edge_count": mention_edge_count,
        "related_edge_count": related_edge_count,
        "updated_nodes": canonical_node_count,
        "updated_edges": mention_edge_count + related_edge_count,
    }
