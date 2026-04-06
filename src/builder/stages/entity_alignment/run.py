from __future__ import annotations

from typing import Callable

from ...contracts import GraphBundle, deduplicate_graph
from ...pipeline.runtime import PipelineRuntime
from .candidates import build_parent_by_child, collect_candidate_nodes
from .merge import UnionFind, build_canonical_graph_parts
from .similarity import collect_candidate_pairs


def run(
    graph_bundle: GraphBundle,
    runtime: PipelineRuntime,
    progress_callback: Callable[[int, int], None] | None = None,
) -> GraphBundle:
    candidate_nodes = collect_candidate_nodes(graph_bundle)
    if not candidate_nodes:
        if progress_callback is not None:
            progress_callback(1, 1)
        graph_bundle.metadata["stage"] = "entity_alignment"
        graph_bundle.metadata.setdefault("reports", {})["entity_alignment"] = {"canonical_concept_count": 0}
        return deduplicate_graph(graph_bundle)

    pair_total = len(candidate_nodes) * (len(candidate_nodes) - 1) // 2
    total_work = max(pair_total + max(len(candidate_nodes), 1), 1)
    current_work = 0
    if progress_callback is not None:
        progress_callback(0, total_work)
    vectors = runtime.embed_texts([candidate.metadata.get("normalized_text", candidate.name) for candidate in candidate_nodes])
    current_work += len(candidate_nodes)
    if progress_callback is not None:
        progress_callback(current_work, total_work)
    candidate_pairs = collect_candidate_pairs(candidate_nodes, vectors)
    union_find = UnionFind([candidate.id for candidate in candidate_nodes])
    for decision in runtime.judge_alignment(candidate_pairs):
        if decision.approved:
            union_find.union(decision.left_id, decision.right_id)
    if progress_callback is not None:
        progress_callback(total_work, total_work)

    candidate_ids = {candidate.id for candidate in candidate_nodes}
    parent_by_child = build_parent_by_child(graph_bundle, candidate_ids)
    canonical_nodes, canonical_edges = build_canonical_graph_parts(graph_bundle, candidate_nodes, parent_by_child, union_find)

    retained_nodes = [
        node
        for node in graph_bundle.nodes
        if not (node.level == "concept" and node.metadata.get("candidate") is True)
    ]
    retained_edges = [
        edge
        for edge in graph_bundle.edges
        if not (edge.type == "MENTIONS" and edge.target in candidate_ids)
    ]

    graph_bundle.nodes = retained_nodes + canonical_nodes
    graph_bundle.edges = retained_edges + canonical_edges
    graph_bundle.metadata["stage"] = "entity_alignment"
    graph_bundle.metadata.setdefault("reports", {})["entity_alignment"] = {
        "candidate_concept_count": len(candidate_nodes),
        "canonical_concept_count": len(canonical_nodes),
    }
    return deduplicate_graph(graph_bundle)
