from __future__ import annotations

from typing import Callable

from ...contracts import GraphBundle, deduplicate_graph
from ...pipeline.incremental import replace_entity_alignment_outputs
from ...pipeline.runtime import PipelineRuntime
from .candidates import build_parent_by_child, collect_candidate_nodes
from .judge import judge_alignment_pairs
from .merge import UnionFind, build_canonical_graph_parts
from .similarity import collect_candidate_pairs


def run(
    graph_bundle: GraphBundle,
    runtime: PipelineRuntime,
    active_source_ids: set[str] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> GraphBundle:
    del active_source_ids
    graph_bundle = replace_entity_alignment_outputs(graph_bundle)
    candidate_nodes = collect_candidate_nodes(graph_bundle)
    if not candidate_nodes:
        if progress_callback is not None:
            progress_callback(1, 1)
        return deduplicate_graph(graph_bundle)

    pair_total = len(candidate_nodes) * (len(candidate_nodes) - 1) // 2
    total_work = max(pair_total + max(len(candidate_nodes), 1), 1)
    current_work = 0
    if progress_callback is not None:
        progress_callback(0, total_work)
    vectors = runtime.embed_texts([candidate.normalized_text or candidate.name for candidate in candidate_nodes])
    current_work += len(candidate_nodes)
    if progress_callback is not None:
        progress_callback(current_work, total_work)
    candidate_pairs = collect_candidate_pairs(candidate_nodes, vectors)
    union_find = UnionFind([candidate.id for candidate in candidate_nodes])
    for decision in judge_alignment_pairs(candidate_pairs):
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
        if not (node.level == "concept" and node.candidate is True)
    ]
    retained_edges = [
        edge
        for edge in graph_bundle.edges
        if not (edge.type == "MENTIONS" and edge.target in candidate_ids)
    ]

    graph_bundle.nodes = retained_nodes + canonical_nodes
    graph_bundle.edges = retained_edges + canonical_edges
    return deduplicate_graph(graph_bundle)
