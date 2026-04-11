from __future__ import annotations

from typing import Callable

from ...contracts import GraphBundle, deduplicate_graph
from ...pipeline.incremental import owner_document_by_node, owner_source_id_for_node, replace_entity_extraction_outputs
from ...pipeline.runtime import PipelineRuntime
from .extract import iter_candidate_nodes
from .materialize import append_concept_candidate


def run(
    graph_bundle: GraphBundle,
    runtime: PipelineRuntime,
    active_source_ids: set[str] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> GraphBundle:
    active_sources = {value for value in (active_source_ids or set()) if value}
    if active_sources:
        graph_bundle = replace_entity_extraction_outputs(graph_bundle, active_source_ids=active_sources)
    concept_counter = max(
        (
            int(node.order or 0)
            for node in graph_bundle.nodes
            if node.level == "concept"
        ),
        default=0,
    )
    seen_mentions: set[tuple[str, str, str, int, int]] = set()
    owners = owner_document_by_node(graph_bundle)
    candidate_nodes = iter_candidate_nodes(graph_bundle)
    if active_sources:
        candidate_nodes = [node for node in candidate_nodes if owner_source_id_for_node(owners, node.id) in active_sources]
    total_candidates = max(len(candidate_nodes), 1)
    if progress_callback is not None:
        progress_callback(0, total_candidates)
    if not candidate_nodes and progress_callback is not None:
        progress_callback(1, 1)
    for index, node in enumerate(candidate_nodes, start=1):
        for prediction in runtime.predict_entities(node.text):
            normalized_text = prediction.normalized_text or prediction.text
            key = (node.id, normalized_text, prediction.label, prediction.start_offset, prediction.end_offset)
            if key in seen_mentions:
                continue
            seen_mentions.add(key)
            concept_counter += 1
            append_concept_candidate(graph_bundle, node=node, prediction=prediction, concept_counter=concept_counter)
        if progress_callback is not None:
            progress_callback(index, total_candidates)
    return deduplicate_graph(graph_bundle)
