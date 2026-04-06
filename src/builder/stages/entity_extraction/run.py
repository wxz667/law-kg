from __future__ import annotations

from typing import Callable

from ...contracts import GraphBundle, deduplicate_graph
from ...pipeline.runtime import PipelineRuntime
from .extract import iter_candidate_nodes
from .materialize import append_concept_candidate


def run(
    graph_bundle: GraphBundle,
    runtime: PipelineRuntime,
    progress_callback: Callable[[int, int], None] | None = None,
) -> GraphBundle:
    concept_counter = 0
    seen_mentions: set[tuple[str, str, str, int, int]] = set()
    candidate_nodes = iter_candidate_nodes(graph_bundle)
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
    graph_bundle.metadata["stage"] = "entity_extraction"
    graph_bundle.metadata.setdefault("reports", {})["entity_extraction"] = {
        "candidate_concept_count": concept_counter,
    }
    return deduplicate_graph(graph_bundle)
