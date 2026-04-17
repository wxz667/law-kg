from __future__ import annotations

from typing import Callable

from ...contracts import GraphBundle, deduplicate_graph
from ...pipeline.incremental import owner_document_by_node, replace_infer_outputs
from ...pipeline.runtime import PipelineRuntime
from .features import build_graph_features
from .materialize import append_predicted_edges


def run(
    graph_bundle: GraphBundle,
    runtime: PipelineRuntime,
    active_source_ids: set[str] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> GraphBundle:
    active_sources = {value for value in (active_source_ids or set()) if value}
    if active_sources:
        graph_bundle = replace_infer_outputs(graph_bundle, active_source_ids=active_sources)
    mention_edges = [edge for edge in graph_bundle.edges if edge.type == "MENTIONS"]
    sources_by_concept = {edge.target for edge in mention_edges}
    total_work = max(len(sources_by_concept), 1)
    if progress_callback is not None:
        progress_callback(0, total_work)
    if not sources_by_concept and progress_callback is not None:
        progress_callback(1, 1)
    features, edge_plans = build_graph_features(
        graph_bundle,
        owner_document_by_node=owner_document_by_node(graph_bundle),
        active_source_ids=active_sources or None,
    )
    predictions = runtime.predict_implicit_relations(features)
    if progress_callback is not None:
        progress_callback(total_work, total_work)
    append_predicted_edges(graph_bundle, predictions, edge_plans)
    return deduplicate_graph(graph_bundle)
