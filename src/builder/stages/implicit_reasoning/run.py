from __future__ import annotations

from typing import Callable

from ...contracts import GraphBundle, deduplicate_graph
from ...pipeline.runtime import PipelineRuntime
from .features import build_graph_features
from .materialize import append_predicted_edges


def run(
    graph_bundle: GraphBundle,
    runtime: PipelineRuntime,
    progress_callback: Callable[[int, int], None] | None = None,
) -> GraphBundle:
    mention_edges = [edge for edge in graph_bundle.edges if edge.type == "MENTIONS"]
    sources_by_concept = {edge.target for edge in mention_edges}
    total_work = max(len(sources_by_concept), 1)
    if progress_callback is not None:
        progress_callback(0, total_work)
    if not sources_by_concept and progress_callback is not None:
        progress_callback(1, 1)
    features, edge_plans = build_graph_features(graph_bundle)
    if progress_callback is not None:
        progress_callback(total_work, total_work)
    predictions = runtime.predict_implicit_relations(features)
    append_predicted_edges(graph_bundle, predictions, edge_plans)
    graph_bundle.metadata["stage"] = "implicit_reasoning"
    graph_bundle.metadata.setdefault("reports", {})["implicit_reasoning"] = {
        "predicted_edge_count": sum(1 for edge in graph_bundle.edges if edge.metadata.get("predicted")),
    }
    return deduplicate_graph(graph_bundle)
