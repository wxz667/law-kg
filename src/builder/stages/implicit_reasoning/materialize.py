from __future__ import annotations

from ...contracts import EdgeRecord, build_edge_id


def append_predicted_edges(graph_bundle, predictions, edge_plans) -> None:
    for prediction, edge_plan in zip(predictions, edge_plans):
        if prediction.score < 0.5:
            continue
        source_id, target_id, concept_id = edge_plan
        graph_bundle.edges.append(
            EdgeRecord(
                id=build_edge_id(source_id, target_id, prediction.relation_type, "predicted"),
                source=source_id,
                target=target_id,
                type=prediction.relation_type,
                weight=prediction.score,
                predicted=True,
                model=prediction.model,
                concept_id=concept_id,
            )
        )
