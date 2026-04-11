from __future__ import annotations

from ...contracts import (
    EdgeRecord,
    GraphBundle,
    ReferenceCandidateRecord,
    RelationClassifyRecord,
    build_edge_id,
    deduplicate_graph,
)


def build_relation_result(
    candidate: ReferenceCandidateRecord,
    *,
    label: str,
    score: float,
    source: str,
) -> RelationClassifyRecord:
    return RelationClassifyRecord(
        id=candidate.id,
        source_node_id=candidate.source_node_id,
        text=candidate.text,
        target_node_ids=list(candidate.target_node_ids),
        target_categories=list(candidate.target_categories),
        label=label,
        score=score,
        source=source,
    )


def update_stats(
    stats: dict[str, object],
    *,
    relation_type: str,
    decision_source: str,
    target_count: int,
    source_category: str,
) -> None:
    stats["model_decision_count"] += int(decision_source.startswith("model_") or decision_source.startswith("rule_corrected_"))
    stats["llm_arbiter_count"] += int(decision_source in {"llm_arbiter", "rule_corrected_llm", "rule_corrected_title_llm"})
    stats["rule_corrected_count"] += int(decision_source.startswith("rule_corrected_"))
    if relation_type == "INTERPRETS":
        stats["interprets_count"] += target_count
        if source_category == "interpretation":
            stats["judicial_interprets_count"] += target_count
    else:
        stats["references_count"] += target_count
        if source_category == "interpretation":
            stats["judicial_references_count"] += target_count
        else:
            stats["ordinary_reference_count"] += target_count
    keyed = f"{source_category}_{relation_type.lower()}"
    stats[keyed] = int(stats.get(keyed, 0)) + target_count


def materialize_relation_plans(
    graph_bundle: GraphBundle,
    results: list[RelationClassifyRecord],
) -> GraphBundle:
    node_ids = {node.id for node in graph_bundle.nodes}
    graph_bundle.edges = [edge for edge in graph_bundle.edges if edge.type not in {"REFERENCES", "INTERPRETS"}]
    for result in results:
        if result.source_node_id not in node_ids:
            continue
        for target_node_id in result.target_node_ids:
            if target_node_id not in node_ids:
                continue
            graph_bundle.edges.append(
                EdgeRecord(
                    id=build_edge_id(result.source_node_id, target_node_id, result.label),
                    source=result.source_node_id,
                    target=target_node_id,
                    type=result.label,
                    weight=resolve_edge_weight(result),
                )
            )
    return deduplicate_graph(graph_bundle)


def resolve_edge_weight(result: RelationClassifyRecord) -> float:
    if result.source.startswith("rule_"):
        return 1.0
    score = min(max(float(result.score), 0.0), 1.0)
    if result.label == "INTERPRETS":
        return score
    return 1.0 - score
