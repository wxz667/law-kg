from __future__ import annotations

from itertools import combinations


def build_graph_features(graph_bundle) -> tuple[list[dict[str, object]], list[tuple[str, str, str]]]:
    concept_nodes = [node for node in graph_bundle.nodes if node.level == "concept"]
    mention_edges = [edge for edge in graph_bundle.edges if edge.type == "MENTIONS"]
    sources_by_concept: dict[str, list[str]] = {}
    for edge in mention_edges:
        sources_by_concept.setdefault(edge.target, []).append(edge.source)

    features: list[dict[str, object]] = []
    edge_plans: list[tuple[str, str, str]] = []
    concept_index = {node.id: node for node in concept_nodes}
    for concept_id, source_nodes in sources_by_concept.items():
        unique_sources = sorted(set(source_nodes))
        if len(unique_sources) < 2:
            continue
        concept = concept_index.get(concept_id)
        for source_id, target_id in combinations(unique_sources, 2):
            features.append(
                {
                    "source_node_id": source_id,
                    "target_node_id": target_id,
                    "concept_id": concept_id,
                    "concept_name": concept.name if concept else concept_id,
                    "overlap_score": 0.82 if len(unique_sources) > 2 else 0.68,
                }
            )
            edge_plans.append((source_id, target_id, concept_id))
    return features, edge_plans
