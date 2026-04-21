from __future__ import annotations

from ...contracts import AlignRelationRecord, EdgeRecord, GraphBundle, InferPairRecord, build_edge_id, deduplicate_graph


def normalize_infer_pair(pair: InferPairRecord, *, min_strength: int) -> AlignRelationRecord | None:
    if not pair.relation or pair.relation == "none" or int(pair.strength) < int(min_strength):
        return None
    if pair.relation == "related":
        left_id, right_id = sorted((pair.left_id, pair.right_id))
        return AlignRelationRecord(left_id=left_id, right_id=right_id, relation="related")
    if pair.relation == "is_subordinate":
        return AlignRelationRecord(left_id=pair.left_id, right_id=pair.right_id, relation="is_subordinate")
    if pair.relation == "has_subordinate":
        return AlignRelationRecord(left_id=pair.right_id, right_id=pair.left_id, relation="is_subordinate")
    return None


def materialize_graph(base_graph: GraphBundle, relations: list[AlignRelationRecord]) -> GraphBundle:
    concept_edge_types = {"RELATED_TO", "HAS_SUBORDINATE"}
    edges = [edge for edge in base_graph.edges if edge.type not in concept_edge_types]
    for row in relations:
        if row.relation == "related":
            left_id, right_id = sorted((row.left_id, row.right_id))
            edges.append(
                EdgeRecord(
                    id=build_edge_id(left_id, right_id, "RELATED_TO"),
                    source=left_id,
                    target=right_id,
                    type="RELATED_TO",
                )
            )
        elif row.relation == "is_subordinate":
            edges.append(
                EdgeRecord(
                    id=build_edge_id(row.right_id, row.left_id, "HAS_SUBORDINATE"),
                    source=row.right_id,
                    target=row.left_id,
                    type="HAS_SUBORDINATE",
                )
            )
    return deduplicate_graph(GraphBundle(nodes=list(base_graph.nodes), edges=edges))


def build_materialize_stats(relations: list[AlignRelationRecord], graph_bundle: GraphBundle) -> dict[str, int]:
    return {
        "relation_count": len(relations),
        "updated_nodes": 0,
        "updated_edges": len([edge for edge in graph_bundle.edges if edge.type in {"RELATED_TO", "HAS_SUBORDINATE"}]),
    }
