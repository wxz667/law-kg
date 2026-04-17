from __future__ import annotations


def collect_candidate_nodes(graph_bundle) -> list[object]:
    return [
        node
        for node in graph_bundle.nodes
        if node.level == "concept" and node.candidate is True
    ]


def build_parent_by_child(graph_bundle, candidate_ids: set[str]) -> dict[str, str]:
    return {
        edge.target: edge.source
        for edge in graph_bundle.edges
        if edge.type == "MENTIONS" and edge.target in candidate_ids
    }
