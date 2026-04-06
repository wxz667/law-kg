from __future__ import annotations


def iter_candidate_nodes(graph_bundle) -> list[object]:
    return [
        node
        for node in list(graph_bundle.nodes)
        if node.level in {"article", "paragraph", "item", "sub_item", "segment", "appendix"} and node.text
    ]
