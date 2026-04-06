from __future__ import annotations

from ...contracts import GraphBundle


def previous_sibling(node_id: str, parent_id: str, node_index: dict[str, object], parent_by_child: dict[str, str]) -> str:
    if not parent_id:
        return ""
    siblings = [
        node_index[child_id]
        for child_id, owner_id in parent_by_child.items()
        if owner_id == parent_id and node_index[child_id].level == node_index[node_id].level
    ]
    ordered = sorted(siblings, key=lambda item: (int(item.metadata.get("order", 0)), item.id))
    previous = ""
    for sibling in ordered:
        if sibling.id == node_id:
            return previous
        previous = sibling.id
    return ""


def ancestor_at_level(node_id: str, level: str, node_index: dict[str, object], parent_by_child: dict[str, str]) -> str:
    current = node_id
    while current in parent_by_child:
        current = parent_by_child[current]
        if node_index[current].level == level:
            return current
    return ""


def owner_document_id(node_id: str, node_index: dict[str, object], parent_by_child: dict[str, str]) -> str:
    current = node_id
    if node_index[current].level == "document":
        return current
    while current in parent_by_child:
        current = parent_by_child[current]
        if node_index[current].level == "document":
            return current
    return ""


def document_title(document_node: object) -> str:
    title = document_node.name
    return title if title.startswith("《") else f"《{title}》"


def build_relation_context(graph_bundle: GraphBundle) -> dict[str, object]:
    node_index = {node.id: node for node in graph_bundle.nodes}
    parent_by_child = {
        edge.target: edge.source
        for edge in graph_bundle.edges
        if edge.type == "CONTAINS"
    }
    owner_document_by_node = {
        node.id: owner_document_id(node.id, node_index, parent_by_child)
        for node in graph_bundle.nodes
    }
    document_nodes = {
        node.id: node
        for node in graph_bundle.nodes
        if node.level == "document"
    }
    article_index: dict[str, dict[str, str]] = {}
    title_to_document_ids: dict[str, list[str]] = {}
    for document_id, document_node in document_nodes.items():
        title_to_document_ids.setdefault(document_title(document_node), []).append(document_id)
    for node in graph_bundle.nodes:
        if node.level != "article":
            continue
        document_id = owner_document_by_node.get(node.id, "")
        if not document_id:
            continue
        article_index.setdefault(document_id, {})[node.name] = node.id
    return {
        "node_index": node_index,
        "parent_by_child": parent_by_child,
        "owner_document_by_node": owner_document_by_node,
        "document_nodes": document_nodes,
        "article_index": article_index,
        "title_to_document_ids": title_to_document_ids,
    }
