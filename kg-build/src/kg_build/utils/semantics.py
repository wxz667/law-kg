from __future__ import annotations

from collections import defaultdict

from ..config import load_schema
from ..contracts import GraphBundle, NodeRecord


def build_structural_maps(bundle: GraphBundle) -> tuple[dict[str, list[str]], dict[str, str]]:
    schema = load_schema()
    structural_edge_types = {
        edge_type
        for edge_type, category in schema.get("edge_type_categories", {}).items()
        if category == "structural"
    }
    children: dict[str, list[str]] = defaultdict(list)
    parent_of: dict[str, str] = {}
    for edge in bundle.edges:
        if edge.type not in structural_edge_types:
            continue
        children[edge.source].append(edge.target)
        parent_of[edge.target] = edge.source
    return dict(children), parent_of


def is_semantic_leaf(
    node_id: str,
    node_index: dict[str, NodeRecord],
    children: dict[str, list[str]],
) -> bool:
    node = node_index[node_id]
    if node.type in {"DocumentNode", "TocNode"}:
        return False
    if children.get(node_id):
        return False
    return bool(node.text.strip())


def is_semantic_aggregate(
    node_id: str,
    node_index: dict[str, NodeRecord],
    children: dict[str, list[str]],
) -> bool:
    node = node_index[node_id]
    if node.type == "DocumentNode":
        return False
    if node.type == "TocNode":
        return bool(children.get(node_id))
    if node.type in {"ProvisionNode", "AppendixNode"}:
        return bool(children.get(node_id))
    return False


def get_semantic_input(
    node_id: str,
    node_index: dict[str, NodeRecord],
    children: dict[str, list[str]],
) -> str:
    node = node_index[node_id]
    if is_semantic_leaf(node_id, node_index, children):
        return node.text.strip()
    if is_semantic_aggregate(node_id, node_index, children):
        return node.summary.strip()
    return ""
