from __future__ import annotations

import re
from dataclasses import dataclass

from ..contracts import GraphBundle
from .legal_reference import build_alias_groups, document_title, extract_document_aliases, title_variants
from .locator import node_sort_key


@dataclass(frozen=True)
class ReferenceGraphContext:
    node_index: dict[str, object]
    parent_by_child: dict[str, str]
    owner_document_by_node: dict[str, str]
    document_nodes: dict[str, object]
    provision_index: dict[str, dict[tuple[str, str, str, str], str]]
    title_to_document_ids: dict[str, list[str]]
    children_by_parent_level: dict[tuple[str, str], list[str]]
    merged_document_aliases: dict[str, dict[str, str]]
    document_alias_groups: dict[str, dict[str, list[tuple[str, str]]]]
    global_document_alias_groups: dict[str, list[tuple[str, str]]]


def previous_sibling(node_id: str, parent_id: str, node_index: dict[str, object], parent_by_child: dict[str, str]) -> str:
    if not parent_id:
        return ""
    siblings = [
        node_index[child_id]
        for child_id, owner_id in parent_by_child.items()
        if owner_id == parent_id and node_index[child_id].level == node_index[node_id].level
    ]
    ordered = sorted(siblings, key=node_sort_key)
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


def tail_label(text: str, suffix: str) -> str:
    matches = re.findall(rf"第(?:[（(])?[一二三四五六七八九十百千万零两〇0-9]+(?:[）)])?{suffix}", text)
    return matches[-1] if matches else ""


def node_label_path(node_id: str, node_index: dict[str, object], parent_by_child: dict[str, str]) -> tuple[str, str, str, str]:
    node = node_index[node_id]
    article_label = ""
    paragraph_label = ""
    item_label = ""
    sub_item_label = ""

    current = node_id
    while True:
        current_node = node_index[current]
        if current_node.level == "article":
            article_label = current_node.name
        elif current_node.level == "paragraph":
            paragraph_label = tail_label(current_node.name, "款")
        elif current_node.level == "item":
            item_label = tail_label(current_node.name, "项")
        elif current_node.level == "sub_item":
            sub_item_label = tail_label(current_node.name, "目")
        if current not in parent_by_child:
            break
        current = parent_by_child[current]
    return article_label, paragraph_label, item_label, sub_item_label


def build_reference_graph_context(graph_bundle: GraphBundle) -> ReferenceGraphContext:
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
    provision_index: dict[str, dict[tuple[str, str, str, str], str]] = {}
    children_by_parent_level: dict[tuple[str, str], list[str]] = {}
    title_to_document_ids: dict[str, list[str]] = {}
    document_aliases: dict[str, dict[str, str]] = {}
    merged_document_aliases: dict[str, dict[str, str]] = {}
    global_document_aliases: dict[str, str] = {}

    for document_id, document_node in document_nodes.items():
        title_to_document_ids.setdefault(document_title(document_node), []).append(document_id)
        document_aliases[document_id] = {}
        for variant in sorted(title_variants(document_node), key=len, reverse=True):
            document_aliases[document_id][variant] = document_title(document_node)
            global_document_aliases.setdefault(variant, document_title(document_node))

    document_texts: dict[str, list[str]] = {document_id: [] for document_id in document_nodes}
    for child_id, parent_id in parent_by_child.items():
        level = node_index[child_id].level
        children_by_parent_level.setdefault((parent_id, level), []).append(child_id)
    for node in graph_bundle.nodes:
        document_id = owner_document_by_node.get(node.id, "")
        if not document_id:
            continue
        if node.text:
            document_texts.setdefault(document_id, []).append(node.text)
        if node.level in {"article", "paragraph", "item", "sub_item"}:
            provision_index.setdefault(document_id, {})[node_label_path(node.id, node_index, parent_by_child)] = node.id
    for key, child_ids in children_by_parent_level.items():
        children_by_parent_level[key] = sorted(
            child_ids,
            key=lambda child_id: node_sort_key(node_index[child_id]),
        )
    for document_id, chunks in document_texts.items():
        document_aliases[document_id].update(extract_document_aliases("\n".join(chunks)))
    for document_id, alias_map in document_aliases.items():
        merged_document_aliases[document_id] = {**global_document_aliases, **alias_map}
    document_alias_groups = {
        document_id: build_alias_groups(alias_map)
        for document_id, alias_map in document_aliases.items()
    }
    return ReferenceGraphContext(
        node_index=node_index,
        parent_by_child=parent_by_child,
        owner_document_by_node=owner_document_by_node,
        document_nodes=document_nodes,
        provision_index=provision_index,
        title_to_document_ids=title_to_document_ids,
        children_by_parent_level=children_by_parent_level,
        merged_document_aliases=merged_document_aliases,
        document_alias_groups=document_alias_groups,
        global_document_alias_groups=build_alias_groups(global_document_aliases),
    )
