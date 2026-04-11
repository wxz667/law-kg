from __future__ import annotations

from collections import defaultdict

from ...contracts import EdgeRecord, NodeRecord, build_edge_id
from ...utils.locator import NodeLocator, node_id_from_locator, node_locator_from_node_id, source_id_from_node_id
from .nodes import build_item_name, build_sub_item_name


def collapse_single_paragraph_item_branches(nodes: list[NodeRecord], edges: list[EdgeRecord]) -> None:
    node_index = {node.id: node for node in nodes}
    children_by_parent: dict[str, list[str]] = defaultdict(list)
    paragraph_parent: dict[str, str] = {}
    for edge in edges:
        if edge.type != "CONTAINS":
            continue
        children_by_parent[edge.source].append(edge.target)
        if edge.target in node_index and node_index[edge.target].level == "paragraph":
            paragraph_parent[edge.target] = edge.source

    collapsed_paragraph_ids: set[str] = set()
    id_remap: dict[str, str] = {}
    renamed_nodes: dict[str, tuple[str, str]] = {}

    for article in nodes:
        if article.level != "article":
            continue
        paragraph_ids = [child_id for child_id in children_by_parent.get(article.id, []) if node_index.get(child_id) and node_index[child_id].level == "paragraph"]
        if len(paragraph_ids) != 1:
            continue
        paragraph_id = paragraph_ids[0]
        item_ids = [child_id for child_id in children_by_parent.get(paragraph_id, []) if node_index.get(child_id) and node_index[child_id].level == "item"]
        if not item_ids:
            continue
        paragraph_node = node_index[paragraph_id]
        if paragraph_node.text and not article.text:
            article.text = paragraph_node.text
        collapsed_paragraph_ids.add(paragraph_id)
        for item_id in item_ids:
            remap_item_branch(
                item_id=item_id,
                article_node=article,
                node_index=node_index,
                children_by_parent=children_by_parent,
                id_remap=id_remap,
                renamed_nodes=renamed_nodes,
            )

    if not collapsed_paragraph_ids and not id_remap:
        return

    for node in nodes:
        if node.id in collapsed_paragraph_ids:
            continue
        if node.id not in id_remap:
            continue
        old_id = node.id
        node.id = id_remap[old_id]
        renamed_name = renamed_nodes.get(old_id)
        if renamed_name is not None:
            node.name = renamed_name[1]

    rewritten_edges: list[EdgeRecord] = []
    for edge in edges:
        if edge.target in collapsed_paragraph_ids:
            continue
        if edge.source in collapsed_paragraph_ids:
            article_id = paragraph_parent.get(edge.source, "")
            if not article_id:
                continue
            source_id = article_id
        else:
            source_id = id_remap.get(edge.source, edge.source)
        target_id = id_remap.get(edge.target, edge.target)
        rewritten_edges.append(
            EdgeRecord(
                id=build_edge_id(source_id, target_id, edge.type),
                source=source_id,
                target=target_id,
                type=edge.type,
            )
        )

    nodes[:] = [node for node in nodes if node.id not in collapsed_paragraph_ids]
    edges[:] = rewritten_edges


def remap_item_branch(
    *,
    item_id: str,
    article_node: NodeRecord,
    node_index: dict[str, NodeRecord],
    children_by_parent: dict[str, list[str]],
    id_remap: dict[str, str],
    renamed_nodes: dict[str, tuple[str, str]],
) -> None:
    item_node = node_index[item_id]
    item_locator = node_locator_from_node_id(item_id)
    if item_locator is None or item_locator.item_no is None:
        return
    source_id = source_id_from_node_id(item_id)
    new_item_id = node_id_from_locator(
        NodeLocator(
            kind="provision",
            article_no=item_locator.article_no,
            article_suffix=item_locator.article_suffix,
            item_no=item_locator.item_no,
        ),
        source_id,
    ) or item_id
    new_item_name = build_item_name(article_node, item_locator.item_no)
    id_remap[item_id] = new_item_id
    renamed_nodes[item_id] = (new_item_id, new_item_name)
    item_node.name = new_item_name

    for sub_item_id in children_by_parent.get(item_id, []):
        sub_item_node = node_index.get(sub_item_id)
        if sub_item_node is None or sub_item_node.level != "sub_item":
            continue
        sub_locator = node_locator_from_node_id(sub_item_id)
        if sub_locator is None or sub_locator.item_no is None or sub_locator.sub_item_no is None:
            continue
        new_sub_item_id = node_id_from_locator(
            NodeLocator(
                kind="provision",
                article_no=sub_locator.article_no,
                article_suffix=sub_locator.article_suffix,
                item_no=sub_locator.item_no,
                sub_item_no=sub_locator.sub_item_no,
            ),
            source_id,
        ) or sub_item_id
        new_sub_item_name = build_sub_item_name(item_node, sub_locator.sub_item_no)
        id_remap[sub_item_id] = new_sub_item_id
        renamed_nodes[sub_item_id] = (new_sub_item_id, new_sub_item_name)
        sub_item_node.name = new_sub_item_name
