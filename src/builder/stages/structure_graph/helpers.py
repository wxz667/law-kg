from __future__ import annotations

from ...contracts import EdgeRecord, NodeRecord, build_edge_id
from ...utils.ids import slugify
from ...utils.document_layout import match_heading_level, normalize_segment_heading
from ...utils.numbers import int_to_cn
from .patterns import LEVEL_ORDER, STRUCTURAL_EDGES


def structural_edge_type(parent_level: str, child_level: str) -> str:
    edge_type = STRUCTURAL_EDGES.get((parent_level, child_level))
    if edge_type is None:
        raise ValueError(f"Unsupported structural edge from {parent_level} to {child_level}")
    return edge_type


def find_parent(level_stack: dict[str, NodeRecord], child_level: str) -> NodeRecord:
    child_index = LEVEL_ORDER.index(child_level)
    for level in reversed(LEVEL_ORDER[:child_index]):
        if level in level_stack:
            return level_stack[level]
    return level_stack["document"]


def clear_lower_levels(level_stack: dict[str, NodeRecord], current_level: str) -> None:
    current_index = LEVEL_ORDER.index(current_level)
    for level in LEVEL_ORDER[current_index + 1 :]:
        level_stack.pop(level, None)


def build_edge(source_id: str, target_id: str, edge_type: str) -> EdgeRecord:
    return EdgeRecord(id=build_edge_id(source_id, target_id, edge_type), source=source_id, target=target_id, type=edge_type)


def source_id_from_node_id(node_id: str) -> str:
    parts = node_id.split(":")
    if len(parts) < 2:
        raise ValueError(f"Unsupported node id: {node_id}")
    return parts[1]


def build_item_name(parent: NodeRecord, item_no: int) -> str:
    short_name = f"第{int_to_cn(item_no)}项"
    if parent.level in {"article", "paragraph", "appendix"}:
        return f"{parent.name}{short_name}"
    return short_name


def build_sub_item_name(item_node: NodeRecord, sub_item_no: int) -> str:
    short_name = f"第{int_to_cn(sub_item_no)}目"
    if item_node.name:
        return f"{item_node.name}{short_name}"
    return short_name


def build_fallback_item_id(
    source_id: str,
    article_no: int | None,
    article_suffix: int | None,
    paragraph_no: int | None,
    segment_no: int | None,
    item_no: int,
    parent_node_id: str | None = None,
) -> str:
    from ...utils.numbers import format_article_key

    if segment_no is not None:
        return f"item:{slugify(source_id)}:segment:{segment_no:04d}:{item_no:02d}"
    if parent_node_id:
        return f"item:{slugify(source_id)}:parent:{slugify(parent_node_id)}:{item_no:02d}"
    if article_no is None:
        raise ValueError("Article-based item fallback id requires article_no.")
    article_key = format_article_key(article_no, article_suffix)
    base = f"item:{slugify(source_id)}:{article_key}"
    if paragraph_no is not None:
        base = f"{base}:{paragraph_no:02d}"
    return f"{base}:{item_no:02d}"


def build_fallback_sub_item_id(
    source_id: str,
    article_no: int | None,
    article_suffix: int | None,
    paragraph_no: int | None,
    segment_no: int | None,
    item_no: int,
    sub_item_no: int,
    parent_node_id: str | None = None,
) -> str:
    from ...utils.numbers import format_article_key

    if segment_no is not None:
        return f"sub_item:{slugify(source_id)}:segment:{segment_no:04d}:{item_no:02d}:{sub_item_no:02d}"
    if parent_node_id:
        return f"sub_item:{slugify(source_id)}:parent:{slugify(parent_node_id)}:{item_no:02d}:{sub_item_no:02d}"
    if article_no is None:
        raise ValueError("Article-based sub-item fallback id requires article_no.")
    article_key = format_article_key(article_no, article_suffix)
    base = f"sub_item:{slugify(source_id)}:{article_key}"
    if paragraph_no is not None:
        base = f"{base}:{paragraph_no:02d}"
    return f"{base}:{item_no:02d}:{sub_item_no:02d}"
