from __future__ import annotations

from ...contracts import DocumentUnitRecord, EdgeRecord, NodeRecord
from ...utils.numbers import chinese_number_to_int, int_to_cn
from .helpers import build_edge, match_heading_level, structural_edge_type
from .items import attach_item_hierarchy, emit_list_items_if_possible, split_item_segments
from .nodes import create_appendix_node
from .patterns import APPENDIX_RE, ITEM_MARKER_RE, SUB_ITEM_MARKER_RE


def finalize_appendices(
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    unit: DocumentUnitRecord,
    document_node: NodeRecord,
    counters: dict[str, int],
) -> None:
    for appendix_no, appendix_label, appendix_lines in split_appendix_blocks(unit.appendix_lines):
        appendix_node = create_appendix_node(source_id=unit.source_id, counters=counters, name=appendix_label)
        nodes.append(appendix_node)
        edges.append(build_edge(document_node.id, appendix_node.id, structural_edge_type(document_node.level, "appendix")))
        block_text = "\n".join(line.strip() for line in appendix_lines if line.strip()).strip()
        if not block_text:
            continue
        if emit_list_items_if_possible(
            nodes=nodes,
            edges=edges,
            source_id=unit.source_id,
            parent=appendix_node,
            counters=counters,
            lines=appendix_lines,
        ):
            continue
        lead, items = split_item_segments(block_text)
        if not items:
            items = split_appendix_row_items(appendix_lines)
        if items:
            attach_item_hierarchy(
                nodes=nodes,
                edges=edges,
                source_id=unit.source_id,
                parent_node=appendix_node,
                parent_level="appendix",
                parent_node_id=appendix_node.id,
                article_no=None,
                article_suffix=None,
                paragraph_no=None,
                segment_no=None,
                parent_text=lead or appendix_label,
                item_segments=items,
            )
            continue
        appendix_node.text = block_text


def split_appendix_row_items(lines: list[str]) -> list[str]:
    normalized_lines = [line.strip() for line in lines if line.strip()]
    if len(normalized_lines) < 2:
        return []
    if any(match_heading_level(line) for line in normalized_lines):
        return []
    if any(ITEM_MARKER_RE.match(line) or SUB_ITEM_MARKER_RE.match(line) for line in normalized_lines):
        return []
    return normalized_lines


def split_appendix_blocks(appendix_lines: list[str]) -> list[tuple[int, str, list[str]]]:
    blocks: list[tuple[int, str, list[str]]] = []
    current_no: int | None = None
    current_label = ""
    current_lines: list[str] = []
    for raw_line in appendix_lines:
        line = raw_line.strip()
        if not line:
            continue
        match = APPENDIX_RE.match(line)
        if match:
            if current_no is not None:
                blocks.append((current_no, current_label, current_lines))
            current_no = chinese_number_to_int(match.group(1))
            current_label = format_appendix_label(current_no)
            current_lines = []
            continue
        if current_no is not None:
            current_lines.append(line)
    if current_no is not None:
        blocks.append((current_no, current_label, current_lines))
    return blocks


def format_appendix_label(appendix_no: int) -> str:
    return f"附件{int_to_cn(appendix_no)}"
