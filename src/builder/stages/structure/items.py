from __future__ import annotations

from ...contracts import EdgeRecord, NodeRecord
from ...utils.numbers import to_fullwidth_digit_text
from .helpers import (
    build_edge,
    structural_edge_type,
)
from .nodes import create_direct_item_node, create_item_node, create_sub_item_node
from .patterns import ITEM_MARKER_RE, NUMBERED_LIST_RE, PURE_INTEGER_RE, SUB_ITEM_MARKER_RE


def emit_list_items_if_possible(
    *,
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    source_id: str,
    parent: NodeRecord,
    counters: dict[str, int],
    lines: list[str],
) -> bool:
    del counters
    records = parse_list_records(lines)
    if not records:
        return False
    for order, (text, _metadata) in enumerate(records, start=1):
        item_node = create_direct_item_node(source_id=source_id, parent=parent, item_no=order, text=text)
        nodes.append(item_node)
        edges.append(build_edge(parent.id, item_node.id, structural_edge_type(parent.level, "item")))
    return True


def parse_list_records(lines: list[str]) -> list[tuple[str, dict[str, object]]]:
    normalized_lines = [line.strip() for line in lines if line.strip()]
    if not normalized_lines:
        return []
    numbered_records = parse_numbered_list_records(normalized_lines)
    if numbered_records:
        return numbered_records
    tabular_records = parse_tabular_list_records(normalized_lines)
    if tabular_records:
        return tabular_records
    return []


def parse_numbered_list_records(lines: list[str]) -> list[tuple[str, dict[str, object]]]:
    records: list[tuple[str, dict[str, object]]] = []
    current_lines: list[str] = []
    current_order = 0
    saw_numbered = False
    for line in lines:
        match = NUMBERED_LIST_RE.match(line)
        if match:
            saw_numbered = True
            if current_lines:
                records.append(("\n".join(current_lines).strip(), {"order": current_order}))
            current_order = int(to_fullwidth_digit_text(match.group("index")))
            current_lines = [match.group("body").strip()]
            continue
        if current_lines:
            current_lines.append(line)
        elif saw_numbered:
            return []
    if current_lines:
        records.append(("\n".join(current_lines).strip(), {"order": current_order}))
    return records if saw_numbered else []


def parse_tabular_list_records(lines: list[str]) -> list[tuple[str, dict[str, object]]]:
    if len(lines) < 4:
        return []
    header_score = sum(1 for item in lines[:6] if item in {"序号", "名称", "司法解释名称", "发文日期、文号", "废止理由", "理由"})
    if header_score < 2:
        return []
    header_end = detect_table_header_end(lines)
    if header_end is None:
        return []
    data_lines = lines[header_end:]
    if not data_lines or not PURE_INTEGER_RE.match(data_lines[0]):
        return []
    records: list[tuple[str, dict[str, object]]] = []
    current_rows: list[str] = []
    current_order = 0
    for line in data_lines:
        if PURE_INTEGER_RE.match(line):
            if current_rows:
                records.append(("\n".join(current_rows).strip(), {"order": current_order}))
            current_order = int(to_fullwidth_digit_text(line))
            current_rows = []
            continue
        current_rows.append(line)
    if current_rows:
        records.append(("\n".join(current_rows).strip(), {"order": current_order}))
    return records


def detect_table_header_end(lines: list[str]) -> int | None:
    for index in range(1, min(len(lines), 12)):
        if PURE_INTEGER_RE.match(lines[index]):
            return index
    return None


def attach_item_hierarchy(
    *,
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    source_id: str,
    parent_node: NodeRecord,
    parent_level: str,
    article_no: int | None,
    article_suffix: int | None,
    paragraph_no: int | None,
    segment_no: int | None,
    parent_node_id: str | None,
    parent_text: str,
    item_segments: list[str],
) -> None:
    del parent_level
    parent_node.text = parent_text.strip()
    for item_index, item_source_text in enumerate(item_segments, start=1):
        _, item_body_text = extract_item_marker(item_source_text)
        item_lead, sub_item_segments = split_sub_item_segments(item_body_text)
        item_node = create_item_node(
            source_id=source_id,
            parent_node=parent_node,
            article_no=article_no,
            article_suffix=article_suffix,
            paragraph_no=paragraph_no,
            segment_no=segment_no,
            item_no=item_index,
            parent_node_id=parent_node_id,
            text=item_lead if sub_item_segments else item_body_text.strip(),
        )
        nodes.append(item_node)
        edges.append(build_edge(parent_node.id, item_node.id, structural_edge_type(parent_node.level, "item")))
        for sub_item_index, sub_item_source_text in enumerate(sub_item_segments, start=1):
            _, sub_item_body_text = extract_sub_item_marker(sub_item_source_text)
            sub_item_node = create_sub_item_node(
                source_id=source_id,
                item_node=item_node,
                article_no=article_no,
                article_suffix=article_suffix,
                paragraph_no=paragraph_no,
                segment_no=segment_no,
                item_no=item_index,
                sub_item_no=sub_item_index,
                parent_node_id=parent_node_id,
                text=sub_item_body_text.strip(),
            )
            nodes.append(sub_item_node)
            edges.append(build_edge(item_node.id, sub_item_node.id, structural_edge_type("item", "sub_item")))


def collapse_item_only_paragraphs(raw_paragraphs: list[str]) -> list[str]:
    paragraph_texts = [text.strip() for text in raw_paragraphs if text and text.strip()]
    if not paragraph_texts:
        return []
    collapsed: list[str] = []
    for text in paragraph_texts:
        if not collapsed:
            collapsed.append(text)
            continue
        if ITEM_MARKER_RE.match(text):
            collapsed[-1] = f"{collapsed[-1]}\n{text}"
            continue
        collapsed.append(text)
    return collapsed


def split_item_segments(text: str) -> tuple[str, list[str]]:
    lines = [line.rstrip() for line in text.splitlines()]
    first_marker_index = next((index for index, line in enumerate(lines) if ITEM_MARKER_RE.match(line.strip())), None)
    if first_marker_index is None:
        return text.strip(), []
    lead_lines = [line for line in lines[:first_marker_index] if line.strip()]
    segments: list[str] = []
    current: list[str] = []
    for raw_line in lines[first_marker_index:]:
        line = raw_line.strip()
        if not line:
            continue
        if ITEM_MARKER_RE.match(line):
            if current:
                segments.append("\n".join(current).strip())
            current = [line]
            continue
        current.append(line)
    if current:
        segments.append("\n".join(current).strip())
    return "\n".join(lead_lines).strip(), segments


def split_sub_item_segments(text: str) -> tuple[str, list[str]]:
    lines = [line.rstrip() for line in text.splitlines()]
    first_marker_index = next((index for index, line in enumerate(lines) if SUB_ITEM_MARKER_RE.match(line.strip())), None)
    if first_marker_index is None:
        return text.strip(), []
    lead_lines = [line for line in lines[:first_marker_index] if line.strip()]
    segments: list[str] = []
    current: list[str] = []
    for raw_line in lines[first_marker_index:]:
        line = raw_line.strip()
        if not line:
            continue
        if SUB_ITEM_MARKER_RE.match(line):
            if current:
                segments.append("\n".join(current).strip())
            current = [line]
            continue
        current.append(line)
    if current:
        segments.append("\n".join(current).strip())
    return "\n".join(lead_lines).strip(), segments


def extract_item_marker(text: str) -> tuple[str, str]:
    match = ITEM_MARKER_RE.match(text)
    if not match:
        return "", text.strip()
    return match.group(1), text[match.end() :].strip()


def extract_sub_item_marker(text: str) -> tuple[str, str]:
    match = SUB_ITEM_MARKER_RE.match(text)
    if not match:
        return "", text.strip()
    return match.group(1), text[match.end() :].strip()
