from __future__ import annotations

import re

from ..config import load_schema
from ..contracts import EdgeRecord, GraphBundle, NodeRecord, SourceDocumentRecord
from ..utils.ids import slugify
from ..utils.locator import NodeLocator, node_id_from_locator, node_locator_from_node_id
from ..utils.numbers import (
    chinese_number_to_int,
    format_article_key,
    int_to_cn,
    parse_article_components,
)

PART_RE = re.compile(r"^第[一二三四五六七八九十百零]+编\s+.+$")
CHAPTER_RE = re.compile(r"^第[一二三四五六七八九十百零]+章\s+.+$")
SECTION_RE = re.compile(r"^第[一二三四五六七八九十百零]+节\s+.+$")
ARTICLE_RE = re.compile(
    r"^(第[一二三四五六七八九十百千万零两〇0-9]+条(?:之[一二三四五六七八九十百千万零两〇0-9]+)?)(.*)$"
)
APPENDIX_RE = re.compile(r"^附件([一二三四五六七八九十百千万零两〇0-9]+)$")
ITEM_MARKER_RE = re.compile(r"((?:（|\()[一二三四五六七八九十]+(?:）|\))|[一二三四五六七八九十]+、)")
SUB_ITEM_MARKER_RE = re.compile(r"((?:[0-9０-９]+[.．、])|(?:（|\()[0-9０-９]+(?:）|\)))")
SEGMENT_HEADING_RE = re.compile(r"^[一二三四五六七八九十百千]+、.+$")

DISPATCH_KEYWORDS = ("通知", "公告", "函", "答复", "批复")

STRUCTURAL_EDGES = {
    (item["parent_level"], item["child_level"]): item["edge_type"]
    for item in load_schema().get("structural_edges", [])
}
LEVEL_ORDER = load_schema().get("levels", [])
LEVEL_TO_NODE_TYPE = load_schema().get("level_to_node_type", {})


def run(source_document: SourceDocumentRecord) -> GraphBundle:
    nodes: list[NodeRecord] = []
    edges: list[EdgeRecord] = []
    counters = {"part": 0, "chapter": 0, "section": 0, "segment": 0, "appendix": 0}

    document_node = build_document_node(source_document)
    nodes.append(document_node)

    document_form = detect_document_form(source_document)
    if document_form == "dispatch":
        finalize_dispatch_document(nodes, edges, source_document, document_node, counters)
    else:
        finalize_codified_document(nodes, edges, source_document, document_node, counters)

    finalize_appendices(nodes, edges, source_document, document_node, counters)

    return GraphBundle(
        graph_id=f"graph:{slugify(source_document.source_id)}",
        nodes=nodes,
        edges=edges,
    )


def build_document_node(source_document: SourceDocumentRecord) -> NodeRecord:
    metadata = dict(source_document.metadata)
    return NodeRecord(
        id=node_id_from_locator(NodeLocator(kind="document"), source_document.source_id)
        or f"document:{slugify(source_document.source_id)}",
        type=LEVEL_TO_NODE_TYPE["document"],
        name=source_document.title,
        level="document",
        document_type=metadata.get("document_type", source_document.source_type),
        document_subtype=metadata.get("document_subtype", ""),
        status=metadata.get("status", ""),
        metadata={
            "issuer": metadata.get("issuer", ""),
            "publish_date": metadata.get("publish_date", ""),
            "effective_date": metadata.get("effective_date", ""),
            "issuer_type": metadata.get("issuer_type", ""),
            "doc_no": metadata.get("doc_no", ""),
            "region": metadata.get("region", ""),
            "preface_text": metadata.get("preface_text", source_document.preface_text),
        },
    )


def detect_document_form(source_document: SourceDocumentRecord) -> str:
    body_lines = [line.strip() for line in source_document.body_lines if line.strip()]
    if any(ARTICLE_RE.match(line) for line in body_lines):
        return "codified"
    if any(SEGMENT_HEADING_RE.match(line) for line in body_lines):
        return "codified"
    title = source_document.title
    preface = source_document.preface_text or ""
    body_text = "\n".join(body_lines[:12])
    if any(keyword in title or keyword in preface or keyword in body_text for keyword in DISPATCH_KEYWORDS):
        return "dispatch"
    return "codified"


def finalize_dispatch_document(
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    source_document: SourceDocumentRecord,
    document_node: NodeRecord,
    counters: dict[str, int],
) -> None:
    text = "\n".join(line.strip() for line in source_document.body_lines if line.strip()).strip()
    if not text:
        return
    create_segment_node(
        nodes=nodes,
        edges=edges,
        source_id=source_document.source_id,
        parent=document_node,
        text=text,
        counters=counters,
        split_items=False,
    )


def finalize_codified_document(
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    source_document: SourceDocumentRecord,
    document_node: NodeRecord,
    counters: dict[str, int],
) -> None:
    body_lines = strip_leading_toc_block(source_document.body_lines)
    level_stack: dict[str, NodeRecord] = {"document": document_node}
    current_article: NodeRecord | None = None
    current_article_paragraphs: list[str] = []
    pending_segment_lines: list[str] = []
    pending_segment_start_index = 0

    for paragraph_index, raw_line in enumerate(body_lines, start=1):
        line = raw_line.strip()
        if not line:
            continue

        level = match_heading_level(line)
        if level is not None:
            if current_article is not None:
                finalize_article(nodes, edges, current_article, current_article_paragraphs)
                current_article = None
                current_article_paragraphs = []
            if pending_segment_lines:
                finalize_segment(
                    nodes=nodes,
                    edges=edges,
                    source_id=source_document.source_id,
                    parent=find_parent(level_stack, "segment"),
                    lines=pending_segment_lines,
                    start_index=pending_segment_start_index,
                    end_index=paragraph_index - 1,
                    counters=counters,
                )
                pending_segment_lines = []

            if level == "article":
                article_node, inline_text = build_article_node(source_document.source_id, line)
                parent = find_parent(level_stack, level)
                nodes.append(article_node)
                edges.append(build_edge(parent.id, article_node.id, structural_edge_type(parent.level, level)))
                level_stack[level] = article_node
                clear_lower_levels(level_stack, level)
                current_article = article_node
                current_article_paragraphs = [inline_text] if inline_text else []
                continue

            toc_node = build_toc_node(level, line, source_document.source_id, counters, paragraph_index)
            parent = find_parent(level_stack, level)
            nodes.append(toc_node)
            edges.append(build_edge(parent.id, toc_node.id, structural_edge_type(parent.level, level)))
            level_stack[level] = toc_node
            clear_lower_levels(level_stack, level)
            continue

        if current_article is not None:
            current_article_paragraphs.append(line)
            continue

        if SEGMENT_HEADING_RE.match(line) and pending_segment_lines:
            finalize_segment(
                nodes=nodes,
                edges=edges,
                source_id=source_document.source_id,
                parent=find_parent(level_stack, "segment"),
                lines=pending_segment_lines,
                start_index=pending_segment_start_index,
                end_index=paragraph_index - 1,
                counters=counters,
            )
            pending_segment_lines = []

        if not pending_segment_lines:
            pending_segment_start_index = paragraph_index
        pending_segment_lines.append(line)

    if current_article is not None:
        finalize_article(nodes, edges, current_article, current_article_paragraphs)
    if pending_segment_lines:
        finalize_segment(
            nodes=nodes,
            edges=edges,
            source_id=source_document.source_id,
            parent=find_parent(level_stack, "segment"),
            lines=pending_segment_lines,
            start_index=pending_segment_start_index,
            end_index=len(body_lines),
            counters=counters,
        )


def strip_leading_toc_block(body_lines: list[str]) -> list[str]:
    """
    Drop a leading table-of-contents block accidentally carried into body_lines.

    Typical false-positive pattern:
    - several consecutive `part/chapter/section` headings
    - no intervening article text or normal content
    - real正文 starts later from the first non-TOC line
    """
    structural_only_levels = {"part", "chapter", "section"}
    index = 0
    heading_count = 0

    while index < len(body_lines):
        line = body_lines[index].strip()
        if not line:
            index += 1
            continue
        level = match_heading_level(line)
        if level in structural_only_levels:
            heading_count += 1
            index += 1
            continue
        break

    if heading_count >= 2 and index < len(body_lines):
        return body_lines[index:]
    return body_lines


def finalize_appendices(
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    source_document: SourceDocumentRecord,
    document_node: NodeRecord,
    counters: dict[str, int],
) -> None:
    for appendix_no, appendix_label, appendix_lines in split_appendix_blocks(source_document.appendix_lines):
        appendix_node = NodeRecord(
            id=node_id_from_locator(NodeLocator(kind="appendix", appendix_no=appendix_no), source_document.source_id)
            or f"appendix:{slugify(source_document.source_id)}:{appendix_no:02d}",
            type=LEVEL_TO_NODE_TYPE["appendix"],
            name=appendix_label,
            level="appendix",
            metadata={},
        )
        nodes.append(appendix_node)
        edges.append(build_edge(document_node.id, appendix_node.id, structural_edge_type("document", "appendix")))

        segments = split_text_blocks(appendix_lines)
        for segment_index, block in enumerate(segments, start=1):
            create_segment_node(
                nodes=nodes,
                edges=edges,
                source_id=source_document.source_id,
                parent=appendix_node,
                text=block,
                counters=counters,
                split_items=False,
            )


def match_heading_level(line: str) -> str | None:
    if PART_RE.match(line):
        return "part"
    if CHAPTER_RE.match(line):
        return "chapter"
    if SECTION_RE.match(line):
        return "section"
    if ARTICLE_RE.match(line):
        return "article"
    return None


def build_toc_node(
    level: str,
    line: str,
    source_id: str,
    counters: dict[str, int],
    body_index: int,
) -> NodeRecord:
    counters[level] += 1
    return NodeRecord(
        id=f"{level}:{slugify(source_id)}:{counters[level]:02d}",
        type=LEVEL_TO_NODE_TYPE[level],
        name=line,
        level=level,
        metadata={},
    )


def build_article_node(source_id: str, line: str) -> tuple[NodeRecord, str]:
    match = ARTICLE_RE.match(line.strip())
    if not match:
        raise ValueError(f"Unable to parse article line: {line}")
    article_label = match.group(1)
    inline_text = match.group(2).strip()
    article_no, article_suffix = parse_article_components(article_label)
    node = NodeRecord(
        id=node_id_from_locator(
            NodeLocator(kind="provision", article_no=article_no, article_suffix=article_suffix),
            source_id,
        )
        or f"article:{slugify(source_id)}:{format_article_key(article_no, article_suffix)}",
        type=LEVEL_TO_NODE_TYPE["article"],
        name=article_label,
        level="article",
        metadata={},
    )
    return node, inline_text


def finalize_article(
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    article_node: NodeRecord,
    raw_paragraphs: list[str],
) -> None:
    paragraph_texts = collapse_item_only_paragraphs(raw_paragraphs)
    if not paragraph_texts:
        return

    locator = node_locator_from_node_id(article_node.id)
    if locator is None or locator.article_no is None:
        raise ValueError(f"Unable to resolve article locator from node id: {article_node.id}")

    article_no = locator.article_no
    article_suffix = locator.article_suffix
    source_id = source_id_from_node_id(article_node.id)

    if len(paragraph_texts) == 1:
        lead, items = split_item_segments(paragraph_texts[0])
        if items:
            paragraph_node = build_paragraph_node(
                article_node=article_node,
                article_no=article_no,
                article_suffix=article_suffix,
                paragraph_no=1,
            )
            nodes.append(paragraph_node)
            edges.append(build_edge(article_node.id, paragraph_node.id, structural_edge_type("article", "paragraph")))
            attach_item_hierarchy(
                nodes=nodes,
                edges=edges,
                source_id=source_id,
                parent_node=paragraph_node,
                parent_level="paragraph",
                article_no=article_no,
                article_suffix=article_suffix,
                paragraph_no=1,
                segment_no=None,
                parent_text=lead,
                item_segments=items,
            )
            return
        article_node.text = paragraph_texts[0].strip()
        return

    for paragraph_index, paragraph_source_text in enumerate(paragraph_texts, start=1):
        paragraph_node = build_paragraph_node(
            article_node=article_node,
            article_no=article_no,
            article_suffix=article_suffix,
            paragraph_no=paragraph_index,
        )
        nodes.append(paragraph_node)
        edges.append(build_edge(article_node.id, paragraph_node.id, structural_edge_type("article", "paragraph")))

        lead, items = split_item_segments(paragraph_source_text)
        if not items:
            paragraph_node.text = paragraph_source_text.strip()
            continue
        attach_item_hierarchy(
            nodes=nodes,
            edges=edges,
            source_id=source_id,
            parent_node=paragraph_node,
            parent_level="paragraph",
            article_no=article_no,
            article_suffix=article_suffix,
            paragraph_no=paragraph_index,
            segment_no=None,
            parent_text=lead,
            item_segments=items,
        )


def build_paragraph_node(
    *,
    article_node: NodeRecord,
    article_no: int,
    article_suffix: int | None,
    paragraph_no: int,
) -> NodeRecord:
    source_id = source_id_from_node_id(article_node.id)
    return NodeRecord(
        id=node_id_from_locator(
            NodeLocator(
                kind="provision",
                article_no=article_no,
                article_suffix=article_suffix,
                paragraph_no=paragraph_no,
            ),
            source_id,
        )
        or f"paragraph:{slugify(source_id)}:{format_article_key(article_no, article_suffix)}:{paragraph_no:02d}",
        type=LEVEL_TO_NODE_TYPE["paragraph"],
        name=f"{article_node.name}第{int_to_cn(paragraph_no)}款",
        level="paragraph",
        metadata={},
    )


def finalize_segment(
    *,
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    source_id: str,
    parent: NodeRecord,
    lines: list[str],
    start_index: int,
    end_index: int,
    counters: dict[str, int],
) -> None:
    text = "\n".join(line.strip() for line in lines if line.strip()).strip()
    if not text:
        return
    create_segment_node(
        nodes=nodes,
        edges=edges,
        source_id=source_id,
        parent=parent,
        text=text,
        counters=counters,
        split_items=True,
    )


def create_segment_node(
    *,
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    source_id: str,
    parent: NodeRecord,
    text: str,
    counters: dict[str, int],
    split_items: bool,
) -> NodeRecord:
    counters["segment"] += 1
    segment_no = counters["segment"]
    segment_node = NodeRecord(
        id=node_id_from_locator(NodeLocator(kind="provision", segment_no=segment_no), source_id)
        or f"segment:{slugify(source_id)}:{segment_no:04d}",
        type=LEVEL_TO_NODE_TYPE["segment"],
        name=f"段{int_to_cn(segment_no)}",
        level="segment",
        metadata={},
    )
    nodes.append(segment_node)
    edges.append(build_edge(parent.id, segment_node.id, structural_edge_type(parent.level, "segment")))

    if not split_items:
        segment_node.text = text.strip()
        return segment_node
    heading_text, body_text = extract_segment_heading(text)
    if heading_text:
        segment_node.text = heading_text
        item_lead, item_segments = split_item_segments(body_text)
        if item_segments:
            attach_item_hierarchy(
                nodes=nodes,
                edges=edges,
                source_id=source_id,
                parent_node=segment_node,
                parent_level="segment",
                article_no=None,
                article_suffix=None,
                paragraph_no=None,
                segment_no=segment_no,
                parent_text=segment_node.text,
                item_segments=item_segments,
            )
            return segment_node
        numeric_items = split_numeric_item_segments(body_text)
        if numeric_items:
            attach_numeric_items_as_items(
                nodes=nodes,
                edges=edges,
                source_id=source_id,
                parent_node=segment_node,
                segment_no=segment_no,
                item_segments=numeric_items,
            )
            return segment_node
        segment_node.text = "\n".join(part for part in (heading_text, body_text.strip()) if part).strip()
        return segment_node
    lead, items = split_item_segments(text)
    if not items:
        segment_node.text = text.strip()
        return segment_node

    attach_item_hierarchy(
        nodes=nodes,
        edges=edges,
        source_id=source_id,
        parent_node=segment_node,
        parent_level="segment",
        article_no=None,
        article_suffix=None,
        paragraph_no=None,
        segment_no=segment_no,
        parent_text=lead,
        item_segments=items,
    )
    return segment_node


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
    parent_text: str,
    item_segments: list[str],
) -> None:
    parent_node.text = parent_text.strip()

    for item_index, item_source_text in enumerate(item_segments, start=1):
        item_marker_text, item_body_text = extract_item_marker(item_source_text)
        item_lead, sub_item_segments = split_sub_item_segments(item_body_text)
        item_locator = NodeLocator(
            kind="provision",
            article_no=article_no,
            article_suffix=article_suffix,
            paragraph_no=paragraph_no,
            segment_no=segment_no,
            item_no=item_index,
        )
        item_node = NodeRecord(
            id=node_id_from_locator(item_locator, source_id)
            or build_fallback_item_id(source_id, article_no, article_suffix, paragraph_no, segment_no, item_index),
            type=LEVEL_TO_NODE_TYPE["item"],
            name=build_item_name(parent_node.name, item_index),
            level="item",
            text=item_lead if sub_item_segments else item_body_text.strip(),
            metadata={},
        )
        nodes.append(item_node)
        edges.append(build_edge(parent_node.id, item_node.id, structural_edge_type(parent_node.level, "item")))

        for sub_item_index, sub_item_source_text in enumerate(sub_item_segments, start=1):
            sub_item_marker_text, sub_item_body_text = extract_sub_item_marker(sub_item_source_text)
            sub_item_locator = NodeLocator(
                kind="provision",
                article_no=article_no,
                article_suffix=article_suffix,
                paragraph_no=paragraph_no,
                segment_no=segment_no,
                item_no=item_index,
                sub_item_no=sub_item_index,
            )
            sub_item_node = NodeRecord(
                id=node_id_from_locator(sub_item_locator, source_id)
                or build_fallback_sub_item_id(
                    source_id,
                    article_no,
                    article_suffix,
                    paragraph_no,
                    segment_no,
                    item_index,
                    sub_item_index,
                ),
                type=LEVEL_TO_NODE_TYPE["sub_item"],
                name=build_sub_item_name(item_node.name, sub_item_index),
                level="sub_item",
                text=sub_item_body_text.strip(),
                metadata={},
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
        if current:
            current.append(line)
        elif lead_lines:
            lead_lines.append(line)
        else:
            lead_lines = [line]
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
        if current:
            current.append(line)
        elif lead_lines:
            lead_lines.append(line)
        else:
            lead_lines = [line]
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


def extract_segment_heading(text: str) -> tuple[str, str]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines or not SEGMENT_HEADING_RE.match(lines[0]):
        return "", text.strip()
    match = ITEM_MARKER_RE.match(lines[0])
    heading = lines[0][match.end() :].strip() if match else lines[0]
    body = "\n".join(lines[1:]).strip()
    return heading, body


def split_numeric_item_segments(text: str) -> list[str]:
    lines = [line.rstrip() for line in text.splitlines()]
    first_marker_index = next((index for index, line in enumerate(lines) if SUB_ITEM_MARKER_RE.match(line.strip())), None)
    if first_marker_index is None:
        return []

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
        if current:
            current.append(line)
    if current:
        segments.append("\n".join(current).strip())
    return segments


def attach_numeric_items_as_items(
    *,
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    source_id: str,
    parent_node: NodeRecord,
    segment_no: int,
    item_segments: list[str],
) -> None:
    for item_index, item_source_text in enumerate(item_segments, start=1):
        _, item_body_text = extract_sub_item_marker(item_source_text)
        item_locator = NodeLocator(
            kind="provision",
            segment_no=segment_no,
            item_no=item_index,
        )
        item_node = NodeRecord(
            id=node_id_from_locator(item_locator, source_id)
            or build_fallback_item_id(source_id, None, None, None, segment_no, item_index),
            type=LEVEL_TO_NODE_TYPE["item"],
            name=build_item_name(parent_node.name, item_index),
            level="item",
            text=item_body_text.strip(),
            metadata={},
        )
        nodes.append(item_node)
        edges.append(build_edge(parent_node.id, item_node.id, structural_edge_type(parent_node.level, "item")))


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
            current_label = line
            current_lines = []
            continue
        if current_no is not None:
            current_lines.append(line)

    if current_no is not None:
        blocks.append((current_no, current_label, current_lines))
    return blocks


def split_text_blocks(lines: list[str]) -> list[str]:
    blocks: list[list[str]] = []
    current: list[str] = []
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            if current:
                blocks.append(current)
                current = []
            continue
        current.append(line)
    if current:
        blocks.append(current)
    return ["\n".join(block).strip() for block in blocks if any(item.strip() for item in block)]


def first_non_empty(lines: list[str]) -> str:
    return next((line.strip() for line in lines if line.strip()), "")


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
    return EdgeRecord(
        id=f"edge:{slugify(edge_type)}:{slugify(source_id)}:{slugify(target_id)}",
        source=source_id,
        target=target_id,
        type=edge_type,
    )


def source_id_from_node_id(node_id: str) -> str:
    parts = node_id.split(":")
    if len(parts) < 2:
        raise ValueError(f"Unsupported node id: {node_id}")
    if parts[0] in {"document", "part", "chapter", "section", "segment", "appendix"}:
        return parts[1]
    if parts[0] in {"article", "paragraph"}:
        return parts[1]
    if parts[0] in {"item", "sub_item"}:
        return parts[1]
    raise ValueError(f"Unsupported node id: {node_id}")


def build_item_name(parent_name: str, item_no: int) -> str:
    return f"{parent_name}第{int_to_cn(item_no)}项"


def build_sub_item_name(item_name: str, sub_item_no: int) -> str:
    return f"{item_name}第{int_to_cn(sub_item_no)}目"


def build_fallback_item_id(
    source_id: str,
    article_no: int | None,
    article_suffix: int | None,
    paragraph_no: int | None,
    segment_no: int | None,
    item_no: int,
) -> str:
    if segment_no is not None:
        return f"item:{slugify(source_id)}:segment:{segment_no:04d}:{item_no:02d}"
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
) -> str:
    if segment_no is not None:
        return (
            f"sub_item:{slugify(source_id)}:segment:{segment_no:04d}:"
            f"{item_no:02d}:{sub_item_no:02d}"
        )
    if article_no is None:
        raise ValueError("Article-based sub-item fallback id requires article_no.")
    article_key = format_article_key(article_no, article_suffix)
    base = f"sub_item:{slugify(source_id)}:{article_key}"
    if paragraph_no is not None:
        base = f"{base}:{paragraph_no:02d}"
    return f"{base}:{item_no:02d}:{sub_item_no:02d}"
