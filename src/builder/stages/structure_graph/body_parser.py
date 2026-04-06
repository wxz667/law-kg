from __future__ import annotations

from ...contracts import DocumentUnitRecord, EdgeRecord, NodeRecord
from ...utils.ids import slugify
from ...utils.document_layout import looks_like_heading_continuation
from ...utils.locator import NodeLocator, node_id_from_locator, node_locator_from_node_id
from ...utils.numbers import format_article_key, int_to_cn, parse_article_components
from .helpers import (
    build_edge,
    clear_lower_levels,
    find_parent,
    match_heading_level,
    source_id_from_node_id,
    structural_edge_type,
)
from .items import (
    attach_item_hierarchy,
    collapse_item_only_paragraphs,
    emit_list_items_if_possible,
    extract_item_marker,
    split_item_segments,
)
from .patterns import (
    ARTICLE_RE,
    ITEM_MARKER_RE,
    LEVEL_TO_NODE_TYPE,
    NUMERIC_ITEM_HEADING_RE,
    NUMBERED_LIST_RE,
    PARAGRAPH_HEADING_RE,
    SEGMENT_HEADING_RE,
)


def finalize_document_body(
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    unit: DocumentUnitRecord,
    document_node: NodeRecord,
    counters: dict[str, int],
) -> None:
    body_lines = [line.strip() for line in unit.body_lines if line.strip()]
    if not body_lines:
        return
    if any(match_heading_level(line) for line in body_lines):
        emit_structured_document_body(nodes, edges, unit, document_node, counters, body_lines)
        return
    if any(SEGMENT_HEADING_RE.match(line) for line in body_lines):
        if emit_hierarchical_title_outline_body(nodes, edges, unit, document_node, counters, body_lines):
            return
        emit_candidate_outline_body(nodes, edges, unit, document_node, counters, body_lines)
        return
    emit_unstructured_document_body(nodes, edges, unit, document_node, counters, body_lines)


def emit_structured_document_body(
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    unit: DocumentUnitRecord,
    document_node: NodeRecord,
    counters: dict[str, int],
    body_lines: list[str],
) -> None:
    level_stack: dict[str, NodeRecord] = {"document": document_node}
    current_article: NodeRecord | None = None
    current_article_paragraphs: list[str] = []
    pending_block_lines: list[str] = []

    def flush_current_article() -> None:
        nonlocal current_article, current_article_paragraphs
        if current_article is None:
            return
        finalize_article(nodes, edges, current_article, current_article_paragraphs)
        current_article = None
        current_article_paragraphs = []

    def flush_pending_block() -> None:
        nonlocal pending_block_lines
        if not pending_block_lines:
            return
        parent = find_parent(level_stack, "segment")
        finalize_non_article_block(
            nodes=nodes,
            edges=edges,
            source_id=unit.source_id,
            parent=parent,
            counters=counters,
            lines=pending_block_lines,
        )
        pending_block_lines = []

    for body_index, raw_line in enumerate(body_lines, start=1):
        line = raw_line.strip()
        if not line:
            continue
        level = match_heading_level(line)
        if level is not None:
            flush_current_article()
            flush_pending_block()
            if level == "article":
                article_node, inline_text = build_article_node(unit.source_id, line, body_index)
                parent = find_parent(level_stack, level)
                nodes.append(article_node)
                edges.append(build_edge(parent.id, article_node.id, structural_edge_type(parent.level, level)))
                level_stack[level] = article_node
                clear_lower_levels(level_stack, level)
                current_article = article_node
                current_article_paragraphs = [inline_text] if inline_text else []
                continue
            toc_node = build_toc_node(level, line, unit.source_id, counters, body_index)
            parent = find_parent(level_stack, level)
            nodes.append(toc_node)
            edges.append(build_edge(parent.id, toc_node.id, structural_edge_type(parent.level, level)))
            level_stack[level] = toc_node
            clear_lower_levels(level_stack, level)
            continue

        if current_article is not None:
            current_article_paragraphs.append(line)
            continue
        pending_block_lines.append(line)

    flush_current_article()
    flush_pending_block()


def emit_candidate_outline_body(
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    unit: DocumentUnitRecord,
    document_node: NodeRecord,
    counters: dict[str, int],
    body_lines: list[str],
) -> None:
    blocks = split_candidate_outline_blocks(body_lines)
    if not blocks:
        emit_unstructured_document_body(nodes, edges, unit, document_node, counters, body_lines)
        return
    for article_index, block in enumerate(blocks, start=1):
        emit_candidate_outline_block(
            nodes=nodes,
            edges=edges,
            source_id=unit.source_id,
            parent=document_node,
            counters=counters,
            block_lines=block,
            article_index=article_index,
        )


def emit_hierarchical_title_outline_body(
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    unit: DocumentUnitRecord,
    document_node: NodeRecord,
    counters: dict[str, int],
    body_lines: list[str],
) -> bool:
    blocks = split_candidate_outline_blocks(body_lines)
    if not blocks:
        return False
    if not any(any(PARAGRAPH_HEADING_RE.match(line.strip()) for line in block[1:]) for block in blocks):
        return False
    if not any(any(NUMBERED_LIST_RE.match(line.strip()) for line in block[1:]) for block in blocks):
        return False

    temp_nodes: list[NodeRecord] = []
    temp_edges: list[EdgeRecord] = []
    temp_counters = dict(counters)
    emitted_any = False
    for chapter_order, block in enumerate(blocks, start=1):
        chapter_heading_line = block[0].strip()
        chapter_title = strip_top_level_marker(chapter_heading_line)
        remainder_lines = [line.strip() for line in block[1:] if line.strip()]
        if not is_title_like_candidate_heading(chapter_title):
            return False

        chapter_node = create_candidate_chapter_node(
            source_id=unit.source_id,
            counters=temp_counters,
            name=chapter_heading_line,
            order=chapter_order,
        )
        temp_nodes.append(chapter_node)
        temp_edges.append(build_edge(document_node.id, chapter_node.id, structural_edge_type(document_node.level, "chapter")))

        paragraph_blocks = split_candidate_paragraph_blocks(remainder_lines)
        if paragraph_blocks:
            if not emit_candidate_title_sections(
                nodes=temp_nodes,
                edges=temp_edges,
                source_id=unit.source_id,
                chapter_node=chapter_node,
                counters=temp_counters,
                paragraph_blocks=paragraph_blocks,
            ):
                return False
            emitted_any = True
            continue

        if not emit_numbered_articles_under_parent(
            nodes=temp_nodes,
            edges=temp_edges,
            source_id=unit.source_id,
            parent=chapter_node,
            counters=temp_counters,
            lines=remainder_lines,
        ):
            return False
        emitted_any = True
    if not emitted_any:
        return False
    nodes.extend(temp_nodes)
    edges.extend(temp_edges)
    counters.update(temp_counters)
    return emitted_any


def emit_candidate_title_sections(
    *,
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    source_id: str,
    chapter_node: NodeRecord,
    counters: dict[str, int],
    paragraph_blocks: list[list[str]],
) -> bool:
    emitted_any = False
    starting_node_count = len(nodes)
    starting_edge_count = len(edges)
    starting_section_count = int(counters.get("section", 0))
    starting_candidate_article_count = int(counters.get("candidate_article", 0))
    for section_order, block in enumerate(paragraph_blocks, start=1):
        section_heading_line = block[0].strip()
        section_title = strip_candidate_paragraph_marker(section_heading_line)
        section_body_lines = [line.strip() for line in block[1:] if line.strip()]
        if not section_title or not is_title_like_candidate_heading(section_title):
            del nodes[starting_node_count:]
            del edges[starting_edge_count:]
            counters["section"] = starting_section_count
            counters["candidate_article"] = starting_candidate_article_count
            return False

        section_node = create_candidate_section_node(
            source_id=source_id,
            parent=chapter_node,
            counters=counters,
            name=section_heading_line,
            order=section_order,
        )
        nodes.append(section_node)
        edges.append(build_edge(chapter_node.id, section_node.id, structural_edge_type(chapter_node.level, "section")))

        if emit_numbered_articles_under_parent(
            nodes=nodes,
            edges=edges,
            source_id=source_id,
            parent=section_node,
            counters=counters,
            lines=section_body_lines,
        ):
            emitted_any = True
            continue

        del nodes[starting_node_count:]
        del edges[starting_edge_count:]
        counters["section"] = starting_section_count
        counters["candidate_article"] = starting_candidate_article_count
        return False
    return emitted_any


def emit_numbered_articles_under_parent(
    *,
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    source_id: str,
    parent: NodeRecord,
    counters: dict[str, int],
    lines: list[str],
) -> bool:
    article_blocks = parse_numbered_article_blocks(lines)
    if not article_blocks:
        return False
    for block_order, article_text in article_blocks:
        article_index = next_candidate_article_index(counters)
        article_node = create_candidate_article_node(
            source_id=source_id,
            article_index=article_index,
            heading_line=str(block_order),
        )
        nodes.append(article_node)
        edges.append(build_edge(parent.id, article_node.id, structural_edge_type(parent.level, "article")))
        finalize_article(nodes, edges, article_node, [article_text])
    return True


def parse_numbered_article_blocks(lines: list[str]) -> list[tuple[int, str]]:
    blocks: list[tuple[int, str]] = []
    current_order = 0
    current_lines: list[str] = []
    saw_numbered = False
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        match = NUMBERED_LIST_RE.match(line)
        if match:
            body = match.group("body").strip()
            if current_lines:
                blocks.append((current_order, "\n".join(current_lines).strip()))
            current_order = int(match.group("index").translate(str.maketrans("０１２３４５６７８９", "0123456789")))
            current_lines = [body] if body else []
            saw_numbered = True
            continue
        if current_lines:
            current_lines.append(line)
            continue
        if saw_numbered:
            return []
        return []
    if current_lines:
        blocks.append((current_order, "\n".join(current_lines).strip()))
    if not saw_numbered:
        return []
    if any(not text or not looks_like_complete_clause_text(text) for _, text in blocks):
        return []
    return blocks


def emit_unstructured_document_body(
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    unit: DocumentUnitRecord,
    document_node: NodeRecord,
    counters: dict[str, int],
    body_lines: list[str],
) -> None:
    if emit_list_items_if_possible(
        nodes=nodes,
        edges=edges,
        source_id=unit.source_id,
        parent=document_node,
        counters=counters,
        lines=body_lines,
    ):
        return
    append_document_segment(
        nodes=nodes,
        edges=edges,
        source_id=unit.source_id,
        parent=document_node,
        text="\n".join(body_lines).strip(),
        counters=counters,
        name="正文",
    )


def append_document_segment(
    *,
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    source_id: str,
    parent: NodeRecord,
    text: str,
    counters: dict[str, int],
    name: str,
) -> NodeRecord | None:
    cleaned_text = text.strip()
    if not cleaned_text:
        return None
    counters["segment"] += 1
    segment_no = counters["segment"]
    node = NodeRecord(
        id=node_id_from_locator(NodeLocator(kind="provision", segment_no=segment_no), source_id)
        or f"segment:{slugify(source_id)}:{segment_no:04d}",
        type=LEVEL_TO_NODE_TYPE["segment"],
        name=name,
        level="segment",
        text=cleaned_text,
        metadata={"order": segment_no},
    )
    nodes.append(node)
    edges.append(build_edge(parent.id, node.id, structural_edge_type(parent.level, "segment")))
    return node


def finalize_non_article_block(
    *,
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    source_id: str,
    parent: NodeRecord,
    counters: dict[str, int],
    lines: list[str],
) -> None:
    if emit_section_outline_block_if_possible(
        nodes=nodes,
        edges=edges,
        source_id=source_id,
        parent=parent,
        counters=counters,
        lines=lines,
    ):
        return
    if emit_list_items_if_possible(
        nodes=nodes,
        edges=edges,
        source_id=source_id,
        parent=parent,
        counters=counters,
        lines=lines,
    ):
        return
    append_document_segment(
        nodes=nodes,
        edges=edges,
        source_id=source_id,
        parent=parent,
        text="\n".join(line.strip() for line in lines if line.strip()),
        counters=counters,
        name="正文",
    )


def emit_section_outline_block_if_possible(
    *,
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    source_id: str,
    parent: NodeRecord,
    counters: dict[str, int],
    lines: list[str],
) -> bool:
    normalized_lines = [line.strip() for line in lines if line.strip()]
    if parent.level not in {"part", "chapter"} or len(normalized_lines) < 2:
        return False
    heading = normalized_lines[0]
    remainder = normalized_lines[1:]
    if match_heading_level(heading) or SEGMENT_HEADING_RE.match(heading):
        return False
    if not looks_like_heading_continuation(heading):
        return False
    if not (emit_list_probe(remainder) or any(SEGMENT_HEADING_RE.match(line) for line in remainder)):
        return False

    counters["section"] += 1
    section_node = NodeRecord(
        id=f"section:{slugify(source_id)}:{counters['section']:02d}",
        type=LEVEL_TO_NODE_TYPE["section"],
        name=heading,
        level="section",
        metadata={"order": counters["section"]},
    )
    nodes.append(section_node)
    edges.append(build_edge(parent.id, section_node.id, structural_edge_type(parent.level, "section")))

    if any(SEGMENT_HEADING_RE.match(line) for line in remainder):
        emit_candidate_outline_body(
            nodes=nodes,
            edges=edges,
            unit=DocumentUnitRecord(source_id=source_id, title="", source_type="", body_lines=remainder, appendix_lines=[], metadata={}),
            document_node=section_node,
            counters=counters,
            body_lines=remainder,
        )
        return True
    return emit_list_items_if_possible(
        nodes=nodes,
        edges=edges,
        source_id=source_id,
        parent=section_node,
        counters=counters,
        lines=remainder,
    )


def split_candidate_outline_blocks(lines: list[str]) -> list[list[str]]:
    blocks: list[list[str]] = []
    current: list[str] = []
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        if SEGMENT_HEADING_RE.match(line):
            if current:
                blocks.append(current)
            current = [line]
            continue
        if current:
            current.append(line)
    if current:
        blocks.append(current)
    return blocks


def emit_candidate_outline_block(
    *,
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    source_id: str,
    parent: NodeRecord,
    counters: dict[str, int],
    block_lines: list[str],
    article_index: int,
) -> None:
    heading_line = block_lines[0].strip()
    heading_text = strip_top_level_marker(heading_line)
    remainder_lines = [line.strip() for line in block_lines[1:] if line.strip()]

    if contains_candidate_paragraphs(remainder_lines) and is_title_like_candidate_heading(heading_text):
        # Title-shaped `一、... / （一）...` blocks should never fall back to candidate articles.
        append_document_segment(
            nodes=nodes,
            edges=edges,
            source_id=source_id,
            parent=parent,
            text="\n".join(block_lines).strip(),
            counters=counters,
            name="正文",
        )
        return

    if should_treat_candidate_block_as_section(heading_text, remainder_lines):
        section_node = create_candidate_section_node(
            source_id=source_id,
            parent=parent,
            counters=counters,
            name=heading_text,
            order=article_index,
        )
        nodes.append(section_node)
        edges.append(build_edge(parent.id, section_node.id, structural_edge_type(parent.level, "section")))
        if not emit_list_items_if_possible(
            nodes=nodes,
            edges=edges,
            source_id=source_id,
            parent=section_node,
            counters=counters,
            lines=remainder_lines,
        ):
            append_document_segment(
                nodes=nodes,
                edges=edges,
                source_id=source_id,
                parent=section_node,
                text="\n".join(remainder_lines).strip(),
                counters=counters,
                name="正文",
            )
        return

    article_node = create_candidate_article_node(source_id=source_id, article_index=article_index, heading_line=heading_line)
    nodes.append(article_node)
    edges.append(build_edge(parent.id, article_node.id, structural_edge_type(parent.level, "article")))

    if contains_candidate_paragraphs(remainder_lines):
        article_node.text = heading_text
        emit_candidate_paragraphs(
            nodes=nodes,
            edges=edges,
            source_id=source_id,
            article_node=article_node,
            article_index=article_index,
            lines=remainder_lines,
        )
        return

    if remainder_lines and any(NUMERIC_ITEM_HEADING_RE.match(line) for line in remainder_lines):
        attach_item_hierarchy(
            nodes=nodes,
            edges=edges,
            source_id=source_id,
            parent_node=article_node,
            parent_level="article",
            parent_node_id=article_node.id,
            article_no=article_index,
            article_suffix=None,
            paragraph_no=None,
            segment_no=None,
            parent_text=heading_text,
            item_segments=remainder_lines,
        )
        return

    raw_paragraphs = [heading_text] if heading_text else []
    raw_paragraphs.extend(remainder_lines)
    if len(raw_paragraphs) > 1:
        finalize_article(nodes, edges, article_node, raw_paragraphs)
        return

    article_text_parts = [heading_text] if heading_text else []
    article_text_parts.extend(remainder_lines)
    article_node.text = "\n".join(part for part in article_text_parts if part).strip()


def should_treat_candidate_block_as_section(heading_text: str, remainder_lines: list[str]) -> bool:
    if not heading_text or not remainder_lines:
        return False
    if contains_candidate_paragraphs(remainder_lines):
        return False
    if not emit_list_probe(remainder_lines):
        return False
    return looks_like_heading_continuation(heading_text)


def contains_candidate_paragraphs(lines: list[str]) -> bool:
    return any(is_section_heading_line(line.strip()) for line in lines)


def looks_like_complete_clause_text(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    if stripped.endswith(("。", "；")):
        return True
    return any(token in stripped for token in ("应当", "可以", "不得", "是指", "包括", "参照", "按照"))


def is_title_like_candidate_heading(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    if stripped.endswith(("。", "；", "：", "！", "？")):
        return False
    return looks_like_heading_continuation(stripped)


def emit_candidate_paragraphs(
    *,
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    source_id: str,
    article_node: NodeRecord,
    article_index: int,
    lines: list[str],
) -> None:
    blocks = split_candidate_paragraph_blocks(lines)
    if not blocks:
        article_node.text = "\n".join(line.strip() for line in lines if line.strip()).strip()
        return

    for paragraph_index, block in enumerate(blocks, start=1):
        heading_line = block[0].strip()
        _, paragraph_text = extract_item_marker(heading_line)
        paragraph_body_lines = [line.strip() for line in block[1:] if line.strip()]
        paragraph_node = NodeRecord(
            id=node_id_from_locator(
                NodeLocator(kind="provision", article_no=article_index, paragraph_no=paragraph_index),
                source_id,
            )
            or f"paragraph:{slugify(source_id)}:{article_index:04d}:{paragraph_index:02d}",
            type=LEVEL_TO_NODE_TYPE["paragraph"],
            name=f"{article_node.name}第{int_to_cn(paragraph_index)}款",
            level="paragraph",
            metadata={"order": paragraph_index},
        )
        nodes.append(paragraph_node)
        edges.append(build_edge(article_node.id, paragraph_node.id, structural_edge_type("article", "paragraph")))

        if emit_list_items_if_possible(
            nodes=nodes,
            edges=edges,
            source_id=source_id,
            parent=paragraph_node,
            counters={"segment": 0},
            lines=paragraph_body_lines,
        ):
            paragraph_node.text = paragraph_text
            continue

        if paragraph_body_lines and any(NUMERIC_ITEM_HEADING_RE.match(line) for line in paragraph_body_lines):
            attach_item_hierarchy(
                nodes=nodes,
                edges=edges,
                source_id=source_id,
                parent_node=paragraph_node,
                parent_level="paragraph",
                parent_node_id=paragraph_node.id,
                article_no=article_index,
                article_suffix=None,
                paragraph_no=paragraph_index,
                segment_no=None,
                parent_text=paragraph_text,
                item_segments=paragraph_body_lines,
            )
            continue

        paragraph_text_parts = [paragraph_text] if paragraph_text else []
        paragraph_text_parts.extend(paragraph_body_lines)
        paragraph_node.text = "\n".join(part for part in paragraph_text_parts if part).strip()


def split_candidate_paragraph_blocks(lines: list[str]) -> list[list[str]]:
    blocks: list[list[str]] = []
    current: list[str] = []
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        if is_section_heading_line(line):
            if current:
                blocks.append(current)
            current = [line]
            continue
        if current:
            current.append(line)
    if current:
        blocks.append(current)
    return blocks


def is_section_heading_line(text: str) -> bool:
    stripped = text.strip()
    if not PARAGRAPH_HEADING_RE.match(stripped):
        return False
    return is_title_like_candidate_heading(strip_candidate_paragraph_marker(stripped))


def strip_top_level_marker(text: str) -> str:
    if not SEGMENT_HEADING_RE.match(text.strip()):
        return text.strip()
    marker_match = ITEM_MARKER_RE.match(text.strip())
    if not marker_match:
        return text.strip()
    return text.strip()[marker_match.end() :].strip()


def strip_candidate_paragraph_marker(text: str) -> str:
    stripped = text.strip()
    if not PARAGRAPH_HEADING_RE.match(stripped):
        return stripped
    closing_index = max(stripped.find("）"), stripped.find(")"))
    if closing_index < 0:
        return stripped
    return stripped[closing_index + 1 :].strip()


def create_candidate_chapter_node(*, source_id: str, counters: dict[str, int], name: str, order: int) -> NodeRecord:
    counters["chapter"] += 1
    return NodeRecord(
        id=f"chapter:{slugify(source_id)}:{counters['chapter']:02d}",
        type=LEVEL_TO_NODE_TYPE["chapter"],
        name=name,
        level="chapter",
        metadata={"order": order},
    )


def create_candidate_section_node(*, source_id: str, parent: NodeRecord, counters: dict[str, int], name: str, order: int) -> NodeRecord:
    del parent
    counters["section"] += 1
    return NodeRecord(
        id=f"section:{slugify(source_id)}:{counters['section']:02d}",
        type=LEVEL_TO_NODE_TYPE["section"],
        name=name,
        level="section",
        metadata={"order": order},
    )


def create_candidate_article_node(*, source_id: str, article_index: int, heading_line: str) -> NodeRecord:
    del heading_line
    return NodeRecord(
        id=node_id_from_locator(NodeLocator(kind="provision", article_no=article_index), source_id)
        or f"article:{slugify(source_id)}:{article_index:04d}",
        type=LEVEL_TO_NODE_TYPE["article"],
        name=f"第{int_to_cn(article_index)}条",
        level="article",
        metadata={"order": article_index},
    )


def next_candidate_article_index(counters: dict[str, int]) -> int:
    counters["candidate_article"] = int(counters.get("candidate_article", 0)) + 1
    return counters["candidate_article"]


def build_toc_node(level: str, line: str, source_id: str, counters: dict[str, int], body_index: int) -> NodeRecord:
    counters[level] += 1
    return NodeRecord(
        id=f"{level}:{slugify(source_id)}:{counters[level]:02d}",
        type=LEVEL_TO_NODE_TYPE[level],
        name=line,
        level=level,
        metadata={"order": body_index},
    )


def build_article_node(source_id: str, line: str, body_index: int) -> tuple[NodeRecord, str]:
    match = ARTICLE_RE.match(line.strip())
    if not match:
        raise ValueError(f"Unable to parse article line: {line}")
    article_label = match.group(1)
    inline_text = (match.group(2) or "").strip()
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
        metadata={"order": body_index, "article_no": article_no, "article_suffix": article_suffix},
    )
    return node, inline_text


def finalize_article(nodes: list[NodeRecord], edges: list[EdgeRecord], article_node: NodeRecord, raw_paragraphs: list[str]) -> None:
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
                parent_node_id=paragraph_node.id,
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

    paragraph_index = 1
    cursor = 0
    while cursor < len(paragraph_texts):
        paragraph_source_text = paragraph_texts[cursor]
        paragraph_node = build_paragraph_node(
            article_node=article_node,
            article_no=article_no,
            article_suffix=article_suffix,
            paragraph_no=paragraph_index,
        )
        nodes.append(paragraph_node)
        edges.append(build_edge(article_node.id, paragraph_node.id, structural_edge_type("article", "paragraph")))

        semicolon_item_lines = collect_semicolon_item_lines(paragraph_texts[cursor + 1 :])
        if looks_like_item_anchor_text(paragraph_source_text) and len(semicolon_item_lines) >= 2:
            attach_item_hierarchy(
                nodes=nodes,
                edges=edges,
                source_id=source_id,
                parent_node=paragraph_node,
                parent_level="paragraph",
                parent_node_id=paragraph_node.id,
                article_no=article_no,
                article_suffix=article_suffix,
                paragraph_no=paragraph_index,
                segment_no=None,
                parent_text=paragraph_source_text.strip(),
                item_segments=semicolon_item_lines,
            )
            cursor += 1 + len(semicolon_item_lines)
            paragraph_index += 1
            continue

        lead, items = split_item_segments(paragraph_source_text)
        if not items:
            paragraph_node.text = paragraph_source_text.strip()
            cursor += 1
            paragraph_index += 1
            continue
        attach_item_hierarchy(
            nodes=nodes,
            edges=edges,
            source_id=source_id,
            parent_node=paragraph_node,
            parent_level="paragraph",
            parent_node_id=paragraph_node.id,
            article_no=article_no,
            article_suffix=article_suffix,
            paragraph_no=paragraph_index,
            segment_no=None,
            parent_text=lead,
            item_segments=items,
        )
        cursor += 1
        paragraph_index += 1


def build_paragraph_node(*, article_node: NodeRecord, article_no: int, article_suffix: int | None, paragraph_no: int) -> NodeRecord:
    source_id = source_id_from_node_id(article_node.id)
    return NodeRecord(
        id=node_id_from_locator(
            NodeLocator(kind="provision", article_no=article_no, article_suffix=article_suffix, paragraph_no=paragraph_no),
            source_id,
        )
        or f"paragraph:{slugify(source_id)}:{format_article_key(article_no, article_suffix)}:{paragraph_no:02d}",
        type=LEVEL_TO_NODE_TYPE["paragraph"],
        name=f"{article_node.name}第{int_to_cn(paragraph_no)}款",
        level="paragraph",
        metadata={"order": paragraph_no},
    )


def looks_like_item_anchor_text(text: str) -> bool:
    stripped = text.strip()
    return bool(stripped) and stripped.endswith("：")


def collect_semicolon_item_lines(lines: list[str]) -> list[str]:
    items: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if ARTICLE_RE.match(stripped) or match_heading_level(stripped) is not None:
            break
        if ITEM_MARKER_RE.match(stripped):
            items.append(stripped)
            continue
        if looks_like_semicolon_item_text(stripped):
            items.append(stripped)
            continue
        break
    return items


def looks_like_semicolon_item_text(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    if stripped.endswith("；"):
        return True
    return stripped.endswith("。") and len(stripped) <= 30


def emit_list_probe(lines: list[str]) -> bool:
    probe_node = NodeRecord(id="probe", type="ProvisionNode", name="probe", level="segment")
    return emit_list_items_if_possible(nodes=[], edges=[], source_id="probe", parent=probe_node, counters={}, lines=lines)
