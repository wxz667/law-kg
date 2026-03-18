from __future__ import annotations

import re

from ..utils.ids import slugify
from ..utils.numbers import (
    format_article_key,
    int_to_cn,
    parse_article_components,
    to_fullwidth_digit_text,
    chinese_number_to_int,
)
from ..contracts import EdgeRecord, GraphBundle, NodeRecord, SourceDocumentRecord

from ..config import load_schema

ARTICLE_LABEL_RE = re.compile(
    r"^(第[一二三四五六七八九十百千万零两〇0-9]+条(?:之[一二三四五六七八九十百千万零两〇0-9]+)?)(.*)$"
)

SEGMENT_REGEX_PATTERNS: dict[str, str] = {
    "part": r"^第[一二三四五六七八九十百零]+编\s+.+$",
    "chapter": r"^第[一二三四五六七八九十百零]+章\s+.+$",
    "section": r"^第[一二三四五六七八九十百零]+节\s+.+$",
    "article": r"^第[一二三四五六七八九十百千万零两〇0-9]+条(?:之[一二三四五六七八九十百千万零两〇0-9]+)?\s+.+$",
    "appendix_heading": r"^附件([一二三四五六七八九十百千万零两〇0-9]+)$",
    "appendix_item_marker": r"^([0-9０-９]+)[.．](.+)$",
    "item_marker": r"（[一二三四五六七八九十]+）",
    "sub_item_marker": r"(?:(?<=^)|(?<=\s)|(?<=；)|(?<=：)|(?<=:))([0-9０-９]+)[.．]",
}

STRUCTURAL_EDGES = {
    (item["parent_level"], item["child_level"]): item["edge_type"]
    for item in load_schema().get("structural_edges", [])
}
LEVEL_ORDER = load_schema().get("levels", [])
LEVEL_TO_NODE_TYPE = load_schema().get("level_to_node_type", {})


def run(source_document: SourceDocumentRecord) -> GraphBundle:
    regexes = {name: re.compile(pattern) for name, pattern in SEGMENT_REGEX_PATTERNS.items()}

    nodes: list[NodeRecord] = []
    edges: list[EdgeRecord] = []
    counters = {"part": 0, "chapter": 0, "section": 0}
    document_node = NodeRecord(
        id=f"document:{slugify(source_document.source_id)}",
        type=LEVEL_TO_NODE_TYPE["document"],
        name=source_document.title,
        level="document",
        source_id=source_document.source_id,
        metadata={
            "source_path": source_document.source_path,
            "source_type": source_document.source_type,
            "checksum": source_document.checksum,
            "preface_text": source_document.preface_text,
            "revision_events": source_document.metadata.get("revision_events", []),
        },
    )
    nodes.append(document_node)

    level_stack: dict[str, NodeRecord] = {"document": document_node}
    current_article: NodeRecord | None = None
    current_article_paragraphs: list[str] = []

    for paragraph_index, line in enumerate(source_document.body_lines, start=1):
        level = match_heading_level(line, regexes)
        if level:
            if current_article is not None:
                finalize_article(nodes, edges, current_article, current_article_paragraphs, regexes)
                current_article = None
                current_article_paragraphs = []

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

            counters[level] += 1
            toc_node = NodeRecord(
                id=f"{level}:{slugify(source_document.source_id)}:{counters[level]:02d}",
                type=LEVEL_TO_NODE_TYPE[level],
                name=line,
                level=level,
                source_id=source_document.source_id,
                address=build_catalog_address(level, counters),
                metadata={
                    "ordinal": counters[level],
                    "heading_line": line,
                    "body_index": paragraph_index,
                },
            )
            parent = find_parent(level_stack, level)
            nodes.append(toc_node)
            edges.append(build_edge(parent.id, toc_node.id, structural_edge_type(parent.level, level)))
            level_stack[level] = toc_node
            clear_lower_levels(level_stack, level)
            continue

        if current_article is not None:
            current_article_paragraphs.append(line)

    if current_article is not None:
        finalize_article(nodes, edges, current_article, current_article_paragraphs, regexes)

    for appendix_no, appendix_label, appendix_lines in split_appendix_blocks(
        source_document.appendix_lines,
        regexes["appendix_heading"],
    ):
        finalize_appendix(
            nodes,
            edges,
            document_node,
            source_document.source_id,
            appendix_no,
            appendix_label,
            appendix_lines,
            regexes["appendix_item_marker"],
        )

    add_appendix_references(nodes, edges)

    return GraphBundle(
        graph_id=f"graph:{slugify(source_document.source_id)}",
        nodes=nodes,
        edges=edges,
    )

def match_heading_level(line: str, regexes: dict[str, re.Pattern[str]]) -> str | None:
    for level in ("part", "chapter", "section", "article"):
        if regexes[level].match(line):
            return level
    return None


def build_article_node(source_id: str, line: str) -> tuple[NodeRecord, str]:
    match = ARTICLE_LABEL_RE.match(line.strip())
    if not match:
        raise ValueError(f"Unable to parse article line: {line}")
    article_label = match.group(1)
    inline_text = match.group(2).strip()
    article_no, article_suffix = parse_article_components(article_label)
    article_key = format_article_key(article_no, article_suffix)
    node = NodeRecord(
        id=f"article:{slugify(source_id)}:{article_key}",
        type=LEVEL_TO_NODE_TYPE["article"],
        name=article_label,
        level="article",
        source_id=source_id,
        address=build_address(article_no, article_suffix, None, None, None),
        metadata={
            "article_label": article_label,
            "article_no": article_no,
            "article_suffix": article_suffix,
        },
    )
    return node, inline_text


def finalize_article(
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    article_node: NodeRecord,
    raw_paragraphs: list[str],
    regexes: dict[str, re.Pattern[str]],
) -> None:
    # 将“项”独占一行的连续段落先折叠回同一逻辑款，避免把每个项误判成新款。
    paragraph_texts = collapse_item_only_paragraphs(raw_paragraphs, regexes["item_marker"])
    if not paragraph_texts:
        return

    article_no = int(article_node.address["article_no"])
    article_suffix = article_node.address.get("article_suffix")
    article_key = format_article_key(article_no, article_suffix)

    if len(paragraph_texts) == 1:
        _, item_segments = split_item_segments(paragraph_texts[0], regexes["item_marker"])
        if item_segments:
            # 单段正文内出现成组列举项时，仍显式生成“第一款”，保持法条引用口径稳定。
            paragraph_node = NodeRecord(
                id=f"paragraph:{slugify(article_node.source_id)}:{article_key}:01",
                type=LEVEL_TO_NODE_TYPE["paragraph"],
                name=build_paragraph_name(article_node.name, 1),
                level="paragraph",
                source_id=article_node.source_id,
                address=build_address(article_no, article_suffix, 1, None, None),
                metadata={"parent_article_id": article_node.id},
            )
            nodes.append(paragraph_node)
            edges.append(
                build_edge(
                    article_node.id,
                    paragraph_node.id,
                    structural_edge_type(article_node.level, paragraph_node.level),
                )
            )
            attach_text_hierarchy(
                nodes=nodes,
                edges=edges,
                article_node=article_node,
                parent_node=paragraph_node,
                parent_level="paragraph",
                source_text=paragraph_texts[0],
                article_no=article_no,
                article_suffix=article_suffix,
                article_key=article_key,
                paragraph_no=1,
                regexes=regexes,
            )
            return
        attach_text_hierarchy(
            nodes=nodes,
            edges=edges,
            article_node=article_node,
            parent_node=article_node,
            parent_level="article",
            source_text=paragraph_texts[0],
            article_no=article_no,
            article_suffix=article_suffix,
            article_key=article_key,
            paragraph_no=None,
            regexes=regexes,
        )
        return

    for paragraph_index, paragraph_source_text in enumerate(paragraph_texts, start=1):
        paragraph_node = NodeRecord(
            id=f"paragraph:{slugify(article_node.source_id)}:{article_key}:{paragraph_index:02d}",
            type=LEVEL_TO_NODE_TYPE["paragraph"],
            name=build_paragraph_name(article_node.name, paragraph_index),
            level="paragraph",
            source_id=article_node.source_id,
            address=build_address(article_no, article_suffix, paragraph_index, None, None),
            metadata={"parent_article_id": article_node.id},
        )
        nodes.append(paragraph_node)
        edges.append(
            build_edge(
                article_node.id,
                paragraph_node.id,
                structural_edge_type(article_node.level, paragraph_node.level),
            )
        )
        attach_text_hierarchy(
            nodes=nodes,
            edges=edges,
            article_node=article_node,
            parent_node=paragraph_node,
            parent_level="paragraph",
            source_text=paragraph_source_text,
            article_no=article_no,
            article_suffix=article_suffix,
            article_key=article_key,
            paragraph_no=paragraph_index,
            regexes=regexes,
        )


def collapse_item_only_paragraphs(
    raw_paragraphs: list[str],
    item_pattern: re.Pattern[str],
) -> list[str]:
    paragraph_texts = [text.strip() for text in raw_paragraphs if text and text.strip()]
    if not paragraph_texts:
        return []

    collapsed: list[str] = []
    for text in paragraph_texts:
        if not collapsed:
            collapsed.append(text)
            continue
        if item_pattern.match(text):
            # 当前行如果直接以“（一）”等项标记起始，则并入上一款正文。
            collapsed[-1] = f"{collapsed[-1]}\n{text}"
            continue
        collapsed.append(text)
    return collapsed


def attach_text_hierarchy(
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    article_node: NodeRecord,
    parent_node: NodeRecord,
    parent_level: str,
    source_text: str,
    article_no: int,
    article_suffix: int | None,
    article_key: str,
    paragraph_no: int | None,
    regexes: dict[str, re.Pattern[str]],
) -> None:
    parent_lead, item_segments = split_item_segments(source_text, regexes["item_marker"])
    parent_node.text = parent_lead if item_segments else source_text.strip()

    for item_index, item_source_text in enumerate(item_segments, start=1):
        item_marker_text, item_body_text = extract_item_marker(item_source_text)
        item_lead, sub_item_segments = split_sub_item_segments(item_body_text, regexes["sub_item_marker"])
        item_name = build_item_name(parent_node.name, item_index)
        item_node = NodeRecord(
            id=build_item_id(article_node.source_id, article_key, paragraph_no, item_index),
            type=LEVEL_TO_NODE_TYPE["item"],
            name=item_name,
            level="item",
            source_id=article_node.source_id,
            text=item_lead if sub_item_segments else item_body_text.strip(),
            address=build_address(article_no, article_suffix, paragraph_no, item_index, None),
            metadata={
                f"parent_{parent_level}_id": parent_node.id,
                "item_marker": item_marker_text,
            },
        )
        nodes.append(item_node)
        edges.append(
            build_edge(
                parent_node.id,
                item_node.id,
                structural_edge_type(parent_node.level, item_node.level),
            )
        )

        for sub_item_index, sub_item_source_text in enumerate(sub_item_segments, start=1):
            sub_item_marker_text, sub_item_body_text = extract_sub_item_marker(sub_item_source_text)
            sub_item_name = build_sub_item_name(item_node.name, sub_item_index)
            sub_item_node = NodeRecord(
                id=build_sub_item_id(
                    article_node.source_id,
                    article_key,
                    paragraph_no,
                    item_index,
                    sub_item_index,
                ),
                type=LEVEL_TO_NODE_TYPE["sub_item"],
                name=sub_item_name,
                level="sub_item",
                source_id=article_node.source_id,
                text=sub_item_body_text.strip(),
                address=build_address(article_no, article_suffix, paragraph_no, item_index, sub_item_index),
                metadata={
                    "parent_item_id": item_node.id,
                    "sub_item_marker": sub_item_marker_text,
                },
            )
            nodes.append(sub_item_node)
            edges.append(
                build_edge(
                    item_node.id,
                    sub_item_node.id,
                    structural_edge_type(item_node.level, sub_item_node.level),
                )
            )


def finalize_appendix(
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
    document_node: NodeRecord,
    source_id: str,
    appendix_no: int,
    appendix_label: str,
    raw_lines: list[str],
    appendix_item_pattern: re.Pattern[str],
) -> None:
    appendix_key = f"{appendix_no:02d}"
    appendix_node = NodeRecord(
        id=f"appendix:{slugify(source_id)}:{appendix_key}",
        type=LEVEL_TO_NODE_TYPE["appendix"],
        name=appendix_label,
        level="appendix",
        source_id=source_id,
        address=build_appendix_address(appendix_no, None),
        metadata={"appendix_label": appendix_label},
    )
    nodes.append(appendix_node)
    edges.append(
        build_edge(
            document_node.id,
            appendix_node.id,
            structural_edge_type(document_node.level, appendix_node.level),
        )
    )

    intro_lines: list[str] = []
    appendix_items: list[tuple[int, str]] = []
    current_item_no: int | None = None
    current_item_lines: list[str] = []

    for raw_line in raw_lines:
        line = raw_line.strip()
        if not line:
            continue
        item_match = appendix_item_pattern.match(line)
        if item_match:
            if current_item_no is not None:
                appendix_items.append((current_item_no, " ".join(current_item_lines).strip()))
            current_item_no = int(to_fullwidth_digit_text(item_match.group(1)))
            current_item_lines = [item_match.group(2).strip()]
            continue
        if current_item_no is not None:
            current_item_lines.append(line)
        else:
            intro_lines.append(line)

    if current_item_no is not None:
        appendix_items.append((current_item_no, " ".join(current_item_lines).strip()))

    appendix_node.text = " ".join(intro_lines).strip()

    for item_no, item_text in appendix_items:
        appendix_item_node = NodeRecord(
            id=f"appendix_item:{slugify(source_id)}:{appendix_key}:{item_no:02d}",
            type=LEVEL_TO_NODE_TYPE["appendix_item"],
            name=f"{appendix_label}第{item_no}项",
            level="appendix_item",
            source_id=source_id,
            text=item_text,
            address=build_appendix_address(appendix_no, item_no),
            metadata={
                "appendix_label": appendix_label,
                "parent_appendix_id": appendix_node.id,
            },
        )
        nodes.append(appendix_item_node)
        edges.append(
            build_edge(
                appendix_node.id,
                appendix_item_node.id,
                structural_edge_type(appendix_node.level, appendix_item_node.level),
            )
        )


def add_appendix_references(
    nodes: list[NodeRecord],
    edges: list[EdgeRecord],
) -> None:
    appendix_nodes = {
        node.metadata.get("appendix_label"): node
        for node in nodes
        if node.level == "appendix"
    }
    existing_refs = {(edge.source, edge.target, edge.type) for edge in edges}

    for node in nodes:
        if node.level not in {"article", "paragraph"}:
            continue
        if not node.text or "附件" not in node.text:
            continue
        for appendix_label, appendix_node in appendix_nodes.items():
            if appendix_label in node.text:
                edge_key = (node.id, appendix_node.id, "REFERENCE_TO")
                if edge_key in existing_refs:
                    continue
                # 附件在正文中以文本引用出现时，补建条文到附件的显式引用边。
                appendix_node.metadata.setdefault("referenced_by_node_ids", []).append(node.id)
                edges.append(
                    EdgeRecord(
                        id=(
                            f"edge:{slugify('REFERENCE_TO')}:"
                            f"{slugify(node.id)}:{slugify(appendix_node.id)}"
                        ),
                        source=node.id,
                        target=appendix_node.id,
                        type="REFERENCE_TO",
                        evidence=[{"text": node.text, "appendix_label": appendix_label}],
                    )
                )
                existing_refs.add(edge_key)


def split_appendix_blocks(
    appendix_lines: list[str],
    appendix_heading_pattern: re.Pattern[str],
) -> list[tuple[int, str, list[str]]]:
    blocks: list[tuple[int, str, list[str]]] = []
    current_no: int | None = None
    current_label = ""
    current_lines: list[str] = []

    for raw_line in appendix_lines:
        line = raw_line.strip()
        if not line:
            continue
        match = appendix_heading_pattern.match(line)
        if match:
            if current_no is not None:
                blocks.append((current_no, current_label, current_lines))
            current_no = chinese_number_to_int(to_fullwidth_digit_text(match.group(1)))
            current_label = line
            current_lines = []
            continue
        if current_no is not None:
            current_lines.append(line)

    if current_no is not None:
        blocks.append((current_no, current_label, current_lines))
    return blocks


def build_item_id(source_id: str, article_key: str, paragraph_no: int | None, item_no: int) -> str:
    base = f"item:{slugify(source_id)}:{article_key}"
    if paragraph_no is not None:
        base = f"{base}:{paragraph_no:02d}"
    return f"{base}:{item_no:02d}"


def build_sub_item_id(
    source_id: str,
    article_key: str,
    paragraph_no: int | None,
    item_no: int,
    sub_item_no: int,
) -> str:
    item_id = build_item_id(source_id, article_key, paragraph_no, item_no)
    return f"{item_id}:{sub_item_no:02d}".replace("item:", "sub_item:", 1)


def find_parent(level_stack: dict[str, NodeRecord], level: str) -> NodeRecord:
    index = LEVEL_ORDER.index(level)
    for parent_level in reversed(LEVEL_ORDER[:index]):
        if parent_level in level_stack:
            return level_stack[parent_level]
    raise ValueError(f"Missing parent for level {level}")


def clear_lower_levels(level_stack: dict[str, NodeRecord], level: str) -> None:
    index = LEVEL_ORDER.index(level)
    for child_level in LEVEL_ORDER[index + 1 :]:
        level_stack.pop(child_level, None)


def split_item_segments(text: str, item_pattern: re.Pattern[str]) -> tuple[str, list[str]]:
    matches = list(item_pattern.finditer(text))
    if not matches:
        return text.strip(), []
    lead_text = text[: matches[0].start()].strip()
    items = []
    for index, match in enumerate(matches):
        start = match.start()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        items.append(text[start:end].strip())
    return lead_text, items


def split_sub_item_segments(text: str, sub_item_pattern: re.Pattern[str]) -> tuple[str, list[str]]:
    matches = list(sub_item_pattern.finditer(text))
    if not matches:
        return text.strip(), []
    lead_text = text[: matches[0].start()].strip()
    sub_items = []
    for index, match in enumerate(matches):
        start = match.start(1)
        end = matches[index + 1].start(1) if index + 1 < len(matches) else len(text)
        sub_items.append(text[start:end].strip())
    return lead_text, sub_items


def extract_item_marker(text: str) -> tuple[str, str]:
    match = re.match(r"^(（[一二三四五六七八九十]+）)(.*)$", text.strip())
    if not match:
        raise ValueError(f"Invalid item text: {text}")
    return match.group(1), match.group(2).strip()


def extract_sub_item_marker(text: str) -> tuple[str, str]:
    match = re.match(r"^([0-9０-９]+[.．])(.*)$", text.strip())
    if not match:
        raise ValueError(f"Invalid sub-item text: {text}")
    return match.group(1), match.group(2).strip()


def build_catalog_address(level: str, counters: dict[str, int]) -> dict[str, int | None]:
    return {
        "part_no": counters["part"] or None,
        "chapter_no": counters["chapter"] or None,
        "section_no": counters["section"] or None,
        "level_marker": level,
    }


def build_address(
    article_no: int,
    article_suffix: int | None,
    paragraph_no: int | None,
    item_no: int | None,
    sub_item_no: int | None,
) -> dict[str, int | None]:
    return {
        "article_no": article_no,
        "article_suffix": article_suffix,
        "paragraph_no": paragraph_no,
        "item_no": item_no,
        "sub_item_no": sub_item_no,
        "appendix_no": None,
        "appendix_item_no": None,
    }


def build_appendix_address(
    appendix_no: int,
    appendix_item_no: int | None,
) -> dict[str, int | None]:
    return {
        "article_no": None,
        "article_suffix": None,
        "paragraph_no": None,
        "item_no": None,
        "sub_item_no": None,
        "appendix_no": appendix_no,
        "appendix_item_no": appendix_item_no,
    }


def build_paragraph_name(article_name: str, paragraph_no: int) -> str:
    return f"{article_name}第{int_to_cn(paragraph_no)}款"


def build_item_name(parent_name: str, item_no: int) -> str:
    return f"{parent_name}第{int_to_cn(item_no)}项"


def build_sub_item_name(item_name: str, sub_item_no: int) -> str:
    return f"{item_name}第{sub_item_no}目"


def build_edge(source_id: str, target_id: str, edge_type: str) -> EdgeRecord:
    return EdgeRecord(
        id=f"edge:{slugify(edge_type)}:{slugify(source_id)}:{slugify(target_id)}",
        source=source_id,
        target=target_id,
        type=edge_type,
    )


def structural_edge_type(parent_level: str, child_level: str) -> str:
    key = (parent_level, child_level)
    if key not in STRUCTURAL_EDGES:
        raise ValueError(f"Missing structural edge rule in schema: {parent_level} -> {child_level}")
    return STRUCTURAL_EDGES[key]
