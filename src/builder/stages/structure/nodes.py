from __future__ import annotations

from ...contracts import DocumentUnitRecord, NodeRecord
from ...utils.ids import slugify
from ...utils.locator import NodeLocator, node_id_from_locator, source_id_from_node_id
from ...utils.numbers import format_article_key, int_to_cn, parse_article_components
from .patterns import ARTICLE_RE, LEVEL_TO_NODE_TYPE


def build_document_node(unit: DocumentUnitRecord) -> NodeRecord:
    source_metadata = unit.metadata
    return NodeRecord(
        id=f"document:{unit.source_id}",
        type=LEVEL_TO_NODE_TYPE["document"],
        name=unit.title,
        level="document",
        category=_string_value(source_metadata.get("category")),
        status=_string_value(source_metadata.get("status")),
        issuer=_string_value(source_metadata.get("issuer")),
        publish_date=_string_value(source_metadata.get("publish_date")),
        effective_date=_string_value(source_metadata.get("effective_date")),
        source_url=_string_value(source_metadata.get("source_url")),
    )


def create_toc_node(
    *,
    level: str,
    line: str,
    source_id: str,
    counters: dict[str, int],
    parent_path: str = "",
) -> NodeRecord:
    counters[level] = _next_scoped_ordinal(counters, level, parent_path)
    return NodeRecord(
        id=_toc_id(level=level, source_id=source_id, parent_path=parent_path, ordinal=int(counters[level])),
        type=LEVEL_TO_NODE_TYPE[level],
        name=line,
        level=level,
    )


def build_article_node(source_id: str, line: str, body_index: int) -> tuple[NodeRecord, str]:
    del body_index
    match = ARTICLE_RE.match(line.strip())
    if not match:
        raise ValueError(f"Unable to parse article line: {line}")
    article_label = match.group(1)
    inline_text = (match.group(2) or "").strip()
    article_no, article_suffix = parse_article_components(article_label)
    return (
        NodeRecord(
            id=_article_id(source_id, article_no, article_suffix),
            type=LEVEL_TO_NODE_TYPE["article"],
            name=article_label,
            level="article",
        ),
        inline_text,
    )


def build_paragraph_node(*, article_node: NodeRecord, article_no: int, article_suffix: int | None, paragraph_no: int) -> NodeRecord:
    source_id = source_id_from_node_id(article_node.id)
    return NodeRecord(
        id=_paragraph_id(source_id, article_no, article_suffix, paragraph_no),
        type=LEVEL_TO_NODE_TYPE["paragraph"],
        name=f"{article_node.name}第{int_to_cn(paragraph_no)}款",
        level="paragraph",
    )


def create_candidate_chapter_node(
    *,
    source_id: str,
    counters: dict[str, int],
    name: str,
    parent_path: str = "",
) -> NodeRecord:
    counters["chapter"] = _next_scoped_ordinal(counters, "chapter", parent_path)
    return NodeRecord(
        id=_toc_id(level="chapter", source_id=source_id, parent_path=parent_path, ordinal=int(counters["chapter"])),
        type=LEVEL_TO_NODE_TYPE["chapter"],
        name=name,
        level="chapter",
    )


def create_candidate_section_node(
    *,
    source_id: str,
    counters: dict[str, int],
    name: str,
    parent_path: str = "",
) -> NodeRecord:
    counters["section"] = _next_scoped_ordinal(counters, "section", parent_path)
    return NodeRecord(
        id=_toc_id(level="section", source_id=source_id, parent_path=parent_path, ordinal=int(counters["section"])),
        type=LEVEL_TO_NODE_TYPE["section"],
        name=name,
        level="section",
    )


def create_candidate_article_node(*, source_id: str, article_index: int) -> NodeRecord:
    return NodeRecord(
        id=_article_id(source_id, article_index, None),
        type=LEVEL_TO_NODE_TYPE["article"],
        name=f"第{int_to_cn(article_index)}条",
        level="article",
    )


def create_segment_node(*, source_id: str, counters: dict[str, int], name: str, text: str) -> NodeRecord:
    counters["segment"] += 1
    segment_no = counters["segment"]
    return NodeRecord(
        id=node_id_from_locator(NodeLocator(kind="provision", segment_no=segment_no), source_id)
        or f"segment:{slugify(source_id)}:{segment_no:04d}",
        type=LEVEL_TO_NODE_TYPE["segment"],
        name=name,
        level="segment",
        text=text.strip(),
    )


def create_appendix_node(*, source_id: str, counters: dict[str, int], name: str) -> NodeRecord:
    counters["appendix"] += 1
    appendix_no = counters["appendix"]
    return NodeRecord(
        id=node_id_from_locator(NodeLocator(kind="appendix", appendix_no=appendix_no), source_id)
        or f"appendix:{slugify(source_id)}:{appendix_no:02d}",
        type=LEVEL_TO_NODE_TYPE["appendix"],
        name=name,
        level="appendix",
    )


def create_direct_item_node(*, source_id: str, parent: NodeRecord, item_no: int, text: str) -> NodeRecord:
    return NodeRecord(
        id=build_fallback_item_id(source_id, None, None, None, None, item_no, parent.id),
        type=LEVEL_TO_NODE_TYPE["item"],
        name=build_item_name(parent, item_no),
        level="item",
        text=text.strip(),
    )


def create_item_node(
    *,
    source_id: str,
    parent_node: NodeRecord,
    article_no: int | None,
    article_suffix: int | None,
    paragraph_no: int | None,
    segment_no: int | None,
    item_no: int,
    parent_node_id: str | None,
    text: str,
) -> NodeRecord:
    return NodeRecord(
        id=node_id_from_locator(
            NodeLocator(
                kind="provision",
                article_no=article_no,
                article_suffix=article_suffix,
                paragraph_no=paragraph_no,
                segment_no=segment_no,
                item_no=item_no,
            ),
            source_id,
        )
        or build_fallback_item_id(source_id, article_no, article_suffix, paragraph_no, segment_no, item_no, parent_node_id),
        type=LEVEL_TO_NODE_TYPE["item"],
        name=build_item_name(parent_node, item_no),
        level="item",
        text=text.strip(),
    )


def create_sub_item_node(
    *,
    source_id: str,
    item_node: NodeRecord,
    article_no: int | None,
    article_suffix: int | None,
    paragraph_no: int | None,
    segment_no: int | None,
    item_no: int,
    sub_item_no: int,
    parent_node_id: str | None,
    text: str,
) -> NodeRecord:
    return NodeRecord(
        id=node_id_from_locator(
            NodeLocator(
                kind="provision",
                article_no=article_no,
                article_suffix=article_suffix,
                paragraph_no=paragraph_no,
                segment_no=segment_no,
                item_no=item_no,
                sub_item_no=sub_item_no,
            ),
            source_id,
        )
        or build_fallback_sub_item_id(
            source_id,
            article_no,
            article_suffix,
            paragraph_no,
            segment_no,
            item_no,
            sub_item_no,
            parent_node_id,
        ),
        type=LEVEL_TO_NODE_TYPE["sub_item"],
        name=build_sub_item_name(item_node, sub_item_no),
        level="sub_item",
        text=text.strip(),
    )


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


def _article_id(source_id: str, article_no: int, article_suffix: int | None) -> str:
    return node_id_from_locator(
        NodeLocator(kind="provision", article_no=article_no, article_suffix=article_suffix),
        source_id,
    ) or f"article:{slugify(source_id)}:{format_article_key(article_no, article_suffix)}"


def _paragraph_id(source_id: str, article_no: int, article_suffix: int | None, paragraph_no: int) -> str:
    return node_id_from_locator(
        NodeLocator(kind="provision", article_no=article_no, article_suffix=article_suffix, paragraph_no=paragraph_no),
        source_id,
    ) or f"paragraph:{slugify(source_id)}:{format_article_key(article_no, article_suffix)}:{paragraph_no:02d}"


def _next_scoped_ordinal(counters: dict[str, int], level: str, parent_path: str) -> int:
    scoped = counters.setdefault("scoped_ordinals", {})
    key = (level, parent_path)
    ordinal = int(scoped.get(key, 0)) + 1
    scoped[key] = ordinal
    return ordinal


def _toc_id(*, level: str, source_id: str, parent_path: str, ordinal: int) -> str:
    if not parent_path:
        return f"{level}:{slugify(source_id)}:{ordinal:02d}"
    return f"{level}:{slugify(source_id)}:{parent_path}:{ordinal:02d}"


def _string_value(value: object) -> str:
    if value in {None, ""}:
        return ""
    return str(value)
