from __future__ import annotations

from dataclasses import dataclass
import re

from .ids import slugify
from .numbers import format_article_key


@dataclass(frozen=True)
class NodeLocator:
    kind: str
    article_no: int | None = None
    article_suffix: int | None = None
    paragraph_no: int | None = None
    item_no: int | None = None
    sub_item_no: int | None = None
    segment_no: int | None = None
    appendix_no: int | None = None


_ARTICLE_KEY_RE = re.compile(r"^(?P<article>\d{4})(?:-(?P<suffix>\d{2}))?$")
_DOCUMENT_ID_RE = re.compile(r"^document:(?P<source>.+)$")
_TOC_ID_RE = re.compile(r"^(?P<kind>part|chapter|section):(?P<source>.+):(?P<tail>.+)$")
_ARTICLE_ID_RE = re.compile(r"^article:(?P<source>.+):(?P<article_key>\d{4}(?:-\d{2})?)$")
_PARAGRAPH_ID_RE = re.compile(
    r"^paragraph:(?P<source>.+):(?P<article_key>\d{4}(?:-\d{2})?):(?P<paragraph>\d{2})$"
)
_ITEM_ID_RE = re.compile(
    r"^item:(?P<source>.+):(?P<article_key>\d{4}(?:-\d{2})?)(?::(?P<paragraph>\d{2}))?:(?P<item>\d{2})$"
)
_SEGMENT_ITEM_ID_RE = re.compile(
    r"^item:(?P<source>.+):segment:(?P<segment>\d{4}):(?P<item>\d{2})$"
)
_SUB_ITEM_ID_RE = re.compile(
    r"^sub_item:(?P<source>.+):(?P<article_key>\d{4}(?:-\d{2})?)(?::(?P<paragraph>\d{2}))?:(?P<item>\d{2}):(?P<sub_item>\d{2})$"
)
_SEGMENT_SUB_ITEM_ID_RE = re.compile(
    r"^sub_item:(?P<source>.+):segment:(?P<segment>\d{4}):(?P<item>\d{2}):(?P<sub_item>\d{2})$"
)
_SEGMENT_ID_RE = re.compile(r"^segment:(?P<source>.+):(?P<segment>\d{4})$")
_APPENDIX_ID_RE = re.compile(r"^appendix:(?P<source>.+):(?P<appendix>\d{2})$")


def source_id_from_node_id(node_id: str) -> str:
    if matched := _DOCUMENT_ID_RE.fullmatch(node_id):
        return matched.group("source")
    if matched := _TOC_ID_RE.fullmatch(node_id):
        return matched.group("source")
    if matched := _ARTICLE_ID_RE.fullmatch(node_id):
        return matched.group("source")
    if matched := _PARAGRAPH_ID_RE.fullmatch(node_id):
        return matched.group("source")
    if matched := _ITEM_ID_RE.fullmatch(node_id):
        return matched.group("source")
    if matched := _SEGMENT_ITEM_ID_RE.fullmatch(node_id):
        return matched.group("source")
    if matched := _SUB_ITEM_ID_RE.fullmatch(node_id):
        return matched.group("source")
    if matched := _SEGMENT_SUB_ITEM_ID_RE.fullmatch(node_id):
        return matched.group("source")
    if matched := _SEGMENT_ID_RE.fullmatch(node_id):
        return matched.group("source")
    if matched := _APPENDIX_ID_RE.fullmatch(node_id):
        return matched.group("source")
    parts = node_id.split(":")
    if len(parts) < 2:
        raise ValueError(f"Unsupported node id: {node_id}")
    return parts[1]


def owner_source_id(owner_id: str) -> str:
    if owner_id.startswith("document:"):
        return source_id_from_node_id(owner_id)
    return owner_id


def _parse_article_key(article_key: str) -> tuple[int, int | None]:
    matched = _ARTICLE_KEY_RE.fullmatch(article_key)
    if not matched:
        raise ValueError(f"Invalid article key: {article_key}")
    article_no = int(matched.group("article"))
    suffix_text = matched.group("suffix")
    return article_no, int(suffix_text) if suffix_text else None


def node_locator_from_node_id(node_id: str) -> NodeLocator | None:
    if _DOCUMENT_ID_RE.fullmatch(node_id):
        return NodeLocator(kind="document")
    if _TOC_ID_RE.fullmatch(node_id):
        return NodeLocator(kind="toc")
    if matched := _ARTICLE_ID_RE.fullmatch(node_id):
        article_no, article_suffix = _parse_article_key(matched.group("article_key"))
        return NodeLocator(kind="provision", article_no=article_no, article_suffix=article_suffix)
    if matched := _PARAGRAPH_ID_RE.fullmatch(node_id):
        article_no, article_suffix = _parse_article_key(matched.group("article_key"))
        return NodeLocator(
            kind="provision",
            article_no=article_no,
            article_suffix=article_suffix,
            paragraph_no=int(matched.group("paragraph")),
        )
    if matched := _ITEM_ID_RE.fullmatch(node_id):
        article_no, article_suffix = _parse_article_key(matched.group("article_key"))
        paragraph_text = matched.group("paragraph")
        return NodeLocator(
            kind="provision",
            article_no=article_no,
            article_suffix=article_suffix,
            paragraph_no=int(paragraph_text) if paragraph_text else None,
            item_no=int(matched.group("item")),
        )
    if matched := _SEGMENT_ITEM_ID_RE.fullmatch(node_id):
        return NodeLocator(
            kind="provision",
            segment_no=int(matched.group("segment")),
            item_no=int(matched.group("item")),
        )
    if matched := _SUB_ITEM_ID_RE.fullmatch(node_id):
        article_no, article_suffix = _parse_article_key(matched.group("article_key"))
        paragraph_text = matched.group("paragraph")
        return NodeLocator(
            kind="provision",
            article_no=article_no,
            article_suffix=article_suffix,
            paragraph_no=int(paragraph_text) if paragraph_text else None,
            item_no=int(matched.group("item")),
            sub_item_no=int(matched.group("sub_item")),
        )
    if matched := _SEGMENT_SUB_ITEM_ID_RE.fullmatch(node_id):
        return NodeLocator(
            kind="provision",
            segment_no=int(matched.group("segment")),
            item_no=int(matched.group("item")),
            sub_item_no=int(matched.group("sub_item")),
        )
    if matched := _SEGMENT_ID_RE.fullmatch(node_id):
        return NodeLocator(kind="provision", segment_no=int(matched.group("segment")))
    if matched := _APPENDIX_ID_RE.fullmatch(node_id):
        return NodeLocator(kind="appendix", appendix_no=int(matched.group("appendix")))
    return None


def node_id_from_locator(locator: NodeLocator, source_id: str) -> str | None:
    source_slug = slugify(source_id)
    if locator.kind == "document":
        return f"document:{source_slug}"
    if locator.kind == "toc":
        return None
    if locator.kind == "appendix":
        if locator.appendix_no is None:
            return None
        return f"appendix:{source_slug}:{locator.appendix_no:02d}"
    if locator.kind != "provision":
        return None
    if locator.segment_no is not None:
        if locator.item_no is None and locator.sub_item_no is None:
            return f"segment:{source_slug}:{locator.segment_no:04d}"
        if locator.sub_item_no is None:
            return f"item:{source_slug}:segment:{locator.segment_no:04d}:{locator.item_no:02d}"
        return (
            f"sub_item:{source_slug}:segment:{locator.segment_no:04d}:"
            f"{locator.item_no:02d}:{locator.sub_item_no:02d}"
        )
    if locator.article_no is None:
        return None
    article_key = format_article_key(locator.article_no, locator.article_suffix)
    if locator.paragraph_no is None and locator.item_no is None and locator.sub_item_no is None:
        return f"article:{source_slug}:{article_key}"
    if locator.item_no is None and locator.sub_item_no is None:
        return f"paragraph:{source_slug}:{article_key}:{locator.paragraph_no:02d}"
    if locator.sub_item_no is None:
        base = f"item:{source_slug}:{article_key}"
        if locator.paragraph_no is not None:
            base = f"{base}:{locator.paragraph_no:02d}"
        return f"{base}:{locator.item_no:02d}"
    base = f"sub_item:{source_slug}:{article_key}"
    if locator.paragraph_no is not None:
        base = f"{base}:{locator.paragraph_no:02d}"
    return f"{base}:{locator.item_no:02d}:{locator.sub_item_no:02d}"


def node_sort_key(node: object) -> tuple[object, ...]:
    node_id = str(getattr(node, "id", ""))
    level = str(getattr(node, "level", ""))
    locator = node_locator_from_node_id(node_id)

    if level in {"part", "chapter", "section", "concept"}:
        return (0, node_id)
    if level == "article" and locator is not None:
        return (1, int(locator.article_no or 0), int(locator.article_suffix or 0), node_id)
    if level == "paragraph" and locator is not None:
        return (2, int(locator.paragraph_no or 0), node_id)
    if level == "item" and locator is not None:
        return (3, int(locator.item_no or 0), node_id)
    if level == "sub_item" and locator is not None:
        return (4, int(locator.sub_item_no or 0), node_id)
    if level == "segment" and locator is not None:
        return (5, int(locator.segment_no or 0), node_id)
    if level == "appendix" and locator is not None:
        return (6, int(locator.appendix_no or 0), node_id)
    if level == "document":
        return (7, node_id)
    return (99, node_id)
