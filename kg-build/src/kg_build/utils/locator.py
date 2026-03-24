from __future__ import annotations

from dataclasses import dataclass
import re
from typing import TYPE_CHECKING, Any

from .ids import slugify
from .numbers import chinese_number_to_int, format_article_key

if TYPE_CHECKING:
    from ..contracts import GraphBundle, NodeRecord


@dataclass(frozen=True)
class NodeLocator:
    kind: str
    law: str = ""
    article_no: int | None = None
    article_suffix: int | None = None
    paragraph_no: int | None = None
    item_no: int | None = None
    sub_item_no: int | None = None
    appendix_no: int | None = None
    appendix_item_no: int | None = None


_ARTICLE_KEY_RE = re.compile(r"^(?P<article>\d{4})(?:-(?P<suffix>\d{2}))?$")
_DOCUMENT_ID_RE = re.compile(r"^document:(?P<source>.+)$")
_ARTICLE_ID_RE = re.compile(r"^article:(?P<source>.+):(?P<article_key>\d{4}(?:-\d{2})?)$")
_PARAGRAPH_ID_RE = re.compile(
    r"^paragraph:(?P<source>.+):(?P<article_key>\d{4}(?:-\d{2})?):(?P<paragraph>\d{2})$"
)
_ITEM_ID_RE = re.compile(
    r"^item:(?P<source>.+):(?P<article_key>\d{4}(?:-\d{2})?)(?::(?P<paragraph>\d{2}))?:(?P<item>\d{2})$"
)
_SUB_ITEM_ID_RE = re.compile(
    r"^sub_item:(?P<source>.+):(?P<article_key>\d{4}(?:-\d{2})?)(?::(?P<paragraph>\d{2}))?:(?P<item>\d{2}):(?P<sub_item>\d{2})$"
)
_APPENDIX_ID_RE = re.compile(r"^appendix:(?P<source>.+):(?P<appendix>\d{2})$")
_APPENDIX_ITEM_ID_RE = re.compile(
    r"^appendix_item:(?P<source>.+):(?P<appendix>\d{2}):(?P<item>\d{2})$"
)

LEGAL_NUM_PATTERN = r"[一二三四五六七八九十百千万零两〇0-9]+"


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
    if matched := _APPENDIX_ID_RE.fullmatch(node_id):
        return NodeLocator(kind="appendix", appendix_no=int(matched.group("appendix")))
    if matched := _APPENDIX_ITEM_ID_RE.fullmatch(node_id):
        return NodeLocator(
            kind="appendix",
            appendix_no=int(matched.group("appendix")),
            appendix_item_no=int(matched.group("item")),
        )
    return None


def node_id_from_locator(
    locator: NodeLocator,
    source_id: str,
    *,
    require_existing: bool = False,
    reference_lookup: dict[str, Any] | None = None,
) -> str | None:
    source_slug = slugify(source_id)
    if locator.kind == "document":
        node_id = f"document:{source_slug}"
    elif locator.kind == "appendix":
        if locator.appendix_no is None:
            return None
        appendix_key = f"{locator.appendix_no:02d}"
        if locator.appendix_item_no is None:
            node_id = f"appendix:{source_slug}:{appendix_key}"
        else:
            node_id = f"appendix_item:{source_slug}:{appendix_key}:{locator.appendix_item_no:02d}"
    elif locator.kind == "provision":
        if locator.article_no is None:
            return None
        article_key = format_article_key(locator.article_no, locator.article_suffix)
        if locator.paragraph_no is None and locator.item_no is None and locator.sub_item_no is None:
            node_id = f"article:{source_slug}:{article_key}"
        elif locator.item_no is None and locator.sub_item_no is None:
            node_id = f"paragraph:{source_slug}:{article_key}:{locator.paragraph_no:02d}"
        elif locator.sub_item_no is None:
            base = f"item:{source_slug}:{article_key}"
            if locator.paragraph_no is not None:
                base = f"{base}:{locator.paragraph_no:02d}"
            node_id = f"{base}:{locator.item_no:02d}"
        else:
            base = f"sub_item:{source_slug}:{article_key}"
            if locator.paragraph_no is not None:
                base = f"{base}:{locator.paragraph_no:02d}"
            node_id = f"{base}:{locator.item_no:02d}:{locator.sub_item_no:02d}"
    else:
        return None

    if require_existing:
        if reference_lookup is None:
            raise ValueError("reference_lookup is required when require_existing=True")
        node_index = reference_lookup.get("node_index", {})
        return node_id if node_id in node_index else None
    return node_id


def _provision_key(locator: NodeLocator) -> tuple[int, int | None, int | None, int | None, int | None]:
    if locator.kind != "provision" or locator.article_no is None:
        raise ValueError(f"Not a provision locator: {locator}")
    return (
        locator.article_no,
        locator.article_suffix,
        locator.paragraph_no,
        locator.item_no,
        locator.sub_item_no,
    )


def _appendix_key(locator: NodeLocator) -> tuple[int, int | None]:
    if locator.kind != "appendix" or locator.appendix_no is None:
        raise ValueError(f"Not an appendix locator: {locator}")
    return locator.appendix_no, locator.appendix_item_no


def build_reference_lookup(bundle: "GraphBundle") -> dict[str, Any]:
    document_node = next((node for node in bundle.nodes if node.type == "DocumentNode"), None)
    appendix_by_key: dict[tuple[int, int | None], "NodeRecord"] = {}
    provision_by_key: dict[tuple[int, int | None, int | None, int | None, int | None], "NodeRecord"] = {}
    node_index: dict[str, "NodeRecord"] = {node.id: node for node in bundle.nodes}
    for node in bundle.nodes:
        locator = node_locator_from_node_id(node.id)
        if locator is None:
            continue
        if locator.kind == "appendix":
            appendix_by_key[_appendix_key(locator)] = node
        elif locator.kind == "provision":
            provision_by_key[_provision_key(locator)] = node
    return {
        "document": document_node,
        "appendix_by_key": appendix_by_key,
        "provision_by_key": provision_by_key,
        "node_index": node_index,
    }


def _resolve_locator(locator: NodeLocator, reference_lookup: dict[str, Any]) -> "NodeRecord | None":
    if locator.kind == "document":
        return reference_lookup.get("document")
    if locator.kind == "appendix":
        return reference_lookup.get("appendix_by_key", {}).get(_appendix_key(locator))
    if locator.kind != "provision":
        return None
    return reference_lookup.get("provision_by_key", {}).get(_provision_key(locator))


def resolve_reference_targets(
    *,
    owner_node_id: str,
    evidence_text: str,
    reference_lookup: dict[str, Any],
) -> list[str]:
    owner_node = reference_lookup.get("node_index", {}).get(owner_node_id)
    if owner_node is None:
        return []
    owner_locator = node_locator_from_node_id(owner_node.id)
    if owner_locator is None:
        return []
    locators = _extract_reference_locators_from_text(evidence_text, owner_locator)
    target_ids: list[str] = []
    seen: set[str] = set()
    for locator in locators:
        target_node = _resolve_locator(locator, reference_lookup)
        if target_node is None or target_node.id in seen:
            continue
        seen.add(target_node.id)
        target_ids.append(target_node.id)
    return target_ids


def _extract_reference_locators_from_text(
    text: str,
    owner_locator: NodeLocator,
) -> list[NodeLocator]:
    results: list[NodeLocator] = []
    seen: set[tuple] = set()

    def add(locator: NodeLocator | None) -> None:
        if locator is None:
            return
        key = (
            locator.kind,
            locator.law,
            locator.article_no,
            locator.article_suffix,
            locator.paragraph_no,
            locator.item_no,
            locator.sub_item_no,
            locator.appendix_no,
            locator.appendix_item_no,
        )
        if key in seen:
            return
        seen.add(key)
        results.append(locator)

    normalized = text.strip()

    for match in re.finditer(
        rf"第({LEGAL_NUM_PATTERN})条(?:之({LEGAL_NUM_PATTERN}))?至第({LEGAL_NUM_PATTERN})条(?:之({LEGAL_NUM_PATTERN}))?",
        normalized,
    ):
        start_article_no = chinese_number_to_int(match.group(1))
        start_article_suffix = chinese_number_to_int(match.group(2)) if match.group(2) else None
        end_article_no = chinese_number_to_int(match.group(3))
        end_article_suffix = chinese_number_to_int(match.group(4)) if match.group(4) else None
        if start_article_suffix is None and end_article_suffix is None and start_article_no <= end_article_no:
            for article_no in range(start_article_no, end_article_no + 1):
                add(NodeLocator(kind="provision", article_no=article_no))
        else:
            add(NodeLocator(kind="provision", article_no=start_article_no, article_suffix=start_article_suffix))
            add(NodeLocator(kind="provision", article_no=end_article_no, article_suffix=end_article_suffix))

    for match in re.finditer(
        rf"第({LEGAL_NUM_PATTERN})条(?:之({LEGAL_NUM_PATTERN}))?(?:第({LEGAL_NUM_PATTERN})款)?(?:第({LEGAL_NUM_PATTERN})项)?(?:第({LEGAL_NUM_PATTERN})目)?",
        normalized,
    ):
        article_no = chinese_number_to_int(match.group(1))
        article_suffix = chinese_number_to_int(match.group(2)) if match.group(2) else None
        paragraph_no = chinese_number_to_int(match.group(3)) if match.group(3) else None
        item_no = chinese_number_to_int(match.group(4)) if match.group(4) else None
        sub_item_no = chinese_number_to_int(match.group(5)) if match.group(5) else None
        add(
            NodeLocator(
                kind="provision",
                article_no=article_no,
                article_suffix=article_suffix,
                paragraph_no=paragraph_no,
                item_no=item_no,
                sub_item_no=sub_item_no,
            )
        )

    for match in re.finditer(r"附件([一二三四五六七八九十百千万零两〇0-9]+)", normalized):
        add(NodeLocator(kind="appendix", appendix_no=chinese_number_to_int(match.group(1))))

    _add_relative_reference_locators(normalized, owner_locator, add)
    return results


def _add_relative_reference_locators(
    text: str,
    owner_locator: NodeLocator,
    add: Any,
) -> None:
    if owner_locator.kind != "provision" or owner_locator.article_no is None:
        return

    def add_paragraph_refs(paragraph_numbers: list[int]) -> None:
        for paragraph_no in paragraph_numbers:
            if paragraph_no <= 0:
                continue
            add(
                NodeLocator(
                    kind="provision",
                    article_no=owner_locator.article_no,
                    article_suffix=owner_locator.article_suffix,
                    paragraph_no=paragraph_no,
                )
            )

    def add_item_refs(paragraph_no: int | None, item_numbers: list[int]) -> None:
        for item_no in item_numbers:
            if item_no <= 0:
                continue
            add(
                NodeLocator(
                    kind="provision",
                    article_no=owner_locator.article_no,
                    article_suffix=owner_locator.article_suffix,
                    paragraph_no=paragraph_no,
                    item_no=item_no,
                )
            )

    if owner_locator.paragraph_no is not None:
        for match in re.finditer(r"前([一二三四五六七八九十百千万零两〇0-9]+)款", text):
            count = chinese_number_to_int(match.group(1))
            add_paragraph_refs(list(range(max(1, owner_locator.paragraph_no - count), owner_locator.paragraph_no)))
        if "前款" in text:
            add_paragraph_refs([owner_locator.paragraph_no - 1])
        for match in re.finditer(r"第([一二三四五六七八九十百千万零两〇0-9]+)款", text):
            add_paragraph_refs([chinese_number_to_int(match.group(1))])
        if "本款" in text:
            add_paragraph_refs([owner_locator.paragraph_no])

    paragraph_for_items = owner_locator.paragraph_no
    if paragraph_for_items is not None and owner_locator.item_no is not None:
        for match in re.finditer(r"前([一二三四五六七八九十百千万零两〇0-9]+)项", text):
            count = chinese_number_to_int(match.group(1))
            add_item_refs(
                paragraph_for_items,
                list(range(max(1, owner_locator.item_no - count), owner_locator.item_no)),
            )
        if "前项" in text:
            add_item_refs(paragraph_for_items, [owner_locator.item_no - 1])
        if "本项" in text:
            add_item_refs(paragraph_for_items, [owner_locator.item_no])

    if paragraph_for_items is not None:
        relative_item_match = re.search(r"前款第(.+?)项", text)
        if relative_item_match and owner_locator.paragraph_no is not None:
            item_numbers = _extract_enumerated_numbers(relative_item_match.group(1), "项")
            add_item_refs(owner_locator.paragraph_no - 1, item_numbers)

    if "本条" in text:
        add(
            NodeLocator(
                kind="provision",
                article_no=owner_locator.article_no,
                article_suffix=owner_locator.article_suffix,
            )
        )
    if "前条" in text and owner_locator.article_no > 1:
        add(NodeLocator(kind="provision", article_no=owner_locator.article_no - 1))


def _extract_enumerated_numbers(text: str, unit: str) -> list[int]:
    numbers: list[int] = []
    for match in re.finditer(rf"第([一二三四五六七八九十百千万零两〇0-9]+){unit}", text):
        numbers.append(chinese_number_to_int(match.group(1)))
    return numbers
