from __future__ import annotations

import re
from dataclasses import dataclass

from ...utils.numbers import chinese_number_to_int, int_to_cn
from ...utils.reference import ancestor_at_level, previous_sibling, tail_label
from .patterns import NON_TARGET_PAREN_RE, PARALLEL_ITEM_GROUP_RE
from .types import ReferenceCandidate, ResolvedReference

LEVEL_ORDER = ("article", "paragraph", "item", "sub_item")
REF_SEGMENT_RE = re.compile(
    r"第(?P<article>[一二三四五六七八九十百千万零两〇0-9]+)条(?:之(?P<article_suffix>[一二三四五六七八九十百千万零两〇0-9]+))?"
    r"(?P<paragraph>第[一二三四五六七八九十百千万零两〇0-9]+款)?"
    r"(?P<item>第(?:[（(][一二三四五六七八九十百千万零两〇0-9]+[）)]|[一二三四五六七八九十百千万零两〇0-9]+)项)?"
    r"(?P<sub_item>第(?:[（(][一二三四五六七八九十百千万零两〇0-9]+[）)]|[一二三四五六七八九十百千万零两〇0-9]+)目)?"
    r"|(?P<paragraph_only>第[一二三四五六七八九十百千万零两〇0-9]+款)"
    r"(?P<paragraph_item>第(?:[（(][一二三四五六七八九十百千万零两〇0-9]+[）)]|[一二三四五六七八九十百千万零两〇0-9]+)项)?"
    r"(?P<paragraph_sub_item>第(?:[（(][一二三四五六七八九十百千万零两〇0-9]+[）)]|[一二三四五六七八九十百千万零两〇0-9]+)目)?"
    r"|(?P<item_only>(?:第)?(?:[（(][一二三四五六七八九十百千万零两〇0-9]+[）)]|[一二三四五六七八九十百千万零两〇0-9]+)项)"
    r"(?P<item_sub_item>第(?:[（(][一二三四五六七八九十百千万零两〇0-9]+[）)]|[一二三四五六七八九十百千万零两〇0-9]+)目)?"
    r"|(?P<sub_item_only>第(?:[（(][一二三四五六七八九十百千万零两〇0-9]+[）)]|[一二三四五六七八九十百千万零两〇0-9]+)目)"
)
@dataclass(frozen=True)
class TargetDescriptor:
    article_label: str = ""
    paragraph_label: str = ""
    item_label: str = ""
    sub_item_label: str = ""
    span_start: int = -1
    span_end: int = -1
    display_text: str = ""
    is_range: bool = False


def resolve_candidates(
    candidates: list[ReferenceCandidate],
    *,
    node_index: dict[str, object],
    parent_by_child: dict[str, str],
    provision_index: dict[str, dict[tuple[str, str, str, str], str]],
    title_to_document_ids: dict[str, list[str]],
    children_by_parent_level: dict[tuple[str, str], list[str]],
    current_document_id: str,
    allow_unresolved_targets: bool = False,
) -> list[ResolvedReference]:
    resolved: list[ResolvedReference] = []
    current_article_id = ""
    current_paragraph_id = ""
    current_item_id = ""
    current_sub_item_id = ""
    current_article_label = ""
    current_paragraph_label = ""
    current_item_label = ""
    current_sub_item_label = ""

    if candidates:
        source_node_id = candidates[0].source_node_id
        source_node = node_index[source_node_id]
        current_article_id = source_node_id if source_node.level == "article" else ancestor_at_level(
            source_node_id, "article", node_index, parent_by_child
        )
        current_paragraph_id = source_node_id if source_node.level == "paragraph" else ancestor_at_level(
            source_node_id, "paragraph", node_index, parent_by_child
        )
        current_item_id = source_node_id if source_node.level == "item" else ancestor_at_level(
            source_node_id, "item", node_index, parent_by_child
        )
        current_sub_item_id = source_node_id if source_node.level == "sub_item" else ancestor_at_level(
            source_node_id, "sub_item", node_index, parent_by_child
        )
        if current_article_id:
            current_article_label = node_index[current_article_id].name
        if current_paragraph_id:
            current_paragraph_label = normalize_sub_label(tail_label(node_index[current_paragraph_id].name, "款"))
        if current_item_id:
            current_item_label = normalize_sub_label(tail_label(node_index[current_item_id].name, "项"))
        if current_sub_item_id:
            current_sub_item_label = normalize_sub_label(tail_label(node_index[current_sub_item_id].name, "目"))

    current_context = {
        "article_id": current_article_id,
        "paragraph_id": current_paragraph_id,
        "item_id": current_item_id,
        "sub_item_id": current_sub_item_id,
        "article_label": current_article_label,
        "paragraph_label": current_paragraph_label,
        "item_label": current_item_label,
        "sub_item_label": current_sub_item_label,
    }

    for candidate in candidates:
        target_document_ids = resolve_target_document_ids(candidate, title_to_document_ids, current_document_id)
        if not target_document_ids and allow_unresolved_targets:
            target_document_ids = [""]
        descriptors = expand_candidate_targets(candidate, current_context)
        for document_id in target_document_ids:
            for descriptor in descriptors:
                target_node_id = resolve_target_node_id(
                    descriptor,
                    candidate,
                    document_id=document_id,
                    node_index=node_index,
                    parent_by_child=parent_by_child,
                    provision_index=provision_index,
                    children_by_parent_level=children_by_parent_level,
                    current_context=current_context,
                )
                resolved.append(
                    ResolvedReference(
                        candidate=candidate,
                        target_node_id=target_node_id,
                        target_ref_text=render_target_ref_text(candidate, descriptor),
                        target_span_start=descriptor.span_start if descriptor.span_start >= 0 else candidate.span_start,
                        target_span_end=descriptor.span_end if descriptor.span_end >= 0 else candidate.span_end,
                    )
                )
    return dedupe_resolved_references(resolved)


def resolve_target_document_ids(
    candidate: ReferenceCandidate,
    title_to_document_ids: dict[str, list[str]],
    current_document_id: str,
) -> list[str]:
    if candidate.kind in {"absolute_article", "alias_article", "bare_article", "shared_document_article"}:
        return title_to_document_ids.get(candidate.document_title, [])
    return [current_document_id]


def expand_candidate_targets(candidate: ReferenceCandidate, current_context: dict[str, str]) -> list[TargetDescriptor]:
    if candidate.kind == "this_article":
        return [TargetDescriptor(article_label=current_context["article_label"], span_start=candidate.span_start, span_end=candidate.span_end, display_text=candidate.target_ref_text)]
    if candidate.kind == "this_paragraph":
        return [TargetDescriptor(article_label=current_context["article_label"], paragraph_label=current_context["paragraph_label"], span_start=candidate.span_start, span_end=candidate.span_end, display_text=candidate.target_ref_text)]
    if candidate.kind == "previous_paragraph":
        return [TargetDescriptor(article_label=current_context["article_label"], paragraph_label="__PREVIOUS__", span_start=candidate.span_start, span_end=candidate.span_end, display_text=candidate.target_ref_text)]
    if candidate.kind == "previous_paragraph_multiple":
        return expand_previous_multiple(candidate, current_context, unit="paragraph")
    if candidate.kind == "previous_item_multiple":
        return expand_previous_multiple(candidate, current_context, unit="item")

    base_text = candidate.matched_text or candidate.target_ref_text
    offset = candidate.span_start if candidate.span_start >= 0 else 0
    shared_item_descriptors = parse_shared_item_group(base_text, offset, current_context)
    if shared_item_descriptors:
        return shared_item_descriptors
    absolute_context = candidate.kind in {
        "absolute_article",
        "alias_article",
        "bare_article",
        "shared_document_article",
    }
    descriptors = parse_reference_text(
        base_text,
        offset,
        article_label="" if absolute_context else current_context["article_label"],
        paragraph_label="" if absolute_context else current_context["paragraph_label"],
        item_label="" if absolute_context else current_context["item_label"],
        sub_item_label="" if absolute_context else current_context["sub_item_label"],
        relative_kind=candidate.kind,
    )
    return descriptors or [
        TargetDescriptor(
            article_label=current_context["article_label"],
            paragraph_label=current_context["paragraph_label"] if "paragraph" in candidate.kind else "",
            item_label=current_context["item_label"] if "item" in candidate.kind else "",
            sub_item_label=current_context["sub_item_label"] if "sub_item" in candidate.kind else "",
            span_start=candidate.span_start,
            span_end=candidate.span_end,
            display_text=base_text,
        )
    ]


def expand_previous_multiple(candidate: ReferenceCandidate, current_context: dict[str, str], unit: str) -> list[TargetDescriptor]:
    count_match = re.search(r"前(?P<count>[一二三四五六七八九十百千万零两〇0-9]+)", candidate.target_ref_text)
    if count_match is None:
        return []
    count = chinese_number_to_int(count_match.group("count"))
    if count <= 0:
        return []
    results: list[TargetDescriptor] = []
    if unit == "paragraph":
        current_value = numeral_value(current_context["paragraph_label"])
        if current_value is None:
            return []
        for value in range(max(1, current_value - count), current_value):
            results.append(
                TargetDescriptor(
                    article_label=current_context["article_label"],
                    paragraph_label=label_for_level("paragraph", value),
                    span_start=candidate.span_start,
                    span_end=candidate.span_end,
                    display_text=candidate.target_ref_text,
                )
            )
        return results
    current_value = numeral_value(current_context["item_label"])
    if current_value is None:
        return []
    for value in range(max(1, current_value - count), current_value):
        results.append(
            TargetDescriptor(
                article_label=current_context["article_label"],
                paragraph_label=current_context["paragraph_label"],
                item_label=label_for_level("item", value),
                span_start=candidate.span_start,
                span_end=candidate.span_end,
                display_text=candidate.target_ref_text,
            )
        )
    return results


def parse_shared_item_group(text: str, offset: int, current_context: dict[str, str]) -> list[TargetDescriptor]:
    stripped = strip_prefix_token(text)
    matched = re.fullmatch(
        r"(?:(?P<paragraph>第[一二三四五六七八九十百千万零两〇0-9]+款))?"
        r"(?P<first>第(?:[（(][一二三四五六七八九十百千万零两〇0-9]+[）)]|[一二三四五六七八九十百千万零两〇0-9]+))"
        r"(?P<rest>(?:(?:或者|以及|、|，|和|及|或)(?:第)?(?:[（(][一二三四五六七八九十百千万零两〇0-9]+[）)]|[一二三四五六七八九十百千万零两〇0-9]+))+)"
        r"项",
        stripped,
    )
    if matched is None:
        return []
    paragraph = normalize_sub_label(matched.group("paragraph")) or current_context["paragraph_label"]
    tokens = [
        matched.group("first"),
        *re.findall(r"(?:第)?(?:[（(][一二三四五六七八九十百千万零两〇0-9]+[）)]|[一二三四五六七八九十百千万零两〇0-9]+)", matched.group("rest")),
    ]
    results: list[TargetDescriptor] = []
    for index, token in enumerate(tokens):
        item_label = normalize_sub_label(f"{token if token.startswith('第') else f'第{token}'}项")
        if not item_label:
            continue
        display_text = f"{matched.group('paragraph') or ''}{token}项" if index == 0 else f"{token}项"
        results.append(
            TargetDescriptor(
                article_label=current_context["article_label"],
                paragraph_label=paragraph,
                item_label=item_label,
                span_start=offset,
                span_end=offset + len(text),
                display_text=display_text,
            )
        )
    return results


def parse_reference_text(
    text: str,
    offset: int,
    *,
    article_label: str,
    paragraph_label: str,
    item_label: str,
    sub_item_label: str,
    relative_kind: str,
) -> list[TargetDescriptor]:
    stripped = strip_prefix_token(text)
    normalized = normalize_reference_reference_text(stripped)
    start_shift = text.find(stripped) if stripped else 0
    matches = list(REF_SEGMENT_RE.finditer(normalized))
    if not matches:
        return []

    results: list[TargetDescriptor] = []
    previous: TargetDescriptor | None = None
    previous_end = 0
    for match in matches:
        connector = normalized[previous_end : match.start()]
        current = descriptor_from_match(
            match,
            article_label=article_label,
            paragraph_label=paragraph_label,
            item_label=item_label,
            sub_item_label=sub_item_label,
            relative_kind=relative_kind,
            span_start=offset + start_shift,
            span_end=offset + start_shift + len(stripped),
            display_text=text[start_shift : start_shift + len(stripped)],
        )
        if previous is not None and "至" in connector:
            results.extend(expand_range(previous, current))
        else:
            results.append(current)
        previous = current
        previous_end = match.end()

    expanded: list[TargetDescriptor] = []
    prior: TargetDescriptor | None = None
    for descriptor in results:
        if not descriptor.is_range:
            descriptor = inherit_descriptor(prior, descriptor)
        expanded.append(descriptor)
        prior = descriptor
    return expanded


def normalize_reference_reference_text(text: str) -> str:
    cleaned = NON_TARGET_PAREN_RE.sub("", text)
    return expand_parallel_item_groups(cleaned)


def expand_parallel_item_groups(text: str) -> str:
    def replace(match: re.Match[str]) -> str:
        prefix = match.group("prefix") or ""
        tokens = [
            match.group("first"),
            *re.findall(
                r"(?:第)?(?:[（(][一二三四五六七八九十百千万零两〇0-9]+[）)]|[一二三四五六七八九十百千万零两〇0-9]+)",
                match.group("rest"),
            ),
        ]
        expanded = "、".join(
            f"{token if token.startswith('第') else f'第{token}'}项"
            for token in tokens
        )
        return f"{prefix}{expanded}"

    return PARALLEL_ITEM_GROUP_RE.sub(replace, text)


def descriptor_from_match(
    match: re.Match[str],
    *,
    article_label: str,
    paragraph_label: str,
    item_label: str,
    sub_item_label: str,
    relative_kind: str,
    span_start: int,
    span_end: int,
    display_text: str,
) -> TargetDescriptor:
    if match.group("article"):
        article = f"第{normalize_numeral(match.group('article'))}条"
        if match.group("article_suffix"):
            article = f"{article}之{normalize_numeral(match.group('article_suffix'))}"
        return TargetDescriptor(
            article_label=article,
            paragraph_label=normalize_sub_label(match.group("paragraph")),
            item_label=normalize_sub_label(match.group("item")),
            sub_item_label=normalize_sub_label(match.group("sub_item")),
            span_start=span_start,
            span_end=span_end,
            display_text=display_text,
        )
    if match.group("paragraph_only"):
        return TargetDescriptor(
            article_label=article_label,
            paragraph_label=normalize_sub_label(match.group("paragraph_only")),
            item_label=normalize_sub_label(match.group("paragraph_item")),
            sub_item_label=normalize_sub_label(match.group("paragraph_sub_item")),
            span_start=span_start,
            span_end=span_end,
            display_text=display_text,
        )
    if match.group("item_only"):
        base_paragraph = paragraph_label if "paragraph" in relative_kind or relative_kind == "standalone_detail" else ""
        return TargetDescriptor(
            article_label=article_label,
            paragraph_label=base_paragraph,
            item_label=normalize_sub_label(match.group("item_only")),
            sub_item_label=normalize_sub_label(match.group("item_sub_item")),
            span_start=span_start,
            span_end=span_end,
            display_text=display_text,
        )
    return TargetDescriptor(
        article_label=article_label,
        paragraph_label=paragraph_label if "paragraph" in relative_kind or relative_kind == "standalone_detail" else "",
        item_label=item_label if "item" in relative_kind else "",
        sub_item_label=normalize_sub_label(match.group("sub_item_only")),
        span_start=span_start,
        span_end=span_end,
        display_text=display_text,
    )


def inherit_descriptor(previous: TargetDescriptor | None, current: TargetDescriptor) -> TargetDescriptor:
    if previous is None:
        return current
    if current.article_label and current.article_label != previous.article_label:
        return current
    return TargetDescriptor(
        article_label=current.article_label or previous.article_label,
        paragraph_label=current.paragraph_label or previous.paragraph_label,
        item_label=current.item_label or (previous.item_label if current.sub_item_label else ""),
        sub_item_label=current.sub_item_label,
        span_start=current.span_start,
        span_end=current.span_end,
        display_text=current.display_text,
        is_range=current.is_range,
    )


def expand_range(start: TargetDescriptor, end: TargetDescriptor, text: str = "") -> list[TargetDescriptor]:
    level = range_level(start, end)
    if not level:
        return [start, end]
    start_value = descriptor_value(start, level)
    end_value = descriptor_value(end, level)
    if start_value is None or end_value is None or end_value < start_value:
        return [start, end]
    display_text = text or f"{start.display_text}至{end.display_text}"
    items: list[TargetDescriptor] = []
    for value in range(start_value, end_value + 1):
        label = label_for_level(level, value)
        items.append(
            TargetDescriptor(
                article_label=start.article_label if level != "article" else label,
                paragraph_label=label if level == "paragraph" else start.paragraph_label,
                item_label=label if level == "item" else start.item_label,
                sub_item_label=label if level == "sub_item" else start.sub_item_label,
                span_start=start.span_start,
                span_end=end.span_end,
                display_text=display_text,
                is_range=True,
            )
        )
    return items


def range_level(start: TargetDescriptor, end: TargetDescriptor) -> str:
    for level in reversed(LEVEL_ORDER):
        if descriptor_value(start, level) is not None and descriptor_value(end, level) is not None:
            if shared_parent(start, end, level):
                return level
    return ""


def shared_parent(start: TargetDescriptor, end: TargetDescriptor, level: str) -> bool:
    if level == "article":
        return True
    if level == "paragraph":
        return start.article_label == end.article_label
    if level == "item":
        return start.article_label == end.article_label and start.paragraph_label == end.paragraph_label
    return (
        start.article_label == end.article_label
        and start.paragraph_label == end.paragraph_label
        and start.item_label == end.item_label
    )


def descriptor_value(descriptor: TargetDescriptor, level: str) -> int | None:
    label = {
        "article": descriptor.article_label,
        "paragraph": descriptor.paragraph_label,
        "item": descriptor.item_label,
        "sub_item": descriptor.sub_item_label,
    }[level]
    if not label:
        return None
    return numeral_value(label)


def resolve_target_node_id(
    descriptor: TargetDescriptor,
    candidate: ReferenceCandidate,
    *,
    document_id: str,
    node_index: dict[str, object],
    parent_by_child: dict[str, str],
    provision_index: dict[str, dict[tuple[str, str, str, str], str]],
    children_by_parent_level: dict[tuple[str, str], list[str]],
    current_context: dict[str, str],
) -> str:
    if descriptor.paragraph_label == "__PREVIOUS__":
        if not current_context["paragraph_id"]:
            return ""
        return previous_sibling(
            current_context["paragraph_id"],
            parent_by_child.get(current_context["paragraph_id"], ""),
            node_index,
            parent_by_child,
        )

    key = (
        descriptor.article_label,
        descriptor.paragraph_label,
        descriptor.item_label,
        descriptor.sub_item_label,
    )
    target_node_id = lookup_provision_node_id(provision_index.get(document_id, {}), key)
    if target_node_id:
        return target_node_id

    if candidate.kind == "this_article":
        return current_context["article_id"]
    if candidate.kind == "this_paragraph":
        return current_context["paragraph_id"]

    return resolve_exact_child_path(
        descriptor,
        document_id=document_id,
        node_index=node_index,
        provision_index=provision_index,
        children_by_parent_level=children_by_parent_level,
    )


def resolve_exact_child_path(
    descriptor: TargetDescriptor,
    *,
    document_id: str,
    node_index: dict[str, object],
    provision_index: dict[str, dict[tuple[str, str, str, str], str]],
    children_by_parent_level: dict[tuple[str, str], list[str]],
) -> str:
    article_id = lookup_provision_node_id(
        provision_index.get(document_id, {}),
        (descriptor.article_label, "", "", ""),
    )
    if not article_id:
        return ""
    if not descriptor.paragraph_label and not descriptor.item_label:
        return article_id
    if descriptor.item_label and not descriptor.paragraph_label:
        item_id = next(
            (
                child_id
                for child_id in children_by_parent_level.get((article_id, "item"), [])
                if normalize_sub_label(tail_label(node_index[child_id].name, "项")) == descriptor.item_label
            ),
            "",
        )
        if not item_id:
            return ""
        if not descriptor.sub_item_label:
            return item_id
        return next(
            (
                child_id
                for child_id in children_by_parent_level.get((item_id, "sub_item"), [])
                if normalize_sub_label(tail_label(node_index[child_id].name, "目")) == descriptor.sub_item_label
            ),
            "",
        )

    paragraph_id = next(
        (
            child_id
            for child_id in children_by_parent_level.get((article_id, "paragraph"), [])
            if normalize_sub_label(tail_label(node_index[child_id].name, "款")) == descriptor.paragraph_label
        ),
        "",
    )
    if not paragraph_id:
        return ""
    if not descriptor.item_label:
        return paragraph_id

    item_id = next(
        (
            child_id
            for child_id in children_by_parent_level.get((paragraph_id, "item"), [])
            if normalize_sub_label(tail_label(node_index[child_id].name, "项")) == descriptor.item_label
        ),
        "",
    )
    if not item_id:
        return ""
    if not descriptor.sub_item_label:
        return item_id

    return next(
        (
            child_id
            for child_id in children_by_parent_level.get((item_id, "sub_item"), [])
            if normalize_sub_label(tail_label(node_index[child_id].name, "目")) == descriptor.sub_item_label
        ),
        "",
    )


def dedupe_resolved_references(items: list[ResolvedReference]) -> list[ResolvedReference]:
    deduped: list[ResolvedReference] = []
    seen: set[tuple[str, str, str, int, int]] = set()
    for item in items:
        key = (
            item.candidate.source_node_id,
            item.target_node_id,
            item.target_ref_text,
            item.target_span_start,
            item.target_span_end,
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def lookup_provision_node_id(
    index: dict[tuple[str, str, str, str], str],
    key: tuple[str, str, str, str],
) -> str:
    candidates = [key]
    item_variants = alternate_label_forms(key[2], "项")
    sub_item_variants = alternate_label_forms(key[3], "目")
    if item_variants or sub_item_variants:
        for item_label in item_variants or [key[2]]:
            for sub_item_label in sub_item_variants or [key[3]]:
                variant = (key[0], key[1], item_label, sub_item_label)
                if variant not in candidates:
                    candidates.append(variant)
    for candidate in candidates:
        target_node_id = index.get(candidate, "")
        if target_node_id:
            return target_node_id
    return ""


def render_target_ref_text(candidate: ReferenceCandidate, descriptor: TargetDescriptor) -> str:
    prefix = reference_prefix(candidate)
    descriptor_text = canonical_descriptor_text(descriptor)
    if candidate.kind == "previous_paragraph_multiple":
        return candidate.target_ref_text
    if candidate.kind == "previous_item_multiple":
        return candidate.target_ref_text
    if candidate.kind in {"this_article", "this_article_detail"}:
        return f"本条{descriptor_text.removeprefix(descriptor.article_label)}" if descriptor.article_label else candidate.target_ref_text
    if candidate.kind in {"this_paragraph", "this_paragraph_detail"}:
        return f"本款{descriptor_text.removeprefix(descriptor.article_label).removeprefix(descriptor.paragraph_label)}" if descriptor.paragraph_label else candidate.target_ref_text
    if candidate.kind == "previous_paragraph":
        return "前款"
    if candidate.kind == "previous_paragraph_detail":
        suffix = descriptor_text.removeprefix(descriptor.article_label).removeprefix(descriptor.paragraph_label)
        return f"前款{suffix}"
    if prefix:
        return f"{prefix}{descriptor_text}"
    return descriptor_text or descriptor.display_text or candidate.target_ref_text


def reference_prefix(candidate: ReferenceCandidate) -> str:
    raw = candidate.matched_text or candidate.target_ref_text
    matched = REF_SEGMENT_RE.search(raw)
    if matched is None:
        return ""
    prefix = raw[: matched.start()].strip()
    return prefix


def canonical_descriptor_text(descriptor: TargetDescriptor) -> str:
    return "".join(
        part
        for part in (
            descriptor.article_label,
            descriptor.paragraph_label,
            descriptor.item_label,
            descriptor.sub_item_label,
        )
        if part and part != "__PREVIOUS__"
    )


def strip_prefix_token(text: str) -> str:
    return re.sub(r"^(?:依照|根据|按照|参照)?(?:《[^》]+》|本法|本条例|本办法|本规定|本决定|本解释|本条|本款|前款)", "", text).strip()


def normalize_sub_label(label: str | None) -> str:
    raw = (label or "").strip()
    if not raw:
        return ""
    suffix = raw[-1]
    return f"第{int_to_cn(numeral_value(raw) or 0)}{suffix}"


def alternate_label_forms(label: str, suffix: str) -> list[str]:
    if not label:
        return []
    value = numeral_value(label)
    if value is None:
        return [label]
    numeral = int_to_cn(value)
    variants = [label]
    parenthesized = f"第（{numeral}）{suffix}"
    plain = f"第{numeral}{suffix}"
    for variant in (parenthesized, plain):
        if variant not in variants:
            variants.append(variant)
    return variants


def normalize_numeral(value: str) -> str:
    return int_to_cn(chinese_number_to_int(value))


def numeral_value(label: str) -> int | None:
    matched = re.search(r"(?:第)?(?:[（(])?(?P<num>[一二三四五六七八九十百千万零两〇0-9]+)(?:[）)])?[条款项目]", label)
    if not matched:
        return None
    return chinese_number_to_int(matched.group("num"))


def label_for_level(level: str, value: int) -> str:
    suffix = {"article": "条", "paragraph": "款", "item": "项", "sub_item": "目"}[level]
    numeral = int_to_cn(value)
    if level in {"item", "sub_item"}:
        return f"第（{numeral}）{suffix}"
    return f"第{numeral}{suffix}"
