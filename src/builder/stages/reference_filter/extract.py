from __future__ import annotations

import re
from functools import lru_cache

from .patterns import (
    ABSOLUTE_ARTICLE_RE,
    ALIAS_ARTICLE_TEMPLATE,
    BARE_ARTICLE_RE,
    CONTINUATION_TARGET_RE,
    LOCAL_ARTICLE_RE,
    NON_TARGET_PAREN_RE,
    PARALLEL_ITEM_GROUP_RE,
    SHARED_DOCUMENT_GAP_RE,
    SHARED_DOCUMENT_ARTICLE_RE,
    SHARED_ITEM_GROUP_RE,
)
from .types import ReferenceCandidate

BOUNDARY_CHARS = {"。", "；", ";", "!", "！", "?", "？"}
LOCAL_REFERENCE_TOKENS = ("本法", "本条例", "本办法", "本规定", "本决定", "本解释")
CONNECTOR_TOKENS = ("或者", "以及", "、", "，", "和", "及", "或", "至", "-", "—")
DOCUMENT_ANCHOR_KINDS = {"absolute_article", "alias_article", "bare_article", "shared_document_article"}


def split_sentences(text: str) -> list[str]:
    sentences: list[str] = []
    buffer: list[str] = []
    quote_stack: list[str] = []
    closing_quotes = {"“": "”", "‘": "’", '"': '"', "《": "》"}

    for char in text:
        buffer.append(char)
        if char in closing_quotes:
            expected_close = closing_quotes[char]
            if expected_close == '"' and quote_stack and quote_stack[-1] == '"':
                quote_stack.pop()
            else:
                quote_stack.append(expected_close)
            continue
        if quote_stack and char == quote_stack[-1]:
            quote_stack.pop()
            continue
        if char in BOUNDARY_CHARS and not quote_stack:
            sentence = "".join(buffer).strip()
            if sentence:
                sentences.append(sentence)
            buffer = []

    trailing = "".join(buffer).strip()
    if trailing:
        sentences.append(trailing)
    return sentences


def extract_candidates(
    sentence: str,
    source_node_id: str,
    current_document_title: str,
    aliases: dict[str, str] | None = None,
    alias_items: list[tuple[str, str]] | None = None,
    evidence_text: str | None = None,
    quote_ranges: list[tuple[int, int]] | None = None,
) -> list[ReferenceCandidate]:
    candidates: list[ReferenceCandidate] = []
    quote_ranges = quote_ranges if quote_ranges is not None else build_quote_ranges(sentence)
    occupied: list[tuple[int, int]] = []
    candidate_evidence_text = evidence_text if evidence_text is not None else sentence
    has_article_marker = "第" in sentence and "条" in sentence
    has_bracket_title = "《" in sentence and "》" in sentence and has_article_marker
    has_local_reference = has_article_marker and any(token in sentence for token in LOCAL_REFERENCE_TOKENS)
    has_shared_connector = has_article_marker and any(token in sentence for token in CONNECTOR_TOKENS)

    def try_add(candidate: ReferenceCandidate) -> None:
        if candidate.span_start < 0 or candidate.span_end <= candidate.span_start:
            return
        if span_in_quotes(candidate.span_start, candidate.span_end, quote_ranges):
            return
        if overlaps_existing(candidate.span_start, candidate.span_end, occupied):
            return
        occupied.append((candidate.span_start, candidate.span_end))
        candidates.append(candidate)

    if has_bracket_title:
        for match in ABSOLUTE_ARTICLE_RE.finditer(sentence):
            full_title = f"《{match.group('title')}》"
            target_end = expand_tail_end(sentence, match.end("ref"))
            try_add(
                build_reference_candidate(
                    sentence=sentence,
                    evidence_text=candidate_evidence_text,
                    source_node_id=source_node_id,
                    document_title=full_title,
                    kind="absolute_article",
                    target_start=match.start("title") - 1,
                    target_end=target_end,
                    matched_text=sentence[match.start("title") - 1 : target_end],
                    doc_token="",
                )
            )

    alias_map = aliases or {}
    relevant_aliases = alias_items or sorted(alias_map.items(), key=lambda item: len(item[0]), reverse=True)
    if has_article_marker and relevant_aliases:
        for alias, full_title in relevant_aliases:
            pattern = compile_alias_pattern(alias)
            for match in pattern.finditer(sentence):
                target_end = expand_tail_end(sentence, match.end("ref"))
                try_add(
                    build_reference_candidate(
                        sentence=sentence,
                        evidence_text=candidate_evidence_text,
                        source_node_id=source_node_id,
                        document_title=full_title,
                        kind="alias_article",
                        target_start=match.start("alias"),
                        target_end=target_end,
                        matched_text=sentence[match.start("alias") : target_end],
                        doc_token=alias,
                        alias_title=full_title,
                    )
                )

    if has_article_marker:
        for match in BARE_ARTICLE_RE.finditer(sentence):
            bare_title = match.group("title")
            if bare_title.startswith(("根据", "依照", "按照", "参照")):
                continue
            if bare_title in LOCAL_REFERENCE_TOKENS:
                continue
            full_title = alias_map.get(bare_title, f"《{bare_title}》")
            try_add(
                build_reference_candidate(
                    sentence=sentence,
                    evidence_text=candidate_evidence_text,
                    source_node_id=source_node_id,
                    document_title=full_title,
                    kind="bare_article",
                    target_start=match.start("title"),
                    target_end=expand_tail_end(sentence, match.end("ref")),
                    matched_text=sentence[match.start("title") : expand_tail_end(sentence, match.end("ref"))],
                    doc_token=bare_title,
                    alias_title=full_title,
                )
            )

    if has_local_reference:
        for match in LOCAL_ARTICLE_RE.finditer(sentence):
            try_add(
                build_reference_candidate(
                    sentence=sentence,
                    evidence_text=candidate_evidence_text,
                    source_node_id=source_node_id,
                    document_title=current_document_title,
                    kind="local_article",
                    target_start=match.start("doc_token"),
                    target_end=expand_tail_end(sentence, match.end("ref")),
                    matched_text=sentence[match.start("doc_token") : expand_tail_end(sentence, match.end("ref"))],
                    doc_token=match.group("doc_token"),
                )
            )

    if has_shared_connector:
        for match in SHARED_DOCUMENT_ARTICLE_RE.finditer(sentence):
            if overlaps_existing(match.start("ref"), match.end("tail") or match.end("ref"), occupied):
                continue
            anchor = nearest_document_anchor(candidates, match.start("ref"))
            if anchor is None:
                continue
            if not has_shared_document_gap(sentence[anchor.span_end : match.start("ref")]):
                continue
            target_end = match.end("tail") if match.group("tail") else match.end("ref")
            try_add(
                build_reference_candidate(
                    sentence=sentence,
                    evidence_text=candidate_evidence_text,
                    source_node_id=source_node_id,
                    document_title=anchor.document_title,
                    kind="shared_document_article",
                    target_start=match.start("ref"),
                    target_end=target_end,
                    matched_text=sentence[match.start("ref") : target_end],
                    doc_token=anchor.doc_token,
                    alias_title=anchor.alias_title,
                )
            )

    return sorted(candidates, key=lambda item: (item.span_start, item.span_end))


def relevant_alias_items(
    sentence: str,
    *,
    local_groups: dict[str, list[tuple[str, str]]] | None = None,
    global_groups: dict[str, list[tuple[str, str]]] | None = None,
) -> list[tuple[str, str]]:
    sentence_chars = {char for char in sentence if char.strip()}
    seen: set[str] = set()
    items: list[tuple[str, str]] = []
    for groups in (local_groups or {}, global_groups or {}):
        for char in sentence_chars:
            for alias, full_title in groups.get(char, ()):
                if alias in seen:
                    continue
                if alias in sentence:
                    items.append((alias, full_title))
                    seen.add(alias)
    items.sort(key=lambda item: len(item[0]), reverse=True)
    return items


def sentence_may_contain_reference(sentence: str) -> bool:
    if re.search(r"第[一二三四五六七八九十百千万零两〇0-9（()）]{1,16}(?:条|款|项|目)", sentence):
        return True
    if any(marker in sentence for marker in ("《", "本法", "本条例", "本办法", "本规定", "本决定", "本解释")):
        return True
    return False


def build_reference_candidate(
    *,
    sentence: str,
    evidence_text: str,
    source_node_id: str,
    document_title: str,
    kind: str,
    target_start: int,
    target_end: int,
    matched_text: str,
    doc_token: str = "",
    alias_title: str = "",
) -> ReferenceCandidate:
    return ReferenceCandidate(
        source_node_id=source_node_id,
        evidence_text=evidence_text,
        target_ref_text=matched_text,
        kind=kind,
        document_title=document_title,
        matched_text=matched_text,
        span_start=target_start,
        span_end=target_end,
        has_multiple_targets=contains_multi_target_connector(matched_text),
        doc_token=doc_token,
        alias_title=alias_title,
    )


@lru_cache(maxsize=2048)
def compile_alias_pattern(alias: str) -> re.Pattern[str]:
    return re.compile(ALIAS_ARTICLE_TEMPLATE.format(alias=re.escape(alias)))


def build_relative_candidate(
    match: re.Match[str],
    sentence: str,
    evidence_text: str,
    source_node_id: str,
    target_ref_text: str,
    kind: str,
    *,
    end_override: int | None = None,
) -> ReferenceCandidate:
    span_end = end_override if end_override is not None else match.end()
    return ReferenceCandidate(
        source_node_id=source_node_id,
        evidence_text=evidence_text,
        target_ref_text=target_ref_text,
        kind=kind,
        matched_text=target_ref_text,
        span_start=match.start(),
        span_end=span_end,
        has_multiple_targets=contains_multi_target_connector(target_ref_text),
    )


def expand_tail_end(sentence: str, start_offset: int) -> int:
    cursor = start_offset
    direct_parallel = PARALLEL_ITEM_GROUP_RE.match(sentence[cursor:])
    if direct_parallel is not None:
        cursor += direct_parallel.end()
    while cursor < len(sentence):
        while True:
            gap_match = re.match(r"\s*", sentence[cursor:])
            cursor += gap_match.end() if gap_match is not None else 0
            interference_match = NON_TARGET_PAREN_RE.match(sentence[cursor:])
            if interference_match is None:
                break
            cursor += interference_match.end()
        matched = re.match(r"(?:或者|以及|、|，|和|及|或|至|-|—)\s*", sentence[cursor:])
        if not matched:
            break
        next_start = cursor + matched.end()
        while True:
            gap_match = re.match(r"\s*", sentence[next_start:])
            next_start += gap_match.end() if gap_match is not None else 0
            interference_match = NON_TARGET_PAREN_RE.match(sentence[next_start:])
            if interference_match is None:
                break
            next_start += interference_match.end()
        target_match = CONTINUATION_TARGET_RE.match(sentence[next_start:])
        if target_match is None:
            break
        cursor = next_start + target_match.end()
    return cursor


def contains_multi_target_connector(text: str) -> bool:
    return any(marker in text for marker in ("或者", "以及", "、", "，", "和", "及", "或", "至", "-", "—"))


def nearest_document_anchor(candidates: list[ReferenceCandidate], start: int) -> ReferenceCandidate | None:
    anchors = [
        candidate
        for candidate in candidates
        if candidate.kind in DOCUMENT_ANCHOR_KINDS
        and candidate.document_title
        and candidate.span_end <= start
    ]
    if not anchors:
        return None
    anchors.sort(key=lambda item: (item.span_end, item.span_start))
    return anchors[-1]


def has_shared_document_gap(text: str) -> bool:
    if not text.strip():
        return False
    return SHARED_DOCUMENT_GAP_RE.fullmatch(text) is not None


def extend_with_shared_item_group(sentence: str, start_offset: int) -> int:
    match = SHARED_ITEM_GROUP_RE.match(sentence, pos=start_offset)
    if match is None:
        return start_offset
    return match.end()


def overlaps_existing(start: int, end: int, spans: list[tuple[int, int]]) -> bool:
    return any(not (end <= span_start or start >= span_end) for span_start, span_end in spans)


def build_quote_ranges(text: str) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    stack: list[tuple[str, int]] = []
    quote_pairs = {"“": "”", "‘": "’", '"': '"', "《": "》"}

    for index, char in enumerate(text):
        if char in quote_pairs:
            close_char = quote_pairs[char]
            if close_char == '"' and stack and stack[-1][0] == '"':
                _, start = stack.pop()
                ranges.append((start, index + 1))
            else:
                stack.append((close_char, index))
            continue
        if stack and char == stack[-1][0]:
            _, start = stack.pop()
            ranges.append((start, index + 1))

    for _, start in stack:
        ranges.append((start, len(text)))
    return ranges


def span_in_quotes(start: int, end: int, quote_ranges: list[tuple[int, int]]) -> bool:
    return any(start >= quote_start and end <= quote_end for quote_start, quote_end in quote_ranges)
