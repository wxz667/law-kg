from __future__ import annotations

import re

from ...contracts import ExtractConceptItem, ExtractConceptRecord

GENERIC_LAW_SUFFIXES = (
    "一般规定",
    "基本规定",
    "定义",
    "制度",
    "法律",
    "规则",
    "主体",
    "内容",
    "机制",
)

_LAW_TITLE_GENERIC_PATTERN = re.compile(r"^.+法(?:一般规定|基本规定)$")
_TRIM_TAIL_PATTERN = re.compile(r"[：:、，,；;（）()\[\]【】\s]+$")
_TRIM_HEAD_PATTERN = re.compile(r"^[：:、，,；;（）()\[\]【】\s]+")
_DESCRIPTION_FILLER_PATTERNS = (
    re.compile(r"^(?:是指|系指|指的是?|表示|定义为|定义成|即为|即|是|指)\s*"),
    re.compile(r"^(?:一种|一类|一个|一项)\s*"),
)


def postprocess_extract_concepts(records: list[ExtractConceptRecord]) -> list[ExtractConceptRecord]:
    return [
        ExtractConceptRecord(id=record.id, concepts=postprocess_concept_items(record.concepts))
        for record in records
    ]


def postprocess_concept_items(concepts: list[ExtractConceptItem]) -> list[ExtractConceptItem]:
    normalized: list[ExtractConceptItem] = []
    seen: set[str] = set()
    for item in concepts:
        cleaned = postprocess_concept_item(item)
        if cleaned is None or cleaned.name in seen:
            continue
        seen.add(cleaned.name)
        normalized.append(cleaned)
    return normalized


def postprocess_concept_item(item: ExtractConceptItem) -> ExtractConceptItem | None:
    cleaned_name = postprocess_concept_name(item.name)
    if not cleaned_name:
        return None
    cleaned_description = postprocess_description(item.description, cleaned_name)
    if not cleaned_description:
        return None
    return ExtractConceptItem(name=cleaned_name, description=cleaned_description)


def postprocess_concept_name(concept: str) -> str | None:
    text = normalize_text(concept)
    if not text:
        return None
    if text in GENERIC_LAW_SUFFIXES:
        return None
    if _LAW_TITLE_GENERIC_PATTERN.fullmatch(text):
        return None
    for suffix in GENERIC_LAW_SUFFIXES:
        if not text.endswith(suffix):
            continue
        stem = normalize_text(_trim_affixes(text[: -len(suffix)]))
        if not stem:
            return None
        if _LAW_TITLE_GENERIC_PATTERN.fullmatch(stem + suffix):
            return None
        return stem
    return text


def postprocess_description(description: str, concept_name: str) -> str | None:
    text = normalize_text(description)
    if not text:
        return None
    text = _trim_affixes(text)
    text = _strip_named_filler_prefix(text, concept_name)
    for pattern in _DESCRIPTION_FILLER_PATTERNS:
        previous = text
        text = pattern.sub("", text).strip()
        if text != previous:
            text = _trim_affixes(text)
    text = normalize_text(text)
    return text or None


def _strip_named_filler_prefix(text: str, concept_name: str) -> str:
    escaped_name = re.escape(normalize_text(concept_name))
    patterns = (rf"^(?:{escaped_name})[，,：:\s]*(?:是指|系指|指的是?|表示|定义为|定义成|即为|即|是|指)\s*",)
    normalized = text
    for pattern in patterns:
        normalized = re.sub(pattern, "", normalized).strip()
    return normalized


def _trim_affixes(text: str) -> str:
    return _TRIM_HEAD_PATTERN.sub("", _TRIM_TAIL_PATTERN.sub("", text))


def normalize_text(text: str) -> str:
    return " ".join(str(text).split())
