from __future__ import annotations

import re

from ...contracts import ExtractConceptRecord

GENERIC_LAW_SUFFIXES = (
    "适用范围",
    "一般规定",
    "基本规定",
    "一般原则",
    "基本原则",
    "定义",
    "制度",
    "规则",
    "适用",
    "范围",
    "主体",
    "内容",
)

_LAW_TITLE_GENERIC_PATTERN = re.compile(r"^.+法(?:适用范围|一般规定|基本规定|基本原则)$")
_TRIM_TAIL_PATTERN = re.compile(r"[：:、，,；;（）()\[\]【】\s]+$")
_TRIM_HEAD_PATTERN = re.compile(r"^[：:、，,；;（）()\[\]【】\s]+")


def postprocess_extract_concepts(records: list[ExtractConceptRecord]) -> list[ExtractConceptRecord]:
    return [
        ExtractConceptRecord(id=record.id, concepts=postprocess_concept_list(record.concepts))
        for record in records
    ]


def postprocess_concept_list(concepts: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for concept in concepts:
        cleaned = postprocess_concept(concept)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)
    return normalized


def postprocess_concept(concept: str) -> str | None:
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


def _trim_affixes(text: str) -> str:
    return _TRIM_HEAD_PATTERN.sub("", _TRIM_TAIL_PATTERN.sub("", text))


def normalize_text(text: str) -> str:
    return " ".join(str(text).split())
