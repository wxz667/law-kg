from __future__ import annotations

import re

from ...contracts import ReferenceCandidateRecord
from ...utils.reference import CATEGORY_RANK

TITLE_INTERPRET_PATTERNS = (
    re.compile(r"关于\[T\].+?\[/T\]的解释$"),
    re.compile(r"关于\[T\].+?\[/T\]适用问题的解释$"),
    re.compile(r"关于适用\[T\].+?\[/T\]的解释$"),
    re.compile(r"关于办理.+?\[T\].+?\[/T\].+?(?:解释|规定|批复|答复)$"),
)


def is_title_level_candidate(candidate: ReferenceCandidateRecord) -> bool:
    return str(candidate.source_node_id).startswith("document:")


def is_obvious_title_interpretation(candidate: ReferenceCandidateRecord) -> bool:
    if not is_title_level_candidate(candidate):
        return False
    text = str(candidate.text or "").strip()
    return any(pattern.search(text) is not None for pattern in TITLE_INTERPRET_PATTERNS)


def allows_same_or_upper_level_relation(
    *,
    source_category: str,
    target_category: str,
) -> bool:
    source_rank = CATEGORY_RANK.get(source_category)
    target_rank = CATEGORY_RANK.get(target_category)
    if source_rank is None or target_rank is None:
        return False
    return target_rank <= source_rank


def correct_relation_type(
    relation_type: str,
    *,
    source_category: str,
    target_category: str,
) -> tuple[str, bool]:
    if relation_type == "INTERPRETS" and not allows_same_or_upper_level_relation(
        source_category=source_category,
        target_category=target_category,
    ):
        return "REFERENCES", True
    if relation_type == "REFERENCES" and not allows_same_or_upper_level_relation(
        source_category=source_category,
        target_category=target_category,
    ):
        return "INTERPRETS", True
    return relation_type, False


def correct_candidate_relation(
    candidate: ReferenceCandidateRecord,
    *,
    relation_type: str,
    source_category: str,
    target_categories: list[str],
) -> tuple[str, bool, str | None]:
    corrected_relation = relation_type
    corrected = False
    correction_source: str | None = None

    if relation_type == "REFERENCES" and is_obvious_title_interpretation(candidate):
        corrected_relation = "INTERPRETS"
        corrected = True
        correction_source = "rule_corrected_title_model"

    for target_category in target_categories[: len(candidate.target_node_ids)]:
        updated_relation, changed = correct_relation_type(
            corrected_relation,
            source_category=source_category,
            target_category=target_category,
        )
        if changed:
            corrected_relation = updated_relation
            corrected = True
            correction_source = f"rule_corrected_{'title_' if correction_source == 'rule_corrected_title_model' else ''}model"
    return corrected_relation, corrected, correction_source
