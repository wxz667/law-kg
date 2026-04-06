from __future__ import annotations

from .patterns import ABSOLUTE_ARTICLE_RE, LOCAL_ARTICLE_RE, PREVIOUS_PARAGRAPH_RE, SENTENCE_SPLIT_RE, THIS_ARTICLE_RE, THIS_PARAGRAPH_RE
from .types import ReferenceCandidate


def split_sentences(text: str) -> list[str]:
    return [item.strip() for item in SENTENCE_SPLIT_RE.split(text) if item.strip()]


def extract_candidates(sentence: str, source_node_id: str, current_document_title: str) -> list[ReferenceCandidate]:
    candidates: list[ReferenceCandidate] = []

    for match in ABSOLUTE_ARTICLE_RE.finditer(sentence):
        full_title = f"《{match.group('title')}》"
        article_label = f"第{match.group('article')}"
        candidates.append(
            ReferenceCandidate(
                source_node_id=source_node_id,
                evidence_text=sentence,
                target_ref_text=f"{full_title}{article_label}",
                kind="absolute_article",
                article_label=article_label,
                document_title=full_title,
            )
        )

    for match in LOCAL_ARTICLE_RE.finditer(sentence):
        article_label = f"第{match.group('article')}"
        candidates.append(
            ReferenceCandidate(
                source_node_id=source_node_id,
                evidence_text=sentence,
                target_ref_text=f"{current_document_title}{article_label}",
                kind="local_article",
                article_label=article_label,
                document_title=current_document_title,
            )
        )

    if THIS_ARTICLE_RE.search(sentence):
        candidates.append(
            ReferenceCandidate(
                source_node_id=source_node_id,
                evidence_text=sentence,
                target_ref_text="本条",
                kind="this_article",
            )
        )
    if THIS_PARAGRAPH_RE.search(sentence):
        candidates.append(
            ReferenceCandidate(
                source_node_id=source_node_id,
                evidence_text=sentence,
                target_ref_text="本款",
                kind="this_paragraph",
            )
        )
    if PREVIOUS_PARAGRAPH_RE.search(sentence):
        candidates.append(
            ReferenceCandidate(
                source_node_id=source_node_id,
                evidence_text=sentence,
                target_ref_text="前款",
                kind="previous_paragraph",
            )
        )
    return candidates
