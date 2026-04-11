from __future__ import annotations

from typing import Any

from ...contracts import LogicalDocumentRecord
from ...utils.document_layout import compact_text_key


def choose_primary_document(
    logical_documents: list[LogicalDocumentRecord],
    *,
    metadata_title: str,
) -> tuple[LogicalDocumentRecord | None, dict[str, Any]]:
    if not logical_documents:
        return None, {"selection": "none"}
    if len(logical_documents) == 1:
        return logical_documents[0], {"selection": "single"}

    metadata_key = compact_text_key(metadata_title)
    exact_matches = [document for document in logical_documents if compact_text_key(document.title) == metadata_key]
    if len(exact_matches) == 1:
        return exact_matches[0], {"selection": "title_match"}
    if len(exact_matches) > 1:
        logical_documents = exact_matches

    best_document = max(logical_documents, key=document_length_score)
    return best_document, {
        "selection": "longest_content",
        "candidate_titles": [document.title for document in logical_documents],
    }


def document_length_score(document: LogicalDocumentRecord) -> tuple[int, int]:
    return (
        sum(len(line.strip()) for line in document.paragraphs if line.strip()),
        len(document.paragraphs),
    )
