from __future__ import annotations

from typing import Any

from ...contracts import LogicalDocumentRecord, NormalizedDocumentRecord
from ...utils.layout import clean_text, merge_structural_heading_continuations
from .partition import build_document_unit


def build_normalized_document(
    source_id: str,
    original_metadata: dict[str, Any],
    selected_document: LogicalDocumentRecord,
) -> NormalizedDocumentRecord:
    cleaned_document = clean_logical_document(selected_document, source_id=source_id)
    unit = build_document_unit(cleaned_document)
    metadata_title = clean_text(str(original_metadata.get("title", "")).strip())
    metadata = {
        key: value
        for key, value in original_metadata.items()
        if key not in {"source_format", "source_id", "title", "content", "appendix_lines"}
    }
    return NormalizedDocumentRecord(
        source_id=source_id,
        title=metadata_title or unit.title,
        content="\n".join(unit.body_lines),
        appendix_lines=list(unit.appendix_lines),
        metadata=metadata,
    )


def clean_logical_document(document: LogicalDocumentRecord, *, source_id: str) -> LogicalDocumentRecord:
    clean_body_lines = merge_structural_heading_continuations(
        [clean_text(line) for line in document.paragraphs if clean_text(line)]
    )
    clean_appendix_lines = [clean_text(line) for line in document.appendix_lines if clean_text(line)]
    return LogicalDocumentRecord(
        source_id=source_id,
        title=clean_text(document.title),
        source_type=document.source_type,
        paragraphs=clean_body_lines,
        appendix_lines=clean_appendix_lines,
        metadata=dict(document.metadata),
    )
