from __future__ import annotations

from pathlib import Path
from typing import Any

from ...contracts import LogicalDocumentRecord, NormalizeIndexEntry, NormalizedDocumentRecord
from ...io import BuildLayout, read_source_document, split_logical_documents, write_normalized_document
from ...utils.document_layout import clean_text, merge_structural_heading_continuations
from .metadata_loader import match_document_path, normalize_match_key
from .partition import build_document_unit


def process_metadata_item(
    metadata: dict[str, Any],
    document_index: dict[str, Path],
    layout: BuildLayout,
) -> NormalizeIndexEntry:
    source_id = str(metadata.get("source_id", "")).strip()
    title = str(metadata.get("title", "")).strip()
    if not source_id:
        return NormalizeIndexEntry(
            source_id="",
            status="failed",
            title=title,
            error_type="invalid_metadata",
            message="Metadata entry is missing source_id.",
        )
    if not title:
        return NormalizeIndexEntry(
            source_id=source_id,
            status="failed",
            error_type="invalid_metadata",
            message="Metadata entry is missing title.",
        )

    matched_document = match_document_path(title, document_index)
    if matched_document is None:
        return NormalizeIndexEntry(
            source_id=source_id,
            status="failed",
            title=title,
            error_type="missing_document",
            message=f"Document not found for metadata title: {title}",
        )

    try:
        physical_source = read_source_document(matched_document, sidecar_metadata=metadata)
        logical_documents = split_logical_documents(physical_source)
        selected_document, selection_details = choose_primary_document(logical_documents, metadata_title=title)
        if selected_document is None:
            return NormalizeIndexEntry(
                source_id=source_id,
                status="failed",
                title=title,
                document_path=str(matched_document),
                error_type="empty_document",
                message="No substantive logical document could be selected.",
            )
        normalized_document = build_normalized_document(source_id, metadata, selected_document)
        if not normalized_document.content.strip() and not normalized_document.appendix_lines:
            return NormalizeIndexEntry(
                source_id=source_id,
                status="failed",
                title=selected_document.title,
                document_path=str(matched_document),
                error_type="empty_content",
                message="Selected logical document has no usable content.",
                details=selection_details,
            )
        artifact_path = layout.normalize_document_path(source_id)
        write_normalized_document(artifact_path, normalized_document)
        return NormalizeIndexEntry(
            source_id=source_id,
            status="completed",
            title=normalized_document.title,
            document_path=str(matched_document),
            artifact_path=str(artifact_path),
            details=selection_details,
        )
    except Exception as exc:
        return NormalizeIndexEntry(
            source_id=source_id,
            status="failed",
            title=title,
            document_path=str(matched_document),
            error_type=exc.__class__.__name__,
            message=str(exc),
        )


def build_normalized_document(
    source_id: str,
    original_metadata: dict[str, Any],
    selected_document: LogicalDocumentRecord,
) -> NormalizedDocumentRecord:
    cleaned_document = clean_logical_document(selected_document, source_id=source_id)
    unit = build_document_unit(cleaned_document)
    metadata = {
        key: value
        for key, value in original_metadata.items()
        if key not in {"source_format", "source_id", "title", "content", "appendix_lines"}
    }
    return NormalizedDocumentRecord(
        source_id=source_id,
        title=unit.title,
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


def choose_primary_document(
    logical_documents: list[LogicalDocumentRecord],
    *,
    metadata_title: str,
) -> tuple[LogicalDocumentRecord | None, dict[str, Any]]:
    if not logical_documents:
        return None, {"selection": "none"}
    if len(logical_documents) == 1:
        return logical_documents[0], {"selection": "single"}

    metadata_key = normalize_match_key(metadata_title)
    exact_matches = [document for document in logical_documents if normalize_match_key(document.title) == metadata_key]
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
