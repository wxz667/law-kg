from __future__ import annotations

from pathlib import Path
from typing import Any

from ...contracts import NormalizeIndexEntry
from ...io import BuildLayout, read_source_document, split_logical_documents, write_normalized_document
from .document import build_normalized_document
from .metadata import match_document_path
from .selection import choose_primary_document


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
