from __future__ import annotations

from pathlib import Path
from typing import Callable

from ...contracts import DocumentUnitRecord
from ...io import read_normalize_index, read_normalized_document


def load_document_units(
    index_path: Path,
    *,
    source_ids: list[str] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> list[DocumentUnitRecord]:
    normalize_index = read_normalize_index(index_path)
    units: list[DocumentUnitRecord] = []
    selected = set(source_ids) if source_ids is not None else None
    candidate_entries = [
        entry
        for entry in normalize_index.entries
        if entry.status == "completed"
        and entry.artifact_path
        and (selected is None or entry.source_id in selected)
    ]
    total = max(len(candidate_entries), 1)
    if progress_callback is not None:
        progress_callback(0, total)
    for index, entry in enumerate(candidate_entries, start=1):
        if entry.status != "completed" or not entry.artifact_path:
            continue
        document = read_normalized_document(Path(entry.artifact_path))
        metadata = dict(document.metadata)
        source_type = str(
            metadata.get("document_type")
            or metadata.get("source_type")
            or metadata.get("category")
            or ""
        )
        units.append(
            DocumentUnitRecord(
                source_id=document.source_id,
                title=document.title,
                source_type=source_type,
                body_lines=[line.strip() for line in document.content.splitlines() if line.strip()],
                appendix_lines=list(document.appendix_lines),
                metadata=metadata,
            )
        )
        if progress_callback is not None:
            progress_callback(index, total)
    return units
