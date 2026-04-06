from __future__ import annotations

from pathlib import Path
from typing import Callable

from ...contracts import NormalizeIndexEntry, NormalizeStageIndex
from ...io import BuildLayout, read_normalize_index, write_json, write_normalize_index
from .metadata_loader import build_document_index, load_metadata_items
from .normalizer import process_metadata_item


def run(
    data_root: Path,
    source_ids: list[str] | None = None,
    *,
    force_rebuild: bool = False,
    progress_callback: Callable[[int, int], None] | None = None,
) -> NormalizeStageIndex:
    data_root = data_root.resolve()
    layout = BuildLayout(data_root)
    metadata_items = load_metadata_items(data_root / "source" / "metadata")
    if source_ids is not None:
        selected = {source_id for source_id in source_ids}
        metadata_items = [item for item in metadata_items if str(item.get("source_id", "")) in selected]
        missing_source_ids = sorted(selected - {str(item.get("source_id", "")) for item in metadata_items})
    else:
        missing_source_ids = []

    document_index = build_document_index(data_root / "source" / "docs")
    entries: list[NormalizeIndexEntry] = []
    existing_by_source_id: dict[str, NormalizeIndexEntry] = {}
    reused_count = 0

    if not force_rebuild and layout.normalize_index_path().exists():
        existing_index = read_normalize_index(layout.normalize_index_path())
        existing_by_source_id = {
            entry.source_id: entry
            for entry in existing_index.entries
            if entry.source_id
        }

    total_sources = len(missing_source_ids) + len(metadata_items)
    if progress_callback is not None:
        progress_callback(0, max(total_sources, 1))
    completed_progress = 0

    for source_id in missing_source_ids:
        entries.append(
            NormalizeIndexEntry(
                source_id=source_id,
                status="failed",
                error_type="missing_metadata",
                message=f"Metadata entry not found for source_id: {source_id}",
            )
        )
        completed_progress += 1
        if progress_callback is not None:
            progress_callback(completed_progress, max(total_sources, 1))

    for metadata in metadata_items:
        source_id = str(metadata.get("source_id", "")).strip()
        existing_entry = existing_by_source_id.get(source_id)
        if existing_entry is not None and can_reuse_entry(existing_entry):
            entries.append(existing_entry)
            reused_count += 1
        else:
            entries.append(process_metadata_item(metadata, document_index, layout))
        completed_progress += 1
        if progress_callback is not None:
            progress_callback(completed_progress, max(total_sources, 1))

    success_count = sum(1 for entry in entries if entry.status == "completed")
    failed_count = len(entries) - success_count
    index = NormalizeStageIndex(
        stage="normalize",
        entries=entries,
        stats={
            "source_count": len(entries),
            "succeeded_sources": success_count,
            "failed_sources": failed_count,
            "reused_sources": reused_count,
        },
    )
    write_normalize_index(layout.normalize_index_path(), index)
    write_json(layout.normalize_log_path(), index.to_dict())
    return index


def can_reuse_entry(entry: NormalizeIndexEntry) -> bool:
    if entry.status == "completed":
        return bool(entry.artifact_path) and Path(entry.artifact_path).exists()
    return True
