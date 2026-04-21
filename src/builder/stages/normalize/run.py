from __future__ import annotations

from pathlib import Path
from typing import Callable

from ...contracts import NormalizeIndexEntry, NormalizeStageIndex
from ...io import BuildLayout, read_normalize_index, write_normalize_index
from .metadata import build_document_index, load_metadata_items
from .normalizer import process_metadata_item
from .types import NormalizeRunRecord


def merge_normalize_index(
    existing_index: NormalizeStageIndex | None,
    updated_records: list[NormalizeRunRecord],
) -> NormalizeStageIndex:
    merged_by_source_id = {
        entry.source_id: entry
        for entry in (existing_index.entries if existing_index is not None else [])
        if entry.source_id
    }
    for record in updated_records:
        if not record.source_id:
            continue
        if record.status == "completed" and record.document:
            merged_by_source_id[record.source_id] = normalize_index_entry_from_record(record)
        else:
            merged_by_source_id.pop(record.source_id, None)
    entries = [merged_by_source_id[key] for key in sorted(merged_by_source_id)]
    return NormalizeStageIndex(entries=entries)


def read_existing_normalize_index(path) -> NormalizeStageIndex | None:
    if not path.exists():
        return None
    return read_normalize_index(path)


def run(
    data_root: Path,
    metadata_root: Path,
    document_root: Path,
    source_ids: list[str] | None = None,
    *,
    force_rebuild: bool = False,
    progress_callback: Callable[[int, int], None] | None = None,
    checkpoint_every: int = 0,
    checkpoint_callback: Callable[[NormalizeStageIndex, list[NormalizeRunRecord]], None] | None = None,
) -> tuple[NormalizeStageIndex, list[NormalizeRunRecord]]:
    data_root = data_root.resolve()
    metadata_root = metadata_root.resolve()
    document_root = document_root.resolve()
    layout = BuildLayout(data_root)
    metadata_items = load_metadata_items(metadata_root)
    if source_ids is not None:
        selected = {source_id for source_id in source_ids}
        metadata_items = [item for item in metadata_items if str(item.get("source_id", "")) in selected]
        missing_source_ids = sorted(selected - {str(item.get("source_id", "")) for item in metadata_items})
    else:
        missing_source_ids = []

    document_index = build_document_index(document_root)
    records: list[NormalizeRunRecord] = []
    existing_by_source_id: dict[str, NormalizeIndexEntry] = {}

    existing_index = read_existing_normalize_index(layout.normalize_index_path())
    if not force_rebuild:
        existing_by_source_id = {
            entry.source_id: entry
            for entry in (existing_index.entries if existing_index is not None else [])
            if entry.source_id
        }

    total_sources = len(missing_source_ids) + len(metadata_items)
    if progress_callback is not None:
        progress_callback(0, max(total_sources, 1))
    completed_progress = 0

    for source_id in missing_source_ids:
        records.append(
            NormalizeRunRecord(
                source_id=source_id,
                status="failed",
                error_type="missing_metadata",
                message=f"Metadata entry not found for source_id: {source_id}",
            )
        )
        completed_progress += 1
        if progress_callback is not None:
            progress_callback(completed_progress, max(total_sources, 1))
        maybe_emit_checkpoint(
            checkpoint_every=checkpoint_every,
            checkpoint_callback=checkpoint_callback,
            completed_progress=completed_progress,
            total_sources=total_sources,
            records=records,
            source_ids=source_ids,
            existing_index=existing_index,
        )

    for metadata in metadata_items:
        source_id = str(metadata.get("source_id", "")).strip()
        existing_entry = existing_by_source_id.get(source_id)
        if existing_entry is not None and can_reuse_entry(existing_entry, layout):
            records.append(
                NormalizeRunRecord(
                    source_id=existing_entry.source_id,
                    status="completed",
                    title=existing_entry.title,
                    document=existing_entry.document,
                    reused=True,
                )
            )
        else:
            records.append(process_metadata_item(metadata, document_index, layout))
        completed_progress += 1
        if progress_callback is not None:
            progress_callback(completed_progress, max(total_sources, 1))
        maybe_emit_checkpoint(
            checkpoint_every=checkpoint_every,
            checkpoint_callback=checkpoint_callback,
            completed_progress=completed_progress,
            total_sources=total_sources,
            records=records,
            source_ids=source_ids,
            existing_index=existing_index,
        )

    index = build_normalize_stage_index(records)
    if source_ids is not None and existing_index is not None:
        index = merge_normalize_index(existing_index, records)
    write_normalize_index(layout.normalize_index_path(), index)
    return index, records


def can_reuse_entry(entry: NormalizeIndexEntry, layout: BuildLayout) -> bool:
    return bool(entry.document) and (layout.normalize_documents_dir() / entry.document).exists()


def build_normalize_stage_index(records: list[NormalizeRunRecord]) -> NormalizeStageIndex:
    entries = [
        normalize_index_entry_from_record(record)
        for record in records
        if record.status == "completed" and record.document
    ]
    return NormalizeStageIndex(entries=sorted(entries, key=lambda entry: entry.source_id))


def normalize_index_entry_from_record(record: NormalizeRunRecord) -> NormalizeIndexEntry:
    return NormalizeIndexEntry(
        source_id=record.source_id,
        title=record.title,
        document=record.document,
    )


def maybe_emit_checkpoint(
    *,
    checkpoint_every: int,
    checkpoint_callback: Callable[[NormalizeStageIndex, list[NormalizeRunRecord]], None] | None,
    completed_progress: int,
    total_sources: int,
    records: list[NormalizeRunRecord],
    source_ids: list[str] | None,
    existing_index: NormalizeStageIndex | None,
) -> None:
    if checkpoint_callback is None or checkpoint_every <= 0 or completed_progress <= 0:
        return
    if completed_progress % checkpoint_every != 0 and completed_progress != total_sources:
        return
    snapshot_index = build_normalize_stage_index(records)
    if source_ids is not None and existing_index is not None:
        snapshot_index = merge_normalize_index(existing_index, records)
    checkpoint_callback(snapshot_index, list(records))
