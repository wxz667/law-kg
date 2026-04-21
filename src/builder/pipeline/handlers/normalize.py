from __future__ import annotations

from ...contracts import StageStateManifest, sanitize_manifest_stats, stage_artifacts, stage_inputs, stage_unit
from ...io import read_normalized_document, write_job_log, write_normalize_index
from ...stages import run_normalize
from ...utils.ids import timestamp_utc
from .common import (
    HandlerResult,
    StageContext,
    build_normalize_stage_stats,
    emit_stage_progress,
    normalize_unit_ids,
    write_stage_state,
)


def run(ctx: StageContext) -> HandlerResult:
    checkpoint_every = ctx.runtime.stage_checkpoint_every(ctx.stage_name)

    def checkpoint_normalize(snapshot_index, partial_entries) -> None:
        ctx.stage_record.artifact_paths = normalize_artifact_paths(ctx)
        ctx.stage_record.stats = build_normalize_stage_stats(partial_entries)
        ctx.stage_record.failures = normalize_failures(partial_entries)
        write_normalize_index(ctx.layout.normalize_index_path(), snapshot_index)
        manifest_stats = build_normalize_manifest_stats(ctx, snapshot_index)
        write_stage_state(
            ctx.layout,
            StageStateManifest(
                stage="normalize",
                inputs=stage_inputs(ctx.layout, "normalize"),
                artifacts=stage_artifacts(ctx.layout, "normalize"),
                updated_at=timestamp_utc(),
                unit=stage_unit("normalize"),
                stats=sanitize_manifest_stats(manifest_stats, stage_name="normalize"),
                processed_units=normalize_unit_ids([entry.source_id for entry in snapshot_index.entries]),
                substages={},
            ),
        )
        write_job_log(ctx.layout.job_log_path(ctx.job_id), ctx.log_record)

    normalize_index, run_records = run_normalize(
        ctx.data_root,
        metadata_root=ctx.runtime.builder_config.metadata,
        document_root=ctx.runtime.builder_config.document,
        source_ids=ctx.selected_source_ids,
        force_rebuild=ctx.force_rebuild,
        progress_callback=lambda current, total_items: emit_stage_progress(
            ctx.stage_progress_callback, ctx.stage_name, current, total_items
        ),
        checkpoint_every=checkpoint_every,
        checkpoint_callback=checkpoint_normalize,
    )
    ctx.stage_record.artifact_paths = normalize_artifact_paths(ctx)
    ctx.stage_record.stats = build_normalize_stage_stats(run_records)
    ctx.stage_record.failures = normalize_failures(run_records)
    manifest_stats = build_normalize_manifest_stats(ctx, normalize_index)
    write_stage_state(
        ctx.layout,
        StageStateManifest(
            stage="normalize",
            inputs=stage_inputs(ctx.layout, "normalize"),
            artifacts=stage_artifacts(ctx.layout, "normalize"),
            updated_at=timestamp_utc(),
            unit=stage_unit("normalize"),
            stats=sanitize_manifest_stats(manifest_stats, stage_name="normalize"),
            processed_units=normalize_unit_ids([entry.source_id for entry in normalize_index.entries]),
            substages={},
        ),
    )
    return HandlerResult(current_graph=None)


def normalize_artifact_paths(ctx: StageContext) -> dict[str, str]:
    return {
        "primary": str(ctx.layout.normalize_index_path()),
        "index": str(ctx.layout.normalize_index_path()),
        "documents": str(ctx.layout.normalize_documents_dir()),
    }


def normalize_failures(entries: list[object]) -> list[dict[str, str]]:
    failures: list[dict[str, str]] = []
    for entry in entries:
        if getattr(entry, "status", "") == "completed":
            continue
        failures.append(
            {
                "source_id": str(getattr(entry, "source_id", "")),
                "title": str(getattr(entry, "title", "")),
                "error_type": str(getattr(entry, "error_type", "")),
                "message": str(getattr(entry, "message", "")),
            }
        )
    return failures


def build_normalize_manifest_stats(ctx: StageContext, index) -> dict[str, object]:
    type_counts: dict[str, int] = {}
    for entry in index.entries:
        document_path = ctx.layout.normalize_documents_dir() / entry.document
        if not document_path.exists():
            continue
        document = read_normalized_document(document_path)
        document_type = str(
            document.metadata.get("category")
            or document.metadata.get("document_type")
            or document.metadata.get("source_type")
            or ""
        ).strip()
        if document_type:
            type_counts[document_type] = type_counts.get(document_type, 0) + 1
    return {
        "total_count": len(index.entries),
        "type_counts": dict(sorted(type_counts.items())),
    }
