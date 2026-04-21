from __future__ import annotations

from ...contracts import StageStateManifest, sanitize_manifest_stats, stage_artifacts, stage_inputs, stage_unit
from ...io import write_job_log, write_normalize_index
from ...stages import run_normalize
from ...utils.ids import timestamp_utc
from .common import (
    HandlerResult,
    StageContext,
    build_normalize_stage_stats,
    emit_stage_progress,
    normalize_unit_ids,
    select_normalize_entries,
    write_stage_state,
)


def run(ctx: StageContext) -> HandlerResult:
    checkpoint_every = ctx.runtime.stage_checkpoint_every(ctx.stage_name)

    def checkpoint_normalize(snapshot_index, partial_entries) -> None:
        processed_source_ids = [
            entry.source_id for entry in partial_entries if entry.status == "completed"
        ]
        ctx.stage_record.artifact_paths = {
            "primary": str(ctx.layout.normalize_index_path()),
            "index": str(ctx.layout.normalize_index_path()),
        }
        ctx.stage_record.stats = build_normalize_stage_stats(partial_entries)
        write_normalize_index(ctx.layout.normalize_index_path(), snapshot_index)
        write_stage_state(
            ctx.layout,
            StageStateManifest(
                stage="normalize",
                inputs=stage_inputs(ctx.layout, "normalize"),
                artifacts=stage_artifacts(ctx.layout, "normalize"),
                updated_at=timestamp_utc(),
                unit=stage_unit("normalize"),
                stats=dict(ctx.stage_record.stats),
                processed_units=normalize_unit_ids(processed_source_ids),
                substages={},
            ),
        )
        write_job_log(ctx.layout.job_log_path(ctx.job_id), ctx.log_record)

    normalize_index = run_normalize(
        ctx.data_root,
        source_ids=ctx.selected_source_ids,
        force_rebuild=ctx.force_rebuild,
        progress_callback=lambda current, total_items: emit_stage_progress(
            ctx.stage_progress_callback, ctx.stage_name, current, total_items
        ),
        checkpoint_every=checkpoint_every,
        checkpoint_callback=checkpoint_normalize,
    )
    selected_entries = select_normalize_entries(normalize_index, ctx.selected_source_ids)
    processed_source_ids = [
        entry.source_id for entry in selected_entries if entry.status == "completed"
    ]
    ctx.stage_record.artifact_paths = {
        "primary": str(ctx.layout.normalize_index_path()),
        "index": str(ctx.layout.normalize_index_path()),
    }
    ctx.stage_record.stats = build_normalize_stage_stats(selected_entries)
    write_stage_state(
        ctx.layout,
        StageStateManifest(
            stage="normalize",
            inputs=stage_inputs(ctx.layout, "normalize"),
            artifacts=stage_artifacts(ctx.layout, "normalize"),
            updated_at=timestamp_utc(),
            unit=stage_unit("normalize"),
            stats=sanitize_manifest_stats(dict(ctx.stage_record.stats), stage_name="normalize"),
            processed_units=normalize_unit_ids(processed_source_ids),
            substages={},
        ),
    )
    return HandlerResult(current_graph=None)
