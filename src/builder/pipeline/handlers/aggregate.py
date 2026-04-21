from __future__ import annotations

from ...io import (
    read_aggregate_concepts,
    read_extract_concepts,
    read_extract_inputs,
    read_stage_manifest,
    write_aggregate_concepts,
    write_job_log,
)
from ...stages import run_aggregate
from ...stages.aggregate.input import build_inputs_from_extract as build_aggregate_inputs_from_extract
from ...stages.aggregate.input import build_output_stats as aggregate_concept_output_stats
from .graph import replace_aggregate_concepts, select_aggregate_concepts
from .common import (
    HandlerResult,
    StageContext,
    build_stage_manifest,
    build_stage_work_stats,
    can_reuse_stage,
    emit_prefilled_stage_progress,
    finalize_stage_work_stats,
    load_graph_snapshot,
    normalize_unit_ids,
    offset_stage_progress_callback,
    resolve_stage_source_ids,
    reusable_stage_unit_ids,
    source_ids_for_input_rows,
    write_stage_state,
)


def aggregate_artifact_paths(ctx: StageContext) -> dict[str, str]:
    return {
        "primary": str(ctx.layout.aggregate_concepts_path()),
        "concepts": str(ctx.layout.aggregate_concepts_path()),
    }


def run(ctx: StageContext) -> HandlerResult:
    input_source_ids = resolve_stage_source_ids(ctx.layout, ctx.stage_name, ctx.selected_source_ids)
    base_graph = load_graph_snapshot(
        ctx.layout,
        ctx.graph_input_stage[ctx.stage_name],
        stage_sequence=ctx.stage_sequence,
        graph_stages=ctx.graph_stages,
    )
    extract_inputs = read_extract_inputs(ctx.layout.extract_inputs_path()) if ctx.layout.extract_inputs_path().exists() else []
    extract_concepts = read_extract_concepts(ctx.layout.extract_concepts_path()) if ctx.layout.extract_concepts_path().exists() else []
    existing_aggregate_concepts = (
        read_aggregate_concepts(ctx.layout.aggregate_concepts_path())
        if ctx.layout.aggregate_concepts_path().exists()
        else []
    )
    aggregate_inputs = build_aggregate_inputs_from_extract(
        extract_inputs,
        extract_concepts,
        graph_bundle=base_graph,
        active_source_ids=set(input_source_ids),
    )
    aggregate_unit_ids = [row.id for row in aggregate_inputs]
    aggregate_input_id_set = set(aggregate_unit_ids)
    aggregate_completed_unit_ids = reusable_stage_unit_ids(
        ctx.layout,
        ctx.stage_name,
        aggregate_unit_ids,
        force_rebuild=ctx.force_rebuild,
    )
    if (
        can_reuse_stage(
            ctx.layout,
            ctx.stage_name,
            aggregate_unit_ids,
            force_rebuild=ctx.force_rebuild,
        )
        and ctx.layout.aggregate_concepts_path().exists()
    ):
        emit_prefilled_stage_progress(
            ctx.stage_progress_callback,
            ctx.stage_name,
            current=len(aggregate_completed_unit_ids),
            total=len(aggregate_unit_ids),
        )
        manifest_stats = dict(read_stage_manifest(ctx.layout.stage_manifest_path(ctx.stage_name)).stats)
        ctx.stage_record.stats = build_stage_work_stats(
            input_source_ids=input_source_ids,
            processed_source_ids=[],
            skipped_source_ids=source_ids_for_input_rows(aggregate_inputs, aggregate_completed_unit_ids),
            work_units_total=len(aggregate_unit_ids),
            work_units_completed=0,
            work_units_skipped=len(aggregate_completed_unit_ids),
        ) | manifest_stats
    else:
        checkpoint_every = ctx.runtime.stage_checkpoint_every(ctx.stage_name)
        ctx.stage_record.artifact_paths = aggregate_artifact_paths(ctx)
        pending_aggregate_inputs = [
            row for row in aggregate_inputs if row.id not in set(aggregate_completed_unit_ids)
        ]
        write_stage_state(
            ctx.layout,
            build_stage_manifest(
                stage_name=ctx.stage_name,
                layout=ctx.layout,
                job_id=ctx.job_id,
                build_target=ctx.source_path_label,
                source_ids=[],
                processed_source_ids=[],
                processed_unit_ids=aggregate_completed_unit_ids,
                input_stage=ctx.graph_input_stage[ctx.stage_name],
                artifact_paths=ctx.stage_record.artifact_paths,
                stats={},
                status="running",
            ),
        )

        def checkpoint_aggregate(
            snapshot_concepts: list,
            snapshot_stats: dict[str, int],
            processed_checkpoint_source_ids: list[str],
            processed_checkpoint_input_ids: list[str],
            successful_checkpoint_input_ids: list[str],
            llm_error_summary: list[dict[str, object]],
        ) -> None:
            del processed_checkpoint_source_ids, processed_checkpoint_input_ids
            retained_scope_rows = [
                row
                for row in select_aggregate_concepts(
                    existing_aggregate_concepts,
                    graph_bundle=base_graph,
                    active_source_ids=set(input_source_ids),
                )
                if row.root in aggregate_input_id_set and row.root in set(aggregate_completed_unit_ids)
            ]
            merged_concepts = replace_aggregate_concepts(
                existing_aggregate_concepts,
                retained_scope_rows + list(snapshot_concepts),
                graph_bundle=base_graph,
                active_source_ids=set(input_source_ids),
            )
            write_aggregate_concepts(ctx.layout.aggregate_concepts_path(), merged_concepts)
            merged_output_stats = aggregate_concept_output_stats(merged_concepts)
            ctx.stage_record.stats = finalize_stage_work_stats(
                dict(snapshot_stats) | merged_output_stats,
                input_source_ids=input_source_ids,
                processed_source_ids=source_ids_for_input_rows(
                    aggregate_inputs,
                    successful_checkpoint_input_ids,
                ),
                skipped_source_ids=[],
                skipped_work_units=0,
            )
            ctx.stage_record.failures = [dict(item) for item in llm_error_summary]
            write_stage_state(
                ctx.layout,
                build_stage_manifest(
                    stage_name=ctx.stage_name,
                    layout=ctx.layout,
                    job_id=ctx.job_id,
                    build_target=ctx.source_path_label,
                    source_ids=[],
                    processed_source_ids=[],
                    processed_unit_ids=normalize_unit_ids(
                        aggregate_completed_unit_ids + successful_checkpoint_input_ids
                    ),
                    input_stage=ctx.graph_input_stage[ctx.stage_name],
                    artifact_paths=aggregate_artifact_paths(ctx),
                    stats=ctx.stage_record.stats,
                    status="running",
                ),
            )
            write_job_log(ctx.layout.job_log_path(ctx.job_id), ctx.log_record)

        emit_prefilled_stage_progress(
            ctx.stage_progress_callback,
            ctx.stage_name,
            current=len(aggregate_completed_unit_ids),
            total=len(aggregate_unit_ids),
        )
        aggregate_result = run_aggregate(
            base_graph,
            ctx.runtime,
            inputs=pending_aggregate_inputs,
            active_source_ids=set(input_source_ids),
            progress_callback=offset_stage_progress_callback(
                ctx.stage_progress_callback,
                ctx.stage_name,
                skipped_units=len(aggregate_completed_unit_ids),
                total_units=len(aggregate_unit_ids),
            ),
            checkpoint_every=checkpoint_every,
            checkpoint_callback=checkpoint_aggregate,
            cancel_event=ctx.cancel_event,
        )
        retained_scope_rows = [
            row
            for row in select_aggregate_concepts(
                existing_aggregate_concepts,
                graph_bundle=base_graph,
                active_source_ids=set(input_source_ids),
            )
            if row.root in aggregate_input_id_set and row.root in set(aggregate_completed_unit_ids)
        ]
        merged_concepts = replace_aggregate_concepts(
            existing_aggregate_concepts,
            retained_scope_rows + list(aggregate_result.concepts),
            graph_bundle=base_graph,
            active_source_ids=set(input_source_ids),
        )
        write_aggregate_concepts(ctx.layout.aggregate_concepts_path(), merged_concepts)
        merged_output_stats = aggregate_concept_output_stats(merged_concepts)
        ctx.stage_record.stats = finalize_stage_work_stats(
            dict(aggregate_result.stats) | merged_output_stats,
            input_source_ids=input_source_ids,
            processed_source_ids=source_ids_for_input_rows(
                aggregate_inputs,
                aggregate_result.successful_input_ids,
            ),
            skipped_source_ids=[],
            skipped_work_units=0,
        )
        ctx.stage_record.failures = [dict(item) for item in aggregate_result.llm_errors]
        write_stage_state(
            ctx.layout,
            build_stage_manifest(
                stage_name=ctx.stage_name,
                layout=ctx.layout,
                job_id=ctx.job_id,
                build_target=ctx.source_path_label,
                source_ids=[],
                processed_source_ids=[],
                processed_unit_ids=normalize_unit_ids(
                    aggregate_completed_unit_ids + aggregate_result.successful_input_ids
                ),
                input_stage=ctx.graph_input_stage[ctx.stage_name],
                artifact_paths=aggregate_artifact_paths(ctx),
                stats=ctx.stage_record.stats,
            ),
        )
    ctx.stage_record.artifact_paths = aggregate_artifact_paths(ctx)
    return HandlerResult(current_graph=None)
