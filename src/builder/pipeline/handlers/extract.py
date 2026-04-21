from __future__ import annotations

from ...io import (
    read_extract_concepts,
    read_extract_inputs,
    write_extract_concepts,
    write_extract_inputs,
    write_job_log,
)
from ...stages import run_extract
from ...stages.extract.input import build_extract_inputs, filter_extract_source_ids
from ...utils.locator import source_id_from_node_id
from .graph import (
    replace_extract_concepts_by_unit_ids,
    replace_extract_inputs,
    select_extract_concepts,
    select_extract_inputs,
)
from .common import (
    HandlerResult,
    StageContext,
    build_stage_manifest,
    build_stage_work_stats,
    build_unit_substage_manifest,
    completed_source_ids_for_input_rows,
    emit_prefilled_stage_progress,
    finalize_stage_work_stats,
    load_graph_snapshot,
    normalize_source_ids,
    normalize_unit_ids,
    offset_stage_progress_callback,
    resolve_stage_source_ids,
    reusable_substage_unit_ids,
    source_ids_for_input_rows,
    subtract_source_ids,
    write_stage_state,
)


def extract_artifact_paths(ctx: StageContext) -> dict[str, str]:
    return {
        "primary": str(ctx.layout.extract_concepts_path()),
        "inputs": str(ctx.layout.extract_inputs_path()),
        "results": str(ctx.layout.extract_concepts_path()),
    }


def extract_inputs_materialized(layout, graph_bundle, source_ids: list[str], substage) -> bool:
    if not source_ids:
        return True
    if not layout.extract_inputs_path().exists():
        return False
    existing_inputs = read_extract_inputs(layout.extract_inputs_path())
    scoped_inputs = select_extract_inputs(
        existing_inputs,
        graph_bundle=graph_bundle,
        active_source_ids=set(source_ids),
    )
    actual_source_ids = {source_id_from_node_id(row.id) for row in scoped_inputs}
    requested_source_ids = set(normalize_source_ids(source_ids))
    if not actual_source_ids.issubset(requested_source_ids):
        return False
    expected_count = int(substage.stats.get("result_count", 0))
    if expected_count > 0 and len(scoped_inputs) != expected_count:
        return False
    return True


def extract_outputs_materialized(layout, graph_bundle, source_ids: list[str], substage) -> bool:
    if not source_ids:
        return True
    if not layout.extract_inputs_path().exists() or not layout.extract_concepts_path().exists():
        return False
    existing_inputs = read_extract_inputs(layout.extract_inputs_path())
    scoped_inputs = select_extract_inputs(
        existing_inputs,
        graph_bundle=graph_bundle,
        active_source_ids=set(source_ids),
    )
    actual_source_ids = {source_id_from_node_id(row.id) for row in scoped_inputs}
    requested_source_ids = set(normalize_source_ids(source_ids))
    if not actual_source_ids.issubset(requested_source_ids):
        return False
    existing_concepts = read_extract_concepts(layout.extract_concepts_path())
    scoped_concepts = select_extract_concepts(
        existing_concepts,
        graph_bundle=graph_bundle,
        active_source_ids=set(source_ids),
    )
    expected_count = int(substage.stats.get("result_count", 0))
    if expected_count > 0 and len(scoped_inputs) != expected_count:
        return False
    expected_result_count = int(substage.stats.get("result_count", 0))
    if expected_result_count > 0 and len(scoped_concepts) != expected_result_count:
        return False
    return True


def run(ctx: StageContext) -> HandlerResult:
    stage_name = ctx.stage_name
    layout = ctx.layout
    input_source_ids = resolve_stage_source_ids(layout, stage_name, ctx.selected_source_ids)
    base_graph = load_graph_snapshot(
        layout,
        ctx.graph_input_stage[stage_name],
        stage_sequence=ctx.stage_sequence,
        graph_stages=ctx.graph_stages,
    )
    input_checkpoint_every = ctx.runtime.substage_checkpoint_every(stage_name, "input")
    extract_checkpoint_every = ctx.runtime.substage_checkpoint_every(stage_name, "extract")
    existing_inputs = (
        read_extract_inputs(layout.extract_inputs_path())
        if layout.extract_inputs_path().exists()
        else []
    )
    existing_concepts = (
        read_extract_concepts(layout.extract_concepts_path())
        if layout.extract_concepts_path().exists()
        else []
    )
    substage_states = {}
    filtered_source_ids = filter_extract_source_ids(
        base_graph,
        active_source_ids=set(input_source_ids),
    )
    ctx.stage_record.artifact_paths = extract_artifact_paths(ctx)
    write_stage_state(
        layout,
        build_stage_manifest(
            stage_name=stage_name,
            layout=layout,
            job_id=ctx.job_id,
            build_target=ctx.source_path_label,
            source_ids=ctx.selected_source_ids,
            processed_source_ids=[],
            input_stage=ctx.graph_input_stage[stage_name],
            artifact_paths=ctx.stage_record.artifact_paths,
            stats={},
            status="running",
            substage_states=substage_states,
        ),
    )

    input_source_ids_for_extract = filtered_source_ids
    input_skipped_source_ids = reusable_substage_unit_ids(
        layout,
        stage_name,
        "input",
        input_source_ids_for_extract,
        force_rebuild=ctx.force_rebuild,
    )
    input_process_source_ids = subtract_source_ids(input_source_ids_for_extract, input_skipped_source_ids)
    if input_process_source_ids:
        def checkpoint_input(
            snapshot_inputs: list,
            processed_checkpoint_source_ids: list[str],
        ) -> None:
            merged_inputs = replace_extract_inputs(
                existing_inputs,
                snapshot_inputs,
                graph_bundle=base_graph,
                active_source_ids=set(input_process_source_ids),
            )
            write_extract_inputs(layout.extract_inputs_path(), merged_inputs)
            input_scope_inputs = select_extract_inputs(
                merged_inputs,
                graph_bundle=base_graph,
                active_source_ids=set(input_source_ids_for_extract),
            )
            substage_states["input"] = build_unit_substage_manifest(
                layout=layout,
                parent_stage="extract",
                stage_name="input",
                processed_units=normalize_source_ids(input_skipped_source_ids + processed_checkpoint_source_ids),
                stats=build_stage_work_stats(
                    input_source_ids=input_source_ids_for_extract,
                    processed_source_ids=processed_checkpoint_source_ids,
                    skipped_source_ids=input_skipped_source_ids,
                    work_units_total=len(input_source_ids_for_extract),
                    work_units_completed=len(processed_checkpoint_source_ids),
                    work_units_skipped=len(input_skipped_source_ids),
                    input_count=len(input_scope_inputs),
                    output_unit_count=len(input_scope_inputs),
                    output_source_count=len({source_id_from_node_id(row.id) for row in input_scope_inputs}),
                ),
            )
            write_stage_state(
                layout,
                build_stage_manifest(
                    stage_name=stage_name,
                    layout=layout,
                    job_id=ctx.job_id,
                    build_target=ctx.source_path_label,
                    source_ids=ctx.selected_source_ids,
                    processed_source_ids=[],
                    input_stage=ctx.graph_input_stage[stage_name],
                    artifact_paths=ctx.stage_record.artifact_paths,
                    stats={},
                    status="running",
                    substage_states=substage_states,
                ),
            )

        aggregated_inputs = build_extract_inputs(
            base_graph,
            active_source_ids=set(input_process_source_ids),
            progress_callback=offset_stage_progress_callback(
                ctx.stage_progress_callback,
                f"{stage_name}::input",
                skipped_units=len(input_skipped_source_ids),
                total_units=len(input_source_ids_for_extract),
            ),
            checkpoint_every=input_checkpoint_every,
            checkpoint_callback=checkpoint_input,
        )
        merged_inputs = replace_extract_inputs(
            existing_inputs,
            aggregated_inputs,
            graph_bundle=base_graph,
            active_source_ids=set(input_process_source_ids),
        )
        write_extract_inputs(layout.extract_inputs_path(), merged_inputs)
        existing_inputs = merged_inputs
    else:
        emit_prefilled_stage_progress(
            ctx.stage_progress_callback,
            f"{stage_name}::input",
            current=len(input_skipped_source_ids),
            total=len(input_source_ids_for_extract),
        )
    aggregated_scope_inputs = select_extract_inputs(
        existing_inputs,
        graph_bundle=base_graph,
        active_source_ids=set(input_source_ids_for_extract),
    )
    substage_states["input"] = build_unit_substage_manifest(
        layout=layout,
        parent_stage="extract",
        stage_name="input",
        processed_units=normalize_source_ids(input_skipped_source_ids + input_process_source_ids),
        stats=build_stage_work_stats(
            input_source_ids=input_source_ids_for_extract,
            processed_source_ids=input_process_source_ids,
            skipped_source_ids=input_skipped_source_ids,
            work_units_total=len(input_source_ids_for_extract),
            work_units_completed=len(input_process_source_ids),
            work_units_skipped=len(input_skipped_source_ids),
            input_count=len(aggregated_scope_inputs),
            output_unit_count=len(aggregated_scope_inputs),
            output_source_count=len({source_id_from_node_id(row.id) for row in aggregated_scope_inputs}),
        ),
    )
    write_stage_state(
        layout,
        build_stage_manifest(
            stage_name=stage_name,
            layout=layout,
            job_id=ctx.job_id,
            build_target=ctx.source_path_label,
            source_ids=ctx.selected_source_ids,
            processed_source_ids=[],
            input_stage=ctx.graph_input_stage[stage_name],
            artifact_paths=ctx.stage_record.artifact_paths,
            stats={},
            status="running",
            substage_states=substage_states,
        ),
    )

    extract_scope_inputs = list(aggregated_scope_inputs)
    extract_scope_input_ids = [row.id for row in extract_scope_inputs]
    extract_scope_source_ids = sorted({source_id_from_node_id(row.id) for row in extract_scope_inputs})
    extract_completed_input_ids = reusable_substage_unit_ids(
        layout,
        stage_name,
        "extract",
        extract_scope_input_ids,
        force_rebuild=ctx.force_rebuild,
    )
    extract_skipped_inputs = [
        row for row in extract_scope_inputs if row.id in set(extract_completed_input_ids)
    ]
    extract_total_units = len(extract_scope_inputs)
    extract_skipped_units = len(extract_skipped_inputs)
    extract_process_inputs = [
        row for row in extract_scope_inputs if row.id not in set(extract_completed_input_ids)
    ]
    extract_process_source_ids = sorted(
        {source_id_from_node_id(row.id) for row in extract_process_inputs}
    )
    extract_skipped_source_ids = completed_source_ids_for_input_rows(
        extract_scope_inputs,
        extract_completed_input_ids,
    )
    extract_scope_concepts = select_extract_concepts(
        existing_concepts,
        graph_bundle=base_graph,
        active_source_ids=set(extract_scope_source_ids),
    )
    if extract_process_inputs:
        active_extract_source_ids = set(extract_process_source_ids)
        prepared_inputs = list(extract_process_inputs)
        emit_prefilled_stage_progress(
            ctx.stage_progress_callback,
            f"{stage_name}::extract",
            current=extract_skipped_units,
            total=extract_total_units,
        )
        ctx.stage_record.artifact_paths = extract_artifact_paths(ctx)
        ctx.stage_record.stats = build_stage_work_stats(
            input_source_ids=extract_scope_source_ids,
            processed_source_ids=[],
            skipped_source_ids=extract_skipped_source_ids,
            work_units_total=extract_total_units,
            work_units_completed=0,
            work_units_skipped=extract_skipped_units,
            input_count=len(prepared_inputs),
            result_count=len(extract_scope_concepts),
            concept_count=sum(len(row.concepts) for row in extract_scope_concepts),
            llm_request_count=0,
            llm_error_count=0,
            retry_count=0,
        )
        substage_states["extract"] = build_unit_substage_manifest(
            layout=layout,
            parent_stage="extract",
            stage_name="extract",
            processed_units=[],
            stats=dict(ctx.stage_record.stats),
        )
        ctx.stage_record.failures = []
        write_stage_state(
            layout,
            build_stage_manifest(
                stage_name=stage_name,
                layout=layout,
                job_id=ctx.job_id,
                build_target=ctx.source_path_label,
                source_ids=ctx.selected_source_ids,
                processed_source_ids=[],
                input_stage=ctx.graph_input_stage[stage_name],
                artifact_paths=ctx.stage_record.artifact_paths,
                stats=ctx.stage_record.stats,
                status="running",
                substage_states=substage_states,
            ),
        )
        write_job_log(layout.job_log_path(ctx.job_id), ctx.log_record)

        def checkpoint_extract(
            snapshot_concepts: list,
            snapshot_stats: dict[str, int],
            processed_checkpoint_source_ids: list[str],
            processed_checkpoint_input_ids: list[str],
            successful_checkpoint_input_ids: list[str],
            llm_error_summary: list[dict[str, object]],
        ) -> None:
            del processed_checkpoint_source_ids, processed_checkpoint_input_ids
            merged_concepts = replace_extract_concepts_by_unit_ids(
                existing_concepts,
                snapshot_concepts,
                graph_bundle=base_graph,
                active_unit_ids=set(successful_checkpoint_input_ids),
            )
            write_extract_concepts(layout.extract_concepts_path(), merged_concepts)
            ctx.stage_record.artifact_paths = extract_artifact_paths(ctx)
            ctx.stage_record.stats = finalize_stage_work_stats(
                dict(snapshot_stats),
                input_source_ids=extract_scope_source_ids,
                processed_source_ids=source_ids_for_input_rows(
                    extract_scope_inputs,
                    successful_checkpoint_input_ids,
                ),
                skipped_source_ids=extract_skipped_source_ids,
                skipped_work_units=extract_skipped_units,
            )
            substage_states["extract"] = build_unit_substage_manifest(
                layout=layout,
                parent_stage="extract",
                stage_name="extract",
                processed_units=normalize_unit_ids(extract_completed_input_ids + successful_checkpoint_input_ids),
                stats=dict(ctx.stage_record.stats),
            )
            ctx.stage_record.failures = [dict(item) for item in llm_error_summary]
            write_stage_state(
                layout,
                build_stage_manifest(
                    stage_name=stage_name,
                    layout=layout,
                    job_id=ctx.job_id,
                    build_target=ctx.source_path_label,
                    source_ids=ctx.selected_source_ids,
                    processed_source_ids=source_ids_for_input_rows(
                        extract_scope_inputs,
                        successful_checkpoint_input_ids,
                    ),
                    unit_ids=extract_scope_input_ids,
                    processed_unit_ids=[],
                    input_stage=ctx.graph_input_stage[stage_name],
                    artifact_paths=ctx.stage_record.artifact_paths,
                    stats=ctx.stage_record.stats,
                    status="running",
                    substage_states=substage_states,
                ),
            )
            write_job_log(layout.job_log_path(ctx.job_id), ctx.log_record)

        result = run_extract(
            base_graph,
            ctx.runtime,
            inputs=prepared_inputs,
            active_source_ids=active_extract_source_ids,
            progress_callback=offset_stage_progress_callback(
                ctx.stage_progress_callback,
                f"{stage_name}::extract",
                skipped_units=extract_skipped_units,
                total_units=extract_total_units,
            ),
            checkpoint_every=extract_checkpoint_every,
            checkpoint_callback=checkpoint_extract,
            cancel_event=ctx.cancel_event,
        )
        merged_concepts = replace_extract_concepts_by_unit_ids(
            existing_concepts,
            result.concepts,
            graph_bundle=base_graph,
            active_unit_ids=set(result.successful_input_ids),
        )
        write_extract_concepts(layout.extract_concepts_path(), merged_concepts)
        ctx.stage_record.stats = finalize_stage_work_stats(
            dict(result.stats),
            input_source_ids=extract_scope_source_ids,
            processed_source_ids=source_ids_for_input_rows(
                extract_scope_inputs,
                result.successful_input_ids,
            ),
            skipped_source_ids=extract_skipped_source_ids,
            skipped_work_units=extract_skipped_units,
        )
        ctx.stage_record.failures = [dict(item) for item in result.llm_errors]
        processed_source_ids = source_ids_for_input_rows(
            extract_scope_inputs,
            result.successful_input_ids,
        )
        substage_states["extract"] = build_unit_substage_manifest(
            layout=layout,
            parent_stage="extract",
            stage_name="extract",
            processed_units=normalize_unit_ids(extract_completed_input_ids + result.successful_input_ids),
            stats=dict(ctx.stage_record.stats),
        )
    else:
        emit_prefilled_stage_progress(
            ctx.stage_progress_callback,
            f"{stage_name}::extract",
            current=extract_skipped_units,
            total=extract_total_units,
        )
        ctx.stage_record.stats = build_stage_work_stats(
            input_source_ids=extract_scope_source_ids,
            processed_source_ids=[],
            skipped_source_ids=extract_skipped_source_ids,
            work_units_total=extract_total_units,
            work_units_completed=0,
            work_units_skipped=extract_skipped_units,
            input_count=len(extract_scope_inputs),
            result_count=len(extract_scope_concepts),
            concept_count=sum(len(row.concepts) for row in extract_scope_concepts),
        )
        processed_source_ids = []
        substage_states["extract"] = build_unit_substage_manifest(
            layout=layout,
            parent_stage="extract",
            stage_name="extract",
            processed_units=[],
            stats=dict(ctx.stage_record.stats),
        )
    ctx.stage_record.artifact_paths = extract_artifact_paths(ctx)
    write_stage_state(
        layout,
        build_stage_manifest(
            stage_name=stage_name,
            layout=layout,
            job_id=ctx.job_id,
            build_target=ctx.source_path_label,
            source_ids=ctx.selected_source_ids,
            processed_source_ids=processed_source_ids,
            unit_ids=extract_scope_input_ids,
            processed_unit_ids=[],
            input_stage=ctx.graph_input_stage[stage_name],
            artifact_paths=ctx.stage_record.artifact_paths,
            stats=ctx.stage_record.stats,
            substage_states=substage_states,
        ),
    )
    return HandlerResult(current_graph=None)
