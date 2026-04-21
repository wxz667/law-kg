from __future__ import annotations

import sys

from ...contracts import GraphBundle, SubstageStateManifest
from ...io import (
    read_classify_pending,
    read_classify_results,
    read_llm_judge_details,
    read_reference_candidates,
    write_classify_pending,
    write_classify_results,
    write_job_log,
    write_llm_judge_details,
)
from ...stages.classify.materialize import materialize_classify_results
from ...stages.classify.run import build_classify_context, run_llm_phase, run_model_phase, select_candidates
from ...utils.locator import owner_source_id
from .graph import (
    replace_classify_outputs_by_unit_ids,
    replace_classify_pending_by_unit_ids,
    replace_llm_judge_details_by_unit_ids,
)
from .common import (
    HandlerResult,
    StageContext,
    build_stage_manifest,
    build_stage_work_stats,
    build_unit_source_map,
    build_unit_substage_manifest,
    load_graph_snapshot,
    normalize_unit_ids,
    offset_stage_progress_callback,
    resolve_stage_source_ids,
    reusable_substage_unit_ids,
    source_ids_for_unit_ids,
    stage_outputs_exist,
    write_stage_graph,
    write_stage_state,
    emit_prefilled_stage_progress,
)


def classify_artifact_paths(ctx: StageContext) -> dict[str, str]:
    return {
        "primary": str(ctx.layout.stage_edges_path("classify")),
        "edges": str(ctx.layout.stage_edges_path("classify")),
        "pending": str(ctx.layout.classify_pending_path()),
        "results": str(ctx.layout.classify_results_path()),
        "llm_judgments": str(ctx.layout.classify_llm_judge_path()),
    }


def _patched_orchestrator_callable(name: str, default):
    orchestrator = sys.modules.get("builder.pipeline.orchestrator")
    if orchestrator is None:
        return default
    return getattr(orchestrator, name, default)


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
    candidates = read_reference_candidates(layout.detect_candidates_path())
    model_checkpoint_every = ctx.runtime.substage_checkpoint_every(stage_name, "model")
    judge_checkpoint_every = ctx.runtime.substage_checkpoint_every(stage_name, "judge")
    scoped_classify_context = build_classify_context(
        base_graph,
        set(input_source_ids) if input_source_ids else set(),
    )
    scoped_candidates = select_candidates(candidates, scoped_classify_context)
    candidate_unit_ids = [row.id for row in scoped_candidates]
    candidate_source_map = (
        build_unit_source_map(
            {
                row.id: owner_source_id(
                    scoped_classify_context.owner_document_by_node.get(row.source_node_id, row.source_node_id)
                )
                for row in scoped_candidates
            }
        )
        if scoped_candidates
        else {}
    )
    completed_candidate_unit_ids: list[str] = []
    completed_candidate_unit_id_set = set(completed_candidate_unit_ids)
    process_candidate_unit_ids = [
        unit_id for unit_id in candidate_unit_ids if unit_id not in completed_candidate_unit_id_set
    ]
    process_candidate_unit_id_set = set(process_candidate_unit_ids)
    skipped_source_ids = (
        source_ids_for_unit_ids(completed_candidate_unit_ids, candidate_source_map)
        if completed_candidate_unit_ids
        else []
    )
    process_source_ids = (
        source_ids_for_unit_ids(process_candidate_unit_ids, candidate_source_map)
        if process_candidate_unit_ids
        else []
    )
    process_source_id_set = set(process_source_ids)
    process_candidates = [
        row for row in scoped_candidates if row.id in process_candidate_unit_id_set
    ]
    process_classify_context = build_classify_context(
        base_graph,
        process_source_id_set,
    )
    existing_results = (
        read_classify_results(layout.classify_results_path())
        if layout.classify_results_path().exists()
        else []
    )
    existing_pending = (
        read_classify_pending(layout.classify_pending_path())
        if layout.classify_pending_path().exists()
        else []
    )
    existing_llm = (
        read_llm_judge_details(layout.classify_llm_judge_path())
        if layout.classify_llm_judge_path().exists()
        else []
    )
    skipped_units = len(completed_candidate_unit_ids)
    substage_states: dict[str, SubstageStateManifest] = {}
    current_graph: GraphBundle | None = None

    def candidate_source_ids(unit_ids: list[str]) -> list[str]:
        if not unit_ids or not candidate_source_map:
            return []
        return source_ids_for_unit_ids(unit_ids, candidate_source_map)

    def classify_active_results(rows: list) -> list:
        return [
            row
            for row in rows
            if owner_source_id(
                process_classify_context.owner_document_by_node.get(row.source_node_id, row.source_node_id)
            ) in process_source_id_set
        ]

    def classify_active_pending(rows: list) -> list:
        return [
            row
            for row in rows
            if owner_source_id(
                process_classify_context.owner_document_by_node.get(row.source_node_id, row.source_node_id)
            ) in process_source_id_set
        ]

    def classify_active_llm(rows: list) -> list:
        return [
            row
            for row in rows
            if owner_source_id(
                process_classify_context.owner_document_by_node.get(row.source_id, row.source_id)
            ) in process_source_id_set
        ]

    if process_candidate_unit_ids:
        model_reused_unit_ids = reusable_substage_unit_ids(
            layout,
            stage_name,
            "model",
            process_candidate_unit_ids,
            force_rebuild=ctx.force_rebuild,
        )
        model_process_candidates = [
            row for row in process_candidates if row.id not in set(model_reused_unit_ids)
        ]
        ctx.stage_record.artifact_paths = classify_artifact_paths(ctx)
        write_stage_state(
            layout,
            build_stage_manifest(
                stage_name=stage_name,
                layout=layout,
                job_id=ctx.job_id,
                build_target=ctx.source_path_label,
                source_ids=ctx.selected_source_ids,
                processed_source_ids=[],
                unit_ids=candidate_unit_ids,
                processed_unit_ids=[],
                input_stage=ctx.graph_input_stage[stage_name],
                artifact_paths=ctx.stage_record.artifact_paths,
                stats={},
                status="running",
                substage_states=substage_states,
            ),
        )

        if model_process_candidates:
            def checkpoint_classify_model(
                snapshot_results: list,
                snapshot_pending: list,
                snapshot_stats: dict[str, object],
                completed_units: int,
                total_units: int,
                processed_checkpoint_source_ids: list[str],
                processed_checkpoint_unit_ids: list[str],
            ) -> None:
                del processed_checkpoint_source_ids, snapshot_stats
                merged_results = replace_classify_outputs_by_unit_ids(
                    existing_results,
                    snapshot_results,
                    graph_bundle=base_graph,
                    active_unit_ids=set(processed_checkpoint_unit_ids),
                )
                merged_pending = replace_classify_pending_by_unit_ids(
                    existing_pending,
                    snapshot_pending,
                    active_unit_ids=set(processed_checkpoint_unit_ids),
                )
                write_classify_results(layout.classify_results_path(), merged_results)
                write_classify_pending(layout.classify_pending_path(), merged_pending)
                model_output_unit_ids = [row.id for row in classify_active_pending(merged_pending)]
                substage_states["model"] = build_unit_substage_manifest(
                    layout=layout,
                    parent_stage="classify",
                    stage_name="model",
                    processed_units=normalize_unit_ids(model_reused_unit_ids + processed_checkpoint_unit_ids),
                    stats=build_stage_work_stats(
                        input_source_ids=process_source_ids,
                        processed_source_ids=candidate_source_ids(processed_checkpoint_unit_ids),
                        skipped_source_ids=candidate_source_ids(model_reused_unit_ids),
                        work_units_total=total_units,
                        work_units_completed=completed_units,
                        work_units_skipped=len(model_reused_unit_ids),
                        candidate_count=len(process_candidate_unit_ids),
                        result_count=len(classify_active_results(merged_results)),
                        pending_count=len(model_output_unit_ids),
                    ),
                )
                ctx.stage_record.stats = build_stage_work_stats(
                    input_source_ids=input_source_ids,
                    processed_source_ids=candidate_source_ids(processed_checkpoint_unit_ids),
                    skipped_source_ids=skipped_source_ids,
                    work_units_total=len(candidate_unit_ids),
                    work_units_completed=len(processed_checkpoint_unit_ids),
                    work_units_skipped=skipped_units + len(model_reused_unit_ids),
                    updated_edges=len(classify_active_results(merged_results)),
                    candidate_count=len(candidate_unit_ids),
                    result_count=len(classify_active_results(merged_results)),
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
                        processed_source_ids=candidate_source_ids(processed_checkpoint_unit_ids),
                        unit_ids=candidate_unit_ids,
                        processed_unit_ids=[],
                        input_stage=ctx.graph_input_stage[stage_name],
                        artifact_paths=ctx.stage_record.artifact_paths,
                        stats=ctx.stage_record.stats,
                        status="running",
                        substage_states=substage_states,
                    ),
                )
                write_job_log(layout.job_log_path(ctx.job_id), ctx.log_record)

            model_phase = _patched_orchestrator_callable("run_model_phase", run_model_phase)
            model_result = model_phase(
                base_graph,
                ctx.runtime,
                model_process_candidates,
                progress_callback=offset_stage_progress_callback(
                    ctx.stage_progress_callback,
                    f"{stage_name}::model",
                    skipped_units=len(model_reused_unit_ids),
                    total_units=len(process_candidate_unit_ids),
                ),
                checkpoint_every=model_checkpoint_every,
                checkpoint_callback=checkpoint_classify_model,
                cancel_event=ctx.cancel_event,
            )
            merged_results = replace_classify_outputs_by_unit_ids(
                existing_results,
                model_result.results,
                graph_bundle=base_graph,
                active_unit_ids=set(model_result.processed_candidate_ids),
            )
            merged_pending = replace_classify_pending_by_unit_ids(
                existing_pending,
                model_result.pending_records,
                active_unit_ids=set(model_result.processed_candidate_ids),
            )
            write_classify_results(layout.classify_results_path(), merged_results)
            write_classify_pending(layout.classify_pending_path(), merged_pending)
            existing_results = merged_results
            existing_pending = merged_pending
            model_output_unit_ids = [row.id for row in classify_active_pending(merged_pending)]
            substage_states["model"] = build_unit_substage_manifest(
                layout=layout,
                parent_stage="classify",
                stage_name="model",
                processed_units=normalize_unit_ids(model_reused_unit_ids + model_result.processed_candidate_ids),
                stats=build_stage_work_stats(
                    input_source_ids=process_source_ids,
                    processed_source_ids=model_result.processed_source_ids,
                    skipped_source_ids=candidate_source_ids(model_reused_unit_ids),
                    work_units_total=len(process_candidate_unit_ids),
                    work_units_completed=len(model_result.processed_candidate_ids),
                    work_units_skipped=len(model_reused_unit_ids),
                    candidate_count=len(process_candidate_unit_ids),
                    result_count=len(classify_active_results(merged_results)),
                    pending_count=len(model_output_unit_ids),
                ),
            )
        else:
            emit_prefilled_stage_progress(
                ctx.stage_progress_callback,
                f"{stage_name}::model",
                current=len(model_reused_unit_ids),
                total=len(process_candidate_unit_ids),
            )
            model_result = type(
                "EmptyModelResult",
                (),
                {
                    "processed_candidate_ids": [],
                    "processed_source_ids": [],
                    "pending_records": [],
                    "results": [],
                    "stats": {},
                },
            )()
            model_output_unit_ids = [row.id for row in classify_active_pending(existing_pending)]
            substage_states["model"] = build_unit_substage_manifest(
                layout=layout,
                parent_stage="classify",
                stage_name="model",
                processed_units=normalize_unit_ids(model_reused_unit_ids),
                stats=build_stage_work_stats(
                    input_source_ids=process_source_ids,
                    processed_source_ids=[],
                    skipped_source_ids=candidate_source_ids(model_reused_unit_ids),
                    work_units_total=len(process_candidate_unit_ids),
                    work_units_completed=0,
                    work_units_skipped=len(model_reused_unit_ids),
                    candidate_count=len(process_candidate_unit_ids),
                    result_count=len(classify_active_results(existing_results)),
                    pending_count=len(model_output_unit_ids),
                ),
            )

        llm_scope_unit_ids = list(model_output_unit_ids)
        llm_unit_source_map = {
            unit_id: candidate_source_map[unit_id]
            for unit_id in llm_scope_unit_ids
            if unit_id in candidate_source_map
        }
        llm_reused_unit_ids = reusable_substage_unit_ids(
            layout,
            stage_name,
            "judge",
            llm_scope_unit_ids,
            force_rebuild=ctx.force_rebuild,
        )
        llm_process_pending = [
            row
            for row in classify_active_pending(existing_pending)
            if row.id in set(llm_scope_unit_ids) and row.id not in set(llm_reused_unit_ids)
        ]
        existing_active_results = {row.id: row for row in classify_active_results(existing_results)}
        existing_active_llm = {
            (row.id or f"{row.source_id}:{row.text}:{row.label}"): row
            for row in classify_active_llm(existing_llm)
        }
        if llm_process_pending:
            def checkpoint_classify_llm(snapshot_results: list, snapshot_llm_judgments: list, completed_units: int) -> None:
                cumulative_results = dict(existing_active_results)
                cumulative_results.update({row.id: row for row in snapshot_results})
                cumulative_llm = dict(existing_active_llm)
                cumulative_llm.update(
                    {
                        (row.id or f"{row.source_id}:{row.text}:{row.label}"): row
                        for row in snapshot_llm_judgments
                    }
                )
                llm_processed_unit_ids = [row.id for row in snapshot_results]
                merged_results = replace_classify_outputs_by_unit_ids(
                    existing_results,
                    list(cumulative_results.values()),
                    graph_bundle=base_graph,
                    active_unit_ids=set(llm_processed_unit_ids),
                )
                merged_llm = replace_llm_judge_details_by_unit_ids(
                    existing_llm,
                    list(cumulative_llm.values()),
                    active_unit_ids=set(llm_processed_unit_ids),
                )
                write_classify_results(layout.classify_results_path(), merged_results)
                write_llm_judge_details(layout.classify_llm_judge_path(), merged_llm)
                llm_completed_unit_ids = sorted(
                    set(llm_reused_unit_ids) | set(llm_processed_unit_ids)
                )
                substage_states["judge"] = build_unit_substage_manifest(
                    layout=layout,
                    parent_stage="classify",
                    stage_name="judge",
                    processed_units=llm_completed_unit_ids,
                    stats=build_stage_work_stats(
                        input_source_ids=source_ids_for_unit_ids(llm_scope_unit_ids, llm_unit_source_map),
                        processed_source_ids=source_ids_for_unit_ids(llm_completed_unit_ids, llm_unit_source_map),
                        skipped_source_ids=source_ids_for_unit_ids(llm_reused_unit_ids, llm_unit_source_map),
                        work_units_total=len(llm_scope_unit_ids),
                        work_units_completed=completed_units,
                        work_units_skipped=len(llm_reused_unit_ids),
                        result_count=len(classify_active_results(merged_results)),
                        llm_judgment_count=len(cumulative_llm),
                    ),
                )
                completed_stage_candidate_ids = normalize_unit_ids(
                    model_result.processed_candidate_ids + llm_processed_unit_ids
                )
                ctx.stage_record.stats = build_stage_work_stats(
                    input_source_ids=input_source_ids,
                    processed_source_ids=candidate_source_ids(completed_stage_candidate_ids),
                    skipped_source_ids=skipped_source_ids,
                    work_units_total=len(candidate_unit_ids),
                    work_units_completed=len(completed_candidate_unit_ids) + len(completed_stage_candidate_ids),
                    work_units_skipped=skipped_units + len(model_reused_unit_ids) + len(llm_reused_unit_ids),
                    updated_edges=len(classify_active_results(merged_results)),
                    candidate_count=len(candidate_unit_ids),
                    result_count=len(classify_active_results(merged_results)),
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
                        processed_source_ids=candidate_source_ids(completed_stage_candidate_ids),
                        unit_ids=candidate_unit_ids,
                        processed_unit_ids=[],
                        input_stage=ctx.graph_input_stage[stage_name],
                        artifact_paths=ctx.stage_record.artifact_paths,
                        stats=ctx.stage_record.stats,
                        status="running",
                        substage_states=substage_states,
                    ),
                )
                write_job_log(layout.job_log_path(ctx.job_id), ctx.log_record)

            llm_phase = _patched_orchestrator_callable("run_llm_phase", run_llm_phase)
            llm_result = llm_phase(
                runtime=ctx.runtime,
                pending_records=llm_process_pending,
                stats=dict(model_result.stats if model_process_candidates else {}),
                progress_callback=offset_stage_progress_callback(
                    ctx.stage_progress_callback,
                    f"{stage_name}::judge",
                    skipped_units=len(llm_reused_unit_ids),
                    total_units=len(llm_scope_unit_ids),
                ),
                checkpoint_every=judge_checkpoint_every,
                checkpoint_callback=checkpoint_classify_llm,
                cancel_event=ctx.cancel_event,
            )
            cumulative_results = dict(existing_active_results)
            cumulative_results.update({row.id: row for row in llm_result.results})
            cumulative_llm = dict(existing_active_llm)
            cumulative_llm.update(
                {
                    (row.id or f"{row.source_id}:{row.text}:{row.label}"): row
                    for row in llm_result.llm_judgments
                }
            )
            llm_processed_unit_ids = [row.id for row in llm_result.results]
            merged_results = replace_classify_outputs_by_unit_ids(
                existing_results,
                list(cumulative_results.values()),
                graph_bundle=base_graph,
                active_unit_ids=set(llm_processed_unit_ids),
            )
            merged_llm = replace_llm_judge_details_by_unit_ids(
                existing_llm,
                list(cumulative_llm.values()),
                active_unit_ids=set(llm_processed_unit_ids),
            )
            write_classify_results(layout.classify_results_path(), merged_results)
            write_llm_judge_details(layout.classify_llm_judge_path(), merged_llm)
            existing_results = merged_results
            existing_llm = merged_llm
            llm_completed_unit_ids = sorted(
                set(llm_reused_unit_ids) | set(llm_processed_unit_ids)
            )
            substage_states["judge"] = build_unit_substage_manifest(
                layout=layout,
                parent_stage="classify",
                stage_name="judge",
                processed_units=llm_completed_unit_ids,
                stats=build_stage_work_stats(
                    input_source_ids=source_ids_for_unit_ids(llm_scope_unit_ids, llm_unit_source_map),
                    processed_source_ids=source_ids_for_unit_ids(llm_completed_unit_ids, llm_unit_source_map),
                    skipped_source_ids=source_ids_for_unit_ids(llm_reused_unit_ids, llm_unit_source_map),
                    work_units_total=len(llm_scope_unit_ids),
                    work_units_completed=len(llm_result.results),
                    work_units_skipped=len(llm_reused_unit_ids),
                    result_count=len(classify_active_results(merged_results)),
                    llm_judgment_count=len(cumulative_llm),
                ),
            )
            llm_errors = llm_result.llm_errors
        else:
            emit_prefilled_stage_progress(
                ctx.stage_progress_callback,
                f"{stage_name}::judge",
                current=len(llm_reused_unit_ids),
                total=len(llm_scope_unit_ids),
            )
            llm_result = type(
                "EmptyLlmResult",
                (),
                {
                    "results": [],
                    "llm_judgments": [],
                    "llm_errors": [],
                },
            )()
            substage_states["judge"] = build_unit_substage_manifest(
                layout=layout,
                parent_stage="classify",
                stage_name="judge",
                processed_units=normalize_unit_ids(llm_reused_unit_ids),
                stats=build_stage_work_stats(
                    input_source_ids=source_ids_for_unit_ids(llm_scope_unit_ids, llm_unit_source_map) if llm_scope_unit_ids else [],
                    processed_source_ids=[],
                    skipped_source_ids=source_ids_for_unit_ids(llm_reused_unit_ids, llm_unit_source_map) if llm_reused_unit_ids else [],
                    work_units_total=len(llm_scope_unit_ids),
                    work_units_completed=0,
                    work_units_skipped=len(llm_reused_unit_ids),
                    result_count=len(classify_active_results(existing_results)),
                ),
            )
            llm_errors = []

        current_graph = materialize_classify_results(base_graph, existing_results)
        write_stage_graph(layout, stage_name, current_graph, write_nodes=False, write_edges=True)
        processed_candidate_unit_ids = normalize_unit_ids(
            completed_candidate_unit_ids
            + model_result.processed_candidate_ids
            + [row.id for row in llm_result.results]
        )
        ctx.stage_record.stats = build_stage_work_stats(
            input_source_ids=input_source_ids,
            processed_source_ids=candidate_source_ids(processed_candidate_unit_ids),
            skipped_source_ids=skipped_source_ids,
            work_units_total=len(candidate_unit_ids),
            work_units_completed=len(processed_candidate_unit_ids),
            work_units_skipped=skipped_units + len(model_reused_unit_ids) + len(llm_reused_unit_ids),
            updated_edges=len(classify_active_results(existing_results)),
            candidate_count=len(candidate_unit_ids),
            result_count=len(classify_active_results(existing_results)),
        )
        ctx.stage_record.failures = [dict(item) for item in llm_errors]
    else:
        current_graph = (
            load_graph_snapshot(
                layout,
                stage_name,
                stage_sequence=ctx.stage_sequence,
                graph_stages=ctx.graph_stages,
            )
            if stage_outputs_exist(layout, stage_name)
            else GraphBundle()
        )
        ctx.stage_record.stats = build_stage_work_stats(
            input_source_ids=input_source_ids,
            processed_source_ids=[],
            skipped_source_ids=skipped_source_ids,
            work_units_total=len(candidate_unit_ids),
            work_units_completed=0,
            work_units_skipped=skipped_units,
            candidate_count=len(candidate_unit_ids),
        )
    ctx.stage_record.artifact_paths = classify_artifact_paths(ctx)
    manifest_processed_unit_ids = normalize_unit_ids(
        completed_candidate_unit_ids
        + substage_states.get("model", SubstageStateManifest()).processed_units
        + substage_states.get("judge", SubstageStateManifest()).processed_units
    )
    write_stage_state(
        layout,
        build_stage_manifest(
            stage_name=stage_name,
            layout=layout,
            job_id=ctx.job_id,
            build_target=ctx.source_path_label,
            source_ids=ctx.selected_source_ids,
            processed_source_ids=candidate_source_ids(manifest_processed_unit_ids),
            unit_ids=candidate_unit_ids,
            processed_unit_ids=[],
            input_stage=ctx.graph_input_stage[stage_name],
            artifact_paths=ctx.stage_record.artifact_paths,
            stats=ctx.stage_record.stats,
            graph_bundle=current_graph,
            substage_states=substage_states,
        ),
    )
    return HandlerResult(current_graph=current_graph)
