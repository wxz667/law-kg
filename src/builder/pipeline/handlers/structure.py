from __future__ import annotations

from ...contracts import GraphBundle
from ...io import write_job_log
from ...stages import run_structure
from .graph import replace_document_subgraphs
from .common import (
    HandlerResult,
    StageContext,
    build_stage_manifest,
    build_stage_work_stats,
    emit_prefilled_stage_progress,
    graph_artifact_paths,
    load_graph_snapshot,
    offset_stage_progress_callback,
    resolve_stage_source_ids,
    reusable_graph_source_ids,
    stage_outputs_exist,
    subtract_source_ids,
    write_stage_graph,
    write_stage_state,
)


def run(ctx: StageContext) -> HandlerResult:
    input_source_ids = resolve_stage_source_ids(ctx.layout, ctx.stage_name, ctx.selected_source_ids)
    skipped_source_ids = reusable_graph_source_ids(
        ctx.layout,
        ctx.stage_name,
        input_source_ids,
        require_nodes=True,
        require_edges=True,
        force_rebuild=ctx.force_rebuild,
    )
    process_source_ids = subtract_source_ids(input_source_ids, skipped_source_ids)
    replacement_graph = GraphBundle()
    current_graph: GraphBundle | None = None
    checkpoint_every = ctx.runtime.stage_checkpoint_every(ctx.stage_name)
    if process_source_ids:
        emit_prefilled_stage_progress(
            ctx.stage_progress_callback,
            ctx.stage_name,
            current=len(skipped_source_ids),
            total=len(input_source_ids),
        )

        def checkpoint_structure(
            snapshot_graph: GraphBundle,
            processed_checkpoint_source_ids: list[str],
            completed_units: int,
            total_units: int,
        ) -> None:
            checkpoint_graph = (
                replace_document_subgraphs(
                    load_graph_snapshot(
                        ctx.layout,
                        ctx.stage_name,
                        stage_sequence=ctx.stage_sequence,
                        graph_stages=ctx.graph_stages,
                    ),
                    snapshot_graph,
                    active_source_ids=set(processed_checkpoint_source_ids),
                    stage_name=ctx.stage_name,
                )
                if stage_outputs_exist(ctx.layout, ctx.stage_name)
                else snapshot_graph
            )
            write_stage_graph(ctx.layout, ctx.stage_name, checkpoint_graph, write_nodes=True, write_edges=True)
            ctx.stage_record.artifact_paths = graph_artifact_paths(ctx.layout, ctx.stage_name)
            ctx.stage_record.stats = build_stage_work_stats(
                input_source_ids=input_source_ids,
                processed_source_ids=processed_checkpoint_source_ids,
                skipped_source_ids=skipped_source_ids,
                work_units_total=total_units,
                work_units_completed=completed_units,
                updated_nodes=len(snapshot_graph.nodes),
                updated_edges=len(snapshot_graph.edges),
                nodes=len(checkpoint_graph.nodes),
                edges=len(checkpoint_graph.edges),
            )
            write_stage_state(
                ctx.layout,
                build_stage_manifest(
                    stage_name=ctx.stage_name,
                    layout=ctx.layout,
                    job_id=ctx.job_id,
                    build_target=ctx.source_path_label,
                    source_ids=ctx.selected_source_ids,
                    processed_source_ids=processed_checkpoint_source_ids,
                    input_stage="",
                    artifact_paths=ctx.stage_record.artifact_paths,
                    stats=ctx.stage_record.stats,
                    graph_bundle=checkpoint_graph,
                    status="running",
                ),
            )
            write_job_log(ctx.layout.job_log_path(ctx.job_id), ctx.log_record)

        replacement_graph = run_structure(
            ctx.data_root,
            source_ids=process_source_ids,
            progress_callback=offset_stage_progress_callback(
                ctx.stage_progress_callback,
                ctx.stage_name,
                skipped_units=len(skipped_source_ids),
                total_units=len(input_source_ids),
            ),
            checkpoint_every=checkpoint_every,
            checkpoint_callback=checkpoint_structure,
        )
        if stage_outputs_exist(ctx.layout, ctx.stage_name):
            current_graph = replace_document_subgraphs(
                load_graph_snapshot(
                    ctx.layout,
                    ctx.stage_name,
                    stage_sequence=ctx.stage_sequence,
                    graph_stages=ctx.graph_stages,
                ),
                replacement_graph,
                active_source_ids=set(process_source_ids),
                stage_name=ctx.stage_name,
            )
        else:
            current_graph = replacement_graph
        write_stage_graph(ctx.layout, ctx.stage_name, current_graph, write_nodes=True, write_edges=True)
    else:
        current_graph = (
            load_graph_snapshot(
                ctx.layout,
                ctx.stage_name,
                stage_sequence=ctx.stage_sequence,
                graph_stages=ctx.graph_stages,
            )
            if stage_outputs_exist(ctx.layout, ctx.stage_name)
            else GraphBundle()
        )
        emit_prefilled_stage_progress(
            ctx.stage_progress_callback,
            ctx.stage_name,
            current=len(skipped_source_ids),
            total=len(input_source_ids),
        )
    ctx.stage_record.artifact_paths = graph_artifact_paths(ctx.layout, ctx.stage_name)
    ctx.stage_record.stats = build_stage_work_stats(
        input_source_ids=input_source_ids,
        processed_source_ids=process_source_ids,
        skipped_source_ids=skipped_source_ids,
        updated_nodes=len(replacement_graph.nodes),
        updated_edges=len(replacement_graph.edges),
        nodes=len(current_graph.nodes),
        edges=len(current_graph.edges),
    )
    write_stage_state(
        ctx.layout,
        build_stage_manifest(
            stage_name=ctx.stage_name,
            layout=ctx.layout,
            job_id=ctx.job_id,
            build_target=ctx.source_path_label,
            source_ids=ctx.selected_source_ids,
            processed_source_ids=process_source_ids,
            input_stage="",
            artifact_paths=ctx.stage_record.artifact_paths,
            stats=ctx.stage_record.stats,
            graph_bundle=current_graph,
        ),
    )
    return HandlerResult(current_graph=current_graph)
