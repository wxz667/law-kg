from __future__ import annotations

from ...contracts import GraphBundle
from ...io import read_reference_candidates, write_job_log, write_reference_candidates
from ...stages import run_detect
from .graph import (
    filter_reference_candidates_by_graph,
    owner_document_by_node,
    owner_source_id_for_node,
    replace_detect_outputs,
)
from .common import (
    HandlerResult,
    StageContext,
    build_stage_manifest,
    build_stage_work_stats,
    emit_prefilled_stage_progress,
    finalize_stage_work_stats,
    load_graph_snapshot,
    normalize_source_ids,
    normalize_unit_ids,
    offset_stage_progress_callback,
    resolve_stage_source_ids,
    reusable_stage_unit_ids,
    write_stage_state,
)


def detect_artifact_paths(ctx: StageContext) -> dict[str, str]:
    return {
        "primary": str(ctx.layout.detect_candidates_path()),
        "candidates": str(ctx.layout.detect_candidates_path()),
    }


def detect_unit_ids(
    graph_bundle: GraphBundle,
    *,
    active_source_ids: set[str],
) -> list[str]:
    owners = owner_document_by_node(graph_bundle)
    document_nodes = {
        node.id: node
        for node in graph_bundle.nodes
        if node.level == "document" and owner_source_id_for_node(owners, node.id) in active_source_ids
    }
    units: list[str] = []
    for document_id, node in sorted(document_nodes.items()):
        if str(getattr(node, "name", "") or "").strip():
            units.append(document_id)
    for node in sorted(graph_bundle.nodes, key=lambda item: item.id):
        if node.level not in {"article", "paragraph", "item", "sub_item", "segment"}:
            continue
        if not str(getattr(node, "text", "") or "").strip():
            continue
        if owner_source_id_for_node(owners, node.id) not in active_source_ids:
            continue
        units.append(node.id)
    return normalize_unit_ids(units)


def detect_unit_ids_for_sources(
    graph_bundle: GraphBundle,
    *,
    active_source_ids: set[str],
    selected_source_ids: set[str],
) -> list[str]:
    owners = owner_document_by_node(graph_bundle)
    return [
        unit_id
        for unit_id in detect_unit_ids(graph_bundle, active_source_ids=selected_source_ids)
        if owner_source_id_for_node(owners, unit_id) in active_source_ids
    ]


def run(ctx: StageContext) -> HandlerResult:
    input_source_ids = resolve_stage_source_ids(ctx.layout, ctx.stage_name, ctx.selected_source_ids)
    base_graph = load_graph_snapshot(
        ctx.layout,
        ctx.graph_input_stage[ctx.stage_name],
        stage_sequence=ctx.stage_sequence,
        graph_stages=ctx.graph_stages,
    )
    scoped_detect_unit_ids = detect_unit_ids(base_graph, active_source_ids=set(input_source_ids))
    completed_detect_unit_ids = reusable_stage_unit_ids(
        ctx.layout,
        ctx.stage_name,
        scoped_detect_unit_ids,
        force_rebuild=ctx.force_rebuild,
    )
    skipped_units = len(completed_detect_unit_ids)
    completed_detect_unit_id_set = set(completed_detect_unit_ids)
    detect_owner_by_node = owner_document_by_node(base_graph)
    skipped_source_ids = sorted(
        {
            owner_source_id_for_node(detect_owner_by_node, unit_id)
            for unit_id in completed_detect_unit_ids
        }
    )
    process_source_ids = sorted(
        {
            owner_source_id_for_node(detect_owner_by_node, unit_id)
            for unit_id in scoped_detect_unit_ids
            if unit_id not in completed_detect_unit_id_set
        }
    )
    checkpoint_every = ctx.runtime.stage_checkpoint_every(ctx.stage_name)
    existing_rows = (
        read_reference_candidates(ctx.layout.detect_candidates_path())
        if ctx.layout.detect_candidates_path().exists()
        else []
    )
    if process_source_ids:
        emit_prefilled_stage_progress(
            ctx.stage_progress_callback,
            ctx.stage_name,
            current=skipped_units,
            total=len(scoped_detect_unit_ids),
        )

        def checkpoint_detect(
            snapshot_candidates: list,
            snapshot_stats: dict[str, object],
            snapshot_profiling: dict[str, object],
            processed_checkpoint_source_ids: list[str],
        ) -> None:
            del snapshot_profiling
            normalized_checkpoint_source_ids = normalize_source_ids(processed_checkpoint_source_ids)
            merged_snapshot = replace_detect_outputs(
                existing_rows,
                snapshot_candidates,
                graph_bundle=base_graph,
                active_source_ids=set(process_source_ids),
            )
            write_reference_candidates(ctx.layout.detect_candidates_path(), merged_snapshot)
            ctx.stage_record.artifact_paths = detect_artifact_paths(ctx)
            ctx.stage_record.stats = finalize_stage_work_stats(
                dict(snapshot_stats),
                input_source_ids=input_source_ids,
                processed_source_ids=normalized_checkpoint_source_ids,
                skipped_source_ids=skipped_source_ids,
                skipped_work_units=skipped_units,
            )
            write_stage_state(
                ctx.layout,
                build_stage_manifest(
                    stage_name=ctx.stage_name,
                    layout=ctx.layout,
                    job_id=ctx.job_id,
                    build_target=ctx.source_path_label,
                    source_ids=ctx.selected_source_ids,
                    processed_source_ids=normalized_checkpoint_source_ids,
                    unit_ids=scoped_detect_unit_ids,
                    processed_unit_ids=detect_unit_ids_for_sources(
                        base_graph,
                        active_source_ids=set(normalized_checkpoint_source_ids),
                        selected_source_ids=set(input_source_ids),
                    ) + completed_detect_unit_ids,
                    input_stage=ctx.graph_input_stage[ctx.stage_name],
                    artifact_paths=ctx.stage_record.artifact_paths,
                    stats=ctx.stage_record.stats,
                    status="running",
                ),
            )
            write_job_log(ctx.layout.job_log_path(ctx.job_id), ctx.log_record)

        result = run_detect(
            base_graph,
            ctx.runtime,
            source_document_ids=set(process_source_ids),
            progress_callback=offset_stage_progress_callback(
                ctx.stage_progress_callback,
                ctx.stage_name,
                skipped_units=skipped_units,
                total_units=len(scoped_detect_unit_ids),
            ),
            checkpoint_every=checkpoint_every,
            checkpoint_callback=checkpoint_detect,
            cancel_event=ctx.cancel_event,
        )
        merged_rows = replace_detect_outputs(
            existing_rows,
            result.candidates,
            graph_bundle=base_graph,
            active_source_ids=set(process_source_ids),
        )
        merged_rows = filter_reference_candidates_by_graph(merged_rows, graph_bundle=base_graph)
        write_reference_candidates(ctx.layout.detect_candidates_path(), merged_rows)
        ctx.stage_record.stats = finalize_stage_work_stats(
            dict(result.stats),
            input_source_ids=input_source_ids,
            processed_source_ids=process_source_ids,
            skipped_source_ids=skipped_source_ids,
            skipped_work_units=skipped_units,
        )
    else:
        emit_prefilled_stage_progress(
            ctx.stage_progress_callback,
            ctx.stage_name,
            current=skipped_units,
            total=len(scoped_detect_unit_ids),
        )
        ctx.stage_record.stats = build_stage_work_stats(
            input_source_ids=input_source_ids,
            processed_source_ids=[],
            skipped_source_ids=skipped_source_ids,
            work_units_total=len(scoped_detect_unit_ids),
            work_units_completed=0,
            work_units_skipped=skipped_units,
            candidate_count=len(existing_rows),
        )
    ctx.stage_record.artifact_paths = detect_artifact_paths(ctx)
    write_stage_state(
        ctx.layout,
        build_stage_manifest(
            stage_name=ctx.stage_name,
            layout=ctx.layout,
            job_id=ctx.job_id,
            build_target=ctx.source_path_label,
            source_ids=ctx.selected_source_ids,
            processed_source_ids=process_source_ids,
            unit_ids=scoped_detect_unit_ids,
            processed_unit_ids=completed_detect_unit_ids + detect_unit_ids_for_sources(
                base_graph,
                active_source_ids=set(process_source_ids),
                selected_source_ids=set(input_source_ids),
            ),
            input_stage=ctx.graph_input_stage[ctx.stage_name],
            artifact_paths=ctx.stage_record.artifact_paths,
            stats=ctx.stage_record.stats,
        ),
    )
    return HandlerResult(current_graph=None)
