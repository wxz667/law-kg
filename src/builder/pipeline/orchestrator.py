from __future__ import annotations

import json
import shutil
import threading
from pathlib import Path
from typing import Callable

from ..contracts import GraphBundle, JobLogRecord, StageRecord, StageStateManifest
from ..io import (
    BuildLayout,
    ensure_stage_dirs,
    read_llm_judge_details,
    read_normalize_index,
    read_reference_candidates,
    read_relation_plans,
    read_stage_edges,
    read_stage_manifest,
    read_stage_nodes,
    write_job_log,
    write_json,
    write_llm_judge_details,
    write_normalize_index,
    write_reference_candidates,
    write_relation_plans,
    write_stage_edges,
    write_stage_manifest,
    write_stage_nodes,
)
from ..stages import (
    run_entity_alignment,
    run_entity_extraction,
    run_implicit_reasoning,
    run_normalize,
    run_reference_filter,
    run_relation_classify,
    run_structure,
)
from ..stages.reference_filter.run import count_reference_filter_units
from ..stages.relation_classify.run import count_relation_classify_units
from ..stages.relation_classify.materialize import materialize_relation_plans
from ..utils.ids import slugify, timestamp_utc
from .incremental import (
    filter_reference_candidates_by_graph,
    filter_relation_classify_outputs_by_graph,
    replace_document_subgraphs,
    replace_llm_judge_details,
    replace_reference_filter_outputs,
    replace_relation_classify_outputs,
)
from .runtime import PipelineRuntime

STAGE_SEQUENCE = (
    "normalize",
    "structure",
    "reference_filter",
    "relation_classify",
    "entity_extraction",
    "entity_alignment",
    "implicit_reasoning",
)

GRAPH_STAGES = {"structure", "relation_classify", "entity_extraction", "entity_alignment", "implicit_reasoning"}
GRAPH_NODE_OUTPUT_STAGES = {"structure", "entity_extraction", "entity_alignment"}
GRAPH_EDGE_OUTPUT_STAGES = {"structure", "relation_classify", "entity_extraction", "entity_alignment", "implicit_reasoning"}
GRAPH_INPUT_STAGE = {
    "structure": "",
    "reference_filter": "structure",
    "relation_classify": "structure",
    "entity_extraction": "relation_classify",
    "entity_alignment": "entity_extraction",
    "implicit_reasoning": "entity_alignment",
}


def build_knowledge_graph(
    *,
    source_id: str,
    data_root: Path,
    start_stage: str | None = None,
    through_stage: str = "implicit_reasoning",
    force_rebuild: bool = False,
    incremental: bool = False,
    report_progress: bool = False,
    stage_callback: Callable[[int, int], None] | None = None,
    stage_progress_callback: Callable[[str, int, int], None] | None = None,
    stage_name_callback: Callable[[str], None] | None = None,
    stage_summary_callback: Callable[[str, dict[str, int]], None] | None = None,
    finalizing_callback: Callable[[str], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> dict[str, object]:
    del report_progress
    del incremental
    return _build_from_source_ids(
        source_ids=[source_id],
        data_root=data_root,
        start_stage=start_stage,
        through_stage=through_stage,
        force_rebuild=force_rebuild,
        job_id=f"build-{slugify(source_id)}",
        source_path_label=source_id,
        stage_callback=stage_callback,
        stage_progress_callback=stage_progress_callback,
        stage_name_callback=stage_name_callback,
        stage_summary_callback=stage_summary_callback,
        finalizing_callback=finalizing_callback,
        cancel_event=cancel_event,
    )


def build_batch_knowledge_graph(
    data_root: Path,
    pattern: str = "*.docx",
    category: str | list[str] | None = None,
    start_stage: str | None = None,
    through_stage: str = "implicit_reasoning",
    force_rebuild: bool = False,
    incremental: bool = False,
    report_progress: bool = False,
    discovery_callback: Callable[[int], None] | None = None,
    progress_callback: Callable[[str, int, int], None] | None = None,
    stage_summary_callback: Callable[[str, dict[str, int]], None] | None = None,
    finalizing_callback: Callable[[str], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> dict[str, object]:
    del pattern
    del report_progress
    del incremental
    data_root = data_root.resolve()
    source_ids = discover_source_ids(data_root, category=category)
    if discovery_callback is not None:
        discovery_callback(len(source_ids))
    return _build_from_source_ids(
        source_ids=source_ids,
        data_root=data_root,
        start_stage=start_stage,
        through_stage=through_stage,
        force_rebuild=force_rebuild,
        clear_stage_range_on_rebuild=force_rebuild and category is None,
        job_id=f"batch-{timestamp_utc().replace(':', '').replace('-', '')}",
        source_path_label=f"batch:{','.join(category) if isinstance(category, list) else (category or 'all')}",
        stage_progress_callback=progress_callback,
        stage_summary_callback=stage_summary_callback,
        finalizing_callback=finalizing_callback,
        cancel_event=cancel_event,
    )


def _build_from_source_ids(
    *,
    source_ids: list[str],
    data_root: Path,
    start_stage: str | None,
    through_stage: str,
    force_rebuild: bool,
    job_id: str,
    source_path_label: str,
    clear_stage_range_on_rebuild: bool = False,
    stage_callback: Callable[[int, int], None] | None = None,
    stage_progress_callback: Callable[[str, int, int], None] | None = None,
    stage_name_callback: Callable[[str], None] | None = None,
    stage_summary_callback: Callable[[str, dict[str, int]], None] | None = None,
    finalizing_callback: Callable[[str], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> dict[str, object]:
    data_root = data_root.resolve()
    ensure_stage_dirs(data_root)
    layout = BuildLayout(data_root)
    start = start_stage or STAGE_SEQUENCE[0]
    stage_names = iter_stage_range(start, through_stage)
    if clear_stage_range_on_rebuild:
        clear_stage_range_outputs(layout, stage_names)
    selected_source_ids = sorted(dict.fromkeys(source_ids))
    log_record = JobLogRecord(
        job_id=job_id,
        build_target=source_path_label,
        data_root=str(data_root),
        status="running",
        started_at=timestamp_utc(),
        start_stage=start,
        end_stage=through_stage,
        source_count=len(selected_source_ids),
    )
    write_job_log(layout.job_log_path(job_id), log_record)
    runtime = PipelineRuntime(data_root)

    completed = 0
    total = len(stage_names)
    current_graph: GraphBundle | None = None

    for stage_name in stage_names:
        if cancel_event is not None and cancel_event.is_set():
            raise KeyboardInterrupt
        if stage_name_callback is not None:
            stage_name_callback(stage_name)
        stage_record = StageRecord(name=stage_name, status="running", started_at=timestamp_utc())
        log_record.stages.append(stage_record)
        write_job_log(layout.job_log_path(job_id), log_record)
        try:
            if stage_name == "normalize":
                checkpoint_every = runtime.stage_checkpoint_every(stage_name)

                def checkpoint_normalize(snapshot_index, partial_entries) -> None:
                    processed_source_ids = [
                        entry.source_id for entry in partial_entries if entry.status == "completed"
                    ]
                    stage_record.artifact_paths = {
                        "primary": str(layout.normalize_index_path()),
                        "index": str(layout.normalize_index_path()),
                        "log": str(layout.normalize_log_path()),
                    }
                    stage_record.stats = build_normalize_stage_stats(partial_entries)
                    write_normalize_index(layout.normalize_index_path(), snapshot_index)
                    write_json(layout.normalize_log_path(), snapshot_index.to_dict())
                    write_stage_state(
                        layout,
                        StageStateManifest(
                            stage_name="normalize",
                            build_target=source_path_label,
                            data_root=str(data_root),
                            job_id=job_id,
                            status="running",
                            source_ids=selected_source_ids,
                            processed_source_ids=processed_source_ids,
                            artifact_paths=dict(stage_record.artifact_paths),
                            updated_at=timestamp_utc(),
                            stats=dict(stage_record.stats),
                        ),
                    )
                    write_job_log(layout.job_log_path(job_id), log_record)

                normalize_index = run_normalize(
                    data_root,
                    source_ids=selected_source_ids,
                    force_rebuild=force_rebuild,
                    progress_callback=lambda current, total_items: emit_stage_progress(
                        stage_progress_callback, stage_name, current, total_items
                    ),
                    checkpoint_every=checkpoint_every,
                    checkpoint_callback=checkpoint_normalize,
                )
                selected_entries = select_normalize_entries(normalize_index, selected_source_ids)
                processed_source_ids = [
                    entry.source_id for entry in selected_entries if entry.status == "completed"
                ]
                stage_record.artifact_paths = {
                    "primary": str(layout.normalize_index_path()),
                    "index": str(layout.normalize_index_path()),
                    "log": str(layout.normalize_log_path()),
                }
                stage_record.stats = build_normalize_stage_stats(selected_entries)
                write_stage_state(
                    layout,
                    StageStateManifest(
                        stage_name="normalize",
                        build_target=source_path_label,
                        data_root=str(data_root),
                        job_id=job_id,
                        status="completed",
                        source_ids=selected_source_ids,
                        processed_source_ids=processed_source_ids,
                        artifact_paths=dict(stage_record.artifact_paths),
                        updated_at=timestamp_utc(),
                        stats=dict(stage_record.stats),
                    ),
                )
            elif stage_name == "structure":
                input_source_ids = resolve_stage_source_ids(layout, stage_name, selected_source_ids)
                skipped_source_ids = reusable_graph_source_ids(
                    layout,
                    stage_name,
                    input_source_ids,
                    require_nodes=True,
                    require_edges=True,
                    force_rebuild=force_rebuild,
                )
                process_source_ids = subtract_source_ids(input_source_ids, skipped_source_ids)
                replacement_graph = GraphBundle()
                checkpoint_every = runtime.stage_checkpoint_every(stage_name)
                if process_source_ids:
                    def checkpoint_structure(
                        snapshot_graph: GraphBundle,
                        processed_checkpoint_source_ids: list[str],
                        completed_units: int,
                        total_units: int,
                    ) -> None:
                        checkpoint_graph = (
                            replace_document_subgraphs(
                                load_graph_snapshot(layout, stage_name),
                                snapshot_graph,
                                active_source_ids=set(processed_checkpoint_source_ids),
                                stage_name=stage_name,
                            )
                            if stage_outputs_exist(layout, stage_name)
                            else snapshot_graph
                        )
                        write_stage_graph(layout, stage_name, checkpoint_graph, write_nodes=True, write_edges=True)
                        stage_record.artifact_paths = graph_artifact_paths(layout, stage_name)
                        stage_record.stats = build_stage_work_stats(
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
                            layout,
                            build_stage_manifest(
                                stage_name=stage_name,
                                layout=layout,
                                job_id=job_id,
                                build_target=source_path_label,
                                source_ids=selected_source_ids,
                                processed_source_ids=processed_checkpoint_source_ids,
                                input_stage="",
                                artifact_paths=stage_record.artifact_paths,
                                stats=stage_record.stats,
                                graph_bundle=checkpoint_graph,
                                status="running",
                            ),
                        )
                        write_job_log(layout.job_log_path(job_id), log_record)

                    replacement_graph = run_structure(
                        data_root,
                        source_ids=process_source_ids,
                        progress_callback=lambda current, total_items: emit_stage_progress(
                            stage_progress_callback, stage_name, current, total_items
                        ),
                        checkpoint_every=checkpoint_every,
                        checkpoint_callback=checkpoint_structure,
                    )
                    if stage_outputs_exist(layout, stage_name):
                        current_graph = replace_document_subgraphs(
                            load_graph_snapshot(layout, stage_name),
                            replacement_graph,
                            active_source_ids=set(process_source_ids),
                            stage_name=stage_name,
                        )
                    else:
                        current_graph = replacement_graph
                    write_stage_graph(layout, stage_name, current_graph, write_nodes=True, write_edges=True)
                else:
                    current_graph = load_graph_snapshot(layout, stage_name) if stage_outputs_exist(layout, stage_name) else GraphBundle()
                    emit_idle_stage_progress(stage_progress_callback, stage_name)
                stage_record.artifact_paths = graph_artifact_paths(layout, stage_name)
                stage_record.stats = build_stage_work_stats(
                    input_source_ids=input_source_ids,
                    processed_source_ids=process_source_ids,
                    skipped_source_ids=skipped_source_ids,
                    updated_nodes=len(replacement_graph.nodes),
                    updated_edges=len(replacement_graph.edges),
                    nodes=len(current_graph.nodes),
                    edges=len(current_graph.edges),
                )
                write_stage_state(
                    layout,
                    build_stage_manifest(
                        stage_name=stage_name,
                        layout=layout,
                        job_id=job_id,
                        build_target=source_path_label,
                        source_ids=selected_source_ids,
                        processed_source_ids=process_source_ids,
                        input_stage="",
                        artifact_paths=stage_record.artifact_paths,
                        stats=stage_record.stats,
                        graph_bundle=current_graph,
                    ),
                )
            elif stage_name == "reference_filter":
                input_source_ids = resolve_stage_source_ids(layout, stage_name, selected_source_ids)
                skipped_source_ids = reusable_artifact_source_ids(
                    layout,
                    stage_name,
                    input_source_ids,
                    force_rebuild=force_rebuild,
                )
                process_source_ids = subtract_source_ids(input_source_ids, skipped_source_ids)
                base_graph = load_graph_snapshot(layout, GRAPH_INPUT_STAGE[stage_name])
                checkpoint_every = runtime.stage_checkpoint_every(stage_name)
                existing_rows = (
                    read_reference_candidates(layout.reference_filter_candidates_path())
                    if layout.reference_filter_candidates_path().exists()
                    else []
                )
                skipped_units = count_reference_filter_units(base_graph, set(skipped_source_ids))
                if process_source_ids:

                    def checkpoint_reference_filter(
                        snapshot_candidates: list,
                        snapshot_stats: dict[str, object],
                        snapshot_profiling: dict[str, object],
                        processed_checkpoint_source_ids: list[str],
                    ) -> None:
                        merged_snapshot = replace_reference_filter_outputs(
                            existing_rows,
                            snapshot_candidates,
                            graph_bundle=base_graph,
                            active_source_ids=set(process_source_ids),
                        )
                        write_reference_candidates(layout.reference_filter_candidates_path(), merged_snapshot)
                        write_json(
                            layout.reference_filter_log_path(),
                            {
                                "status": "running",
                                "stats": finalize_stage_work_stats(
                                    dict(snapshot_stats),
                                    processed_source_ids=processed_checkpoint_source_ids,
                                    skipped_source_ids=skipped_source_ids,
                                    skipped_work_units=skipped_units,
                                ),
                                "profiling": snapshot_profiling,
                            },
                        )
                        stage_record.artifact_paths = {
                            "primary": str(layout.reference_filter_candidates_path()),
                            "candidates": str(layout.reference_filter_candidates_path()),
                        }
                        stage_record.stats = finalize_stage_work_stats(
                            dict(snapshot_stats),
                            processed_source_ids=processed_checkpoint_source_ids,
                            skipped_source_ids=skipped_source_ids,
                            skipped_work_units=skipped_units,
                        )
                        write_stage_state(
                            layout,
                            build_stage_manifest(
                                stage_name=stage_name,
                                layout=layout,
                                job_id=job_id,
                                build_target=source_path_label,
                                source_ids=selected_source_ids,
                                processed_source_ids=processed_checkpoint_source_ids,
                                input_stage=GRAPH_INPUT_STAGE[stage_name],
                                artifact_paths=stage_record.artifact_paths,
                                stats=stage_record.stats,
                                status="running",
                            ),
                        )
                        write_job_log(layout.job_log_path(job_id), log_record)

                    result = run_reference_filter(
                        base_graph,
                        runtime,
                        source_document_ids=set(process_source_ids),
                        progress_callback=lambda current, total_items: emit_stage_progress(
                            stage_progress_callback, stage_name, current, total_items
                        ),
                        checkpoint_every=checkpoint_every,
                        checkpoint_callback=checkpoint_reference_filter,
                        cancel_event=cancel_event,
                    )
                    merged_rows = replace_reference_filter_outputs(
                        existing_rows,
                        result.candidates,
                        graph_bundle=base_graph,
                        active_source_ids=set(process_source_ids),
                    )
                    merged_rows = filter_reference_candidates_by_graph(merged_rows, graph_bundle=base_graph)
                    write_reference_candidates(layout.reference_filter_candidates_path(), merged_rows)
                    stage_record.stats = finalize_stage_work_stats(
                        dict(result.stats),
                        processed_source_ids=process_source_ids,
                        skipped_source_ids=skipped_source_ids,
                        skipped_work_units=skipped_units,
                    )
                    write_json(
                        layout.reference_filter_log_path(),
                        {
                            "status": "completed",
                            "stats": dict(stage_record.stats),
                            "profiling": result.profiling,
                        },
                    )
                else:
                    emit_idle_stage_progress(stage_progress_callback, stage_name)
                    stage_record.stats = build_stage_work_stats(
                        input_source_ids=input_source_ids,
                        processed_source_ids=[],
                        skipped_source_ids=skipped_source_ids,
                        work_units_total=skipped_units,
                        work_units_completed=0,
                        candidate_count=len(existing_rows),
                    )
                stage_record.artifact_paths = {
                    "primary": str(layout.reference_filter_candidates_path()),
                    "candidates": str(layout.reference_filter_candidates_path()),
                }
                write_stage_state(
                    layout,
                    build_stage_manifest(
                        stage_name=stage_name,
                        layout=layout,
                        job_id=job_id,
                        build_target=source_path_label,
                        source_ids=selected_source_ids,
                        processed_source_ids=process_source_ids,
                        input_stage=GRAPH_INPUT_STAGE[stage_name],
                        artifact_paths=stage_record.artifact_paths,
                        stats=stage_record.stats,
                    ),
                )
            elif stage_name == "relation_classify":
                input_source_ids = resolve_stage_source_ids(layout, stage_name, selected_source_ids)
                skipped_source_ids = reusable_artifact_source_ids(
                    layout,
                    stage_name,
                    input_source_ids,
                    force_rebuild=force_rebuild,
                )
                process_source_ids = subtract_source_ids(input_source_ids, skipped_source_ids)
                base_graph = load_graph_snapshot(layout, GRAPH_INPUT_STAGE[stage_name])
                candidates = read_reference_candidates(layout.reference_filter_candidates_path())
                checkpoint_every = runtime.stage_checkpoint_every(stage_name)
                existing_results = (
                    read_relation_plans(layout.relation_classify_plans_path())
                    if layout.relation_classify_plans_path().exists()
                    else []
                )
                existing_llm = (
                    read_llm_judge_details(layout.relation_classify_llm_judge_path())
                    if layout.relation_classify_llm_judge_path().exists()
                    else []
                )
                skipped_units = count_relation_classify_units(base_graph, candidates, set(skipped_source_ids))
                if process_source_ids:
                    def checkpoint_relation_classify(
                        snapshot_results: list,
                        snapshot_llm_judgments: list,
                        snapshot_stats: dict[str, object],
                        completed_units: int,
                        total_units: int,
                        processed_checkpoint_source_ids: list[str],
                    ) -> None:
                        merged_results = replace_relation_classify_outputs(
                            existing_results,
                            snapshot_results,
                            graph_bundle=base_graph,
                            active_source_ids=set(process_source_ids),
                        )
                        merged_results = filter_relation_classify_outputs_by_graph(
                            merged_results,
                            graph_bundle=base_graph,
                        )
                        merged_llm = replace_llm_judge_details(
                            existing_llm,
                            snapshot_llm_judgments,
                            graph_bundle=base_graph,
                            active_source_ids=set(process_source_ids),
                        )
                        write_relation_plans(layout.relation_classify_plans_path(), merged_results)
                        write_llm_judge_details(layout.relation_classify_llm_judge_path(), merged_llm)
                        checkpoint_graph = materialize_relation_plans(base_graph, merged_results)
                        write_stage_graph(layout, stage_name, checkpoint_graph, write_nodes=False, write_edges=True)
                        stage_record.artifact_paths = relation_artifact_paths(layout)
                        stage_record.stats = build_stage_work_stats(
                            input_source_ids=input_source_ids,
                            processed_source_ids=processed_checkpoint_source_ids,
                            skipped_source_ids=skipped_source_ids,
                            work_units_total=total_units + skipped_units,
                            work_units_completed=completed_units,
                            work_units_skipped=skipped_units,
                            updated_edges=int(snapshot_stats.get("result_count", 0)),
                            candidate_count=int(snapshot_stats.get("candidate_count", 0)),
                            result_count=int(snapshot_stats.get("result_count", 0)),
                        )
                        stage_record.stats.update(
                            {
                                key: value
                                for key, value in dict(snapshot_stats).items()
                                if key not in stage_record.stats
                            }
                        )
                        write_json(
                            layout.relation_classify_log_path(),
                            {"status": "running", "stats": dict(stage_record.stats), "llm_errors": []},
                        )
                        write_stage_state(
                            layout,
                            build_stage_manifest(
                                stage_name=stage_name,
                                layout=layout,
                                job_id=job_id,
                                build_target=source_path_label,
                                source_ids=selected_source_ids,
                                processed_source_ids=processed_checkpoint_source_ids,
                                input_stage=GRAPH_INPUT_STAGE[stage_name],
                                artifact_paths=stage_record.artifact_paths,
                                stats=stage_record.stats,
                                graph_bundle=checkpoint_graph,
                                status="running",
                            ),
                        )
                        write_job_log(layout.job_log_path(job_id), log_record)

                    result = run_relation_classify(
                        base_graph,
                        runtime,
                        candidates,
                        source_document_ids=set(process_source_ids),
                        model_progress_callback=lambda current, total_items: emit_stage_progress(
                            stage_progress_callback, f"{stage_name}::model", current, total_items
                        ),
                        llm_progress_callback=lambda current, total_items: emit_stage_progress(
                            stage_progress_callback, f"{stage_name}::llm", current, total_items
                        ),
                        checkpoint_every=checkpoint_every,
                        checkpoint_callback=checkpoint_relation_classify,
                        cancel_event=cancel_event,
                    )
                    merged_results = replace_relation_classify_outputs(
                        existing_results,
                        result.results,
                        graph_bundle=base_graph,
                        active_source_ids=set(process_source_ids),
                    )
                    merged_results = filter_relation_classify_outputs_by_graph(
                        merged_results,
                        graph_bundle=base_graph,
                    )
                    merged_llm = replace_llm_judge_details(
                        existing_llm,
                        result.llm_judgments,
                        graph_bundle=base_graph,
                        active_source_ids=set(process_source_ids),
                    )
                    write_relation_plans(layout.relation_classify_plans_path(), merged_results)
                    write_llm_judge_details(layout.relation_classify_llm_judge_path(), merged_llm)
                    current_graph = materialize_relation_plans(base_graph, merged_results)
                    write_stage_graph(layout, stage_name, current_graph, write_nodes=False, write_edges=True)
                    stage_record.stats = finalize_stage_work_stats(
                        dict(result.stats),
                        processed_source_ids=process_source_ids,
                        skipped_source_ids=skipped_source_ids,
                        skipped_work_units=skipped_units,
                        updated_edges=int(result.stats.get("result_count", 0)),
                    )
                    write_json(
                        layout.relation_classify_log_path(),
                        {"stats": dict(stage_record.stats), "llm_errors": result.llm_errors},
                    )
                else:
                    current_graph = load_graph_snapshot(layout, stage_name) if stage_outputs_exist(layout, stage_name) else GraphBundle()
                    emit_idle_stage_progress(stage_progress_callback, stage_name)
                    stage_record.stats = build_stage_work_stats(
                        input_source_ids=input_source_ids,
                        processed_source_ids=[],
                        skipped_source_ids=skipped_source_ids,
                        work_units_total=skipped_units,
                        work_units_completed=0,
                        candidate_count=count_relation_classify_units(base_graph, candidates, set(input_source_ids)),
                    )
                stage_record.artifact_paths = relation_artifact_paths(layout)
                write_stage_state(
                    layout,
                    build_stage_manifest(
                        stage_name=stage_name,
                        layout=layout,
                        job_id=job_id,
                        build_target=source_path_label,
                        source_ids=selected_source_ids,
                        processed_source_ids=process_source_ids,
                        input_stage=GRAPH_INPUT_STAGE[stage_name],
                        artifact_paths=stage_record.artifact_paths,
                        stats=stage_record.stats,
                        graph_bundle=current_graph,
                    ),
                )
            elif stage_name == "entity_extraction":
                input_source_ids = resolve_stage_source_ids(layout, stage_name, selected_source_ids)
                current_graph = run_graph_stage(
                    stage_name,
                    layout,
                    runtime,
                    input_source_ids,
                    stage_progress_callback,
                    force_rebuild=force_rebuild,
                )
                write_stage_graph(layout, stage_name, current_graph, write_nodes=True, write_edges=True)
                stage_record.artifact_paths = graph_artifact_paths(layout, stage_name)
                stage_record.stats = build_graph_stage_stats(current_graph, input_source_ids)
                write_stage_state(
                    layout,
                    build_stage_manifest(
                        stage_name=stage_name,
                        layout=layout,
                        job_id=job_id,
                        build_target=source_path_label,
                        source_ids=selected_source_ids,
                        processed_source_ids=input_source_ids,
                        input_stage=GRAPH_INPUT_STAGE[stage_name],
                        artifact_paths=stage_record.artifact_paths,
                        stats=stage_record.stats,
                        graph_bundle=current_graph,
                    ),
                )
            elif stage_name == "entity_alignment":
                input_source_ids = resolve_stage_source_ids(layout, stage_name, selected_source_ids)
                if can_reuse_stage(
                    layout,
                    stage_name,
                    input_source_ids,
                    require_nodes=True,
                    require_edges=True,
                    force_rebuild=force_rebuild,
                ):
                    current_graph = load_graph_snapshot(layout, stage_name)
                    emit_idle_stage_progress(stage_progress_callback, stage_name)
                else:
                    base_graph = load_graph_snapshot(layout, GRAPH_INPUT_STAGE[stage_name])
                    current_graph = run_entity_alignment(
                        base_graph,
                        runtime,
                        active_source_ids=set(input_source_ids) if input_source_ids else None,
                        progress_callback=lambda current, total_items: emit_stage_progress(
                            stage_progress_callback, stage_name, current, total_items
                        ),
                    )
                    write_stage_graph(layout, stage_name, current_graph, write_nodes=True, write_edges=True)
                stage_record.artifact_paths = graph_artifact_paths(layout, stage_name)
                stage_record.stats = build_graph_stage_stats(current_graph, input_source_ids)
                write_stage_state(
                    layout,
                    build_stage_manifest(
                        stage_name=stage_name,
                        layout=layout,
                        job_id=job_id,
                        build_target=source_path_label,
                        source_ids=selected_source_ids,
                        processed_source_ids=input_source_ids,
                        input_stage=GRAPH_INPUT_STAGE[stage_name],
                        artifact_paths=stage_record.artifact_paths,
                        stats=stage_record.stats,
                        graph_bundle=current_graph,
                    ),
                )
            elif stage_name == "implicit_reasoning":
                input_source_ids = resolve_stage_source_ids(layout, stage_name, selected_source_ids)
                current_graph = run_graph_stage(
                    stage_name,
                    layout,
                    runtime,
                    input_source_ids,
                    stage_progress_callback,
                    force_rebuild=force_rebuild,
                )
                write_stage_graph(layout, stage_name, current_graph, write_nodes=False, write_edges=True)
                stage_record.artifact_paths = graph_artifact_paths(layout, stage_name)
                stage_record.stats = build_graph_stage_stats(current_graph, input_source_ids)
                write_stage_state(
                    layout,
                    build_stage_manifest(
                        stage_name=stage_name,
                        layout=layout,
                        job_id=job_id,
                        build_target=source_path_label,
                        source_ids=selected_source_ids,
                        processed_source_ids=input_source_ids,
                        input_stage=GRAPH_INPUT_STAGE[stage_name],
                        artifact_paths=stage_record.artifact_paths,
                        stats=stage_record.stats,
                        graph_bundle=current_graph,
                    ),
                )
            else:
                raise ValueError(f"Unsupported stage: {stage_name}")

            stage_record.status = "completed"
            stage_record.finished_at = timestamp_utc()
            completed += 1
            emit_stage_summary(stage_summary_callback, stage_name, stage_record.stats)
            if stage_callback is not None:
                stage_callback(completed, total)
            write_job_log(layout.job_log_path(job_id), log_record)
        except Exception as exc:
            stage_record.status = "failed"
            stage_record.error = str(exc)
            stage_record.finished_at = timestamp_utc()
            log_record.status = "failed"
            log_record.finished_at = timestamp_utc()
            write_job_log(layout.job_log_path(job_id), log_record)
            raise

    final_graph = GraphBundle()
    if through_stage in GRAPH_STAGES:
        final_graph = load_graph_snapshot(layout, through_stage)
        if finalizing_callback is not None:
            finalizing_callback("finalize")
        write_stage_nodes(layout.final_nodes_path(), final_graph.nodes)
        write_stage_edges(layout.final_edges_path(), final_graph.edges)
    log_record.status = "completed"
    log_record.finished_at = timestamp_utc()
    log_record.final_artifact_paths = {}
    if through_stage in GRAPH_STAGES:
        log_record.final_artifact_paths = {
            "nodes": str(layout.final_nodes_path()),
            "edges": str(layout.final_edges_path()),
        }
    updated_nodes = sum(int(stage.stats.get("updated_nodes", 0)) for stage in log_record.stages)
    updated_edges = sum(int(stage.stats.get("updated_edges", 0)) for stage in log_record.stages)
    log_record.stats = {
        "completed_stages": completed,
        "source_count": len(selected_source_ids),
        "updated_nodes": updated_nodes,
        "updated_edges": updated_edges,
        "final_nodes": len(final_graph.nodes),
        "final_edges": len(final_graph.edges),
    }
    write_job_log(layout.job_log_path(job_id), log_record)
    artifact_paths = {
        stage.name: stage.artifact_paths.get("primary", stage.graph_path)
        for stage in log_record.stages
        if stage.artifact_paths or stage.graph_path
    }
    if through_stage in GRAPH_STAGES:
        artifact_paths["final_nodes"] = str(layout.final_nodes_path())
        artifact_paths["final_edges"] = str(layout.final_edges_path())
    artifact_paths["job_log"] = str(layout.job_log_path(job_id))
    return {
        "status": log_record.status,
        "job_id": job_id,
        "start_stage": start,
        "through_stage": through_stage,
        "manifest_path": str(layout.job_log_path(job_id)),
        "updated_nodes": updated_nodes,
        "updated_edges": updated_edges,
        "artifact_paths": artifact_paths,
    }


def select_normalize_entries(index, selected_source_ids: list[str]) -> list[object]:
    selected = set(selected_source_ids)
    return [entry for entry in index.entries if entry.source_id in selected]


def build_normalize_stage_stats(entries: list[object]) -> dict[str, int]:
    skipped_count = sum(1 for entry in entries if bool(entry.details.get("reused")))
    succeeded_count = sum(
        1
        for entry in entries
        if not bool(entry.details.get("reused")) and entry.status == "completed"
    )
    failed_count = sum(
        1
        for entry in entries
        if not bool(entry.details.get("reused")) and entry.status != "completed"
    )
    return {
        "source_count": len(entries),
        "succeeded_sources": succeeded_count,
        "failed_sources": failed_count,
        "reused_sources": skipped_count,
        "work_units_total": len(entries),
        "work_units_completed": succeeded_count,
        "work_units_failed": failed_count,
        "work_units_skipped": skipped_count,
        "updated_nodes": 0,
        "updated_edges": 0,
    }


def resolve_stage_source_ids(layout: BuildLayout, stage_name: str, selected_source_ids: list[str]) -> list[str]:
    selected = sorted(dict.fromkeys(selected_source_ids))
    if stage_name == "normalize":
        return selected
    if stage_name == "structure":
        if not layout.normalize_index_path().exists():
            return []
        normalize_index = read_normalize_index(layout.normalize_index_path())
        completed_source_ids = {
            entry.source_id
            for entry in normalize_index.entries
            if entry.status == "completed"
        }
        return [source_id for source_id in selected if source_id in completed_source_ids]
    previous_stage = STAGE_SEQUENCE[STAGE_SEQUENCE.index(stage_name) - 1]
    manifest_path = layout.stage_manifest_path(previous_stage)
    if not manifest_path.exists():
        return []
    processed_source_ids = set(read_stage_manifest(manifest_path).processed_source_ids)
    return [source_id for source_id in selected if source_id in processed_source_ids]


def subtract_source_ids(source_ids: list[str], skipped_source_ids: list[str]) -> list[str]:
    skipped = set(skipped_source_ids)
    return [source_id for source_id in source_ids if source_id not in skipped]


def reusable_graph_source_ids(
    layout: BuildLayout,
    stage_name: str,
    source_ids: list[str],
    *,
    require_nodes: bool,
    require_edges: bool,
    force_rebuild: bool,
) -> list[str]:
    if force_rebuild or not source_ids:
        return []
    manifest_path = layout.stage_manifest_path(stage_name)
    if not manifest_path.exists():
        return []
    if require_nodes and not layout.stage_nodes_path(stage_name).exists():
        return []
    if require_edges and not layout.stage_edges_path(stage_name).exists():
        return []
    manifest = read_stage_manifest(manifest_path)
    if manifest.status != "completed":
        return []
    processed_source_ids = set(manifest.processed_source_ids)
    return [source_id for source_id in source_ids if source_id in processed_source_ids]


def reusable_artifact_source_ids(
    layout: BuildLayout,
    stage_name: str,
    source_ids: list[str],
    *,
    force_rebuild: bool,
) -> list[str]:
    if force_rebuild or not source_ids:
        return []
    manifest_path = layout.stage_manifest_path(stage_name)
    if not manifest_path.exists():
        return []
    manifest = read_stage_manifest(manifest_path)
    if manifest.status != "completed":
        return []
    if stage_name == "reference_filter":
        artifact_path = layout.reference_filter_candidates_path()
        if not artifact_matches_manifest_stats(artifact_path, int(manifest.stats.get("candidate_count", 0))):
            return []
    elif stage_name == "relation_classify":
        results_path = layout.relation_classify_plans_path()
        edges_path = layout.stage_edges_path(stage_name)
        if not artifact_matches_manifest_stats(results_path, int(manifest.stats.get("result_count", 0))):
            return []
        if not artifact_matches_manifest_stats(edges_path, int(manifest.stats.get("result_count", 0))):
            return []
    else:
        return []
    processed_source_ids = set(manifest.processed_source_ids)
    return [source_id for source_id in source_ids if source_id in processed_source_ids]


def build_stage_work_stats(
    *,
    input_source_ids: list[str],
    processed_source_ids: list[str],
    skipped_source_ids: list[str],
    work_units_total: int | None = None,
    work_units_completed: int | None = None,
    work_units_failed: int = 0,
    work_units_skipped: int | None = None,
    updated_nodes: int = 0,
    updated_edges: int = 0,
    **extra: int,
) -> dict[str, int]:
    completed_units = len(processed_source_ids) if work_units_completed is None else int(work_units_completed)
    total_units = len(input_source_ids) if work_units_total is None else int(work_units_total)
    skipped_units = max(total_units - completed_units - int(work_units_failed), 0) if work_units_skipped is None else int(work_units_skipped)
    stats = {
        "source_count": len(input_source_ids),
        "succeeded_sources": len(processed_source_ids),
        "failed_sources": 0,
        "reused_sources": len(skipped_source_ids),
        "processed_source_count": len(processed_source_ids),
        "skipped_source_count": len(skipped_source_ids),
        "work_units_total": total_units,
        "work_units_completed": completed_units,
        "work_units_failed": int(work_units_failed),
        "work_units_skipped": skipped_units,
        "updated_nodes": int(updated_nodes),
        "updated_edges": int(updated_edges),
    }
    stats.update({key: int(value) for key, value in extra.items()})
    return stats


def finalize_stage_work_stats(
    stats: dict[str, object],
    *,
    processed_source_ids: list[str],
    skipped_source_ids: list[str],
    skipped_work_units: int,
    updated_nodes: int = 0,
    updated_edges: int = 0,
) -> dict[str, object]:
    processed_work_units = int(
        stats.get(
            "work_units_completed",
            stats.get("work_units_total", 0),
        )
    )
    stats.update(
        build_stage_work_stats(
            input_source_ids=processed_source_ids + skipped_source_ids,
            processed_source_ids=processed_source_ids,
            skipped_source_ids=skipped_source_ids,
            work_units_total=processed_work_units + int(skipped_work_units),
            work_units_completed=processed_work_units,
            work_units_skipped=int(skipped_work_units),
            updated_nodes=updated_nodes,
            updated_edges=updated_edges,
        )
    )
    return stats


def emit_idle_stage_progress(
    callback: Callable[[str, int, int], None] | None,
    stage_name: str,
) -> None:
    emit_stage_progress(callback, stage_name, 1, 1)


def run_graph_stage(
    stage_name: str,
    layout: BuildLayout,
    runtime: PipelineRuntime,
    selected_source_ids: list[str],
    stage_progress_callback: Callable[[str, int, int], None] | None,
    *,
    force_rebuild: bool,
) -> GraphBundle:
    if can_reuse_stage(
        layout,
        stage_name,
        selected_source_ids,
        require_nodes=stage_name in GRAPH_NODE_OUTPUT_STAGES,
        require_edges=stage_name in GRAPH_EDGE_OUTPUT_STAGES,
        force_rebuild=force_rebuild,
    ):
        emit_idle_stage_progress(stage_progress_callback, stage_name)
        return load_graph_snapshot(layout, stage_name)
    base_stage = stage_name if stage_outputs_exist(layout, stage_name) else GRAPH_INPUT_STAGE[stage_name]
    base_graph = load_graph_snapshot(layout, base_stage)
    if stage_name == "entity_extraction":
        return run_entity_extraction(
            base_graph,
            runtime,
            active_source_ids=set(selected_source_ids),
            progress_callback=lambda current, total_items: emit_stage_progress(
                stage_progress_callback, stage_name, current, total_items
            ),
        )
    if stage_name == "implicit_reasoning":
        return run_implicit_reasoning(
            base_graph,
            runtime,
            active_source_ids=set(selected_source_ids),
            progress_callback=lambda current, total_items: emit_stage_progress(
                stage_progress_callback, stage_name, current, total_items
            ),
        )
    raise ValueError(f"Unsupported graph stage runner: {stage_name}")


def build_stage_manifest(
    *,
    stage_name: str,
    layout: BuildLayout,
    job_id: str,
    build_target: str,
    source_ids: list[str],
    processed_source_ids: list[str],
    input_stage: str,
    artifact_paths: dict[str, str],
    stats: dict[str, object],
    graph_bundle: GraphBundle | None = None,
    status: str = "completed",
) -> StageStateManifest:
    node_stage = ""
    edge_stage = ""
    if input_stage:
        node_stage, edge_stage = resolve_input_stage_sources(layout, input_stage)
    current_source_ids = derive_stage_source_ids(graph_bundle) if graph_bundle is not None else []
    if layout.stage_manifest_path(stage_name).exists():
        previous = read_stage_manifest(layout.stage_manifest_path(stage_name))
        merged_source_ids = sorted(set(previous.source_ids) | set(current_source_ids or source_ids))
        merged_processed_ids = sorted(set(previous.processed_source_ids) | set(processed_source_ids))
    else:
        merged_source_ids = sorted(current_source_ids or source_ids)
        merged_processed_ids = sorted(set(processed_source_ids))
    return StageStateManifest(
        stage_name=stage_name,
        build_target=build_target,
        data_root=str(layout.data_root),
        job_id=job_id,
        status=status,
        source_ids=list(merged_source_ids),
        processed_source_ids=list(merged_processed_ids),
        artifact_paths=dict(artifact_paths),
        input_node_stage=node_stage,
        input_edge_stage=edge_stage,
        updated_at=timestamp_utc(),
        stats=dict(stats),
    )


def write_stage_state(layout: BuildLayout, manifest: StageStateManifest) -> None:
    write_stage_manifest(layout.stage_manifest_path(manifest.stage_name), manifest)


def stage_outputs_exist(layout: BuildLayout, stage_name: str) -> bool:
    return layout.stage_nodes_path(stage_name).exists() or layout.stage_edges_path(stage_name).exists()


def can_reuse_stage(
    layout: BuildLayout,
    stage_name: str,
    source_ids: list[str],
    *,
    require_nodes: bool = False,
    require_edges: bool = False,
    force_rebuild: bool = False,
) -> bool:
    if force_rebuild:
        return False
    manifest_path = layout.stage_manifest_path(stage_name)
    if not manifest_path.exists():
        return False
    manifest = read_stage_manifest(manifest_path)
    if manifest.status != "completed":
        return False
    if sorted(manifest.processed_source_ids) != sorted(source_ids):
        return False
    if require_nodes and not layout.stage_nodes_path(stage_name).exists():
        return False
    if require_edges and not layout.stage_edges_path(stage_name).exists():
        return False
    return True


def artifact_matches_manifest_stats(path: Path, expected_count: int) -> bool:
    if expected_count <= 0:
        return path.exists()
    return path.exists() and path.stat().st_size > 0


def load_graph_snapshot(layout: BuildLayout, stage_name: str) -> GraphBundle:
    if not stage_name:
        return GraphBundle()
    stage_index = STAGE_SEQUENCE.index(stage_name)
    nodes: list = []
    edges: list = []
    for index in range(stage_index, -1, -1):
        candidate = STAGE_SEQUENCE[index]
        if candidate in GRAPH_STAGES and not nodes and layout.stage_nodes_path(candidate).exists():
            nodes = read_stage_nodes(layout.stage_nodes_path(candidate))
        if candidate in GRAPH_STAGES and not edges and layout.stage_edges_path(candidate).exists():
            edges = read_stage_edges(layout.stage_edges_path(candidate))
        if nodes and edges:
            break
    bundle = GraphBundle(nodes=nodes, edges=edges)
    if bundle.nodes or bundle.edges:
        bundle.validate_edge_references()
    return bundle


def resolve_input_stage_sources(layout: BuildLayout, stage_name: str) -> tuple[str, str]:
    stage_index = STAGE_SEQUENCE.index(stage_name)
    node_stage = ""
    edge_stage = ""
    for index in range(stage_index, -1, -1):
        candidate = STAGE_SEQUENCE[index]
        if candidate in GRAPH_STAGES and not node_stage and layout.stage_nodes_path(candidate).exists():
            node_stage = candidate
        if candidate in GRAPH_STAGES and not edge_stage and layout.stage_edges_path(candidate).exists():
            edge_stage = candidate
        if node_stage and edge_stage:
            break
    return node_stage, edge_stage


def derive_stage_source_ids(graph_bundle: GraphBundle | None) -> list[str]:
    if graph_bundle is None:
        return []
    return sorted(
        {
            node.id.split(":", 1)[1]
            for node in graph_bundle.nodes
            if node.level == "document" and node.id.startswith("document:")
        }
    )


def write_stage_graph(
    layout: BuildLayout,
    stage_name: str,
    graph_bundle: GraphBundle,
    *,
    write_nodes: bool,
    write_edges: bool,
) -> None:
    graph_bundle.validate_edge_references()
    if write_nodes:
        write_stage_nodes(layout.stage_nodes_path(stage_name), graph_bundle.nodes)
    elif layout.stage_nodes_path(stage_name).exists():
        layout.stage_nodes_path(stage_name).unlink()
    if write_edges:
        write_stage_edges(layout.stage_edges_path(stage_name), graph_bundle.edges)
    elif layout.stage_edges_path(stage_name).exists():
        layout.stage_edges_path(stage_name).unlink()


def graph_artifact_paths(layout: BuildLayout, stage_name: str) -> dict[str, str]:
    paths: dict[str, str] = {}
    primary = layout.stage_nodes_path(stage_name) if layout.stage_nodes_path(stage_name).exists() else layout.stage_edges_path(stage_name)
    if primary.exists():
        paths["primary"] = str(primary)
    if layout.stage_nodes_path(stage_name).exists():
        paths["nodes"] = str(layout.stage_nodes_path(stage_name))
    if layout.stage_edges_path(stage_name).exists():
        paths["edges"] = str(layout.stage_edges_path(stage_name))
    return paths


def clear_stage_range_outputs(layout: BuildLayout, stage_names: tuple[str, ...]) -> None:
    for stage_name in stage_names:
        clear_stage_outputs(layout, stage_name)
    _unlink_if_exists(layout.final_nodes_path())
    _unlink_if_exists(layout.final_edges_path())


def clear_stage_outputs(layout: BuildLayout, stage_name: str) -> None:
    _unlink_if_exists(layout.stage_manifest_path(stage_name))
    if stage_name == "normalize":
        _unlink_if_exists(layout.normalize_index_path())
        _unlink_if_exists(layout.normalize_log_path())
        shutil.rmtree(layout.normalize_documents_dir(), ignore_errors=True)
        layout.normalize_documents_dir().mkdir(parents=True, exist_ok=True)
        return
    if stage_name == "reference_filter":
        _unlink_if_exists(layout.reference_filter_candidates_path())
        _unlink_if_exists(layout.reference_filter_log_path())
        return
    if stage_name == "relation_classify":
        _unlink_if_exists(layout.relation_classify_plans_path())
        _unlink_if_exists(layout.relation_classify_llm_judge_path())
        _unlink_if_exists(layout.relation_classify_log_path())
    _unlink_if_exists(layout.stage_nodes_path(stage_name))
    _unlink_if_exists(layout.stage_edges_path(stage_name))


def _unlink_if_exists(path: Path) -> None:
    if path.exists():
        path.unlink()


def relation_artifact_paths(layout: BuildLayout) -> dict[str, str]:
    return {
        "primary": str(layout.stage_edges_path("relation_classify")),
        "edges": str(layout.stage_edges_path("relation_classify")),
        "results": str(layout.relation_classify_plans_path()),
        "llm_judgments": str(layout.relation_classify_llm_judge_path()),
    }


def build_graph_stage_stats(graph_bundle: GraphBundle, source_ids: list[str]) -> dict[str, int]:
    return {
        "source_count": len(source_ids),
        "succeeded_sources": len(source_ids),
        "failed_sources": 0,
        "reused_sources": 0,
        "work_units_total": len(source_ids),
        "work_units_completed": len(source_ids),
        "work_units_failed": 0,
        "work_units_skipped": 0,
        "updated_nodes": 0,
        "updated_edges": 0,
        "nodes": len(graph_bundle.nodes),
        "edges": len(graph_bundle.edges),
    }
def emit_stage_progress(
    callback: Callable[[str, int, int], None] | None,
    stage_name: str,
    current: int,
    total: int,
) -> None:
    if callback is not None:
        callback(stage_name, current, total)


def emit_stage_summary(
    callback: Callable[[str, dict[str, int]], None] | None,
    stage_name: str,
    stats: dict[str, object],
) -> None:
    if callback is None:
        return
    callback(
        stage_name,
        {
            "succeeded": int(stats.get("work_units_completed", stats.get("succeeded_sources", 0))),
            "failed": int(stats.get("work_units_failed", stats.get("failed_sources", 0))),
            "skipped": int(stats.get("work_units_skipped", stats.get("reused_sources", 0))),
        },
    )


def discover_source_ids(data_root: Path, category: str | list[str] | None = None) -> list[str]:
    metadata_root = data_root / "source" / "metadata"
    source_ids: list[str] = []
    categories = (
        {str(value).strip() for value in category if str(value).strip()}
        if isinstance(category, list)
        else ({str(category).strip()} if category else set())
    )
    for path in sorted(metadata_root.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            continue
        for item in payload:
            if not isinstance(item, dict):
                continue
            if categories and str(item.get("category", "")) not in categories:
                continue
            source_id = str(item.get("source_id", "")).strip()
            if source_id:
                source_ids.append(source_id)
    return source_ids


def resolve_source_id(source_arg: str, data_root: Path) -> str:
    source_arg = source_arg.strip()
    if not source_arg:
        raise ValueError("source_id cannot be empty.")
    data_root = data_root.resolve()
    catalog = discover_source_ids(data_root)
    if source_arg in catalog:
        return source_arg
    raise ValueError(f"Unknown source_id: {source_arg}")


def iter_stage_range(start_stage: str, through_stage: str) -> tuple[str, ...]:
    start_index = STAGE_SEQUENCE.index(start_stage)
    end_index = STAGE_SEQUENCE.index(through_stage)
    return STAGE_SEQUENCE[start_index : end_index + 1]
