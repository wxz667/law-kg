from __future__ import annotations

import json
import shutil
import threading
from pathlib import Path
from typing import Any, Callable

from ..contracts import GraphBundle, JobLogRecord, StageRecord, StageStateManifest, SubstageStateManifest
from ..io import (
    BuildLayout,
    ensure_stage_dirs,
    read_align_pairs,
    read_classify_pending,
    read_classify_results,
    read_concept_vectors,
    read_embedded_concepts,
    read_extract_concepts,
    read_extract_inputs,
    read_llm_judge_details,
    read_normalize_index,
    read_reference_candidates,
    read_stage_edges,
    read_stage_manifest,
    read_stage_nodes,
    write_job_log,
    write_align_pairs,
    write_classify_pending,
    write_classify_results,
    write_concept_vectors,
    write_embedded_concepts,
    write_extract_concepts,
    write_extract_inputs,
    write_llm_judge_details,
    write_normalize_index,
    write_reference_candidates,
    write_stage_edges,
    write_stage_manifest,
    write_stage_nodes,
)
from ..stages import (
    run_embed,
    run_extract,
    run_infer,
    run_normalize,
    run_detect,
    run_structure,
)
from ..stages.align.classify import classify_pairs
from ..stages.align.pair import build_pair_stats, build_pairs
from ..stages.align.resolve import run as resolve_align_pairs
from ..stages.detect.run import count_detect_units
from ..stages.embed.run import build_embed_stats, resolve_embed_runtime_config
from ..stages.extract.aggregate import build_extract_inputs, count_extract_units, filter_extract_source_ids
from ..stages.classify.run import count_classify_units
from ..stages.classify.run import build_classify_context, run_llm_phase, run_model_phase, select_candidates
from ..stages.classify.materialize import materialize_classify_results
from ..utils.ids import slugify, timestamp_utc
from ..utils.locator import source_id_from_node_id
from .incremental import (
    filter_reference_candidates_by_graph,
    filter_classify_outputs_by_graph,
    replace_document_subgraphs,
    replace_extract_concepts,
    replace_extract_inputs,
    select_extract_concepts,
    select_extract_inputs,
    replace_llm_judge_details,
    replace_classify_pending,
    replace_detect_outputs,
    replace_classify_outputs,
)
from .runtime import PipelineRuntime

STAGE_SEQUENCE = (
    "normalize",
    "structure",
    "detect",
    "classify",
    "extract",
    "embed",
    "align",
    "infer",
)

GRAPH_STAGES = {"structure", "classify", "align", "infer"}
GRAPH_NODE_OUTPUT_STAGES = {"structure", "align"}
GRAPH_EDGE_OUTPUT_STAGES = {"structure", "classify", "align", "infer"}
GRAPH_INPUT_STAGE = {
    "structure": "",
    "detect": "structure",
    "classify": "structure",
    "extract": "classify",
    "embed": "classify",
    "align": "classify",
    "infer": "align",
}


def build_knowledge_graph(
    *,
    source_id: str | list[str],
    data_root: Path,
    start_stage: str | None = None,
    through_stage: str = "infer",
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
    source_ids = [source_id] if isinstance(source_id, str) else list(source_id)
    selected_source_ids = [str(value).strip() for value in source_ids if str(value).strip()]
    if not selected_source_ids:
        raise ValueError("build_knowledge_graph requires at least one source_id.")
    job_id = (
        f"build-{slugify(selected_source_ids[0])}"
        if len(selected_source_ids) == 1
        else f"build-{timestamp_utc().replace(':', '').replace('-', '')}"
    )
    build_target = (
        selected_source_ids[0]
        if len(selected_source_ids) == 1
        else f"selected:{','.join(selected_source_ids)}"
    )
    return _build_from_source_ids(
        source_ids=selected_source_ids,
        data_root=data_root,
        start_stage=start_stage,
        through_stage=through_stage,
        force_rebuild=force_rebuild,
        job_id=job_id,
        source_path_label=build_target,
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
    through_stage: str = "infer",
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
                    }
                    stage_record.stats = build_normalize_stage_stats(partial_entries)
                    write_normalize_index(layout.normalize_index_path(), snapshot_index)
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
            elif stage_name == "detect":
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
                    read_reference_candidates(layout.detect_candidates_path())
                    if layout.detect_candidates_path().exists()
                    else []
                )
                skipped_units = count_detect_units(base_graph, set(skipped_source_ids))
                if process_source_ids:

                    def checkpoint_detect(
                        snapshot_candidates: list,
                        snapshot_stats: dict[str, object],
                        snapshot_profiling: dict[str, object],
                        processed_checkpoint_source_ids: list[str],
                    ) -> None:
                        normalized_checkpoint_source_ids = normalize_source_ids(processed_checkpoint_source_ids)
                        merged_snapshot = replace_detect_outputs(
                            existing_rows,
                            snapshot_candidates,
                            graph_bundle=base_graph,
                            active_source_ids=set(process_source_ids),
                        )
                        write_reference_candidates(layout.detect_candidates_path(), merged_snapshot)
                        stage_record.artifact_paths = {
                            "primary": str(layout.detect_candidates_path()),
                            "candidates": str(layout.detect_candidates_path()),
                        }
                        stage_record.stats = finalize_stage_work_stats(
                            dict(snapshot_stats),
                            processed_source_ids=normalized_checkpoint_source_ids,
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
                                processed_source_ids=normalized_checkpoint_source_ids,
                                input_stage=GRAPH_INPUT_STAGE[stage_name],
                                artifact_paths=stage_record.artifact_paths,
                                stats=stage_record.stats,
                                status="running",
                            ),
                        )
                        write_job_log(layout.job_log_path(job_id), log_record)

                    result = run_detect(
                        base_graph,
                        runtime,
                        source_document_ids=set(process_source_ids),
                        progress_callback=lambda current, total_items: emit_stage_progress(
                            stage_progress_callback, stage_name, current, total_items
                        ),
                        checkpoint_every=checkpoint_every,
                        checkpoint_callback=checkpoint_detect,
                        cancel_event=cancel_event,
                    )
                    merged_rows = replace_detect_outputs(
                        existing_rows,
                        result.candidates,
                        graph_bundle=base_graph,
                        active_source_ids=set(process_source_ids),
                    )
                    merged_rows = filter_reference_candidates_by_graph(merged_rows, graph_bundle=base_graph)
                    write_reference_candidates(layout.detect_candidates_path(), merged_rows)
                    stage_record.stats = finalize_stage_work_stats(
                        dict(result.stats),
                        processed_source_ids=process_source_ids,
                        skipped_source_ids=skipped_source_ids,
                        skipped_work_units=skipped_units,
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
                    "primary": str(layout.detect_candidates_path()),
                    "candidates": str(layout.detect_candidates_path()),
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
            elif stage_name == "classify":
                input_source_ids = resolve_stage_source_ids(layout, stage_name, selected_source_ids)
                skipped_source_ids = reusable_artifact_source_ids(
                    layout,
                    stage_name,
                    input_source_ids,
                    force_rebuild=force_rebuild,
                )
                process_source_ids = subtract_source_ids(input_source_ids, skipped_source_ids)
                base_graph = load_graph_snapshot(layout, GRAPH_INPUT_STAGE[stage_name])
                candidates = read_reference_candidates(layout.detect_candidates_path())
                checkpoint_every = runtime.stage_checkpoint_every(stage_name)
                classify_context = build_classify_context(base_graph, set(process_source_ids) if process_source_ids else set())
                scoped_candidates = select_candidates(candidates, classify_context)
                candidate_unit_ids = [row.id for row in scoped_candidates]
                candidate_source_map = build_unit_source_map(
                    {
                        row.id: owner_source_id(
                            classify_context.owner_document_by_node.get(row.source_node_id, row.source_node_id)
                        )
                        for row in scoped_candidates
                    }
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
                skipped_units = count_classify_units(base_graph, candidates, set(skipped_source_ids))
                substage_states: dict[str, SubstageStateManifest] = {}

                def classify_active_results(rows: list) -> list:
                    return [
                        row
                        for row in rows
                        if owner_source_id(
                            classify_context.owner_document_by_node.get(row.source_node_id, row.source_node_id)
                        ) in set(process_source_ids)
                    ]

                def classify_active_pending(rows: list) -> list:
                    return [
                        row
                        for row in rows
                        if owner_source_id(
                            classify_context.owner_document_by_node.get(row.source_node_id, row.source_node_id)
                        ) in set(process_source_ids)
                    ]

                def classify_active_llm(rows: list) -> list:
                    return [
                        row
                        for row in rows
                        if owner_source_id(
                            classify_context.owner_document_by_node.get(row.source_id, row.source_id)
                        ) in set(process_source_ids)
                    ]

                if process_source_ids:
                    model_reused_unit_ids = reusable_substage_unit_ids(
                        layout,
                        stage_name,
                        "model",
                        candidate_unit_ids,
                        force_rebuild=force_rebuild,
                    )
                    model_process_candidates = [
                        row for row in scoped_candidates if row.id not in set(model_reused_unit_ids)
                    ]
                    stage_record.artifact_paths = classify_artifact_paths(layout)
                    write_stage_state(
                        layout,
                        build_stage_manifest(
                            stage_name=stage_name,
                            layout=layout,
                            job_id=job_id,
                            build_target=source_path_label,
                            source_ids=selected_source_ids,
                            processed_source_ids=[],
                            input_stage=GRAPH_INPUT_STAGE[stage_name],
                            artifact_paths=stage_record.artifact_paths,
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
                            merged_results = replace_classify_outputs(
                                existing_results,
                                snapshot_results,
                                graph_bundle=base_graph,
                                active_source_ids=set(process_source_ids),
                            )
                            merged_results = filter_classify_outputs_by_graph(merged_results, graph_bundle=base_graph)
                            merged_pending = replace_classify_pending(
                                existing_pending,
                                snapshot_pending,
                                graph_bundle=base_graph,
                                active_source_ids=set(process_source_ids),
                            )
                            write_classify_results(layout.classify_results_path(), merged_results)
                            write_classify_pending(layout.classify_pending_path(), merged_pending)
                            model_output_unit_ids = [row.id for row in classify_active_pending(merged_pending)]
                            substage_states["model"] = build_unit_substage_manifest(
                                stage_name="model",
                                unit_ids=candidate_unit_ids,
                                unit_source_map=candidate_source_map,
                                processed_unit_ids=sorted(
                                    set(model_reused_unit_ids) | set(processed_checkpoint_unit_ids)
                                ),
                                output_unit_ids=model_output_unit_ids,
                                skipped_unit_ids=model_reused_unit_ids,
                                stats=build_stage_work_stats(
                                    input_source_ids=process_source_ids,
                                    processed_source_ids=processed_checkpoint_source_ids,
                                    skipped_source_ids=source_ids_for_unit_ids(model_reused_unit_ids, candidate_source_map),
                                    work_units_total=total_units,
                                    work_units_completed=completed_units,
                                    work_units_skipped=len(model_reused_unit_ids),
                                    candidate_count=len(candidate_unit_ids),
                                    result_count=len(classify_active_results(merged_results)),
                                    pending_count=len(model_output_unit_ids),
                                ),
                                status="running",
                            )
                            stage_record.stats = build_stage_work_stats(
                                input_source_ids=input_source_ids,
                                processed_source_ids=processed_checkpoint_source_ids,
                                skipped_source_ids=skipped_source_ids,
                                work_units_total=len(candidate_unit_ids) + skipped_units,
                                work_units_completed=completed_units,
                                work_units_skipped=skipped_units + len(model_reused_unit_ids),
                                updated_edges=len(classify_active_results(merged_results)),
                                candidate_count=len(candidate_unit_ids) + skipped_units,
                                result_count=len(classify_active_results(merged_results)),
                            )
                            stage_record.failures = []
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
                                    substage_states=substage_states,
                                ),
                            )
                            write_job_log(layout.job_log_path(job_id), log_record)

                        model_result = run_model_phase(
                            base_graph,
                            runtime,
                            model_process_candidates,
                            progress_callback=offset_stage_progress_callback(
                                stage_progress_callback,
                                f"{stage_name}::model",
                                skipped_units=len(model_reused_unit_ids),
                                total_units=len(candidate_unit_ids),
                            ),
                            checkpoint_every=checkpoint_every,
                            checkpoint_callback=checkpoint_classify_model,
                            cancel_event=cancel_event,
                        )
                        merged_results = replace_classify_outputs(
                            existing_results,
                            model_result.results,
                            graph_bundle=base_graph,
                            active_source_ids=set(process_source_ids),
                        )
                        merged_results = filter_classify_outputs_by_graph(merged_results, graph_bundle=base_graph)
                        merged_pending = replace_classify_pending(
                            existing_pending,
                            model_result.pending_records,
                            graph_bundle=base_graph,
                            active_source_ids=set(process_source_ids),
                        )
                        write_classify_results(layout.classify_results_path(), merged_results)
                        write_classify_pending(layout.classify_pending_path(), merged_pending)
                        existing_results = merged_results
                        existing_pending = merged_pending
                        model_output_unit_ids = [row.id for row in classify_active_pending(merged_pending)]
                        substage_states["model"] = build_unit_substage_manifest(
                            stage_name="model",
                            unit_ids=candidate_unit_ids,
                            unit_source_map=candidate_source_map,
                            processed_unit_ids=sorted(
                                set(model_reused_unit_ids) | set(model_result.processed_candidate_ids)
                            ),
                            output_unit_ids=model_output_unit_ids,
                            skipped_unit_ids=model_reused_unit_ids,
                            stats=build_stage_work_stats(
                                input_source_ids=process_source_ids,
                                processed_source_ids=model_result.processed_source_ids,
                                skipped_source_ids=source_ids_for_unit_ids(model_reused_unit_ids, candidate_source_map),
                                work_units_total=len(candidate_unit_ids),
                                work_units_completed=len(model_result.processed_candidate_ids),
                                work_units_skipped=len(model_reused_unit_ids),
                                candidate_count=len(candidate_unit_ids),
                                result_count=len(classify_active_results(merged_results)),
                                pending_count=len(model_output_unit_ids),
                            ),
                            status="completed",
                        )
                    else:
                        emit_prefilled_stage_progress(
                            stage_progress_callback,
                            f"{stage_name}::model",
                            current=len(model_reused_unit_ids),
                            total=len(candidate_unit_ids),
                        )
                        model_output_unit_ids = substage_output_unit_ids(layout, stage_name, "model", candidate_unit_ids)
                        substage_states["model"] = build_unit_substage_manifest(
                            stage_name="model",
                            unit_ids=candidate_unit_ids,
                            unit_source_map=candidate_source_map,
                            processed_unit_ids=model_reused_unit_ids,
                            output_unit_ids=model_output_unit_ids,
                            skipped_unit_ids=model_reused_unit_ids,
                            stats=build_stage_work_stats(
                                input_source_ids=process_source_ids,
                                processed_source_ids=[],
                                skipped_source_ids=source_ids_for_unit_ids(model_reused_unit_ids, candidate_source_map),
                                work_units_total=len(candidate_unit_ids),
                                work_units_completed=0,
                                work_units_skipped=len(model_reused_unit_ids),
                                candidate_count=len(candidate_unit_ids),
                                result_count=len(classify_active_results(existing_results)),
                                pending_count=len(model_output_unit_ids),
                            ),
                            status="completed",
                        )

                    llm_scope_unit_ids = list(substage_states["model"].output_unit_ids)
                    llm_unit_source_map = {
                        unit_id: candidate_source_map[unit_id]
                        for unit_id in llm_scope_unit_ids
                        if unit_id in candidate_source_map
                    }
                    llm_reused_unit_ids = reusable_substage_unit_ids(
                        layout,
                        stage_name,
                        "llm_judge",
                        llm_scope_unit_ids,
                        force_rebuild=force_rebuild,
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
                            merged_results = replace_classify_outputs(
                                existing_results,
                                list(cumulative_results.values()),
                                graph_bundle=base_graph,
                                active_source_ids=set(process_source_ids),
                            )
                            merged_results = filter_classify_outputs_by_graph(merged_results, graph_bundle=base_graph)
                            merged_llm = replace_llm_judge_details(
                                existing_llm,
                                list(cumulative_llm.values()),
                                graph_bundle=base_graph,
                                active_source_ids=set(process_source_ids),
                            )
                            write_classify_results(layout.classify_results_path(), merged_results)
                            write_llm_judge_details(layout.classify_llm_judge_path(), merged_llm)
                            llm_completed_unit_ids = sorted(
                                set(llm_reused_unit_ids) | {row.id for row in snapshot_results}
                            )
                            substage_states["llm_judge"] = build_unit_substage_manifest(
                                stage_name="llm_judge",
                                unit_ids=llm_scope_unit_ids,
                                unit_source_map=llm_unit_source_map,
                                processed_unit_ids=llm_completed_unit_ids,
                                output_unit_ids=llm_completed_unit_ids,
                                skipped_unit_ids=llm_reused_unit_ids,
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
                                status="running",
                            )
                            stage_record.stats = build_stage_work_stats(
                                input_source_ids=input_source_ids,
                                processed_source_ids=process_source_ids,
                                skipped_source_ids=skipped_source_ids,
                                work_units_total=len(candidate_unit_ids) + skipped_units,
                                work_units_completed=len(model_process_candidates),
                                work_units_skipped=skipped_units + len(model_reused_unit_ids),
                                updated_edges=len(classify_active_results(merged_results)),
                                candidate_count=len(candidate_unit_ids) + skipped_units,
                                result_count=len(classify_active_results(merged_results)),
                            )
                            stage_record.failures = []
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
                                    status="running",
                                    substage_states=substage_states,
                                ),
                            )
                            write_job_log(layout.job_log_path(job_id), log_record)

                        llm_result = run_llm_phase(
                            runtime=runtime,
                            pending_records=llm_process_pending,
                            stats=dict(model_result.stats if model_process_candidates else {}),
                            progress_callback=offset_stage_progress_callback(
                                stage_progress_callback,
                                f"{stage_name}::llm",
                                skipped_units=len(llm_reused_unit_ids),
                                total_units=len(llm_scope_unit_ids),
                            ),
                            checkpoint_every=checkpoint_every,
                            checkpoint_callback=checkpoint_classify_llm,
                            cancel_event=cancel_event,
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
                        merged_results = replace_classify_outputs(
                            existing_results,
                            list(cumulative_results.values()),
                            graph_bundle=base_graph,
                            active_source_ids=set(process_source_ids),
                        )
                        merged_results = filter_classify_outputs_by_graph(merged_results, graph_bundle=base_graph)
                        merged_llm = replace_llm_judge_details(
                            existing_llm,
                            list(cumulative_llm.values()),
                            graph_bundle=base_graph,
                            active_source_ids=set(process_source_ids),
                        )
                        write_classify_results(layout.classify_results_path(), merged_results)
                        write_llm_judge_details(layout.classify_llm_judge_path(), merged_llm)
                        existing_results = merged_results
                        existing_llm = merged_llm
                        llm_completed_unit_ids = sorted(
                            set(llm_reused_unit_ids) | {row.id for row in llm_result.results}
                        )
                        substage_states["llm_judge"] = build_unit_substage_manifest(
                            stage_name="llm_judge",
                            unit_ids=llm_scope_unit_ids,
                            unit_source_map=llm_unit_source_map,
                            processed_unit_ids=llm_completed_unit_ids,
                            output_unit_ids=llm_completed_unit_ids,
                            skipped_unit_ids=llm_reused_unit_ids,
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
                            status="completed",
                        )
                        llm_errors = llm_result.llm_errors
                    else:
                        emit_prefilled_stage_progress(
                            stage_progress_callback,
                            f"{stage_name}::llm",
                            current=len(llm_reused_unit_ids),
                            total=len(llm_scope_unit_ids),
                        )
                        substage_states["llm_judge"] = build_unit_substage_manifest(
                            stage_name="llm_judge",
                            unit_ids=llm_scope_unit_ids,
                            unit_source_map=llm_unit_source_map,
                            processed_unit_ids=llm_reused_unit_ids,
                            output_unit_ids=llm_reused_unit_ids,
                            skipped_unit_ids=llm_reused_unit_ids,
                            stats=build_stage_work_stats(
                                input_source_ids=source_ids_for_unit_ids(llm_scope_unit_ids, llm_unit_source_map) if llm_scope_unit_ids else [],
                                processed_source_ids=[],
                                skipped_source_ids=source_ids_for_unit_ids(llm_reused_unit_ids, llm_unit_source_map) if llm_reused_unit_ids else [],
                                work_units_total=len(llm_scope_unit_ids),
                                work_units_completed=0,
                                work_units_skipped=len(llm_reused_unit_ids),
                                result_count=len(classify_active_results(existing_results)),
                            ),
                            status="completed",
                        )
                        llm_errors = []

                    current_graph = materialize_classify_results(base_graph, existing_results)
                    write_stage_graph(layout, stage_name, current_graph, write_nodes=False, write_edges=True)
                    stage_record.stats = build_stage_work_stats(
                        input_source_ids=input_source_ids,
                        processed_source_ids=process_source_ids,
                        skipped_source_ids=skipped_source_ids,
                        work_units_total=len(candidate_unit_ids) + skipped_units,
                        work_units_completed=len(model_process_candidates),
                        work_units_skipped=skipped_units + len(model_reused_unit_ids),
                        updated_edges=len(classify_active_results(existing_results)),
                        candidate_count=len(candidate_unit_ids) + skipped_units,
                        result_count=len(classify_active_results(existing_results)),
                    )
                    stage_record.failures = [dict(item) for item in llm_errors]
                else:
                    current_graph = load_graph_snapshot(layout, stage_name) if stage_outputs_exist(layout, stage_name) else GraphBundle()
                    emit_idle_stage_progress(stage_progress_callback, stage_name)
                    stage_record.stats = build_stage_work_stats(
                        input_source_ids=input_source_ids,
                        processed_source_ids=[],
                        skipped_source_ids=skipped_source_ids,
                        work_units_total=skipped_units,
                        work_units_completed=0,
                        candidate_count=count_classify_units(base_graph, candidates, set(input_source_ids)),
                    )
                stage_record.artifact_paths = classify_artifact_paths(layout)
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
                        substage_states=substage_states,
                    ),
                )
            elif stage_name == "extract":
                input_source_ids = resolve_stage_source_ids(layout, stage_name, selected_source_ids)
                base_graph = load_graph_snapshot(layout, GRAPH_INPUT_STAGE[stage_name])
                checkpoint_every = runtime.stage_checkpoint_every(stage_name)
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
                substage_states: dict[str, SubstageStateManifest] = {}
                filtered_source_ids = filter_extract_source_ids(
                    base_graph,
                    active_source_ids=set(input_source_ids),
                )
                stage_record.artifact_paths = extract_artifact_paths(layout)
                write_stage_state(
                    layout,
                    build_stage_manifest(
                        stage_name=stage_name,
                        layout=layout,
                        job_id=job_id,
                        build_target=source_path_label,
                        source_ids=selected_source_ids,
                        processed_source_ids=[],
                        input_stage=GRAPH_INPUT_STAGE[stage_name],
                        artifact_paths=stage_record.artifact_paths,
                        stats={},
                        status="running",
                        substage_states=substage_states,
                    ),
                )

                aggregate_source_ids = filtered_source_ids
                aggregate_skipped_source_ids = reusable_substage_unit_ids(
                    layout,
                    stage_name,
                    "aggregate",
                    aggregate_source_ids,
                    force_rebuild=force_rebuild,
                )
                aggregate_process_source_ids = subtract_source_ids(aggregate_source_ids, aggregate_skipped_source_ids)
                if aggregate_process_source_ids:
                    def checkpoint_aggregate(
                        snapshot_inputs: list,
                        processed_checkpoint_source_ids: list[str],
                    ) -> None:
                        merged_inputs = replace_extract_inputs(
                            existing_inputs,
                            snapshot_inputs,
                            graph_bundle=base_graph,
                            active_source_ids=set(aggregate_process_source_ids),
                        )
                        write_extract_inputs(layout.extract_inputs_path(), merged_inputs)
                        aggregate_scope_inputs = select_extract_inputs(
                            merged_inputs,
                            graph_bundle=base_graph,
                            active_source_ids=set(aggregate_source_ids),
                        )
                        aggregate_output_unit_ids = [row.id for row in aggregate_scope_inputs]
                        aggregate_checkpoint_source_map = build_unit_source_map(
                            {source_id: source_id for source_id in aggregate_source_ids}
                            | {row.id: source_id_from_node_id(row.id) for row in aggregate_scope_inputs}
                        )
                        substage_states["aggregate"] = build_unit_substage_manifest(
                            stage_name="aggregate",
                            unit_ids=aggregate_source_ids,
                            unit_source_map=aggregate_checkpoint_source_map,
                            processed_unit_ids=sorted(
                                set(aggregate_skipped_source_ids) | set(processed_checkpoint_source_ids)
                            ),
                            output_unit_ids=aggregate_output_unit_ids,
                            skipped_unit_ids=aggregate_skipped_source_ids,
                            stats=build_stage_work_stats(
                                input_source_ids=aggregate_source_ids,
                                processed_source_ids=processed_checkpoint_source_ids,
                                skipped_source_ids=aggregate_skipped_source_ids,
                                work_units_total=len(aggregate_source_ids),
                                work_units_completed=len(processed_checkpoint_source_ids),
                                work_units_skipped=len(aggregate_skipped_source_ids),
                                input_count=len(aggregate_scope_inputs),
                                output_unit_count=len(aggregate_scope_inputs),
                                output_source_count=len({source_id_from_node_id(row.id) for row in aggregate_scope_inputs}),
                            ),
                            status="running",
                        )
                        write_stage_state(
                            layout,
                            build_stage_manifest(
                                stage_name=stage_name,
                                layout=layout,
                                job_id=job_id,
                                build_target=source_path_label,
                                source_ids=selected_source_ids,
                                processed_source_ids=[],
                                input_stage=GRAPH_INPUT_STAGE[stage_name],
                                artifact_paths=stage_record.artifact_paths,
                                stats={},
                                status="running",
                                substage_states=substage_states,
                            ),
                        )

                    aggregated_inputs = build_extract_inputs(
                        base_graph,
                        active_source_ids=set(aggregate_process_source_ids),
                        progress_callback=offset_stage_progress_callback(
                            stage_progress_callback,
                            f"{stage_name}::aggregate",
                            skipped_units=len(aggregate_skipped_source_ids),
                            total_units=len(aggregate_source_ids),
                        ),
                        checkpoint_every=checkpoint_every,
                        checkpoint_callback=checkpoint_aggregate,
                    )
                    merged_inputs = replace_extract_inputs(
                        existing_inputs,
                        aggregated_inputs,
                        graph_bundle=base_graph,
                        active_source_ids=set(aggregate_process_source_ids),
                    )
                    write_extract_inputs(layout.extract_inputs_path(), merged_inputs)
                    existing_inputs = merged_inputs
                else:
                    emit_prefilled_stage_progress(
                        stage_progress_callback,
                        f"{stage_name}::aggregate",
                        current=len(aggregate_skipped_source_ids),
                        total=len(aggregate_source_ids),
                    )
                aggregated_scope_inputs = select_extract_inputs(
                    existing_inputs,
                    graph_bundle=base_graph,
                    active_source_ids=set(aggregate_source_ids),
                )
                aggregate_processed_source_ids = sorted(set(aggregate_skipped_source_ids) | set(aggregate_process_source_ids))
                aggregate_output_unit_ids = [row.id for row in aggregated_scope_inputs]
                aggregate_unit_source_map = build_unit_source_map(
                    {source_id: source_id for source_id in aggregate_source_ids}
                    | {row.id: source_id_from_node_id(row.id) for row in aggregated_scope_inputs}
                )
                substage_states["aggregate"] = build_unit_substage_manifest(
                    stage_name="aggregate",
                    unit_ids=aggregate_source_ids,
                    unit_source_map=aggregate_unit_source_map,
                    processed_unit_ids=aggregate_processed_source_ids,
                    output_unit_ids=aggregate_output_unit_ids,
                    skipped_unit_ids=aggregate_skipped_source_ids,
                    stats=build_stage_work_stats(
                        input_source_ids=aggregate_source_ids,
                        processed_source_ids=aggregate_process_source_ids,
                        skipped_source_ids=aggregate_skipped_source_ids,
                        work_units_total=len(aggregate_source_ids),
                        work_units_completed=len(aggregate_process_source_ids),
                        work_units_skipped=len(aggregate_skipped_source_ids),
                        input_count=len(aggregated_scope_inputs),
                        output_unit_count=len(aggregated_scope_inputs),
                        output_source_count=len({source_id_from_node_id(row.id) for row in aggregated_scope_inputs}),
                    ),
                    status="completed",
                )
                write_stage_state(
                    layout,
                    build_stage_manifest(
                        stage_name=stage_name,
                        layout=layout,
                        job_id=job_id,
                        build_target=source_path_label,
                        source_ids=selected_source_ids,
                        processed_source_ids=[],
                        input_stage=GRAPH_INPUT_STAGE[stage_name],
                        artifact_paths=stage_record.artifact_paths,
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
                    force_rebuild=force_rebuild,
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
                extract_skipped_source_ids = completed_extract_source_ids(
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
                        stage_progress_callback,
                        f"{stage_name}::extract",
                        current=extract_skipped_units,
                        total=extract_total_units,
                    )
                    stage_record.artifact_paths = extract_artifact_paths(layout)
                    stage_record.stats = build_stage_work_stats(
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
                    extract_manifest_source_map = (
                        build_unit_source_map(
                            {unit_id: source_id_from_node_id(unit_id) for unit_id in extract_scope_input_ids}
                        )
                        if extract_scope_input_ids
                        else {}
                    )
                    substage_states["extract"] = build_unit_substage_manifest(
                        stage_name="extract",
                        unit_ids=extract_scope_input_ids,
                        unit_source_map=extract_manifest_source_map,
                        processed_unit_ids=extract_completed_input_ids,
                        output_unit_ids=extract_completed_input_ids,
                        skipped_unit_ids=extract_completed_input_ids,
                        stats=dict(stage_record.stats),
                        status="running",
                    )
                    stage_record.failures = []
                    write_stage_state(
                        layout,
                        build_stage_manifest(
                            stage_name=stage_name,
                            layout=layout,
                            job_id=job_id,
                            build_target=source_path_label,
                            source_ids=selected_source_ids,
                            processed_source_ids=[],
                            input_stage=GRAPH_INPUT_STAGE[stage_name],
                            artifact_paths=stage_record.artifact_paths,
                            stats=stage_record.stats,
                            status="running",
                            substage_states=substage_states,
                        ),
                    )
                    write_job_log(layout.job_log_path(job_id), log_record)

                    def checkpoint_extract(
                        snapshot_concepts: list,
                        snapshot_stats: dict[str, int],
                        processed_checkpoint_source_ids: list[str],
                        processed_checkpoint_input_ids: list[str],
                        successful_checkpoint_input_ids: list[str],
                        llm_error_summary: list[dict[str, object]],
                    ) -> None:
                        merged_concepts = replace_extract_concepts(
                            existing_concepts,
                            snapshot_concepts,
                            graph_bundle=base_graph,
                            active_source_ids=active_extract_source_ids,
                        )
                        write_extract_concepts(layout.extract_concepts_path(), merged_concepts)
                        checkpoint_completed_input_ids = sorted(
                            set(extract_completed_input_ids) | set(processed_checkpoint_input_ids)
                        )
                        checkpoint_source_map = (
                            build_unit_source_map(
                                {unit_id: source_id_from_node_id(unit_id) for unit_id in extract_scope_input_ids}
                            )
                            if extract_scope_input_ids
                            else {}
                        )
                        stage_record.artifact_paths = extract_artifact_paths(layout)
                        stage_record.stats = finalize_stage_work_stats(
                            dict(snapshot_stats),
                            processed_source_ids=processed_checkpoint_source_ids,
                            skipped_source_ids=extract_skipped_source_ids,
                            skipped_work_units=extract_skipped_units,
                        )
                        substage_states["extract"] = build_unit_substage_manifest(
                            stage_name="extract",
                            unit_ids=extract_scope_input_ids,
                            unit_source_map=checkpoint_source_map,
                            processed_unit_ids=checkpoint_completed_input_ids,
                            output_unit_ids=successful_checkpoint_input_ids,
                            skipped_unit_ids=extract_completed_input_ids,
                            stats=dict(stage_record.stats),
                            status="running",
                        )
                        stage_record.failures = [dict(item) for item in llm_error_summary]
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
                                substage_states=substage_states,
                            ),
                        )
                        write_job_log(layout.job_log_path(job_id), log_record)

                    result = run_extract(
                        base_graph,
                        runtime,
                        inputs=prepared_inputs,
                        active_source_ids=active_extract_source_ids,
                        progress_callback=offset_stage_progress_callback(
                            stage_progress_callback,
                            f"{stage_name}::extract",
                            skipped_units=extract_skipped_units,
                            total_units=extract_total_units,
                        ),
                        checkpoint_every=checkpoint_every,
                        checkpoint_callback=checkpoint_extract,
                        cancel_event=cancel_event,
                    )
                    merged_concepts = replace_extract_concepts(
                        existing_concepts,
                        result.concepts,
                        graph_bundle=base_graph,
                        active_source_ids=active_extract_source_ids,
                    )
                    write_extract_concepts(layout.extract_concepts_path(), merged_concepts)
                    stage_record.stats = finalize_stage_work_stats(
                        dict(result.stats),
                        processed_source_ids=result.processed_source_ids,
                        skipped_source_ids=extract_skipped_source_ids,
                        skipped_work_units=extract_skipped_units,
                    )
                    stage_record.failures = [dict(item) for item in result.llm_errors]
                    processed_source_ids = result.processed_source_ids
                    final_completed_input_ids = sorted(set(extract_completed_input_ids) | set(result.processed_input_ids))
                    final_successful_input_ids = sorted(set(extract_completed_input_ids) | set(result.successful_input_ids))
                    final_source_map = (
                        build_unit_source_map(
                            {unit_id: source_id_from_node_id(unit_id) for unit_id in extract_scope_input_ids}
                        )
                        if extract_scope_input_ids
                        else {}
                    )
                    substage_states["extract"] = build_unit_substage_manifest(
                        stage_name="extract",
                        unit_ids=extract_scope_input_ids,
                        unit_source_map=final_source_map,
                        processed_unit_ids=final_completed_input_ids,
                        output_unit_ids=final_successful_input_ids,
                        skipped_unit_ids=extract_completed_input_ids,
                        stats=dict(stage_record.stats),
                        status="completed",
                    )
                else:
                    emit_prefilled_stage_progress(
                        stage_progress_callback,
                        f"{stage_name}::extract",
                        current=extract_skipped_units,
                        total=extract_total_units,
                    )
                    stage_record.stats = build_stage_work_stats(
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
                    extract_manifest_source_map = (
                        build_unit_source_map(
                            {unit_id: source_id_from_node_id(unit_id) for unit_id in extract_scope_input_ids}
                        )
                        if extract_scope_input_ids
                        else {}
                    )
                    substage_states["extract"] = build_unit_substage_manifest(
                        stage_name="extract",
                        unit_ids=extract_scope_input_ids,
                        unit_source_map=extract_manifest_source_map,
                        processed_unit_ids=extract_completed_input_ids,
                        output_unit_ids=extract_completed_input_ids,
                        skipped_unit_ids=extract_completed_input_ids,
                        stats=dict(stage_record.stats),
                        status="completed",
                    )
                stage_record.artifact_paths = extract_artifact_paths(layout)
                write_stage_state(
                    layout,
                    build_stage_manifest(
                        stage_name=stage_name,
                        layout=layout,
                        job_id=job_id,
                        build_target=source_path_label,
                        source_ids=selected_source_ids,
                        processed_source_ids=processed_source_ids,
                        input_stage=GRAPH_INPUT_STAGE[stage_name],
                        artifact_paths=stage_record.artifact_paths,
                        stats=stage_record.stats,
                        substage_states=substage_states,
                    ),
                )
            elif stage_name == "embed":
                input_source_ids = resolve_stage_source_ids(layout, stage_name, selected_source_ids)
                base_graph = load_graph_snapshot(layout, GRAPH_INPUT_STAGE[stage_name])
                scoped_extract_concepts = (
                    select_extract_concepts(
                        read_extract_concepts(layout.extract_concepts_path()) if layout.extract_concepts_path().exists() else [],
                        graph_bundle=base_graph,
                        active_source_ids=set(input_source_ids),
                    )
                    if input_source_ids
                    else []
                )
                if (
                    can_reuse_stage(
                        layout,
                        stage_name,
                        input_source_ids,
                        force_rebuild=force_rebuild,
                    )
                    and layout.embed_concepts_path().exists()
                    and layout.embed_vectors_path().exists()
                ):
                    emit_idle_stage_progress(stage_progress_callback, stage_name)
                    embed_runtime_config = resolve_embed_runtime_config(runtime)
                    existing_vectors = read_concept_vectors(layout.embed_vectors_path())
                    stage_record.stats = build_embed_stats(
                        input_source_ids,
                        len(scoped_extract_concepts),
                        len(existing_vectors),
                        provider=embed_runtime_config.provider,
                        model=embed_runtime_config.model,
                        backend="provider",
                        vector_dimension=len(existing_vectors[0].vector) if existing_vectors else 0,
                    )
                else:
                    checkpoint_every = runtime.stage_checkpoint_every(stage_name)

                    def checkpoint_embed(
                        snapshot_concepts: list,
                        snapshot_vectors: list,
                        snapshot_stats: dict[str, int],
                        processed_checkpoint_source_ids: list[str],
                        processed_checkpoint_concept_ids: list[str],
                    ) -> None:
                        write_embedded_concepts(layout.embed_concepts_path(), snapshot_concepts)
                        write_concept_vectors(layout.embed_vectors_path(), snapshot_vectors)
                        concept_source_map = (
                            build_unit_source_map(
                                {
                                    row.id: source_id_from_node_id(row.source_node_id)
                                    for row in snapshot_concepts
                                }
                            )
                            if snapshot_concepts
                            else {}
                        )
                        substage_states = {}
                        if concept_source_map:
                            substage_states["embed"] = build_unit_substage_manifest(
                                stage_name="embed",
                                unit_ids=[row.id for row in snapshot_concepts],
                                unit_source_map=concept_source_map,
                                processed_unit_ids=processed_checkpoint_concept_ids,
                                output_unit_ids=processed_checkpoint_concept_ids,
                                skipped_unit_ids=[],
                                stats=dict(snapshot_stats),
                                status="running",
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
                                artifact_paths=embed_artifact_paths(layout),
                                stats=snapshot_stats,
                                status="running",
                                substage_states=substage_states,
                            ),
                        )

                    embed_result = run_embed(
                        scoped_extract_concepts,
                        runtime,
                        progress_callback=lambda current, total_items: emit_stage_progress(
                            stage_progress_callback, stage_name, current, total_items
                        ),
                        checkpoint_every=checkpoint_every,
                        checkpoint_callback=checkpoint_embed,
                    )
                    write_embedded_concepts(layout.embed_concepts_path(), embed_result.concepts)
                    write_concept_vectors(layout.embed_vectors_path(), embed_result.vectors)
                    stage_record.stats = dict(embed_result.stats)
                    substage_states = {}
                    concept_source_map = (
                        build_unit_source_map(
                            {
                                row.id: source_id_from_node_id(row.source_node_id)
                                for row in embed_result.concepts
                            }
                        )
                        if embed_result.concepts
                        else {}
                    )
                    if concept_source_map:
                        substage_states["embed"] = build_unit_substage_manifest(
                            stage_name="embed",
                            unit_ids=[row.id for row in embed_result.concepts],
                            unit_source_map=concept_source_map,
                            processed_unit_ids=embed_result.processed_concept_ids,
                            output_unit_ids=embed_result.processed_concept_ids,
                            skipped_unit_ids=[],
                            stats=dict(stage_record.stats),
                            status="completed",
                        )
                    write_stage_state(
                        layout,
                        build_stage_manifest(
                            stage_name=stage_name,
                            layout=layout,
                            job_id=job_id,
                            build_target=source_path_label,
                            source_ids=selected_source_ids,
                            processed_source_ids=embed_result.processed_source_ids,
                            input_stage=GRAPH_INPUT_STAGE[stage_name],
                            artifact_paths=embed_artifact_paths(layout),
                            stats=stage_record.stats,
                            substage_states=substage_states,
                        ),
                    )
                stage_record.artifact_paths = embed_artifact_paths(layout)
            elif stage_name == "align":
                input_source_ids = resolve_stage_source_ids(layout, stage_name, selected_source_ids)
                if can_reuse_stage(
                    layout,
                    stage_name,
                    input_source_ids,
                    require_nodes=True,
                    require_edges=True,
                    force_rebuild=force_rebuild,
                ) and layout.align_pairs_path().exists():
                    current_graph = load_graph_snapshot(layout, stage_name)
                    emit_idle_stage_progress(stage_progress_callback, stage_name)
                    stage_record.stats = build_graph_stage_stats(current_graph, input_source_ids)
                else:
                    base_graph = load_graph_snapshot(layout, GRAPH_INPUT_STAGE[stage_name])
                    all_embedded_concepts = read_embedded_concepts(layout.embed_concepts_path()) if layout.embed_concepts_path().exists() else []
                    all_vectors = read_concept_vectors(layout.embed_vectors_path()) if layout.embed_vectors_path().exists() else []
                    embedded_concepts = [
                        row
                        for row in all_embedded_concepts
                        if source_id_from_node_id(row.source_node_id) in set(input_source_ids)
                    ]
                    embedded_concept_ids = {row.id for row in embedded_concepts}
                    concept_vectors = [row for row in all_vectors if row.id in embedded_concept_ids]
                    concept_source_map = build_unit_source_map(
                        {
                            row.id: source_id_from_node_id(row.source_node_id)
                            for row in embedded_concepts
                        }
                    )
                    pair_checkpoint_every = runtime.substage_checkpoint_every(stage_name, "pair")
                    pair_progress = offset_stage_progress_callback(
                        stage_progress_callback,
                        f"{stage_name}::pair",
                        skipped_units=0,
                        total_units=max(len(embedded_concepts), 1),
                    )

                    substage_states: dict[str, SubstageStateManifest] = {}

                    def checkpoint_align_pair(
                        snapshot_pairs: list,
                        snapshot_stats: dict[str, int],
                        processed_checkpoint_concept_ids: list[str],
                    ) -> None:
                        write_align_pairs(layout.align_pairs_path(), snapshot_pairs)
                        if concept_source_map:
                            substage_states["pair"] = build_unit_substage_manifest(
                                stage_name="pair",
                                unit_ids=[row.id for row in embedded_concepts],
                                unit_source_map=concept_source_map,
                                processed_unit_ids=processed_checkpoint_concept_ids,
                                output_unit_ids=processed_checkpoint_concept_ids,
                                skipped_unit_ids=[],
                                stats=dict(snapshot_stats),
                                status="running",
                            )
                        write_stage_state(
                            layout,
                            build_stage_manifest(
                                stage_name=stage_name,
                                layout=layout,
                                job_id=job_id,
                                build_target=source_path_label,
                                source_ids=selected_source_ids,
                                processed_source_ids=source_ids_for_unit_ids(processed_checkpoint_concept_ids, concept_source_map)
                                if concept_source_map and processed_checkpoint_concept_ids
                                else [],
                                input_stage=GRAPH_INPUT_STAGE[stage_name],
                                artifact_paths=align_artifact_paths(layout),
                                stats=dict(snapshot_stats),
                                status="running",
                                substage_states=substage_states,
                            ),
                        )

                    pairs = build_pairs(
                        embedded_concepts,
                        concept_vectors,
                        runtime,
                        progress_callback=pair_progress,
                        checkpoint_every=pair_checkpoint_every,
                        checkpoint_callback=checkpoint_align_pair,
                    )
                    write_align_pairs(layout.align_pairs_path(), pairs)
                    if concept_source_map:
                        substage_states["pair"] = build_unit_substage_manifest(
                            stage_name="pair",
                            unit_ids=[row.id for row in embedded_concepts],
                            unit_source_map=concept_source_map,
                            processed_unit_ids=[row.id for row in embedded_concepts],
                            output_unit_ids=[row.id for row in embedded_concepts],
                            skipped_unit_ids=[],
                            stats=build_pair_stats(len(embedded_concepts), len(embedded_concepts), pairs),
                            status="completed",
                        )

                    pending_pairs = [row for row in pairs if row.relation == "pending"]
                    classify_pair_ids = [f"{row.left_id}|{row.right_id}" for row in pending_pairs]
                    classify_source_map = (
                        build_unit_source_map(
                            {
                                f"{row.left_id}|{row.right_id}": source_id_from_node_id(
                                    next(item.source_node_id for item in embedded_concepts if item.id == row.left_id)
                                )
                                for row in pending_pairs
                            }
                        )
                        if pending_pairs
                        else {}
                    )
                    classify_checkpoint_every = runtime.substage_checkpoint_every(stage_name, "classify")

                    def checkpoint_align_classify(
                        snapshot_pairs: list,
                        snapshot_stats: dict[str, int],
                        processed_checkpoint_pair_ids: list[str],
                        llm_error_summary: list[dict[str, Any]],
                    ) -> None:
                        del llm_error_summary
                        write_align_pairs(layout.align_pairs_path(), snapshot_pairs)
                        if classify_source_map:
                            substage_states["classify"] = build_unit_substage_manifest(
                                stage_name="classify",
                                unit_ids=classify_pair_ids,
                                unit_source_map=classify_source_map,
                                processed_unit_ids=processed_checkpoint_pair_ids,
                                output_unit_ids=processed_checkpoint_pair_ids,
                                skipped_unit_ids=[],
                                stats=dict(snapshot_stats),
                                status="running",
                            )
                        write_stage_state(
                            layout,
                            build_stage_manifest(
                                stage_name=stage_name,
                                layout=layout,
                                job_id=job_id,
                                build_target=source_path_label,
                                source_ids=selected_source_ids,
                                processed_source_ids=[],
                                input_stage=GRAPH_INPUT_STAGE[stage_name],
                                artifact_paths=align_artifact_paths(layout),
                                stats=dict(snapshot_stats),
                                status="running",
                                substage_states=substage_states,
                            ),
                        )

                    classify_result = classify_pairs(
                        pairs,
                        runtime,
                        progress_callback=offset_stage_progress_callback(
                            stage_progress_callback,
                            f"{stage_name}::classify",
                            skipped_units=0,
                            total_units=max(len(pending_pairs), 1),
                        ),
                        checkpoint_every=classify_checkpoint_every,
                        checkpoint_callback=checkpoint_align_classify,
                        cancel_event=cancel_event,
                    )
                    pairs = classify_result.pairs
                    write_align_pairs(layout.align_pairs_path(), pairs)
                    if classify_source_map:
                        substage_states["classify"] = build_unit_substage_manifest(
                            stage_name="classify",
                            unit_ids=classify_pair_ids,
                            unit_source_map=classify_source_map,
                            processed_unit_ids=classify_result.processed_pair_ids,
                            output_unit_ids=classify_result.processed_pair_ids,
                            skipped_unit_ids=[],
                            stats=dict(classify_result.stats),
                            status="completed",
                        )
                    resolve_checkpoint_every = runtime.substage_checkpoint_every(stage_name, "resolve")
                    emit_stage_progress(stage_progress_callback, f"{stage_name}::resolve", 0, 1)
                    if resolve_checkpoint_every > 0:
                        substage_states["resolve"] = build_unit_substage_manifest(
                            stage_name="resolve",
                            unit_ids=input_source_ids or ["resolve"],
                            unit_source_map=build_unit_source_map(
                                {
                                    unit_id: source_id_from_node_id(f"document:{unit_id}")
                                    for unit_id in (input_source_ids or ["resolve"])
                                }
                            ),
                            processed_unit_ids=[],
                            output_unit_ids=[],
                            skipped_unit_ids=[],
                            stats={"input_count": len(input_source_ids)},
                            status="running",
                        )
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
                                artifact_paths=align_artifact_paths(layout),
                                stats=stage_record.stats,
                                substage_states=substage_states,
                                status="running",
                            ),
                        )
                    resolve_result = resolve_align_pairs(base_graph, embedded_concepts, pairs)
                    current_graph = resolve_result.graph_bundle
                    write_stage_graph(layout, stage_name, current_graph, write_nodes=True, write_edges=True)
                    substage_states["resolve"] = build_unit_substage_manifest(
                        stage_name="resolve",
                        unit_ids=input_source_ids or ["resolve"],
                        unit_source_map=build_unit_source_map(
                            {
                                unit_id: source_id_from_node_id(f"document:{unit_id}")
                                for unit_id in (input_source_ids or ["resolve"])
                            }
                        ),
                        processed_unit_ids=input_source_ids or ["resolve"],
                        output_unit_ids=input_source_ids or ["resolve"],
                        skipped_unit_ids=[],
                        stats=dict(resolve_result.stats),
                        status="completed",
                    )
                    emit_stage_progress(stage_progress_callback, f"{stage_name}::resolve", 1, 1)
                    stage_record.stats = build_graph_stage_stats(current_graph, input_source_ids)
                    stage_record.stats.update(
                        {
                            "pair_count": len(pairs),
                            "equivalent_count": sum(1 for row in pairs if row.relation == "equivalent"),
                            "related_count": sum(1 for row in pairs if row.relation == "related"),
                            "ignored_pair_count": sum(1 for row in pairs if row.relation == "ignore"),
                        }
                    )
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
                            artifact_paths=align_artifact_paths(layout),
                            stats=stage_record.stats,
                            graph_bundle=current_graph,
                            substage_states=substage_states,
                        ),
                    )
                stage_record.artifact_paths = graph_artifact_paths(layout, stage_name)
                stage_record.artifact_paths = align_artifact_paths(layout)
                if not layout.stage_manifest_path(stage_name).exists() or read_stage_manifest(layout.stage_manifest_path(stage_name)).status != "completed":
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
            elif stage_name == "infer":
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
    processed_source_ids = set(normalize_source_ids(read_stage_manifest(manifest_path).processed_source_ids))
    return [source_id for source_id in selected if source_id in processed_source_ids]


def subtract_source_ids(source_ids: list[str], skipped_source_ids: list[str]) -> list[str]:
    skipped = set(skipped_source_ids)
    return [source_id for source_id in source_ids if source_id not in skipped]


def normalize_source_ids(source_ids: list[str]) -> list[str]:
    normalized: list[str] = []
    for source_id in source_ids:
        value = str(source_id).strip()
        if not value:
            continue
        normalized.append(source_id_from_node_id(value) if value.startswith("document:") else value)
    return sorted(dict.fromkeys(normalized))


def normalize_unit_ids(unit_ids: list[str]) -> list[str]:
    return sorted(dict.fromkeys(str(value).strip() for value in unit_ids if str(value).strip()))


def build_unit_source_map(unit_source_map: dict[str, str]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for unit_id, source_id in unit_source_map.items():
        normalized_unit_id = str(unit_id).strip()
        normalized_source_id = normalize_source_ids([str(source_id)])[0] if str(source_id).strip() else ""
        if not normalized_unit_id or not normalized_source_id:
            raise ValueError("substage unit/source mapping must not contain empty ids")
        normalized[normalized_unit_id] = normalized_source_id
    return normalized


def source_ids_for_unit_ids(unit_ids: list[str], unit_source_map: dict[str, str]) -> list[str]:
    normalized_map = build_unit_source_map(unit_source_map)
    missing_unit_ids = [unit_id for unit_id in normalize_unit_ids(unit_ids) if unit_id not in normalized_map]
    if missing_unit_ids:
        raise ValueError(f"missing source mapping for substage unit ids: {', '.join(missing_unit_ids[:5])}")
    return normalize_source_ids([normalized_map[unit_id] for unit_id in normalize_unit_ids(unit_ids)])


def build_unit_substage_manifest(
    *,
    stage_name: str,
    unit_ids: list[str],
    unit_source_map: dict[str, str],
    processed_unit_ids: list[str],
    output_unit_ids: list[str],
    skipped_unit_ids: list[str],
    stats: dict[str, object],
    status: str,
) -> SubstageStateManifest:
    normalized_units = normalize_unit_ids(unit_ids)
    normalized_processed = normalize_unit_ids(processed_unit_ids)
    normalized_output = normalize_unit_ids(output_unit_ids)
    normalized_skipped = normalize_unit_ids(skipped_unit_ids)
    normalized_map = build_unit_source_map(unit_source_map)
    unknown_unit_ids = [
        unit_id
        for unit_id in normalized_units + normalized_processed + normalized_output + normalized_skipped
        if unit_id not in normalized_map
    ]
    if unknown_unit_ids:
        raise ValueError(
            f"missing source mapping for substage manifest units: {', '.join(sorted(dict.fromkeys(unknown_unit_ids))[:5])}"
        )
    return SubstageStateManifest(
        name=stage_name,
        status=status,
        unit_ids=normalized_units,
        source_ids=source_ids_for_unit_ids(normalized_units, normalized_map),
        processed_unit_ids=normalized_processed,
        processed_source_ids=source_ids_for_unit_ids(normalized_processed, normalized_map),
        output_unit_ids=normalized_output,
        output_source_ids=source_ids_for_unit_ids(normalized_output, normalized_map),
        skipped_unit_ids=normalized_skipped,
        skipped_source_ids=source_ids_for_unit_ids(normalized_skipped, normalized_map),
        stats=dict(stats),
        updated_at=timestamp_utc(),
    )


def merge_unit_substage_manifest(
    previous: SubstageStateManifest | None,
    current: SubstageStateManifest,
) -> SubstageStateManifest:
    if previous is None:
        return current
    return SubstageStateManifest(
        name=current.name,
        status=current.status,
        unit_ids=normalize_unit_ids(previous.unit_ids + current.unit_ids),
        source_ids=normalize_source_ids(previous.source_ids + current.source_ids),
        processed_unit_ids=normalize_unit_ids(previous.processed_unit_ids + current.processed_unit_ids),
        processed_source_ids=normalize_source_ids(previous.processed_source_ids + current.processed_source_ids),
        output_unit_ids=normalize_unit_ids(previous.output_unit_ids + current.output_unit_ids),
        output_source_ids=normalize_source_ids(previous.output_source_ids + current.output_source_ids),
        skipped_unit_ids=normalize_unit_ids(current.skipped_unit_ids),
        skipped_source_ids=normalize_source_ids(current.skipped_source_ids),
        stats=dict(current.stats),
        updated_at=current.updated_at,
    )


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
    processed_source_ids = set(normalize_source_ids(manifest.processed_source_ids))
    return [source_id for source_id in source_ids if source_id in processed_source_ids]


def reusable_substage_unit_ids(
    layout: BuildLayout,
    stage_name: str,
    substage_name: str,
    unit_ids: list[str],
    *,
    force_rebuild: bool = False,
    validator: Callable[[list[str], SubstageStateManifest], bool] | None = None,
) -> list[str]:
    if force_rebuild or not unit_ids:
        return []
    manifest_path = layout.stage_manifest_path(stage_name)
    if not manifest_path.exists():
        return []
    manifest = read_stage_manifest(manifest_path)
    substage = manifest.substages.get(substage_name)
    if substage is None or substage.status not in {"completed", "running"}:
        return []
    reusable_unit_ids = [
        unit_id
        for unit_id in normalize_unit_ids(unit_ids)
        if unit_id in set(normalize_unit_ids(substage.processed_unit_ids))
    ]
    if validator is not None and reusable_unit_ids and not validator(reusable_unit_ids, substage):
        return []
    return reusable_unit_ids


def substage_output_unit_ids(
    layout: BuildLayout,
    stage_name: str,
    substage_name: str,
    unit_ids: list[str] | None = None,
) -> list[str]:
    manifest_path = layout.stage_manifest_path(stage_name)
    if not manifest_path.exists():
        return []
    manifest = read_stage_manifest(manifest_path)
    substage = manifest.substages.get(substage_name)
    if substage is None:
        return []
    output_unit_ids = normalize_unit_ids(substage.output_unit_ids)
    if unit_ids is None:
        return output_unit_ids
    requested_unit_ids = set(normalize_unit_ids(unit_ids))
    return [unit_id for unit_id in output_unit_ids if unit_id in requested_unit_ids]


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
    if stage_name == "detect":
        artifact_path = layout.detect_candidates_path()
        if not artifact_matches_manifest_stats(artifact_path, int(manifest.stats.get("candidate_count", 0))):
            return []
    elif stage_name == "classify":
        results_path = layout.classify_results_path()
        edges_path = layout.stage_edges_path(stage_name)
        if not artifact_matches_manifest_stats(results_path, int(manifest.stats.get("result_count", 0))):
            return []
        if not artifact_matches_manifest_stats(edges_path, int(manifest.stats.get("result_count", 0))):
            return []
    else:
        return []
    processed_source_ids = set(normalize_source_ids(manifest.processed_source_ids))
    return [source_id for source_id in source_ids if source_id in processed_source_ids]


def extract_inputs_materialized(
    layout: BuildLayout,
    graph_bundle: GraphBundle,
    source_ids: list[str],
    substage: SubstageStateManifest,
) -> bool:
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
    if set(source_ids) == set(substage.source_ids):
        expected_count = int(substage.stats.get("input_count", 0))
        if len(scoped_inputs) != expected_count:
            return False
        expected_output_count = int(substage.stats.get("output_source_count", 0))
        if expected_output_count > 0 and len(actual_source_ids) != expected_output_count:
            return False
        return True
    return True


def extract_outputs_materialized(
    layout: BuildLayout,
    graph_bundle: GraphBundle,
    source_ids: list[str],
    substage: SubstageStateManifest,
) -> bool:
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
    if set(source_ids) == set(substage.source_ids):
        expected_count = int(substage.stats.get("input_count", 0))
        if len(scoped_inputs) != expected_count:
            return False
        expected_result_count = int(substage.stats.get("result_count", 0))
        if len(scoped_concepts) != expected_result_count:
            return False
    return True


def completed_extract_source_ids(
    inputs: list[object],
    completed_input_ids: list[str],
) -> list[str]:
    if not inputs:
        return []
    completed_input_id_set = {str(value) for value in completed_input_ids}
    totals: dict[str, int] = {}
    completed: dict[str, int] = {}
    for row in inputs:
        source_id = source_id_from_node_id(str(getattr(row, "id", "")))
        totals[source_id] = int(totals.get(source_id, 0)) + 1
        if str(getattr(row, "id", "")) in completed_input_id_set:
            completed[source_id] = int(completed.get(source_id, 0)) + 1
    return sorted(source_id for source_id, total in totals.items() if completed.get(source_id, 0) >= total)


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
    total_work_units = int(stats.get("work_units_total", 0))
    completed_work_units = int(stats.get("work_units_completed", 0))
    failed_work_units = int(stats.get("work_units_failed", 0))
    stats.update(
        build_stage_work_stats(
            input_source_ids=processed_source_ids + skipped_source_ids,
            processed_source_ids=processed_source_ids,
            skipped_source_ids=skipped_source_ids,
            work_units_total=total_work_units + int(skipped_work_units),
            work_units_completed=completed_work_units,
            work_units_failed=failed_work_units,
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


def emit_prefilled_stage_progress(
    callback: Callable[[str, int, int], None] | None,
    stage_name: str,
    *,
    current: int,
    total: int,
) -> None:
    if total <= 0:
        emit_idle_stage_progress(callback, stage_name)
        return
    emit_stage_progress(callback, stage_name, current, total)


def offset_stage_progress_callback(
    callback: Callable[[str, int, int], None] | None,
    stage_name: str,
    *,
    skipped_units: int,
    total_units: int,
) -> Callable[[int, int], None]:
    def report(current: int, total: int) -> None:
        del total
        emit_prefilled_stage_progress(
            callback,
            stage_name,
            current=skipped_units + current,
            total=total_units,
        )

    return report


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
    if stage_name == "infer":
        return run_infer(
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
    substage_states: dict[str, SubstageStateManifest] | None = None,
) -> StageStateManifest:
    node_stage = ""
    edge_stage = ""
    if input_stage:
        node_stage, edge_stage = resolve_input_stage_sources(layout, input_stage)
    current_source_ids = derive_stage_source_ids(graph_bundle) if graph_bundle is not None else []
    merged_substages: dict[str, SubstageStateManifest] = {}
    if layout.stage_manifest_path(stage_name).exists():
        previous = read_stage_manifest(layout.stage_manifest_path(stage_name))
        merged_source_ids = normalize_source_ids(previous.source_ids + (current_source_ids or source_ids))
        merged_processed_ids = normalize_source_ids(previous.processed_source_ids + processed_source_ids)
        merged_substages = dict(previous.substages)
    else:
        merged_source_ids = normalize_source_ids(current_source_ids or source_ids)
        merged_processed_ids = normalize_source_ids(processed_source_ids)
    for name, state in (substage_states or {}).items():
        merged_substages[name] = merge_unit_substage_manifest(merged_substages.get(name), state)
    if stage_name == "extract":
        merged_substages = {
            name: state
            for name, state in merged_substages.items()
            if name in {"aggregate", "extract"}
        }
    if stage_name == "embed":
        merged_substages = {
            name: state
            for name, state in merged_substages.items()
            if name in {"embed"}
        }
    if stage_name == "classify":
        merged_substages = {
            name: state
            for name, state in merged_substages.items()
            if name in {"model", "llm_judge"}
        }
    if stage_name == "align":
        merged_substages = {
            name: state
            for name, state in merged_substages.items()
            if name in {"pair", "classify", "resolve"}
        }
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
        substages=merged_substages,
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
    if normalize_source_ids(manifest.processed_source_ids) != normalize_source_ids(source_ids):
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
        shutil.rmtree(layout.normalize_documents_dir(), ignore_errors=True)
        layout.normalize_documents_dir().mkdir(parents=True, exist_ok=True)
        return
    if stage_name == "detect":
        _unlink_if_exists(layout.detect_candidates_path())
        return
    if stage_name == "classify":
        _unlink_if_exists(layout.classify_pending_path())
        _unlink_if_exists(layout.classify_results_path())
        _unlink_if_exists(layout.classify_llm_judge_path())
    if stage_name == "extract":
        _unlink_if_exists(layout.extract_inputs_path())
        _unlink_if_exists(layout.extract_concepts_path())
    if stage_name == "embed":
        _unlink_if_exists(layout.embed_concepts_path())
        _unlink_if_exists(layout.embed_vectors_path())
        return
    if stage_name == "align":
        _unlink_if_exists(layout.align_pairs_path())
    _unlink_if_exists(layout.stage_nodes_path(stage_name))
    _unlink_if_exists(layout.stage_edges_path(stage_name))


def _unlink_if_exists(path: Path) -> None:
    if path.exists():
        path.unlink()


def classify_artifact_paths(layout: BuildLayout) -> dict[str, str]:
    return {
        "primary": str(layout.stage_edges_path("classify")),
        "edges": str(layout.stage_edges_path("classify")),
        "pending": str(layout.classify_pending_path()),
        "results": str(layout.classify_results_path()),
        "llm_judgments": str(layout.classify_llm_judge_path()),
    }


def extract_artifact_paths(layout: BuildLayout) -> dict[str, str]:
    return {
        "primary": str(layout.extract_concepts_path()),
        "inputs": str(layout.extract_inputs_path()),
        "results": str(layout.extract_concepts_path()),
    }


def embed_artifact_paths(layout: BuildLayout) -> dict[str, str]:
    return {
        "primary": str(layout.embed_concepts_path()),
        "concepts": str(layout.embed_concepts_path()),
        "vectors": str(layout.embed_vectors_path()),
    }


def align_artifact_paths(layout: BuildLayout) -> dict[str, str]:
    primary = layout.stage_nodes_path("align") if layout.stage_nodes_path("align").exists() else layout.align_pairs_path()
    paths = {
        "primary": str(primary),
        "pairs": str(layout.align_pairs_path()),
    }
    if layout.stage_nodes_path("align").exists():
        paths["nodes"] = str(layout.stage_nodes_path("align"))
    if layout.stage_edges_path("align").exists():
        paths["edges"] = str(layout.stage_edges_path("align"))
    return paths


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
