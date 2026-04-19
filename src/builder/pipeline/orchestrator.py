from __future__ import annotations

import hashlib
import json
import shutil
import threading
import traceback
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable

from ..contracts import (
    AggregateConceptRecord,
    AlignConceptRecord,
    AlignPairRecord,
    AlignRelationRecord,
    ConceptVectorRecord,
    EquivalenceRecord,
    GraphBundle,
    JobLogRecord,
    StageRecord,
    StageStateManifest,
    SubstageStateManifest,
    deduplicate_graph,
)
from ..io import (
    BuildLayout,
    ensure_stage_dirs,
    read_aggregate_concepts,
    read_align_canonical_concepts,
    read_align_pairs,
    read_align_relations,
    read_classify_pending,
    read_classify_results,
    read_concept_vectors,
    read_extract_concepts,
    read_extract_inputs,
    read_llm_judge_details,
    read_normalize_index,
    read_reference_candidates,
    read_stage_edges,
    read_stage_edges_unchecked,
    read_stage_manifest,
    read_stage_nodes,
    read_stage_nodes_unchecked,
    write_job_log,
    write_classify_pending,
    write_classify_results,
    write_aggregate_concepts,
    write_align_canonical_concepts,
    write_align_pairs,
    write_align_relations,
    write_concept_vectors,
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
    run_align,
    run_aggregate,
    run_extract,
    run_normalize,
    run_detect,
    run_structure,
)
from ..stages.detect.run import count_detect_units
from ..stages.extract.input import build_extract_inputs, count_extract_units, filter_extract_source_ids
from ..stages.classify.run import build_classify_context, run_llm_phase, run_model_phase, select_candidates
from ..stages.aggregate.input import build_inputs_from_extract as build_aggregate_inputs_from_extract
from ..stages.aggregate.input import build_output_stats as aggregate_concept_output_stats
from ..stages.classify.materialize import materialize_classify_results
from ..utils.ids import timestamp_utc
from ..utils.locator import owner_source_id, source_id_from_node_id
from .manifest_spec import (
    graph_type_stats,
    stage_artifacts,
    stage_inputs,
    stage_unit,
    substage_artifacts,
    substage_inputs,
    substage_unit,
)
from .incremental import (
    filter_reference_candidates_by_graph,
    owner_document_by_node,
    owner_source_id_for_node,
    replace_document_subgraphs,
    replace_extract_concepts_by_unit_ids,
    replace_extract_inputs,
    replace_aggregate_concepts,
    select_aggregate_concepts,
    select_extract_concepts,
    select_extract_inputs,
    replace_detect_outputs,
    replace_classify_outputs_by_unit_ids,
    replace_classify_pending_by_unit_ids,
    replace_llm_judge_details_by_unit_ids,
)
from .runtime import PipelineRuntime

STAGE_SEQUENCE = (
    "normalize",
    "structure",
    "detect",
    "classify",
    "extract",
    "aggregate",
    "align",
)

SUBSTAGE_PARENT_STAGES = {"classify", "extract", "align"}

GRAPH_STAGES = {"structure", "classify", "align"}
GRAPH_NODE_OUTPUT_STAGES = {"structure", "align"}
GRAPH_EDGE_OUTPUT_STAGES = {"structure", "classify", "align"}
GRAPH_INPUT_STAGE = {
    "structure": "",
    "detect": "structure",
    "classify": "structure",
    "extract": "classify",
    "aggregate": "classify",
    "align": "classify",
}


def build_job_id(prefix: str = "build") -> str:
    return f"{prefix}-{timestamp_utc().replace(':', '').replace('-', '')}"


def build_knowledge_graph(
    *,
    source_id: str | list[str],
    data_root: Path,
    start_stage: str | None = None,
    through_stage: str = "align",
    force_rebuild: bool = False,
    incremental: bool = False,
    report_progress: bool = False,
    stage_callback: Callable[[int, int], None] | None = None,
    stage_progress_callback: Callable[[str, int, int], None] | None = None,
    stage_name_callback: Callable[[str], None] | None = None,
    stage_summary_callback: Callable[[str, dict[str, int]], None] | None = None,
    finalizing_callback: Callable[[str], None] | None = None,
    stage_error_callback: Callable[[str, str, str], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> dict[str, object]:
    del report_progress
    del incremental
    source_ids = [source_id] if isinstance(source_id, str) else list(source_id)
    selected_source_ids = [str(value).strip() for value in source_ids if str(value).strip()]
    if not selected_source_ids:
        raise ValueError("build_knowledge_graph requires at least one source_id.")
    job_id = build_job_id("build")
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
        stage_error_callback=stage_error_callback,
        cancel_event=cancel_event,
    )


def build_batch_knowledge_graph(
    data_root: Path,
    pattern: str = "*.docx",
    category: str | list[str] | None = None,
    start_stage: str | None = None,
    through_stage: str = "align",
    force_rebuild: bool = False,
    incremental: bool = False,
    report_progress: bool = False,
    discovery_callback: Callable[[int], None] | None = None,
    progress_callback: Callable[[str, int, int], None] | None = None,
    stage_summary_callback: Callable[[str, dict[str, int]], None] | None = None,
    finalizing_callback: Callable[[str], None] | None = None,
    stage_error_callback: Callable[[str, str, str], None] | None = None,
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
        job_id=build_job_id("batch"),
        source_path_label=f"batch:{','.join(category) if isinstance(category, list) else (category or 'all')}",
        stage_progress_callback=progress_callback,
        stage_summary_callback=stage_summary_callback,
        finalizing_callback=finalizing_callback,
        stage_error_callback=stage_error_callback,
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
    stage_error_callback: Callable[[str, str, str], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> dict[str, object]:
    data_root = data_root.resolve()
    ensure_stage_dirs(data_root)
    layout = BuildLayout(data_root)
    previous_final_graph = (
        load_graph_snapshot(layout, through_stage)
        if through_stage in GRAPH_STAGES and stage_outputs_exist(layout, through_stage)
        else None
    )
    previous_align_concepts = (
        read_align_canonical_concepts(layout.align_concepts_path())
        if through_stage == "align" and layout.align_concepts_path().exists()
        else []
    )
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
        stage_previous_graph = (
            load_graph_snapshot(layout, stage_name)
            if stage_name in GRAPH_STAGES and stage_outputs_exist(layout, stage_name)
            else None
        )
        stage_previous_align_concepts = (
            read_align_canonical_concepts(layout.align_concepts_path())
            if stage_name == "align" and layout.align_concepts_path().exists()
            else []
        )
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
                            stage="normalize",
                            inputs=stage_inputs(layout, "normalize"),
                            artifacts=stage_artifacts(layout, "normalize"),
                            updated_at=timestamp_utc(),
                            unit=stage_unit("normalize"),
                            stats=dict(stage_record.stats),
                            processed_units=normalize_unit_ids(processed_source_ids),
                            substages={},
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
                        stage="normalize",
                        inputs=stage_inputs(layout, "normalize"),
                        artifacts=stage_artifacts(layout, "normalize"),
                        updated_at=timestamp_utc(),
                        unit=stage_unit("normalize"),
                        stats=sanitize_manifest_stats(dict(stage_record.stats), stage_name="normalize"),
                        processed_units=normalize_unit_ids(processed_source_ids),
                        substages={},
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
                    emit_prefilled_stage_progress(
                        stage_progress_callback,
                        stage_name,
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
                        progress_callback=offset_stage_progress_callback(
                            stage_progress_callback,
                            stage_name,
                            skipped_units=len(skipped_source_ids),
                            total_units=len(input_source_ids),
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
                    emit_prefilled_stage_progress(
                        stage_progress_callback,
                        stage_name,
                        current=len(skipped_source_ids),
                        total=len(input_source_ids),
                    )
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
                base_graph = load_graph_snapshot(layout, GRAPH_INPUT_STAGE[stage_name])
                scoped_detect_unit_ids = detect_unit_ids(base_graph, active_source_ids=set(input_source_ids))
                completed_detect_unit_ids = reusable_stage_unit_ids(
                    layout,
                    stage_name,
                    scoped_detect_unit_ids,
                    force_rebuild=force_rebuild,
                )
                skipped_units = len(completed_detect_unit_ids)
                completed_detect_unit_id_set = set(completed_detect_unit_ids)
                detect_owner_by_node = owner_document_by_node(base_graph)
                process_source_ids = sorted(
                    {
                        owner_source_id_for_node(detect_owner_by_node, unit_id)
                        for unit_id in scoped_detect_unit_ids
                        if unit_id not in completed_detect_unit_id_set
                    }
                )
                checkpoint_every = runtime.stage_checkpoint_every(stage_name)
                existing_rows = (
                    read_reference_candidates(layout.detect_candidates_path())
                    if layout.detect_candidates_path().exists()
                    else []
                )
                if process_source_ids:
                    emit_prefilled_stage_progress(
                        stage_progress_callback,
                        stage_name,
                        current=skipped_units,
                        total=len(scoped_detect_unit_ids),
                    )

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
                            input_source_ids=input_source_ids,
                            processed_source_ids=normalized_checkpoint_source_ids,
                            skipped_source_ids=[],
                            skipped_work_units=0,
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
                                unit_ids=scoped_detect_unit_ids,
                                processed_unit_ids=detect_unit_ids_for_sources(
                                    base_graph,
                                    active_source_ids=set(normalized_checkpoint_source_ids),
                                    selected_source_ids=set(input_source_ids),
                                ) + completed_detect_unit_ids,
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
                        progress_callback=offset_stage_progress_callback(
                            stage_progress_callback,
                            stage_name,
                            skipped_units=skipped_units,
                            total_units=len(scoped_detect_unit_ids),
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
                        input_source_ids=input_source_ids,
                        processed_source_ids=process_source_ids,
                        skipped_source_ids=[],
                        skipped_work_units=0,
                    )
                else:
                    emit_prefilled_stage_progress(
                        stage_progress_callback,
                        stage_name,
                        current=skipped_units,
                        total=len(scoped_detect_unit_ids),
                    )
                    stage_record.stats = build_stage_work_stats(
                        input_source_ids=input_source_ids,
                        processed_source_ids=[],
                        skipped_source_ids=[],
                        work_units_total=len(scoped_detect_unit_ids),
                        work_units_completed=skipped_units,
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
                        unit_ids=scoped_detect_unit_ids,
                        processed_unit_ids=completed_detect_unit_ids + detect_unit_ids_for_sources(
                            base_graph,
                            active_source_ids=set(process_source_ids),
                            selected_source_ids=set(input_source_ids),
                        ),
                        input_stage=GRAPH_INPUT_STAGE[stage_name],
                        artifact_paths=stage_record.artifact_paths,
                        stats=stage_record.stats,
                    ),
                )
            elif stage_name == "classify":
                input_source_ids = resolve_stage_source_ids(layout, stage_name, selected_source_ids)
                base_graph = load_graph_snapshot(layout, GRAPH_INPUT_STAGE[stage_name])
                candidates = read_reference_candidates(layout.detect_candidates_path())
                checkpoint_every = runtime.stage_checkpoint_every(stage_name)
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
                        force_rebuild=force_rebuild,
                    )
                    model_process_candidates = [
                        row for row in process_candidates if row.id not in set(model_reused_unit_ids)
                    ]
                    emit_prefilled_stage_progress(
                        stage_progress_callback,
                        stage_name,
                        current=skipped_units,
                        total=len(candidate_unit_ids),
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
                            processed_source_ids=[],
                            unit_ids=candidate_unit_ids,
                            processed_unit_ids=[],
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
                            del processed_checkpoint_source_ids
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
                            stage_record.stats = build_stage_work_stats(
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
                            stage_record.failures = []
                            write_stage_state(
                                layout,
                                build_stage_manifest(
                                    stage_name=stage_name,
                                    layout=layout,
                                    job_id=job_id,
                                    build_target=source_path_label,
                                    source_ids=selected_source_ids,
                                    processed_source_ids=candidate_source_ids(processed_checkpoint_unit_ids),
                                    unit_ids=candidate_unit_ids,
                                    processed_unit_ids=[],
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
                                total_units=len(process_candidate_unit_ids),
                            ),
                            checkpoint_every=checkpoint_every,
                            checkpoint_callback=checkpoint_classify_model,
                            cancel_event=cancel_event,
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
                            stage_progress_callback,
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
                            substage_states["llm_judge"] = build_unit_substage_manifest(
                                layout=layout,
                                parent_stage="classify",
                                stage_name="llm_judge",
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
                            stage_record.stats = build_stage_work_stats(
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
                            stage_record.failures = []
                            write_stage_state(
                                layout,
                                build_stage_manifest(
                                    stage_name=stage_name,
                                    layout=layout,
                                    job_id=job_id,
                                    build_target=source_path_label,
                                    source_ids=selected_source_ids,
                                    processed_source_ids=candidate_source_ids(completed_stage_candidate_ids),
                                    unit_ids=candidate_unit_ids,
                                    processed_unit_ids=[],
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
                        substage_states["llm_judge"] = build_unit_substage_manifest(
                            layout=layout,
                            parent_stage="classify",
                            stage_name="llm_judge",
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
                            stage_progress_callback,
                            f"{stage_name}::llm",
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
                        substage_states["llm_judge"] = build_unit_substage_manifest(
                            layout=layout,
                            parent_stage="classify",
                            stage_name="llm_judge",
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
                    stage_record.stats = build_stage_work_stats(
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
                    stage_record.failures = [dict(item) for item in llm_errors]
                else:
                    current_graph = load_graph_snapshot(layout, stage_name) if stage_outputs_exist(layout, stage_name) else GraphBundle()
                    emit_prefilled_stage_progress(
                        stage_progress_callback,
                        stage_name,
                        current=skipped_units,
                        total=len(candidate_unit_ids),
                    )
                    stage_record.stats = build_stage_work_stats(
                        input_source_ids=input_source_ids,
                        processed_source_ids=[],
                        skipped_source_ids=skipped_source_ids,
                        work_units_total=len(candidate_unit_ids),
                        work_units_completed=0,
                        work_units_skipped=skipped_units,
                        candidate_count=len(candidate_unit_ids),
                    )
                stage_record.artifact_paths = classify_artifact_paths(layout)
                manifest_processed_unit_ids = normalize_unit_ids(
                    completed_candidate_unit_ids
                    + substage_states.get("model", SubstageStateManifest()).processed_units
                    + substage_states.get("llm_judge", SubstageStateManifest()).processed_units
                )
                write_stage_state(
                    layout,
                    build_stage_manifest(
                        stage_name=stage_name,
                        layout=layout,
                        job_id=job_id,
                        build_target=source_path_label,
                        source_ids=selected_source_ids,
                        processed_source_ids=candidate_source_ids(manifest_processed_unit_ids),
                        unit_ids=candidate_unit_ids,
                        processed_unit_ids=[],
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

                input_source_ids_for_extract = filtered_source_ids
                input_skipped_source_ids = reusable_substage_unit_ids(
                    layout,
                    stage_name,
                    "input",
                    input_source_ids_for_extract,
                    force_rebuild=force_rebuild,
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
                        active_source_ids=set(input_process_source_ids),
                        progress_callback=offset_stage_progress_callback(
                            stage_progress_callback,
                            f"{stage_name}::input",
                            skipped_units=len(input_skipped_source_ids),
                            total_units=len(input_source_ids_for_extract),
                        ),
                        checkpoint_every=checkpoint_every,
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
                        stage_progress_callback,
                        f"{stage_name}::input",
                        current=len(input_skipped_source_ids),
                        total=len(input_source_ids_for_extract),
                    )
                aggregated_scope_inputs = select_extract_inputs(
                    existing_inputs,
                    graph_bundle=base_graph,
                    active_source_ids=set(input_source_ids_for_extract),
                )
                input_processed_source_ids = sorted(set(input_skipped_source_ids) | set(input_process_source_ids))
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
                    substage_states["extract"] = build_unit_substage_manifest(
                        layout=layout,
                        parent_stage="extract",
                        stage_name="extract",
                        processed_units=[],
                        stats=dict(stage_record.stats),
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
                        merged_concepts = replace_extract_concepts_by_unit_ids(
                            existing_concepts,
                            snapshot_concepts,
                            graph_bundle=base_graph,
                            active_unit_ids=set(successful_checkpoint_input_ids),
                        )
                        write_extract_concepts(layout.extract_concepts_path(), merged_concepts)
                        checkpoint_completed_input_ids = sorted(
                            set(extract_completed_input_ids) | set(processed_checkpoint_input_ids)
                        )
                        stage_record.artifact_paths = extract_artifact_paths(layout)
                        stage_record.stats = finalize_stage_work_stats(
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
                            stats=dict(stage_record.stats),
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
                                processed_source_ids=source_ids_for_input_rows(
                                    extract_scope_inputs,
                                    successful_checkpoint_input_ids,
                                ),
                                unit_ids=extract_scope_input_ids,
                                processed_unit_ids=[],
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
                    merged_concepts = replace_extract_concepts_by_unit_ids(
                        existing_concepts,
                        result.concepts,
                        graph_bundle=base_graph,
                        active_unit_ids=set(result.successful_input_ids),
                    )
                    write_extract_concepts(layout.extract_concepts_path(), merged_concepts)
                    stage_record.stats = finalize_stage_work_stats(
                        dict(result.stats),
                        input_source_ids=extract_scope_source_ids,
                        processed_source_ids=source_ids_for_input_rows(
                            extract_scope_inputs,
                            result.successful_input_ids,
                        ),
                        skipped_source_ids=extract_skipped_source_ids,
                        skipped_work_units=extract_skipped_units,
                    )
                    stage_record.failures = [dict(item) for item in result.llm_errors]
                    processed_source_ids = source_ids_for_input_rows(
                        extract_scope_inputs,
                        result.successful_input_ids,
                    )
                    substage_states["extract"] = build_unit_substage_manifest(
                        layout=layout,
                        parent_stage="extract",
                        stage_name="extract",
                        processed_units=normalize_unit_ids(extract_completed_input_ids + result.successful_input_ids),
                        stats=dict(stage_record.stats),
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
                    substage_states["extract"] = build_unit_substage_manifest(
                        layout=layout,
                        parent_stage="extract",
                        stage_name="extract",
                        processed_units=[],
                        stats=dict(stage_record.stats),
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
                        unit_ids=extract_scope_input_ids,
                        processed_unit_ids=[],
                        input_stage=GRAPH_INPUT_STAGE[stage_name],
                        artifact_paths=stage_record.artifact_paths,
                        stats=stage_record.stats,
                        substage_states=substage_states,
                    ),
                )
            elif stage_name == "aggregate":
                input_source_ids = resolve_stage_source_ids(layout, stage_name, selected_source_ids)
                base_graph = load_graph_snapshot(layout, GRAPH_INPUT_STAGE[stage_name])
                extract_inputs = read_extract_inputs(layout.extract_inputs_path()) if layout.extract_inputs_path().exists() else []
                extract_concepts = read_extract_concepts(layout.extract_concepts_path()) if layout.extract_concepts_path().exists() else []
                existing_aggregate_concepts = (
                    read_aggregate_concepts(layout.aggregate_concepts_path())
                    if layout.aggregate_concepts_path().exists()
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
                    layout,
                    stage_name,
                    aggregate_unit_ids,
                    force_rebuild=force_rebuild,
                )
                if (
                    can_reuse_stage(
                        layout,
                        stage_name,
                        aggregate_unit_ids,
                        force_rebuild=force_rebuild,
                    )
                    and layout.aggregate_concepts_path().exists()
                ):
                    emit_prefilled_stage_progress(
                        stage_progress_callback,
                        stage_name,
                        current=len(aggregate_completed_unit_ids),
                        total=len(aggregate_unit_ids),
                    )
                    stage_record.stats = dict(read_stage_manifest(layout.stage_manifest_path(stage_name)).stats)
                else:
                    checkpoint_every = runtime.stage_checkpoint_every(stage_name)
                    stage_record.artifact_paths = aggregate_artifact_paths(layout)
                    pending_aggregate_inputs = [
                        row for row in aggregate_inputs if row.id not in set(aggregate_completed_unit_ids)
                    ]
                    write_stage_state(
                        layout,
                        build_stage_manifest(
                            stage_name=stage_name,
                            layout=layout,
                            job_id=job_id,
                            build_target=source_path_label,
                            source_ids=[],
                            processed_source_ids=[],
                            processed_unit_ids=aggregate_completed_unit_ids,
                            input_stage=GRAPH_INPUT_STAGE[stage_name],
                            artifact_paths=stage_record.artifact_paths,
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
                        write_aggregate_concepts(layout.aggregate_concepts_path(), merged_concepts)
                        merged_output_stats = aggregate_concept_output_stats(merged_concepts)
                        stage_record.stats = finalize_stage_work_stats(
                            dict(snapshot_stats) | merged_output_stats,
                            input_source_ids=input_source_ids,
                            processed_source_ids=source_ids_for_input_rows(
                                aggregate_inputs,
                                successful_checkpoint_input_ids,
                            ),
                            skipped_source_ids=[],
                            skipped_work_units=0,
                        )
                        stage_record.failures = [dict(item) for item in llm_error_summary]
                        write_stage_state(
                            layout,
                            build_stage_manifest(
                                stage_name=stage_name,
                                layout=layout,
                                job_id=job_id,
                                build_target=source_path_label,
                                source_ids=[],
                                processed_source_ids=[],
                                processed_unit_ids=normalize_unit_ids(
                                    aggregate_completed_unit_ids + successful_checkpoint_input_ids
                                ),
                                input_stage=GRAPH_INPUT_STAGE[stage_name],
                                artifact_paths=aggregate_artifact_paths(layout),
                                stats=stage_record.stats,
                                status="running",
                            ),
                        )
                        write_job_log(layout.job_log_path(job_id), log_record)

                    emit_prefilled_stage_progress(
                        stage_progress_callback,
                        stage_name,
                        current=len(aggregate_completed_unit_ids),
                        total=len(aggregate_unit_ids),
                    )
                    aggregate_result = run_aggregate(
                        base_graph,
                        runtime,
                        inputs=pending_aggregate_inputs,
                        active_source_ids=set(input_source_ids),
                        progress_callback=offset_stage_progress_callback(
                            stage_progress_callback,
                            stage_name,
                            skipped_units=len(aggregate_completed_unit_ids),
                            total_units=len(aggregate_unit_ids),
                        ),
                        checkpoint_every=checkpoint_every,
                        checkpoint_callback=checkpoint_aggregate,
                        cancel_event=cancel_event,
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
                    write_aggregate_concepts(layout.aggregate_concepts_path(), merged_concepts)
                    merged_output_stats = aggregate_concept_output_stats(merged_concepts)
                    stage_record.stats = finalize_stage_work_stats(
                        dict(aggregate_result.stats) | merged_output_stats,
                        input_source_ids=input_source_ids,
                        processed_source_ids=source_ids_for_input_rows(
                            aggregate_inputs,
                            aggregate_result.successful_input_ids,
                        ),
                        skipped_source_ids=[],
                        skipped_work_units=0,
                    )
                    stage_record.failures = [dict(item) for item in aggregate_result.llm_errors]
                    write_stage_state(
                        layout,
                        build_stage_manifest(
                            stage_name=stage_name,
                            layout=layout,
                            job_id=job_id,
                            build_target=source_path_label,
                            source_ids=[],
                            processed_source_ids=[],
                            processed_unit_ids=normalize_unit_ids(
                                aggregate_completed_unit_ids + aggregate_result.successful_input_ids
                            ),
                            input_stage=GRAPH_INPUT_STAGE[stage_name],
                            artifact_paths=aggregate_artifact_paths(layout),
                            stats=stage_record.stats,
                        ),
                    )
                stage_record.artifact_paths = aggregate_artifact_paths(layout)
            elif stage_name == "align":
                input_source_ids = resolve_stage_source_ids(layout, stage_name, selected_source_ids)
                base_graph = load_graph_snapshot(layout, GRAPH_INPUT_STAGE[stage_name])
                aggregate_concepts = (
                    read_aggregate_concepts(layout.aggregate_concepts_path())
                    if layout.aggregate_concepts_path().exists()
                    else []
                )
                scope_aggregate_concepts = select_aggregate_concepts(
                    aggregate_concepts,
                    graph_bundle=base_graph,
                    active_source_ids=set(input_source_ids),
                )
                all_run_concepts = build_align_scope_concepts(aggregate_concepts)
                scope_concepts = build_align_scope_concepts(scope_aggregate_concepts)
                align_root_ids = normalize_unit_ids([row.root for row in scope_concepts])
                existing_align_state = read_align_stage_state(layout)
                retained_state = prune_align_stage_state(existing_align_state, scope_concepts)
                scoped_run_concepts = list(scope_concepts)
                scoped_run_concept_ids = normalize_unit_ids([row.id for row in scoped_run_concepts])
                reusable_embed_concept_ids = reusable_align_embed_concept_ids_for_scope(
                    layout,
                    scoped_run_concepts,
                    force_rebuild=force_rebuild,
                )
                reusable_embed_concept_id_set = set(reusable_embed_concept_ids)
                reusable_scoped_vectors = [
                    row for row in existing_align_state.vectors if row.id in reusable_embed_concept_id_set
                ]
                scoped_embed_concepts = [
                    row for row in scoped_run_concepts if row.id not in reusable_embed_concept_id_set
                ]
                available_scope_vectors = dedupe_vectors(reusable_scoped_vectors)
                reusable_recall_concept_ids = reusable_align_recall_concept_ids_for_scope(
                    layout,
                    scoped_run_concepts,
                    scope_vectors=available_scope_vectors,
                    retained_concepts=retained_state.concepts,
                    force_rebuild=force_rebuild,
                )
                reusable_recall_concept_id_set = set(reusable_recall_concept_ids)
                recall_reused_all = reusable_recall_concept_id_set == set(scoped_run_concept_ids)
                reusable_scoped_pairs = (
                    select_align_pairs_for_scope(existing_align_state.pairs, scoped_run_concept_ids)
                    if recall_reused_all
                    else []
                )
                reusable_judge_pair_ids = (
                    reusable_align_judge_pair_ids_for_scope(
                        layout,
                        reusable_scoped_pairs,
                        scoped_run_concepts,
                        retained_concepts=retained_state.concepts,
                        force_rebuild=force_rebuild,
                    )
                    if recall_reused_all
                    else []
                )
                reusable_judge_pair_id_set = set(reusable_judge_pair_ids)
                reusable_scoped_pairs_for_run = (
                    list(reusable_scoped_pairs)
                    if reusable_judge_pair_ids and reusable_judge_pair_id_set == {
                        f"{row.left_id}\t{row.right_id}" for row in reusable_scoped_pairs if row.relation
                    }
                    else clear_align_pair_relations(reusable_scoped_pairs)
                )
                scoped_recall_concepts = [] if recall_reused_all else list(scoped_run_concepts)
                prepared_state = AlignStageState(
                    concepts=dedupe_align_concepts(retained_state.concepts),
                    vectors=dedupe_vectors(retained_state.vectors + reusable_scoped_vectors),
                    pairs=dedupe_align_pairs(retained_state.pairs + reusable_scoped_pairs_for_run),
                    relations=list(retained_state.relations),
                )
                stage_record.artifact_paths = align_artifact_paths(layout)
                write_align_stage_artifacts(layout, prepared_state, graph_bundle=None)
                substage_states: dict[str, SubstageStateManifest] = {}
                write_stage_state(
                    layout,
                    build_stage_manifest(
                        stage_name=stage_name,
                        layout=layout,
                        job_id=job_id,
                        build_target=source_path_label,
                        source_ids=[],
                        processed_source_ids=[],
                        processed_unit_ids=[],
                        input_stage=GRAPH_INPUT_STAGE[stage_name],
                        artifact_paths=stage_record.artifact_paths,
                        stats={},
                        status="running",
                        substage_states=substage_states,
                    ),
                )

                def checkpoint_align_embed(
                    snapshot_vectors: list[ConceptVectorRecord],
                    snapshot_stats: dict[str, int],
                    processed_concept_ids: list[str],
                    llm_error_summary: list[dict[str, object]],
                ) -> None:
                    processed_embed_concept_ids = normalize_unit_ids(reusable_embed_concept_ids + processed_concept_ids)
                    merged_state = AlignStageState(
                        concepts=list(prepared_state.concepts),
                        vectors=dedupe_vectors(retained_state.vectors + reusable_scoped_vectors + list(snapshot_vectors)),
                        pairs=list(prepared_state.pairs),
                        relations=list(retained_state.relations),
                    )
                    write_align_stage_artifacts(layout, merged_state, graph_bundle=None)
                    substage_states["embed"] = build_unit_substage_manifest(
                        layout=layout,
                        parent_stage="align",
                        stage_name="embed",
                        processed_units=processed_embed_concept_ids,
                        stats=build_align_embed_manifest_stats(merged_state.vectors),
                    )
                    stage_record.failures = [dict(item) for item in llm_error_summary]
                    stage_record.stats = dict(stage_record.stats) | {
                        "concept_count": len(prepared_state.concepts),
                        "vector_count": len(merged_state.vectors),
                        "llm_request_count": int(snapshot_stats.get("llm_request_count", 0)),
                        "llm_error_count": int(snapshot_stats.get("llm_error_count", 0)),
                        "retry_count": int(snapshot_stats.get("retry_count", 0)),
                    }
                    write_stage_state(
                        layout,
                        build_stage_manifest(
                            stage_name=stage_name,
                            layout=layout,
                            job_id=job_id,
                            build_target=source_path_label,
                            source_ids=[],
                            processed_source_ids=[],
                            processed_unit_ids=[],
                            input_stage=GRAPH_INPUT_STAGE[stage_name],
                            artifact_paths=align_artifact_paths(layout),
                            stats=stage_record.stats,
                            status="running",
                            substage_states=substage_states,
                        ),
                    )
                    write_job_log(layout.job_log_path(job_id), log_record)

                def checkpoint_align_recall(
                    snapshot_pairs: list[AlignPairRecord],
                    snapshot_stats: dict[str, int],
                    processed_concept_ids: list[str],
                ) -> None:
                    processed_recall_concept_ids = normalize_unit_ids(reusable_recall_concept_ids + processed_concept_ids)
                    merged_state = AlignStageState(
                        concepts=list(prepared_state.concepts),
                        vectors=dedupe_vectors(retained_state.vectors + reusable_scoped_vectors),
                        pairs=dedupe_align_pairs(retained_state.pairs + reusable_scoped_pairs_for_run + list(snapshot_pairs)),
                        relations=list(retained_state.relations),
                    )
                    write_align_stage_artifacts(layout, merged_state, graph_bundle=None)
                    current_scope_vectors = [
                        row
                        for row in (
                            read_concept_vectors(layout.align_vectors_path())
                            if layout.align_vectors_path().exists()
                            else merged_state.vectors
                        )
                        if row.id in set(scoped_run_concept_ids)
                    ]
                    substage_states["recall"] = build_unit_substage_manifest(
                        layout=layout,
                        parent_stage="align",
                        stage_name="recall",
                        processed_units=processed_recall_concept_ids,
                        stats=build_align_recall_manifest_stats(merged_state.pairs),
                        metadata={
                            "scope_fingerprint": align_recall_fingerprint(
                                scoped_run_concepts,
                                current_scope_vectors,
                                retained_state.concepts,
                            ),
                        },
                    )
                    stage_record.stats = dict(stage_record.stats) | {
                        "pair_count": len(merged_state.pairs),
                        "pending_count": sum(1 for row in merged_state.pairs if row.relation == ""),
                    }
                    write_stage_state(
                        layout,
                        build_stage_manifest(
                            stage_name=stage_name,
                            layout=layout,
                            job_id=job_id,
                            build_target=source_path_label,
                            source_ids=[],
                            processed_source_ids=[],
                            processed_unit_ids=[],
                            input_stage=GRAPH_INPUT_STAGE[stage_name],
                            artifact_paths=align_artifact_paths(layout),
                            stats=stage_record.stats,
                            status="running",
                            substage_states=substage_states,
                        ),
                    )
                    write_job_log(layout.job_log_path(job_id), log_record)

                def checkpoint_align_judge(
                    snapshot_pairs: list[AlignPairRecord],
                    snapshot_stats: dict[str, int],
                    processed_pair_ids: list[str],
                    llm_error_summary: list[dict[str, object]],
                ) -> None:
                    processed_judge_pair_ids = normalize_unit_ids(reusable_judge_pair_ids + processed_pair_ids)
                    merged_state = AlignStageState(
                        concepts=list(prepared_state.concepts),
                        vectors=dedupe_vectors(retained_state.vectors + reusable_scoped_vectors),
                        pairs=dedupe_align_pairs(retained_state.pairs + list(snapshot_pairs)),
                        relations=list(retained_state.relations),
                    )
                    write_align_stage_artifacts(layout, merged_state, graph_bundle=None)
                    substage_states["judge"] = build_unit_substage_manifest(
                        layout=layout,
                        parent_stage="align",
                        stage_name="judge",
                        processed_units=processed_judge_pair_ids,
                        stats=build_align_judge_manifest_stats(merged_state.pairs),
                        metadata={
                            "scope_fingerprint": align_judge_fingerprint(
                                select_align_pairs_for_scope(snapshot_pairs, scoped_run_concept_ids),
                                scoped_run_concepts,
                                retained_state.concepts,
                            ),
                        },
                    )
                    stage_record.failures = [dict(item) for item in llm_error_summary]
                    stage_record.stats = dict(stage_record.stats) | {
                        "pair_count": len(merged_state.pairs),
                        "pending_count": sum(1 for row in merged_state.pairs if row.relation == ""),
                        "llm_request_count": int(snapshot_stats.get("llm_request_count", 0)),
                        "llm_error_count": int(snapshot_stats.get("llm_error_count", 0)),
                        "retry_count": int(snapshot_stats.get("retry_count", 0)),
                    }
                    write_stage_state(
                        layout,
                        build_stage_manifest(
                            stage_name=stage_name,
                            layout=layout,
                            job_id=job_id,
                            build_target=source_path_label,
                            source_ids=[],
                            processed_source_ids=[],
                            processed_unit_ids=[],
                            input_stage=GRAPH_INPUT_STAGE[stage_name],
                            artifact_paths=align_artifact_paths(layout),
                            stats=stage_record.stats,
                            status="running",
                            substage_states=substage_states,
                        ),
                    )
                    write_job_log(layout.job_log_path(job_id), log_record)

                align_result = run_align(
                    base_graph,
                    runtime,
                    all_concepts=all_run_concepts,
                    retained_vectors=dedupe_vectors(retained_state.vectors + reusable_scoped_vectors),
                    retained_pairs=dedupe_align_pairs(retained_state.pairs + reusable_scoped_pairs_for_run),
                    retained_concepts=retained_state.concepts,
                    scoped_concepts=scoped_run_concepts,
                    scoped_embed_concepts=scoped_embed_concepts,
                    scoped_recall_concepts=scoped_recall_concepts,
                    embed_progress_callback=dynamic_stage_progress_callback(
                        stage_progress_callback,
                        f"{stage_name}::embed",
                        skipped_units=len(reusable_embed_concept_ids),
                    ),
                    recall_progress_callback=dynamic_stage_progress_callback(
                        stage_progress_callback,
                        f"{stage_name}::recall",
                        skipped_units=len(reusable_recall_concept_ids),
                    ),
                    judge_progress_callback=dynamic_stage_progress_callback(
                        stage_progress_callback,
                        f"{stage_name}::judge",
                        skipped_units=len(reusable_judge_pair_ids),
                    ),
                    embed_checkpoint_every=runtime.substage_checkpoint_every(stage_name, "embed"),
                    recall_checkpoint_every=runtime.substage_checkpoint_every(stage_name, "recall"),
                    judge_checkpoint_every=runtime.substage_checkpoint_every(stage_name, "judge"),
                    embed_checkpoint_callback=checkpoint_align_embed,
                    recall_checkpoint_callback=checkpoint_align_recall,
                    judge_checkpoint_callback=checkpoint_align_judge,
                    cancel_event=cancel_event,
                )
                final_state = AlignStageState(
                    concepts=list(align_result.concepts),
                    vectors=list(align_result.vectors),
                    pairs=list(align_result.pairs),
                    relations=list(align_result.relations),
                )
                write_align_stage_artifacts(layout, final_state, graph_bundle=align_result.graph_bundle)
                current_graph = align_result.graph_bundle
                scope_pair_rows = select_align_pairs_for_scope(align_result.pairs, scoped_run_concept_ids)
                scope_pair_ids = [f"{row.left_id}\t{row.right_id}" for row in scope_pair_rows if row.relation]
                substage_states["embed"] = build_unit_substage_manifest(
                    layout=layout,
                    parent_stage="align",
                    stage_name="embed",
                    processed_units=scoped_run_concept_ids,
                    stats=build_align_embed_manifest_stats(align_result.vectors),
                )
                substage_states["recall"] = build_unit_substage_manifest(
                    layout=layout,
                    parent_stage="align",
                    stage_name="recall",
                    processed_units=scoped_run_concept_ids,
                    stats=build_align_recall_manifest_stats(align_result.pairs),
                    metadata={
                        "scope_fingerprint": align_recall_fingerprint(
                            scoped_run_concepts,
                            [row for row in align_result.vectors if row.id in set(scoped_run_concept_ids)],
                            retained_state.concepts,
                        ),
                    },
                )
                substage_states["judge"] = build_unit_substage_manifest(
                    layout=layout,
                    parent_stage="align",
                    stage_name="judge",
                    processed_units=scope_pair_ids,
                    stats=build_align_judge_manifest_stats(align_result.pairs),
                    metadata={
                        "scope_fingerprint": align_judge_fingerprint(
                            scope_pair_rows,
                            scoped_run_concepts,
                            retained_state.concepts,
                        ),
                    },
                )
                stage_record.stats = build_stage_work_stats(
                    input_source_ids=input_source_ids,
                    processed_source_ids=input_source_ids,
                    skipped_source_ids=[],
                    work_units_total=len(align_root_ids),
                    work_units_completed=len(align_root_ids),
                    work_units_skipped=0,
                    updated_nodes=int(align_result.stats.get("updated_nodes", 0)),
                    updated_edges=int(align_result.stats.get("updated_edges", 0)),
                    concept_count=len(align_result.concepts),
                    vector_count=len(align_result.vectors),
                    pair_count=len(align_result.pairs),
                    relation_count=len(align_result.relations),
                    llm_request_count=int(align_result.stats.get("llm_request_count", 0)),
                    llm_error_count=int(align_result.stats.get("llm_error_count", 0)),
                    retry_count=int(align_result.stats.get("retry_count", 0)),
                )
                stage_record.failures = [dict(item) for item in align_result.llm_errors]
                write_stage_state(
                    layout,
                    build_stage_manifest(
                        stage_name=stage_name,
                        layout=layout,
                        job_id=job_id,
                        build_target=source_path_label,
                        source_ids=[],
                        processed_source_ids=[],
                        processed_unit_ids=[],
                        input_stage=GRAPH_INPUT_STAGE[stage_name],
                        artifact_paths=align_artifact_paths(layout),
                        stats=stage_record.stats,
                        graph_bundle=align_result.graph_bundle,
                        substage_states=substage_states,
                    ),
                )
                stage_record.artifact_paths = align_artifact_paths(layout)
            else:
                raise ValueError(f"Unsupported stage: {stage_name}")

            if stage_name in GRAPH_STAGES:
                stage_current_align_concepts = (
                    read_align_canonical_concepts(layout.align_concepts_path())
                    if stage_name == "align" and layout.align_concepts_path().exists()
                    else []
                )
                stage_record.stats = dict(stage_record.stats) | build_graph_update_stats(
                    stage_previous_graph,
                    current_graph if current_graph is not None else GraphBundle(),
                    source_ids=selected_source_ids,
                    previous_align_concepts=stage_previous_align_concepts,
                    current_align_concepts=stage_current_align_concepts,
                )
                stage_record.stats = with_graph_stats(stage_record.stats, current_graph)
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
            if stage_error_callback is not None:
                stage_error_callback(
                    stage_name,
                    traceback.format_exc(),
                    str(layout.job_log_path(job_id)),
                )
            raise

    final_graph = GraphBundle()
    if through_stage in GRAPH_STAGES:
        final_graph = current_graph if current_graph is not None else load_graph_snapshot(layout, through_stage)
        if finalizing_callback is not None:
            finalizing_callback("finalize")
        write_stage_nodes(layout.final_nodes_path(), final_graph.nodes)
        write_stage_edges(layout.final_edges_path(), final_graph.edges)
    current_align_concepts = (
        read_align_canonical_concepts(layout.align_concepts_path())
        if through_stage == "align" and layout.align_concepts_path().exists()
        else []
    )
    log_record.status = "completed"
    log_record.finished_at = timestamp_utc()
    log_record.final_artifact_paths = {}
    if through_stage in GRAPH_STAGES:
        log_record.final_artifact_paths = {
            "nodes": str(layout.final_nodes_path()),
            "edges": str(layout.final_edges_path()),
        }
    final_graph_stats = build_graph_type_stats(final_graph)
    graph_update_stats = build_graph_update_stats(
        previous_final_graph,
        final_graph,
        source_ids=selected_source_ids,
        previous_align_concepts=previous_align_concepts,
        current_align_concepts=current_align_concepts,
    )
    log_record.stats = {
        "completed_stages": completed,
        "source_count": len(selected_source_ids),
        **graph_update_stats,
        **final_graph_stats,
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
        "source_count": len(selected_source_ids),
        "updated_nodes": int(graph_update_stats.get("updated_nodes", 0)),
        "updated_edges": int(graph_update_stats.get("updated_edges", 0)),
        "node_count": int(final_graph_stats.get("node_count", 0)),
        "edge_count": int(final_graph_stats.get("edge_count", 0)),
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
    return selected


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


def build_align_embed_manifest_stats(vectors: list[ConceptVectorRecord]) -> dict[str, int]:
    return {
        "vector_count": len(vectors),
        "result_count": len(vectors),
    }


def build_align_recall_manifest_stats(pairs: list[AlignPairRecord]) -> dict[str, int]:
    return {
        "pair_count": len(pairs),
        "result_count": len(pairs),
    }


def build_align_judge_manifest_stats(pairs: list[AlignPairRecord]) -> dict[str, int]:
    relation_keys = (
        "equivalent",
        "is_subordinate",
        "has_subordinate",
        "related",
        "none",
    )
    stats = {
        "pair_count": len(pairs),
        "result_count": len(pairs),
    }
    for relation in relation_keys:
        stats[f"{relation}_count"] = sum(1 for row in pairs if row.relation == relation)
    return stats


def build_graph_type_stats(graph_bundle: GraphBundle) -> dict[str, object]:
    return graph_type_stats(graph_bundle)


def build_graph_update_stats(
    previous_graph: GraphBundle | None,
    current_graph: GraphBundle,
    *,
    source_ids: list[str] | None = None,
    previous_align_concepts: list[EquivalenceRecord] | None = None,
    current_align_concepts: list[EquivalenceRecord] | None = None,
) -> dict[str, int]:
    selected_source_ids = set(normalize_source_ids(source_ids or []))

    def concept_root_map(rows: list[EquivalenceRecord] | None) -> dict[str, set[str]]:
        node_id_prefixes = (
            "document:",
            "part:",
            "chapter:",
            "section:",
            "article:",
            "paragraph:",
            "item:",
            "sub_item:",
            "segment:",
            "appendix:",
        )
        return {
            row.id: {
                source_id_from_node_id(root_id) if root_id.startswith(node_id_prefixes) else root_id
                for root_id in row.root_ids
                if str(root_id).strip()
            }
            for row in (rows or [])
            if row.id
        }

    previous_concept_roots = concept_root_map(previous_align_concepts)
    current_concept_roots = concept_root_map(current_align_concepts)
    previous = previous_graph or GraphBundle()
    previous_owners = owner_document_by_node(previous)
    current_owners = owner_document_by_node(current_graph)
    previous_edges_by_id = {edge.id: edge for edge in previous.edges if edge.id}
    current_edges_by_id = {edge.id: edge for edge in current_graph.edges if edge.id}

    def node_in_scope(
        node_id: str,
        *,
        owners: dict[str, str],
        concept_roots: dict[str, set[str]],
    ) -> bool:
        if not selected_source_ids:
            return True
        if node_id.startswith("concept:"):
            return bool(concept_roots.get(node_id, set()) & selected_source_ids)
        return owner_source_id_for_node(owners, node_id) in selected_source_ids

    def changed_record_count(
        previous_rows: list[object],
        current_rows: list[object],
        *,
        previous_scope: Callable[[str], bool],
        current_scope: Callable[[str], bool],
    ) -> int:
        previous_payloads = {
            str(getattr(row, "id")): dict(getattr(row, "to_dict")())
            for row in previous_rows
            if str(getattr(row, "id", "")).strip() and previous_scope(str(getattr(row, "id")))
        }
        current_payloads = {
            str(getattr(row, "id")): dict(getattr(row, "to_dict")())
            for row in current_rows
            if str(getattr(row, "id", "")).strip() and current_scope(str(getattr(row, "id")))
        }
        all_ids = set(previous_payloads) | set(current_payloads)
        return sum(1 for row_id in all_ids if previous_payloads.get(row_id) != current_payloads.get(row_id))

    previous = previous_graph or GraphBundle()

    def previous_edge_in_scope(edge_id: str) -> bool:
        edge = previous_edges_by_id.get(edge_id)
        if edge is None:
            return False
        return node_in_scope(edge.source, owners=previous_owners, concept_roots=previous_concept_roots) or node_in_scope(
            edge.target,
            owners=previous_owners,
            concept_roots=previous_concept_roots,
        )

    def current_edge_in_scope(edge_id: str) -> bool:
        edge = current_edges_by_id.get(edge_id)
        if edge is None:
            return False
        return node_in_scope(edge.source, owners=current_owners, concept_roots=current_concept_roots) or node_in_scope(
            edge.target,
            owners=current_owners,
            concept_roots=current_concept_roots,
        )

    return {
        "updated_nodes": changed_record_count(
            previous.nodes,
            current_graph.nodes,
            previous_scope=lambda node_id: node_in_scope(
                node_id,
                owners=previous_owners,
                concept_roots=previous_concept_roots,
            ),
            current_scope=lambda node_id: node_in_scope(
                node_id,
                owners=current_owners,
                concept_roots=current_concept_roots,
            ),
        ),
        "updated_edges": changed_record_count(
            previous.edges,
            current_graph.edges,
            previous_scope=previous_edge_in_scope,
            current_scope=current_edge_in_scope,
        ),
    }


MANIFEST_RUNTIME_STAT_KEYS = {
    "source_count",
    "succeeded_sources",
    "failed_sources",
    "reused_sources",
    "processed_source_count",
    "skipped_source_count",
    "work_units_total",
    "work_units_completed",
    "work_units_failed",
    "work_units_skipped",
    "work_units_attempted",
    "llm_request_count",
    "llm_error_count",
    "retry_count",
    "input_count",
}

MANIFEST_GRAPH_STAT_KEYS = {
    "node_count",
    "edge_count",
    "node_type_counts",
    "edge_type_counts",
}

MANIFEST_STAGE_STAT_KEYS: dict[str, set[str]] = {
    "normalize": {"document_count"},
    "structure": set(MANIFEST_GRAPH_STAT_KEYS),
    "detect": {"candidate_count"},
    "classify": {
        "result_count",
        "edge_count",
        "edge_type_counts",
        "interprets_count",
        "references_count",
        "ordinary_reference_count",
        "judicial_interprets_count",
        "judicial_references_count",
    },
    "extract": {"result_count", "concept_count"},
    "aggregate": {
        "result_count",
        "concept_count",
        "core_concept_count",
        "subordinate_concept_count",
    },
    "align": {
        "concept_count",
        "vector_count",
        "pair_count",
        "relation_count",
        *MANIFEST_GRAPH_STAT_KEYS,
    },
}

MANIFEST_SUBSTAGE_STAT_KEYS: dict[tuple[str, str], set[str]] = {
    ("classify", "model"): {
        "result_count",
        "interprets_count",
        "references_count",
        "ordinary_reference_count",
        "judicial_interprets_count",
        "judicial_references_count",
    },
    ("classify", "llm_judge"): {
        "result_count",
        "interprets_count",
        "references_count",
        "ordinary_reference_count",
        "judicial_interprets_count",
        "judicial_references_count",
    },
    ("extract", "input"): {"output_source_count", "result_count"},
    ("extract", "extract"): {"result_count", "concept_count"},
    ("align", "embed"): {"vector_count", "result_count"},
    ("align", "recall"): {"pair_count", "result_count"},
    ("align", "judge"): {
        "pair_count",
        "result_count",
        "equivalent_count",
        "is_subordinate_count",
        "has_subordinate_count",
        "related_count",
        "none_count",
    },
}


def sanitize_manifest_stats(
    stats: dict[str, object],
    *,
    stage_name: str,
    substage_name: str | None = None,
) -> dict[str, object]:
    cleaned = {
        key: value
        for key, value in dict(stats).items()
        if key not in MANIFEST_RUNTIME_STAT_KEYS
    }
    if substage_name is not None:
        allowed = MANIFEST_SUBSTAGE_STAT_KEYS.get((stage_name, substage_name), set())
    else:
        allowed = MANIFEST_STAGE_STAT_KEYS.get(stage_name, set())
    if stage_name == "normalize":
        document_count = cleaned.get("document_count", stats.get("document_count"))
        if document_count is None:
            document_count = stats.get("work_units_completed", stats.get("succeeded_sources", stats.get("source_count", 0)))
        cleaned["document_count"] = int(document_count or 0)
    return {
        key: value
        for key, value in cleaned.items()
        if key in allowed
    }


def with_graph_stats(stats: dict[str, object], graph_bundle: GraphBundle | None) -> dict[str, object]:
    merged = dict(stats)
    if graph_bundle is not None:
        merged.update(build_graph_type_stats(graph_bundle))
    return merged


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


def reusable_stage_unit_ids(
    layout: BuildLayout,
    stage_name: str,
    unit_ids: list[str],
    *,
    force_rebuild: bool = False,
) -> list[str]:
    if force_rebuild or not unit_ids:
        return []
    manifest_path = layout.stage_manifest_path(stage_name)
    if not manifest_path.exists():
        return []
    manifest = read_stage_manifest(manifest_path)
    processed_unit_ids = set(normalize_unit_ids(manifest.processed_units))
    return [unit_id for unit_id in normalize_unit_ids(unit_ids) if unit_id in processed_unit_ids]


def build_unit_substage_manifest(
    *,
    layout: BuildLayout,
    parent_stage: str,
    stage_name: str,
    processed_units: list[str],
    stats: dict[str, object],
    metadata: dict[str, object] | None = None,
) -> SubstageStateManifest:
    normalized_processed = normalize_unit_ids(processed_units)
    return SubstageStateManifest(
        inputs=substage_inputs(layout, parent_stage, stage_name),
        artifacts=substage_artifacts(layout, parent_stage, stage_name),
        updated_at=timestamp_utc(),
        unit=substage_unit(parent_stage, stage_name),
        stats=sanitize_manifest_stats(dict(stats), stage_name=parent_stage, substage_name=stage_name),
        metadata=dict(metadata or {}),
        processed_units=normalized_processed,
    )


def merge_unit_substage_manifest(
    *,
    parent_stage: str,
    stage_name: str,
    previous: SubstageStateManifest | None,
    current: SubstageStateManifest,
) -> SubstageStateManifest:
    if previous is None:
        return current
    merged_stats = (
        dict(current.stats)
        if current.stats
        else sanitize_manifest_stats(dict(previous.stats), stage_name=parent_stage, substage_name=stage_name)
    )
    merged_metadata = (
        dict(current.metadata)
        if current.metadata
        else dict(previous.metadata)
    )
    return SubstageStateManifest(
        inputs=list(current.inputs),
        artifacts=list(current.artifacts),
        updated_at=current.updated_at,
        unit=current.unit,
        stats=merged_stats,
        metadata=merged_metadata,
        processed_units=normalize_unit_ids(previous.processed_units + current.processed_units),
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
    processed_unit_ids = set(normalize_unit_ids(manifest.processed_units))
    return [source_id for source_id in normalize_source_ids(source_ids) if source_id in processed_unit_ids]


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
    if substage is None:
        return []
    reusable_unit_ids = [
        unit_id
        for unit_id in normalize_unit_ids(unit_ids)
        if unit_id in set(normalize_unit_ids(substage.processed_units))
    ]
    if validator is not None and reusable_unit_ids and not validator(reusable_unit_ids, substage):
        return []
    return reusable_unit_ids


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
    expected_count = int(substage.stats.get("result_count", 0))
    if expected_count > 0 and len(scoped_inputs) != expected_count:
        return False
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
    expected_count = int(substage.stats.get("result_count", 0))
    if expected_count > 0 and len(scoped_inputs) != expected_count:
        return False
    expected_result_count = int(substage.stats.get("result_count", 0))
    if expected_result_count > 0 and len(scoped_concepts) != expected_result_count:
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


def source_ids_for_input_rows(rows: list[object], input_ids: list[str]) -> list[str]:
    if not rows or not input_ids:
        return []
    unit_source_map = build_unit_source_map(
        {
            str(getattr(row, "id", "")): source_id_from_node_id(str(getattr(row, "id", "")))
            for row in rows
            if str(getattr(row, "id", ""))
        }
    )
    return source_ids_for_unit_ids(input_ids, unit_source_map)


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
    input_source_ids: list[str],
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
            input_source_ids=input_source_ids,
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


def dynamic_stage_progress_callback(
    callback: Callable[[str, int, int], None] | None,
    stage_name: str,
    *,
    skipped_units: int = 0,
) -> Callable[[int, int], None]:
    def report(current: int, total: int) -> None:
        emit_prefilled_stage_progress(
            callback,
            stage_name,
            current=skipped_units + current,
            total=max(skipped_units + total, 1),
        )

    return report


def build_stage_manifest(
    *,
    stage_name: str,
    layout: BuildLayout,
    job_id: str,
    build_target: str,
    source_ids: list[str],
    processed_source_ids: list[str],
    unit_ids: list[str] | None = None,
    processed_unit_ids: list[str] | None = None,
    input_stage: str,
    artifact_paths: dict[str, str],
    stats: dict[str, object],
    metadata: dict[str, object] | None = None,
    graph_bundle: GraphBundle | None = None,
    status: str = "completed",
    substage_states: dict[str, SubstageStateManifest] | None = None,
) -> StageStateManifest:
    has_substages = stage_name in SUBSTAGE_PARENT_STAGES
    graph_stats = build_graph_type_stats(graph_bundle) if graph_bundle is not None else {}
    current_processed_unit_ids = normalize_unit_ids(
        processed_unit_ids if processed_unit_ids is not None else processed_source_ids
    )
    merged_substages: dict[str, SubstageStateManifest] = {}
    if layout.stage_manifest_path(stage_name).exists():
        previous = read_stage_manifest(layout.stage_manifest_path(stage_name))
        merged_processed_unit_ids = (
            []
            if has_substages
            else normalize_unit_ids(previous.processed_units + current_processed_unit_ids)
        )
        merged_substages = {
            name: SubstageStateManifest(
                inputs=list(state.inputs),
                artifacts=list(state.artifacts),
                updated_at=state.updated_at,
                unit=state.unit,
                stats=sanitize_manifest_stats(dict(state.stats), stage_name=stage_name, substage_name=name),
                metadata=dict(state.metadata),
                processed_units=normalize_unit_ids(state.processed_units),
            )
            for name, state in previous.substages.items()
        }
        merged_stats = (
            sanitize_manifest_stats({**dict(stats), **graph_stats}, stage_name=stage_name)
            if stats
            else sanitize_manifest_stats({**dict(previous.stats), **graph_stats}, stage_name=stage_name)
        )
        merged_metadata = dict(metadata) if metadata else dict(previous.metadata)
    else:
        merged_processed_unit_ids = [] if has_substages else current_processed_unit_ids
        merged_stats = sanitize_manifest_stats({**dict(stats), **graph_stats}, stage_name=stage_name)
        merged_metadata = dict(metadata or {})
    for name, state in (substage_states or {}).items():
        merged_substages[name] = merge_unit_substage_manifest(
            parent_stage=stage_name,
            stage_name=name,
            previous=merged_substages.get(name),
            current=state,
        )
    if stage_name == "extract":
        merged_substages = {
            name: state
            for name, state in merged_substages.items()
            if name in {"input", "extract"}
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
            if name in {"embed", "recall", "judge"}
        }
    return StageStateManifest(
        stage=stage_name,
        inputs=stage_inputs(layout, stage_name),
        artifacts=stage_artifacts(layout, stage_name),
        updated_at=timestamp_utc(),
        unit=stage_unit(stage_name),
        stats=merged_stats,
        metadata=merged_metadata,
        processed_units=list(merged_processed_unit_ids),
        substages=merged_substages,
    )


def write_stage_state(layout: BuildLayout, manifest: StageStateManifest) -> None:
    write_stage_manifest(layout.stage_manifest_path(manifest.stage), manifest)


def stage_outputs_exist(layout: BuildLayout, stage_name: str) -> bool:
    return layout.stage_nodes_path(stage_name).exists() or layout.stage_edges_path(stage_name).exists()


def can_reuse_stage(
    layout: BuildLayout,
    stage_name: str,
    unit_ids: list[str],
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
    if set(normalize_unit_ids(unit_ids)) - set(normalize_unit_ids(manifest.processed_units)):
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
            nodes = read_stage_nodes_unchecked(layout.stage_nodes_path(candidate))
        if candidate in GRAPH_STAGES and not edges and layout.stage_edges_path(candidate).exists():
            edges = read_stage_edges_unchecked(layout.stage_edges_path(candidate))
        if nodes and edges:
            break
    return GraphBundle(nodes=nodes, edges=edges)


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


@dataclass
class AlignStageState:
    concepts: list[EquivalenceRecord]
    vectors: list[ConceptVectorRecord]
    pairs: list[AlignPairRecord]
    relations: list[AlignRelationRecord]


def read_align_stage_state(layout: BuildLayout) -> AlignStageState:
    return AlignStageState(
        concepts=read_align_canonical_concepts(layout.align_concepts_path()) if layout.align_concepts_path().exists() else [],
        vectors=read_concept_vectors(layout.align_vectors_path()) if layout.align_vectors_path().exists() else [],
        pairs=read_align_pairs(layout.align_pairs_path()) if layout.align_pairs_path().exists() else [],
        relations=read_align_relations(layout.align_relations_path()) if layout.align_relations_path().exists() else [],
    )


def build_align_scope_concepts(rows: list[AggregateConceptRecord]) -> list[AlignConceptRecord]:
    return [
        AlignConceptRecord(
            id=row.id,
            name=row.name,
            description=row.description,
            parent=row.parent,
            root=row.root,
        )
        for row in rows
    ]


def reusable_align_embed_concept_ids_for_scope(
    layout: BuildLayout,
    scope_concepts: list[AlignConceptRecord],
    *,
    force_rebuild: bool,
) -> list[str]:
    concept_ids = normalize_unit_ids([row.id for row in scope_concepts])
    if not concept_ids:
        return []
    current_by_id = {row.id: row for row in scope_concepts}

    def validator(reusable_unit_ids: list[str], substage: SubstageStateManifest) -> bool:
        del substage
        if not layout.align_vectors_path().exists():
            return False
        existing_vectors = {row.id: row for row in read_concept_vectors(layout.align_vectors_path())}
        for concept_id in reusable_unit_ids:
            current = current_by_id.get(concept_id)
            if current is None or concept_id not in existing_vectors:
                return False
        return True

    return reusable_substage_unit_ids(
        layout,
        "align",
        "embed",
        concept_ids,
        force_rebuild=force_rebuild,
        validator=validator,
    )


def reusable_align_recall_concept_ids_for_scope(
    layout: BuildLayout,
    scope_concepts: list[AlignConceptRecord],
    *,
    scope_vectors: list[ConceptVectorRecord],
    retained_concepts: list[EquivalenceRecord],
    force_rebuild: bool,
) -> list[str]:
    concept_ids = normalize_unit_ids([row.id for row in scope_concepts])
    if not concept_ids:
        return []
    fingerprint = align_recall_fingerprint(scope_concepts, scope_vectors, retained_concepts)

    def validator(reusable_unit_ids: list[str], substage: SubstageStateManifest) -> bool:
        if str(substage.metadata.get("scope_fingerprint", "")) != fingerprint:
            return False
        if not layout.align_pairs_path().exists():
            return False
        existing_pairs = select_align_pairs_for_scope(read_align_pairs(layout.align_pairs_path()), concept_ids)
        return int(substage.stats.get("result_count", len(existing_pairs))) == len(existing_pairs)

    return reusable_substage_unit_ids(
        layout,
        "align",
        "recall",
        concept_ids,
        force_rebuild=force_rebuild,
        validator=validator,
    )


def reusable_align_judge_pair_ids_for_scope(
    layout: BuildLayout,
    scope_pairs: list[AlignPairRecord],
    scope_concepts: list[AlignConceptRecord],
    *,
    retained_concepts: list[EquivalenceRecord],
    force_rebuild: bool,
) -> list[str]:
    pair_ids = normalize_unit_ids([f"{row.left_id}\t{row.right_id}" for row in scope_pairs if row.relation])
    if not pair_ids:
        return []
    fingerprint = align_judge_fingerprint(scope_pairs, scope_concepts, retained_concepts)

    def validator(reusable_unit_ids: list[str], substage: SubstageStateManifest) -> bool:
        if str(substage.metadata.get("scope_fingerprint", "")) != fingerprint:
            return False
        if not layout.align_pairs_path().exists():
            return False
        existing_pairs = select_align_pairs_for_scope(
            read_align_pairs(layout.align_pairs_path()),
            [row.id for row in scope_concepts],
        )
        judged_pair_ids = {
            f"{row.left_id}\t{row.right_id}"
            for row in existing_pairs
            if row.relation
        }
        return set(reusable_unit_ids).issubset(judged_pair_ids)

    return reusable_substage_unit_ids(
        layout,
        "align",
        "judge",
        pair_ids,
        force_rebuild=force_rebuild,
        validator=validator,
    )


def select_align_pairs_for_scope(rows: list[AlignPairRecord], concept_ids: list[str]) -> list[AlignPairRecord]:
    concept_id_set = set(normalize_unit_ids(concept_ids))
    return [row for row in dedupe_align_pairs(rows) if row.left_id in concept_id_set]


def clear_align_pair_relations(rows: list[AlignPairRecord]) -> list[AlignPairRecord]:
    return [replace(row, relation="") for row in rows]


def align_recall_fingerprint(
    scope_concepts: list[AlignConceptRecord],
    scope_vectors: list[ConceptVectorRecord],
    retained_concepts: list[EquivalenceRecord],
) -> str:
    vector_by_id = {row.id: row for row in scope_vectors}
    payload = {
        "scope_concepts": [row.to_dict() for row in sorted(scope_concepts, key=lambda item: item.id)],
        "vectors": [
            vector_by_id[row.id].to_dict()
            for row in sorted(scope_concepts, key=lambda item: item.id)
            if row.id in vector_by_id
        ],
        "retained_concepts": [row.to_dict() for row in sorted(retained_concepts, key=lambda item: item.id)],
    }
    return stable_payload_fingerprint(payload)


def align_judge_fingerprint(
    scope_pairs: list[AlignPairRecord],
    scope_concepts: list[AlignConceptRecord],
    retained_concepts: list[EquivalenceRecord],
) -> str:
    payload = {
        "pairs": [row.to_dict() for row in dedupe_align_pairs(scope_pairs)],
        "scope_concepts": [row.to_dict() for row in sorted(scope_concepts, key=lambda item: item.id)],
        "retained_concepts": [row.to_dict() for row in sorted(retained_concepts, key=lambda item: item.id)],
    }
    return stable_payload_fingerprint(payload)


def stable_payload_fingerprint(payload: object) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()


def prune_align_stage_state(state: AlignStageState, scoped_concepts: list[AlignConceptRecord]) -> AlignStageState:
    active_roots = set(normalize_unit_ids([row.root for row in scoped_concepts]))
    removed_raw_ids = {row.id for row in scoped_concepts}
    kept_raw_ids = {
        member_id
        for row in state.concepts
        for member_id in row.member_ids
        if member_id not in removed_raw_ids
    }
    kept_vectors = [row for row in state.vectors if row.id in kept_raw_ids]
    kept_concepts: list[EquivalenceRecord] = []
    removed_concept_ids: set[str] = set()
    touched_concept_ids: set[str] = set()
    for row in state.concepts:
        member_ids = [member_id for member_id in row.member_ids if member_id in kept_raw_ids]
        if len(member_ids) != len(row.member_ids):
            touched_concept_ids.add(row.id)
        if not member_ids:
            removed_concept_ids.add(row.id)
            continue
        root_ids = [root_id for root_id in row.root_ids if root_id not in active_roots]
        kept_concepts.append(
            EquivalenceRecord(
                id=row.id,
                name=row.name,
                description=row.description,
                member_ids=sorted(member_ids),
                root_ids=sorted(root_ids),
            )
        )
    affected_concept_ids = removed_concept_ids | touched_concept_ids
    kept_pairs = [
        row
        for row in state.pairs
        if row.left_id not in removed_raw_ids
        and row.right_id not in removed_raw_ids
        and row.left_id not in affected_concept_ids
        and row.right_id not in affected_concept_ids
    ]
    kept_relations = [
        row
        for row in state.relations
        if row.left_id not in affected_concept_ids and row.right_id not in affected_concept_ids
    ]
    return AlignStageState(
        concepts=sorted(kept_concepts, key=lambda item: item.id),
        vectors=kept_vectors,
        pairs=dedupe_align_pairs(kept_pairs),
        relations=dedupe_align_relations(kept_relations),
    )


def dedupe_align_concepts(rows: list[EquivalenceRecord]) -> list[EquivalenceRecord]:
    deduped = {row.id: row for row in rows}
    return [deduped[key] for key in sorted(deduped)]


def dedupe_vectors(rows: list[ConceptVectorRecord]) -> list[ConceptVectorRecord]:
    deduped = {row.id: row for row in rows}
    return [deduped[key] for key in sorted(deduped)]


def dedupe_align_pairs(rows: list[AlignPairRecord]) -> list[AlignPairRecord]:
    deduped = {(row.left_id, row.right_id): row for row in rows if row.left_id and row.right_id}
    return [deduped[key] for key in sorted(deduped)]


def dedupe_align_relations(rows: list[AlignRelationRecord]) -> list[AlignRelationRecord]:
    deduped = {(row.left_id, row.right_id, row.relation): row for row in rows if row.left_id and row.right_id}
    return [deduped[key] for key in sorted(deduped)]


def write_align_stage_artifacts(
    layout: BuildLayout,
    state: AlignStageState,
    *,
    graph_bundle: GraphBundle | None,
) -> None:
    write_align_canonical_concepts(layout.align_concepts_path(), dedupe_align_concepts(state.concepts))
    write_concept_vectors(layout.align_vectors_path(), dedupe_vectors(state.vectors))
    write_align_pairs(layout.align_pairs_path(), dedupe_align_pairs(state.pairs))
    write_align_relations(layout.align_relations_path(), dedupe_align_relations(state.relations))
    if graph_bundle is not None:
        write_stage_graph(
            layout,
            "align",
            graph_bundle,
            write_nodes=True,
            write_edges=True,
        )


def align_artifact_paths(layout: BuildLayout) -> dict[str, str]:
    return {
        "primary": str(layout.stage_nodes_path("align")),
        "concepts": str(layout.align_concepts_path()),
        "vectors": str(layout.align_vectors_path()),
        "pairs": str(layout.align_pairs_path()),
        "relations": str(layout.align_relations_path()),
        "nodes": str(layout.stage_nodes_path("align")),
        "edges": str(layout.stage_edges_path("align")),
    }


def source_ids_for_roots(root_ids: list[str]) -> list[str]:
    return normalize_source_ids([source_id_from_node_id(root_id) for root_id in root_ids if root_id])


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
    if stage_name == "aggregate":
        _unlink_if_exists(layout.aggregate_concepts_path())
    if stage_name == "align":
        _unlink_if_exists(layout.align_concepts_path())
        _unlink_if_exists(layout.align_vectors_path())
        _unlink_if_exists(layout.align_pairs_path())
        _unlink_if_exists(layout.align_relations_path())
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


def aggregate_artifact_paths(layout: BuildLayout) -> dict[str, str]:
    return {
        "primary": str(layout.aggregate_concepts_path()),
        "concepts": str(layout.aggregate_concepts_path()),
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
