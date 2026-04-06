from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

from ..contracts import GraphBundle, JobManifest, StageRecord
from ..io import (
    BuildLayout,
    ensure_stage_dirs,
    read_graph_bundle,
    read_normalize_index,
    write_graph_bundle,
    write_manifest,
)
from ..stages import (
    run_entity_alignment,
    run_entity_extraction,
    run_explicit_relations,
    run_implicit_reasoning,
    run_normalize,
    run_structure_graph,
)
from ..utils.ids import slugify, timestamp_utc
from .runtime import PipelineRuntime

STAGE_SEQUENCE = (
    "normalize",
    "structure_graph",
    "explicit_relations",
    "entity_extraction",
    "entity_alignment",
    "implicit_reasoning",
)


def build_knowledge_graph(
    *,
    source_id: str,
    data_root: Path,
    start_stage: str | None = None,
    through_stage: str = "implicit_reasoning",
    force_rebuild: bool = False,
    report_progress: bool = False,
    stage_callback: Callable[[int, int], None] | None = None,
    stage_progress_callback: Callable[[str, int, int], None] | None = None,
    stage_name_callback: Callable[[str], None] | None = None,
    stage_summary_callback: Callable[[str, dict[str, int]], None] | None = None,
    finalizing_callback: Callable[[str], None] | None = None,
) -> dict[str, object]:
    del report_progress
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
    )


def build_batch_knowledge_graph(
    data_root: Path,
    pattern: str = "*.docx",
    category: str | None = None,
    start_stage: str | None = None,
    through_stage: str = "implicit_reasoning",
    force_rebuild: bool = False,
    report_progress: bool = False,
    discovery_callback: Callable[[int], None] | None = None,
    progress_callback: Callable[[str, int, int], None] | None = None,
    stage_summary_callback: Callable[[str, dict[str, int]], None] | None = None,
    finalizing_callback: Callable[[str], None] | None = None,
) -> dict[str, object]:
    del pattern
    del report_progress
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
        job_id=f"batch-{timestamp_utc().replace(':', '').replace('-', '')}",
        source_path_label=f"batch:{category or 'all'}",
        stage_progress_callback=progress_callback,
        stage_summary_callback=stage_summary_callback,
        finalizing_callback=finalizing_callback,
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
    stage_callback: Callable[[int, int], None] | None = None,
    stage_progress_callback: Callable[[str, int, int], None] | None = None,
    stage_name_callback: Callable[[str], None] | None = None,
    stage_summary_callback: Callable[[str, dict[str, int]], None] | None = None,
    finalizing_callback: Callable[[str], None] | None = None,
) -> dict[str, object]:
    data_root = data_root.resolve()
    ensure_stage_dirs(data_root)
    layout = BuildLayout(data_root)
    start = start_stage or STAGE_SEQUENCE[0]
    stage_names = iter_stage_range(start, through_stage)
    manifest = JobManifest(
        job_id=job_id,
        build_target=source_path_label,
        data_root=str(data_root),
        status="running",
        started_at=timestamp_utc(),
        start_stage=start,
        end_stage=through_stage,
        source_count=len(source_ids),
    )
    write_manifest(layout.manifest_path(job_id), manifest)
    runtime = PipelineRuntime(data_root)

    completed = 0
    total = len(stage_names)
    graph_bundle: GraphBundle | None = None
    selected_source_ids = sorted(dict.fromkeys(source_ids))

    for stage_name in stage_names:
        if stage_name_callback is not None:
            stage_name_callback(stage_name)
        stage_record = StageRecord(
            name=stage_name,
            status="running",
            started_at=timestamp_utc(),
        )
        manifest.stages.append(stage_record)
        write_manifest(layout.manifest_path(job_id), manifest)

        try:
            if stage_name == "normalize":
                normalize_index = run_normalize(
                    data_root,
                    source_ids=selected_source_ids,
                    force_rebuild=force_rebuild,
                    progress_callback=lambda current, total: emit_stage_progress(
                        stage_progress_callback,
                        stage_name,
                        current,
                        total,
                    ),
                )
                stage_record.failures = [
                    {
                        "source_id": entry.source_id,
                        "source_path": entry.document_path,
                        "error_type": entry.error_type,
                        "message": entry.message,
                    }
                    for entry in normalize_index.entries
                    if entry.status != "completed"
                ]
                if not normalize_index.stats.get("succeeded_sources", 0):
                    raise ValueError("Normalize stage did not produce any valid normalized documents.")
                stage_record.artifact_paths = {
                    "primary": str(layout.normalize_index_path()),
                    "index": str(layout.normalize_index_path()),
                    "log": str(layout.normalize_log_path()),
                }
                stage_record.stats = dict(normalize_index.stats)
            else:
                reused_bundle = None if force_rebuild else try_reuse_graph_stage(
                    layout=layout,
                    stage_name=stage_name,
                    source_ids=selected_source_ids,
                )
                if reused_bundle is not None:
                    graph_bundle = reused_bundle
                    emit_stage_progress(stage_progress_callback, stage_name, len(selected_source_ids), len(selected_source_ids))
                    stage_record.graph_path = str(layout.stage_graph_path(stage_name))
                    stage_record.artifact_paths = {
                        "primary": str(layout.stage_graph_path(stage_name)),
                        "graph_bundle": str(layout.stage_graph_path(stage_name)),
                    }
                    stage_record.stats = build_graph_stage_stats(
                        graph_bundle=graph_bundle,
                        source_ids=selected_source_ids,
                        reused_sources=len(selected_source_ids),
                    )
                else:
                    if stage_name == "structure_graph":
                        graph_bundle = run_structure_graph(
                            data_root,
                            source_ids=selected_source_ids,
                            progress_callback=lambda current, total: emit_stage_progress(
                                stage_progress_callback,
                                stage_name,
                                current,
                                total,
                            ),
                        )
                    else:
                        previous_stage = previous_stage_name(stage_name)
                        graph_bundle = graph_bundle or read_graph_bundle(layout.stage_graph_path(previous_stage))
                        graph_bundle.metadata["source_ids"] = list(selected_source_ids)
                        if stage_name == "explicit_relations":
                            graph_bundle = run_explicit_relations(
                                graph_bundle,
                                runtime,
                                progress_callback=lambda current, total: emit_stage_progress(
                                    stage_progress_callback,
                                    stage_name,
                                    current,
                                    total,
                                ),
                            )
                        elif stage_name == "entity_extraction":
                            graph_bundle = run_entity_extraction(
                                graph_bundle,
                                runtime,
                                progress_callback=lambda current, total: emit_stage_progress(
                                    stage_progress_callback,
                                    stage_name,
                                    current,
                                    total,
                                ),
                            )
                        elif stage_name == "entity_alignment":
                            graph_bundle = run_entity_alignment(
                                graph_bundle,
                                runtime,
                                progress_callback=lambda current, total: emit_stage_progress(
                                    stage_progress_callback,
                                    stage_name,
                                    current,
                                    total,
                                ),
                            )
                        elif stage_name == "implicit_reasoning":
                            graph_bundle = run_implicit_reasoning(
                                graph_bundle,
                                runtime,
                                progress_callback=lambda current, total: emit_stage_progress(
                                    stage_progress_callback,
                                    stage_name,
                                    current,
                                    total,
                                ),
                            )
                        else:
                            raise ValueError(f"Unsupported stage: {stage_name}")
                    graph_bundle.metadata["source_ids"] = list(selected_source_ids)
                    graph_bundle.metadata["stage"] = stage_name
                    write_graph_bundle(layout.stage_graph_path(stage_name), graph_bundle)
                    stage_record.graph_path = str(layout.stage_graph_path(stage_name))
                    stage_record.artifact_paths = {
                        "primary": str(layout.stage_graph_path(stage_name)),
                        "graph_bundle": str(layout.stage_graph_path(stage_name)),
                    }
                    stage_record.stats = build_graph_stage_stats(
                        graph_bundle=graph_bundle,
                        source_ids=selected_source_ids,
                        reused_sources=0,
                    )

            stage_record.status = "completed"
            stage_record.finished_at = timestamp_utc()
            completed += 1
            emit_stage_summary(stage_summary_callback, stage_name, stage_record.stats)
            if stage_callback is not None:
                stage_callback(completed, total)
            write_manifest(layout.manifest_path(job_id), manifest)
        except Exception as exc:
            stage_record.status = "failed"
            stage_record.error = str(exc)
            stage_record.finished_at = timestamp_utc()
            manifest.status = "failed"
            manifest.finished_at = timestamp_utc()
            emit_stage_summary(stage_summary_callback, stage_name, stage_record.stats, failed_override=max(len(source_ids), len(stage_record.failures) or 1))
            write_manifest(layout.manifest_path(job_id), manifest)
            raise

    if graph_bundle is not None:
        if finalizing_callback is not None:
            finalizing_callback("finalize")
        write_graph_bundle(layout.final_graph_path(), graph_bundle)
    normalize_failures = sum(len(stage.failures) for stage in manifest.stages if stage.name == "normalize")
    manifest.status = "partial" if normalize_failures else "completed"
    manifest.finished_at = timestamp_utc()
    manifest.final_graph_path = str(layout.final_graph_path()) if graph_bundle is not None else ""
    manifest.stats = {
        "completed_stages": completed,
        "source_count": len(selected_source_ids),
        "succeeded_sources": len(selected_source_ids) - normalize_failures,
        "failed_sources": normalize_failures,
        "final_nodes": len(graph_bundle.nodes) if graph_bundle is not None else 0,
        "final_edges": len(graph_bundle.edges) if graph_bundle is not None else 0,
    }
    write_manifest(layout.manifest_path(job_id), manifest)
    artifact_paths = {
        stage.name: stage.artifact_paths.get("primary", stage.graph_path)
        for stage in manifest.stages
        if stage.artifact_paths or stage.graph_path
    }
    if manifest.final_graph_path:
        artifact_paths["final_graph_bundle"] = manifest.final_graph_path
    return {
        "status": manifest.status,
        "job_id": job_id,
        "start_stage": start,
        "through_stage": through_stage,
        "manifest_path": str(layout.manifest_path(job_id)),
        "completed_count": manifest.stats["succeeded_sources"],
        "failed_count": manifest.stats["failed_sources"],
        "artifact_paths": artifact_paths,
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
    *,
    failed_override: int | None = None,
) -> None:
    if callback is None:
        return
    callback(
        stage_name,
        {
            "succeeded": int(stats.get("succeeded_sources", 0)),
            "failed": failed_override if failed_override is not None else int(stats.get("failed_sources", 0)),
            "reused": int(stats.get("reused_sources", 0)),
        },
    )


def build_graph_stage_stats(
    *,
    graph_bundle: GraphBundle,
    source_ids: list[str],
    reused_sources: int,
) -> dict[str, int]:
    return {
        "source_count": len(source_ids),
        "succeeded_sources": len(source_ids),
        "failed_sources": 0,
        "reused_sources": reused_sources,
        "nodes": len(graph_bundle.nodes),
        "edges": len(graph_bundle.edges),
    }


def try_reuse_graph_stage(
    *,
    layout: BuildLayout,
    stage_name: str,
    source_ids: list[str],
) -> GraphBundle | None:
    graph_path = layout.stage_graph_path(stage_name)
    if not graph_path.exists():
        return None
    graph_bundle = read_graph_bundle(graph_path)
    existing_source_ids = sorted(str(value) for value in graph_bundle.metadata.get("source_ids", []))
    if existing_source_ids != sorted(source_ids):
        return None
    if str(graph_bundle.metadata.get("stage", "")) != stage_name:
        return None
    return graph_bundle


def discover_source_ids(data_root: Path, category: str | None = None) -> list[str]:
    metadata_root = data_root / "source" / "metadata"
    source_ids: list[str] = []
    for path in sorted(metadata_root.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            continue
        for item in payload:
            if not isinstance(item, dict):
                continue
            if category and str(item.get("category", "")) != category:
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


def previous_stage_name(stage_name: str) -> str:
    index = STAGE_SEQUENCE.index(stage_name)
    if index == 0:
        raise ValueError(f"Stage {stage_name} has no previous stage.")
    return STAGE_SEQUENCE[index - 1]
