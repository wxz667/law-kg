from __future__ import annotations

import json
import threading
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from ..contracts import (
    EquivalenceRecord,
    GraphBundle,
    JobLogRecord,
    StageRecord,
)
from ..io import (
    BuildLayout,
    ensure_stage_dirs,
    read_align_canonical_concepts,
    write_job_log,
    write_stage_edges,
    write_stage_nodes,
)
from ..utils.ids import project_root, timestamp_utc
from ..utils.locator import source_id_from_node_id
from .handlers.common import (
    StageContext,
    build_graph_type_stats,
    load_graph_snapshot,
    normalize_source_ids,
    run_stage,
    stage_outputs_exist,
)
from .handlers.graph import owner_document_by_node, owner_source_id_for_node
from .runtime import PipelineRuntime

STAGE_SEQUENCE = (
    "normalize",
    "structure",
    "detect",
    "classify",
    "extract",
    "aggregate",
    "align",
    "infer",
)

SUBSTAGE_PARENT_STAGES = {"classify", "extract", "align", "infer"}

GRAPH_STAGES = {"structure", "classify", "align", "infer"}
GRAPH_NODE_OUTPUT_STAGES = {"structure", "align", "infer"}
GRAPH_EDGE_OUTPUT_STAGES = {"structure", "classify", "align", "infer"}
GRAPH_INPUT_STAGE = {
    "structure": "",
    "detect": "structure",
    "classify": "structure",
    "extract": "classify",
    "aggregate": "classify",
    "align": "classify",
    "infer": "align",
}

DEFAULT_CONFIG_PATH = project_root() / "configs" / "config.json"


@dataclass(frozen=True)
class BuilderConfig:
    data: Path
    metadata: Path
    document: Path
    config_path: Path = DEFAULT_CONFIG_PATH


def load_builder_config(
    config_path: Path | None = None,
    *,
    data_override: Path | None = None,
) -> BuilderConfig:
    path = (config_path or DEFAULT_CONFIG_PATH).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    builder = payload.get("builder")
    if not isinstance(builder, dict):
        raise ValueError(f"{path.name} is missing top-level 'builder' configuration.")
    missing = [key for key in ("data", "metadata", "document") if not str(builder.get(key, "")).strip()]
    if missing:
        joined = ", ".join(f"builder.{key}" for key in missing)
        raise ValueError(f"{path.name} is missing required builder path configuration: {joined}.")
    return BuilderConfig(
        data=resolve_builder_config_path(data_override if data_override is not None else builder["data"]),
        metadata=resolve_builder_config_path(builder["metadata"]),
        document=resolve_builder_config_path(builder["document"]),
        config_path=path,
    )


def resolve_builder_config_path(value: Any) -> Path:
    path = Path(str(value).strip())
    if not str(path):
        raise ValueError("Builder path configuration cannot be empty.")
    return path if path.is_absolute() else (project_root() / path).resolve()


def build_job_id(prefix: str = "build") -> str:
    return f"{prefix}-{timestamp_utc().replace(':', '').replace('-', '')}"


def build_knowledge_graph(
    *,
    source_id: str | list[str] | None = None,
    data_root: Path | None = None,
    builder_config: BuilderConfig | None = None,
    category: str | list[str] | None = None,
    all_sources: bool = False,
    start_stage: str | None = None,
    through_stage: str = "infer",
    force_rebuild: bool = False,
    incremental: bool = False,
    report_progress: bool = False,
    discovery_callback: Callable[[int], None] | None = None,
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
    builder_config = builder_config or load_builder_config(data_override=data_root)
    data_root = builder_config.data
    if source_id is not None:
        source_ids = [source_id] if isinstance(source_id, str) else list(source_id)
        source_path_label = (
            str(source_ids[0]).strip()
            if len(source_ids) == 1
            else f"selected:{','.join(str(value).strip() for value in source_ids if str(value).strip())}"
        )
    elif category is not None or all_sources:
        source_ids = discover_source_ids(builder_config.metadata, category=category)
        source_path_label = (
            f"category:{','.join(category) if isinstance(category, list) else category}"
            if category is not None
            else "all"
        )
    else:
        raise ValueError("build_knowledge_graph requires source_id, category, or all_sources=True.")
    selected_source_ids = [str(value).strip() for value in source_ids if str(value).strip()]
    if not selected_source_ids:
        raise ValueError("build_knowledge_graph requires at least one discovered source_id.")
    if discovery_callback is not None:
        discovery_callback(len(selected_source_ids))
    job_id = build_job_id("build")
    return _build_from_source_ids(
        source_ids=selected_source_ids,
        builder_config=builder_config,
        start_stage=start_stage,
        through_stage=through_stage,
        force_rebuild=force_rebuild,
        job_id=job_id,
        source_path_label=source_path_label,
        stage_callback=stage_callback,
        stage_progress_callback=stage_progress_callback,
        stage_name_callback=stage_name_callback,
        stage_summary_callback=stage_summary_callback,
        finalizing_callback=finalizing_callback,
        stage_error_callback=stage_error_callback,
        cancel_event=cancel_event,
    )


def _build_from_source_ids(
    *,
    source_ids: list[str],
    builder_config: BuilderConfig,
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
    stage_error_callback: Callable[[str, str, str], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> dict[str, object]:
    data_root = builder_config.data
    ensure_stage_dirs(data_root)
    layout = BuildLayout(data_root, metadata_root=builder_config.metadata, document_root=builder_config.document)
    previous_final_graph = (
        load_graph_snapshot(
            layout,
            through_stage,
            stage_sequence=tuple(STAGE_SEQUENCE),
            graph_stages=set(GRAPH_STAGES),
        )
        if through_stage in GRAPH_STAGES
        and through_stage not in {"align", "infer"}
        and stage_outputs_exist(layout, through_stage)
        else None
    )
    previous_align_concepts = (
        read_align_canonical_concepts(layout.align_concepts_path())
        if through_stage == "infer" and layout.align_concepts_path().exists()
        else []
    )
    start = start_stage or STAGE_SEQUENCE[0]
    stage_names = iter_stage_range(start, through_stage)
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
    runtime = PipelineRuntime(builder_config)

    completed = 0
    total = len(stage_names)
    current_graph: GraphBundle | None = None

    for stage_name in stage_names:
        if cancel_event is not None and cancel_event.is_set():
            raise KeyboardInterrupt
        if stage_name_callback is not None:
            stage_name_callback(stage_name)
        stage_previous_graph = (
            load_graph_snapshot(
                layout,
                stage_name,
                stage_sequence=tuple(STAGE_SEQUENCE),
                graph_stages=set(GRAPH_STAGES),
            )
            if stage_name in GRAPH_STAGES
            and stage_name not in {"align", "infer"}
            and stage_outputs_exist(layout, stage_name)
            else None
        )
        stage_previous_align_concepts: list[EquivalenceRecord] = []
        stage_record = StageRecord(name=stage_name, status="running", started_at=timestamp_utc())
        log_record.stages.append(stage_record)
        write_job_log(layout.job_log_path(job_id), log_record)
        try:
            handler_result = run_stage(
                StageContext(
                    stage_name=stage_name,
                    data_root=data_root,
                    layout=layout,
                    runtime=runtime,
                    selected_source_ids=selected_source_ids,
                    force_rebuild=force_rebuild,
                    job_id=job_id,
                    source_path_label=source_path_label,
                    stage_record=stage_record,
                    log_record=log_record,
                    graph_stages=set(GRAPH_STAGES),
                    stage_sequence=tuple(STAGE_SEQUENCE),
                    graph_input_stage=dict(GRAPH_INPUT_STAGE),
                    stage_progress_callback=stage_progress_callback,
                    cancel_event=cancel_event,
                )
            )
            if handler_result is not None:
                current_graph = handler_result.current_graph
            else:
                raise ValueError(f"Unsupported stage: {stage_name}")

            if stage_name in GRAPH_STAGES:
                stage_current_align_concepts = (
                    read_align_canonical_concepts(layout.align_concepts_path())
                    if stage_name in {"align", "infer"} and layout.align_concepts_path().exists()
                    else []
                )
                if not handler_result.suppress_graph_update_stats:
                    stage_record.stats = dict(stage_record.stats) | build_graph_update_stats(
                        stage_previous_graph,
                        current_graph if current_graph is not None else GraphBundle(),
                        source_ids=selected_source_ids,
                        previous_align_concepts=stage_previous_align_concepts,
                        current_align_concepts=stage_current_align_concepts,
                    )
                stage_record.stats = with_graph_stats(stage_record.stats, current_graph)
            stage_record.status = resolve_terminal_stage_status(stage_record.stats)
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
    final_graph_loaded = False
    final_graph_stats: dict[str, object] = {}
    if through_stage in GRAPH_STAGES:
        if current_graph is not None:
            final_graph = current_graph
            final_graph_loaded = True
            if finalizing_callback is not None:
                finalizing_callback("finalize")
            write_stage_nodes(layout.final_nodes_path(), final_graph.nodes)
            write_stage_edges(layout.final_edges_path(), final_graph.edges)
            final_graph_stats = build_graph_type_stats(final_graph)
        elif layout.final_nodes_path().exists() and layout.final_edges_path().exists():
            final_graph_stats = graph_stats_from_stage_stats(log_record.stages[-1].stats if log_record.stages else {})
        else:
            final_graph = load_graph_snapshot(
                layout,
                through_stage,
                stage_sequence=tuple(STAGE_SEQUENCE),
                graph_stages=set(GRAPH_STAGES),
            )
            final_graph_loaded = True
            if finalizing_callback is not None:
                finalizing_callback("finalize")
            write_stage_nodes(layout.final_nodes_path(), final_graph.nodes)
            write_stage_edges(layout.final_edges_path(), final_graph.edges)
            final_graph_stats = build_graph_type_stats(final_graph)
    current_align_concepts = (
        read_align_canonical_concepts(layout.align_concepts_path())
        if through_stage == "infer" and layout.align_concepts_path().exists()
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
    graph_update_stats = (
        build_graph_update_stats(
            previous_final_graph,
            final_graph,
            source_ids=selected_source_ids,
            previous_align_concepts=previous_align_concepts,
            current_align_concepts=current_align_concepts,
        )
        if final_graph_loaded
        else {"updated_nodes": 0, "updated_edges": 0}
    )
    if through_stage == "infer" and log_record.stages:
        graph_update_stats = {
            "updated_nodes": int(log_record.stages[-1].stats.get("updated_nodes", 0)),
            "updated_edges": int(log_record.stages[-1].stats.get("updated_edges", 0)),
        }
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


def graph_stats_from_stage_stats(stats: dict[str, object]) -> dict[str, object]:
    keys = {"node_count", "edge_count", "node_type_counts", "edge_type_counts"}
    return {key: value for key, value in stats.items() if key in keys}


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


def with_graph_stats(stats: dict[str, object], graph_bundle: GraphBundle | None) -> dict[str, object]:
    merged = dict(stats)
    if graph_bundle is not None:
        merged.update(build_graph_type_stats(graph_bundle))
    return merged


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


def resolve_terminal_stage_status(stats: dict[str, object]) -> str:
    failed = int(stats.get("work_units_failed", stats.get("failed_sources", 0)))
    completed = int(stats.get("work_units_completed", stats.get("succeeded_sources", 0)))
    skipped = int(stats.get("work_units_skipped", stats.get("reused_sources", 0)))
    if failed == 0 and completed == 0 and skipped > 0:
        return "skipped"
    return "completed"


def discover_source_ids(metadata_root: Path, category: str | list[str] | None = None) -> list[str]:
    metadata_root = metadata_root.resolve()
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


def resolve_source_id(source_arg: str, metadata_root: Path) -> str:
    source_arg = source_arg.strip()
    if not source_arg:
        raise ValueError("source_id cannot be empty.")
    catalog = discover_source_ids(metadata_root)
    if source_arg in catalog:
        return source_arg
    raise ValueError(f"Unknown source_id: {source_arg}")


def iter_stage_range(start_stage: str, through_stage: str) -> tuple[str, ...]:
    start_index = STAGE_SEQUENCE.index(start_stage)
    end_index = STAGE_SEQUENCE.index(through_stage)
    return STAGE_SEQUENCE[start_index : end_index + 1]
