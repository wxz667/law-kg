from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from ...contracts import (
    GraphBundle,
    JobLogRecord,
    StageRecord,
    StageStateManifest,
    SubstageStateManifest,
    graph_type_stats,
    sanitize_manifest_stats,
    stage_artifacts,
    stage_inputs,
    stage_unit,
    substage_artifacts,
    substage_inputs,
    substage_unit,
)
from ...io import (
    BuildLayout,
    read_normalize_index,
    read_stage_edges_unchecked,
    read_stage_manifest,
    read_stage_nodes_unchecked,
    write_stage_edges,
    write_stage_manifest,
    write_stage_nodes,
)
from ...utils.ids import timestamp_utc
from ...utils.locator import source_id_from_node_id
from ..runtime import PipelineRuntime


@dataclass
class StageScope:
    unit_ids: list[str] = field(default_factory=list)
    source_ids: list[str] = field(default_factory=list)
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class StageRunResult:
    processed_unit_ids: list[str] = field(default_factory=list)
    skipped_unit_ids: list[str] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)
    artifact_paths: dict[str, str] = field(default_factory=dict)
    substage_states: dict[str, SubstageStateManifest] = field(default_factory=dict)
    graph_bundle: GraphBundle | None = None
    failures: list[dict[str, object]] = field(default_factory=list)


@dataclass
class StageContext:
    stage_name: str
    data_root: Path
    layout: BuildLayout
    runtime: PipelineRuntime
    selected_source_ids: list[str]
    force_rebuild: bool
    job_id: str
    source_path_label: str
    stage_record: StageRecord
    log_record: JobLogRecord
    graph_stages: set[str]
    stage_sequence: tuple[str, ...]
    graph_input_stage: dict[str, str]
    stage_progress_callback: Callable[[str, int, int], None] | None = None
    cancel_event: Any = None


@dataclass
class HandlerResult:
    current_graph: GraphBundle | None = None
    suppress_graph_update_stats: bool = False


def get_handler(stage_name: str):
    if stage_name == "normalize":
        from . import normalize

        return normalize
    if stage_name == "structure":
        from . import structure

        return structure
    if stage_name == "detect":
        from . import detect

        return detect
    if stage_name == "classify":
        from . import classify

        return classify
    if stage_name == "extract":
        from . import extract

        return extract
    if stage_name == "aggregate":
        from . import aggregate

        return aggregate
    if stage_name == "align":
        from . import align

        return align
    if stage_name == "infer":
        from . import infer

        return infer
    return None


def run_stage(ctx: StageContext) -> HandlerResult | None:
    handler = get_handler(ctx.stage_name)
    if handler is None:
        return None
    return handler.run(ctx)


def emit_stage_progress(
    callback: Callable[[str, int, int], None] | None,
    stage_name: str,
    current: int,
    total: int,
) -> None:
    if callback is not None:
        callback(stage_name, current, total)


def emit_prefilled_stage_progress(
    callback: Callable[[str, int, int], None] | None,
    stage_name: str,
    *,
    current: int,
    total: int,
) -> None:
    if callback is None:
        return
    if total <= 0:
        callback(stage_name, 1, 1)
        return
    callback(stage_name, current, total)


def offset_stage_progress_callback(
    callback: Callable[[str, int, int], None] | None,
    stage_name: str,
    *,
    skipped_units: int,
    total_units: int,
):
    if callback is None:
        return None

    def report(current: int, _total: int) -> None:
        emit_prefilled_stage_progress(
            callback,
            stage_name,
            current=int(skipped_units) + int(current),
            total=int(total_units),
        )

    return report


def dynamic_stage_progress_callback(
    callback: Callable[[str, int, int], None] | None,
    stage_name: str,
    *,
    skipped_units: int = 0,
):
    if callback is None:
        return None

    def report(current: int, total: int) -> None:
        emit_prefilled_stage_progress(
            callback,
            stage_name,
            current=int(skipped_units) + int(current),
            total=max(int(skipped_units) + int(total), 1),
        )

    return report


def normalize_unit_ids(unit_ids: list[str]) -> list[str]:
    return sorted(dict.fromkeys(str(value).strip() for value in unit_ids if str(value).strip()))


def normalize_source_ids(source_ids: list[str]) -> list[str]:
    normalized: list[str] = []
    for source_id in source_ids:
        value = str(source_id).strip()
        if not value:
            continue
        normalized.append(source_id_from_node_id(value) if value.startswith("document:") else value)
    return sorted(dict.fromkeys(normalized))


def subtract_source_ids(source_ids: list[str], skipped_source_ids: list[str]) -> list[str]:
    skipped = set(skipped_source_ids)
    return [source_id for source_id in source_ids if source_id not in skipped]


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


def build_graph_type_stats(graph_bundle: GraphBundle) -> dict[str, object]:
    return graph_type_stats(graph_bundle)


def build_unit_substage_manifest(
    *,
    layout: BuildLayout,
    parent_stage: str,
    stage_name: str,
    processed_units: list[str],
    stats: dict[str, object],
    metadata: dict[str, object] | None = None,
) -> SubstageStateManifest:
    return SubstageStateManifest(
        inputs=substage_inputs(layout, parent_stage, stage_name),
        artifacts=substage_artifacts(layout, parent_stage, stage_name),
        updated_at=timestamp_utc(),
        unit=substage_unit(parent_stage, stage_name),
        stats=sanitize_manifest_stats(dict(stats), stage_name=parent_stage, substage_name=stage_name),
        metadata=dict(metadata or {}),
        processed_units=normalize_unit_ids(processed_units),
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
        substages=dict(current.substages or previous.substages),
    )


def build_stage_manifest(
    *,
    stage_name: str,
    layout: BuildLayout,
    job_id: str = "",
    build_target: str = "",
    source_ids: list[str] | None = None,
    processed_source_ids: list[str] | None = None,
    unit_ids: list[str] | None = None,
    processed_unit_ids: list[str] | None = None,
    input_stage: str = "",
    artifact_paths: dict[str, str] | None = None,
    stats: dict[str, object] | None = None,
    metadata: dict[str, object] | None = None,
    graph_bundle: GraphBundle | None = None,
    status: str = "completed",
    substage_states: dict[str, SubstageStateManifest] | None = None,
) -> StageStateManifest:
    del job_id, build_target, source_ids, unit_ids, input_stage, status
    has_substages = stage_name in {"classify", "extract", "align", "infer"}
    graph_stats = build_graph_type_stats(graph_bundle) if graph_bundle is not None else {}
    processed_source_ids = list(processed_source_ids or [])
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
                substages=dict(state.substages),
            )
            for name, state in previous.substages.items()
        }
        merged_stats = (
            sanitize_manifest_stats({**dict(stats or {}), **graph_stats}, stage_name=stage_name)
            if stats
            else sanitize_manifest_stats({**dict(previous.stats), **graph_stats}, stage_name=stage_name)
        )
        merged_metadata = dict(metadata) if metadata else dict(previous.metadata)
    else:
        merged_processed_unit_ids = [] if has_substages else current_processed_unit_ids
        merged_stats = sanitize_manifest_stats({**dict(stats or {}), **graph_stats}, stage_name=stage_name)
        merged_metadata = dict(metadata or {})
    for name, state in (substage_states or {}).items():
        merged_substages[name] = merge_unit_substage_manifest(
            parent_stage=stage_name,
            stage_name=name,
            previous=merged_substages.get(name),
            current=state,
        )
    allowed_substages = {
        "classify": {"model", "judge"},
        "extract": {"input", "extract"},
        "align": {"embed", "recall", "judge"},
    }
    if stage_name in allowed_substages:
        merged_substages = {
            name: state
            for name, state in merged_substages.items()
            if name in allowed_substages[stage_name]
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


def load_graph_snapshot(
    layout: BuildLayout,
    stage_name: str,
    *,
    stage_sequence: tuple[str, ...],
    graph_stages: set[str],
) -> GraphBundle:
    if not stage_name:
        return GraphBundle()
    stage_index = stage_sequence.index(stage_name)
    nodes: list = []
    edges: list = []
    for index in range(stage_index, -1, -1):
        candidate = stage_sequence[index]
        if candidate in graph_stages and not nodes and layout.stage_nodes_path(candidate).exists():
            nodes = read_stage_nodes_unchecked(layout.stage_nodes_path(candidate))
        if candidate in graph_stages and not edges and layout.stage_edges_path(candidate).exists():
            edges = read_stage_edges_unchecked(layout.stage_edges_path(candidate))
        if nodes and edges:
            break
    return GraphBundle(nodes=nodes, edges=edges)


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
    processed_unit_ids = set(normalize_unit_ids(substage.processed_units))
    reusable_unit_ids = [
        unit_id
        for unit_id in normalize_unit_ids(unit_ids)
        if unit_id in processed_unit_ids
    ]
    if validator is not None and reusable_unit_ids and not validator(reusable_unit_ids, substage):
        return []
    return reusable_unit_ids


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


def completed_source_ids_for_input_rows(inputs: list[object], completed_input_ids: list[str]) -> list[str]:
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
