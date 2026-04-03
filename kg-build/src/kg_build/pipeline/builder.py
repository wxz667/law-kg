from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from ..config import snapshot_config
from ..contracts import BuildManifest, GraphBundle, SourceDocumentRecord, StageRecord
from ..io import (
    read_graph_bundle,
    read_json,
    read_source_document_json,
    save_manifest,
    write_graph_bundle,
    write_json,
    write_source_document_json,
)
from ..stages import run_ingest, run_link, run_segment
from ..utils.ids import build_id_from_source, repo_root, slugify, timestamp_utc
from ..utils.progress import ConsoleStageProgressReporter, StageProgressReporter

PROCESSING_STAGE_SEQUENCE = ("ingest", "segment", "link")
STAGE_SEQUENCE = PROCESSING_STAGE_SEQUENCE
GRAPH_AGGREGATE_STAGES = ("segment", "link")
MAX_GRAPH_CHUNK_NODES = 50000


@dataclass(frozen=True)
class StageSpec:
    name: str
    dir_name: str
    artifact_name: str
    artifact_key: str


STAGE_SPECS: tuple[StageSpec, ...] = (
    StageSpec("ingest", "01_ingest", "source_document.json", "source_document"),
    StageSpec("segment", "02_segment", "graph.bundle.json", "segment_bundle"),
    StageSpec("link", "03_link", "graph.bundle.json", "link_bundle"),
)
STAGE_SPECS_BY_NAME = {spec.name: spec for spec in STAGE_SPECS}


@dataclass(frozen=True)
class BuildPaths:
    scope: str
    source_path: Path
    data_root: Path
    intermediate_root: Path
    cache_root: Path
    manifest_path: Path
    ingest_artifact: Path
    segment_artifact: Path
    link_artifact: Path


def build_knowledge_graph(
    source_path: Path,
    data_root: Path,
    start_stage: str | None = None,
    through_stage: str = "link",
    force_rebuild: bool = False,
    report_progress: bool = False,
    stage_callback: Callable[[int, int], None] | None = None,
    stage_name_callback: Callable[[str], None] | None = None,
    finalizing_callback: Callable[[str], None] | None = None,
    refresh_aggregates: bool = True,
) -> dict[str, object]:
    source_path = resolve_repo_path(source_path)
    data_root = resolve_repo_path(data_root)
    validate_processing_stage(through_stage)

    build_paths = build_paths_for_source(source_path, data_root)
    current_build_id = build_id_from_source(source_path)
    effective_start_stage = (
        "ingest"
        if force_rebuild
        else (start_stage or detect_default_start_stage(build_paths, current_build_id, through_stage))
    )
    validate_stage_range(effective_start_stage, through_stage)

    manifest = BuildManifest(
        build_id=current_build_id,
        source_path=str(source_path.resolve()),
        status="running",
        started_at=timestamp_utc(),
        config_snapshot=snapshot_config(),
    )
    save_manifest(build_paths.manifest_path, manifest)
    reporter: StageProgressReporter | None = ConsoleStageProgressReporter() if report_progress else None

    try:
        total_stage_steps = PROCESSING_STAGE_SEQUENCE.index(through_stage) + 1
        completed_stage_steps = 0

        def mark_stage_complete() -> None:
            nonlocal completed_stage_steps
            completed_stage_steps += 1
            if stage_callback is not None:
                stage_callback(completed_stage_steps, total_stage_steps)

        latest_artifact: SourceDocumentRecord | GraphBundle | None = None
        latest_artifact_path: Path | None = None
        latest_stage = "ingest"

        if stage_name_callback is not None:
            stage_name_callback("ingest")
        source_document = run_ingest_stage(
            build_paths=build_paths,
            effective_start_stage=effective_start_stage,
            manifest=manifest,
            reporter=reporter,
        )
        latest_artifact = source_document
        latest_artifact_path = build_paths.ingest_artifact
        latest_stage = "ingest"
        mark_stage_complete()

        if stage_reached("segment", through_stage):
            if stage_name_callback is not None:
                stage_name_callback("segment")
            segment_bundle = run_segment_stage(
                build_paths=build_paths,
                effective_start_stage=effective_start_stage,
                manifest=manifest,
                reporter=reporter,
            )
            latest_artifact = segment_bundle
            latest_artifact_path = build_paths.segment_artifact
            latest_stage = "segment"
            mark_stage_complete()

        if stage_reached("link", through_stage):
            if stage_name_callback is not None:
                stage_name_callback("link")
            link_bundle = run_link_stage(
                build_paths=build_paths,
                effective_start_stage=effective_start_stage,
                manifest=manifest,
                reporter=reporter,
            )
            latest_artifact = link_bundle
            latest_artifact_path = build_paths.link_artifact
            latest_stage = "link"
            mark_stage_complete()

        if latest_artifact is None or latest_artifact_path is None:
            raise RuntimeError("No artifact available for serialization.")

        if finalizing_callback is not None:
            finalizing_callback("finalize")
        finalize_delivery_outputs(
            artifact=latest_artifact,
            artifact_stage=latest_stage,
            artifact_path=latest_artifact_path,
            build_paths=build_paths,
            manifest=manifest,
            reporter=reporter,
        )
        if refresh_aggregates:
            refresh_aggregated_outputs(data_root, through_stage)
        return finalize_manifest(
            manifest=manifest,
            manifest_path=build_paths.manifest_path,
            start_stage=effective_start_stage,
            through_stage=through_stage,
        )
    except Exception as exc:
        manifest.status = "failed"
        manifest.finished_at = timestamp_utc()
        if manifest.stages:
            last_stage = manifest.stages[-1]
            if not last_stage.error:
                last_stage.error = str(exc)
                last_stage.status = "failed"
        save_manifest(build_paths.manifest_path, manifest)
        raise


def build_batch_knowledge_graph(
    data_root: Path,
    pattern: str = "*.docx",
    category: str | None = None,
    start_stage: str | None = None,
    through_stage: str = "link",
    force_rebuild: bool = False,
    report_progress: bool = False,
    progress_callback: Callable[[str, int, int], None] | None = None,
    finalizing_callback: Callable[[str], None] | None = None,
) -> dict[str, object]:
    data_root = resolve_repo_path(data_root)
    sources = discover_source_files(data_root, pattern=pattern, category=category)
    stage_start = start_stage or "ingest"
    validate_stage_range(stage_start, through_stage)
    stages = PROCESSING_STAGE_SEQUENCE[
        PROCESSING_STAGE_SEQUENCE.index(stage_start) : PROCESSING_STAGE_SEQUENCE.index(through_stage) + 1
    ]
    results: dict[Path, dict[str, object]] = {}
    failures: list[dict[str, str]] = []
    active_sources = list(sources)

    for stage_name in stages:
        stage_total = len(active_sources)
        next_active_sources: list[Path] = []
        if progress_callback is not None:
            progress_callback(stage_name, 0, stage_total)
        for index, source_path in enumerate(active_sources, start=1):
            try:
                result = build_knowledge_graph(
                    source_path=source_path,
                    data_root=data_root,
                    start_stage=stage_name,
                    through_stage=stage_name,
                    force_rebuild=force_rebuild and stage_name == stages[0],
                    report_progress=report_progress,
                    refresh_aggregates=False,
                )
                results[source_path] = result
                next_active_sources.append(source_path)
            except Exception as exc:
                error_type = getattr(exc, "error_type", exc.__class__.__name__)
                failures.append(
                    {
                        "source_path": str(source_path.resolve()),
                        "stage": stage_name,
                        "error_type": str(error_type),
                        "error": str(exc),
                    }
                )
            finally:
                if progress_callback is not None:
                    progress_callback(stage_name, index, stage_total)
        active_sources = next_active_sources
        if finalizing_callback is not None:
            finalizing_callback("finalize")
        refresh_aggregated_outputs(data_root, stage_name)

    status = "completed" if not failures else ("failed" if not results else "partial")
    report_path = write_batch_report(
        data_root=data_root,
        category=category,
        pattern=pattern,
        start_stage=stage_start,
        through_stage=through_stage,
        completed_count=len(active_sources),
        failed_count=len(failures),
        failures=failures,
        status=status,
    )
    return {
        "status": status,
        "data_root": str(data_root.resolve()),
        "source_root": str((data_root / "raw").resolve()),
        "category": category or "",
        "pattern": pattern,
        "start_stage": stage_start,
        "through_stage": through_stage,
        "completed_count": len(active_sources),
        "failed_count": len(failures),
        "results": list(results.values()),
        "failures": failures,
        "report_path": str(report_path.resolve()),
    }


def discover_source_files(data_root: Path, pattern: str = "*.docx", category: str | None = None) -> list[Path]:
    data_root = resolve_repo_path(data_root)
    raw_root = data_root / "raw"
    source_root = raw_root / category if category else raw_root
    if not source_root.exists():
        return []
    return sorted(path for path in source_root.rglob(pattern) if path.is_file())


def build_paths_for_source(source_path: Path, data_root: Path) -> BuildPaths:
    scope = slugify(f"{source_path.parent.name}-{source_path.stem}")
    intermediate_root = data_root / "intermediate"
    cache_root = data_root / ".cache" / "kg-build"
    manifest_path = cache_root / "manifests" / f"{scope}.build_manifest.json"
    return BuildPaths(
        scope=scope,
        source_path=source_path,
        data_root=data_root,
        intermediate_root=intermediate_root,
        cache_root=cache_root,
        manifest_path=manifest_path,
        ingest_artifact=intermediate_root / "01_ingest" / f"{scope}.source_document.json",
        segment_artifact=cache_root / "02_segment" / scope / "graph.bundle.json",
        link_artifact=cache_root / "03_link" / scope / "graph.bundle.json",
    )


def run_ingest_stage(
    *,
    build_paths: BuildPaths,
    effective_start_stage: str,
    manifest: BuildManifest,
    reporter: StageProgressReporter | None,
) -> SourceDocumentRecord:
    output_dir = build_paths.ingest_artifact.parent
    artifact_path = build_paths.ingest_artifact
    if is_stage_before("ingest", effective_start_stage):
        ensure_artifact_exists(artifact_path, "ingest")
        source_document = read_source_document_json(artifact_path)
        stage = build_reused_stage_record("ingest", output_dir, artifact_path)
        if reporter is not None:
            reporter.stage_reused("ingest")
    else:
        if reporter is not None:
            reporter.stage_started("ingest")
        source_document = run_ingest(build_paths.source_path)
        write_source_document_json(artifact_path, source_document)
        stage = StageRecord(
            name="ingest",
            status="completed",
            output_dir=str(output_dir.resolve()),
            artifact_path=str(artifact_path.resolve()),
        )
        if reporter is not None:
            reporter.stage_completed("ingest")
    manifest.stages.append(stage)
    manifest.artifact_paths["source_document"] = str(artifact_path.resolve())
    save_manifest(build_paths.manifest_path, manifest)
    return source_document


def run_segment_stage(
    *,
    build_paths: BuildPaths,
    effective_start_stage: str,
    manifest: BuildManifest,
    reporter: StageProgressReporter | None,
) -> GraphBundle:
    output_dir = build_paths.segment_artifact.parent
    artifact_path = build_paths.segment_artifact
    if is_stage_before("segment", effective_start_stage):
        ensure_artifact_exists(artifact_path, "segment")
        bundle = read_graph_bundle(artifact_path)
        stage = build_reused_stage_record("segment", output_dir, artifact_path)
        if reporter is not None:
            reporter.stage_reused("segment")
    else:
        if reporter is not None:
            reporter.stage_started("segment")
        source_document = read_source_document_json(build_paths.ingest_artifact)
        bundle = run_segment(source_document)
        write_graph_bundle(artifact_path, bundle)
        stage = StageRecord(
            name="segment",
            status="completed",
            output_dir=str(output_dir.resolve()),
            artifact_path=str(artifact_path.resolve()),
        )
        if reporter is not None:
            reporter.stage_completed("segment")
    manifest.stages.append(stage)
    manifest.artifact_paths["segment_bundle"] = str(artifact_path.resolve())
    save_manifest(build_paths.manifest_path, manifest)
    return bundle


def run_link_stage(
    *,
    build_paths: BuildPaths,
    effective_start_stage: str,
    manifest: BuildManifest,
    reporter: StageProgressReporter | None,
) -> GraphBundle:
    output_dir = build_paths.link_artifact.parent
    artifact_path = build_paths.link_artifact
    if is_stage_before("link", effective_start_stage):
        ensure_artifact_exists(artifact_path, "link")
        bundle = read_graph_bundle(artifact_path)
        stage = build_reused_stage_record("link", output_dir, artifact_path)
        if reporter is not None:
            reporter.stage_reused("link")
    else:
        if reporter is not None:
            reporter.stage_started("link")
        bundle = (
            read_aggregate_subgraph_for_scope(build_paths.data_root, "segment", build_paths.scope)
            if effective_start_stage == "link"
            else read_graph_bundle(build_paths.segment_artifact)
        )
        linked_bundle, link_stats = run_link(bundle, output_dir)
        write_graph_bundle(artifact_path, linked_bundle)
        bundle = linked_bundle
        notes = f"samples={link_stats['sample_count']} predictions={link_stats['prediction_count']}"
        stage = StageRecord(
            name="link",
            status="completed",
            output_dir=str(output_dir.resolve()),
            artifact_path=str(artifact_path.resolve()),
            notes=notes,
        )
        manifest.artifact_paths["link_samples"] = link_stats["samples_path"]
        manifest.artifact_paths["link_predictions"] = link_stats["predictions_path"]
        if reporter is not None:
            reporter.stage_completed("link")
    manifest.stages.append(stage)
    manifest.artifact_paths["link_bundle"] = str(artifact_path.resolve())
    save_manifest(build_paths.manifest_path, manifest)
    return bundle


def finalize_delivery_outputs(
    *,
    artifact: SourceDocumentRecord | GraphBundle,
    artifact_stage: str,
    artifact_path: Path,
    build_paths: BuildPaths,
    manifest: BuildManifest,
    reporter: StageProgressReporter | None,
) -> None:
    if isinstance(artifact, GraphBundle):
        artifact.validate_edge_references()
        manifest.artifact_paths["delivery_stage"] = artifact_stage
        manifest.artifact_paths["delivery_source_bundle"] = str(artifact_path.resolve())
    else:
        manifest.artifact_paths["delivery_stage"] = artifact_stage
        manifest.artifact_paths["delivery_source_document"] = str(artifact_path.resolve())
    save_manifest(build_paths.manifest_path, manifest)


def validate_processing_stage(stage_name: str) -> None:
    if stage_name not in PROCESSING_STAGE_SEQUENCE:
        raise ValueError(f"Unsupported processing stage: {stage_name}")


def validate_stage_range(start_stage: str, through_stage: str) -> None:
    if start_stage not in PROCESSING_STAGE_SEQUENCE:
        raise ValueError(f"Unsupported start stage: {start_stage}")
    if through_stage not in PROCESSING_STAGE_SEQUENCE:
        raise ValueError(f"Unsupported through stage: {through_stage}")
    if PROCESSING_STAGE_SEQUENCE.index(start_stage) > PROCESSING_STAGE_SEQUENCE.index(through_stage):
        raise ValueError(f"Stage range is invalid: {start_stage} -> {through_stage}")


def detect_default_start_stage(build_paths: BuildPaths, current_build_id: str, through_stage: str) -> str:
    existing_build_id = read_existing_build_id(build_paths.manifest_path)
    if existing_build_id and existing_build_id != current_build_id:
        return "ingest"
    if stage_reached("link", through_stage) and build_paths.link_artifact.exists():
        return "link"
    if stage_reached("segment", through_stage) and build_paths.segment_artifact.exists():
        return "segment"
    if build_paths.ingest_artifact.exists():
        return "segment"
    return "ingest"


def read_existing_build_id(manifest_path: Path) -> str:
    if not manifest_path.exists():
        return ""
    import json

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    return str(payload.get("build_id", ""))


def build_reused_stage_record(stage_name: str, output_dir: Path, artifact_path: Path) -> StageRecord:
    return StageRecord(
        name=stage_name,
        status="reused",
        output_dir=str(output_dir.resolve()),
        artifact_path=str(artifact_path.resolve()),
    )


def ensure_artifact_exists(path: Path, stage_name: str) -> None:
    if not path.exists():
        raise FileNotFoundError(
            f"Cannot resume from stage '{stage_name}' because artifact does not exist: {path}"
        )


def is_stage_before(candidate: str, boundary: str) -> bool:
    return PROCESSING_STAGE_SEQUENCE.index(candidate) < PROCESSING_STAGE_SEQUENCE.index(boundary)


def stage_reached(candidate: str, through_stage: str) -> bool:
    return PROCESSING_STAGE_SEQUENCE.index(candidate) <= PROCESSING_STAGE_SEQUENCE.index(through_stage)


def finalize_manifest(
    *,
    manifest: BuildManifest,
    manifest_path: Path,
    start_stage: str,
    through_stage: str,
) -> dict[str, object]:
    manifest.status = "completed"
    manifest.finished_at = timestamp_utc()
    save_manifest(manifest_path, manifest)
    return {
        "status": manifest.status,
        "build_id": manifest.build_id,
        "start_stage": start_stage,
        "through_stage": through_stage,
        "artifact_paths": manifest.artifact_paths,
        "manifest_path": str(manifest_path.resolve()),
    }


def write_batch_report(
    *,
    data_root: Path,
    category: str | None,
    pattern: str,
    start_stage: str,
    through_stage: str,
    completed_count: int,
    failed_count: int,
    failures: list[dict[str, str]],
    status: str,
) -> Path:
    report_dir = data_root / "logs" / "kg-build"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"batch-{timestamp_utc().replace(':', '-').replace('.', '-')}.json"
    write_json(
        report_path,
        {
            "status": status,
            "category": category or "",
            "pattern": pattern,
            "start_stage": start_stage,
            "through_stage": through_stage,
            "completed_count": completed_count,
            "failed_count": failed_count,
            "failures": failures,
        },
    )
    return report_path


def refresh_aggregated_outputs(data_root: Path, through_stage: str) -> None:
    for stage_name in GRAPH_AGGREGATE_STAGES:
        if stage_reached(stage_name, through_stage):
            refresh_stage_aggregate(data_root, stage_name)
    if through_stage in GRAPH_AGGREGATE_STAGES:
        publish_stage_aggregate(data_root, through_stage)


def refresh_stage_aggregate(data_root: Path, stage_name: str) -> None:
    spec = STAGE_SPECS_BY_NAME[stage_name]
    stage_root = data_root / "intermediate" / spec.dir_name
    artifact_paths = sorted(cache_stage_root(data_root, stage_name).glob("*/graph.bundle.json"))
    write_graph_chunks(artifact_paths, stage_root)


def publish_stage_aggregate(data_root: Path, stage_name: str) -> None:
    artifact_paths = sorted(cache_stage_root(data_root, stage_name).glob("*/graph.bundle.json"))
    export_root = data_root / "exports" / "json"
    write_graph_chunks(artifact_paths, export_root)


def write_graph_chunks(artifact_paths: list[Path], output_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    for path in output_root.glob("graph.bundle-*.json"):
        path.unlink()
    for stale_name in ("graph.bundle.json", "serialize_result.json"):
        stale_path = output_root / stale_name
        if stale_path.exists():
            stale_path.unlink()
    index_path = output_root / "graph.index.json"
    if index_path.exists():
        index_path.unlink()

    chunk_nodes: list = []
    chunk_edges: list = []
    chunk_index = 1
    chunk_items: list[dict[str, str]] = []
    index_rows: list[dict[str, str]] = []

    def flush_chunk() -> None:
        nonlocal chunk_nodes, chunk_edges, chunk_index, chunk_items
        if not chunk_nodes and not chunk_edges:
            return
        file_name = f"graph.bundle-{chunk_index:04d}.json"
        bundle = GraphBundle(
            graph_id=f"graph:chunk:{chunk_index:04d}",
            nodes=chunk_nodes,
            edges=chunk_edges,
        )
        write_graph_bundle(output_root / file_name, bundle)
        for row in chunk_items:
            row["chunk_file"] = file_name
            index_rows.append(row)
        chunk_nodes = []
        chunk_edges = []
        chunk_items = []
        chunk_index += 1

    for artifact_path in artifact_paths:
        bundle = read_graph_bundle(artifact_path)
        if chunk_nodes and len(chunk_nodes) + len(bundle.nodes) > MAX_GRAPH_CHUNK_NODES:
            flush_chunk()
        chunk_nodes.extend(bundle.nodes)
        chunk_edges.extend(bundle.edges)
        document_node = next((node for node in bundle.nodes if node.type == "DocumentNode"), None)
        chunk_items.append(
            {
                "scope": artifact_path.parent.name,
                "document_node_id": "" if document_node is None else document_node.id,
            }
        )

    flush_chunk()
    write_json(index_path, {"documents": index_rows})


def read_aggregate_subgraph_for_scope(data_root: Path, stage_name: str, scope: str) -> GraphBundle:
    spec = STAGE_SPECS_BY_NAME[stage_name]
    stage_root = data_root / "intermediate" / spec.dir_name
    cache_root = cache_stage_root(data_root, stage_name)
    index_path = stage_root / "graph.index.json"
    if not index_path.exists():
        fallback_path = cache_root / scope / "graph.bundle.json"
        ensure_artifact_exists(fallback_path, stage_name)
        return read_graph_bundle(fallback_path)

    payload = read_json(index_path)
    row = next((item for item in payload.get("documents", []) if item.get("scope") == scope), None)
    if row is None:
        fallback_path = cache_root / scope / "graph.bundle.json"
        ensure_artifact_exists(fallback_path, stage_name)
        return read_graph_bundle(fallback_path)

    bundle = read_graph_bundle(stage_root / row["chunk_file"])
    document_node_id = row.get("document_node_id", "")
    if not document_node_id:
        return bundle

    child_map: dict[str, list[str]] = {}
    for edge in bundle.edges:
        if edge.type != "HAS_CHILD":
            continue
        child_map.setdefault(edge.source, []).append(edge.target)

    keep_ids = {document_node_id}
    stack = [document_node_id]
    while stack:
        current = stack.pop()
        for child_id in child_map.get(current, []):
            if child_id in keep_ids:
                continue
            keep_ids.add(child_id)
            stack.append(child_id)

    nodes = [node for node in bundle.nodes if node.id in keep_ids]
    edges = [edge for edge in bundle.edges if edge.source in keep_ids and edge.target in keep_ids]
    return GraphBundle(graph_id=f"graph:{scope}:{stage_name}", nodes=nodes, edges=edges)


def resolve_repo_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    return (repo_root() / path).resolve()


def cache_stage_root(data_root: Path, stage_name: str) -> Path:
    spec = STAGE_SPECS_BY_NAME[stage_name]
    return data_root / ".cache" / "kg-build" / spec.dir_name
