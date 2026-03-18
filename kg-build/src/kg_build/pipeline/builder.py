from __future__ import annotations

from pathlib import Path

from ..common import build_id_from_source, repo_root, timestamp_utc
from ..config import snapshot_config
from ..contracts import BuildManifest, GraphBundle, StageRecord
from ..io import (
    read_graph_bundle,
    read_source_document_json,
    save_manifest,
    write_graph_bundle,
    write_json,
    write_jsonl,
    write_source_document_json,
)
from ..stages import (
    run_aggr,
    run_conv,
    run_dedup,
    run_embed,
    run_extract,
    run_ingest,
    run_pred,
    run_segment,
    run_serialize,
    run_summarize,
)

STAGE_SEQUENCE = (
    "ingest",
    "segment",
    "summarize",
    "extract",
    "aggr",
    "conv",
    "embed",
    "dedup",
    "pred",
    "serialize",
)

TODO_STAGE_RUNNERS = [
    ("03_summarize", "summarize", run_summarize),
    ("04_extract", "extract", run_extract),
    ("05_aggr", "aggr", run_aggr),
    ("06_conv", "conv", run_conv),
    ("07_embed", "embed", run_embed),
    ("08_dedup", "dedup", run_dedup),
    ("09_pred", "pred", run_pred),
]


def build_knowledge_graph(
    source_path: Path,
    data_root: Path,
    start_stage: str = "ingest",
    end_stage: str = "serialize",
) -> dict[str, object]:
    source_path = resolve_repo_path(source_path)
    data_root = resolve_repo_path(data_root)
    validate_stage_range(start_stage, end_stage)
    intermediate_root = data_root / "intermediate"
    graph_root = data_root / "graph"
    manifest_path = data_root / "manifest" / "build_manifest.json"

    manifest = BuildManifest(
        build_id=build_id_from_source(source_path),
        source_path=str(source_path.resolve()),
        status="running",
        started_at=timestamp_utc(),
        config_snapshot=snapshot_config(),
    )
    save_manifest(manifest_path, manifest)

    try:
        ingest_dir = intermediate_root / "01_ingest"
        ingest_artifact = ingest_dir / "source_document.json"
        if is_stage_before("ingest", start_stage):
            ensure_artifact_exists(ingest_artifact, "ingest")
            ingest_stage = build_reused_stage_record("ingest", ingest_dir, ingest_artifact)
        else:
            source_document = run_ingest(source_path)
            write_source_document_json(ingest_artifact, source_document)
            ingest_stage = StageRecord(
                name="ingest",
                status="completed",
                output_dir=str(ingest_dir.resolve()),
                artifact_path=str(ingest_artifact.resolve()),
            )
            write_stage_result(
                ingest_dir / "stage_result.json",
                ingest_stage,
            )
        manifest.stages.append(ingest_stage)
        manifest.artifact_paths["source_document"] = str(ingest_artifact.resolve())
        save_manifest(manifest_path, manifest)
        if end_stage == "ingest":
            return finalize_manifest(manifest, manifest_path, start_stage, end_stage)

        segment_dir = intermediate_root / "02_segment"
        segment_artifact = segment_dir / "graph.bundle.json"
        if is_stage_before("segment", start_stage):
            ensure_artifact_exists(segment_artifact, "segment")
            segment_stage = build_reused_stage_record("segment", segment_dir, segment_artifact)
        else:
            ingested_source = read_source_document_json(ingest_artifact)
            bundle = run_segment(ingested_source)
            segment_stage = StageRecord(
                name="segment",
                status="completed",
                output_dir=str(segment_dir.resolve()),
                artifact_path=str(segment_artifact.resolve()),
            )
            write_graph_bundle(segment_artifact, bundle)
            write_stage_result(segment_dir / "stage_result.json", segment_stage)
        manifest.stages.append(segment_stage)
        manifest.artifact_paths["segment_bundle"] = segment_stage.artifact_path
        save_manifest(manifest_path, manifest)
        if end_stage == "segment":
            return finalize_manifest(manifest, manifest_path, start_stage, end_stage)

        previous_bundle_path = segment_artifact
        for stage_dir_name, stage_name, runner in TODO_STAGE_RUNNERS:
            stage_dir = intermediate_root / stage_dir_name
            stage_artifact = stage_dir / "graph.bundle.json"
            if is_stage_before(stage_name, start_stage):
                ensure_artifact_exists(stage_artifact, stage_name)
                stage_record = build_reused_stage_record(stage_name, stage_dir, stage_artifact)
                extra_artifacts = existing_todo_stage_sidecars(stage_name, stage_dir)
            else:
                bundle = read_graph_bundle(previous_bundle_path)
                bundle, note = runner(bundle)
                extra_artifacts = write_todo_stage_sidecars(stage_name, stage_dir)
                stage_record = StageRecord(
                    name=stage_name,
                    status="todo",
                    output_dir=str(stage_dir.resolve()),
                    artifact_path=str(stage_artifact.resolve()),
                    notes=note,
                )
                write_graph_bundle(stage_artifact, bundle)
                write_stage_result(stage_dir / "stage_result.json", stage_record)
            manifest.stages.append(stage_record)
            manifest.artifact_paths[f"{stage_name}_bundle"] = stage_record.artifact_path
            for artifact_name, artifact_path in extra_artifacts.items():
                manifest.artifact_paths[f"{stage_name}_{artifact_name}"] = artifact_path
            save_manifest(manifest_path, manifest)
            previous_bundle_path = stage_artifact
            if end_stage == stage_name:
                return finalize_manifest(manifest, manifest_path, start_stage, end_stage)

        serialize_stage = StageRecord(
            name="serialize",
            status="completed",
            output_dir=str(graph_root.resolve()),
            artifact_path=str((graph_root / "graph.bundle.json").resolve()),
        )
        final_bundle = read_graph_bundle(previous_bundle_path)
        serialize_paths = run_serialize(final_bundle, graph_root)
        manifest.stages.append(serialize_stage)
        manifest.artifact_paths.update(serialize_paths)
        return finalize_manifest(manifest, manifest_path, start_stage, end_stage)
    except Exception as exc:
        manifest.status = "failed"
        manifest.finished_at = timestamp_utc()
        manifest.stages.append(
            StageRecord(
                name="build",
                status="failed",
                output_dir=str(data_root.resolve()),
                artifact_path="",
                error=str(exc),
            )
        )
        save_manifest(manifest_path, manifest)
        raise


def resolve_repo_path(path: Path) -> Path:
    return path if path.is_absolute() else (repo_root() / path).resolve()


def validate_stage_range(start_stage: str, end_stage: str) -> None:
    if start_stage not in STAGE_SEQUENCE:
        raise ValueError(f"Unsupported start stage: {start_stage}")
    if end_stage not in STAGE_SEQUENCE:
        raise ValueError(f"Unsupported end stage: {end_stage}")
    if STAGE_SEQUENCE.index(start_stage) > STAGE_SEQUENCE.index(end_stage):
        raise ValueError(f"start_stage {start_stage} must not be after end_stage {end_stage}")


def is_stage_before(left: str, right: str) -> bool:
    return STAGE_SEQUENCE.index(left) < STAGE_SEQUENCE.index(right)


def ensure_artifact_exists(path: Path, stage_name: str) -> None:
    if not path.exists():
        raise FileNotFoundError(
            f"Stage {stage_name} was requested to be reused, but artifact is missing: {path}"
        )


def write_stage_result(path: Path, stage: StageRecord) -> None:
    write_json(path, stage.to_dict())


def stage_record_payload(stage: StageRecord) -> dict[str, str]:
    return {
        "name": stage.name,
        "status": stage.status,
        "artifact_path": stage.artifact_path,
        "notes": stage.notes,
    }


def write_todo_stage_sidecars(stage_name: str, stage_dir: Path) -> dict[str, str]:
    if stage_name != "embed":
        return {}
    embeddings_path = stage_dir / "embeddings.jsonl"
    write_jsonl(embeddings_path, [])
    return {"embeddings_jsonl": str(embeddings_path.resolve())}


def existing_todo_stage_sidecars(stage_name: str, stage_dir: Path) -> dict[str, str]:
    if stage_name != "embed":
        return {}
    embeddings_path = stage_dir / "embeddings.jsonl"
    if not embeddings_path.exists():
        raise FileNotFoundError(
            f"Stage embed was requested to be reused, but sidecar artifact is missing: {embeddings_path}"
        )
    return {"embeddings_jsonl": str(embeddings_path.resolve())}


def build_reused_stage_record(stage_name: str, stage_dir: Path, artifact_path: Path) -> StageRecord:
    return StageRecord(
        name=stage_name,
        status="reused",
        output_dir=str(stage_dir.resolve()),
        artifact_path=str(artifact_path.resolve()),
        notes="Reused existing stage artifact.",
    )


def finalize_manifest(
    manifest: BuildManifest,
    manifest_path: Path,
    start_stage: str,
    end_stage: str,
) -> dict[str, object]:
    executed_stages = [stage for stage in manifest.stages if stage.status != "reused"]
    if end_stage != "serialize":
        manifest.status = "completed_partial"
    elif any(stage.status == "todo" for stage in executed_stages):
        manifest.status = "completed_with_todo"
    else:
        manifest.status = "completed"
    manifest.finished_at = timestamp_utc()
    save_manifest(manifest_path, manifest)
    return {
        "build_id": manifest.build_id,
        "status": manifest.status,
        "start_stage": start_stage,
        "end_stage": end_stage,
        "manifest_path": str(manifest_path.resolve()),
        "artifact_paths": manifest.artifact_paths,
    }
