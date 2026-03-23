from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from ..config import snapshot_config
from ..contracts import BuildManifest, GraphBundle, StageRecord
from ..io import (
    read_graph_bundle,
    read_source_document_json,
    save_manifest,
    write_graph_bundle,
    write_json,
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
from ..utils.ids import build_id_from_source, repo_root, timestamp_utc
from ..utils.progress import ConsoleStageProgressReporter, StageProgressReporter

BundleRunner = Callable[[GraphBundle], tuple[GraphBundle, str]]


@dataclass(frozen=True)
class StageSpec:
    name: str
    dir_name: str
    artifact_name: str
    artifact_key: str
    implemented: bool
    produces_bundle: bool = False
    runner: BundleRunner | None = None


STAGE_SEQUENCE = (
    "ingest",
    "segment",
    "summarize",
    "extract",
    "conv",
    "aggr",
    "embed",
    "dedup",
    "pred",
    "serialize",
)

STAGE_SPECS: tuple[StageSpec, ...] = (
    StageSpec(
        name="ingest",
        dir_name="01_ingest",
        artifact_name="source_document.json",
        artifact_key="source_document",
        implemented=True,
    ),
    StageSpec(
        name="segment",
        dir_name="02_segment",
        artifact_name="graph.bundle.json",
        artifact_key="segment_bundle",
        implemented=True,
        produces_bundle=True,
    ),
    StageSpec(
        name="summarize",
        dir_name="03_summarize",
        artifact_name="graph.bundle.json",
        artifact_key="summarize_bundle",
        implemented=True,
        produces_bundle=True,
        runner=run_summarize,
    ),
    StageSpec(
        name="extract",
        dir_name="04_extract",
        artifact_name="graph.bundle.json",
        artifact_key="extract_bundle",
        implemented=True,
        produces_bundle=True,
        runner=run_extract,
    ),
    StageSpec(
        name="conv",
        dir_name="05_conv",
        artifact_name="graph.bundle.json",
        artifact_key="conv_bundle",
        implemented=False,
        produces_bundle=True,
        runner=run_conv,
    ),
    StageSpec(
        name="aggr",
        dir_name="06_aggr",
        artifact_name="graph.bundle.json",
        artifact_key="aggr_bundle",
        implemented=False,
        produces_bundle=True,
        runner=run_aggr,
    ),
    StageSpec(
        name="embed",
        dir_name="07_embed",
        artifact_name="graph.bundle.json",
        artifact_key="embed_bundle",
        implemented=False,
        produces_bundle=True,
        runner=run_embed,
    ),
    StageSpec(
        name="dedup",
        dir_name="08_dedup",
        artifact_name="graph.bundle.json",
        artifact_key="dedup_bundle",
        implemented=False,
        produces_bundle=True,
        runner=run_dedup,
    ),
    StageSpec(
        name="pred",
        dir_name="09_pred",
        artifact_name="graph.bundle.json",
        artifact_key="pred_bundle",
        implemented=False,
        produces_bundle=True,
        runner=run_pred,
    ),
)

STAGE_SPECS_BY_NAME = {spec.name: spec for spec in STAGE_SPECS}
RESUMABLE_STAGE_NAMES = tuple(
    spec.name
    for spec in STAGE_SPECS
    if spec.name != "serialize" and spec.implemented
)


def build_knowledge_graph(
    source_path: Path,
    data_root: Path,
    start_stage: str | None = None,
    end_stage: str = "serialize",
    report_progress: bool = False,
) -> dict[str, object]:
    source_path = resolve_repo_path(source_path)
    data_root = resolve_repo_path(data_root)
    effective_start_stage = start_stage or detect_default_start_stage(data_root, end_stage)
    validate_stage_range(effective_start_stage, end_stage)

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
    reporter: StageProgressReporter | None = ConsoleStageProgressReporter() if report_progress else None

    try:
        current_bundle_path: Path | None = None
        current_bundle_stage = ""

        ingest_spec = STAGE_SPECS_BY_NAME["ingest"]
        ingest_dir = stage_output_dir(intermediate_root, ingest_spec)
        ingest_artifact = stage_artifact_path(intermediate_root, ingest_spec)
        if is_stage_before("ingest", effective_start_stage):
            ensure_artifact_exists(ingest_artifact, "ingest")
            ingest_stage = build_reused_stage_record("ingest", ingest_dir, ingest_artifact)
            if reporter is not None:
                reporter.stage_reused("ingest")
        else:
            if reporter is not None:
                reporter.stage_started("ingest")
            source_document = run_ingest(source_path)
            write_source_document_json(ingest_artifact, source_document)
            ingest_stage = StageRecord(
                name="ingest",
                status="completed",
                output_dir=str(ingest_dir.resolve()),
                artifact_path=str(ingest_artifact.resolve()),
            )
            write_stage_result(ingest_dir / "stage_result.json", ingest_stage)
            if reporter is not None:
                reporter.stage_completed("ingest")
        manifest.stages.append(ingest_stage)
        manifest.artifact_paths[ingest_spec.artifact_key] = str(ingest_artifact.resolve())
        save_manifest(manifest_path, manifest)
        if end_stage == "ingest":
            return finalize_manifest(manifest, manifest_path, effective_start_stage, end_stage)

        segment_spec = STAGE_SPECS_BY_NAME["segment"]
        segment_dir = stage_output_dir(intermediate_root, segment_spec)
        segment_artifact = stage_artifact_path(intermediate_root, segment_spec)
        if is_stage_before("segment", effective_start_stage):
            ensure_artifact_exists(segment_artifact, "segment")
            segment_stage = build_reused_stage_record("segment", segment_dir, segment_artifact)
            if reporter is not None:
                reporter.stage_reused("segment")
        else:
            if reporter is not None:
                reporter.stage_started("segment")
            ingested_source = read_source_document_json(ingest_artifact)
            bundle = run_segment(ingested_source)
            write_graph_bundle(segment_artifact, bundle)
            segment_stage = StageRecord(
                name="segment",
                status="completed",
                output_dir=str(segment_dir.resolve()),
                artifact_path=str(segment_artifact.resolve()),
            )
            write_stage_result(segment_dir / "stage_result.json", segment_stage)
            if reporter is not None:
                reporter.stage_completed("segment")
        manifest.stages.append(segment_stage)
        manifest.artifact_paths[segment_spec.artifact_key] = str(segment_artifact.resolve())
        save_manifest(manifest_path, manifest)
        current_bundle_path = segment_artifact
        current_bundle_stage = "segment"
        if end_stage == "segment":
            return finalize_manifest(manifest, manifest_path, effective_start_stage, end_stage)

        for spec in STAGE_SPECS[2:]:
            if is_stage_before(spec.name, effective_start_stage):
                if spec.implemented:
                    stage_dir = stage_output_dir(intermediate_root, spec)
                    stage_artifact = stage_artifact_path(intermediate_root, spec)
                    ensure_artifact_exists(stage_artifact, spec.name)
                    stage_record = build_reused_stage_record(spec.name, stage_dir, stage_artifact)
                    if reporter is not None:
                        reporter.stage_reused(spec.name)
                    manifest.stages.append(stage_record)
                    manifest.artifact_paths[spec.artifact_key] = str(stage_artifact.resolve())
                    save_manifest(manifest_path, manifest)
                    if spec.produces_bundle:
                        current_bundle_path = stage_artifact
                        current_bundle_stage = spec.name
                continue

            if not spec.implemented:
                skip_note = (
                    "Skipped: stage is not implemented in the current pipeline. "
                    f"Serialization will continue from the latest available bundle produced by "
                    f"stage '{current_bundle_stage}'."
                )
                if reporter is not None:
                    reporter.stage_skipped(spec.name, "not implemented")
                stage_record = StageRecord(
                    name=spec.name,
                    status="skipped",
                    output_dir="",
                    artifact_path="",
                    notes=skip_note,
                )
                manifest.stages.append(stage_record)
                save_manifest(manifest_path, manifest)
                if end_stage == spec.name:
                    return finalize_manifest(
                        manifest,
                        manifest_path,
                        effective_start_stage,
                        end_stage,
                    )
                continue

            if current_bundle_path is None:
                raise RuntimeError(f"Stage {spec.name} requires an upstream graph bundle.")

            stage_dir = stage_output_dir(intermediate_root, spec)
            stage_artifact = stage_artifact_path(intermediate_root, spec)
            bundle = read_graph_bundle(current_bundle_path)
            if reporter is not None:
                reporter.stage_started(spec.name)
            if spec.name == "summarize":
                bundle, note = run_summarize(
                    bundle,
                    show_progress=report_progress,
                    stage_dir=stage_dir,
                    reporter=reporter,
                )
            elif spec.name == "extract":
                bundle, note = run_extract(
                    bundle,
                    show_progress=report_progress,
                    stage_dir=stage_dir,
                    reporter=reporter,
                )
            else:
                bundle, note = spec.runner(bundle)
            write_graph_bundle(stage_artifact, bundle)
            stage_record = StageRecord(
                name=spec.name,
                status="completed",
                output_dir=str(stage_dir.resolve()),
                artifact_path=str(stage_artifact.resolve()),
                notes=note,
            )
            write_stage_result(stage_dir / "stage_result.json", stage_record)
            if reporter is not None:
                reporter.stage_completed(spec.name)
            manifest.stages.append(stage_record)
            manifest.artifact_paths[spec.artifact_key] = str(stage_artifact.resolve())
            save_manifest(manifest_path, manifest)
            if spec.produces_bundle:
                current_bundle_path = stage_artifact
                current_bundle_stage = spec.name
            if end_stage == spec.name:
                return finalize_manifest(manifest, manifest_path, effective_start_stage, end_stage)

        if current_bundle_path is None:
            raise RuntimeError("Serialize requires a graph bundle, but none is available.")

        final_bundle = read_graph_bundle(current_bundle_path)
        if reporter is not None:
            reporter.stage_started("serialize")
        serialize_paths = run_serialize(
            final_bundle,
            graph_root,
            source_stage=current_bundle_stage,
            source_bundle_path=current_bundle_path,
        )
        serialize_stage = StageRecord(
            name="serialize",
            status="completed",
            output_dir=str(graph_root.resolve()),
            artifact_path=serialize_paths["graph_bundle"],
            notes=(
                "Serialized the latest available graph bundle "
                f"from stage '{current_bundle_stage}'."
            ),
        )
        manifest.stages.append(serialize_stage)
        manifest.artifact_paths.update(serialize_paths)
        if reporter is not None:
            reporter.stage_completed("serialize", f"(from stage '{current_bundle_stage}')")
        return finalize_manifest(manifest, manifest_path, effective_start_stage, end_stage)
    except KeyboardInterrupt:
        manifest.status = "interrupted"
        manifest.finished_at = timestamp_utc()
        manifest.stages.append(
            StageRecord(
                name="build",
                status="interrupted",
                output_dir=str(data_root.resolve()),
                artifact_path="",
                error="Build interrupted by user.",
            )
        )
        save_manifest(manifest_path, manifest)
        raise
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


def stage_output_dir(intermediate_root: Path, spec: StageSpec) -> Path:
    return intermediate_root / spec.dir_name


def stage_artifact_path(intermediate_root: Path, spec: StageSpec) -> Path:
    return stage_output_dir(intermediate_root, spec) / spec.artifact_name


def detect_default_start_stage(data_root: Path, end_stage: str) -> str:
    latest_stage = latest_available_stage(data_root, end_stage)
    if latest_stage is None:
        return "ingest"
    next_stage = next_stage_after(latest_stage)
    if next_stage is None or STAGE_SEQUENCE.index(next_stage) > STAGE_SEQUENCE.index(end_stage):
        return latest_stage
    return next_stage


def latest_available_stage(data_root: Path, end_stage: str) -> str | None:
    intermediate_root = data_root / "intermediate"
    for stage_name in reversed(RESUMABLE_STAGE_NAMES):
        if STAGE_SEQUENCE.index(stage_name) > STAGE_SEQUENCE.index(end_stage):
            continue
        spec = STAGE_SPECS_BY_NAME[stage_name]
        if stage_artifact_path(intermediate_root, spec).exists():
            return stage_name
    return None


def next_stage_after(stage_name: str) -> str | None:
    index = STAGE_SEQUENCE.index(stage_name)
    if index + 1 >= len(STAGE_SEQUENCE):
        return None
    return STAGE_SEQUENCE[index + 1]


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
    if end_stage != "serialize":
        manifest.status = "completed_partial"
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
