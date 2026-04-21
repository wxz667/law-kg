from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .graph import GraphBundle

if TYPE_CHECKING:
    from ..io.paths import BuildLayout

STAGE_UNIT_KIND = {
    "normalize": "source",
    "structure": "source",
    "detect": "node",
    "classify": "candidate",
    "extract": "node",
    "aggregate": "node",
    "align": "concept",
    "infer": "concept",
}

SUBSTAGE_UNIT_KIND = {
    ("classify", "model"): "candidate",
    ("classify", "judge"): "candidate",
    ("extract", "input"): "source",
    ("extract", "extract"): "node",
    ("align", "embed"): "concept",
    ("align", "recall"): "concept",
    ("align", "judge"): "pair",
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
    "normalize": {"total_count", "type_counts"},
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
    "infer": {
        "pair_count",
        "judgment_count",
        "relation_count",
        "accepted_count",
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
    ("classify", "judge"): {
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


def infer_pass_substage_name(pass_index: int) -> str:
    return f"pass_{int(pass_index)}"


def infer_pass_index_from_substage(substage_name: str) -> int | None:
    marker = "pass_"
    if not substage_name.startswith(marker):
        return None
    suffix = substage_name[len(marker):]
    return int(suffix) if suffix.isdigit() and int(suffix) > 0 else None


@dataclass
class StageRecord:
    name: str
    status: str
    graph_path: str = ""
    artifact_paths: dict[str, str] = field(default_factory=dict)
    failures: list[dict[str, str]] = field(default_factory=list)
    error: str = ""
    started_at: str = ""
    finished_at: str = ""
    stats: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "name": self.name,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "stats": self.stats,
        }
        if self.graph_path:
            payload["graph_path"] = self.graph_path
        if self.artifact_paths:
            payload["artifact_paths"] = self.artifact_paths
        if self.failures:
            payload["failures"] = self.failures
        if self.error:
            payload["error"] = self.error
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "StageRecord":
        return cls(
            name=payload["name"],
            status=payload["status"],
            graph_path=payload.get("graph_path", ""),
            artifact_paths=dict(payload.get("artifact_paths", {})),
            failures=list(payload.get("failures", [])),
            error=payload.get("error", ""),
            started_at=payload.get("started_at", ""),
            finished_at=payload.get("finished_at", ""),
            stats=dict(payload.get("stats", {})),
        )


@dataclass
class JobLogRecord:
    job_id: str
    build_target: str
    data_root: str
    status: str
    started_at: str
    start_stage: str
    end_stage: str
    source_count: int
    finished_at: str = ""
    stages: list[StageRecord] = field(default_factory=list)
    final_artifact_paths: dict[str, str] = field(default_factory=dict)
    stats: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "build_target": self.build_target,
            "data_root": self.data_root,
            "status": self.status,
            "started_at": self.started_at,
            "start_stage": self.start_stage,
            "end_stage": self.end_stage,
            "source_count": self.source_count,
            "finished_at": self.finished_at,
            "stages": [stage.to_dict() for stage in self.stages],
            "final_artifact_paths": self.final_artifact_paths,
            "stats": self.stats,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "JobLogRecord":
        return cls(
            job_id=payload["job_id"],
            build_target=payload["build_target"],
            data_root=payload["data_root"],
            status=payload["status"],
            started_at=payload["started_at"],
            start_stage=payload["start_stage"],
            end_stage=payload["end_stage"],
            source_count=int(payload.get("source_count", 0)),
            finished_at=payload.get("finished_at", ""),
            stages=[StageRecord.from_dict(item) for item in payload.get("stages", [])],
            final_artifact_paths=dict(payload.get("final_artifact_paths", {})),
            stats=dict(payload.get("stats", {})),
        )


@dataclass
class SubstageStateManifest:
    inputs: list[str] = field(default_factory=list)
    artifacts: list[str] = field(default_factory=list)
    updated_at: str = ""
    unit: str = ""
    stats: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    processed_units: list[str] = field(default_factory=list)
    substages: dict[str, "SubstageStateManifest"] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "inputs": list(self.inputs),
            "artifacts": list(self.artifacts),
            "updated_at": self.updated_at,
            "unit": self.unit,
            "stats": dict(self.stats),
        }
        if self.metadata:
            payload["metadata"] = dict(self.metadata)
        if self.substages:
            payload["substages"] = {
                name: state.to_dict()
                for name, state in self.substages.items()
            }
        elif self.processed_units:
            payload["processed_units"] = list(self.processed_units)
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "SubstageStateManifest":
        substages = {
            str(name): SubstageStateManifest.from_dict(item)
            for name, item in payload.get("substages", {}).items()
        }
        return cls(
            inputs=[str(value) for value in payload.get("inputs", [])],
            artifacts=[str(value) for value in payload.get("artifacts", [])],
            updated_at=str(payload.get("updated_at", "") or ""),
            unit=str(payload.get("unit", "") or ""),
            stats=dict(payload.get("stats", {})),
            metadata=dict(payload.get("metadata", {})),
            processed_units=[] if substages else [str(value) for value in payload.get("processed_units", [])],
            substages=substages,
        )


@dataclass
class StageStateManifest:
    stage: str
    inputs: list[str] = field(default_factory=list)
    artifacts: list[str] = field(default_factory=list)
    updated_at: str = ""
    unit: str = ""
    stats: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    processed_units: list[str] = field(default_factory=list)
    substages: dict[str, SubstageStateManifest] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "stage": self.stage,
            "inputs": list(self.inputs),
            "artifacts": list(self.artifacts),
            "updated_at": self.updated_at,
            "unit": self.unit,
            "stats": dict(self.stats),
        }
        if self.metadata:
            payload["metadata"] = dict(self.metadata)
        if self.substages:
            payload["substages"] = {
                name: state.to_dict()
                for name, state in self.substages.items()
            }
        elif self.processed_units:
            payload["processed_units"] = list(self.processed_units)
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "StageStateManifest":
        substages = {
            str(name): SubstageStateManifest.from_dict(item)
            for name, item in payload.get("substages", {}).items()
        }
        return cls(
            stage=str(payload["stage"]),
            inputs=[str(value) for value in payload.get("inputs", [])],
            artifacts=[str(value) for value in payload.get("artifacts", [])],
            updated_at=str(payload.get("updated_at", "") or ""),
            unit=str(payload.get("unit", "") or ""),
            stats=dict(payload.get("stats", {})),
            metadata=dict(payload.get("metadata", {})),
            processed_units=[] if substages else [str(value) for value in payload.get("processed_units", [])],
            substages=substages,
        )


def stage_inputs(layout: BuildLayout, stage_name: str) -> list[str]:
    if stage_name == "normalize":
        if layout.metadata_root is None or layout.document_root is None:
            raise ValueError("Normalize stage inputs require configured metadata and document paths.")
        return [
            str(layout.metadata_root),
            str(layout.document_root),
        ]
    if stage_name == "structure":
        return [
            str(layout.normalize_documents_dir()),
            str(layout.normalize_index_path()),
        ]
    if stage_name == "detect":
        return [str(layout.stage_nodes_path("structure")), str(layout.stage_edges_path("structure"))]
    if stage_name == "classify":
        return [
            str(layout.detect_candidates_path()),
            str(layout.stage_nodes_path("structure")),
            str(layout.stage_edges_path("structure")),
        ]
    if stage_name == "extract":
        return [str(layout.stage_nodes_path("classify")), str(layout.stage_edges_path("classify"))]
    if stage_name == "aggregate":
        return [
            str(layout.stage_nodes_path("classify")),
            str(layout.stage_edges_path("classify")),
            str(layout.extract_inputs_path()),
            str(layout.extract_concepts_path()),
        ]
    if stage_name == "align":
        return [
            str(layout.stage_nodes_path("classify")),
            str(layout.stage_edges_path("classify")),
            str(layout.aggregate_concepts_path()),
        ]
    if stage_name == "infer":
        return [
            str(layout.align_concepts_path()),
            str(layout.align_vectors_path()),
            str(layout.align_relations_path()),
            str(layout.stage_nodes_path("align")),
            str(layout.stage_edges_path("align")),
        ]
    raise ValueError(f"Unsupported stage: {stage_name}")


def stage_artifacts(layout: BuildLayout, stage_name: str) -> list[str]:
    if stage_name == "normalize":
        return [str(layout.normalize_documents_dir()), str(layout.normalize_index_path())]
    if stage_name == "structure":
        return [str(layout.stage_nodes_path("structure")), str(layout.stage_edges_path("structure"))]
    if stage_name == "detect":
        return [str(layout.detect_candidates_path())]
    if stage_name == "classify":
        return [
            str(layout.classify_results_path()),
            str(layout.classify_pending_path()),
            str(layout.classify_llm_judge_path()),
            str(layout.stage_edges_path("classify")),
        ]
    if stage_name == "extract":
        return [str(layout.extract_inputs_path()), str(layout.extract_concepts_path())]
    if stage_name == "aggregate":
        return [str(layout.aggregate_concepts_path())]
    if stage_name == "align":
        return [
            str(layout.aggregate_concepts_path()),
            str(layout.align_vectors_path()),
            str(layout.align_pairs_path()),
            str(layout.align_concepts_path()),
            str(layout.align_relations_path()),
            str(layout.stage_nodes_path("align")),
            str(layout.stage_edges_path("align")),
        ]
    if stage_name == "infer":
        paths = [
            str(layout.infer_relations_path()),
            str(layout.stage_nodes_path("infer")),
            str(layout.stage_edges_path("infer")),
        ]
        paths.extend(str(path) for path in layout.infer_pair_paths())
        return paths
    raise ValueError(f"Unsupported stage: {stage_name}")


def substage_inputs(layout: BuildLayout, parent_stage: str, substage_name: str) -> list[str]:
    if (parent_stage, substage_name) == ("classify", "model"):
        return [
            str(layout.detect_candidates_path()),
            str(layout.stage_nodes_path("structure")),
            str(layout.stage_edges_path("structure")),
        ]
    if (parent_stage, substage_name) == ("classify", "judge"):
        return [str(layout.classify_pending_path())]
    if (parent_stage, substage_name) == ("extract", "input"):
        return [str(layout.stage_nodes_path("classify")), str(layout.stage_edges_path("classify"))]
    if (parent_stage, substage_name) == ("extract", "extract"):
        return [str(layout.extract_inputs_path())]
    if (parent_stage, substage_name) == ("align", "embed"):
        return [str(layout.aggregate_concepts_path())]
    if (parent_stage, substage_name) == ("align", "recall"):
        return [
            str(layout.aggregate_concepts_path()),
            str(layout.align_vectors_path()),
            str(layout.align_concepts_path()),
        ]
    if (parent_stage, substage_name) == ("align", "judge"):
        return [
            str(layout.aggregate_concepts_path()),
            str(layout.align_pairs_path()),
            str(layout.align_concepts_path()),
        ]
    if parent_stage == "infer":
        pass_index = infer_pass_index_from_substage(substage_name)
        if pass_index is not None:
            return [
                str(layout.align_concepts_path()),
                str(layout.align_vectors_path()),
                str(layout.align_relations_path()),
                str(layout.stage_nodes_path("align")),
                str(layout.stage_edges_path("align")),
            ]
    raise ValueError(f"Unsupported substage: {parent_stage}::{substage_name}")


def substage_artifacts(layout: BuildLayout, parent_stage: str, substage_name: str) -> list[str]:
    if (parent_stage, substage_name) == ("classify", "model"):
        return [str(layout.classify_results_path()), str(layout.classify_pending_path())]
    if (parent_stage, substage_name) == ("classify", "judge"):
        return [
            str(layout.classify_results_path()),
            str(layout.classify_llm_judge_path()),
            str(layout.stage_edges_path("classify")),
        ]
    if (parent_stage, substage_name) == ("extract", "input"):
        return [str(layout.extract_inputs_path())]
    if (parent_stage, substage_name) == ("extract", "extract"):
        return [str(layout.extract_concepts_path())]
    if (parent_stage, substage_name) == ("align", "embed"):
        return [str(layout.align_vectors_path())]
    if (parent_stage, substage_name) == ("align", "recall"):
        return [str(layout.align_pairs_path())]
    if (parent_stage, substage_name) == ("align", "judge"):
        return [str(layout.align_pairs_path())]
    if parent_stage == "infer":
        pass_index = infer_pass_index_from_substage(substage_name)
        if pass_index is not None:
            return [
                str(layout.infer_pairs_path(pass_index)),
            ]
    raise ValueError(f"Unsupported substage: {parent_stage}::{substage_name}")


def stage_unit(stage_name: str) -> str:
    return STAGE_UNIT_KIND[stage_name]


def substage_unit(parent_stage: str, substage_name: str) -> str:
    if parent_stage == "infer":
        if infer_pass_index_from_substage(substage_name) is not None:
            return "pass"
    return SUBSTAGE_UNIT_KIND[(parent_stage, substage_name)]


def graph_type_stats(graph_bundle: GraphBundle) -> dict[str, object]:
    node_type_counts: dict[str, int] = {}
    edge_type_counts: dict[str, int] = {}
    for node in graph_bundle.nodes:
        node_type_counts[node.type] = int(node_type_counts.get(node.type, 0)) + 1
    for edge in graph_bundle.edges:
        edge_type_counts[edge.type] = int(edge_type_counts.get(edge.type, 0)) + 1
    return {
        "node_count": len(graph_bundle.nodes),
        "edge_count": len(graph_bundle.edges),
        "node_type_counts": dict(sorted(node_type_counts.items())),
        "edge_type_counts": dict(sorted(edge_type_counts.items())),
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
    if stage_name == "infer" and substage_name is not None:
        if infer_pass_index_from_substage(substage_name) is not None:
            allowed = {
                "pair_count",
                "judgment_count",
                "result_count",
                "accepted_count",
            }
    return {
        key: value
        for key, value in cleaned.items()
        if key in allowed
    }
