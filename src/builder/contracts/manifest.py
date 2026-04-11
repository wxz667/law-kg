from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


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
class StageStateManifest:
    stage_name: str
    build_target: str
    data_root: str
    job_id: str = ""
    status: str = ""
    source_ids: list[str] = field(default_factory=list)
    processed_source_ids: list[str] = field(default_factory=list)
    artifact_paths: dict[str, str] = field(default_factory=dict)
    input_node_stage: str = ""
    input_edge_stage: str = ""
    updated_at: str = ""
    stats: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage_name": self.stage_name,
            "build_target": self.build_target,
            "data_root": self.data_root,
            "job_id": self.job_id,
            "status": self.status,
            "source_ids": list(self.source_ids),
            "processed_source_ids": list(self.processed_source_ids),
            "artifact_paths": dict(self.artifact_paths),
            "input_node_stage": self.input_node_stage,
            "input_edge_stage": self.input_edge_stage,
            "updated_at": self.updated_at,
            "stats": dict(self.stats),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "StageStateManifest":
        return cls(
            stage_name=payload["stage_name"],
            build_target=payload.get("build_target", ""),
            data_root=payload.get("data_root", ""),
            job_id=payload.get("job_id", ""),
            status=payload.get("status", ""),
            source_ids=[str(value) for value in payload.get("source_ids", [])],
            processed_source_ids=[str(value) for value in payload.get("processed_source_ids", [])],
            artifact_paths=dict(payload.get("artifact_paths", {})),
            input_node_stage=payload.get("input_node_stage", ""),
            input_edge_stage=payload.get("input_edge_stage", ""),
            updated_at=payload.get("updated_at", ""),
            stats=dict(payload.get("stats", {})),
        )
