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
class SubstageStateManifest:
    inputs: list[str] = field(default_factory=list)
    artifacts: list[str] = field(default_factory=list)
    updated_at: str = ""
    unit: str = ""
    stats: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    processed_units: list[str] = field(default_factory=list)

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
        if self.processed_units:
            payload["processed_units"] = list(self.processed_units)
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "SubstageStateManifest":
        return cls(
            inputs=[str(value) for value in payload.get("inputs", [])],
            artifacts=[str(value) for value in payload.get("artifacts", [])],
            updated_at=str(payload.get("updated_at", "") or ""),
            unit=str(payload.get("unit", "") or ""),
            stats=dict(payload.get("stats", {})),
            metadata=dict(payload.get("metadata", {})),
            processed_units=[str(value) for value in payload.get("processed_units", [])],
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
        return cls(
            stage=str(payload["stage"]),
            inputs=[str(value) for value in payload.get("inputs", [])],
            artifacts=[str(value) for value in payload.get("artifacts", [])],
            updated_at=str(payload.get("updated_at", "") or ""),
            unit=str(payload.get("unit", "") or ""),
            stats=dict(payload.get("stats", {})),
            metadata=dict(payload.get("metadata", {})),
            processed_units=[str(value) for value in payload.get("processed_units", [])],
            substages={
                str(name): SubstageStateManifest.from_dict(item)
                for name, item in payload.get("substages", {}).items()
            },
        )
