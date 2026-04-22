from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class StageRecord:
    name: str
    status: str
    output_dir: str
    artifact_path: str
    notes: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BuildManifest:
    build_id: str
    source_path: str
    status: str
    started_at: str
    finished_at: str = ""
    stages: list[StageRecord] = field(default_factory=list)
    config_snapshot: dict[str, Any] = field(default_factory=dict)
    artifact_paths: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "build_id": self.build_id,
            "source_path": self.source_path,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "stages": [stage.to_dict() for stage in self.stages],
            "config_snapshot": self.config_snapshot,
            "artifact_paths": self.artifact_paths,
        }
