from __future__ import annotations

from pathlib import Path

STAGE_OUTPUT_DIRS = {
    "normalize": "01_normalize",
    "structure_graph": "02_structure_graph",
    "explicit_relations": "03_explicit_relations",
    "entity_extraction": "04_entity_extraction",
    "entity_alignment": "05_entity_alignment",
    "implicit_reasoning": "06_implicit_reasoning",
}


class BuildLayout:
    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root
        self.intermediate_root = data_root / "intermediate"
        self.exports_root = data_root / "exports" / "json"
        self.logs_root = data_root.parent / "logs" / "builder"
        self.manifests_root = self.logs_root

    def stage_dir(self, stage_name: str) -> Path:
        return self.intermediate_root / STAGE_OUTPUT_DIRS[stage_name]

    def stage_graph_path(self, stage_name: str, shard_index: int = 1) -> Path:
        return self.stage_dir(stage_name) / f"graph_bundle-{shard_index:04d}.json"

    def normalize_documents_dir(self) -> Path:
        return self.stage_dir("normalize") / "documents"

    def normalize_document_path(self, source_id: str) -> Path:
        return self.normalize_documents_dir() / f"{source_id}.json"

    def normalize_index_path(self) -> Path:
        return self.stage_dir("normalize") / "normalize_index.json"

    def normalize_log_path(self) -> Path:
        return self.logs_root / "normalize-report.json"

    def manifest_path(self, job_id: str) -> Path:
        return self.manifests_root / f"{job_id}.json"

    def final_graph_path(self, shard_index: int = 1) -> Path:
        return self.exports_root / f"graph_bundle-{shard_index:04d}.json"


def ensure_stage_dirs(data_root: Path) -> None:
    intermediate_root = data_root / "intermediate"
    for dir_name in STAGE_OUTPUT_DIRS.values():
        (intermediate_root / dir_name).mkdir(parents=True, exist_ok=True)
    (data_root / "exports" / "json").mkdir(parents=True, exist_ok=True)
    (data_root.parent / "logs" / "builder").mkdir(parents=True, exist_ok=True)
