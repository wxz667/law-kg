from __future__ import annotations

from pathlib import Path

STAGE_OUTPUT_DIRS = {
    "normalize": "01_normalize",
    "structure": "02_structure",
    "reference_filter": "03_reference_filter",
    "relation_classify": "04_relation_classify",
    "entity_extraction": "05_entity_extraction",
    "entity_alignment": "06_entity_alignment",
    "implicit_reasoning": "07_implicit_reasoning",
}


class BuildLayout:
    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root
        self.intermediate_root = data_root / "intermediate" / "builder"
        self.exports_root = data_root / "exports" / "json"
        self.logs_root = data_root.parent / "logs" / "builder"
        self.state_root = data_root / "manifest" / "builder"

    def stage_dir(self, stage_name: str) -> Path:
        return self.intermediate_root / STAGE_OUTPUT_DIRS[stage_name]

    def stage_nodes_path(self, stage_name: str) -> Path:
        return self.stage_dir(stage_name) / "nodes.jsonl"

    def stage_edges_path(self, stage_name: str) -> Path:
        return self.stage_dir(stage_name) / "edges.jsonl"

    def stage_primary_artifact_path(self, stage_name: str) -> Path:
        if stage_name == "reference_filter":
            return self.reference_filter_candidates_path()
        if stage_name == "relation_classify":
            return self.relation_classify_plans_path()
        if self.stage_nodes_path(stage_name).exists():
            return self.stage_nodes_path(stage_name)
        return self.stage_edges_path(stage_name)

    def reference_filter_candidates_path(self) -> Path:
        return self.stage_dir("reference_filter") / "candidates.jsonl"

    def relation_classify_plans_path(self) -> Path:
        return self.stage_dir("relation_classify") / "results.jsonl"

    def relation_classify_llm_judge_path(self) -> Path:
        return self.stage_dir("relation_classify") / "llm_judgments.jsonl"

    def reference_filter_log_path(self) -> Path:
        return self.logs_root / "reference_filter" / "report.json"

    def relation_classify_log_path(self) -> Path:
        return self.logs_root / "relation_classify" / "report.json"

    def normalize_documents_dir(self) -> Path:
        return self.stage_dir("normalize") / "documents"

    def normalize_document_path(self, source_id: str) -> Path:
        return self.normalize_documents_dir() / f"{source_id}.json"

    def normalize_index_path(self) -> Path:
        return self.stage_dir("normalize") / "normalize_index.json"

    def normalize_log_path(self) -> Path:
        return self.logs_root / "normalize-report.json"

    def job_log_path(self, job_id: str) -> Path:
        return self.logs_root / f"{job_id}.json"

    def stage_manifest_path(self, stage_name: str) -> Path:
        return self.state_root / f"{stage_name}.json"

    def final_nodes_path(self) -> Path:
        return self.exports_root / "nodes.jsonl"

    def final_edges_path(self) -> Path:
        return self.exports_root / "edges.jsonl"


def ensure_stage_dirs(data_root: Path) -> None:
    intermediate_root = data_root / "intermediate" / "builder"
    for dir_name in STAGE_OUTPUT_DIRS.values():
        (intermediate_root / dir_name).mkdir(parents=True, exist_ok=True)
    (data_root / "exports" / "json").mkdir(parents=True, exist_ok=True)
    (data_root / "manifest" / "builder").mkdir(parents=True, exist_ok=True)
    logs_root = data_root.parent / "logs" / "builder"
    logs_root.mkdir(parents=True, exist_ok=True)
    (logs_root / "reference_filter").mkdir(parents=True, exist_ok=True)
    (logs_root / "relation_classify").mkdir(parents=True, exist_ok=True)
