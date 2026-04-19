from __future__ import annotations

from pathlib import Path

STAGE_OUTPUT_DIRS = {
    "normalize": "01_normalize",
    "structure": "02_structure",
    "detect": "03_detect",
    "classify": "04_classify",
    "extract": "05_extract",
    "aggregate": "06_aggregate",
    "align": "07_align",
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
        if stage_name == "detect":
            return self.detect_candidates_path()
        if stage_name == "classify":
            return self.classify_results_path()
        if stage_name == "extract":
            return self.extract_concepts_path()
        if stage_name == "aggregate":
            return self.aggregate_concepts_path()
        if stage_name == "align":
            return self.stage_nodes_path("align")
        if self.stage_nodes_path(stage_name).exists():
            return self.stage_nodes_path(stage_name)
        return self.stage_edges_path(stage_name)

    def detect_candidates_path(self) -> Path:
        return self.stage_dir("detect") / "candidates.jsonl"

    def classify_results_path(self) -> Path:
        return self.stage_dir("classify") / "results.jsonl"

    def classify_pending_path(self) -> Path:
        return self.stage_dir("classify") / "pending.jsonl"

    def classify_llm_judge_path(self) -> Path:
        return self.stage_dir("classify") / "llm_judgments.jsonl"

    def extract_inputs_path(self) -> Path:
        return self.stage_dir("extract") / "inputs.jsonl"

    def extract_concepts_path(self) -> Path:
        return self.stage_dir("extract") / "concepts.jsonl"

    def aggregate_concepts_path(self) -> Path:
        return self.stage_dir("aggregate") / "concepts.jsonl"

    def align_concepts_path(self) -> Path:
        return self.stage_dir("align") / "concepts.jsonl"

    def align_vectors_path(self) -> Path:
        return self.stage_dir("align") / "vectors.jsonl"

    def align_pairs_path(self) -> Path:
        return self.stage_dir("align") / "pairs.jsonl"

    def align_relations_path(self) -> Path:
        return self.stage_dir("align") / "relations.jsonl"

    def normalize_documents_dir(self) -> Path:
        return self.stage_dir("normalize") / "documents"

    def normalize_document_path(self, source_id: str) -> Path:
        return self.normalize_documents_dir() / f"{source_id}.json"

    def normalize_index_path(self) -> Path:
        return self.stage_dir("normalize") / "normalize_index.json"

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
