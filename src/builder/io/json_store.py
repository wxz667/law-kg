from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..contracts import (
    EdgeRecord,
    JobLogRecord,
    LlmJudgeDetailRecord,
    NodeRecord,
    NormalizeStageIndex,
    NormalizedDocumentRecord,
    ReferenceCandidateRecord,
    RelationClassifyRecord,
    SourceDocumentRecord,
    StageStateManifest,
)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(row, ensure_ascii=False) for row in rows]
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    temp_path.replace(path)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def write_source_document_json(path: Path, source_document: SourceDocumentRecord) -> None:
    write_json(path, source_document.to_dict())


def read_source_document_json(path: Path) -> SourceDocumentRecord:
    return SourceDocumentRecord.from_dict(read_json(path))


def write_normalized_document(path: Path, document: NormalizedDocumentRecord) -> None:
    write_json(path, document.to_dict())


def read_normalized_document(path: Path) -> NormalizedDocumentRecord:
    return NormalizedDocumentRecord.from_dict(read_json(path))


def write_normalize_index(path: Path, index: NormalizeStageIndex) -> None:
    write_json(path, index.to_dict())


def read_normalize_index(path: Path) -> NormalizeStageIndex:
    return NormalizeStageIndex.from_dict(read_json(path))


def write_stage_nodes(path: Path, nodes: list[NodeRecord]) -> None:
    write_jsonl(path, [node.to_dict() for node in nodes])


def read_stage_nodes(path: Path) -> list[NodeRecord]:
    return [NodeRecord.from_dict(row) for row in read_jsonl(path)]


def write_stage_edges(path: Path, edges: list[EdgeRecord]) -> None:
    write_jsonl(path, [edge.to_dict() for edge in edges])


def read_stage_edges(path: Path) -> list[EdgeRecord]:
    return [EdgeRecord.from_dict(row) for row in read_jsonl(path)]


def write_job_log(path: Path, manifest: JobLogRecord) -> None:
    write_json(path, manifest.to_dict())


def read_job_log(path: Path) -> JobLogRecord:
    return JobLogRecord.from_dict(read_json(path))


def write_stage_manifest(path: Path, manifest: StageStateManifest) -> None:
    write_json(path, manifest.to_dict())


def read_stage_manifest(path: Path) -> StageStateManifest:
    return StageStateManifest.from_dict(read_json(path))


def write_reference_candidates(path: Path, rows: list[ReferenceCandidateRecord]) -> None:
    write_jsonl(path, [row.to_dict() for row in rows])


def read_reference_candidates(path: Path) -> list[ReferenceCandidateRecord]:
    return [ReferenceCandidateRecord.from_dict(row) for row in read_jsonl(path)]


def write_relation_plans(path: Path, rows: list[RelationClassifyRecord]) -> None:
    write_jsonl(path, [row.to_dict() for row in rows])


def read_relation_plans(path: Path) -> list[RelationClassifyRecord]:
    return [RelationClassifyRecord.from_dict(row) for row in read_jsonl(path)]


def write_llm_judge_details(path: Path, rows: list[LlmJudgeDetailRecord]) -> None:
    write_jsonl(path, [row.to_dict() for row in rows])


def read_llm_judge_details(path: Path) -> list[LlmJudgeDetailRecord]:
    return [LlmJudgeDetailRecord.from_dict(row) for row in read_jsonl(path)]
