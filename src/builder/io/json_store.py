from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..contracts import GraphBundle, JobManifest, NormalizeStageIndex, NormalizedDocumentRecord, SourceDocumentRecord


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


def write_graph_bundle(path: Path, bundle: GraphBundle) -> None:
    bundle.validate_edge_references()
    write_json(path, bundle.to_dict())


def read_graph_bundle(path: Path) -> GraphBundle:
    return GraphBundle.from_dict(read_json(path))


def write_manifest(path: Path, manifest: JobManifest) -> None:
    write_json(path, manifest.to_dict())


def read_manifest(path: Path) -> JobManifest:
    return JobManifest.from_dict(read_json(path))
