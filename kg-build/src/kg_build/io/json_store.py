from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..contracts import GraphBundle, SourceDocumentRecord


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(row, ensure_ascii=False) for row in rows]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def write_source_document_json(path: Path, source_document: SourceDocumentRecord) -> None:
    write_json(path, source_document.to_dict())


def read_source_document_json(path: Path) -> SourceDocumentRecord:
    return SourceDocumentRecord.from_dict(read_json(path))


def write_graph_bundle(path: Path, bundle: GraphBundle) -> None:
    bundle.validate_edge_references()
    write_json(path, bundle.to_dict())


def read_graph_bundle(path: Path) -> GraphBundle:
    return GraphBundle.from_dict(read_json(path))
