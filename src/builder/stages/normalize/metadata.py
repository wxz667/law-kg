from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...utils.layout import compact_text_key


def load_metadata_items(metadata_root: Path) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for path in sorted(metadata_root.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError(f"Metadata file must contain a list: {path}")
        for item in payload:
            if isinstance(item, dict):
                items.append(dict(item))
            else:
                raise ValueError(f"Metadata entry must be an object: {path}")
    return items


def build_document_index(docs_root: Path) -> dict[str, Path]:
    index: dict[str, Path] = {}
    for path in sorted(docs_root.glob("*.docx")):
        index[path.stem] = path.resolve()
        normalized_key = compact_text_key(path.stem)
        index.setdefault(normalized_key, path.resolve())
    return index


def match_document_path(title: str, document_index: dict[str, Path]) -> Path | None:
    if title in document_index:
        return document_index[title]
    return document_index.get(compact_text_key(title))
