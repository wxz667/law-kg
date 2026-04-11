from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    materialized = [json.dumps(row, ensure_ascii=False) for row in rows]
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text("\n".join(materialized) + ("\n" if materialized else ""), encoding="utf-8")
    temp_path.replace(path)


def append_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    materialized = [json.dumps(row, ensure_ascii=False) for row in rows]
    if not materialized:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(materialized) + "\n")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows
