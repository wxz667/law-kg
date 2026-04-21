from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .io import read_jsonl


@dataclass(frozen=True)
class ReferenceCandidate:
    id: str
    source_node_id: str
    text: str
    target_node_ids: list[str]
    target_categories: list[str]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ReferenceCandidate":
        return cls(
            id=str(payload["id"]),
            source_node_id=str(payload["source_node_id"]),
            text=str(payload["text"]),
            target_node_ids=[str(value) for value in payload.get("target_node_ids", []) if str(value).strip()],
            target_categories=[str(value) for value in payload.get("target_categories", []) if str(value).strip()],
        )


def read_reference_candidates(path: Path) -> list[ReferenceCandidate]:
    return [ReferenceCandidate.from_dict(row) for row in read_jsonl(path)]
