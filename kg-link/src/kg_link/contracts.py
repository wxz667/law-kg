from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class RelationSample:
    sample_id: str
    source_node_id: str
    source_node_type: str
    source_name: str
    source_text: str
    parent_node_id: str = ""
    document_node_id: str = ""
    has_children: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RelationPrediction:
    sample_id: str
    source_node_id: str
    relation_type: str
    target_node_id: str
    confidence: float = 0.0
    evidence: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
