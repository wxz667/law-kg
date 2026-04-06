from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReferenceCandidate:
    source_node_id: str
    evidence_text: str
    target_ref_text: str
    kind: str
    article_label: str = ""
    document_title: str = ""


@dataclass(frozen=True)
class ResolvedReference:
    candidate: ReferenceCandidate
    target_node_id: str


@dataclass(frozen=True)
class RelationEdgePlan:
    source_node_id: str
    target_node_id: str
    relation_type: str
    score: float
    evidence_text: str
    target_ref_text: str
    model: str
