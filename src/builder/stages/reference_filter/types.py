from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ...contracts import ReferenceCandidateRecord
from ...utils.reference_graph import ReferenceGraphContext


@dataclass(frozen=True)
class ReferenceCandidate:
    source_node_id: str
    evidence_text: str
    target_ref_text: str
    kind: str
    article_label: str = ""
    paragraph_label: str = ""
    item_label: str = ""
    sub_item_label: str = ""
    document_title: str = ""
    matched_text: str = ""
    span_start: int = -1
    span_end: int = -1
    has_multiple_targets: bool = False
    doc_token: str = ""
    alias_title: str = ""


@dataclass(frozen=True)
class ResolvedReference:
    candidate: ReferenceCandidate
    target_node_id: str
    target_ref_text: str = ""
    target_span_start: int = -1
    target_span_end: int = -1


@dataclass
class ReferenceFilterProfiling:
    extract_seconds: float = 0.0
    resolve_seconds: float = 0.0
    mark_seconds: float = 0.0
    candidate_kind_counts: dict[str, int] = field(default_factory=dict)

    def merge(self, other: "ReferenceFilterProfiling") -> None:
        self.extract_seconds += other.extract_seconds
        self.resolve_seconds += other.resolve_seconds
        self.mark_seconds += other.mark_seconds
        for key, value in other.candidate_kind_counts.items():
            self.candidate_kind_counts[key] = int(self.candidate_kind_counts.get(key, 0)) + int(value)

    def bump_kind(self, kind: str) -> None:
        self.candidate_kind_counts[kind] = int(self.candidate_kind_counts.get(kind, 0)) + 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "extract_seconds": round(self.extract_seconds, 6),
            "resolve_seconds": round(self.resolve_seconds, 6),
            "mark_seconds": round(self.mark_seconds, 6),
            "candidate_kind_counts": dict(self.candidate_kind_counts),
        }


@dataclass
class ReferenceFilterResult:
    candidates: list[ReferenceCandidateRecord] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)
    profiling: dict[str, Any] = field(default_factory=dict)


@dataclass
class DocumentScanResult:
    candidates: list[ReferenceCandidateRecord] = field(default_factory=list)
    source_units: int = 0
    sentences_scanned: int = 0
    raw_candidates: int = 0
    resolved_targets: int = 0
    dropped_quoted: int = 0
    dropped_special_targets: int = 0
    dropped_meaningless_self: int = 0
    profiling: ReferenceFilterProfiling = field(default_factory=ReferenceFilterProfiling)


@dataclass(frozen=True)
class ReferenceFilterContext(ReferenceGraphContext):
    special_document_ids: set[str]
    category_by_document_id: dict[str, str]
