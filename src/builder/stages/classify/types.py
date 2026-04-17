from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ...contracts import (
    ClassifyPendingRecord,
    GraphBundle,
    LlmJudgeDetailRecord,
    ReferenceCandidateRecord,
    ClassifyRecord,
)
from ...utils.reference_graph import ReferenceGraphContext


@dataclass
class ClassifyResult:
    results: list[ClassifyRecord] = field(default_factory=list)
    llm_judgments: list[LlmJudgeDetailRecord] = field(default_factory=list)
    graph_bundle: GraphBundle | None = None
    llm_errors: list[dict[str, Any]] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)


@dataclass
class ClassifyModelResult:
    results: list[ClassifyRecord] = field(default_factory=list)
    pending_records: list[ClassifyPendingRecord] = field(default_factory=list)
    processed_source_ids: list[str] = field(default_factory=list)
    processed_candidate_ids: list[str] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ClassifyContext(ReferenceGraphContext):
    active_source_ids: set[str] | None


@dataclass(frozen=True)
class PendingArbitration:
    candidate: ReferenceCandidateRecord
    prediction: Any
    source_category: str
    target_categories: list[str]
    is_legislative_interpretation: bool
