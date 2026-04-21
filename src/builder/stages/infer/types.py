from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ...contracts import AlignRelationRecord, GraphBundle, InferPairRecord


@dataclass(frozen=True)
class InferRecallPassConfig:
    top_k_per_concept: int
    score_threshold: float
    semantic_weight: float
    aa_weight: float
    ca_weight: float
    bridge_weight: float


@dataclass(frozen=True)
class InferRecallRuntimeConfig:
    matrix_block_size: int
    device_preference: str
    semantic_candidate_multiplier: int
    semantic_candidate_floor: int
    allow_same_document_pairs: bool


@dataclass(frozen=True)
class InferJudgeRuntimeConfig:
    provider: str
    model: str
    batch_size: int
    concurrent_requests: int
    request_timeout_seconds: int
    max_retries: int
    min_strength: int
    params: dict[str, Any]
    rate_limit: dict[str, Any]


@dataclass(frozen=True)
class InferBridgeEvidence:
    count: int = 0
    score: float = 0.0
    evidence_root_ids: tuple[str, ...] = ()
    summary: str = ""


@dataclass
class InferRecallPassResult:
    pairs: list[InferPairRecord] = field(default_factory=list)
    processed_concept_ids: list[str] = field(default_factory=list)
    stats: dict[str, int] = field(default_factory=dict)


@dataclass
class InferJudgeResult:
    pairs: list[InferPairRecord] = field(default_factory=list)
    processed_pair_ids: list[str] = field(default_factory=list)
    stats: dict[str, int] = field(default_factory=dict)
    llm_errors: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class InferResult:
    pairs: list[InferPairRecord] = field(default_factory=list)
    relations: list[AlignRelationRecord] = field(default_factory=list)
    graph_bundle: GraphBundle = field(default_factory=GraphBundle)
    stats: dict[str, int] = field(default_factory=dict)
    llm_errors: list[dict[str, Any]] = field(default_factory=list)
