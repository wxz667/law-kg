from __future__ import annotations

from ...contracts import AlignRelationRecord, EquivalenceRecord, InferPairRecord
from ...utils.math import cosine_topk_matches
from .features import InferFeatureStore, aa_score, bridge_evidence, ca_score, shares_document_context, shares_local_structural_context
from .types import InferRecallPassConfig, InferRecallPassResult, InferRecallRuntimeConfig

LEGACY_RECALL_PASS_DEFAULTS = (
    {
        "top_k_per_concept": 12,
        "score_threshold": 0.70,
        "semantic_weight": 0.60,
        "aa_weight": 0.00,
        "ca_weight": 0.30,
        "bridge_weight": 0.10,
    },
    {
        "top_k_per_concept": 8,
        "score_threshold": 0.68,
        "semantic_weight": 0.55,
        "aa_weight": 0.25,
        "ca_weight": 0.10,
        "bridge_weight": 0.10,
    },
)


def resolve_recall_runtime_config(runtime) -> InferRecallRuntimeConfig:
    payload = dict(runtime.infer_config().get("recall", {}))
    return InferRecallRuntimeConfig(
        matrix_block_size=max(int(payload.get("matrix_block_size", 512) or 512), 1),
        device_preference=str(payload.get("device_preference", "auto") or "auto").strip().lower() or "auto",
        semantic_candidate_multiplier=max(int(payload.get("semantic_candidate_multiplier", 8) or 8), 1),
        semantic_candidate_floor=max(int(payload.get("semantic_candidate_floor", 64) or 64), 1),
        allow_same_document_pairs=bool(payload.get("allow_same_document_pairs", False)),
    )


def _default_pass_payload(pass_index: int) -> dict[str, float]:
    if pass_index <= len(LEGACY_RECALL_PASS_DEFAULTS):
        return dict(LEGACY_RECALL_PASS_DEFAULTS[pass_index - 1])
    return dict(LEGACY_RECALL_PASS_DEFAULTS[-1])


def _build_recall_pass_config(payload: dict[str, object], *, pass_index: int) -> InferRecallPassConfig:
    defaults = _default_pass_payload(pass_index)
    return InferRecallPassConfig(
        top_k_per_concept=max(int(payload.get("top_k_per_concept", defaults["top_k_per_concept"]) or defaults["top_k_per_concept"]), 1),
        score_threshold=float(payload.get("score_threshold", defaults["score_threshold"]) or defaults["score_threshold"]),
        semantic_weight=float(payload.get("semantic_weight", defaults["semantic_weight"]) or defaults["semantic_weight"]),
        aa_weight=float(payload.get("aa_weight", defaults["aa_weight"]) or defaults["aa_weight"]),
        ca_weight=float(payload.get("ca_weight", defaults["ca_weight"]) or defaults["ca_weight"]),
        bridge_weight=float(payload.get("bridge_weight", defaults["bridge_weight"]) or defaults["bridge_weight"]),
    )


def resolve_recall_pass_configs(runtime) -> list[InferRecallPassConfig]:
    recall_payload = dict(runtime.infer_config().get("recall", {}))
    configured_passes = recall_payload.get("pass", [])
    if isinstance(configured_passes, list) and configured_passes:
        return [
            _build_recall_pass_config(dict(item) if isinstance(item, dict) else {}, pass_index=index)
            for index, item in enumerate(configured_passes, start=1)
        ]
    legacy_payloads = []
    for index in range(1, len(LEGACY_RECALL_PASS_DEFAULTS) + 1):
        item = recall_payload.get(f"pass{index}", None)
        legacy_payloads.append(dict(item) if isinstance(item, dict) else {})
    return [
        _build_recall_pass_config(payload, pass_index=index)
        for index, payload in enumerate(legacy_payloads, start=1)
    ]


def resolve_recall_pass_config(runtime, pass_index: int) -> InferRecallPassConfig:
    configs = resolve_recall_pass_configs(runtime)
    if pass_index < 1 or pass_index > len(configs):
        raise ValueError(f"infer recall pass index out of range: {pass_index}")
    return configs[pass_index - 1]


def run_recall_pass(
    *,
    pass_index: int,
    runtime,
    scoped_concepts: list[EquivalenceRecord],
    all_concepts: list[EquivalenceRecord],
    feature_store: InferFeatureStore,
    semantic_shortlists: dict[str, list[tuple[str, float]]],
    existing_relations: list[AlignRelationRecord],
    blocked_pair_keys: set[tuple[str, str]] | None = None,
    progress_callback=None,
    checkpoint_every: int = 0,
    checkpoint_callback=None,
) -> InferRecallPassResult:
    config = resolve_recall_pass_config(runtime, pass_index)
    runtime_config = resolve_recall_runtime_config(runtime)
    existing_relation_pairs = {
        tuple(sorted((row.left_id, row.right_id)))
        for row in existing_relations
        if row.left_id and row.right_id
    }
    blocked_pairs = set(blocked_pair_keys or set())
    deduped_by_key: dict[tuple[str, str], InferPairRecord] = {}
    total = len(scoped_concepts)
    processed_concept_ids: list[str] = []
    next_checkpoint = max(int(checkpoint_every or 0), 0)

    if progress_callback is not None:
        progress_callback(0, total)

    for index, left in enumerate(sorted(scoped_concepts, key=lambda item: item.id), start=1):
        per_concept: list[InferPairRecord] = []
        for right_id, semantic in semantic_shortlists.get(left.id, []):
            if left.id == right_id:
                continue
            pair_key = tuple(sorted((left.id, right_id)))
            if pair_key in existing_relation_pairs or pair_key in blocked_pairs:
                continue
            if shares_local_structural_context(left.id, right_id, feature_store):
                continue
            if not runtime_config.allow_same_document_pairs and shares_document_context(left.id, right_id, feature_store):
                continue
            aa = aa_score(left.id, right_id, feature_store)
            ca = ca_score(left.id, right_id, feature_store)
            bridge = bridge_evidence(left.id, right_id, feature_store).score
            score = (
                config.semantic_weight * semantic
                + config.aa_weight * aa
                + config.ca_weight * ca
                + config.bridge_weight * bridge
            )
            if score < config.score_threshold:
                continue
            normalized_left, normalized_right = pair_key
            per_concept.append(
                InferPairRecord(
                    left_id=normalized_left,
                    right_id=normalized_right,
                    pass_index=pass_index,
                    semantic_score=semantic,
                    aa_score=aa,
                    ca_score=ca,
                    bridge_score=bridge,
                    score=score,
                )
            )
        per_concept.sort(key=lambda item: (-item.score, item.right_id, item.left_id))
        for candidate in per_concept[: config.top_k_per_concept]:
            candidate_key = (candidate.left_id, candidate.right_id)
            previous = deduped_by_key.get(candidate_key)
            if previous is None or candidate.score > previous.score:
                deduped_by_key[candidate_key] = candidate
        processed_concept_ids.append(left.id)
        if progress_callback is not None:
            progress_callback(index, total)
        if checkpoint_callback is not None and checkpoint_every > 0:
            should_checkpoint = index >= total or (next_checkpoint > 0 and index >= next_checkpoint)
            if should_checkpoint:
                snapshot_pairs = [deduped_by_key[key] for key in sorted(deduped_by_key)]
                checkpoint_callback(
                    snapshot_pairs,
                    {
                        "pair_count": len(snapshot_pairs),
                        "result_count": len(snapshot_pairs),
                    },
                    list(processed_concept_ids),
                )
                while next_checkpoint > 0 and index >= next_checkpoint:
                    next_checkpoint += checkpoint_every

    all_pairs = [deduped_by_key[key] for key in sorted(deduped_by_key)]
    return InferRecallPassResult(
        pairs=all_pairs,
        processed_concept_ids=processed_concept_ids,
        stats={
            "pair_count": len(all_pairs),
            "result_count": len(all_pairs),
        },
    )


def build_semantic_shortlists(
    *,
    runtime,
    scoped_concepts: list[EquivalenceRecord],
    all_concepts: list[EquivalenceRecord],
    feature_store: InferFeatureStore,
) -> dict[str, list[tuple[str, float]]]:
    if not scoped_concepts or not all_concepts:
        return {}
    runtime_config = resolve_recall_runtime_config(runtime)
    pass_configs = resolve_recall_pass_configs(runtime)
    search_limit = min(
        len(all_concepts),
        max(
            max(config.top_k_per_concept for config in pass_configs) * runtime_config.semantic_candidate_multiplier,
            runtime_config.semantic_candidate_floor,
        ),
    )
    ordered_all = sorted(all_concepts, key=lambda item: item.id)
    ordered_scoped = sorted(scoped_concepts, key=lambda item: item.id)
    concept_ids = [row.id for row in ordered_all]
    matches = cosine_topk_matches(
        [feature_store.concept_vectors[row.id] for row in ordered_scoped],
        [feature_store.concept_vectors[row.id] for row in ordered_all],
        top_k=search_limit,
        threshold=-1.0,
        block_size=runtime_config.matrix_block_size,
        device_preference=runtime_config.device_preference,
    )
    shortlists: dict[str, list[tuple[str, float]]] = {}
    for concept, row_matches in zip(ordered_scoped, matches):
        shortlists[concept.id] = [
            (
                concept_ids[target_index],
                max(0.0, min(1.0, (score + 1.0) / 2.0)),
            )
            for target_index, score in row_matches
        ]
    return shortlists
