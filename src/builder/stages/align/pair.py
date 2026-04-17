from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ...contracts import AlignPairRecord, ConceptVectorRecord, EmbeddedConceptRecord


@dataclass(frozen=True)
class PairRuntimeConfig:
    top_k: int
    related_threshold: float
    equivalent_threshold: float


def resolve_pair_runtime_config(runtime: Any) -> PairRuntimeConfig:
    payload = dict(runtime.align_config().get("pair", {}))
    related_threshold = float(payload.get("related_threshold", 0.72) or 0.72)
    equivalent_threshold = float(payload.get("equivalent_threshold", 0.9) or 0.9)
    if equivalent_threshold < related_threshold:
        raise ValueError("builder.align.pair.equivalent_threshold must be >= related_threshold.")
    return PairRuntimeConfig(
        top_k=max(int(payload.get("top_k", 20) or 20), 1),
        related_threshold=related_threshold,
        equivalent_threshold=equivalent_threshold,
    )


def build_pairs(
    concepts: list[EmbeddedConceptRecord],
    vectors: list[ConceptVectorRecord],
    runtime: Any,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
    checkpoint_every: int = 0,
    checkpoint_callback: Callable[[list[AlignPairRecord], dict[str, int], list[str]], None] | None = None,
) -> list[AlignPairRecord]:
    config = resolve_pair_runtime_config(runtime)
    if not concepts or not vectors:
        if progress_callback is not None:
            progress_callback(1, 1)
        return []
    vector_by_id = {row.id: row.vector for row in vectors}
    ordered = [row for row in concepts if row.id in vector_by_id]
    if not ordered:
        if progress_callback is not None:
            progress_callback(1, 1)
        return []
    total = len(ordered)
    if progress_callback is not None:
        progress_callback(0, total)
    pair_scores: dict[tuple[int, int], float] = {}
    processed_concept_ids: list[str] = []
    next_checkpoint = max(int(checkpoint_every or 0), 0)
    for left_index, left in enumerate(ordered):
        equivalent_candidates: list[tuple[float, int]] = []
        pending_candidates: list[tuple[float, int]] = []
        left_vector = vector_by_id[left.id]
        for right_index, right in enumerate(ordered):
            if left_index == right_index:
                continue
            similarity = cosine_similarity(left_vector, vector_by_id[right.id])
            same_node = left.source_node_id == right.source_node_id
            if similarity >= config.equivalent_threshold:
                equivalent_candidates.append((similarity, right_index))
            elif not same_node and similarity >= config.related_threshold:
                pending_candidates.append((similarity, right_index))
        pending_candidates.sort(key=lambda item: (-item[0], item[1]))
        for similarity, right_index in equivalent_candidates + pending_candidates[: config.top_k]:
            key = (min(left_index, right_index), max(left_index, right_index))
            previous = pair_scores.get(key)
            if previous is None or similarity > previous:
                pair_scores[key] = similarity
        processed_concept_ids.append(left.id)
        if progress_callback is not None:
            progress_callback(left_index + 1, total)
        if checkpoint_callback is not None and checkpoint_every > 0:
            if left_index + 1 >= total:
                snapshot_pairs = materialize_pairs(ordered, pair_scores, config.equivalent_threshold)
                checkpoint_callback(
                    snapshot_pairs,
                    build_pair_stats(total, len(processed_concept_ids), snapshot_pairs),
                    list(processed_concept_ids),
                )
            elif next_checkpoint > 0 and left_index + 1 >= next_checkpoint:
                snapshot_pairs = materialize_pairs(ordered, pair_scores, config.equivalent_threshold)
                checkpoint_callback(
                    snapshot_pairs,
                    build_pair_stats(total, len(processed_concept_ids), snapshot_pairs),
                    list(processed_concept_ids),
                )
                while next_checkpoint > 0 and left_index + 1 >= next_checkpoint:
                    next_checkpoint += checkpoint_every

    return materialize_pairs(ordered, pair_scores, config.equivalent_threshold)


def materialize_pairs(
    concepts: list[EmbeddedConceptRecord],
    pair_scores: dict[tuple[int, int], float],
    equivalent_threshold: float,
) -> list[AlignPairRecord]:
    pairs: list[AlignPairRecord] = []
    for (left_index, right_index), similarity in sorted(pair_scores.items()):
        left = concepts[left_index]
        right = concepts[right_index]
        relation = "equivalent" if similarity >= equivalent_threshold else "pending"
        pairs.append(
            AlignPairRecord(
                left_id=left.id,
                right_id=right.id,
                left_text=left.text,
                right_text=right.text,
                similarity=similarity,
                relation=relation,
            )
        )
    return pairs


def build_pair_stats(input_count: int, processed_count: int, pairs: list[AlignPairRecord]) -> dict[str, int]:
    return {
        "input_count": input_count,
        "processed_count": processed_count,
        "output_count": len(pairs),
        "pair_count": len(pairs),
        "equivalent_count": sum(1 for row in pairs if row.relation == "equivalent"),
        "pending_count": sum(1 for row in pairs if row.relation == "pending"),
    }


def cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right:
        return 0.0
    numerator = sum(a * b for a, b in zip(left, right))
    left_norm = sum(value * value for value in left) ** 0.5 or 1.0
    right_norm = sum(value * value for value in right) ** 0.5 or 1.0
    return numerator / (left_norm * right_norm)
