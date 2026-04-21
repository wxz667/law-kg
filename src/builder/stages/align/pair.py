from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import Any, Callable

from ...contracts import AlignConceptRecord, AlignPairRecord, ConceptVectorRecord, EquivalenceRecord
from ...utils.math import CosineMatrixIndex, normalize_numpy_matrix
from ...utils.math import cosine_similarity

try:
    import numpy as np
except ModuleNotFoundError:  # pragma: no cover
    np = None


@dataclass(frozen=True)
class PairRuntimeConfig:
    top_k_per_concept: int
    similarity_threshold: float
    matrix_block_size: int
    device_preference: str


@dataclass(frozen=True)
class PairTarget:
    id: str
    root_ids: tuple[str, ...]
    vector: list[float]


def resolve_pair_runtime_config(runtime: Any) -> PairRuntimeConfig:
    payload = dict(runtime.align_config().get("recall", {}))
    return PairRuntimeConfig(
        top_k_per_concept=max(int(payload.get("top_k_per_concept", 20) or 20), 1),
        similarity_threshold=float(payload.get("similarity_threshold", 0.82) or 0.82),
        matrix_block_size=max(int(payload.get("matrix_block_size", 512) or 512), 1),
        device_preference=str(payload.get("device_preference", "auto") or "auto").strip().lower() or "auto",
    )


def build_pairs(
    scoped_concepts: list[AlignConceptRecord],
    all_concepts: list[AlignConceptRecord],
    vectors: list[ConceptVectorRecord],
    equivalence: list[EquivalenceRecord],
    runtime: Any,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
    checkpoint_every: int = 0,
    checkpoint_callback: Callable[[list[AlignPairRecord], dict[str, int], list[str]], None] | None = None,
) -> list[AlignPairRecord]:
    config = resolve_pair_runtime_config(runtime)
    vector_by_id = {row.id: row.vector for row in vectors}
    concept_by_id = {row.id: row for row in all_concepts}
    scoped = [row for row in scoped_concepts if row.id in vector_by_id]
    if progress_callback is not None:
        progress_callback(0, len(scoped))
    if not scoped:
        return []

    equivalence_targets = build_equivalence_targets(equivalence, concept_by_id, vector_by_id)
    if np is not None:
        return build_pairs_numpy(
            scoped,
            vector_by_id,
            equivalence_targets,
            config,
            progress_callback=progress_callback,
            checkpoint_every=checkpoint_every,
            checkpoint_callback=checkpoint_callback,
        )
    return build_pairs_python(
        scoped,
        vector_by_id,
        equivalence_targets,
        config,
        progress_callback=progress_callback,
        checkpoint_every=checkpoint_every,
        checkpoint_callback=checkpoint_callback,
    )


def build_pairs_numpy(
    scoped: list[AlignConceptRecord],
    vector_by_id: dict[str, list[float]],
    equivalence_targets: list[PairTarget],
    config: PairRuntimeConfig,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
    checkpoint_every: int = 0,
    checkpoint_callback: Callable[[list[AlignPairRecord], dict[str, int], list[str]], None] | None = None,
) -> list[AlignPairRecord]:
    raw_ids = [row.id for row in scoped]
    raw_roots = [row.root for row in scoped]
    raw_matrix = normalize_numpy_matrix([vector_by_id[row.id] for row in scoped])
    raw_index = CosineMatrixIndex(
        [vector_by_id[row.id] for row in scoped],
        device_preference=config.device_preference,
    )
    eq_matrix = build_equivalence_matrix(equivalence_targets, raw_matrix.shape[1])
    eq_index = CosineMatrixIndex(
        [row.vector for row in equivalence_targets],
        device_preference=config.device_preference,
    )
    eq_root_sets = [set(row.root_ids) for row in equivalence_targets]
    produced_pairs: list[AlignPairRecord] = []
    seen_pair_keys: set[tuple[str, str]] = set()
    processed_concept_ids: list[str] = []
    next_checkpoint = max(int(checkpoint_every or 0), 0)
    block_size = min(config.matrix_block_size, len(scoped))
    for left_start in range(0, len(scoped), block_size):
        left_end = min(left_start + block_size, len(scoped))
        left_block = raw_matrix[left_start:left_end]
        candidate_heaps: list[list[tuple[float, str]]] = [[] for _ in range(left_end - left_start)]

        if eq_matrix.shape[0] > 0:
            eq_scores = eq_index.score_normalized_block(left_block)
            for local_index in range(left_end - left_start):
                left_root = raw_roots[left_start + local_index]
                target_indices = np.flatnonzero(eq_scores[local_index] >= config.similarity_threshold)
                for target_index in target_indices.tolist():
                    if left_root in eq_root_sets[target_index]:
                        continue
                    push_candidate(
                        candidate_heaps[local_index],
                        float(eq_scores[local_index, target_index]),
                        equivalence_targets[target_index].id,
                        config.top_k_per_concept,
                    )

        raw_scores = raw_index.score_normalized_block(left_block)
        for local_index in range(left_end - left_start):
            left_root = raw_roots[left_start + local_index]
            start_offset = left_start + local_index + 1
            if start_offset >= len(scoped):
                continue
            raw_indices = np.flatnonzero(raw_scores[local_index, start_offset:] >= config.similarity_threshold)
            for offset in raw_indices.tolist():
                right_index = start_offset + offset
                if left_root == raw_roots[right_index]:
                    continue
                push_candidate(
                    candidate_heaps[local_index],
                    float(raw_scores[local_index, right_index]),
                    raw_ids[right_index],
                    config.top_k_per_concept,
                )

        for local_index, heap in enumerate(candidate_heaps):
            left_id = raw_ids[left_start + local_index]
            append_heap_pairs(
                left_id,
                heap,
                produced_pairs,
                seen_pair_keys,
            )
            processed_concept_ids.append(left_id)
            current_count = left_start + local_index + 1
            if progress_callback is not None:
                progress_callback(current_count, len(scoped))
            if checkpoint_callback is not None and checkpoint_every > 0:
                if current_count >= len(scoped):
                    checkpoint_callback(
                        list(produced_pairs),
                        build_pair_stats(scoped, processed_concept_ids, produced_pairs),
                        list(processed_concept_ids),
                    )
                elif next_checkpoint > 0 and current_count >= next_checkpoint:
                    checkpoint_callback(
                        list(produced_pairs),
                        build_pair_stats(scoped, processed_concept_ids, produced_pairs),
                        list(processed_concept_ids),
                    )
                    while next_checkpoint > 0 and current_count >= next_checkpoint:
                        next_checkpoint += checkpoint_every
    return produced_pairs


def build_pairs_python(
    scoped: list[AlignConceptRecord],
    vector_by_id: dict[str, list[float]],
    equivalence_targets: list[PairTarget],
    config: PairRuntimeConfig,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
    checkpoint_every: int = 0,
    checkpoint_callback: Callable[[list[AlignPairRecord], dict[str, int], list[str]], None] | None = None,
) -> list[AlignPairRecord]:
    produced_pairs: list[AlignPairRecord] = []
    seen_pair_keys: set[tuple[str, str]] = set()
    processed_concept_ids: list[str] = []
    next_checkpoint = max(int(checkpoint_every or 0), 0)
    target_root_sets = [set(target.root_ids) for target in equivalence_targets]
    for left_index, left in enumerate(scoped):
        left_vector = vector_by_id[left.id]
        candidates: list[tuple[float, str]] = []
        for target, target_root_ids in zip(equivalence_targets, target_root_sets):
            if left.root in target_root_ids:
                continue
            similarity = cosine_similarity(left_vector, target.vector)
            if similarity >= config.similarity_threshold:
                candidates.append((similarity, target.id))
        for right in scoped[left_index + 1 :]:
            if left.root == right.root:
                continue
            right_vector = vector_by_id.get(right.id)
            if right_vector is None:
                continue
            similarity = cosine_similarity(left_vector, right_vector)
            if similarity >= config.similarity_threshold:
                candidates.append((similarity, right.id))
        append_top_k_pairs(
            left.id,
            candidates,
            config.top_k_per_concept,
            produced_pairs,
            seen_pair_keys,
        )
        processed_concept_ids.append(left.id)
        if progress_callback is not None:
            progress_callback(left_index + 1, len(scoped))
        if checkpoint_callback is not None and checkpoint_every > 0:
            if left_index + 1 >= len(scoped):
                checkpoint_callback(
                    list(produced_pairs),
                    build_pair_stats(scoped, processed_concept_ids, produced_pairs),
                    list(processed_concept_ids),
                )
            elif next_checkpoint > 0 and left_index + 1 >= next_checkpoint:
                checkpoint_callback(
                    list(produced_pairs),
                    build_pair_stats(scoped, processed_concept_ids, produced_pairs),
                    list(processed_concept_ids),
                )
                while next_checkpoint > 0 and left_index + 1 >= next_checkpoint:
                    next_checkpoint += checkpoint_every
    return produced_pairs


def append_top_k_pairs(
    left_id: str,
    candidates: list[tuple[float, str]],
    top_k_per_concept: int,
    produced_pairs: list[AlignPairRecord],
    seen_pair_keys: set[tuple[str, str]],
) -> None:
    candidates.sort(key=lambda item: (-item[0], item[1]))
    for similarity, target_id in candidates[:top_k_per_concept]:
        pair_key = (left_id, target_id)
        if pair_key in seen_pair_keys:
            continue
        produced_pairs.append(
            AlignPairRecord(
                left_id=left_id,
                right_id=target_id,
                relation="",
                similarity=float(similarity),
            )
        )
        seen_pair_keys.add(pair_key)


def append_heap_pairs(
    left_id: str,
    heap: list[tuple[float, str]],
    produced_pairs: list[AlignPairRecord],
    seen_pair_keys: set[tuple[str, str]],
) -> None:
    for similarity, target_id in sorted(heap, key=lambda item: (-item[0], item[1])):
        pair_key = (left_id, target_id)
        if pair_key in seen_pair_keys:
            continue
        produced_pairs.append(
            AlignPairRecord(
                left_id=left_id,
                right_id=target_id,
                relation="",
                similarity=float(similarity),
            )
        )
        seen_pair_keys.add(pair_key)


def push_candidate(
    heap: list[tuple[float, str]],
    similarity: float,
    target_id: str,
    limit: int,
) -> None:
    item = (similarity, target_id)
    if len(heap) < limit:
        heapq.heappush(heap, item)
        return
    if item <= heap[0]:
        return
    heapq.heapreplace(heap, item)


def build_equivalence_matrix(equivalence_targets: list[PairTarget], width: int):
    if np is None:
        return []
    if not equivalence_targets:
        return np.zeros((0, width), dtype=np.float32)
    return normalize_numpy_matrix([row.vector for row in equivalence_targets])


def build_equivalence_targets(
    equivalence: list[EquivalenceRecord],
    concept_by_id: dict[str, AlignConceptRecord],
    vector_by_id: dict[str, list[float]],
) -> list[PairTarget]:
    targets: list[PairTarget] = []
    for row in equivalence:
        member_id = choose_representative_member(row, concept_by_id)
        if not member_id:
            continue
        vector = vector_by_id.get(member_id)
        if not vector:
            continue
        targets.append(
            PairTarget(
                id=row.id,
                root_ids=tuple(sorted({str(value) for value in row.root_ids if str(value).strip()})),
                vector=vector,
            )
        )
    return targets


def choose_representative_member(
    row: EquivalenceRecord,
    concept_by_id: dict[str, AlignConceptRecord],
) -> str:
    exact_matches = sorted(
        member_id
        for member_id in row.member_ids
        if member_id in concept_by_id and concept_by_id[member_id].name == row.name
    )
    if exact_matches:
        return exact_matches[0]
    available = sorted(member_id for member_id in row.member_ids if member_id in concept_by_id)
    return available[0] if available else ""


def build_pair_stats(
    scoped_concepts: list[AlignConceptRecord],
    processed_concept_ids: list[str],
    pairs: list[AlignPairRecord],
) -> dict[str, int]:
    return {
        "input_count": len(scoped_concepts),
        "processed_count": len(processed_concept_ids),
        "pair_count": len(pairs),
        "pending_count": sum(1 for row in pairs if row.relation == ""),
    }
