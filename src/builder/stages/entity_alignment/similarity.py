from __future__ import annotations

from itertools import combinations
from math import sqrt


def cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right:
        return 0.0
    numerator = sum(a * b for a, b in zip(left, right))
    left_norm = sqrt(sum(value * value for value in left)) or 1.0
    right_norm = sqrt(sum(value * value for value in right)) or 1.0
    return numerator / (left_norm * right_norm)


def collect_candidate_pairs(candidate_nodes: list[object], vectors: list[list[float]]) -> list[dict[str, object]]:
    candidate_pairs: list[dict[str, object]] = []
    for left_index, right_index in combinations(range(len(candidate_nodes)), 2):
        left = candidate_nodes[left_index]
        right = candidate_nodes[right_index]
        similarity = cosine_similarity(vectors[left_index], vectors[right_index])
        left_text = str(left.metadata.get("normalized_text", left.name))
        right_text = str(right.metadata.get("normalized_text", right.name))
        if left_text == right_text or similarity >= 0.72:
            candidate_pairs.append(
                {
                    "left_id": left.id,
                    "right_id": right.id,
                    "left_text": left_text,
                    "right_text": right_text,
                    "similarity": similarity,
                }
            )
    return candidate_pairs
