from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence

try:
    import numpy as np
except ModuleNotFoundError:  # pragma: no cover
    np = None

try:
    import torch
except ModuleNotFoundError:  # pragma: no cover
    torch = None


@dataclass(frozen=True)
class SimilarityBackend:
    kind: str
    device: str = "cpu"


def cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    if left_norm <= 0.0 or right_norm <= 0.0:
        return 0.0
    dot = sum(left_value * right_value for left_value, right_value in zip(left, right))
    return float(dot / (left_norm * right_norm))


class CosineMatrixIndex:
    def __init__(
        self,
        target_vectors: Sequence[Sequence[float]],
        *,
        device_preference: str = "auto",
    ) -> None:
        self.backend = resolve_similarity_backend(device_preference)
        self.matrix = normalize_numpy_matrix(target_vectors)
        self.row_count = len(target_vectors)
        self._target_tensor = None
        if np is not None and self.backend.kind == "torch" and torch is not None and getattr(self.matrix, "size", 0) > 0:
            with torch.no_grad():
                self._target_tensor = torch.as_tensor(self.matrix, dtype=torch.float32, device=self.backend.device)

    def score_normalized_block(self, query_matrix):
        if np is None:
            left_rows = len(query_matrix)
            return [
                [
                    cosine_similarity(list(query_matrix[left_index]), list(self.matrix[right_index]))
                    for right_index in range(self.row_count)
                ]
                for left_index in range(left_rows)
            ]
        if self.matrix.size == 0 or query_matrix.size == 0:
            return np.zeros((query_matrix.shape[0], self.row_count), dtype=np.float32)
        if self._target_tensor is not None and torch is not None:
            with torch.no_grad():
                query_tensor = torch.as_tensor(query_matrix, dtype=torch.float32, device=self.backend.device)
                scores = query_tensor @ self._target_tensor.T
                return scores.detach().to("cpu").numpy().astype(np.float32, copy=False)
        return query_matrix @ self.matrix.T


def resolve_similarity_backend(device_preference: str = "auto") -> SimilarityBackend:
    preference = str(device_preference or "auto").strip().lower() or "auto"
    if preference not in {"auto", "cpu", "cuda"}:
        preference = "auto"
    if preference == "cuda":
        if torch is not None and torch.cuda.is_available():
            return SimilarityBackend(kind="torch", device="cuda")
        if np is not None:
            return SimilarityBackend(kind="numpy", device="cpu")
        if torch is not None:
            return SimilarityBackend(kind="torch", device="cpu")
        return SimilarityBackend(kind="python", device="cpu")
    if preference == "cpu":
        if np is not None:
            return SimilarityBackend(kind="numpy", device="cpu")
        if torch is not None:
            return SimilarityBackend(kind="torch", device="cpu")
        return SimilarityBackend(kind="python", device="cpu")
    if torch is not None and torch.cuda.is_available():
        return SimilarityBackend(kind="torch", device="cuda")
    if np is not None:
        return SimilarityBackend(kind="numpy", device="cpu")
    if torch is not None:
        return SimilarityBackend(kind="torch", device="cpu")
    return SimilarityBackend(kind="python", device="cpu")


def normalize_numpy_matrix(vectors: Sequence[Sequence[float]]):
    if np is None:
        return []
    if not vectors:
        return np.zeros((0, 0), dtype=np.float32)
    matrix = np.asarray(vectors, dtype=np.float32)
    if matrix.size == 0:
        width = len(vectors[0]) if vectors and vectors[0] else 0
        return np.zeros((len(vectors), width), dtype=np.float32)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0.0] = 1.0
    return matrix / norms


def cosine_topk_matches(
    query_vectors: Sequence[Sequence[float]],
    target_vectors: Sequence[Sequence[float]],
    *,
    top_k: int,
    threshold: float = 0.0,
    block_size: int = 512,
    device_preference: str = "auto",
) -> list[list[tuple[int, float]]]:
    top_k = max(int(top_k or 0), 0)
    if top_k <= 0 or not query_vectors or not target_vectors:
        return [[] for _ in range(len(query_vectors))]
    if np is None:
        return _cosine_topk_matches_python(query_vectors, target_vectors, top_k=top_k, threshold=threshold)

    target_index = CosineMatrixIndex(target_vectors, device_preference=device_preference)
    query_matrix = normalize_numpy_matrix(query_vectors)
    if query_matrix.size == 0 or getattr(target_index.matrix, "size", 0) == 0:
        return [[] for _ in range(len(query_vectors))]
    results: list[list[tuple[int, float]]] = []
    effective_block_size = max(int(block_size or 0), 1)
    candidate_count = target_index.row_count
    effective_top_k = min(top_k, candidate_count)

    for start in range(0, query_matrix.shape[0], effective_block_size):
        stop = min(start + effective_block_size, query_matrix.shape[0])
        score_block = target_index.score_normalized_block(query_matrix[start:stop])
        results.extend(
            _topk_rows_from_scores(
                score_block,
                top_k=effective_top_k,
                threshold=threshold,
            )
        )
    return results


def _topk_rows_from_scores(scores, *, top_k: int, threshold: float) -> list[list[tuple[int, float]]]:
    if np is None:
        return []
    if scores.size == 0 or top_k <= 0:
        return [[] for _ in range(scores.shape[0])]
    row_count, target_count = scores.shape
    if top_k >= target_count:
        top_indices = np.argsort(-scores, axis=1)[:, :top_k]
    else:
        partition = np.argpartition(scores, target_count - top_k, axis=1)[:, -top_k:]
        partition_scores = np.take_along_axis(scores, partition, axis=1)
        order = np.argsort(-partition_scores, axis=1)
        top_indices = np.take_along_axis(partition, order, axis=1)

    rows: list[list[tuple[int, float]]] = []
    for row_index in range(row_count):
        row_matches: list[tuple[int, float]] = []
        for target_index in top_indices[row_index].tolist():
            score = float(scores[row_index, target_index])
            if score < threshold:
                continue
            row_matches.append((int(target_index), score))
        rows.append(row_matches)
    return rows


def _cosine_topk_matches_python(
    query_vectors: Sequence[Sequence[float]],
    target_vectors: Sequence[Sequence[float]],
    *,
    top_k: int,
    threshold: float,
) -> list[list[tuple[int, float]]]:
    results: list[list[tuple[int, float]]] = []
    for query in query_vectors:
        row = [
            (target_index, cosine_similarity(list(query), list(target)))
            for target_index, target in enumerate(target_vectors)
        ]
        row = [item for item in row if item[1] >= threshold]
        row.sort(key=lambda item: (-item[1], item[0]))
        results.append(row[:top_k])
    return results
