from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AlignmentDecision:
    left_id: str
    right_id: str
    approved: bool
    score: float
    model: str


def judge_alignment_pairs(candidate_pairs: list[dict[str, object]]) -> list[AlignmentDecision]:
    decisions: list[AlignmentDecision] = []
    for pair in candidate_pairs:
        left_text = str(pair.get("left_text", "")).strip()
        right_text = str(pair.get("right_text", "")).strip()
        similarity = float(pair.get("similarity", 0.0))
        approved = (
            left_text == right_text
            or (left_text and right_text and (left_text in right_text or right_text in left_text) and similarity >= 0.55)
            or similarity >= 0.84
        )
        score = 0.92 if approved and left_text == right_text else max(similarity, 0.35)
        decisions.append(
            AlignmentDecision(
                left_id=str(pair.get("left_id", "")),
                right_id=str(pair.get("right_id", "")),
                approved=approved,
                score=score,
                model="builder-align-judge",
            )
        )
    return decisions
