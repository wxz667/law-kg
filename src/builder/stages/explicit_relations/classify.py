from __future__ import annotations

from ...pipeline.runtime import PipelineRuntime
from .types import RelationEdgePlan, ResolvedReference


def classify_resolved_references(runtime: PipelineRuntime, resolved: list[ResolvedReference]) -> list[RelationEdgePlan]:
    if not resolved:
        return []
    predictions = runtime.predict_relations([item.candidate.evidence_text for item in resolved])
    plans: list[RelationEdgePlan] = []
    for item, prediction in zip(resolved, predictions):
        plans.append(
            RelationEdgePlan(
                source_node_id=item.candidate.source_node_id,
                target_node_id=item.target_node_id,
                relation_type=prediction.relation_type,
                score=prediction.score,
                evidence_text=item.candidate.evidence_text,
                target_ref_text=item.candidate.target_ref_text,
                model=prediction.model,
            )
        )
    return plans
