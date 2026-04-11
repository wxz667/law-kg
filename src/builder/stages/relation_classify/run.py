from __future__ import annotations

import threading
from typing import Any, Callable

from ...contracts import GraphBundle, ReferenceCandidateRecord, RelationClassifyRecord
from ...utils.legal_reference import candidate_source_category, is_legislative_interpretation_document
from ...utils.locator import owner_source_id
from ...utils.reference_graph import build_reference_graph_context
from .arbiter import arbitrate_uncertain_candidates
from .classify import (
    iter_predict_interprets_batches,
    is_prediction_low_confidence,
    resolve_interprets_policy,
)
from .materialize import build_relation_result, materialize_relation_plans, update_stats
from .rules import correct_candidate_relation
from .types import PendingArbitration, RelationClassifyContext, RelationClassifyResult


def run(
    graph_bundle: GraphBundle,
    runtime: Any,
    candidates: list[ReferenceCandidateRecord],
    source_document_ids: set[str] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    model_progress_callback: Callable[[int, int], None] | None = None,
    llm_progress_callback: Callable[[int, int], None] | None = None,
    checkpoint_every: int = 0,
    checkpoint_callback: Callable[[list[RelationClassifyRecord], list[object], dict[str, Any], int, int, list[str]], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> RelationClassifyResult:
    context = build_classify_context(graph_bundle, source_document_ids)
    selected_candidates = select_candidates(candidates, context)
    stats = build_relation_stats(selected_candidates, context)
    total_candidates = max(len(selected_candidates), 1)

    emit_progress(progress_callback, 0, total_candidates)
    if not selected_candidates:
        return RelationClassifyResult(
            stats=stats,
            graph_bundle=materialize_relation_plans(graph_bundle, []),
        )

    policy = resolve_interprets_policy(runtime)
    results: list[RelationClassifyRecord] = []
    pending: list[PendingArbitration] = []
    llm_judgments = []
    completed_candidates = 0
    processed_source_ids: set[str] = set()

    def emit_checkpoint() -> None:
        if checkpoint_callback is None:
            return
        checkpoint_callback(
            list(results),
            list(llm_judgments),
            dict(stats),
            completed_candidates,
            total_candidates,
            sorted(processed_source_ids),
        )

    batches = iter_predict_interprets_batches(
        runtime,
        [candidate.text for candidate in selected_candidates],
        batch_size=int(policy["prediction_batch_size"]),
        cancel_event=cancel_event,
        progress_callback=model_progress_callback,
    )
    emit_progress(progress_callback, 0, total_candidates)

    for start_index, batch_predictions in batches:
        batch_candidates = selected_candidates[start_index : start_index + len(batch_predictions)]
        for candidate, prediction in zip(batch_candidates, batch_predictions):
            processed_source_ids.add(
                owner_source_id(context.owner_document_by_node.get(candidate.source_node_id, candidate.source_node_id))
            )
            source_category = candidate_source_category(candidate)
            target_categories = list(candidate.target_categories)
            document_id = context.owner_document_by_node.get(candidate.source_node_id, candidate.source_node_id)
            document_node = context.document_nodes.get(document_id)
            is_legislative = is_legislative_interpretation_document(document_node)
            predicted_relation = "INTERPRETS" if bool(getattr(prediction, "is_interprets", False)) else "REFERENCES"
            corrected_relation, corrected, correction_source = correct_candidate_relation(
                candidate,
                relation_type=predicted_relation,
                source_category=source_category,
                target_categories=target_categories,
            )

            if corrected:
                results.append(
                    build_relation_result(
                        candidate,
                        label=corrected_relation,
                        score=float(getattr(prediction, "score", 0.0)),
                        source=correction_source or "rule_corrected_model",
                    )
                )
                update_stats(
                    stats,
                    relation_type=corrected_relation,
                    decision_source=correction_source or "rule_corrected_model",
                    target_count=len(candidate.target_node_ids),
                    source_category=source_category,
                )
                completed_candidates += 1
                continue

            if is_prediction_low_confidence(prediction, policy=policy) and bool(policy["use_llm_for_uncertain"]):
                pending.append(
                    PendingArbitration(
                        candidate=candidate,
                        prediction=prediction,
                        source_category=source_category,
                        target_categories=target_categories,
                        is_legislative_interpretation=is_legislative,
                    )
                )
                continue

            score = float(getattr(prediction, "score", 0.0))
            if score >= float(policy["high_confidence_true_threshold"]):
                decision_source = "model_high_confidence"
            elif score <= float(policy["low_confidence_false_threshold"]):
                decision_source = "model_low_confidence"
            else:
                decision_source = "model_uncertain_fallback"
            results.append(
                build_relation_result(
                    candidate,
                    label=corrected_relation,
                    score=score,
                    source=decision_source,
                )
            )
            update_stats(
                stats,
                relation_type=corrected_relation,
                decision_source=decision_source,
                target_count=len(candidate.target_node_ids),
                source_category=source_category,
            )
            completed_candidates += 1
        emit_progress(progress_callback, completed_candidates, total_candidates)
        if checkpoint_every > 0 and completed_candidates > 0 and (
            completed_candidates % checkpoint_every == 0 or completed_candidates == total_candidates
        ):
            emit_checkpoint()

    if pending:
        arbitration = arbitrate_uncertain_candidates(
            runtime=runtime,
            pending=pending,
            cancel_event=cancel_event,
            progress_callback=llm_progress_callback,
            stats=stats,
            checkpoint_every=checkpoint_every,
            checkpoint_callback=lambda arbitration_results, arbitration_judgments, arbitration_completed: checkpoint_callback(
                list(results) + list(arbitration_results),
                list(llm_judgments) + list(arbitration_judgments),
                dict(stats),
                completed_candidates + arbitration_completed,
                total_candidates,
                sorted(processed_source_ids),
            )
            if checkpoint_callback is not None
            else None,
        )
        results.extend(arbitration.results)
        llm_judgments = arbitration.llm_judgments
        llm_errors = arbitration.llm_errors
        completed_candidates = total_candidates
    else:
        llm_errors = []

    stats["result_count"] = len(results)
    emit_progress(progress_callback, total_candidates, total_candidates)
    if checkpoint_every > 0:
        emit_checkpoint()
    return RelationClassifyResult(
        results=results,
        llm_judgments=llm_judgments,
        graph_bundle=materialize_relation_plans(graph_bundle, results),
        llm_errors=llm_errors,
        stats=stats,
    )

def build_classify_context(
    graph_bundle: GraphBundle,
    source_document_ids: set[str] | None,
) -> RelationClassifyContext:
    context = build_reference_graph_context(graph_bundle)
    return RelationClassifyContext(
        node_index=context.node_index,
        parent_by_child=context.parent_by_child,
        owner_document_by_node=context.owner_document_by_node,
        document_nodes=context.document_nodes,
        provision_index=context.provision_index,
        title_to_document_ids=context.title_to_document_ids,
        children_by_parent_level=context.children_by_parent_level,
        merged_document_aliases=context.merged_document_aliases,
        document_alias_groups=context.document_alias_groups,
        global_document_alias_groups=context.global_document_alias_groups,
        active_source_ids=None if source_document_ids is None else {value for value in source_document_ids if value},
    )


def select_candidates(
    candidates: list[ReferenceCandidateRecord],
    context: RelationClassifyContext,
) -> list[ReferenceCandidateRecord]:
    if context.active_source_ids is None:
        return list(candidates)
    return [
        candidate
        for candidate in candidates
        if owner_source_id(context.owner_document_by_node.get(candidate.source_node_id, candidate.source_node_id)) in context.active_source_ids
    ]


def build_relation_stats(
    candidates: list[ReferenceCandidateRecord],
    context: RelationClassifyContext,
) -> dict[str, Any]:
    source_categories = [candidate_source_category(candidate) for candidate in candidates]
    source_ids = {
        owner_source_id(context.owner_document_by_node.get(candidate.source_node_id, candidate.source_node_id))
        for candidate in candidates
    }
    source_category_counts: dict[str, int] = {}
    for source_category in source_categories:
        source_category_counts[source_category] = source_category_counts.get(source_category, 0) + 1
    succeeded_sources = len(context.active_source_ids) if context.active_source_ids is not None else len(source_ids)
    return {
        "source_count": succeeded_sources,
        "succeeded_sources": succeeded_sources,
        "failed_sources": 0,
        "reused_sources": 0,
        "work_units_total": len(candidates),
        "work_units_completed": len(candidates),
        "work_units_failed": 0,
        "work_units_skipped": 0,
        "candidate_count": len(candidates),
        "result_count": 0,
        "model_decision_count": 0,
        "llm_arbiter_count": 0,
        "rule_corrected_count": 0,
        "interprets_count": 0,
        "references_count": 0,
        "ordinary_reference_count": 0,
        "judicial_interprets_count": 0,
        "judicial_references_count": 0,
        "source_category_counts": dict(source_category_counts),
    }


def count_relation_classify_units(
    graph_bundle: GraphBundle,
    candidates: list[ReferenceCandidateRecord],
    source_document_ids: set[str] | None = None,
) -> int:
    context = build_classify_context(graph_bundle, source_document_ids)
    return len(select_candidates(candidates, context))


def emit_progress(
    callback: Callable[[int, int], None] | None,
    current: int,
    total: int,
) -> None:
    if callback is not None:
        callback(current, total)
