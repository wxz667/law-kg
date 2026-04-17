from __future__ import annotations

import threading
from typing import Any, Callable

from ...contracts import ClassifyPendingRecord, GraphBundle, ReferenceCandidateRecord, ClassifyRecord
from ...utils.legal_reference import candidate_source_category, is_legislative_interpretation_document
from ...utils.locator import owner_source_id
from ...utils.reference_graph import build_reference_graph_context
from .arbiter import arbitrate_uncertain_candidates
from .classify import (
    iter_predict_interprets_batches,
    is_prediction_low_confidence,
    resolve_interprets_policy,
)
from .materialize import build_relation_result, materialize_classify_results, update_stats
from .rules import correct_candidate_relation
from .types import ClassifyContext, ClassifyModelResult, ClassifyResult, PendingArbitration


def run(
    graph_bundle: GraphBundle,
    runtime: Any,
    candidates: list[ReferenceCandidateRecord],
    source_document_ids: set[str] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    model_progress_callback: Callable[[int, int], None] | None = None,
    llm_progress_callback: Callable[[int, int], None] | None = None,
    checkpoint_every: int = 0,
    checkpoint_callback: Callable[[list[ClassifyRecord], list[object], dict[str, Any], int, int, list[str]], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> ClassifyResult:
    context = build_classify_context(graph_bundle, source_document_ids)
    selected_candidates = select_candidates(candidates, context)
    total_candidates = max(len(selected_candidates), 1)
    emit_progress(progress_callback, 0, total_candidates)
    if not selected_candidates:
        stats = build_relation_stats(selected_candidates, context)
        return ClassifyResult(
            stats=stats,
            graph_bundle=materialize_classify_results(graph_bundle, []),
        )

    model_result = run_model_phase(
        graph_bundle,
        runtime,
        candidates,
        source_document_ids=source_document_ids,
        progress_callback=model_progress_callback,
        checkpoint_every=checkpoint_every,
        checkpoint_callback=None,
        cancel_event=cancel_event,
    )
    emit_progress(progress_callback, len(model_result.processed_candidate_ids), total_candidates)

    llm_result = run_llm_phase(
        runtime=runtime,
        pending_records=model_result.pending_records,
        stats=dict(model_result.stats),
        progress_callback=llm_progress_callback,
        checkpoint_every=checkpoint_every,
        checkpoint_callback=None,
        cancel_event=cancel_event,
    )
    final_results = list(model_result.results) + list(llm_result.results)
    final_stats = dict(llm_result.stats)
    final_stats["result_count"] = len(final_results)
    emit_progress(progress_callback, total_candidates, total_candidates)
    if checkpoint_callback is not None:
        checkpoint_callback(
            final_results,
            list(llm_result.llm_judgments),
            dict(final_stats),
            total_candidates,
            total_candidates,
            list(model_result.processed_source_ids),
        )
    return ClassifyResult(
        results=final_results,
        llm_judgments=llm_result.llm_judgments,
        graph_bundle=materialize_classify_results(graph_bundle, final_results),
        llm_errors=llm_result.llm_errors,
        stats=final_stats,
    )


def run_model_phase(
    graph_bundle: GraphBundle,
    runtime: Any,
    candidates: list[ReferenceCandidateRecord],
    source_document_ids: set[str] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    checkpoint_every: int = 0,
    checkpoint_callback: Callable[[list[ClassifyRecord], list[ClassifyPendingRecord], dict[str, Any], int, int, list[str], list[str]], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> ClassifyModelResult:
    context = build_classify_context(graph_bundle, source_document_ids)
    selected_candidates = select_candidates(candidates, context)
    stats = build_relation_stats(selected_candidates, context)
    total_candidates = max(len(selected_candidates), 1)
    emit_progress(progress_callback, 0, total_candidates)
    if not selected_candidates:
        return ClassifyModelResult(stats=stats)

    policy = resolve_interprets_policy(runtime)
    results: list[ClassifyRecord] = []
    pending: list[PendingArbitration] = []
    completed_candidates = 0
    processed_source_ids: set[str] = set()
    processed_candidate_ids: list[str] = []

    def emit_checkpoint() -> None:
        if checkpoint_callback is None:
            return
        checkpoint_callback(
            list(results),
            serialize_pending_records(pending),
            dict(stats),
            completed_candidates,
            total_candidates,
            sorted(processed_source_ids),
            list(processed_candidate_ids),
        )

    batches = iter_predict_interprets_batches(
        runtime,
        [candidate.text for candidate in selected_candidates],
        batch_size=int(policy["prediction_batch_size"]),
        cancel_event=cancel_event,
        progress_callback=progress_callback,
    )

    for start_index, batch_predictions in batches:
        batch_candidates = selected_candidates[start_index : start_index + len(batch_predictions)]
        for candidate, prediction in zip(batch_candidates, batch_predictions):
            processed_candidate_ids.append(candidate.id)
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
                completed_candidates += 1
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
        if checkpoint_every > 0 and completed_candidates > 0 and (
            completed_candidates % checkpoint_every == 0 or completed_candidates == len(selected_candidates)
        ):
            emit_checkpoint()

    stats["result_count"] = len(results)
    return ClassifyModelResult(
        results=results,
        pending_records=serialize_pending_records(pending),
        processed_source_ids=sorted(processed_source_ids),
        processed_candidate_ids=sorted(dict.fromkeys(processed_candidate_ids)),
        stats=stats,
    )


def run_llm_phase(
    *,
    runtime: Any,
    pending_records: list[ClassifyPendingRecord],
    stats: dict[str, Any],
    progress_callback: Callable[[int, int], None] | None = None,
    checkpoint_every: int = 0,
    checkpoint_callback: Callable[[list[ClassifyRecord], list[object], int], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> ClassifyResult:
    pending = deserialize_pending_records(pending_records)
    if not pending:
        emit_progress(progress_callback, 1, 1)
        stats = dict(stats)
        stats["result_count"] = int(stats.get("result_count", 0))
        return ClassifyResult(results=[], llm_judgments=[], llm_errors=[], stats=stats)
    arbitration = arbitrate_uncertain_candidates(
        runtime=runtime,
        pending=pending,
        cancel_event=cancel_event,
        progress_callback=progress_callback,
        stats=dict(stats),
        checkpoint_every=checkpoint_every,
        checkpoint_callback=checkpoint_callback,
    )
    arbitration.stats["result_count"] = int(arbitration.stats.get("result_count", 0))
    return arbitration


def serialize_pending_records(pending: list[PendingArbitration]) -> list[ClassifyPendingRecord]:
    return [
        ClassifyPendingRecord(
            id=item.candidate.id,
            source_node_id=item.candidate.source_node_id,
            text=item.candidate.text,
            target_node_ids=list(item.candidate.target_node_ids),
            target_categories=list(item.candidate.target_categories),
            source_category=item.source_category,
            prediction_is_interprets=bool(getattr(item.prediction, "is_interprets", False)),
            prediction_score=float(getattr(item.prediction, "score", 0.0)),
            is_legislative_interpretation=item.is_legislative_interpretation,
        )
        for item in pending
    ]


def deserialize_pending_records(pending_records: list[ClassifyPendingRecord]) -> list[PendingArbitration]:
    pending: list[PendingArbitration] = []
    for row in pending_records:
        candidate = ReferenceCandidateRecord(
            id=row.id,
            source_node_id=row.source_node_id,
            text=row.text,
            target_node_ids=list(row.target_node_ids),
            target_categories=list(row.target_categories),
        )
        prediction = type(
            "PendingPrediction",
            (),
            {
                "is_interprets": row.prediction_is_interprets,
                "score": row.prediction_score,
            },
        )()
        pending.append(
            PendingArbitration(
                candidate=candidate,
                prediction=prediction,
                source_category=row.source_category,
                target_categories=list(row.target_categories),
                is_legislative_interpretation=row.is_legislative_interpretation,
            )
        )
    return pending


def build_classify_context(
    graph_bundle: GraphBundle,
    source_document_ids: set[str] | None,
) -> ClassifyContext:
    context = build_reference_graph_context(graph_bundle)
    return ClassifyContext(
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
    context: ClassifyContext,
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
    context: ClassifyContext,
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


def count_classify_units(
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
