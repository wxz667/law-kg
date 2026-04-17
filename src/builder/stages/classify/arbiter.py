from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable

from ...contracts import LlmJudgeDetailRecord, ClassifyRecord
from .classify import build_llm_payload
from .llm import judge_relation_conflicts, resolve_llm_conflict_runtime_config
from .materialize import build_relation_result, update_stats
from .rules import correct_candidate_relation
from .types import PendingArbitration, ClassifyResult


def arbitrate_uncertain_candidates(
    *,
    runtime: Any,
    pending: list[PendingArbitration],
    cancel_event: threading.Event | None,
    progress_callback: Callable[[int, int], None] | None,
    stats: dict[str, Any],
    checkpoint_every: int = 0,
    checkpoint_callback: Callable[[list[ClassifyRecord], list[LlmJudgeDetailRecord], int], None] | None = None,
) -> ClassifyResult:
    emit_progress(progress_callback, 0, len(pending))
    results_by_start: dict[int, list[ClassifyRecord]] = {}
    llm_judgments_by_start: dict[int, list[LlmJudgeDetailRecord]] = {}
    llm_errors: list[dict[str, Any]] = []
    runtime_config = resolve_llm_conflict_runtime_config(runtime)
    step = runtime_config.batch_size
    completed = 0
    batches: list[tuple[int, list[PendingArbitration], list[dict[str, object]]]] = []

    for start in range(0, len(pending), step):
        chunk = pending[start : start + step]
        payloads = [
            build_llm_payload(
                sample_id=item.candidate.id,
                text=item.candidate.text,
                prediction=item.prediction,
                source_category=item.source_category,
                target_categories=item.target_categories,
                is_legislative_interpretation=item.is_legislative_interpretation,
            )
            for item in chunk
        ]
        batches.append((start, chunk, payloads))

    max_workers = max(1, min(runtime_config.concurrent_requests, len(batches) or 1))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {
            executor.submit(judge_llm_chunk, runtime, payloads): (start, chunk)
            for start, chunk, payloads in batches
        }
        for future in as_completed(future_to_batch):
            if cancel_event is not None and cancel_event.is_set():
                raise KeyboardInterrupt
            start, chunk = future_to_batch[future]
            decisions, batch_errors = future.result()
            batch_results, batch_judgments = build_chunk_outputs(chunk, decisions, stats)
            results_by_start[start] = batch_results
            llm_judgments_by_start[start] = batch_judgments
            llm_errors.extend(batch_errors)
            completed += len(chunk)
            emit_progress(progress_callback, completed, len(pending))
            if checkpoint_callback is not None and checkpoint_every > 0 and (
                completed % checkpoint_every == 0 or completed == len(pending)
            ):
                checkpoint_callback(
                    flatten_ordered_records(results_by_start),
                    flatten_ordered_records(llm_judgments_by_start),
                    completed,
                )

    return ClassifyResult(
        results=flatten_ordered_records(results_by_start),
        llm_judgments=flatten_ordered_records(llm_judgments_by_start),
        llm_errors=llm_errors,
        stats=stats,
    )


def judge_llm_chunk(
    runtime: Any,
    payloads: list[dict[str, object]],
) -> tuple[list[Any], list[dict[str, Any]]]:
    try:
        return list(judge_relation_conflicts(runtime, payloads)), []
    except Exception as exc:
        return [
            type(
                "FallbackDecision",
                (),
                {
                    "is_interprets": payload.get("model_is_interprets", False),
                    "score": payload.get("model_score", 0.0),
                    "model": "llm-error-fallback",
                    "reason": "LLM调用失败，回退为小模型低置信判定。",
                },
            )()
            for payload in payloads
        ], [{"error_type": exc.__class__.__name__, "message": str(exc), "payload_count": len(payloads)}]


def build_chunk_outputs(
    chunk: list[PendingArbitration],
    decisions: list[Any],
    stats: dict[str, Any],
) -> tuple[list[ClassifyRecord], list[LlmJudgeDetailRecord]]:
    results: list[ClassifyRecord] = []
    llm_judgments: list[LlmJudgeDetailRecord] = []
    for item, decision in zip(chunk, decisions):
        raw_relation = "INTERPRETS" if bool(getattr(decision, "is_interprets", False)) else "REFERENCES"
        relation_type, corrected, correction_source = correct_candidate_relation(
            item.candidate,
            relation_type=raw_relation,
            source_category=item.source_category,
            target_categories=item.target_categories,
        )
        decision_source = "llm_arbiter"
        if corrected:
            if correction_source == "rule_corrected_title_model":
                decision_source = "rule_corrected_title_llm"
            else:
                decision_source = "rule_corrected_llm"
        elif getattr(decision, "model", "") == "llm-error-fallback":
            decision_source = "model_uncertain_error_fallback"
        results.append(
            build_relation_result(
                item.candidate,
                label=relation_type,
                score=max(float(getattr(item.prediction, "score", 0.0)), float(getattr(decision, "score", 0.0))),
                source=decision_source,
            )
        )
        llm_judgments.append(
            LlmJudgeDetailRecord(
                id=item.candidate.id,
                source_id=item.candidate.source_node_id,
                text=item.candidate.text,
                label=relation_type,
                reason=str(getattr(decision, "reason", "")),
            )
        )
        update_stats(
            stats,
            relation_type=relation_type,
            decision_source=decision_source,
            target_count=len(item.candidate.target_node_ids),
            source_category=item.source_category,
        )
    return results, llm_judgments


def flatten_ordered_records(records_by_start: dict[int, list[Any]]) -> list[Any]:
    ordered: list[Any] = []
    for start in sorted(records_by_start):
        ordered.extend(records_by_start[start])
    return ordered


def emit_progress(
    callback: Callable[[int, int], None] | None,
    current: int,
    total: int,
) -> None:
    if callback is not None:
        callback(current, total)
