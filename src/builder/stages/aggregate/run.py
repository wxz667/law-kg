from __future__ import annotations

import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

from ...contracts import AggregateConceptRecord
from ...pipeline.handlers.graph import owner_document_by_node, owner_source_id_for_node
from ...pipeline.runtime import PipelineRuntime
from .concepts import aggregate_concept_stats
from .llm import aggregate_concepts_batch, resolve_aggregate_runtime_config
from .types import AggregateInputRecord, AggregateResult


def run(
    graph_bundle,
    runtime: PipelineRuntime,
    *,
    inputs: list[AggregateInputRecord] | None = None,
    active_source_ids: set[str] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    checkpoint_every: int = 0,
    checkpoint_callback: Callable[[list, dict[str, int], list[str], list[str], list[str], list[dict[str, object]]], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> AggregateResult:
    active_sources = {value for value in (active_source_ids or set()) if value}
    inputs = list(inputs or [])
    total_inputs = len(inputs)
    if progress_callback is not None:
        progress_callback(0, max(total_inputs, 1))
    if not inputs:
        if progress_callback is not None:
            progress_callback(1, 1)
        return AggregateResult(
            inputs=[],
            concepts=[],
            processed_source_ids=sorted(active_sources),
            processed_input_ids=[],
            successful_input_ids=[],
            stats=build_stats(
                inputs=[],
                concepts=[],
                total_requests=0,
                retry_count=0,
                llm_errors=[],
                failed_input_ids=[],
                completed_input_count=0,
            ),
            llm_errors=[],
        )

    runtime_config = resolve_aggregate_runtime_config(runtime)
    node_index = {node.id: node for node in graph_bundle.nodes}
    owners = owner_document_by_node(graph_bundle)
    batches = build_document_batches(
        inputs,
        owners=owners,
        node_index=node_index,
        batch_size=runtime_config.batch_size,
    )
    input_owner_source_ids = {row.id: owner_source_id_for_node(owners, row.id) for row in inputs}
    source_input_totals = Counter(input_owner_source_ids.values())
    source_ids_without_inputs = sorted(active_sources - set(source_input_totals))
    completed_per_source: Counter[str] = Counter()
    failed_input_ids: set[str] = set()
    attempted_input_ids: set[str] = set()
    successful_input_ids: set[str] = set()
    llm_errors: list[dict[str, object]] = []
    concepts_by_start: dict[int, list[AggregateConceptRecord]] = {}
    completed_inputs = 0
    total_requests = 0
    retry_count = 0
    next_checkpoint = max(int(checkpoint_every or 0), 0)

    max_workers = max(1, min(runtime_config.concurrent_requests, len(batches) or 1))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {
            executor.submit(aggregate_concepts_batch, runtime, batch["inputs"]): (start, batch)
            for start, batch in enumerate(batches)
        }
        for future in as_completed(future_to_batch):
            if cancel_event is not None and cancel_event.is_set():
                raise KeyboardInterrupt
            start, batch = future_to_batch[future]
            batch_result = future.result()
            concepts_by_start[start] = list(batch_result.concepts)
            llm_errors.extend(batch_result.errors)
            total_requests += int(batch_result.request_count)
            retry_count += int(batch_result.retry_count)
            batch_failed_ids = set(batch_result.failed_input_ids)
            failed_input_ids.update(batch_failed_ids)
            for row in batch["inputs"]:
                attempted_input_ids.add(row.id)
                source_id = input_owner_source_ids.get(row.id, "")
                completed_per_source[source_id] += 1
                if row.id not in batch_failed_ids:
                    successful_input_ids.add(row.id)
            completed_inputs += len(batch["inputs"])
            if progress_callback is not None:
                progress_callback(completed_inputs, max(total_inputs, 1))
            should_checkpoint = False
            if checkpoint_callback is not None and checkpoint_every > 0:
                if completed_inputs >= total_inputs:
                    should_checkpoint = True
                elif next_checkpoint > 0 and completed_inputs >= next_checkpoint:
                    should_checkpoint = True
                    while next_checkpoint > 0 and completed_inputs >= next_checkpoint:
                        next_checkpoint += checkpoint_every
            if should_checkpoint:
                snapshot_concepts = _materialized_concepts(concepts_by_start)
                checkpoint_callback(
                    snapshot_concepts,
                    build_stats(
                        inputs=inputs,
                        concepts=snapshot_concepts,
                        total_requests=total_requests,
                        retry_count=retry_count,
                        llm_errors=llm_errors,
                        failed_input_ids=sorted(failed_input_ids),
                        completed_input_count=completed_inputs,
                    ),
                    _completed_source_ids(
                        source_input_totals,
                        completed_per_source,
                        failed_input_ids,
                        input_owner_source_ids,
                        source_ids_without_inputs,
                    ),
                    sorted(attempted_input_ids),
                    sorted(successful_input_ids),
                    summarize_llm_errors(llm_errors),
                )

    concepts = _materialized_concepts(concepts_by_start)
    processed_source_ids = _completed_source_ids(
        source_input_totals,
        completed_per_source,
        failed_input_ids,
        input_owner_source_ids,
        source_ids_without_inputs,
    )
    return AggregateResult(
        inputs=inputs,
        concepts=concepts,
        processed_source_ids=processed_source_ids,
        processed_input_ids=sorted(attempted_input_ids),
        successful_input_ids=sorted(successful_input_ids),
        stats=build_stats(
            inputs=inputs,
            concepts=concepts,
            total_requests=total_requests,
            retry_count=retry_count,
            llm_errors=llm_errors,
            failed_input_ids=sorted(failed_input_ids),
            completed_input_count=total_inputs,
        ),
        llm_errors=summarize_llm_errors(llm_errors),
    )


def build_stats(
    *,
    inputs: list,
    concepts: list[AggregateConceptRecord],
    total_requests: int,
    retry_count: int,
    llm_errors: list[dict[str, object]],
    failed_input_ids: list[str],
    completed_input_count: int | None = None,
) -> dict[str, int]:
    failed_count = len(failed_input_ids)
    observed_completed = len(inputs) if completed_input_count is None else int(completed_input_count)
    succeeded_count = max(observed_completed - failed_count, 0)
    concept_stats = aggregate_concept_stats(concepts)
    return {
        "work_units_total": len(inputs),
        "work_units_completed": succeeded_count,
        "work_units_failed": failed_count,
        "work_units_skipped": 0,
        "work_units_attempted": int(observed_completed),
        "input_count": len(inputs),
        **concept_stats,
        "llm_request_count": int(total_requests),
        "llm_error_count": len(llm_errors),
        "retry_count": int(retry_count),
    }


def build_document_batches(
    inputs: list[AggregateInputRecord],
    *,
    owners: dict[str, str],
    node_index: dict[str, object],
    batch_size: int,
) -> list[dict[str, object]]:
    batches: list[dict[str, object]] = []
    grouped: dict[str, list[AggregateInputRecord]] = {}
    document_order: list[str] = []
    for row in inputs:
        document_id = owners.get(row.id, row.id)
        if document_id not in grouped:
            grouped[document_id] = []
            document_order.append(document_id)
        grouped[document_id].append(row)
    for document_id in document_order:
        document_title = str(getattr(node_index.get(document_id), "name", "")).strip()
        rows = grouped.get(document_id, [])
        for start in range(0, len(rows), max(batch_size, 1)):
            batches.append(
                {
                    "document_id": document_id,
                    "document_title": document_title,
                    "inputs": rows[start : start + max(batch_size, 1)],
                }
            )
    return batches


def _flatten_concepts(concepts_by_start: dict[int, list[AggregateConceptRecord]]) -> list[AggregateConceptRecord]:
    ordered: list[AggregateConceptRecord] = []
    for start in sorted(concepts_by_start):
        ordered.extend(concepts_by_start[start])
    return ordered


def _materialized_concepts(concepts_by_start: dict[int, list[AggregateConceptRecord]]) -> list[AggregateConceptRecord]:
    return [row for row in _flatten_concepts(concepts_by_start) if str(getattr(row, "id", "")).strip()]


def _completed_source_ids(
    source_input_totals: Counter[str],
    completed_per_source: Counter[str],
    failed_input_ids: set[str],
    input_owner_source_ids: dict[str, str],
    source_ids_without_inputs: list[str],
) -> list[str]:
    failed_sources = {input_owner_source_ids.get(node_id, "") for node_id in failed_input_ids}
    completed = {
        source_id
        for source_id, total in source_input_totals.items()
        if source_id and completed_per_source.get(source_id, 0) >= total and source_id not in failed_sources
    }
    completed.update(source_id for source_id in source_ids_without_inputs if source_id)
    return sorted(completed)


def summarize_llm_errors(errors: list[dict[str, object]], *, limit: int = 10) -> list[dict[str, object]]:
    summarized: dict[tuple[str, str], dict[str, object]] = {}
    for item in errors:
        error_type = str(item.get("error_type", "") or "")
        message = str(item.get("message", "") or "")
        key = (error_type, message)
        entry = summarized.get(key)
        if entry is None:
            sample_ids = [str(value) for value in item.get("input_ids", [])[:3]]
            entry = {
                "error_type": error_type,
                "message": message,
                "count": 0,
                "max_attempt": 0,
                "sample_input_ids": sample_ids,
            }
            summarized[key] = entry
        entry["count"] = int(entry.get("count", 0)) + 1
        entry["max_attempt"] = max(int(entry.get("max_attempt", 0)), int(item.get("attempt", 0) or 0))
    ordered = sorted(
        summarized.values(),
        key=lambda value: (-int(value.get("count", 0)), str(value.get("error_type", "")), str(value.get("message", ""))),
    )
    return ordered[:limit]
