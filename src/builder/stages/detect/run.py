from __future__ import annotations

import threading
from collections import Counter
from typing import Any, Callable

from ...contracts import GraphBundle, ReferenceCandidateRecord
from ...utils.legal_reference import (
    is_excluded_reference_document,
    is_judicial_interpretation_document,
    normalize_reference_category,
    should_scan_title_candidates,
)
from ...utils.locator import owner_source_id
from ...utils.reference_graph import build_reference_graph_context
from .scan import resolve_document_worker_count, scan_documents
from .types import DetectContext, DetectProfiling, DetectResult

PROVISION_LEVELS = {"article", "paragraph", "item", "sub_item", "segment"}


def run(
    graph_bundle: GraphBundle,
    runtime: Any,
    source_document_ids: set[str] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    checkpoint_every: int = 0,
    checkpoint_callback: Callable[[list[ReferenceCandidateRecord], dict[str, Any], dict[str, Any], list[str]], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> DetectResult:
    context = build_detect_context(graph_bundle)
    active_source_ids = None if source_document_ids is None else {value for value in source_document_ids if value}
    document_items = collect_document_items(graph_bundle, context, active_source_ids)
    document_items.sort(
        key=lambda item: (
            int(is_judicial_interpretation_document(context.document_nodes.get(item[0]))),
            len(item[1]),
            item[0],
        ),
        reverse=True,
    )
    total_units = max(count_document_items_units(document_items, context), 1)
    emit_progress(progress_callback, 0, total_units)

    progress_lock = threading.Lock()
    processed_units = 0
    checkpoint_lock = threading.Lock()
    processed_documents = 0
    processed_document_ids: set[str] = set()
    checkpoint_every = int(checkpoint_every or runtime.detect_config().get("checkpoint_every", 50) or 50)
    accumulated_candidates: list[ReferenceCandidateRecord] = []
    accumulated_stats_counter: Counter[str] = Counter()
    accumulated_profiling = DetectProfiling()

    def advance_progress(delta: int) -> None:
        nonlocal processed_units
        if delta <= 0:
            return
        if cancel_event is not None and cancel_event.is_set():
            return
        with progress_lock:
            processed_units += delta
            current = min(processed_units, total_units)
        emit_progress(progress_callback, current, total_units)

    def handle_document_result(_document_id: str, document_result: Any) -> None:
        nonlocal processed_documents
        with checkpoint_lock:
            processed_document_ids.add(_document_id)
            accumulated_candidates.extend(document_result.candidates)
            accumulated_stats_counter.update(
                {
                    "source_units": document_result.source_units,
                    "sentences_scanned": document_result.sentences_scanned,
                    "raw_candidates": document_result.raw_candidates,
                    "resolved_targets": document_result.resolved_targets,
                    "dropped_quoted": document_result.dropped_quoted,
                    "dropped_special_targets": document_result.dropped_special_targets,
                    "dropped_meaningless_self": document_result.dropped_meaningless_self,
                }
            )
            accumulated_profiling.merge(document_result.profiling)
            processed_documents += 1
            should_checkpoint = checkpoint_callback is not None and (
                processed_documents % max(checkpoint_every, 1) == 0
                or processed_documents == len(document_items)
            )
            if not should_checkpoint:
                return
            snapshot_candidates = sorted(
                list(accumulated_candidates),
                key=lambda row: (row.source_node_id, row.text, tuple(row.target_node_ids)),
            )
            snapshot_stats = build_detect_stats(
                candidates=snapshot_candidates,
                document_items=document_items,
                active_source_ids=active_source_ids,
                context=context,
                stats_counter=Counter(accumulated_stats_counter),
            )
            snapshot_profiling = accumulated_profiling.to_dict()
        checkpoint_callback(snapshot_candidates, snapshot_stats, snapshot_profiling, sorted(processed_document_ids))

    worker_count = resolve_document_worker_count(runtime, len(document_items))
    results = scan_documents(
        context=context,
        document_items=document_items,
        worker_count=worker_count,
        progress_callback=advance_progress,
        result_callback=handle_document_result,
        cancel_event=cancel_event,
    )
    merged_candidates, stats_counter, profiling = merge_scan_results(results)
    stats = build_detect_stats(
        candidates=merged_candidates,
        document_items=document_items,
        active_source_ids=active_source_ids,
        context=context,
        stats_counter=stats_counter,
    )
    emit_progress(progress_callback, total_units, total_units)
    return DetectResult(candidates=merged_candidates, stats=stats, profiling=profiling.to_dict())


def build_detect_context(graph_bundle: GraphBundle) -> DetectContext:
    context = build_reference_graph_context(graph_bundle)
    document_nodes = context.document_nodes
    return DetectContext(
        node_index=context.node_index,
        parent_by_child=context.parent_by_child,
        owner_document_by_node=context.owner_document_by_node,
        document_nodes=document_nodes,
        provision_index=context.provision_index,
        title_to_document_ids=context.title_to_document_ids,
        children_by_parent_level=context.children_by_parent_level,
        merged_document_aliases=context.merged_document_aliases,
        document_alias_groups=context.document_alias_groups,
        global_document_alias_groups=context.global_document_alias_groups,
        special_document_ids={
            document_id
            for document_id, node in document_nodes.items()
            if is_excluded_reference_document(node)
        },
        category_by_document_id={
            document_id: normalize_reference_category(node)
            for document_id, node in document_nodes.items()
            if not is_excluded_reference_document(node)
        },
    )


def collect_document_items(
    graph_bundle: GraphBundle,
    context: DetectContext,
    active_source_ids: set[str] | None,
) -> list[tuple[str, list[Any]]]:
    candidate_nodes = [
        node
        for node in graph_bundle.nodes
        if node.level in PROVISION_LEVELS
        and node.text
        and (
            active_source_ids is None
            or owner_source_id(context.owner_document_by_node.get(node.id, "")) in active_source_ids
        )
    ]
    document_to_nodes: dict[str, list[Any]] = {}
    for node in candidate_nodes:
        document_to_nodes.setdefault(context.owner_document_by_node.get(node.id, ""), []).append(node)
    return [(document_id, nodes) for document_id, nodes in document_to_nodes.items() if document_id]


def merge_scan_results(
    results: list[Any],
) -> tuple[list[ReferenceCandidateRecord], Counter[str], DetectProfiling]:
    profiling = DetectProfiling()
    merged_candidates: list[ReferenceCandidateRecord] = []
    stats_counter: Counter[str] = Counter()
    for result in results:
        merged_candidates.extend(result.candidates)
        stats_counter.update(
            {
                "source_units": result.source_units,
                "sentences_scanned": result.sentences_scanned,
                "raw_candidates": result.raw_candidates,
                "resolved_targets": result.resolved_targets,
                "dropped_quoted": result.dropped_quoted,
                "dropped_special_targets": result.dropped_special_targets,
                "dropped_meaningless_self": result.dropped_meaningless_self,
            }
        )
        profiling.merge(result.profiling)
    merged_candidates.sort(key=lambda row: (row.source_node_id, row.text, tuple(row.target_node_ids)))
    return merged_candidates, stats_counter, profiling


def count_detect_units(
    graph_bundle: GraphBundle,
    source_document_ids: set[str] | None = None,
) -> int:
    context = build_detect_context(graph_bundle)
    document_items = collect_document_items(
        graph_bundle,
        context,
        None if source_document_ids is None else {value for value in source_document_ids if value},
    )
    return count_document_items_units(document_items, context)


def count_document_items_units(
    document_items: list[tuple[str, list[Any]]],
    context: DetectContext,
) -> int:
    return sum(
        len(nodes) + int(should_scan_title_candidates(context.document_nodes.get(document_id)))
        for document_id, nodes in document_items
    )


def build_detect_stats(
    *,
    candidates: list[ReferenceCandidateRecord],
    document_items: list[tuple[str, list[Any]]],
    active_source_ids: set[str] | None,
    context: DetectContext,
    stats_counter: Counter[str],
) -> dict[str, int]:
    source_total = len(active_source_ids) if active_source_ids is not None else len(document_items)
    work_units = int(stats_counter.get("source_units", 0))
    return {
        "source_count": source_total,
        "succeeded_sources": source_total,
        "failed_sources": 0,
        "reused_sources": 0,
        "work_units_total": work_units,
        "work_units_completed": work_units,
        "work_units_failed": 0,
        "work_units_skipped": 0,
        "candidate_count": len(candidates),
        "documents_with_candidates": len(
            {
                owner_source_id(context.owner_document_by_node.get(row.source_node_id, row.source_node_id))
                for row in candidates
            }
        ),
        **{key: int(value) for key, value in stats_counter.items()},
    }


def emit_progress(
    progress_callback: Callable[[int, int], None] | None,
    current: int,
    total: int,
) -> None:
    if progress_callback is not None:
        progress_callback(current, total)
