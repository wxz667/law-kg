from __future__ import annotations

import os
import re
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import Any, Callable
from uuid import uuid4

from ...contracts import ReferenceCandidateRecord
from ...utils.reference import (
    candidate_source_prefix,
    document_title,
    is_judicial_interpretation_document,
    is_legislative_interpretation_document,
    should_scan_title_candidates,
    should_use_title_candidates_exclusively,
)
from .extract import (
    build_quote_ranges,
    extract_candidates,
    relevant_alias_items,
    sentence_may_contain_reference,
    split_sentences,
    span_in_quotes,
)
from .evidence import build_augmented_evidence_text
from .marking import extract_target_text, has_single_target_marker, mark_target_text
from .patterns import REFERENCE_ANCHOR_RE, REFERENCE_START_RE
from .resolve import resolve_candidates
from .rules import (
    is_meaningless_self_reference,
    should_skip_ordinary_reference_sentence,
)
from .types import DocumentScanResult, DetectContext


def scan_documents(
    *,
    context: DetectContext,
    document_items: list[tuple[str, list[Any]]],
    worker_count: int,
    progress_callback: Callable[[int], None] | None,
    result_callback: Callable[[str, DocumentScanResult], None] | None,
    cancel_event: threading.Event | None,
) -> list[DocumentScanResult]:
    if worker_count <= 1:
        results: list[DocumentScanResult] = []
        for document_id, nodes in document_items:
            if cancel_event is not None and cancel_event.is_set():
                raise KeyboardInterrupt
            document_result = process_document_candidates(
                context=context,
                document_id=document_id,
                nodes=nodes,
                progress_callback=progress_callback,
                cancel_event=cancel_event,
            )
            results.append(document_result)
            if result_callback is not None:
                result_callback(document_id, document_result)
        return results

    executor = ThreadPoolExecutor(max_workers=worker_count)
    futures: dict[Future[DocumentScanResult], str] = {}
    try:
        for document_id, nodes in document_items:
            future = executor.submit(
                process_document_candidates,
                context=context,
                document_id=document_id,
                nodes=nodes,
                progress_callback=progress_callback,
                cancel_event=cancel_event,
            )
            futures[future] = document_id

        results: list[DocumentScanResult] = []
        pending = set(futures)
        while pending:
            if cancel_event is not None and cancel_event.is_set():
                for future in pending:
                    future.cancel()
                raise KeyboardInterrupt
            done, pending = wait(pending, timeout=0.1, return_when=FIRST_COMPLETED)
            for future in done:
                document_id = futures[future]
                document_result = future.result()
                results.append(document_result)
                if result_callback is not None:
                    result_callback(document_id, document_result)
        return results
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def process_document_candidates(
    *,
    context: DetectContext,
    document_id: str,
    nodes: list[Any],
    progress_callback: Callable[[int], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> DocumentScanResult:
    result = DocumentScanResult()
    current_document_node = context.document_nodes.get(document_id)
    if current_document_node is None:
        if progress_callback is not None:
            progress_callback(len(nodes))
        return result
    if document_id in context.special_document_ids:
        if progress_callback is not None:
            progress_callback(len(nodes))
        return result

    aliases = context.merged_document_aliases.get(document_id, {})
    local_alias_groups = context.document_alias_groups.get(document_id, {})
    current_document_title = document_title(current_document_node)
    grouped: dict[tuple[str, str], list[str]] = {}
    grouped_target_categories: dict[tuple[str, str], list[str]] = {}
    judicial_document = is_judicial_interpretation_document(current_document_node)
    legislative_document = is_legislative_interpretation_document(current_document_node)
    title_only_document = should_use_title_candidates_exclusively(current_document_node)

    title_candidates_found = False
    if should_scan_title_candidates(current_document_node):
        title_text = str(getattr(current_document_node, "name", "") or "").strip()
        if title_text:
            before = len(grouped)
            process_text_unit(
                context=context,
                text=title_text,
                source_node_id=document_id,
                current_document_id=document_id,
                current_document_title=current_document_title,
                aliases=aliases,
                local_alias_groups=local_alias_groups,
                global_document_alias_groups=context.global_document_alias_groups,
                grouped=grouped,
                grouped_target_categories=grouped_target_categories,
                result=result,
                cancel_event=cancel_event,
                allow_sentence_skip=False,
            )
            title_candidates_found = len(grouped) > before
        result.source_units += 1
        if progress_callback is not None:
            progress_callback(1)

    if title_only_document and title_candidates_found:
        if progress_callback is not None:
            progress_callback(len(nodes))
        source_type = candidate_source_prefix(current_document_node)
        result.candidates = build_candidate_records(
            grouped=grouped,
            grouped_target_categories=grouped_target_categories,
            source_type=source_type,
        )
        return result

    for node in nodes:
        if cancel_event is not None and cancel_event.is_set():
            break
        node_text = str(getattr(node, "text", "") or "").strip()
        if not node_text:
            result.source_units += 1
            if progress_callback is not None:
                progress_callback(1)
            continue
        if not sentence_may_contain_reference(node_text):
            result.source_units += 1
            if progress_callback is not None:
                progress_callback(1)
            continue
        for sentence in split_sentences(node_text):
            if cancel_event is not None and cancel_event.is_set():
                break
            process_text_unit(
                context=context,
                text=sentence,
                source_node_id=node.id,
                current_document_id=document_id,
                current_document_title=current_document_title,
                aliases=aliases,
                local_alias_groups=local_alias_groups,
                global_document_alias_groups=context.global_document_alias_groups,
                grouped=grouped,
                grouped_target_categories=grouped_target_categories,
                result=result,
                cancel_event=cancel_event,
                allow_sentence_skip=not (judicial_document or legislative_document),
            )
        result.source_units += 1
        if progress_callback is not None:
            progress_callback(1)

    source_type = candidate_source_prefix(current_document_node)
    result.candidates = build_candidate_records(
        grouped=grouped,
        grouped_target_categories=grouped_target_categories,
        source_type=source_type,
    )
    return result


def build_candidate_records(
    *,
    grouped: dict[tuple[str, str], list[str]],
    grouped_target_categories: dict[tuple[str, str], list[str]],
    source_type: str,
) -> list[ReferenceCandidateRecord]:
    rows: list[ReferenceCandidateRecord] = []
    for source_key in sorted(grouped):
        target_ids = grouped[source_key]
        if not target_ids:
            continue
        rows.append(
            ReferenceCandidateRecord(
                id=f"{source_type}:{uuid4().hex}",
                source_node_id=source_key[0],
                text=source_key[1],
                target_node_ids=list(target_ids),
                target_categories=list(grouped_target_categories.get(source_key, [])),
            )
        )
    return rows


def process_text_unit(
    *,
    context: DetectContext,
    text: str,
    source_node_id: str,
    current_document_id: str,
    current_document_title: str,
    aliases: dict[str, str],
    local_alias_groups: dict[str, list[tuple[str, str]]],
    global_document_alias_groups: dict[str, list[tuple[str, str]]],
    grouped: dict[tuple[str, str], list[str]],
    grouped_target_categories: dict[tuple[str, str], list[str]],
    result: DocumentScanResult,
    cancel_event: threading.Event | None,
    allow_sentence_skip: bool,
) -> None:
    sentence = str(text or "").strip()
    if len(sentence) < 2:
        return
    result.sentences_scanned += 1
    if allow_sentence_skip and should_skip_ordinary_reference_sentence(sentence):
        return
    if not sentence_may_contain_reference(sentence):
        return
    quote_ranges = build_quote_ranges(sentence)
    alias_items = relevant_alias_items(
        sentence,
        local_groups=local_alias_groups,
        global_groups=global_document_alias_groups,
    )
    extract_started = time.perf_counter()
    candidates = extract_candidates(
        sentence,
        source_node_id,
        current_document_title,
        aliases=aliases,
        alias_items=alias_items,
        evidence_text=sentence,
        quote_ranges=quote_ranges,
    )
    result.profiling.extract_seconds += time.perf_counter() - extract_started
    if not candidates:
        return

    augmented_evidence = build_augmented_evidence_text(
        sentence,
        source_node_id,
        context.node_index,
        context.parent_by_child,
    )
    evidence_text = augmented_evidence.text
    result.raw_candidates += len(candidates)
    for candidate in candidates:
        result.profiling.bump_kind(candidate.kind)

    resolve_started = time.perf_counter()
    resolved = resolve_candidates(
        candidates,
        node_index=context.node_index,
        parent_by_child=context.parent_by_child,
        provision_index=context.provision_index,
        title_to_document_ids=context.title_to_document_ids,
        children_by_parent_level=context.children_by_parent_level,
        current_document_id=current_document_id,
    )
    result.profiling.resolve_seconds += time.perf_counter() - resolve_started
    candidate_group_spans = build_candidate_group_spans(candidates, evidence_text, prefix_offset=augmented_evidence.offset)
    resolved_group_spans = build_resolved_group_spans(resolved, prefix_offset=augmented_evidence.offset)

    for item in resolved:
        if cancel_event is not None and cancel_event.is_set():
            return
        if not item.target_node_id:
            continue
        if span_in_quotes(item.target_span_start, item.target_span_end, quote_ranges):
            result.dropped_quoted += 1
            continue
        target_document_id = context.owner_document_by_node.get(item.target_node_id, "")
        if target_document_id in context.special_document_ids:
            result.dropped_special_targets += 1
            continue
        if is_meaningless_self_reference(item.target_ref_text):
            result.dropped_meaningless_self += 1
            continue
        mark_start, mark_end = resolve_target_mark_span(
            item,
            evidence_text=evidence_text,
            candidate_group_spans=candidate_group_spans,
            grouped_spans=resolved_group_spans,
            prefix_offset=augmented_evidence.offset,
        )
        mark_started = time.perf_counter()
        marked_text = mark_target_text(evidence_text, mark_start, mark_end, fallback_to_full_text=False)
        result.profiling.mark_seconds += time.perf_counter() - mark_started
        if not has_single_target_marker(marked_text):
            continue
        target_payload = extract_target_text(marked_text)
        if not is_reference_anchor_text(target_payload):
            continue
        target_ids = grouped.setdefault((source_node_id, marked_text), [])
        if item.target_node_id not in target_ids:
            target_ids.append(item.target_node_id)
        target_categories = grouped_target_categories.setdefault((source_node_id, marked_text), [])
        target_category = target_category_for_node_id(item.target_node_id, context)
        if target_category and target_category not in target_categories:
            target_categories.append(target_category)
        result.resolved_targets += 1


def resolve_document_worker_count(runtime: Any, document_count: int) -> int:
    config_loader = getattr(runtime, "detect_config", None)
    filter_config = config_loader() if callable(config_loader) else {}
    configured = int(filter_config.get("document_workers", 0) or 0)
    if configured > 0:
        return min(max(configured, 1), max(document_count, 1))
    cpu_total = os.cpu_count() or 1
    return min(max(1, cpu_total - 1), max(document_count, 1), 12)


def resolve_target_mark_span(
    item: Any,
    *,
    evidence_text: str,
    candidate_group_spans: dict[int, tuple[int, int]] | None = None,
    grouped_spans: dict[int, tuple[int, int]] | None = None,
    prefix_offset: int = 0,
) -> tuple[int, int]:
    start = int(getattr(item, "target_span_start", -1)) + prefix_offset
    end = int(getattr(item, "target_span_end", -1)) + prefix_offset
    candidate = getattr(item, "candidate", None)
    candidate_start = -1
    candidate_end = -1
    if candidate is not None:
        grouped_candidate = (candidate_group_spans or {}).get(id(candidate))
        if grouped_candidate is not None and is_precise_candidate_span(evidence_text, grouped_candidate[0], grouped_candidate[1]):
            return grouped_candidate
    if candidate is not None:
        candidate_start = int(getattr(candidate, "span_start", -1)) + prefix_offset
        candidate_end = int(getattr(candidate, "span_end", -1)) + prefix_offset
        if is_precise_candidate_span(evidence_text, candidate_start, candidate_end):
            return candidate_start, candidate_end
    if candidate is not None:
        grouped = (grouped_spans or {}).get(id(candidate))
        if grouped is not None and is_valid_reference_span(evidence_text, grouped[0], grouped[1]):
            return grouped
    if is_valid_reference_span(evidence_text, start, end):
        return start, end
    if candidate is not None:
        if is_valid_reference_span(evidence_text, candidate_start, candidate_end):
            return candidate_start, candidate_end
        rebuilt = rebuild_candidate_span(evidence_text, candidate, prefix_offset=prefix_offset)
        if rebuilt is not None:
            return rebuilt
    return start, end


def build_resolved_group_spans(resolved: list[Any], *, prefix_offset: int = 0) -> dict[int, tuple[int, int]]:
    grouped: dict[int, tuple[int, int]] = {}
    spans_by_candidate: dict[int, list[tuple[int, int]]] = {}
    for item in resolved:
        candidate = getattr(item, "candidate", None)
        if candidate is None:
            continue
        start = int(getattr(item, "target_span_start", -1))
        end = int(getattr(item, "target_span_end", -1))
        if start < 0 or end <= start:
            continue
        spans_by_candidate.setdefault(id(candidate), []).append((start + prefix_offset, end + prefix_offset))
    for candidate_id, spans in spans_by_candidate.items():
        grouped[candidate_id] = (min(start for start, _ in spans), max(end for _, end in spans))
    return grouped


def build_candidate_group_spans(
    candidates: list[Any],
    evidence_text: str,
    *,
    prefix_offset: int = 0,
) -> dict[int, tuple[int, int]]:
    if len(candidates) < 2:
        return {}
    ordered = sorted(candidates, key=lambda item: (int(getattr(item, "span_start", -1)), int(getattr(item, "span_end", -1))))
    grouped: dict[int, tuple[int, int]] = {}
    current_group = [ordered[0]]
    for candidate in ordered[1:]:
        previous = current_group[-1]
        prev_end = int(getattr(previous, "span_end", -1))
        current_start = int(getattr(candidate, "span_start", -1))
        if prev_end < 0 or current_start < 0:
            continue
        gap = evidence_text[prefix_offset + prev_end : prefix_offset + current_start]
        if is_parallel_reference_gap(gap):
            current_group.append(candidate)
            continue
        assign_candidate_group_span(grouped, current_group, prefix_offset=prefix_offset)
        current_group = [candidate]
    assign_candidate_group_span(grouped, current_group, prefix_offset=prefix_offset)
    return grouped


def assign_candidate_group_span(
    grouped: dict[int, tuple[int, int]],
    current_group: list[Any],
    *,
    prefix_offset: int,
) -> None:
    if len(current_group) <= 1:
        return
    start = min(int(getattr(item, "span_start", -1)) for item in current_group)
    end = max(int(getattr(item, "span_end", -1)) for item in current_group)
    if start < 0 or end <= start:
        return
    shifted = (start + prefix_offset, end + prefix_offset)
    for item in current_group:
        grouped[id(item)] = shifted


def rebuild_candidate_span(
    evidence_text: str,
    candidate: Any,
    *,
    prefix_offset: int,
) -> tuple[int, int] | None:
    raw = str(getattr(candidate, "matched_text", "") or getattr(candidate, "target_ref_text", "") or "").strip()
    if not raw:
        return None
    starts: list[int] = []
    cursor = 0
    while True:
        found = evidence_text.find(raw, cursor)
        if found < 0:
            break
        starts.append(found)
        cursor = found + 1
    if len(starts) != 1:
        return None
    rebuilt = (starts[0], starts[0] + len(raw))
    if is_valid_reference_span(evidence_text, rebuilt[0], rebuilt[1]):
        return rebuilt
    candidate_start = int(getattr(candidate, "span_start", -1))
    candidate_end = int(getattr(candidate, "span_end", -1))
    if candidate_start >= 0 and candidate_end > candidate_start:
        shifted = (candidate_start + prefix_offset, candidate_end + prefix_offset)
        if is_valid_reference_span(evidence_text, shifted[0], shifted[1]):
            return shifted
    return None


def is_valid_reference_span(evidence_text: str, start: int, end: int) -> bool:
    if start < 0 or end <= start or end > len(evidence_text):
        return False
    return is_reference_anchor_text(evidence_text[start:end])


def is_precise_candidate_span(evidence_text: str, start: int, end: int) -> bool:
    if not is_valid_reference_span(evidence_text, start, end):
        return False
    span_text = evidence_text[start:end]
    match = REFERENCE_ANCHOR_RE.search(span_text)
    if match is None:
        return False
    prefix = span_text[: match.start()].strip("，,：:；; ")
    if prefix in {"", "根据", "依照", "按照", "参照", "适用"}:
        return True
    return REFERENCE_START_RE.search(span_text) is not None


def is_reference_anchor_text(text: str) -> bool:
    return bool(REFERENCE_ANCHOR_RE.search(str(text or "").strip()))


def target_category_for_node_id(target_node_id: str, context: DetectContext) -> str:
    if not target_node_id:
        return ""
    document_id = context.owner_document_by_node.get(target_node_id, target_node_id)
    return context.category_by_document_id.get(document_id, "")


def is_parallel_reference_gap(text: str) -> bool:
    compact = re.sub(r"\s+", "", text)
    if not compact:
        return True
    compact = compact.strip("“”\"'‘’")
    return compact in {"、", "和", "及", "以及", "或者", "或", "，", ","}
