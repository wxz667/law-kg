from __future__ import annotations

import hashlib
import random
import re
import shutil
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any

from builder.io import read_reference_candidates

from .config import canonical_label, label_to_bool, load_interprets_filter_config, resolve_distill_runtime_config
from .distill import distill_sample_batch
from .io import read_jsonl, write_json, write_jsonl

CANDIDATE_FILE = "candidates.jsonl"
TARGET_OPEN = "[T]"
TARGET_CLOSE = "[/T]"
HINT_ORDER = ("true", "false")
SOURCE_CATEGORY_ORDER = ("constitution", "law", "regulation", "interpretation")
PARALLEL_GROUP_ORDER = ("parallel", "single")
DEFAULT_ADAPTIVE_CONFIG = {
    "enabled": True,
    "oversample_factor": 5.0,
    "min_positive_hint_precision": 0.15,
    "min_negative_hint_precision": 0.85,
    "hard_negative_target_ratio": 0.35,
    "max_in_flight_batches": 2,
}
COLLECTION_PROGRESS_SHARE = 0.15
SOURCE_NODE_TRAILING_SEGMENTS = {
    "document": 0,
    "article": 1,
    "paragraph": 2,
    "item": 3,
    "sub_item": 4,
    "segment": 1,
    "appendix": 1,
}
SOURCE_LEVEL_PRIORITY = {
    "article": 5,
    "paragraph": 4,
    "item": 3,
    "sub_item": 2,
    "segment": 1,
    "appendix": 1,
}
POSITIVE_NODE_MARKERS = ("所称", "是指", "系指", "包括以下情形", "属于", "认定为", "视为")
NEGATIVE_NODE_MARKERS = ("根据", "依照", "按照", "参照", "除外", "规定办理", "规定处理", "规定处罚")
SMALL_SAMPLE_POOL_THRESHOLD = 400
EXPLICIT_TARGET_INTERPRET_PATTERNS = (
    r"(?:不)?属于<TARGET>规定的",
    r"(?:不)?认定为<TARGET>规定的",
    r"(?:不)?视为<TARGET>规定的",
    r"<TARGET>所称",
    r"<TARGET>规定的[“\"《][^”\"》]{1,30}[”\"》]",
)


def has_single_target_marker(text: str) -> bool:
    value = str(text or "")
    return value.count(TARGET_OPEN) == 1 and value.count(TARGET_CLOSE) == 1


def mark_target_text(
    text: str,
    start: int,
    end: int,
    *,
    replacement_text: str | None = None,
    fallback_to_full_text: bool = False,
) -> str:
    text = str(text or "")
    if start < 0 or end <= start or end > len(text):
        if not fallback_to_full_text:
            return text
        target_text = replacement_text if replacement_text is not None else text
        return f"{TARGET_OPEN}{target_text}{TARGET_CLOSE}"
    target_text = replacement_text if replacement_text is not None else text[start:end]
    if replacement_text:
        overlap = overlapping_prefix_length(text[:start], replacement_text)
        if overlap > 0:
            start -= overlap
    return f"{text[:start]}{TARGET_OPEN}{target_text}{TARGET_CLOSE}{text[end:]}"


def overlapping_prefix_length(prefix_text: str, replacement_text: str) -> int:
    upper_bound = min(len(prefix_text), len(replacement_text))
    for overlap in range(upper_bound, 0, -1):
        if prefix_text.endswith(replacement_text[:overlap]):
            return overlap
    return 0


def build_dataset(
    output_dir: Path,
    reference_filter_dir: Path | None = None,
    config_path: Path | None = None,
    limit: int | None = None,
    intermediate_dir: Path | None = None,
    logs_dir: Path | None = None,
    progress_callback: Any | None = None,
    phase_progress_callback: Any | None = None,
    graph_dir: Path | None = None,
    incremental: bool = False,
    cancel_event: Any | None = None,
) -> dict[str, Any]:
    config = load_interprets_filter_config(config_path)
    dataset_config = config.dataset
    requested_total = limit if limit is not None else int(dataset_config.get("max_samples", 1500))
    sample_size = min(requested_total, int(dataset_config.get("max_samples", 1500)))
    intermediate_root = intermediate_dir or Path("data/intermediate/interprets_filter")
    candidate_root = reference_filter_dir or graph_dir
    del logs_dir
    if candidate_root is None:
        raise ValueError("build_dataset requires reference_filter_dir.")
    if not (candidate_root / CANDIDATE_FILE).exists():
        raise FileNotFoundError(f"Missing reference_filter candidates: {candidate_root / CANDIDATE_FILE}")

    seed_rows = load_existing_detailed_rows(intermediate_root) if incremental else []
    if not incremental:
        cleanup_intermediate_root(intermediate_root)
    if len(seed_rows) >= sample_size:
        distilled_rows = rebalance_final_dataset(seed_rows, dataset_config, sample_size)
        sampled_candidates: list[dict[str, Any]] = []
        batches: list[dict[str, Any]] = []
        distill_stats = {"failed": 0, "succeeded": len(distilled_rows)}
        validation = {"incremental_reuse": len(seed_rows)}
    else:
        existing_ids = {str(row["sample_id"]) for row in seed_rows}
        candidates = collect_candidates(
            reference_filter_dir=candidate_root,
            pool_limit=estimate_candidate_pool_limit(sample_size, dataset_config, config.distill),
            label_weights=dict(dataset_config.get("label_weights", {})),
            cancel_event=cancel_event,
        )
        candidates = [row for row in candidates if row["sample_id"] not in existing_ids]
        validation = validate_candidate_pool(candidates)
        sampled_candidates, distilled_rows, batches, distill_stats = adaptive_distill(
            candidates=candidates,
            sample_size=sample_size,
            dataset_config=dataset_config,
            distill_config=config.distill,
            intermediate_root=intermediate_root,
            progress_callback=progress_callback,
            cancel_event=cancel_event,
            seed_rows=seed_rows,
        )
    split_distilled_rows = split_dataset(distilled_rows, dataset_config)
    final_rows = project_training_rows(distilled_rows)
    split_rows = {split_name: project_training_rows(rows) for split_name, rows in split_distilled_rows.items()}

    persist_intermediate_snapshot(intermediate_root, distilled_rows, int(dataset_config.get("review_sample_size", 20)))

    write_jsonl(output_dir / "distilled.jsonl", final_rows)
    write_jsonl(output_dir / "train.jsonl", split_rows["train"])
    write_jsonl(output_dir / "dev.jsonl", split_rows["dev"])
    write_jsonl(output_dir / "test.jsonl", split_rows["test"])
    quality_report = audit_distilled_rows(distilled_rows)
    write_json(intermediate_root / "quality_report.json", quality_report)

    return {
        "requested": requested_total,
        "sampled_candidates": len(sampled_candidates),
        "reused_samples": len(seed_rows),
        "distilled": len(final_rows),
        "distill_failures": distill_stats["failed"],
        "train": len(split_rows["train"]),
        "dev": len(split_rows["dev"]),
        "test": len(split_rows["test"]),
        "label_distribution": counter_to_dict(Counter(canonical_label(row["label"]) for row in distilled_rows), HINT_ORDER),
        "hint_distribution": counter_to_dict(Counter(row["interpret_hint"] for row in sampled_candidates), HINT_ORDER),
        "source_category_distribution": counter_to_dict(Counter(str(row.get("source_category", "")) for row in distilled_rows), SOURCE_CATEGORY_ORDER),
        "validation": validation,
        "quality_report": quality_report,
        "adaptive_batches": batches,
    }


def cleanup_intermediate_root(intermediate_root: Path) -> None:
    for path in (
        intermediate_root / "candidate_pool.jsonl",
        intermediate_root / "sampled_candidates.jsonl",
        intermediate_root / "batches",
    ):
        if not path.exists():
            continue
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


def persist_intermediate_snapshot(
    intermediate_root: Path,
    distilled_rows: list[dict[str, Any]],
    review_sample_size: int,
) -> None:
    write_jsonl(intermediate_root / "distilled_detailed.jsonl", project_detailed_rows(distilled_rows))
    write_jsonl(
        intermediate_root / "review_samples.jsonl",
        [
            {
                "sample_id": row["sample_id"],
                "text": row["text"],
                "label": row["label"],
                "teacher_reason": row["teacher_reason"],
                "teacher_model": row.get("teacher_model", ""),
            }
            for row in distilled_rows[:review_sample_size]
        ],
    )


def load_existing_detailed_rows(intermediate_root: Path) -> list[dict[str, Any]]:
    path = intermediate_root / "distilled_detailed.jsonl"
    rows = read_jsonl(path)
    reusable: list[dict[str, Any]] = []
    for row in rows:
        sample_id = str(row.get("sample_id", "")).strip()
        text = str(row.get("text", "")).strip()
        label = canonical_label(row.get("label", ""))
        if not sample_id or not text or label not in {"true", "false"}:
            continue
        reusable.append(
            {
                "sample_id": sample_id,
                "source_node_id": str(row.get("source_node_id", "")).strip(),
                "text": text,
                "interpret_hint": canonical_label(row.get("interpret_hint", False)),
                "hard_negative": bool(row.get("hard_negative", False)),
                "source_category": str(row.get("source_category", "")).strip(),
                "target_categories": [str(value) for value in row.get("target_categories", []) if str(value).strip()],
                "target_count": int(row.get("target_count", 1) or 1),
                "is_parallel_target_group": bool(row.get("is_parallel_target_group", False)),
                "is_title_level_candidate": bool(row.get("is_title_level_candidate", False)),
                "is_legislative_interpretation_source": bool(row.get("is_legislative_interpretation_source", False)),
                "has_same_level_target": bool(row.get("has_same_level_target", False)),
                "label": label,
                "teacher_reason": str(row.get("teacher_reason", "")).strip(),
                "teacher_model": str(row.get("teacher_model", "")).strip(),
            }
        )
    return reusable


def adaptive_distill(
    candidates: list[dict[str, Any]],
    sample_size: int,
    dataset_config: dict[str, Any],
    distill_config: dict[str, Any],
    intermediate_root: Path,
    progress_callback: Any | None,
    seed_rows: list[dict[str, Any]] | None = None,
    cancel_event: Any | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    adaptive_config = {**DEFAULT_ADAPTIVE_CONFIG, **dict(dataset_config.get("adaptive_sampling", {}))}
    review_sample_size = int(dataset_config.get("review_sample_size", 20))
    target_total = sample_size
    seed_rows = list(seed_rows or [])
    remaining_target = max(target_total - len(seed_rows), 0)
    batch_size, concurrent_limit = partial_distill_runtime(distill_config)
    if target_total <= 0 or (remaining_target <= 0 and seed_rows):
        final_rows = rebalance_final_dataset(seed_rows, dataset_config, target_total) if seed_rows else []
        persist_intermediate_snapshot(intermediate_root, final_rows, review_sample_size)
        if progress_callback is not None:
            progress_callback(target_total or 1, target_total or 1)
        return [], final_rows, [], {"failed": 0, "succeeded": len(final_rows)}
    if remaining_target <= 0:
        if progress_callback is not None:
            progress_callback(1, 1)
        return [], [], [], {"failed": 0, "succeeded": 0}

    desired_counts = desired_label_counts(target_total, dataset_config)
    desired_source_counts = desired_source_category_counts(target_total, seed_rows + candidates)
    max_distill_total = estimate_max_distill_total(remaining_target, len(candidates), dataset_config)
    configured_in_flight = int(adaptive_config.get("max_in_flight_batches", 0) or 0)
    max_in_flight_batches = min(
        max_distill_total if max_distill_total > 0 else 1,
        configured_in_flight if configured_in_flight > 0 else concurrent_limit,
        concurrent_limit,
    )

    sampled_candidates: list[dict[str, Any]] = []
    distilled_rows: list[dict[str, Any]] = list(seed_rows)
    selected_ids: set[str] = {str(row["sample_id"]) for row in seed_rows}
    total_failures = 0
    batches: list[dict[str, Any]] = []
    batch_counter = 0

    if progress_callback is not None:
        progress_callback(min(len(distilled_rows), target_total), target_total)
    persist_intermediate_snapshot(intermediate_root, distilled_rows, review_sample_size)
    persist_adaptive_state(
        intermediate_root,
        finalize_batch_reports(batches, desired_counts, desired_source_counts),
        total_failures,
        target_total,
        max_distill_total,
        persisted_rows=len(distilled_rows),
        persist_every=batch_size,
        in_flight_limit=max_in_flight_batches,
    )

    executor = ThreadPoolExecutor(max_workers=max_in_flight_batches)
    in_flight: dict[Future[tuple[list[dict[str, Any]], dict[str, int]]], tuple[int, list[dict[str, Any]]]] = {}
    try:
        while True:
            if cancel_event is not None and getattr(cancel_event, "is_set", lambda: False)():
                for future in in_flight:
                    future.cancel()
                raise KeyboardInterrupt
            while len(in_flight) < max_in_flight_batches:
                observed = Counter(canonical_label(row["label"]) for row in distilled_rows)
                if len(distilled_rows) >= target_total and has_required_balance(observed, desired_counts):
                    break
                remaining = [row for row in candidates if row["sample_id"] not in selected_ids]
                if not remaining:
                    break
                current_batch_budget = estimate_next_batch_size(batch_size, desired_counts, distilled_rows, dataset_config)
                current_batch_size = min(
                    batch_size,
                    current_batch_budget,
                    max(0, max_distill_total - len(sampled_candidates)),
                    len(remaining),
                )
                if current_batch_size <= 0:
                    break
                batch_candidates = select_next_batch(
                    remaining,
                    current_batch_size,
                    dataset_config,
                    distilled_rows,
                    sampled_candidates,
                    desired_counts,
                    desired_source_counts,
                )
                if not batch_candidates:
                    break
                batch_counter += 1
                sampled_candidates.extend(batch_candidates)
                selected_ids.update(row["sample_id"] for row in batch_candidates)
                future = executor.submit(invoke_distill_sample_batch, batch_candidates, distill_config, cancel_event)
                in_flight[future] = (batch_counter, batch_candidates)

            if not in_flight:
                break

            done, _pending = wait(in_flight, timeout=0.1, return_when=FIRST_COMPLETED)
            for future in done:
                batch_index, batch_candidates = in_flight.pop(future)
                try:
                    batch_rows, batch_stats = future.result()
                except Exception:
                    batch_rows, batch_stats = [], {"failed": len(batch_candidates), "succeeded": 0}
                distilled_rows.extend(batch_rows)
                total_failures += int(batch_stats.get("failed", 0))
                observed = Counter(canonical_label(row["label"]) for row in distilled_rows)
                batches.append(
                    {
                        "batch": batch_index,
                        "sampled": len(batch_candidates),
                        "distilled": len(batch_rows),
                        "failed": batch_stats["failed"],
                        "hint_distribution": counter_to_dict(Counter(row["interpret_hint"] for row in batch_candidates), HINT_ORDER),
                        "label_distribution": counter_to_dict(Counter(canonical_label(row["label"]) for row in batch_rows), HINT_ORDER),
                        "source_category_distribution": counter_to_dict(Counter(str(row.get("source_category", "")) for row in batch_rows), SOURCE_CATEGORY_ORDER),
                    }
                )
                persist_adaptive_state(
                    intermediate_root,
                    finalize_batch_reports(batches, desired_counts, desired_source_counts),
                    total_failures,
                    target_total,
                    max_distill_total,
                    persisted_rows=len(distilled_rows),
                    persist_every=batch_size,
                    in_flight_limit=max_in_flight_batches,
                )
                persist_intermediate_snapshot(intermediate_root, distilled_rows, review_sample_size)
                if progress_callback is not None:
                    progress_callback(min(len(distilled_rows), target_total), target_total)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    final_rows = rebalance_final_dataset(distilled_rows, dataset_config, target_total)
    persist_intermediate_snapshot(intermediate_root, final_rows, review_sample_size)
    persist_adaptive_state(
        intermediate_root,
        finalize_batch_reports(batches, desired_counts, desired_source_counts),
        total_failures,
        target_total,
        max_distill_total,
        persisted_rows=len(final_rows),
        persist_every=batch_size,
        in_flight_limit=max_in_flight_batches,
    )
    final_ids = {row["sample_id"] for row in final_rows}
    final_candidates = [row for row in sampled_candidates if row["sample_id"] in final_ids]
    return final_candidates, final_rows, finalize_batch_reports(batches, desired_counts, desired_source_counts), {"failed": total_failures, "succeeded": len(final_rows)}


def select_next_batch(
    candidates: list[dict[str, Any]],
    target_total: int,
    dataset_config: dict[str, Any],
    distilled_rows: list[dict[str, Any]],
    sampled_candidates: list[dict[str, Any]],
    desired_counts: dict[str, int],
    desired_source_counts: dict[str, int],
) -> list[dict[str, Any]]:
    seed = int(dataset_config.get("random_seed", 42))
    adaptive_config = {**DEFAULT_ADAPTIVE_CONFIG, **dict(dataset_config.get("adaptive_sampling", {}))}
    observed = Counter(canonical_label(row["label"]) for row in distilled_rows)
    true_deficit = max(desired_counts.get("true", 0) - observed.get("true", 0), 0)
    false_deficit = max(desired_counts.get("false", 0) - observed.get("false", 0), 0)

    positives = [row for row in candidates if row["interpret_hint"] == "true"]
    negatives = [row for row in candidates if row["interpret_hint"] == "false"]
    positive_precision = estimate_hint_precision(
        distilled_rows,
        hint="true",
        label="true",
        fallback=float(adaptive_config.get("min_positive_hint_precision", 0.15)),
    )
    negative_precision = estimate_hint_precision(
        distilled_rows,
        hint="false",
        label="false",
        fallback=float(adaptive_config.get("min_negative_hint_precision", 0.85)),
    )
    true_need = true_deficit / max(positive_precision, 0.01) if true_deficit > 0 else 0.0
    false_need = false_deficit / max(negative_precision, 0.01) if false_deficit > 0 else 0.0
    total_need = true_need + false_need
    base_true_share = float(dict(dataset_config.get("label_weights", {})).get("true", 0.5))
    positive_share = (true_need / total_need) if total_need > 0 else base_true_share

    sampled_granularity = Counter(str(row.get("granularity", "")) for row in sampled_candidates)
    remaining_granularity = Counter(str(row.get("granularity", "")) for row in candidates)
    sampled_source_categories = Counter(str(row.get("source_category", "")) for row in sampled_candidates)
    observed_source_categories = Counter(str(row.get("source_category", "")) for row in distilled_rows)
    sampled_parallel_groups = Counter("parallel" if bool(row.get("is_parallel_target_group")) else "single" for row in sampled_candidates)
    observed_parallel_groups = Counter("parallel" if bool(row.get("is_parallel_target_group")) else "single" for row in distilled_rows)
    desired_parallel_counts = desired_parallel_group_counts(target_total, candidates)
    target_hard_negative = max(
        1,
        int(round(desired_counts.get("false", 0) * float(adaptive_config.get("hard_negative_target_ratio", 0.35)))),
    ) if desired_counts.get("false", 0) > 0 else 0
    observed_hard_negative = sum(
        1
        for row in distilled_rows
        if canonical_label(row.get("label")) == "false" and bool(row.get("hard_negative"))
    )
    needs_hard_negative = observed_hard_negative < target_hard_negative

    positives.sort(
        key=lambda row: dynamic_candidate_priority(
            row,
            seed=seed,
            sampled_granularity=sampled_granularity,
            remaining_granularity=remaining_granularity,
            sampled_source_categories=sampled_source_categories,
            observed_source_categories=observed_source_categories,
            desired_source_counts=desired_source_counts,
            sampled_parallel_groups=sampled_parallel_groups,
            observed_parallel_groups=observed_parallel_groups,
            desired_parallel_counts=desired_parallel_counts,
            needs_hard_negative=False,
            label_need=true_need,
        ),
        reverse=True,
    )
    negatives.sort(
        key=lambda row: dynamic_candidate_priority(
            row,
            seed=seed,
            sampled_granularity=sampled_granularity,
            remaining_granularity=remaining_granularity,
            sampled_source_categories=sampled_source_categories,
            observed_source_categories=observed_source_categories,
            desired_source_counts=desired_source_counts,
            sampled_parallel_groups=sampled_parallel_groups,
            observed_parallel_groups=observed_parallel_groups,
            desired_parallel_counts=desired_parallel_counts,
            needs_hard_negative=needs_hard_negative,
            label_need=false_need,
        ),
        reverse=True,
    )

    selected: list[dict[str, Any]] = []
    positive_quota = min(len(positives), target_total, max(0, int(round(target_total * positive_share))))
    if true_deficit > 0 and positives and positive_quota <= 0:
        positive_quota = 1
    if false_deficit > 0 and true_deficit <= 0 and negatives and positive_quota >= target_total:
        positive_quota = max(target_total - 1, 0)

    selected.extend(positives[:positive_quota])
    remaining_slots = target_total - len(selected)
    if remaining_slots > 0:
        selected.extend(negatives[: min(len(negatives), remaining_slots)])
    remaining_slots = target_total - len(selected)
    if remaining_slots > 0:
        selected_ids = {row["sample_id"] for row in selected}
        leftovers = [row for row in candidates if row["sample_id"] not in selected_ids]
        leftovers.sort(
            key=lambda row: dynamic_candidate_priority(
                row,
                seed=seed,
                sampled_granularity=sampled_granularity,
                remaining_granularity=remaining_granularity,
                sampled_source_categories=sampled_source_categories,
                observed_source_categories=observed_source_categories,
                desired_source_counts=desired_source_counts,
                sampled_parallel_groups=sampled_parallel_groups,
                observed_parallel_groups=observed_parallel_groups,
                desired_parallel_counts=desired_parallel_counts,
                needs_hard_negative=needs_hard_negative,
                label_need=true_need if row.get("interpret_hint") == "true" else false_need,
            ),
            reverse=True,
        )
        selected.extend(leftovers[:remaining_slots])
    return selected[:target_total]


def dynamic_candidate_priority(
    row: dict[str, Any],
    *,
    seed: int,
    sampled_granularity: Counter[str],
    remaining_granularity: Counter[str],
    sampled_source_categories: Counter[str],
    observed_source_categories: Counter[str],
    desired_source_counts: dict[str, int],
    sampled_parallel_groups: Counter[str],
    observed_parallel_groups: Counter[str],
    desired_parallel_counts: dict[str, int],
    needs_hard_negative: bool,
    label_need: float,
) -> tuple[Any, ...]:
    granularity = str(row.get("granularity", ""))
    source_category = str(row.get("source_category", ""))
    parallel_group = "parallel" if bool(row.get("is_parallel_target_group")) else "single"
    title_priority = 1 if canonical_label(row.get("interpret_hint")) == "true" and bool(row.get("is_title_level_candidate")) else 0
    same_level_priority = 1 if canonical_label(row.get("interpret_hint")) == "true" and bool(row.get("has_same_level_target")) else 0
    legislative_title_priority = 1 if canonical_label(row.get("interpret_hint")) == "true" and bool(row.get("is_legislative_interpretation_source")) else 0
    sampled_count = sampled_granularity.get(granularity, 0)
    remaining_count = remaining_granularity.get(granularity, 0)
    granularity_pressure = remaining_count / max(sampled_count + 1, 1)
    category_deficit = max(desired_source_counts.get(source_category, 0) - observed_source_categories.get(source_category, 0), 0)
    sampled_category_count = sampled_source_categories.get(source_category, 0)
    source_category_pressure = category_deficit / max(sampled_category_count + 1, 1)
    parallel_deficit = max(desired_parallel_counts.get(parallel_group, 0) - observed_parallel_groups.get(parallel_group, 0), 0)
    sampled_parallel_count = sampled_parallel_groups.get(parallel_group, 0)
    parallel_pressure = parallel_deficit / max(sampled_parallel_count + 1, 1)
    hard_negative_priority = 1 if needs_hard_negative and bool(row.get("hard_negative")) else 0
    candidate_priority = candidate_sort_key(row, seed)
    return (
        1 if parallel_group == "parallel" else 0,
        title_priority,
        same_level_priority,
        legislative_title_priority,
        float(label_need),
        parallel_deficit,
        parallel_pressure,
        category_deficit,
        source_category_pressure,
        hard_negative_priority,
        granularity_pressure,
        *candidate_priority,
    )


def persist_adaptive_state(
    intermediate_root: Path,
    batches: list[dict[str, Any]],
    total_failures: int,
    target_total: int,
    max_distill_total: int,
    *,
    persisted_rows: int,
    persist_every: int,
    in_flight_limit: int,
) -> None:
    write_json(
        intermediate_root / "adaptive_state.json",
        {
            "target_total": target_total,
            "max_distill_total": max_distill_total,
            "persisted_rows": persisted_rows,
            "persist_every": persist_every,
            "in_flight_limit": in_flight_limit,
            "batches": batches,
            "total_failures": total_failures,
        },
    )


def finalize_batch_reports(
    batches: list[dict[str, Any]],
    desired_counts: dict[str, int],
    desired_source_counts: dict[str, int],
) -> list[dict[str, Any]]:
    ordered = sorted(batches, key=lambda item: int(item["batch"]))
    observed: Counter[str] = Counter()
    observed_sources: Counter[str] = Counter()
    finalized: list[dict[str, Any]] = []
    for batch in ordered:
        observed.update({label: int(count) for label, count in dict(batch.get("label_distribution", {})).items()})
        observed_sources.update({label: int(count) for label, count in dict(batch.get("source_category_distribution", {})).items()})
        finalized.append(
            {
                **batch,
                "cumulative_label_distribution": counter_to_dict(observed, HINT_ORDER),
                "remaining_label_deficit": remaining_label_deficit(observed, desired_counts),
                "cumulative_source_category_distribution": counter_to_dict(observed_sources, SOURCE_CATEGORY_ORDER),
                "remaining_source_category_deficit": remaining_label_deficit(observed_sources, desired_source_counts),
            }
        )
    return finalized


def remaining_label_deficit(observed: Counter[str], desired_counts: dict[str, int]) -> dict[str, int]:
    return {
        label: max(desired_counts.get(label, 0) - observed.get(label, 0), 0)
        for label in desired_counts
    }


def make_collection_progress_callback(progress_callback: Any, sample_size: int, collection_budget: int) -> Any:
    def _callback(scanned: int, total: int) -> None:
        if total <= 0:
            progress_callback(0, sample_size)
            return
        current = min(collection_budget, int(collection_budget * (scanned / total)))
        progress_callback(current, sample_size)

    return _callback


def make_distill_progress_callback(progress_callback: Any, sample_size: int, collection_budget: int, expected_total: int) -> Any:
    distill_span = max(sample_size - collection_budget, 1)

    def _callback(completed: int, total: int) -> None:
        effective_total = max(total, expected_total, 1)
        if effective_total <= 0:
            progress_callback(collection_budget, sample_size)
            return
        current = collection_budget + min(distill_span, int(distill_span * (completed / effective_total)))
        progress_callback(min(current, sample_size), sample_size)

    return _callback


def estimate_candidate_pool_limit(
    sample_size: int,
    dataset_config: dict[str, Any],
    distill_config: dict[str, Any] | None = None,
) -> int:
    multiplier = float(dataset_config.get("candidate_pool_multiplier", 12.0))
    configured_limit = max(int(dataset_config.get("candidate_pool_minimum", 1200)), int(sample_size * multiplier))
    adaptive_config = {**DEFAULT_ADAPTIVE_CONFIG, **dict(dataset_config.get("adaptive_sampling", {}))}
    oversample_factor = float(adaptive_config.get("oversample_factor", 5.0))
    batch_size, concurrent_requests = partial_distill_runtime(distill_config or {})
    downstream_capacity = max(sample_size, int(sample_size * oversample_factor))
    request_buffer = max(batch_size * concurrent_requests // 2, min(sample_size, 80))
    effective_limit = max(180, downstream_capacity + request_buffer)
    return min(configured_limit, effective_limit)


def partial_distill_runtime(distill_config: dict[str, Any]) -> tuple[int, int]:
    if distill_config.get("provider") and distill_config.get("model"):
        runtime = resolve_distill_runtime_config(distill_config)
        return runtime.batch_size, runtime.concurrent_requests
    return max(1, int(distill_config.get("batch_size", 1))), max(1, int(distill_config.get("concurrent_requests", 1)))


def estimate_max_distill_total(sample_size: int, available_candidates: int, dataset_config: dict[str, Any]) -> int:
    adaptive_config = {**DEFAULT_ADAPTIVE_CONFIG, **dict(dataset_config.get("adaptive_sampling", {}))}
    oversample_factor = float(adaptive_config.get("oversample_factor", 5.0))
    return min(available_candidates, max(sample_size, int(sample_size * oversample_factor)))


def collect_candidates(
    reference_filter_dir: Path,
    *,
    pool_limit: int | None = None,
    label_weights: dict[str, Any] | None = None,
    progress_callback: Any | None = None,
    phase_progress_callback: Any | None = None,
    cancel_event: Any | None = None,
) -> list[dict[str, Any]]:
    candidate_path = reference_filter_dir / CANDIDATE_FILE
    raw_candidates = read_reference_candidates(candidate_path)
    total_raw = max(len(raw_candidates), 1)
    if phase_progress_callback is not None:
        phase_progress_callback("prefilter", 0, total_raw)

    filtered_candidates = [
        candidate
        for candidate in raw_candidates
        if has_single_target_marker(candidate.text)
        and candidate.target_node_ids
        and len(candidate.target_categories) == len(candidate.target_node_ids)
    ]
    if phase_progress_callback is not None:
        phase_progress_callback("prefilter", total_raw, total_raw)

    filtered_candidates.sort(key=reference_candidate_priority, reverse=True)
    total_filtered = max(len(filtered_candidates), 1)
    if phase_progress_callback is not None:
        phase_progress_callback("collect", 0, total_filtered)

    candidates: list[dict[str, Any]] = []
    hint_counts: Counter[str] = Counter()
    category_counts: Counter[str] = Counter()
    parallel_counts: Counter[str] = Counter()
    target_hint_counts = desired_hint_counts(pool_limit, label_weights or {})
    target_category_counts = desired_source_category_targets(pool_limit, filtered_candidates)
    target_parallel_counts = desired_parallel_group_targets(pool_limit, filtered_candidates)
    for index, candidate in enumerate(filtered_candidates, start=1):
        if cancel_event is not None and getattr(cancel_event, "is_set", lambda: False)():
            raise KeyboardInterrupt
        interpret_hint = infer_interpret_hint(candidate.text)
        source_category = source_category_from_sample_id(candidate.id)
        target_count = len(candidate.target_node_ids)
        parallel_group = "parallel" if target_count > 1 else "single"
        target_categories = list(candidate.target_categories)
        candidates.append(
            {
                "sample_id": candidate.id,
                "source_node_id": candidate.source_node_id,
                "text": candidate.text,
                "interpret_hint": interpret_hint,
                "hard_negative": is_hard_negative(candidate.text),
                "granularity": source_node_granularity(candidate.source_node_id),
                "source_category": source_category,
                "target_categories": target_categories,
                "target_category_signature": target_category_signature(target_categories),
                "target_count": target_count,
                "is_parallel_target_group": target_count > 1,
                "is_title_level_candidate": str(candidate.source_node_id).startswith("document:"),
                "is_legislative_interpretation_source": (
                    source_category == "law"
                    and str(candidate.source_node_id).startswith("document:")
                    and "全国人民代表大会常务委员会关于《" in candidate.text
                    and candidate.text.endswith("的解释")
                ),
                "has_same_level_target": any(category == source_category for category in target_categories),
            }
        )
        hint_counts.update((interpret_hint,))
        category_counts.update((source_category,))
        parallel_counts.update((parallel_group,))
        if (
            pool_limit is not None
            and len(candidates) >= pool_limit
            and meets_hint_targets(hint_counts, target_hint_counts)
            and meets_hint_targets(category_counts, target_category_counts)
            and meets_hint_targets(parallel_counts, target_parallel_counts)
        ):
            if progress_callback is not None:
                progress_callback(total_filtered, total_filtered)
            if phase_progress_callback is not None:
                phase_progress_callback("collect", total_filtered, total_filtered)
            return candidates
        if index % 50 == 0 or index == total_filtered:
            if progress_callback is not None:
                progress_callback(index, total_filtered)
            if phase_progress_callback is not None:
                phase_progress_callback("collect", index, total_filtered)
    return candidates


def reference_candidate_priority(candidate: Any) -> tuple[int, int, int, int, int, int]:
    text = str(getattr(candidate, "text", "") or "")
    level = source_node_granularity(str(getattr(candidate, "source_node_id", "")))
    target_count = len(getattr(candidate, "target_node_ids", []) or [])
    positive_hits = sum(marker in text for marker in POSITIVE_NODE_MARKERS)
    negative_hits = sum(marker in text for marker in NEGATIVE_NODE_MARKERS)
    reference_hits = text.count("第") + text.count("本条") + text.count("本款")
    level_priority = SOURCE_LEVEL_PRIORITY.get(level, 0)
    preferred_length = min(abs(len(text) - 90), 200)
    return (
        int(target_count > 1),
        min(target_count, 9),
        int(str(getattr(candidate, "source_node_id", "")).startswith("document:")),
        positive_hits + negative_hits,
        reference_hits,
        level_priority,
        -preferred_length,
    )


def desired_hint_counts(pool_limit: int | None, label_weights: dict[str, Any]) -> dict[str, int]:
    if pool_limit is None or pool_limit <= 0:
        return {}
    true_weight = float(label_weights.get("true", 0.58))
    false_weight = max(0.0, 1.0 - true_weight)
    floor = max(16, int(pool_limit * 0.12))
    return {
        "true": max(floor, int(pool_limit * true_weight * 0.35)),
        "false": max(floor, int(pool_limit * false_weight * 0.35)),
    }


def desired_source_category_targets(pool_limit: int | None, candidates: list[Any]) -> dict[str, int]:
    if pool_limit is None or pool_limit <= 0:
        return {}
    available = Counter(source_category_from_sample_id(getattr(candidate, "id", "")) for candidate in candidates)
    categories = [category for category in SOURCE_CATEGORY_ORDER if available.get(category, 0) > 0]
    if not categories:
        return {}
    base = max(1, pool_limit // len(categories))
    counts = {category: min(available.get(category, 0), base) for category in categories}
    assigned = sum(counts.values())
    remaining = max(pool_limit - assigned, 0)
    while remaining > 0:
        best_category = max(
            categories,
            key=lambda category: (available.get(category, 0) - counts.get(category, 0), -SOURCE_CATEGORY_ORDER.index(category)),
        )
        if available.get(best_category, 0) <= counts.get(best_category, 0):
            break
        counts[best_category] += 1
        remaining -= 1
    return counts


def desired_parallel_group_targets(pool_limit: int | None, candidates: list[Any]) -> dict[str, int]:
    if pool_limit is None or pool_limit <= 0:
        return {}
    parallel_available = sum(1 for candidate in candidates if len(getattr(candidate, "target_node_ids", []) or []) > 1)
    if parallel_available <= 0:
        return {}
    desired_parallel = max(12, int(round(pool_limit * 0.28)))
    desired_parallel = min(parallel_available, desired_parallel)
    return {"parallel": desired_parallel} if desired_parallel > 0 else {}


def desired_label_counts(target_total: int, dataset_config: dict[str, Any]) -> dict[str, int]:
    weights = dict(dataset_config.get("label_weights", {}))
    true_weight = float(weights.get("true", 0.5))
    desired_true = int(round(target_total * true_weight))
    desired_true = min(max(desired_true, 0), target_total)
    return {"true": desired_true, "false": target_total - desired_true}


def desired_source_category_counts(target_total: int, rows: list[dict[str, Any]]) -> dict[str, int]:
    available = Counter(str(row.get("source_category", "")) for row in rows)
    categories = [category for category in SOURCE_CATEGORY_ORDER if available.get(category, 0) > 0]
    if not categories or target_total <= 0:
        return {}
    base = target_total // len(categories)
    counts = {category: min(available.get(category, 0), base) for category in categories}
    assigned = sum(counts.values())
    remaining = max(target_total - assigned, 0)
    while remaining > 0:
        best_category = max(
            categories,
            key=lambda category: (available.get(category, 0) - counts.get(category, 0), -SOURCE_CATEGORY_ORDER.index(category)),
        )
        if available.get(best_category, 0) <= counts.get(best_category, 0):
            break
        counts[best_category] = counts.get(best_category, 0) + 1
        remaining -= 1
    return counts


def desired_parallel_group_counts(target_total: int, rows: list[dict[str, Any]]) -> dict[str, int]:
    if target_total <= 0:
        return {}
    parallel_available = sum(1 for row in rows if bool(row.get("is_parallel_target_group")))
    if parallel_available <= 0:
        return {}
    desired_parallel = max(12, int(round(target_total * 0.28)))
    desired_parallel = min(parallel_available, desired_parallel)
    return {"parallel": desired_parallel} if desired_parallel > 0 else {}


def estimate_next_batch_size(
    batch_size: int,
    desired_counts: dict[str, int],
    distilled_rows: list[dict[str, Any]],
    dataset_config: dict[str, Any],
) -> int:
    adaptive_config = {**DEFAULT_ADAPTIVE_CONFIG, **dict(dataset_config.get("adaptive_sampling", {}))}
    observed = Counter(canonical_label(row["label"]) for row in distilled_rows)
    true_deficit = max(desired_counts.get("true", 0) - observed.get("true", 0), 0)
    false_deficit = max(desired_counts.get("false", 0) - observed.get("false", 0), 0)
    if true_deficit <= 0 and false_deficit <= 0:
        return 1
    positive_precision = estimate_hint_precision(
        distilled_rows,
        hint="true",
        label="true",
        fallback=float(adaptive_config.get("min_positive_hint_precision", 0.15)),
    )
    negative_precision = estimate_hint_precision(
        distilled_rows,
        hint="false",
        label="false",
        fallback=float(adaptive_config.get("min_negative_hint_precision", 0.85)),
    )
    estimated_need = 0.0
    if true_deficit > 0:
        estimated_need += true_deficit / max(positive_precision, 0.01)
    if false_deficit > 0:
        estimated_need += false_deficit / max(negative_precision, 0.01)
    return max(1, min(batch_size, int(round(estimated_need + 0.25))))


def has_required_balance(observed: Counter[str], desired_counts: dict[str, int]) -> bool:
    return all(observed.get(label, 0) >= desired_counts.get(label, 0) for label in desired_counts)


def estimate_hint_precision(rows: list[dict[str, Any]], *, hint: str, label: str, fallback: float) -> float:
    hint_rows = [row for row in rows if row.get("interpret_hint") == hint]
    if not hint_rows:
        return fallback
    matched = sum(canonical_label(row["label"]) == label for row in hint_rows)
    return max(matched / len(hint_rows), fallback)


def meets_hint_targets(counts: Counter[str], targets: dict[str, int]) -> bool:
    if not targets:
        return True
    return all(counts.get(label, 0) >= required for label, required in targets.items())


def infer_interpret_hint(marked_text: str, match: Any | None = None) -> str:
    if match is not None:
        start = int(getattr(match, "target_span_start", getattr(match, "span_start", -1)))
        end = int(getattr(match, "target_span_end", getattr(match, "span_end", -1)))
        replacement_text = getattr(match, "target_ref_text", None)
        marked_text = mark_target_text(marked_text, start, end, replacement_text=replacement_text)
    if is_hard_negative(marked_text):
        return "false"
    if is_interpret_signal(marked_text):
        return "true"
    return "false"


def is_interpret_signal(marked_text: str) -> bool:
    compact = compact_text(re.sub(r"\[T\].*?\[/T\]", "<TARGET>", str(marked_text or "")))
    if has_definition_basis_mismatch(compact):
        return False
    if has_source_term_defined_by_target(compact):
        return False
    if has_title_level_interpret_signal(compact):
        return True
    if any(re.search(pattern, compact) for pattern in EXPLICIT_TARGET_INTERPRET_PATTERNS):
        return True
    if any(marker in compact for marker in ("除外", "依照", "根据", "按照", "参照", "所列", "规定办理", "规定的情形")):
        return False
    explicit_patterns = EXPLICIT_TARGET_INTERPRET_PATTERNS + (r"(包括以下情形|是指|系指).{0,24}",)
    if any(re.search(pattern, compact) for pattern in explicit_patterns):
        return True
    definition_markers = ("所称", "是指", "系指")
    definition_positions = [compact.find(marker) for marker in definition_markers if marker in compact]
    target_pos = compact.find("<TARGET>")
    return bool(definition_positions and target_pos >= 0 and target_pos < min(definition_positions))


def has_title_level_interpret_signal(compact_marked: str) -> bool:
    patterns = (
        r"^全国人民代表大会常务委员会关于<TARGET>的解释$",
        r"^关于<TARGET>的解释$",
        r"^最高人民法院关于<TARGET>的解释$",
        r"^最高人民法院关于.*<TARGET>.*的解释$",
        r"^.*关于<TARGET>适用问题的解释$",
    )
    return any(re.search(pattern, compact_marked) for pattern in patterns)


def has_definition_basis_mismatch(compact_marked: str) -> bool:
    definition_markers = ("所称", "是指", "系指")
    basis_markers = ("依照", "根据", "按照", "参照")
    target_pos = compact_marked.find("<TARGET>")
    if target_pos < 0:
        return False
    definition_positions = [compact_marked.find(marker) for marker in definition_markers if marker in compact_marked]
    if not definition_positions:
        return False
    definition_pos = min(definition_positions)
    if target_pos <= definition_pos:
        return False
    window = compact_marked[definition_pos:target_pos]
    return any(marker in window for marker in basis_markers)


def has_source_term_defined_by_target(compact_marked: str) -> bool:
    patterns = (
        r"(?:本法|本条例|本规定|本解释|本办法|本细则|本决定|本通则).{0,40}(?:所称|是指|系指).{0,80}<TARGET>(?:所)?规定的",
        r"(?:本法|本条例|本规定|本解释|本办法|本细则|本决定|本通则).{0,40}包括.{0,40}<TARGET>(?:所)?规定的",
    )
    return any(re.search(pattern, compact_marked) for pattern in patterns)


def is_hard_negative(sentence: str) -> bool:
    compact = compact_text(str(sentence or ""))
    target_compact = re.sub(r"\[T\].*?\[/T\]", "<TARGET>", compact)
    if any(re.search(pattern, target_compact) for pattern in EXPLICIT_TARGET_INTERPRET_PATTERNS):
        return False
    compact = compact.replace(TARGET_OPEN, "").replace(TARGET_CLOSE, "")
    markers = (
        "依照",
        "根据",
        "按照",
        "参照",
        "除外",
        "所列",
        "规定的情形",
        "规定办理",
        "规定处理",
        "规定处罚",
    )
    return any(marker in compact for marker in markers)


def infer_granularity(match: Any) -> str:
    target_text = str(getattr(match, "target_ref_text", ""))
    if target_text.endswith("目"):
        return "sub_item"
    if target_text.endswith("项"):
        return "item"
    if target_text.endswith("款"):
        return "paragraph"
    return "article"


def rebalance_final_dataset(rows: list[dict[str, Any]], dataset_config: dict[str, Any], target_total: int) -> list[dict[str, Any]]:
    if target_total <= 1:
        return rows[:target_total]
    if len(rows) <= target_total:
        observed = Counter(canonical_label(row["label"]) for row in rows)
        desired = desired_label_counts(target_total, dataset_config)
        if len(rows) == target_total and has_required_balance(observed, desired):
            return ensure_parallel_group_coverage(rows, rows, target_total)
    seed = int(dataset_config.get("random_seed", 42))
    desired = desired_label_counts(target_total, dataset_config)
    target_positive = desired.get("true", 0)
    target_negative = desired.get("false", 0)
    positives = [row for row in rows if canonical_label(row["label"]) == "true"]
    negatives = [row for row in rows if canonical_label(row["label"]) == "false"]
    if (target_positive == 0 or target_negative == 0) and len(rows) >= target_total:
        return ensure_parallel_group_coverage(rows[:target_total], rows, target_total)
    positives.sort(key=lambda row: candidate_sort_key(row, seed), reverse=True)
    negatives.sort(key=lambda row: (int(bool(row.get("hard_negative"))),) + candidate_sort_key(row, seed), reverse=True)
    if len(positives) >= target_positive and len(negatives) >= target_negative:
        selected = positives[:target_positive]
        selected.extend(negatives[:target_negative])
        return ensure_parallel_group_coverage(ensure_source_category_coverage(selected[:target_total], rows, target_total), rows, target_total)
    positive_count = min(len(positives), target_positive)
    negative_count = min(len(negatives), target_negative)
    selected = positives[:positive_count]
    selected.extend(negatives[:negative_count])
    remaining_slots = target_total - len(selected)
    if remaining_slots > 0:
        selected_ids = {row["sample_id"] for row in selected}
        leftovers = [row for row in positives + negatives if row["sample_id"] not in selected_ids]
        leftovers.sort(key=lambda row: candidate_sort_key(row, seed), reverse=True)
        selected.extend(leftovers[:remaining_slots])
    return ensure_parallel_group_coverage(ensure_source_category_coverage(selected[:target_total], rows, target_total), rows, target_total)


def candidate_sort_key(row: dict[str, Any], seed: int) -> tuple[int, int, int, int, int, str]:
    positive_hint = 1 if canonical_label(row.get("interpret_hint")) == "true" else 0
    title_priority = 1 if positive_hint and bool(row.get("is_title_level_candidate")) else 0
    same_level_priority = 1 if positive_hint and bool(row.get("has_same_level_target")) else 0
    granularity_priority = {"sub_item": 4, "item": 3, "paragraph": 2, "article": 1}.get(str(row.get("granularity", "")), 0)
    hard_negative = 1 if row.get("hard_negative") else 0
    text_length = min(len(str(row.get("text", ""))), 280)
    digest = hashlib.sha256(f"{seed}:{row['sample_id']}".encode("utf-8")).hexdigest()
    return (title_priority, same_level_priority, hard_negative, granularity_priority, text_length, digest)


def ensure_source_category_coverage(
    selected_rows: list[dict[str, Any]],
    all_rows: list[dict[str, Any]],
    target_total: int,
) -> list[dict[str, Any]]:
    desired_categories = {str(row.get("source_category", "")) for row in all_rows if str(row.get("source_category", ""))}
    if not desired_categories or len(selected_rows) >= target_total and desired_categories.issubset({str(row.get("source_category", "")) for row in selected_rows}):
        return selected_rows[:target_total]

    selected = list(selected_rows[:target_total])
    selected_ids = {row["sample_id"] for row in selected}
    present = {str(row.get("source_category", "")) for row in selected}
    for category in SOURCE_CATEGORY_ORDER:
        if category not in desired_categories or category in present:
            continue
        replacement = next((row for row in all_rows if row["sample_id"] not in selected_ids and str(row.get("source_category", "")) == category), None)
        if replacement is None:
            continue
        drop_index = next(
            (
                index
                for index, row in enumerate(selected)
                if Counter(str(item.get("source_category", "")) for item in selected)[str(row.get("source_category", ""))] > 1
            ),
            len(selected) - 1 if selected else None,
        )
        if drop_index is None:
            break
        removed = selected.pop(drop_index)
        selected_ids.discard(removed["sample_id"])
        selected.append(replacement)
        selected_ids.add(replacement["sample_id"])
        present.add(category)
    return selected[:target_total]


def ensure_parallel_group_coverage(
    selected_rows: list[dict[str, Any]],
    all_rows: list[dict[str, Any]],
    target_total: int,
) -> list[dict[str, Any]]:
    desired_parallel = desired_parallel_group_counts(target_total, all_rows).get("parallel", 0)
    if desired_parallel <= 0:
        return selected_rows[:target_total]
    selected = list(selected_rows[:target_total])
    selected_ids = {row["sample_id"] for row in selected}
    current_parallel = sum(1 for row in selected if bool(row.get("is_parallel_target_group")))
    if current_parallel >= desired_parallel:
        return selected
    replacements = [row for row in all_rows if row["sample_id"] not in selected_ids and bool(row.get("is_parallel_target_group"))]
    for replacement in replacements:
        drop_index = next((index for index, row in enumerate(selected) if not bool(row.get("is_parallel_target_group"))), None)
        if drop_index is None:
            break
        removed = selected.pop(drop_index)
        selected_ids.discard(removed["sample_id"])
        selected.append(replacement)
        selected_ids.add(replacement["sample_id"])
        current_parallel += 1
        if current_parallel >= desired_parallel:
            break
    return selected[:target_total]


def split_dataset(rows: list[dict[str, Any]], dataset_config: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    split_config = dict(dataset_config.get("splits", {}))
    train_ratio = float(split_config.get("train", 0.8))
    dev_ratio = float(split_config.get("dev", 0.1))
    seed = int(dataset_config.get("random_seed", 42))

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[source_document_id(str(row["source_node_id"]))].append(row)

    document_ids = list(grouped)
    if len(document_ids) == 1:
        only_document = document_ids[0]
        return {"train": list(grouped[only_document]), "dev": [], "test": []}

    rng = random.Random(seed)
    rng.shuffle(document_ids)
    split_names = ("train", "dev", "test")
    row_targets = allocate_target_sizes(len(rows), train_ratio, dev_ratio)
    label_totals = Counter(canonical_label(row["label"]) for row in rows)
    label_targets = {
        label: allocate_target_sizes(count, train_ratio, dev_ratio)
        for label, count in label_totals.items()
    }
    doc_rows = {
        document_id: grouped[document_id]
        for document_id in document_ids
    }
    doc_counts = {
        document_id: Counter(canonical_label(row["label"]) for row in doc_rows[document_id])
        for document_id in document_ids
    }
    docs_by_priority = sorted(
        document_ids,
        key=lambda document_id: (
            len(doc_rows[document_id]),
            max(doc_counts[document_id].values(), default=0),
            len(doc_counts[document_id]),
            document_id,
        ),
        reverse=True,
    )

    split_documents = {split_name: [] for split_name in split_names}
    split_counts = {split_name: Counter() for split_name in split_names}
    split_sizes = {split_name: 0 for split_name in split_names}

    for index, document_id in enumerate(docs_by_priority):
        remaining_docs = len(docs_by_priority) - index
        empty_splits = [split_name for split_name in split_names if not split_documents[split_name]]
        if empty_splits and remaining_docs == len(empty_splits):
            chosen_split = empty_splits[0]
        else:
            chosen_split = min(
                split_names,
                key=lambda split_name: split_assignment_score(
                    split_name=split_name,
                    doc_size=len(doc_rows[document_id]),
                    doc_counts=doc_counts[document_id],
                    split_sizes=split_sizes,
                    split_counts=split_counts,
                    row_targets=row_targets,
                    label_targets=label_targets,
                    current_doc_count=len(split_documents[split_name]),
                ),
            )
        split_documents[chosen_split].append(document_id)
        split_sizes[chosen_split] += len(doc_rows[document_id])
        split_counts[chosen_split].update(doc_counts[document_id])

    rebalance_split_labels(
        split_documents=split_documents,
        split_sizes=split_sizes,
        split_counts=split_counts,
        doc_rows=doc_rows,
        doc_counts=doc_counts,
        row_targets=row_targets,
        label_targets=label_targets,
        labels=tuple(label_totals),
    )

    return {
        split_name: [row for document_id in split_documents[split_name] for row in doc_rows[document_id]]
        for split_name in split_names
    }


def allocate_target_sizes(total: int, train_ratio: float, dev_ratio: float) -> dict[str, int]:
    if total <= 0:
        return {"train": 0, "dev": 0, "test": 0}
    raw = {
        "train": total * train_ratio,
        "dev": total * dev_ratio,
        "test": total * max(0.0, 1.0 - train_ratio - dev_ratio),
    }
    counts = {split_name: int(raw_value) for split_name, raw_value in raw.items()}
    assigned = sum(counts.values())
    remainders = sorted(
        raw.items(),
        key=lambda item: (item[1] - int(item[1]), item[0]),
        reverse=True,
    )
    for split_name, _ in remainders:
        if assigned >= total:
            break
        counts[split_name] += 1
        assigned += 1
    return counts


def split_assignment_score(
    split_name: str,
    doc_size: int,
    doc_counts: Counter[str],
    split_sizes: dict[str, int],
    split_counts: dict[str, Counter[str]],
    row_targets: dict[str, int],
    label_targets: dict[str, dict[str, int]],
    current_doc_count: int,
) -> tuple[float, ...]:
    projected_size = split_sizes[split_name] + doc_size
    size_target = row_targets.get(split_name, 0)
    size_penalty = abs(projected_size - size_target) + max(projected_size - size_target, 0) * 3

    label_penalty = 0.0
    for label, target_counts in label_targets.items():
        projected_count = split_counts[split_name].get(label, 0) + doc_counts.get(label, 0)
        target_count = target_counts.get(split_name, 0)
        label_penalty += abs(projected_count - target_count) + max(projected_count - target_count, 0) * 4

    empty_penalty = 0.25 if current_doc_count == 0 else 0.0
    return (label_penalty, size_penalty, empty_penalty, projected_size)


def rebalance_split_labels(
    split_documents: dict[str, list[str]],
    split_sizes: dict[str, int],
    split_counts: dict[str, Counter[str]],
    doc_rows: dict[str, list[dict[str, Any]]],
    doc_counts: dict[str, Counter[str]],
    row_targets: dict[str, int],
    label_targets: dict[str, dict[str, int]],
    labels: tuple[str, ...],
) -> None:
    donor_order = ("train", "dev", "test")
    recipient_order = ("dev", "test", "train")
    for label in labels:
        total_label_count = sum(split_counts[split_name].get(label, 0) for split_name in recipient_order)
        if total_label_count <= 0:
            continue
        for recipient in recipient_order:
            if split_counts[recipient].get(label, 0) > 0:
                continue
            best_move: tuple[tuple[float, ...], str, str] | None = None
            for donor in donor_order:
                if donor == recipient or len(split_documents[donor]) <= 1 or split_counts[donor].get(label, 0) <= 1:
                    continue
                for document_id in split_documents[donor]:
                    if doc_counts[document_id].get(label, 0) <= 0:
                        continue
                    score = move_score(
                        donor=donor,
                        recipient=recipient,
                        document_id=document_id,
                        split_sizes=split_sizes,
                        split_counts=split_counts,
                        doc_rows=doc_rows,
                        doc_counts=doc_counts,
                        row_targets=row_targets,
                        label_targets=label_targets,
                    )
                    if best_move is None or score < best_move[0]:
                        best_move = (score, donor, document_id)
            if best_move is None:
                continue
            _score, donor, document_id = best_move
            split_documents[donor].remove(document_id)
            split_documents[recipient].append(document_id)
            split_sizes[donor] -= len(doc_rows[document_id])
            split_sizes[recipient] += len(doc_rows[document_id])
            split_counts[donor].subtract(doc_counts[document_id])
            split_counts[recipient].update(doc_counts[document_id])
            split_counts[donor] += Counter()


def move_score(
    donor: str,
    recipient: str,
    document_id: str,
    split_sizes: dict[str, int],
    split_counts: dict[str, Counter[str]],
    doc_rows: dict[str, list[dict[str, Any]]],
    doc_counts: dict[str, Counter[str]],
    row_targets: dict[str, int],
    label_targets: dict[str, dict[str, int]],
) -> tuple[float, ...]:
    doc_size = len(doc_rows[document_id])
    size_penalty = 0.0
    for split_name, direction in ((donor, -1), (recipient, 1)):
        projected_size = split_sizes[split_name] + (direction * doc_size)
        target_size = row_targets.get(split_name, 0)
        size_penalty += abs(projected_size - target_size) + max(projected_size - target_size, 0) * 3

    label_penalty = 0.0
    for label, targets in label_targets.items():
        doc_label_count = doc_counts[document_id].get(label, 0)
        donor_projected = split_counts[donor].get(label, 0) - doc_label_count
        recipient_projected = split_counts[recipient].get(label, 0) + doc_label_count
        label_penalty += abs(donor_projected - targets.get(donor, 0)) + abs(recipient_projected - targets.get(recipient, 0))
    return (label_penalty, size_penalty, doc_size)


def project_training_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"sample_id": row["sample_id"], "text": row["text"], "label": label_to_bool(row["label"])} for row in rows]


def project_detailed_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = (
        "sample_id",
        "source_node_id",
        "text",
        "interpret_hint",
        "hard_negative",
        "source_category",
        "target_categories",
        "target_count",
        "is_parallel_target_group",
        "is_title_level_candidate",
        "is_legislative_interpretation_source",
        "has_same_level_target",
        "label",
        "teacher_reason",
        "teacher_model",
    )
    return [{key: row[key] for key in keys if key in row} for row in rows]


def validate_candidate_pool(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    invalid_marker_count = sum(not has_single_target_marker(str(row.get("text", ""))) for row in candidates)
    empty_target_count = sum(not extract_target_payload(str(row.get("text", ""))) for row in candidates)
    duplicate_count = len(candidates) - len({str(row.get("sample_id", "")).strip() for row in candidates})
    return {
        "documents_with_candidates": len({source_document_id(str(row["source_node_id"])) for row in candidates}),
        "candidate_count": len(candidates),
        "hint_counts": counter_to_dict(Counter(row["interpret_hint"] for row in candidates), HINT_ORDER),
        "source_category_counts": counter_to_dict(Counter(str(row.get("source_category", "")) for row in candidates), SOURCE_CATEGORY_ORDER),
        "granularity_counts": counter_to_dict(Counter(row["granularity"] for row in candidates), ("article", "paragraph", "item", "sub_item")),
        "invalid_marker_count": invalid_marker_count,
        "empty_target_count": empty_target_count,
        "duplicate_candidate_count": duplicate_count,
    }


def audit_distilled_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    issues: list[dict[str, Any]] = []
    for row in rows:
        row_issues: list[str] = []
        text = str(row.get("text", ""))
        target_text = extract_target_payload(text)
        if not has_single_target_marker(text):
            row_issues.append("invalid_marker_count")
        if not target_text:
            row_issues.append("empty_target")
        if canonical_label(row.get("label")) not in {"true", "false"}:
            row_issues.append("invalid_label")
        if not str(row.get("teacher_reason", "")).strip():
            row_issues.append("missing_teacher_reason")
        if row_issues:
            issues.append(
                {
                    "sample_id": row.get("sample_id", ""),
                    "issues": row_issues,
                    "text": text,
                }
            )
    return {
        "row_count": len(rows),
        "issue_count": len(issues),
        "issue_examples": issues[:20],
    }


def source_document_id(source_node_id: str) -> str:
    parts = str(source_node_id).split(":")
    if parts and parts[0] == "document":
        return str(source_node_id)
    if len(parts) <= 2:
        return str(source_node_id)
    tail_segments = SOURCE_NODE_TRAILING_SEGMENTS.get(parts[0], 1)
    if len(parts) <= tail_segments + 1:
        return ":".join(parts[1:])
    return ":".join(parts[1:-tail_segments])


def source_node_granularity(source_node_id: str) -> str:
    prefix = str(source_node_id).split(":", 1)[0]
    if prefix in {"document", "segment", "appendix"}:
        return "article"
    if prefix in {"article", "paragraph", "item", "sub_item"}:
        return prefix
    return "article"


def source_category_from_sample_id(sample_id: str) -> str:
    category = str(sample_id).split(":", 1)[0]
    if category == "judicial":
        category = "interpretation"
    if category == "local":
        category = "regulation"
    if category not in SOURCE_CATEGORY_ORDER:
        raise ValueError(f"Unsupported sample source category: {sample_id}")
    return category


def target_category_signature(target_categories: list[str]) -> str:
    values = [str(value).strip() for value in target_categories if str(value).strip()]
    return "|".join(sorted(dict.fromkeys(values)))


def compact_text(sentence: str) -> str:
    return re.sub(r"\s+", "", sentence)


def counter_to_dict(counter: Counter[Any], order: tuple[str, ...]) -> dict[str, int]:
    keys = list(order) + [key for key in counter if key not in order]
    return {str(key): int(counter.get(key, 0)) for key in keys if counter.get(key, 0) > 0}


def extract_target_payload(marked_text: str) -> str:
    match = re.search(r"\[T\](?P<target>.+?)\[/T\]", str(marked_text or ""), re.DOTALL)
    return match.group("target").strip() if match else ""


def invoke_distill_sample_batch(
    samples: list[dict[str, Any]],
    distill_config: dict[str, Any],
    cancel_event: Any | None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    try:
        return distill_sample_batch(samples, distill_config, cancel_event=cancel_event)
    except TypeError as exc:
        if "cancel_event" not in str(exc):
            raise
        return distill_sample_batch(samples, distill_config)
