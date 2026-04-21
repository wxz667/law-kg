from __future__ import annotations

from dataclasses import replace
from typing import Any, Callable

from ...contracts import AlignRelationRecord, EquivalenceRecord, GraphBundle, InferPairRecord
from .features import build_feature_store
from .judge import judge_pairs, pair_is_accepted, resolve_infer_judge_runtime_config
from .materialize import build_materialize_stats, materialize_graph, normalize_infer_pair
from .recall import build_semantic_shortlists, resolve_recall_pass_configs, run_recall_pass
from .types import InferResult


def run(
    base_graph: GraphBundle,
    runtime: Any,
    *,
    concepts: list[EquivalenceRecord],
    raw_vectors: list,
    align_relations: list[AlignRelationRecord],
    retained_pairs: list[InferPairRecord],
    retained_relations: list[AlignRelationRecord],
    scoped_concepts: list[EquivalenceRecord],
    seed_judge_pairs: list[InferPairRecord] | None = None,
    judge_cache_pairs: list[InferPairRecord] | None = None,
    recall_progress_callback: Callable[[int, int], None] | None = None,
    judge_progress_callback: Callable[[int, int], None] | None = None,
    recall_progress_callback_for_pass: Callable[[int], Callable[[int, int], None] | None] | None = None,
    judge_progress_callback_for_pass: Callable[[int], Callable[[int, int], None] | None] | None = None,
    recall_checkpoint_every: int = 0,
    judge_checkpoint_every: int = 0,
    recall_checkpoint_callback: Callable[[int, list[InferPairRecord], dict[str, int], list[str]], None] | None = None,
    judge_checkpoint_callback: Callable[[int, list[InferPairRecord], dict[str, int], list[str], list[dict[str, object]], list[AlignRelationRecord]], None] | None = None,
    cancel_event=None,
) -> InferResult:
    scoped_concept_ids = [row.id for row in scoped_concepts if row.id]
    pass_configs = resolve_recall_pass_configs(runtime)
    current_pairs = dedupe_pairs(list(retained_pairs))
    current_infer_relations = dedupe_relations(list(retained_relations))
    blocked_pair_keys = {(row.left_id, row.right_id) for row in current_pairs}
    concepts_by_id = {row.id: row for row in concepts}
    judge_cache = build_judge_cache(judge_cache_pairs or [], concepts_by_id)

    feature_store_for_shortlists = build_feature_store(
        concepts,
        raw_vectors,
        dedupe_relations(list(align_relations) + list(current_infer_relations)),
        base_graph,
    )
    semantic_shortlists = build_semantic_shortlists(
        runtime=runtime,
        scoped_concepts=scoped_concepts,
        all_concepts=concepts,
        feature_store=feature_store_for_shortlists,
    )
    judge_config = resolve_infer_judge_runtime_config(runtime)
    llm_errors: list[dict[str, object]] = []
    total_llm_request_count = 0
    total_retry_count = 0

    def apply_judge_pairs(pass_index: int, pairs_to_judge: list[InferPairRecord]) -> None:
        nonlocal current_pairs, current_infer_relations, llm_errors, total_llm_request_count, total_retry_count
        if not pairs_to_judge:
            return
        pass_judge_progress_callback = (
            judge_progress_callback_for_pass(pass_index)
            if judge_progress_callback_for_pass is not None
            else judge_progress_callback
        )
        cached_pairs, uncached_pairs = apply_judge_cache(pairs_to_judge, judge_cache, concepts_by_id)
        cached_pair_ids = [pair_id(row.left_id, row.right_id) for row in cached_pairs]

        def report_judge_progress(current: int, _total: int) -> None:
            if pass_judge_progress_callback is None:
                return
            pass_judge_progress_callback(
                min(len(cached_pairs) + int(current), len(pairs_to_judge)),
                len(pairs_to_judge),
            )

        retained_pairs_before_pass = list(current_pairs)
        retained_relations_before_pass = list(current_infer_relations)

        def checkpoint_judge(
            snapshot_pairs: list[InferPairRecord],
            snapshot_stats: dict[str, int],
            processed_pair_ids: list[str],
            llm_error_summary: list[dict[str, object]],
        ) -> None:
            if judge_checkpoint_callback is None:
                return
            snapshot_with_cache = dedupe_pairs(list(cached_pairs) + list(snapshot_pairs))
            merged_pairs = dedupe_pairs(retained_pairs_before_pass + snapshot_with_cache)
            merged_relations = dedupe_relations(
                retained_relations_before_pass
                + [
                    row
                    for row in (
                        normalize_infer_pair(pair, min_strength=judge_config.min_strength)
                        for pair in snapshot_with_cache
                    )
                    if row is not None
                ]
            )
            judge_checkpoint_callback(
                pass_index,
                merged_pairs,
                {
                    "judgment_count": count_judged_pairs(merged_pairs),
                    "result_count": count_judged_pairs(merged_pairs),
                    **dict(snapshot_stats),
                },
                sorted(dict.fromkeys(cached_pair_ids + processed_pair_ids)),
                llm_error_summary,
                merged_relations,
            )

        if uncached_pairs:
            judge_result = judge_pairs(
                uncached_pairs,
                concepts=concepts,
                runtime=runtime,
                progress_callback=report_judge_progress,
                checkpoint_every=judge_checkpoint_every,
                checkpoint_callback=checkpoint_judge,
                cancel_event=cancel_event,
            )
            judged_pairs = dedupe_pairs(list(cached_pairs) + list(judge_result.pairs))
            llm_errors.extend(judge_result.llm_errors)
            total_llm_request_count += int(judge_result.stats.get("llm_request_count", 0))
            total_retry_count += int(judge_result.stats.get("retry_count", 0))
            processed_pair_ids = sorted(dict.fromkeys(cached_pair_ids + judge_result.processed_pair_ids))
            judge_llm_errors = judge_result.llm_errors
        else:
            judged_pairs = list(cached_pairs)
            processed_pair_ids = sorted(dict.fromkeys(cached_pair_ids))
            judge_llm_errors = []
            if pass_judge_progress_callback is not None:
                pass_judge_progress_callback(len(pairs_to_judge), len(pairs_to_judge))

        current_pairs = dedupe_pairs(list(current_pairs) + list(judged_pairs))
        accepted_relations = [
            row
            for row in (
                normalize_infer_pair(pair, min_strength=judge_config.min_strength)
                for pair in judged_pairs
            )
            if row is not None
        ]
        current_infer_relations = dedupe_relations(list(current_infer_relations) + accepted_relations)
        if judge_checkpoint_callback is not None:
            judge_checkpoint_callback(
                pass_index,
                list(current_pairs),
                {
                    "judgment_count": count_judged_pairs(current_pairs),
                    "result_count": count_judged_pairs(current_pairs),
                },
                processed_pair_ids,
                judge_llm_errors,
                list(current_infer_relations),
            )

    seed_pairs_by_pass: dict[int, list[InferPairRecord]] = {}
    for pair in dedupe_pairs(list(seed_judge_pairs or [])):
        seed_pairs_by_pass.setdefault(int(pair.pass_index), []).append(pair)
    if not scoped_concepts and not seed_pairs_by_pass and judge_progress_callback is not None:
        judge_progress_callback(0, 0)
    for pass_index in sorted(seed_pairs_by_pass):
        apply_judge_pairs(pass_index, seed_pairs_by_pass[pass_index])

    for pass_index, _pass_config in enumerate(pass_configs, start=1):
        pass_recall_progress_callback = (
            recall_progress_callback_for_pass(pass_index)
            if recall_progress_callback_for_pass is not None
            else recall_progress_callback
        )
        recall_progress = monotonic_progress_callback(pass_recall_progress_callback, total=len(scoped_concepts))
        seed_relations = dedupe_relations(list(align_relations) + list(current_infer_relations))
        feature_store = build_feature_store(concepts, raw_vectors, seed_relations, base_graph)
        retained_pairs_before_pass = list(current_pairs)

        def checkpoint_recall(
            snapshot_pairs: list[InferPairRecord],
            snapshot_stats: dict[str, int],
            processed_concept_ids: list[str],
        ) -> None:
            if recall_checkpoint_callback is None:
                return
            merged_pairs = dedupe_pairs(retained_pairs_before_pass + list(snapshot_pairs))
            recall_checkpoint_callback(
                pass_index,
                merged_pairs,
                {
                    "pair_count": len(merged_pairs),
                    "result_count": len(merged_pairs),
                    **dict(snapshot_stats),
                },
                processed_concept_ids,
            )

        recall_result = run_recall_pass(
            pass_index=pass_index,
            runtime=runtime,
            scoped_concepts=scoped_concepts,
            all_concepts=concepts,
            feature_store=feature_store,
            semantic_shortlists=semantic_shortlists,
            existing_relations=seed_relations,
            blocked_pair_keys=blocked_pair_keys,
            progress_callback=recall_progress,
            checkpoint_every=recall_checkpoint_every,
            checkpoint_callback=checkpoint_recall,
        )
        current_pairs = dedupe_pairs(list(current_pairs) + list(recall_result.pairs))
        blocked_pair_keys.update((row.left_id, row.right_id) for row in recall_result.pairs)
        if recall_checkpoint_callback is not None:
            recall_checkpoint_callback(
                pass_index,
                list(current_pairs),
                {
                    "pair_count": len(current_pairs),
                    "result_count": len(current_pairs),
                },
                sorted(scoped_concept_ids),
            )
        apply_judge_pairs(pass_index, recall_result.pairs)

    cumulative_relations = dedupe_relations(list(align_relations) + list(current_infer_relations))
    graph_bundle = materialize_graph(base_graph, cumulative_relations)
    materialize_stats = build_materialize_stats(cumulative_relations, graph_bundle)
    stats = {
        "pair_count": len(current_pairs),
        "judgment_count": count_judged_pairs(current_pairs),
        "relation_count": len(current_infer_relations),
        "accepted_count": sum(1 for row in current_pairs if pair_is_accepted(row, min_strength=judge_config.min_strength)),
        "llm_request_count": int(total_llm_request_count),
        "llm_error_count": len(llm_errors),
        "retry_count": int(total_retry_count),
        **materialize_stats,
    }
    return InferResult(
        pairs=list(current_pairs),
        relations=current_infer_relations,
        graph_bundle=graph_bundle,
        stats=stats,
        llm_errors=llm_errors,
    )


def dedupe_pairs(rows: list[InferPairRecord]) -> list[InferPairRecord]:
    deduped: dict[tuple[str, str, int], InferPairRecord] = {}
    for row in rows:
        key = (row.left_id, row.right_id, int(row.pass_index))
        previous = deduped.get(key)
        if previous is None or pair_rank(row) > pair_rank(previous):
            deduped[key] = row
    return [deduped[key] for key in sorted(deduped)]


def pair_rank(row: InferPairRecord) -> tuple[int, float]:
    return (1 if row.relation else 0, float(row.score))


def count_judged_pairs(rows: list[InferPairRecord]) -> int:
    return sum(1 for row in rows if row.relation)


def build_judge_cache(
    rows: list[InferPairRecord],
    concepts_by_id: dict[str, EquivalenceRecord],
) -> dict[tuple[str, str, str, str], InferPairRecord]:
    cache: dict[tuple[str, str, str, str], InferPairRecord] = {}
    for row in rows:
        if not row.relation:
            continue
        key = judge_cache_key(row, concepts_by_id)
        if key is None:
            continue
        cache[key] = row
    return cache


def apply_judge_cache(
    rows: list[InferPairRecord],
    cache: dict[tuple[str, str, str, str], InferPairRecord],
    concepts_by_id: dict[str, EquivalenceRecord],
) -> tuple[list[InferPairRecord], list[InferPairRecord]]:
    cached: list[InferPairRecord] = []
    uncached: list[InferPairRecord] = []
    for row in rows:
        key = judge_cache_key(row, concepts_by_id)
        cached_row = cache.get(key) if key is not None else None
        if cached_row is None:
            uncached.append(row)
            continue
        cached.append(replace(row, relation=cached_row.relation, strength=cached_row.strength))
    return cached, uncached


def judge_cache_key(
    row: InferPairRecord,
    concepts_by_id: dict[str, EquivalenceRecord],
) -> tuple[str, str, str, str] | None:
    left = concepts_by_id.get(row.left_id)
    right = concepts_by_id.get(row.right_id)
    if left is None or right is None:
        return None
    return (
        normalize_cache_text(left.name),
        normalize_cache_text(left.description),
        normalize_cache_text(right.name),
        normalize_cache_text(right.description),
    )


def normalize_cache_text(value: str) -> str:
    return " ".join(str(value or "").split())


def pair_id(left_id: str, right_id: str) -> str:
    return f"{left_id}\t{right_id}"


def dedupe_relations(rows: list[AlignRelationRecord]) -> list[AlignRelationRecord]:
    deduped = {(row.left_id, row.right_id, row.relation): row for row in rows if row.left_id and row.right_id}
    return [deduped[key] for key in sorted(deduped)]


def monotonic_progress_callback(callback, *, total: int):
    state = {"current": 0}

    def report(current: int, _reported_total: int) -> None:
        if callback is None:
            return
        if total <= 0:
            callback(0, 0)
            return
        state["current"] = max(state["current"], min(max(int(current), 0), total))
        callback(state["current"], total)

    return report
