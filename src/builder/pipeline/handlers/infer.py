from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace

from ...contracts import (
    AlignRelationRecord,
    EquivalenceRecord,
    GraphBundle,
    InferPairRecord,
    SubstageStateManifest,
    infer_pass_index_from_substage,
    infer_pass_substage_name,
    sanitize_manifest_stats,
    substage_artifacts,
    substage_inputs,
    substage_unit,
)
from ...io import (
    BuildLayout,
    read_align_canonical_concepts,
    read_align_relations,
    read_concept_vectors,
    read_infer_pairs,
    read_infer_relations,
    read_stage_manifest,
    write_infer_pairs,
    write_infer_relations,
    write_job_log,
)
from ...stages import run_infer
from ...stages.infer.materialize import normalize_infer_pair
from ...stages.infer.recall import resolve_recall_pass_configs
from ...utils.ids import timestamp_utc
from ...utils.locator import source_id_from_node_id
from ..incremental import ReuseDecision, build_upstream_signature_for_stage, get_infer_reuse_decision
from .common import (
    HandlerResult,
    StageContext,
    build_stage_manifest,
    build_stage_work_stats,
    dynamic_stage_progress_callback,
    emit_prefilled_stage_progress,
    load_graph_snapshot,
    merge_unit_substage_manifest,
    normalize_source_ids,
    normalize_unit_ids,
    resolve_stage_source_ids,
    write_stage_graph,
    write_stage_state,
)


@dataclass
class InferStageState:
    pairs: list[InferPairRecord]
    relations: list[AlignRelationRecord]


def decide_reuse(layout, *, force_rebuild: bool = False) -> ReuseDecision:
    return get_infer_reuse_decision(layout, force_rebuild=force_rebuild)


def infer_artifact_paths(layout: BuildLayout) -> dict[str, str]:
    paths = {
        "primary": str(layout.stage_nodes_path("infer")),
        "relations": str(layout.infer_relations_path()),
        "nodes": str(layout.stage_nodes_path("infer")),
        "edges": str(layout.stage_edges_path("infer")),
    }
    for path in layout.infer_pair_paths():
        paths[path.stem] = str(path)
    return paths


def read_infer_stage_state(layout: BuildLayout) -> InferStageState:
    pair_rows: list[InferPairRecord] = []
    for path in layout.infer_pair_paths():
        pair_rows.extend(read_infer_pairs(path))
    return InferStageState(
        pairs=dedupe_infer_pairs(pair_rows),
        relations=read_infer_relations(layout.infer_relations_path()) if layout.infer_relations_path().exists() else [],
    )


def infer_reused_concept_count(layout) -> int:
    manifest_path = layout.stage_manifest_path("infer")
    if not manifest_path.exists():
        return 0
    manifest = read_stage_manifest(manifest_path)
    pass_states = [
        state
        for name, state in manifest.substages.items()
        if infer_pass_index_from_substage(name) is not None
    ]
    if not pass_states:
        return int(manifest.stats.get("work_units_total", 0) or manifest.stats.get("concept_count", 0) or 0)
    counts: list[int] = []
    for pass_state in pass_states:
        recall_state = pass_state.substages.get("recall")
        if recall_state is not None:
            counts.append(len(recall_state.processed_units))
    return min(counts) if counts else 0


def infer_reused_pair_counts_by_pass(layout) -> dict[int, tuple[int, int]]:
    manifest_path = layout.stage_manifest_path("infer")
    if not manifest_path.exists():
        return {}
    manifest = read_stage_manifest(manifest_path)
    counts: dict[int, tuple[int, int]] = {}
    for name, pass_state in manifest.substages.items():
        pass_index = infer_pass_index_from_substage(name)
        if pass_index is None:
            continue
        recall_state = pass_state.substages.get("recall")
        judge_state = pass_state.substages.get("judge")
        total = int((recall_state.stats if recall_state is not None else pass_state.stats).get("pair_count", 0) or 0)
        judged = int((judge_state.stats if judge_state is not None else pass_state.stats).get("judgment_count", 0) or 0)
        counts[pass_index] = (judged, total)
    return counts


def build_infer_recall_pass_manifest_stats(
    pairs: list[InferPairRecord],
    *,
    pass_index: int,
) -> dict[str, int]:
    pass_pairs = [row for row in pairs if int(row.pass_index) == int(pass_index)]
    return {
        "pair_count": len(pass_pairs),
        "result_count": len(pass_pairs),
    }


def build_infer_judge_manifest_stats(pairs: list[InferPairRecord], *, min_strength: int = 7) -> dict[str, int]:
    judged_pairs = [row for row in pairs if row.relation]
    return {
        "judgment_count": len(judged_pairs),
        "result_count": len(judged_pairs),
        "accepted_count": sum(
            1
            for row in judged_pairs
            if row.relation != "none" and int(row.strength) >= int(min_strength)
        ),
        "none_count": sum(1 for row in judged_pairs if row.relation == "none"),
        "related_count": sum(1 for row in judged_pairs if row.relation == "related"),
        "is_subordinate_count": sum(1 for row in judged_pairs if row.relation == "is_subordinate"),
        "has_subordinate_count": sum(1 for row in judged_pairs if row.relation == "has_subordinate"),
    }


def build_infer_judge_pass_manifest_stats(
    pairs: list[InferPairRecord],
    *,
    pass_index: int,
    min_strength: int = 7,
) -> dict[str, int]:
    pass_pairs = [row for row in pairs if int(row.pass_index) == int(pass_index)]
    return build_infer_judge_manifest_stats(pass_pairs, min_strength=min_strength)


def build_infer_pass_substage_manifest(
    *,
    layout: BuildLayout,
    pass_index: int,
    processed_concept_ids: list[str],
    pairs: list[InferPairRecord],
    processed_pair_ids: list[str],
    min_strength: int,
    recall_fingerprint: str,
    judge_fingerprint: str,
) -> SubstageStateManifest:
    pass_pairs = [row for row in pairs if int(row.pass_index) == int(pass_index)]
    pass_judged_pairs = [row for row in pass_pairs if row.relation]
    pass_name = infer_pass_substage_name(pass_index)
    return SubstageStateManifest(
        inputs=substage_inputs(layout, "infer", pass_name),
        artifacts=substage_artifacts(layout, "infer", pass_name),
        updated_at=timestamp_utc(),
        unit=substage_unit("infer", pass_name),
        stats=sanitize_manifest_stats(
            {
                "pair_count": len(pass_pairs),
                "judgment_count": len(pass_judged_pairs),
                "result_count": len(pass_pairs),
                "accepted_count": build_infer_judge_manifest_stats(
                    pass_pairs,
                    min_strength=min_strength,
                ).get("accepted_count", 0),
            },
            stage_name="infer",
            substage_name=pass_name,
        ),
        metadata={"pass_index": int(pass_index)},
        processed_units=[],
        substages={
            "recall": SubstageStateManifest(
                inputs=[
                    str(layout.align_concepts_path()),
                    str(layout.align_vectors_path()),
                    str(layout.align_relations_path()),
                    str(layout.stage_nodes_path("align")),
                    str(layout.stage_edges_path("align")),
                ],
                artifacts=[str(layout.infer_pairs_path(pass_index))],
                updated_at=timestamp_utc(),
                unit="concept",
                stats=build_infer_recall_pass_manifest_stats(pass_pairs, pass_index=pass_index),
                metadata={"scope_fingerprint": recall_fingerprint},
                processed_units=normalize_unit_ids(processed_concept_ids),
            ),
            "judge": SubstageStateManifest(
                inputs=[
                    str(layout.align_concepts_path()),
                    str(layout.stage_nodes_path("align")),
                    str(layout.stage_edges_path("align")),
                    str(layout.infer_pairs_path(pass_index)),
                ],
                artifacts=[str(layout.infer_pairs_path(pass_index))],
                updated_at=timestamp_utc(),
                unit="pair",
                stats=build_infer_judge_pass_manifest_stats(
                    pass_pairs,
                    pass_index=pass_index,
                    min_strength=min_strength,
                ),
                metadata={"scope_fingerprint": judge_fingerprint},
                processed_units=normalize_unit_ids(processed_pair_ids),
            ),
        },
    )


def stable_payload_fingerprint(payload: object) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()


def infer_recall_fingerprint(
    scope_concepts: list[EquivalenceRecord],
    all_concepts: list[EquivalenceRecord],
    align_relations: list[AlignRelationRecord],
    graph_bundle: GraphBundle | None = None,
) -> str:
    del graph_bundle
    payload = {
        "scope_concepts": [row.to_dict() for row in sorted(scope_concepts, key=lambda item: item.id)],
        "all_concepts": [row.to_dict() for row in sorted(all_concepts, key=lambda item: item.id)],
        "align_relations": [row.to_dict() for row in dedupe_align_relations(align_relations)],
    }
    return stable_payload_fingerprint(payload)


def infer_judge_fingerprint(
    scope_pairs: list[InferPairRecord],
    scope_concepts: list[EquivalenceRecord],
) -> str:
    payload = {
        "pairs": [infer_pair_recall_payload(row) for row in dedupe_infer_pairs(scope_pairs)],
        "scope_concepts": [row.to_dict() for row in sorted(scope_concepts, key=lambda item: item.id)],
    }
    return stable_payload_fingerprint(payload)


def infer_pair_recall_payload(row: InferPairRecord) -> dict[str, object]:
    payload = row.to_dict()
    payload["relation"] = ""
    payload["strength"] = 0
    return payload


def select_infer_pairs_for_scope(rows: list[InferPairRecord], concept_ids: list[str]) -> list[InferPairRecord]:
    concept_id_set = set(normalize_unit_ids(concept_ids))
    return [
        row
        for row in dedupe_infer_pairs(rows)
        if row.left_id in concept_id_set or row.right_id in concept_id_set
    ]


def merge_recalled_pairs_preserving_judgment(
    existing: list[InferPairRecord],
    recalled: list[InferPairRecord],
) -> list[InferPairRecord]:
    judged_by_key = {
        (row.left_id, row.right_id, int(row.pass_index)): row
        for row in existing
        if row.relation
    }
    merged: list[InferPairRecord] = []
    for row in list(existing) + list(recalled):
        judged = judged_by_key.get((row.left_id, row.right_id, int(row.pass_index)))
        if judged is not None:
            merged.append(replace(row, relation=judged.relation, strength=judged.strength))
        else:
            merged.append(row)
    return dedupe_infer_pairs(merged)


def source_ids_for_roots(root_ids: list[str]) -> list[str]:
    return normalize_source_ids([source_id_from_node_id(root_id) for root_id in root_ids if root_id])


def infer_source_ids_for_concept_ids(
    scope_concepts: list[EquivalenceRecord],
    concept_ids: list[str],
) -> list[str]:
    concept_id_set = set(normalize_unit_ids(concept_ids))
    root_ids = [
        root_id
        for row in scope_concepts
        if row.id in concept_id_set
        for root_id in row.root_ids
    ]
    return source_ids_for_roots(root_ids)


def dedupe_align_relations(rows: list[AlignRelationRecord]) -> list[AlignRelationRecord]:
    deduped = {(row.left_id, row.right_id, row.relation): row for row in rows if row.left_id and row.right_id}
    return [deduped[key] for key in sorted(deduped)]


def dedupe_infer_pairs(rows: list[InferPairRecord]) -> list[InferPairRecord]:
    deduped: dict[tuple[str, str, int], InferPairRecord] = {}
    for row in rows:
        key = (row.left_id, row.right_id, int(row.pass_index))
        previous = deduped.get(key)
        if previous is None:
            deduped[key] = row
            continue
        if row.relation and not previous.relation:
            deduped[key] = row
            continue
        if previous.relation and not row.relation:
            deduped[key] = replace(row, relation=previous.relation, strength=previous.strength)
            continue
        if float(row.score) >= float(previous.score):
            deduped[key] = row
    return [deduped[key] for key in sorted(deduped)]


def count_judged_infer_pairs(rows: list[InferPairRecord]) -> int:
    return sum(1 for row in rows if row.relation)


def write_infer_stage_artifacts(
    layout: BuildLayout,
    state: InferStageState,
    *,
    graph_bundle: GraphBundle | None,
    min_strength: int,
) -> None:
    for path in [layout.stage_dir("infer") / "candidates.jsonl", layout.stage_dir("infer") / "judgments.jsonl"]:
        _unlink_if_exists(path)
    for path in sorted(layout.stage_dir("infer").glob("candidates_*.jsonl")):
        _unlink_if_exists(path)
    for path in sorted(layout.stage_dir("infer").glob("judgments_*.jsonl")):
        _unlink_if_exists(path)

    deduped_pairs = dedupe_infer_pairs(state.pairs)
    inferred_relations = [
        row
        for row in (
            normalize_infer_pair(pair, min_strength=min_strength)
            for pair in deduped_pairs
        )
        if row is not None
    ]
    write_infer_relations(layout.infer_relations_path(), dedupe_align_relations(inferred_relations))
    pair_passes = {int(row.pass_index) for row in deduped_pairs if int(row.pass_index) > 0}
    for pass_index in sorted(pair_passes):
        write_infer_pairs(
            layout.infer_pairs_path(pass_index),
            [row for row in deduped_pairs if int(row.pass_index) == pass_index],
        )
    for path in layout.infer_pair_paths():
        stem = path.stem.split("_", 1)
        if len(stem) == 2 and stem[1].isdigit() and int(stem[1]) not in pair_passes:
            _unlink_if_exists(path)
    if graph_bundle is not None:
        write_stage_graph(
            layout,
            "infer",
            graph_bundle,
            write_nodes=True,
            write_edges=True,
        )


def build_infer_update_stats(
    previous_relations: list[AlignRelationRecord],
    current_relations: list[AlignRelationRecord],
) -> dict[str, int]:
    previous_keys = {
        (row.left_id, row.right_id, row.relation)
        for row in previous_relations
        if row.left_id and row.right_id and row.relation
    }
    current_keys = {
        (row.left_id, row.right_id, row.relation)
        for row in current_relations
        if row.left_id and row.right_id and row.relation
    }
    return {
        "updated_nodes": 0,
        "updated_edges": len(previous_keys.symmetric_difference(current_keys)),
    }


def _unlink_if_exists(path) -> None:
    if path.exists():
        path.unlink()


def run(ctx: StageContext) -> HandlerResult:
    stage_name = ctx.stage_name
    layout = ctx.layout
    input_source_ids = resolve_stage_source_ids(layout, stage_name, ctx.selected_source_ids)
    reuse_decision = decide_reuse(layout, force_rebuild=ctx.force_rebuild)
    if reuse_decision.is_full:
        infer_manifest = read_stage_manifest(layout.stage_manifest_path(stage_name))
        reused_concept_count = infer_reused_concept_count(layout)
        for pass_index, (judged_count, pair_count) in sorted(infer_reused_pair_counts_by_pass(layout).items()):
            pass_name = infer_pass_substage_name(pass_index)
            emit_prefilled_stage_progress(
                ctx.stage_progress_callback,
                f"{stage_name}::{pass_name}::recall",
                current=reused_concept_count,
                total=reused_concept_count,
            )
            emit_prefilled_stage_progress(
                ctx.stage_progress_callback,
                f"{stage_name}::{pass_name}::judge",
                current=judged_count,
                total=pair_count,
            ) if pair_count > 0 else (
                ctx.stage_progress_callback(f"{stage_name}::{pass_name}::judge", 0, 0)
                if ctx.stage_progress_callback is not None
                else None
            )
        ctx.stage_record.artifact_paths = infer_artifact_paths(layout)
        ctx.stage_record.stats = build_stage_work_stats(
            input_source_ids=input_source_ids,
            processed_source_ids=[],
            skipped_source_ids=input_source_ids,
            work_units_total=reused_concept_count,
            work_units_completed=0,
            work_units_skipped=reused_concept_count,
            updated_nodes=0,
            updated_edges=0,
            llm_request_count=0,
            llm_error_count=0,
            retry_count=0,
        ) | dict(infer_manifest.stats)
        write_stage_state(
            layout,
            build_stage_manifest(
                stage_name=stage_name,
                layout=layout,
                job_id=ctx.job_id,
                build_target=ctx.source_path_label,
                source_ids=[],
                processed_source_ids=[],
                processed_unit_ids=[],
                input_stage=ctx.graph_input_stage[stage_name],
                artifact_paths=infer_artifact_paths(layout),
                stats=ctx.stage_record.stats,
                metadata={"upstream_signature": reuse_decision.upstream_signature},
            ),
        )
        ctx.stage_record.failures = []
        return HandlerResult(current_graph=None, suppress_graph_update_stats=True)

    base_graph = load_graph_snapshot(
        layout,
        ctx.graph_input_stage[stage_name],
        stage_sequence=ctx.stage_sequence,
        graph_stages=ctx.graph_stages,
    )
    align_concepts = read_align_canonical_concepts(layout.align_concepts_path()) if layout.align_concepts_path().exists() else []
    align_vectors = read_concept_vectors(layout.align_vectors_path()) if layout.align_vectors_path().exists() else []
    align_relations = read_align_relations(layout.align_relations_path()) if layout.align_relations_path().exists() else []
    scope_concepts = list(align_concepts)
    scoped_concept_ids = normalize_unit_ids([row.id for row in scope_concepts])
    existing_state = read_infer_stage_state(layout)
    previous_relations = list(existing_state.relations)
    judge_cache_pairs = dedupe_infer_pairs(existing_state.pairs)
    retained_pairs: list[InferPairRecord] = []
    retained_relations: list[AlignRelationRecord] = []
    pending_seed_judge_pairs: list[InferPairRecord] = []
    reusable_judge_pair_count_by_pass: dict[int, int] = {}

    ctx.stage_record.artifact_paths = infer_artifact_paths(layout)
    judge_min_strength = max(int(ctx.runtime.infer_config().get("judge", {}).get("min_strength", 7) or 7), 0)
    substage_states: dict[str, SubstageStateManifest] = {}
    write_stage_state(
        layout,
        build_stage_manifest(
            stage_name=stage_name,
            layout=layout,
            job_id=ctx.job_id,
            build_target=ctx.source_path_label,
            source_ids=[],
            processed_source_ids=[],
            processed_unit_ids=[],
            input_stage=ctx.graph_input_stage[stage_name],
            artifact_paths=ctx.stage_record.artifact_paths,
            stats={},
            status="running",
            substage_states=substage_states,
        ),
    )

    current_pair_state: dict[str, list[InferPairRecord]] = {"pairs": list(retained_pairs)}
    recall_fingerprint = infer_recall_fingerprint(scope_concepts, align_concepts, align_relations)

    def checkpoint_infer_recall(
        pass_index: int,
        snapshot_pairs: list[InferPairRecord],
        snapshot_stats: dict[str, int],
        processed_concept_ids: list[str],
    ) -> None:
        merged_pairs = merge_recalled_pairs_preserving_judgment(retained_pairs, snapshot_pairs)
        merged_state = InferStageState(pairs=merged_pairs, relations=retained_relations)
        write_infer_stage_artifacts(layout, merged_state, graph_bundle=None, min_strength=judge_min_strength)
        pass_pairs = [row for row in merged_state.pairs if int(row.pass_index) == int(pass_index)]
        pass_name = infer_pass_substage_name(pass_index)
        substage_states[pass_name] = merge_unit_substage_manifest(
            parent_stage="infer",
            stage_name=pass_name,
            previous=substage_states.get(pass_name),
            current=build_infer_pass_substage_manifest(
                layout=layout,
                pass_index=pass_index,
                processed_concept_ids=processed_concept_ids,
                pairs=pass_pairs,
                processed_pair_ids=[f"{row.left_id}\t{row.right_id}" for row in pass_pairs if row.relation],
                min_strength=judge_min_strength,
                recall_fingerprint=recall_fingerprint,
                judge_fingerprint=infer_judge_fingerprint(pass_pairs, scope_concepts),
            ),
        )
        ctx.stage_record.stats = dict(ctx.stage_record.stats) | {
            "pair_count": len(merged_state.pairs),
            "result_count": len(merged_state.pairs),
            **dict(snapshot_stats),
        }
        write_stage_state(
            layout,
            build_stage_manifest(
                stage_name=stage_name,
                layout=layout,
                job_id=ctx.job_id,
                build_target=ctx.source_path_label,
                source_ids=[],
                processed_source_ids=[],
                processed_unit_ids=[],
                input_stage=ctx.graph_input_stage[stage_name],
                artifact_paths=infer_artifact_paths(layout),
                stats=ctx.stage_record.stats,
                status="running",
                substage_states=substage_states,
            ),
        )
        write_job_log(layout.job_log_path(ctx.job_id), ctx.log_record)

    def checkpoint_infer_judge(
        pass_index: int,
        snapshot_pairs: list[InferPairRecord],
        snapshot_stats: dict[str, int],
        processed_pair_ids: list[str],
        llm_error_summary: list[dict[str, object]],
        snapshot_relations: list[AlignRelationRecord],
    ) -> None:
        merged_pairs = merge_recalled_pairs_preserving_judgment(current_pair_state["pairs"], snapshot_pairs)
        merged_relations = dedupe_align_relations(retained_relations + list(snapshot_relations))
        merged_state = InferStageState(pairs=merged_pairs, relations=merged_relations)
        write_infer_stage_artifacts(layout, merged_state, graph_bundle=None, min_strength=judge_min_strength)
        pass_pairs = [row for row in merged_state.pairs if int(row.pass_index) == int(pass_index)]
        pass_name = infer_pass_substage_name(pass_index)
        reused_pair_ids = [f"{row.left_id}\t{row.right_id}" for row in retained_pairs if row.relation]
        substage_states[pass_name] = merge_unit_substage_manifest(
            parent_stage="infer",
            stage_name=pass_name,
            previous=substage_states.get(pass_name),
            current=build_infer_pass_substage_manifest(
                layout=layout,
                pass_index=pass_index,
                processed_concept_ids=[],
                pairs=pass_pairs,
                processed_pair_ids=normalize_unit_ids(reused_pair_ids + processed_pair_ids),
                min_strength=judge_min_strength,
                recall_fingerprint=recall_fingerprint,
                judge_fingerprint=infer_judge_fingerprint(pass_pairs, scope_concepts),
            ),
        )
        ctx.stage_record.failures = [dict(item) for item in llm_error_summary]
        ctx.stage_record.stats = dict(ctx.stage_record.stats) | {
            "judgment_count": count_judged_infer_pairs(merged_state.pairs),
            "result_count": count_judged_infer_pairs(merged_state.pairs),
            **dict(snapshot_stats),
        }
        write_stage_state(
            layout,
            build_stage_manifest(
                stage_name=stage_name,
                layout=layout,
                job_id=ctx.job_id,
                build_target=ctx.source_path_label,
                source_ids=[],
                processed_source_ids=[],
                processed_unit_ids=[],
                input_stage=ctx.graph_input_stage[stage_name],
                artifact_paths=infer_artifact_paths(layout),
                stats=ctx.stage_record.stats,
                status="running",
                substage_states=substage_states,
            ),
        )
        write_job_log(layout.job_log_path(ctx.job_id), ctx.log_record)

    def capture_infer_recall_snapshot(
        pass_index: int,
        snapshot_pairs: list[InferPairRecord],
        snapshot_stats: dict[str, int],
        processed_concept_ids: list[str],
    ) -> None:
        current_pair_state["pairs"] = merge_recalled_pairs_preserving_judgment(retained_pairs, snapshot_pairs)
        checkpoint_infer_recall(pass_index, snapshot_pairs, snapshot_stats, processed_concept_ids)

    infer_result = run_infer(
        base_graph,
        ctx.runtime,
        concepts=align_concepts,
        raw_vectors=align_vectors,
        align_relations=align_relations,
        retained_pairs=retained_pairs,
        retained_relations=retained_relations,
        scoped_concepts=scope_concepts,
        seed_judge_pairs=pending_seed_judge_pairs,
        judge_cache_pairs=judge_cache_pairs,
        recall_progress_callback_for_pass=lambda pass_index: dynamic_stage_progress_callback(
            ctx.stage_progress_callback,
            f"{stage_name}::{infer_pass_substage_name(pass_index)}::recall",
            skipped_units=0,
        ),
        judge_progress_callback_for_pass=lambda pass_index: dynamic_stage_progress_callback(
            ctx.stage_progress_callback,
            f"{stage_name}::{infer_pass_substage_name(pass_index)}::judge",
            skipped_units=reusable_judge_pair_count_by_pass.get(int(pass_index), 0),
        ),
        recall_checkpoint_every=ctx.runtime.substage_checkpoint_every(stage_name, "recall"),
        judge_checkpoint_every=ctx.runtime.substage_checkpoint_every(stage_name, "judge"),
        recall_checkpoint_callback=capture_infer_recall_snapshot,
        judge_checkpoint_callback=checkpoint_infer_judge,
        cancel_event=ctx.cancel_event,
    )
    final_pairs = merge_recalled_pairs_preserving_judgment(retained_pairs, infer_result.pairs)
    final_relations = dedupe_align_relations(list(infer_result.relations))
    final_state = InferStageState(pairs=final_pairs, relations=final_relations)
    write_infer_stage_artifacts(layout, final_state, graph_bundle=infer_result.graph_bundle, min_strength=judge_min_strength)

    scope_pairs = select_infer_pairs_for_scope(final_pairs, scoped_concept_ids)
    infer_pass_count = len(resolve_recall_pass_configs(ctx.runtime))
    for pass_index in range(1, infer_pass_count + 1):
        pass_pairs = [row for row in scope_pairs if int(row.pass_index) == pass_index]
        substage_states[infer_pass_substage_name(pass_index)] = build_infer_pass_substage_manifest(
            layout=layout,
            pass_index=pass_index,
            processed_concept_ids=scoped_concept_ids,
            pairs=pass_pairs,
            processed_pair_ids=[f"{row.left_id}\t{row.right_id}" for row in pass_pairs if row.relation],
            min_strength=judge_min_strength,
            recall_fingerprint=recall_fingerprint,
            judge_fingerprint=infer_judge_fingerprint(pass_pairs, scope_concepts),
        )

    processed_concept_ids = scoped_concept_ids
    ctx.stage_record.stats = build_stage_work_stats(
        input_source_ids=input_source_ids,
        processed_source_ids=infer_source_ids_for_concept_ids(scope_concepts, processed_concept_ids),
        skipped_source_ids=[],
        work_units_total=len(scoped_concept_ids),
        work_units_completed=len(processed_concept_ids),
        work_units_skipped=0,
        updated_nodes=int(infer_result.stats.get("updated_nodes", 0)),
        updated_edges=int(infer_result.stats.get("updated_edges", 0)),
        pair_count=len(final_pairs),
        judgment_count=count_judged_infer_pairs(final_pairs),
        relation_count=len(final_relations),
        accepted_count=int(infer_result.stats.get("accepted_count", 0)),
        llm_request_count=int(infer_result.stats.get("llm_request_count", 0)),
        llm_error_count=int(infer_result.stats.get("llm_error_count", 0)),
        retry_count=int(infer_result.stats.get("retry_count", 0)),
    ) | build_infer_update_stats(previous_relations, final_relations)
    ctx.stage_record.failures = [dict(item) for item in infer_result.llm_errors]
    write_stage_state(
        layout,
        build_stage_manifest(
            stage_name=stage_name,
            layout=layout,
            job_id=ctx.job_id,
            build_target=ctx.source_path_label,
            source_ids=[],
            processed_source_ids=[],
            processed_unit_ids=[],
            input_stage=ctx.graph_input_stage[stage_name],
            artifact_paths=infer_artifact_paths(layout),
            stats=ctx.stage_record.stats,
            metadata={"upstream_signature": build_upstream_signature_for_stage(layout, ctx.graph_input_stage[stage_name])},
            graph_bundle=infer_result.graph_bundle,
            substage_states=substage_states,
        ),
    )
    return HandlerResult(current_graph=infer_result.graph_bundle, suppress_graph_update_stats=True)
