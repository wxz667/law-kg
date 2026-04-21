from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace

from ...contracts import (
    AggregateConceptRecord,
    AlignConceptRecord,
    AlignPairRecord,
    AlignRelationRecord,
    ConceptVectorRecord,
    EquivalenceRecord,
    GraphBundle,
    SubstageStateManifest,
)
from ...io import (
    BuildLayout,
    read_aggregate_concepts,
    read_align_canonical_concepts,
    read_align_pairs,
    read_align_relations,
    read_concept_vectors,
    read_stage_manifest,
    write_align_canonical_concepts,
    write_align_pairs,
    write_align_relations,
    write_concept_vectors,
    write_job_log,
)
from ...stages import run_align
from ...utils.ids import timestamp_utc
from ...utils.locator import source_id_from_node_id
from .common import (
    HandlerResult,
    StageContext,
    build_stage_manifest,
    build_stage_work_stats,
    build_unit_substage_manifest,
    dynamic_stage_progress_callback,
    emit_prefilled_stage_progress,
    load_graph_snapshot,
    normalize_source_ids,
    normalize_unit_ids,
    resolve_stage_source_ids,
    reusable_substage_unit_ids,
    stage_outputs_exist,
    write_stage_graph,
    write_stage_state,
)


@dataclass
class AlignStageState:
    concepts: list[EquivalenceRecord]
    vectors: list[ConceptVectorRecord]
    pairs: list[AlignPairRecord]
    relations: list[AlignRelationRecord]


def read_align_stage_state(layout: BuildLayout) -> AlignStageState:
    return AlignStageState(
        concepts=read_align_canonical_concepts(layout.align_concepts_path()) if layout.align_concepts_path().exists() else [],
        vectors=read_concept_vectors(layout.align_vectors_path()) if layout.align_vectors_path().exists() else [],
        pairs=read_align_pairs(layout.align_pairs_path()) if layout.align_pairs_path().exists() else [],
        relations=read_align_relations(layout.align_relations_path()) if layout.align_relations_path().exists() else [],
    )


def build_align_scope_concepts(rows: list[AggregateConceptRecord]) -> list[AlignConceptRecord]:
    return [
        AlignConceptRecord(
            id=row.id,
            name=row.name,
            description=row.description,
            parent=row.parent,
            root=row.root,
        )
        for row in rows
    ]


def select_aggregate_concepts_by_source_ids(
    rows: list[AggregateConceptRecord],
    *,
    active_source_ids: set[str],
) -> list[AggregateConceptRecord]:
    active = set(normalize_source_ids(list(active_source_ids)))
    if not active:
        return []
    return [row for row in rows if source_id_from_node_id(row.root) in active]


def reusable_align_embed_concept_ids_for_scope(
    layout: BuildLayout,
    scope_concepts: list[AlignConceptRecord],
    *,
    force_rebuild: bool,
) -> list[str]:
    concept_ids = normalize_unit_ids([row.id for row in scope_concepts])
    if not concept_ids:
        return []
    return reusable_substage_unit_ids(
        layout,
        "align",
        "embed",
        concept_ids,
        force_rebuild=force_rebuild,
    )


def reusable_align_recall_concept_ids_for_scope(
    layout: BuildLayout,
    scope_concepts: list[AlignConceptRecord],
    *,
    scope_vectors: list[ConceptVectorRecord],
    retained_concepts: list[EquivalenceRecord],
    force_rebuild: bool,
) -> list[str]:
    del scope_vectors, retained_concepts
    concept_ids = normalize_unit_ids([row.id for row in scope_concepts])
    if not concept_ids:
        return []
    return reusable_substage_unit_ids(
        layout,
        "align",
        "recall",
        concept_ids,
        force_rebuild=force_rebuild,
    )


def reusable_align_judge_pair_ids_for_scope(
    layout: BuildLayout,
    scope_pairs: list[AlignPairRecord],
    scope_concepts: list[AlignConceptRecord],
    *,
    retained_concepts: list[EquivalenceRecord],
    force_rebuild: bool,
) -> list[str]:
    del scope_concepts, retained_concepts
    pair_ids = normalize_unit_ids([f"{row.left_id}\t{row.right_id}" for row in scope_pairs if row.relation])
    if not pair_ids:
        return []
    return reusable_substage_unit_ids(
        layout,
        "align",
        "judge",
        pair_ids,
        force_rebuild=force_rebuild,
    )


def select_align_pairs_for_scope(rows: list[AlignPairRecord], concept_ids: list[str]) -> list[AlignPairRecord]:
    concept_id_set = set(normalize_unit_ids(concept_ids))
    return [
        row
        for row in dedupe_align_pairs(rows)
        if row.left_id in concept_id_set or row.right_id in concept_id_set
    ]


def clear_align_pair_relations_except(
    rows: list[AlignPairRecord],
    *,
    concept_ids: list[str],
    reusable_pair_ids: set[str],
) -> list[AlignPairRecord]:
    concept_id_set = set(normalize_unit_ids(concept_ids))
    updated: list[AlignPairRecord] = []
    for row in rows:
        pair_id = f"{row.left_id}\t{row.right_id}"
        if (
            (row.left_id in concept_id_set or row.right_id in concept_id_set)
            and pair_id not in reusable_pair_ids
        ):
            updated.append(replace(row, relation=""))
            continue
        updated.append(row)
    return updated


def judge_reprocess_pair_ids(
    rows: list[AlignPairRecord],
    *,
    reusable_pair_ids: set[str],
) -> set[tuple[str, str]]:
    return {
        (row.left_id, row.right_id)
        for row in rows
        if f"{row.left_id}\t{row.right_id}" not in reusable_pair_ids
    }


def drop_align_relations_for_pair_ids(
    rows: list[AlignRelationRecord],
    *,
    cleared_pair_ids: set[tuple[str, str]],
) -> list[AlignRelationRecord]:
    return [
        row
        for row in rows
        if (row.left_id, row.right_id) not in cleared_pair_ids
    ]


def align_processed_concept_ids(
    *,
    scoped_recall_concepts: list[AlignConceptRecord],
    judge_reprocess_pairs: list[AlignPairRecord],
    reusable_judge_pair_ids: set[str],
    scope_concepts: list[AlignConceptRecord],
    retained_concepts: list[EquivalenceRecord],
) -> list[str]:
    concept_ids = {row.id for row in scoped_recall_concepts if row.id}
    if judge_reprocess_pairs:
        concept_ids.update(
            concept_ids_for_align_pair_rows(
                rows=judge_reprocess_pairs,
                reusable_pair_ids=reusable_judge_pair_ids,
                scope_concepts=scope_concepts,
                retained_concepts=retained_concepts,
            )
        )
    return normalize_unit_ids(list(concept_ids))


def concept_ids_for_align_pair_rows(
    *,
    rows: list[AlignPairRecord],
    reusable_pair_ids: set[str],
    scope_concepts: list[AlignConceptRecord],
    retained_concepts: list[EquivalenceRecord],
) -> set[str]:
    del retained_concepts
    scoped_ids = {row.id for row in scope_concepts if row.id}
    concept_ids: set[str] = set()
    for row in rows:
        pair_id = f"{row.left_id}\t{row.right_id}"
        if pair_id in reusable_pair_ids:
            continue
        for concept_id in (row.left_id, row.right_id):
            if concept_id in scoped_ids:
                concept_ids.add(concept_id)
    return concept_ids


def source_ids_for_align_concept_ids(
    scope_concepts: list[AlignConceptRecord],
    concept_ids: list[str],
) -> list[str]:
    concept_id_set = set(normalize_unit_ids(concept_ids))
    return normalize_source_ids(
        [
            source_id_from_node_id(row.root)
            for row in scope_concepts
            if row.id in concept_id_set and row.root
        ]
    )


def align_recall_fingerprint(
    scope_concepts: list[AlignConceptRecord],
    scope_vectors: list[ConceptVectorRecord],
    retained_concepts: list[EquivalenceRecord],
) -> str:
    vector_by_id = {row.id: row for row in scope_vectors}
    retained = external_align_retained_concepts(scope_concepts, retained_concepts)
    payload = {
        "scope_concepts": [row.to_dict() for row in sorted(scope_concepts, key=lambda item: item.id)],
        "vectors": [
            vector_by_id[row.id].to_dict()
            for row in sorted(scope_concepts, key=lambda item: item.id)
            if row.id in vector_by_id
        ],
        "retained_concepts": [row.to_dict() for row in sorted(retained, key=lambda item: item.id)],
    }
    return stable_payload_fingerprint(payload)


def align_judge_fingerprint(
    scope_pairs: list[AlignPairRecord],
    scope_concepts: list[AlignConceptRecord],
    retained_concepts: list[EquivalenceRecord],
) -> str:
    retained = external_align_retained_concepts(scope_concepts, retained_concepts)
    payload = {
        "pairs": [row.to_dict() for row in dedupe_align_pairs(scope_pairs)],
        "scope_concepts": [row.to_dict() for row in sorted(scope_concepts, key=lambda item: item.id)],
        "retained_concepts": [row.to_dict() for row in sorted(retained, key=lambda item: item.id)],
    }
    return stable_payload_fingerprint(payload)


def external_align_retained_concepts(
    scope_concepts: list[AlignConceptRecord],
    retained_concepts: list[EquivalenceRecord],
) -> list[EquivalenceRecord]:
    scope_root_ids = {row.root for row in scope_concepts if row.root}
    if not scope_root_ids:
        return list(retained_concepts)
    return [
        row
        for row in retained_concepts
        if not ({root_id for root_id in row.root_ids if root_id} & scope_root_ids)
    ]


def stable_payload_fingerprint(payload: object) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()


def prune_align_stage_state(
    state: AlignStageState,
    scoped_concepts: list[AlignConceptRecord],
    *,
    all_concepts: list[AlignConceptRecord],
) -> AlignStageState:
    root_by_member = {row.id: row.root for row in all_concepts if row.id}
    current_raw_ids = set(root_by_member)
    removed_raw_ids = {row.id for row in scoped_concepts} | {
        member_id
        for row in state.concepts
        for member_id in row.member_ids
        if member_id not in current_raw_ids
    }
    kept_raw_ids = {
        member_id
        for row in state.concepts
        for member_id in row.member_ids
        if member_id not in removed_raw_ids
    }
    kept_vectors = [row for row in state.vectors if row.id in kept_raw_ids]
    kept_concepts: list[EquivalenceRecord] = []
    removed_concept_ids: set[str] = set()
    touched_concept_ids: set[str] = set()
    for row in state.concepts:
        member_ids = [member_id for member_id in row.member_ids if member_id in kept_raw_ids]
        if len(member_ids) != len(row.member_ids):
            touched_concept_ids.add(row.id)
        if not member_ids:
            removed_concept_ids.add(row.id)
            continue
        root_ids = sorted(
            {
                root_by_member[member_id]
                for member_id in member_ids
                if member_id in root_by_member and root_by_member[member_id]
            }
        )
        kept_concepts.append(
            EquivalenceRecord(
                id=row.id,
                name=row.name,
                description=row.description,
                member_ids=sorted(member_ids),
                root_ids=root_ids,
                representative_member_id=(
                    row.representative_member_id
                    if row.representative_member_id in member_ids
                    else sorted(member_ids)[0]
                ),
            )
        )
    affected_concept_ids = removed_concept_ids | touched_concept_ids
    kept_pairs = [
        row
        for row in state.pairs
        if row.left_id not in removed_raw_ids
        and row.right_id not in removed_raw_ids
        and row.left_id not in affected_concept_ids
        and row.right_id not in affected_concept_ids
    ]
    kept_relations = [
        row
        for row in state.relations
        if row.left_id not in affected_concept_ids and row.right_id not in affected_concept_ids
    ]
    return AlignStageState(
        concepts=sorted(kept_concepts, key=lambda item: item.id),
        vectors=kept_vectors,
        pairs=dedupe_align_pairs(kept_pairs),
        relations=dedupe_align_relations(kept_relations),
    )


def dedupe_align_concepts(rows: list[EquivalenceRecord]) -> list[EquivalenceRecord]:
    deduped = {row.id: row for row in rows}
    return [deduped[key] for key in sorted(deduped)]


def dedupe_vectors(rows: list[ConceptVectorRecord]) -> list[ConceptVectorRecord]:
    deduped = {row.id: row for row in rows}
    return [deduped[key] for key in sorted(deduped)]


def dedupe_align_pairs(rows: list[AlignPairRecord]) -> list[AlignPairRecord]:
    deduped = {(row.left_id, row.right_id): row for row in rows if row.left_id and row.right_id}
    return [deduped[key] for key in sorted(deduped)]


def dedupe_align_relations(rows: list[AlignRelationRecord]) -> list[AlignRelationRecord]:
    deduped = {(row.left_id, row.right_id, row.relation): row for row in rows if row.left_id and row.right_id}
    return [deduped[key] for key in sorted(deduped)]


def build_align_embed_manifest_stats(vectors: list[ConceptVectorRecord]) -> dict[str, int]:
    return {
        "vector_count": len(vectors),
        "result_count": len(vectors),
    }


def build_align_recall_manifest_stats(pairs: list[AlignPairRecord]) -> dict[str, int]:
    return {
        "pair_count": len(pairs),
        "result_count": len(pairs),
    }


def build_align_judge_manifest_stats(pairs: list[AlignPairRecord]) -> dict[str, int]:
    relation_keys = (
        "equivalent",
        "is_subordinate",
        "has_subordinate",
        "related",
        "none",
    )
    stats = {
        "pair_count": len(pairs),
        "result_count": len(pairs),
    }
    for relation in relation_keys:
        stats[f"{relation}_count"] = sum(1 for row in pairs if row.relation == relation)
    return stats


def write_align_checkpoint_artifacts(
    layout: BuildLayout,
    *,
    vectors: list[ConceptVectorRecord] | None = None,
    pairs: list[AlignPairRecord] | None = None,
) -> None:
    if vectors is not None:
        write_concept_vectors(layout.align_vectors_path(), dedupe_vectors(vectors))
    if pairs is not None:
        write_align_pairs(layout.align_pairs_path(), dedupe_align_pairs(pairs))


def write_align_stage_artifacts(
    layout: BuildLayout,
    state: AlignStageState,
    *,
    graph_bundle: GraphBundle | None,
) -> None:
    write_align_canonical_concepts(layout.align_concepts_path(), dedupe_align_concepts(state.concepts))
    write_concept_vectors(layout.align_vectors_path(), dedupe_vectors(state.vectors))
    write_align_pairs(layout.align_pairs_path(), dedupe_align_pairs(state.pairs))
    write_align_relations(layout.align_relations_path(), dedupe_align_relations(state.relations))
    if graph_bundle is not None:
        write_stage_graph(
            layout,
            "align",
            graph_bundle,
            write_nodes=True,
            write_edges=True,
        )


def align_artifact_paths(layout: BuildLayout, *, include_graph: bool = True) -> dict[str, str]:
    paths = {
        "concepts": str(layout.align_concepts_path()),
        "vectors": str(layout.align_vectors_path()),
        "pairs": str(layout.align_pairs_path()),
        "relations": str(layout.align_relations_path()),
    }
    if include_graph:
        paths = {
            "primary": str(layout.stage_nodes_path("align")),
            **paths,
            "nodes": str(layout.stage_nodes_path("align")),
            "edges": str(layout.stage_edges_path("align")),
        }
    else:
        paths = {"primary": str(layout.align_pairs_path()), **paths}
    return paths


def source_ids_for_roots(root_ids: list[str]) -> list[str]:
    return normalize_source_ids([source_id_from_node_id(root_id) for root_id in root_ids if root_id])


def graph_stats_from_stage_stats(stats: dict[str, object]) -> dict[str, object]:
    return {
        key: stats[key]
        for key in ("node_count", "edge_count", "node_type_counts", "edge_type_counts")
        if key in stats
    }


def run(ctx: StageContext) -> HandlerResult:
    stage_name = ctx.stage_name
    layout = ctx.layout
    input_source_ids = resolve_stage_source_ids(layout, stage_name, ctx.selected_source_ids)
    aggregate_concepts = (
        read_aggregate_concepts(layout.aggregate_concepts_path())
        if layout.aggregate_concepts_path().exists()
        else []
    )
    scope_aggregate_concepts = select_aggregate_concepts_by_source_ids(
        aggregate_concepts,
        active_source_ids=set(input_source_ids),
    )
    all_run_concepts = build_align_scope_concepts(aggregate_concepts)
    scope_concepts = build_align_scope_concepts(scope_aggregate_concepts)
    scoped_run_concepts = list(scope_concepts)
    scoped_run_concept_ids = normalize_unit_ids([row.id for row in scoped_run_concepts])
    reusable_embed_concept_ids = reusable_align_embed_concept_ids_for_scope(
        layout,
        scoped_run_concepts,
        force_rebuild=ctx.force_rebuild,
    )
    reusable_recall_concept_ids = reusable_align_recall_concept_ids_for_scope(
        layout,
        scoped_run_concepts,
        scope_vectors=[],
        retained_concepts=[],
        force_rebuild=ctx.force_rebuild,
    )
    align_manifest = (
        read_stage_manifest(layout.stage_manifest_path(stage_name))
        if layout.stage_manifest_path(stage_name).exists()
        else None
    )
    align_manifest_judge_units = (
        normalize_unit_ids(align_manifest.substages.get("judge", SubstageStateManifest()).processed_units)
        if align_manifest is not None
        else []
    )
    manifest_only_align_reuse = (
        not ctx.force_rebuild
        and set(scoped_run_concept_ids).issubset(set(reusable_embed_concept_ids))
        and set(scoped_run_concept_ids).issubset(set(reusable_recall_concept_ids))
    )
    if manifest_only_align_reuse:
        ctx.stage_record.artifact_paths = align_artifact_paths(layout)
        emit_prefilled_stage_progress(
            ctx.stage_progress_callback,
            f"{stage_name}::embed",
            current=len(scoped_run_concept_ids),
            total=len(scoped_run_concept_ids),
        )
        emit_prefilled_stage_progress(
            ctx.stage_progress_callback,
            f"{stage_name}::recall",
            current=len(scoped_run_concept_ids),
            total=len(scoped_run_concept_ids),
        )
        emit_prefilled_stage_progress(
            ctx.stage_progress_callback,
            f"{stage_name}::judge",
            current=len(align_manifest_judge_units),
            total=len(align_manifest_judge_units),
        )
        manifest_stats = dict(align_manifest.stats if align_manifest is not None else {})
        ctx.stage_record.stats = build_stage_work_stats(
            input_source_ids=input_source_ids,
            processed_source_ids=[],
            skipped_source_ids=source_ids_for_align_concept_ids(scoped_run_concepts, scoped_run_concept_ids),
            work_units_total=len(scoped_run_concept_ids),
            work_units_completed=0,
            work_units_skipped=len(scoped_run_concept_ids),
            updated_nodes=0,
            updated_edges=0,
            concept_count=int(manifest_stats.get("concept_count", 0) or 0),
            vector_count=int(manifest_stats.get("vector_count", 0) or 0),
            pair_count=int(manifest_stats.get("pair_count", 0) or 0),
            relation_count=int(manifest_stats.get("relation_count", 0) or 0),
            llm_request_count=0,
            llm_error_count=0,
            retry_count=0,
        )
        ctx.stage_record.stats = dict(ctx.stage_record.stats) | graph_stats_from_stage_stats(manifest_stats)
        ctx.stage_record.failures = []
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
                artifact_paths=align_artifact_paths(layout),
                stats=ctx.stage_record.stats,
            ),
        )
        return HandlerResult(current_graph=None)

    existing_align_state = read_align_stage_state(layout)
    reusable_embed_concept_id_set = set(reusable_embed_concept_ids)
    scoped_embed_concepts = [
        row for row in scoped_run_concepts if row.id not in reusable_embed_concept_id_set
    ]
    available_scope_vectors = [
        row for row in existing_align_state.vectors if row.id in reusable_embed_concept_id_set
    ]
    reusable_recall_concept_id_set = set(reusable_recall_concept_ids)
    recall_process_concepts = [
        row for row in scoped_run_concepts if row.id not in reusable_recall_concept_id_set
    ]
    retained_state = prune_align_stage_state(
        existing_align_state,
        recall_process_concepts,
        all_concepts=all_run_concepts,
    )
    retained_state = AlignStageState(
        concepts=list(retained_state.concepts),
        vectors=dedupe_vectors(retained_state.vectors + available_scope_vectors),
        pairs=list(retained_state.pairs),
        relations=list(retained_state.relations),
    )
    reusable_scope_pairs = select_align_pairs_for_scope(
        retained_state.pairs,
        reusable_recall_concept_ids,
    )
    reusable_judge_pair_ids = reusable_align_judge_pair_ids_for_scope(
        layout,
        reusable_scope_pairs,
        scoped_run_concepts,
        retained_concepts=retained_state.concepts,
        force_rebuild=ctx.force_rebuild,
    )
    reusable_judge_pair_id_set = set(reusable_judge_pair_ids)
    retained_state = AlignStageState(
        concepts=list(retained_state.concepts),
        vectors=list(retained_state.vectors),
        pairs=clear_align_pair_relations_except(
            retained_state.pairs,
            concept_ids=reusable_recall_concept_ids,
            reusable_pair_ids=reusable_judge_pair_id_set,
        ),
        relations=drop_align_relations_for_pair_ids(
            retained_state.relations,
            cleared_pair_ids=judge_reprocess_pair_ids(
                reusable_scope_pairs,
                reusable_pair_ids=reusable_judge_pair_id_set,
            ),
        ),
    )
    scoped_recall_concepts = list(recall_process_concepts)
    ctx.stage_record.artifact_paths = align_artifact_paths(layout)
    substage_states: dict[str, SubstageStateManifest] = {}
    processed_align_concept_ids = align_processed_concept_ids(
        scoped_recall_concepts=scoped_recall_concepts,
        judge_reprocess_pairs=reusable_scope_pairs,
        reusable_judge_pair_ids=reusable_judge_pair_id_set,
        scope_concepts=scoped_run_concepts,
        retained_concepts=retained_state.concepts,
    )
    skipped_align_concept_ids = [
        concept_id for concept_id in scoped_run_concept_ids if concept_id not in set(processed_align_concept_ids)
    ]
    fully_reused_align_stage = (
        not scoped_embed_concepts
        and not scoped_recall_concepts
        and not processed_align_concept_ids
        and stage_outputs_exist(layout, stage_name)
    )
    if fully_reused_align_stage:
        emit_prefilled_stage_progress(
            ctx.stage_progress_callback,
            f"{stage_name}::embed",
            current=len(scoped_run_concept_ids),
            total=len(scoped_run_concept_ids),
        )
        emit_prefilled_stage_progress(
            ctx.stage_progress_callback,
            f"{stage_name}::recall",
            current=len(scoped_run_concept_ids),
            total=len(scoped_run_concept_ids),
        )
        scope_pair_rows = select_align_pairs_for_scope(retained_state.pairs, scoped_run_concept_ids)
        scope_pair_ids = [f"{row.left_id}\t{row.right_id}" for row in scope_pair_rows if row.relation]
        emit_prefilled_stage_progress(
            ctx.stage_progress_callback,
            f"{stage_name}::judge",
            current=len(scope_pair_ids),
            total=len(scope_pair_ids),
        )
        ctx.stage_record.stats = build_stage_work_stats(
            input_source_ids=input_source_ids,
            processed_source_ids=[],
            skipped_source_ids=source_ids_for_align_concept_ids(scoped_run_concepts, skipped_align_concept_ids),
            work_units_total=len(scoped_run_concept_ids),
            work_units_completed=0,
            work_units_skipped=len(skipped_align_concept_ids),
            updated_nodes=0,
            updated_edges=0,
            concept_count=len(existing_align_state.concepts),
            vector_count=len(existing_align_state.vectors),
            pair_count=len(existing_align_state.pairs),
            relation_count=len(existing_align_state.relations),
            llm_request_count=0,
            llm_error_count=0,
            retry_count=0,
        )
        ctx.stage_record.failures = []
        ctx.stage_record.artifact_paths = align_artifact_paths(layout)
        return HandlerResult(current_graph=None)

    ctx.stage_record.artifact_paths = align_artifact_paths(layout, include_graph=False)
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

    latest_align_vectors = dedupe_vectors(retained_state.vectors + available_scope_vectors)

    def checkpoint_align_embed(
        snapshot_vectors: list[ConceptVectorRecord],
        snapshot_stats: dict[str, int],
        processed_concept_ids: list[str],
        llm_error_summary: list[dict[str, object]],
    ) -> None:
        nonlocal latest_align_vectors
        processed_embed_concept_ids = normalize_unit_ids(reusable_embed_concept_ids + processed_concept_ids)
        latest_align_vectors = dedupe_vectors(retained_state.vectors + list(snapshot_vectors))
        write_align_checkpoint_artifacts(layout, vectors=latest_align_vectors)
        substage_states["embed"] = build_unit_substage_manifest(
            layout=layout,
            parent_stage="align",
            stage_name="embed",
            processed_units=processed_embed_concept_ids,
            stats=build_align_embed_manifest_stats(latest_align_vectors),
        )
        ctx.stage_record.failures = [dict(item) for item in llm_error_summary]
        ctx.stage_record.stats = dict(ctx.stage_record.stats) | {
            "concept_count": len(retained_state.concepts),
            "vector_count": len(latest_align_vectors),
            "llm_request_count": int(snapshot_stats.get("llm_request_count", 0)),
            "llm_error_count": int(snapshot_stats.get("llm_error_count", 0)),
            "retry_count": int(snapshot_stats.get("retry_count", 0)),
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
                artifact_paths=align_artifact_paths(layout, include_graph=False),
                stats=ctx.stage_record.stats,
                status="running",
                substage_states=substage_states,
            ),
        )
        write_job_log(layout.job_log_path(ctx.job_id), ctx.log_record)

    def checkpoint_align_recall(
        snapshot_pairs: list[AlignPairRecord],
        snapshot_stats: dict[str, int],
        processed_concept_ids: list[str],
    ) -> None:
        del snapshot_stats
        processed_recall_concept_ids = normalize_unit_ids(reusable_recall_concept_ids + processed_concept_ids)
        merged_state = AlignStageState(
            concepts=list(retained_state.concepts),
            vectors=dedupe_vectors(latest_align_vectors),
            pairs=dedupe_align_pairs(retained_state.pairs + list(snapshot_pairs)),
            relations=list(retained_state.relations),
        )
        write_align_checkpoint_artifacts(layout, pairs=merged_state.pairs)
        current_scope_vectors = [
            row
            for row in latest_align_vectors
            if row.id in set(scoped_run_concept_ids)
        ]
        substage_states["recall"] = build_unit_substage_manifest(
            layout=layout,
            parent_stage="align",
            stage_name="recall",
            processed_units=processed_recall_concept_ids,
            stats=build_align_recall_manifest_stats(merged_state.pairs),
            metadata={
                "scope_fingerprint": align_recall_fingerprint(
                    scoped_run_concepts,
                    current_scope_vectors,
                    retained_state.concepts,
                ),
            },
        )
        ctx.stage_record.stats = dict(ctx.stage_record.stats) | {
            "pair_count": len(merged_state.pairs),
            "pending_count": sum(1 for row in merged_state.pairs if row.relation == ""),
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
                artifact_paths=align_artifact_paths(layout, include_graph=False),
                stats=ctx.stage_record.stats,
                status="running",
                substage_states=substage_states,
            ),
        )
        write_job_log(layout.job_log_path(ctx.job_id), ctx.log_record)

    def checkpoint_align_judge(
        snapshot_pairs: list[AlignPairRecord],
        snapshot_stats: dict[str, int],
        processed_pair_ids: list[str],
        llm_error_summary: list[dict[str, object]],
    ) -> None:
        processed_judge_pair_ids = normalize_unit_ids(reusable_judge_pair_ids + processed_pair_ids)
        merged_state = AlignStageState(
            concepts=list(retained_state.concepts),
            vectors=dedupe_vectors(latest_align_vectors),
            pairs=dedupe_align_pairs(retained_state.pairs + list(snapshot_pairs)),
            relations=list(retained_state.relations),
        )
        write_align_checkpoint_artifacts(layout, pairs=merged_state.pairs)
        substage_states["judge"] = build_unit_substage_manifest(
            layout=layout,
            parent_stage="align",
            stage_name="judge",
            processed_units=processed_judge_pair_ids,
            stats=build_align_judge_manifest_stats(merged_state.pairs),
            metadata={
                "scope_fingerprint": align_judge_fingerprint(
                    select_align_pairs_for_scope(snapshot_pairs, scoped_run_concept_ids),
                    scoped_run_concepts,
                    retained_state.concepts,
                ),
            },
        )
        ctx.stage_record.failures = [dict(item) for item in llm_error_summary]
        ctx.stage_record.stats = dict(ctx.stage_record.stats) | {
            "pair_count": len(merged_state.pairs),
            "pending_count": sum(1 for row in merged_state.pairs if row.relation == ""),
            "llm_request_count": int(snapshot_stats.get("llm_request_count", 0)),
            "llm_error_count": int(snapshot_stats.get("llm_error_count", 0)),
            "retry_count": int(snapshot_stats.get("retry_count", 0)),
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
                artifact_paths=align_artifact_paths(layout, include_graph=False),
                stats=ctx.stage_record.stats,
                status="running",
                substage_states=substage_states,
            ),
        )
        write_job_log(layout.job_log_path(ctx.job_id), ctx.log_record)

    align_result = run_align(
        load_graph_snapshot(
            layout,
            ctx.graph_input_stage[stage_name],
            stage_sequence=ctx.stage_sequence,
            graph_stages=ctx.graph_stages,
        ),
        ctx.runtime,
        all_concepts=all_run_concepts,
        retained_vectors=dedupe_vectors(retained_state.vectors),
        retained_pairs=dedupe_align_pairs(retained_state.pairs),
        retained_concepts=retained_state.concepts,
        scoped_concepts=scoped_run_concepts,
        scoped_embed_concepts=scoped_embed_concepts,
        scoped_recall_concepts=scoped_recall_concepts,
        embed_progress_callback=dynamic_stage_progress_callback(
            ctx.stage_progress_callback,
            f"{stage_name}::embed",
            skipped_units=len(reusable_embed_concept_ids),
        ),
        recall_progress_callback=dynamic_stage_progress_callback(
            ctx.stage_progress_callback,
            f"{stage_name}::recall",
            skipped_units=len(reusable_recall_concept_ids),
        ),
        judge_progress_callback=dynamic_stage_progress_callback(
            ctx.stage_progress_callback,
            f"{stage_name}::judge",
            skipped_units=len(reusable_judge_pair_ids),
        ),
        embed_checkpoint_every=ctx.runtime.substage_checkpoint_every(stage_name, "embed"),
        recall_checkpoint_every=ctx.runtime.substage_checkpoint_every(stage_name, "recall"),
        judge_checkpoint_every=ctx.runtime.substage_checkpoint_every(stage_name, "judge"),
        embed_checkpoint_callback=checkpoint_align_embed,
        recall_checkpoint_callback=checkpoint_align_recall,
        judge_checkpoint_callback=checkpoint_align_judge,
        cancel_event=ctx.cancel_event,
    )
    final_state = AlignStageState(
        concepts=list(align_result.concepts),
        vectors=list(align_result.vectors),
        pairs=list(align_result.pairs),
        relations=list(align_result.relations),
    )
    write_align_stage_artifacts(layout, final_state, graph_bundle=align_result.graph_bundle)
    scope_pair_rows = select_align_pairs_for_scope(align_result.pairs, scoped_run_concept_ids)
    scope_pair_ids = [f"{row.left_id}\t{row.right_id}" for row in scope_pair_rows if row.relation]
    substage_states["embed"] = build_unit_substage_manifest(
        layout=layout,
        parent_stage="align",
        stage_name="embed",
        processed_units=scoped_run_concept_ids,
        stats=build_align_embed_manifest_stats(align_result.vectors),
    )
    substage_states["recall"] = build_unit_substage_manifest(
        layout=layout,
        parent_stage="align",
        stage_name="recall",
        processed_units=scoped_run_concept_ids,
        stats=build_align_recall_manifest_stats(align_result.pairs),
        metadata={
            "scope_fingerprint": align_recall_fingerprint(
                scoped_run_concepts,
                [row for row in align_result.vectors if row.id in set(scoped_run_concept_ids)],
                retained_state.concepts,
            ),
        },
    )
    substage_states["judge"] = build_unit_substage_manifest(
        layout=layout,
        parent_stage="align",
        stage_name="judge",
        processed_units=scope_pair_ids,
        stats=build_align_judge_manifest_stats(align_result.pairs),
        metadata={
            "scope_fingerprint": align_judge_fingerprint(
                scope_pair_rows,
                scoped_run_concepts,
                retained_state.concepts,
            ),
        },
    )
    ctx.stage_record.stats = build_stage_work_stats(
        input_source_ids=input_source_ids,
        processed_source_ids=source_ids_for_align_concept_ids(scoped_run_concepts, processed_align_concept_ids),
        skipped_source_ids=source_ids_for_align_concept_ids(scoped_run_concepts, skipped_align_concept_ids),
        work_units_total=len(scoped_run_concept_ids),
        work_units_completed=len(processed_align_concept_ids),
        work_units_skipped=len(skipped_align_concept_ids),
        updated_nodes=int(align_result.stats.get("updated_nodes", 0)),
        updated_edges=int(align_result.stats.get("updated_edges", 0)),
        concept_count=len(align_result.concepts),
        vector_count=len(align_result.vectors),
        pair_count=len(align_result.pairs),
        relation_count=len(align_result.relations),
        llm_request_count=int(align_result.stats.get("llm_request_count", 0)),
        llm_error_count=int(align_result.stats.get("llm_error_count", 0)),
        retry_count=int(align_result.stats.get("retry_count", 0)),
    )
    ctx.stage_record.failures = [dict(item) for item in align_result.llm_errors]
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
            artifact_paths=align_artifact_paths(layout),
            stats=ctx.stage_record.stats,
            graph_bundle=align_result.graph_bundle,
            substage_states=substage_states,
        ),
    )
    ctx.stage_record.artifact_paths = align_artifact_paths(layout)
    return HandlerResult(current_graph=align_result.graph_bundle)
