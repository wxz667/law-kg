from __future__ import annotations

from dataclasses import replace
from ..contracts import (
    AggregateConceptRecord,
    ClassifyPendingRecord,
    ExtractConceptRecord,
    ExtractInputRecord,
    GraphBundle,
    LlmJudgeDetailRecord,
    NodeRecord,
    NormalizeIndexEntry,
    NormalizeStageIndex,
    ReferenceCandidateRecord,
    ClassifyRecord,
    deduplicate_graph,
)
from ..io import read_normalize_index
from ..utils.locator import owner_source_id


def owner_document_by_node(graph_bundle: GraphBundle) -> dict[str, str]:
    node_index = {node.id: node for node in graph_bundle.nodes}
    parent_by_child = {
        edge.target: edge.source
        for edge in graph_bundle.edges
        if edge.type == "CONTAINS"
    }
    owners: dict[str, str] = {}
    for node in graph_bundle.nodes:
        current = node.id
        if node.level == "document":
            owners[node.id] = node.id
            continue
        while current in parent_by_child:
            current = parent_by_child[current]
            parent = node_index.get(current)
            if parent is not None and parent.level == "document":
                owners[node.id] = parent.id
                break
    return owners


def owner_source_id_for_node(owners: dict[str, str], node_id: str) -> str:
    return owner_source_id(owners.get(node_id, node_id))


def graph_node_ids(graph_bundle: GraphBundle) -> set[str]:
    return {node.id for node in graph_bundle.nodes}


def filter_reference_candidates_by_graph(
    rows: list[ReferenceCandidateRecord],
    *,
    graph_bundle: GraphBundle,
) -> list[ReferenceCandidateRecord]:
    node_ids = graph_node_ids(graph_bundle)
    return [
        row
        for row in rows
        if row.source_node_id in node_ids
        and all(target_node_id in node_ids for target_node_id in row.target_node_ids)
    ]


def filter_classify_outputs_by_graph(
    rows: list[ClassifyRecord],
    *,
    graph_bundle: GraphBundle,
) -> list[ClassifyRecord]:
    node_ids = graph_node_ids(graph_bundle)
    filtered: list[ClassifyRecord] = []
    for row in rows:
        if row.source_node_id not in node_ids:
            continue
        target_node_ids = [target_node_id for target_node_id in row.target_node_ids if target_node_id in node_ids]
        if not target_node_ids:
            continue
        if len(target_node_ids) == len(row.target_node_ids):
            filtered.append(row)
        else:
            filtered.append(replace(row, target_node_ids=target_node_ids))
    return filtered


def filter_extract_inputs_by_graph(
    rows: list[ExtractInputRecord],
    *,
    graph_bundle: GraphBundle,
) -> list[ExtractInputRecord]:
    node_ids = graph_node_ids(graph_bundle)
    deduped: dict[str, ExtractInputRecord] = {}
    for row in rows:
        if row.id in node_ids:
            deduped[row.id] = row
    return [deduped[key] for key in sorted(deduped)]


def filter_extract_concepts_by_graph(
    rows: list[ExtractConceptRecord],
    *,
    graph_bundle: GraphBundle,
) -> list[ExtractConceptRecord]:
    node_ids = graph_node_ids(graph_bundle)
    deduped: dict[str, ExtractConceptRecord] = {}
    for row in rows:
        if row.id in node_ids:
            deduped[row.id] = row
    return list(deduped.values())


def filter_aggregate_concepts_by_graph(
    rows: list[AggregateConceptRecord],
    *,
    graph_bundle: GraphBundle,
) -> list[AggregateConceptRecord]:
    node_ids = graph_node_ids(graph_bundle)
    deduped: dict[str, AggregateConceptRecord] = {}
    for row in rows:
        if row.root in node_ids:
            deduped[row.id] = row
    return list(deduped.values())


def merge_normalize_index(
    existing_index: NormalizeStageIndex | None,
    updated_entries: list[NormalizeIndexEntry],
) -> NormalizeStageIndex:
    merged_by_source_id = {
        entry.source_id: entry
        for entry in (existing_index.entries if existing_index is not None else [])
        if entry.source_id
    }
    for entry in updated_entries:
        merged_by_source_id[entry.source_id] = entry
    merged_entries = [merged_by_source_id[key] for key in sorted(merged_by_source_id)]
    success_count = sum(1 for entry in merged_entries if entry.status == "completed")
    failed_count = len(merged_entries) - success_count
    reused_count = sum(1 for entry in merged_entries if entry.details.get("reused") is True)
    return NormalizeStageIndex(
        stage="normalize",
        entries=merged_entries,
        stats={
            "source_count": len(merged_entries),
            "succeeded_sources": success_count,
            "failed_sources": failed_count,
            "reused_sources": reused_count,
        },
    )


def read_existing_normalize_index(path) -> NormalizeStageIndex | None:
    if not path.exists():
        return None
    return read_normalize_index(path)


def replace_document_subgraphs(
    existing_bundle: GraphBundle,
    replacement_bundle: GraphBundle,
    *,
    active_source_ids: set[str],
    stage_name: str,
) -> GraphBundle:
    owners = owner_document_by_node(existing_bundle)
    replacement_node_ids = {node.id for node in replacement_bundle.nodes}
    keep_nodes = [
        node
        for node in existing_bundle.nodes
        if owner_source_id_for_node(owners, node.id) not in active_source_ids
    ]
    keep_edges = [
        edge
        for edge in existing_bundle.edges
        if edge.source not in replacement_node_ids
        and edge.target not in replacement_node_ids
        and owner_source_id_for_node(owners, edge.source) not in active_source_ids
        and owner_source_id_for_node(owners, edge.target) not in active_source_ids
    ]
    del stage_name
    merged = GraphBundle(
        nodes=keep_nodes + list(replacement_bundle.nodes),
        edges=keep_edges + list(replacement_bundle.edges),
    )
    return deduplicate_graph(merged)


def replace_detect_outputs(
    rows: list[ReferenceCandidateRecord],
    replacements: list[ReferenceCandidateRecord],
    *,
    graph_bundle: GraphBundle,
    active_source_ids: set[str],
) -> list[ReferenceCandidateRecord]:
    owners = owner_document_by_node(graph_bundle)
    kept = [
        row
        for row in rows
        if owner_source_id_for_node(owners, row.source_node_id) not in active_source_ids
    ]
    return filter_reference_candidates_by_graph(kept + list(replacements), graph_bundle=graph_bundle)


def replace_classify_outputs_by_unit_ids(
    rows: list[ClassifyRecord],
    replacements: list[ClassifyRecord],
    *,
    graph_bundle: GraphBundle,
    active_unit_ids: set[str],
) -> list[ClassifyRecord]:
    kept = [row for row in rows if row.id not in active_unit_ids]
    return filter_classify_outputs_by_graph(kept + list(replacements), graph_bundle=graph_bundle)


def replace_llm_judge_details_by_unit_ids(
    rows: list[LlmJudgeDetailRecord],
    replacements: list[LlmJudgeDetailRecord],
    *,
    active_unit_ids: set[str],
) -> list[LlmJudgeDetailRecord]:
    kept = [row for row in rows if row.id not in active_unit_ids]
    deduped: dict[str, LlmJudgeDetailRecord] = {}
    ordered_rows = kept + list(replacements)
    for index, row in enumerate(ordered_rows):
        key = row.id or f"{row.source_id}:{row.text}:{row.label}:{index}"
        deduped[key] = row
    return list(deduped.values())


def replace_classify_pending_by_unit_ids(
    rows: list[ClassifyPendingRecord],
    replacements: list[ClassifyPendingRecord],
    *,
    active_unit_ids: set[str],
) -> list[ClassifyPendingRecord]:
    deduped: dict[str, ClassifyPendingRecord] = {}
    for row in [item for item in rows if item.id not in active_unit_ids] + list(replacements):
        deduped[row.id] = row
    return list(deduped.values())


def replace_extract_inputs(
    rows: list[ExtractInputRecord],
    replacements: list[ExtractInputRecord],
    *,
    graph_bundle: GraphBundle,
    active_source_ids: set[str],
) -> list[ExtractInputRecord]:
    owners = owner_document_by_node(graph_bundle)
    kept = [
        row
        for row in rows
        if owner_source_id_for_node(owners, row.id) not in active_source_ids
    ]
    return filter_extract_inputs_by_graph(kept + list(replacements), graph_bundle=graph_bundle)


def replace_extract_inputs_by_unit_ids(
    rows: list[ExtractInputRecord],
    replacements: list[ExtractInputRecord],
    *,
    graph_bundle: GraphBundle,
    active_unit_ids: set[str],
) -> list[ExtractInputRecord]:
    kept = [row for row in rows if row.id not in active_unit_ids]
    return filter_extract_inputs_by_graph(kept + list(replacements), graph_bundle=graph_bundle)


def replace_extract_concepts(
    rows: list[ExtractConceptRecord],
    replacements: list[ExtractConceptRecord],
    *,
    graph_bundle: GraphBundle,
    active_source_ids: set[str],
) -> list[ExtractConceptRecord]:
    owners = owner_document_by_node(graph_bundle)
    kept = [
        row
        for row in rows
        if owner_source_id_for_node(owners, row.id) not in active_source_ids
    ]
    return filter_extract_concepts_by_graph(kept + list(replacements), graph_bundle=graph_bundle)


def replace_extract_concepts_by_unit_ids(
    rows: list[ExtractConceptRecord],
    replacements: list[ExtractConceptRecord],
    *,
    graph_bundle: GraphBundle,
    active_unit_ids: set[str],
) -> list[ExtractConceptRecord]:
    kept = [row for row in rows if row.id not in active_unit_ids]
    return filter_extract_concepts_by_graph(kept + list(replacements), graph_bundle=graph_bundle)


def replace_aggregate_concepts(
    rows: list[AggregateConceptRecord],
    replacements: list[AggregateConceptRecord],
    *,
    graph_bundle: GraphBundle,
    active_source_ids: set[str],
) -> list[AggregateConceptRecord]:
    owners = owner_document_by_node(graph_bundle)
    kept = [
        row
        for row in rows
        if owner_source_id_for_node(owners, row.root) not in active_source_ids
    ]
    return filter_aggregate_concepts_by_graph(kept + list(replacements), graph_bundle=graph_bundle)


def replace_aggregate_concepts_by_unit_ids(
    rows: list[AggregateConceptRecord],
    replacements: list[AggregateConceptRecord],
    *,
    graph_bundle: GraphBundle,
    active_unit_ids: set[str],
) -> list[AggregateConceptRecord]:
    kept = [row for row in rows if row.root not in active_unit_ids]
    return filter_aggregate_concepts_by_graph(kept + list(replacements), graph_bundle=graph_bundle)


def select_extract_inputs(
    rows: list[ExtractInputRecord],
    *,
    graph_bundle: GraphBundle,
    active_source_ids: set[str],
) -> list[ExtractInputRecord]:
    owners = owner_document_by_node(graph_bundle)
    filtered = [
        row
        for row in filter_extract_inputs_by_graph(rows, graph_bundle=graph_bundle)
        if owner_source_id_for_node(owners, row.id) in active_source_ids
    ]
    return sorted(filtered, key=lambda row: row.id)


def select_extract_concepts(
    rows: list[ExtractConceptRecord],
    *,
    graph_bundle: GraphBundle,
    active_source_ids: set[str],
) -> list[ExtractConceptRecord]:
    owners = owner_document_by_node(graph_bundle)
    filtered = [
        row
        for row in filter_extract_concepts_by_graph(rows, graph_bundle=graph_bundle)
        if owner_source_id_for_node(owners, row.id) in active_source_ids
    ]
    return sorted(filtered, key=lambda row: row.id)


def select_aggregate_concepts(
    rows: list[AggregateConceptRecord],
    *,
    graph_bundle: GraphBundle,
    active_source_ids: set[str],
) -> list[AggregateConceptRecord]:
    owners = owner_document_by_node(graph_bundle)
    filtered = [
        row
        for row in filter_aggregate_concepts_by_graph(rows, graph_bundle=graph_bundle)
        if owner_source_id_for_node(owners, row.root) in active_source_ids
    ]
    return sorted(filtered, key=lambda row: (row.root, row.parent, row.id))


def replace_infer_outputs(
    graph_bundle: GraphBundle,
    *,
    active_source_ids: set[str],
) -> GraphBundle:
    del active_source_ids
    return deduplicate_graph(graph_bundle)
