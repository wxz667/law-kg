from __future__ import annotations

from ...contracts import GraphBundle
from ...pipeline.incremental import select_extract_concepts, select_extract_inputs
from .concepts import aggregate_concept_stats
from .types import AggregateInputRecord


def build_inputs_from_extract(
    extract_inputs: list[object],
    extract_concepts: list[object],
    *,
    graph_bundle: GraphBundle,
    active_source_ids: set[str],
) -> list[AggregateInputRecord]:
    input_by_id = {
        row.id: row
        for row in select_extract_inputs(
            list(extract_inputs),
            graph_bundle=graph_bundle,
            active_source_ids=active_source_ids,
        )
    }
    scoped_concepts = select_extract_concepts(
        list(extract_concepts),
        graph_bundle=graph_bundle,
        active_source_ids=active_source_ids,
    )
    records: list[AggregateInputRecord] = []
    for concept_record in scoped_concepts:
        input_record = input_by_id.get(concept_record.id)
        if input_record is None:
            continue
        records.append(
            AggregateInputRecord(
                id=input_record.id,
                hierarchy=input_record.hierarchy,
                concepts=list(concept_record.concepts),
            )
        )
    return records


def build_output_stats(rows: list[object]) -> dict[str, int]:
    return aggregate_concept_stats(list(rows))
