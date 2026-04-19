from __future__ import annotations

from typing import Any

from ...contracts import AggregateConceptRecord, AggregateCoreConcept
from ...utils.ids import checksum_text


def build_core_concept_id(root_id: str, concept_name: str) -> str:
    token = checksum_text(f"{root_id}\tcore\t{concept_name}")[:24]
    return f"concept:{token}"


def build_subordinate_concept_id(root_id: str, parent_name: str, concept_name: str) -> str:
    token = checksum_text(f"{root_id}\tsubordinate\t{parent_name}\t{concept_name}")[:24]
    return f"concept:{token}"


def flatten_structured_concepts(root_id: str, concepts: list[AggregateCoreConcept]) -> list[AggregateConceptRecord]:
    flattened: list[AggregateConceptRecord] = []
    for core in concepts:
        core_id = build_core_concept_id(root_id, core.concept)
        flattened.append(
            AggregateConceptRecord(
                id=core_id,
                name=core.concept,
                description=core.description,
                parent="",
                root=root_id,
            )
        )
        for subordinate in core.subordinates:
            flattened.append(
                AggregateConceptRecord(
                    id=build_subordinate_concept_id(root_id, core.concept, subordinate.concept),
                    name=subordinate.concept,
                    description=subordinate.description,
                    parent=core_id,
                    root=root_id,
                )
            )
    return flattened


def aggregate_concept_stats(rows: list[AggregateConceptRecord]) -> dict[str, int]:
    core_count = sum(1 for row in rows if not str(row.parent).strip())
    subordinate_count = len(rows) - core_count
    return {
        "result_count": len(rows),
        "concept_count": len(rows),
        "core_concept_count": core_count,
        "subordinate_concept_count": subordinate_count,
    }


def is_flat_aggregate_payload(payload: dict[str, Any]) -> bool:
    return {
        "id",
        "name",
        "description",
        "parent",
        "root",
    }.issubset(payload)

def is_legacy_aggregate_payload(payload: dict[str, Any]) -> bool:
    return "id" in payload and isinstance(payload.get("concepts"), list)
