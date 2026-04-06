from __future__ import annotations

from ...contracts import EdgeRecord, NodeRecord, build_edge_id
from ...utils.ids import slugify


def append_concept_candidate(graph_bundle, *, node, prediction, concept_counter: int) -> None:
    normalized_text = prediction.normalized_text or prediction.text
    concept_id = f"concept_candidate:{slugify(graph_bundle.document_id)}:{concept_counter:04d}"
    graph_bundle.nodes.append(
        NodeRecord(
            id=concept_id,
            type="ConceptNode",
            name=normalized_text,
            level="concept",
            text=prediction.text,
            metadata={
                "candidate": True,
                "alignment_status": "pending",
                "label": prediction.label,
                "model": prediction.model,
                "start_offset": prediction.start_offset,
                "end_offset": prediction.end_offset,
                "aliases": [prediction.text],
                "normalized_text": normalized_text,
                "order": concept_counter,
            },
        )
    )
    graph_bundle.edges.append(
        EdgeRecord(
            id=build_edge_id(node.id, concept_id, "MENTIONS"),
            source=node.id,
            target=concept_id,
            type="MENTIONS",
            metadata={
                "predicted": False,
                "label": prediction.label,
                "start_offset": prediction.start_offset,
                "end_offset": prediction.end_offset,
            },
        )
    )
