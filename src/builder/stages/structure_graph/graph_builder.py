from __future__ import annotations

from typing import Callable

from ...contracts import DocumentUnitRecord, EdgeRecord, GraphBundle, NodeRecord, deduplicate_graph
from .appendices import finalize_appendices
from .body_parser import finalize_document_body
from .patterns import LEVEL_TO_NODE_TYPE


def run_structure_graph(
    units: list[DocumentUnitRecord],
    *,
    progress_callback: Callable[[int, int], None] | None = None,
) -> GraphBundle:
    nodes: list[NodeRecord] = []
    edges: list[EdgeRecord] = []
    counters = {"part": 0, "chapter": 0, "section": 0, "segment": 0, "appendix": 0, "candidate_article": 0}
    total = max(len(units), 1)
    if progress_callback is not None:
        progress_callback(0, total)
    if progress_callback is not None and not units:
        progress_callback(1, 1)
    for index, unit in enumerate(units, start=1):
        document_node = build_document_node(unit)
        nodes.append(document_node)
        finalize_document_body(nodes, edges, unit, document_node, counters)
        finalize_appendices(nodes, edges, unit, document_node, counters)
        if progress_callback is not None:
            progress_callback(index, total)

    bundle = GraphBundle(
        bundle_id="structure_graph:0001",
        document_id="corpus",
        nodes=nodes,
        edges=edges,
        metadata={
            "stage": "structure_graph",
            "document_count": len(units),
            "source_ids": [unit.source_id for unit in units],
        },
    )
    return deduplicate_graph(bundle)


def build_document_node(unit: DocumentUnitRecord) -> NodeRecord:
    source_metadata = unit.metadata
    metadata = {
        key: value
        for key, value in source_metadata.items()
        if key
        not in {
            "category",
            "document_type",
            "document_subtype",
            "status",
            "issuer",
            "publish_date",
            "effective_date",
            "source_url",
            "source_id",
            "title",
        }
    }
    return NodeRecord(
        id=unit.source_id,
        type=LEVEL_TO_NODE_TYPE["document"],
        name=unit.title,
        level="document",
        category=string_value(source_metadata.get("category")),
        status=string_value(source_metadata.get("status")),
        issuer=string_value(source_metadata.get("issuer")),
        publish_date=string_value(source_metadata.get("publish_date")),
        effective_date=string_value(source_metadata.get("effective_date")),
        source_url=string_value(source_metadata.get("source_url")),
        metadata=metadata,
    )


def string_value(value: object) -> str:
    if value in {None, ""}:
        return ""
    return str(value)
