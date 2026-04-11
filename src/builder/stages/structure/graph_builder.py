from __future__ import annotations

from typing import Callable

from ...contracts import DocumentUnitRecord, EdgeRecord, GraphBundle, NodeRecord, deduplicate_graph
from .appendices import finalize_appendices
from .body_parser import finalize_document_body
from .collapse import collapse_single_paragraph_item_branches
from .nodes import build_document_node


def run_structure(
    units: list[DocumentUnitRecord],
    *,
    progress_callback: Callable[[int, int], None] | None = None,
    checkpoint_every: int = 0,
    checkpoint_callback: Callable[[GraphBundle, list[str], int, int], None] | None = None,
) -> GraphBundle:
    nodes: list[NodeRecord] = []
    edges: list[EdgeRecord] = []
    total = max(len(units), 1)
    if progress_callback is not None:
        progress_callback(0, total)
    if progress_callback is not None and not units:
        progress_callback(1, 1)
    for index, unit in enumerate(units, start=1):
        counters = {
            "part": 0,
            "chapter": 0,
            "section": 0,
            "segment": 0,
            "appendix": 0,
            "candidate_article": 0,
            "scoped_ordinals": {},
        }
        document_node = build_document_node(unit)
        nodes.append(document_node)
        finalize_document_body(nodes, edges, unit, document_node, counters)
        finalize_appendices(nodes, edges, unit, document_node, counters)
        collapse_single_paragraph_item_branches(nodes, edges)
        if progress_callback is not None:
            progress_callback(index, total)
        if checkpoint_callback is not None and checkpoint_every > 0 and (
            index % checkpoint_every == 0 or index == len(units)
        ):
            checkpoint_callback(
                deduplicate_graph(GraphBundle(nodes=list(nodes), edges=list(edges))),
                [item.source_id for item in units[:index]],
                index,
                total,
            )

    collapse_single_paragraph_item_branches(nodes, edges)

    bundle = GraphBundle(nodes=nodes, edges=edges)
    return deduplicate_graph(bundle)
