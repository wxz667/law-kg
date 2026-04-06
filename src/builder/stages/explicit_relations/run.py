from __future__ import annotations

from typing import Any, Callable

from ...contracts import EdgeRecord, GraphBundle, build_edge_id, deduplicate_graph
from ...pipeline.runtime import PipelineRuntime
from .classify import classify_resolved_references
from .extract import extract_candidates, split_sentences
from .helpers import build_relation_context, document_title
from .resolve import resolve_candidates


def run(
    graph_bundle: GraphBundle,
    runtime: PipelineRuntime,
    progress_callback: Callable[[int, int], None] | None = None,
) -> GraphBundle:
    context = build_relation_context(graph_bundle)
    node_index = context["node_index"]
    parent_by_child = context["parent_by_child"]
    owner_document_by_node = context["owner_document_by_node"]
    document_nodes = context["document_nodes"]
    article_index = context["article_index"]
    title_to_document_ids = context["title_to_document_ids"]

    unresolved: list[dict[str, Any]] = []
    candidate_nodes = [
        node
        for node in graph_bundle.nodes
        if node.level in {"article", "paragraph", "item", "sub_item", "segment", "appendix"} and node.text
    ]
    total_candidates = max(len(candidate_nodes), 1)
    if progress_callback is not None:
        progress_callback(0, total_candidates)
    if not candidate_nodes and progress_callback is not None:
        progress_callback(1, 1)

    for index, node in enumerate(candidate_nodes, start=1):
        document_id = owner_document_by_node.get(node.id, "")
        current_document_node = document_nodes.get(document_id)
        if current_document_node is None:
            if progress_callback is not None:
                progress_callback(index, total_candidates)
            continue
        for sentence in split_sentences(node.text):
            candidates = extract_candidates(sentence, node.id, document_title(current_document_node))
            if not candidates:
                continue
            resolved = resolve_candidates(
                candidates,
                node_index=node_index,
                parent_by_child=parent_by_child,
                article_index=article_index,
                title_to_document_ids=title_to_document_ids,
                current_document_id=document_id,
            )
            for plan in classify_resolved_references(runtime, resolved):
                if not plan.target_node_id:
                    unresolved.append(
                        {
                            "source_node_id": plan.source_node_id,
                            "target_ref_text": plan.target_ref_text,
                            "evidence_text": plan.evidence_text,
                            "relation_type": plan.relation_type,
                            "score": plan.score,
                        }
                    )
                    continue
                graph_bundle.edges.append(
                    EdgeRecord(
                        id=build_edge_id(plan.source_node_id, plan.target_node_id, plan.relation_type),
                        source=plan.source_node_id,
                        target=plan.target_node_id,
                        type=plan.relation_type,
                        weight=plan.score,
                        evidence=[{"text": plan.evidence_text, "target_ref_text": plan.target_ref_text}],
                        metadata={"model": plan.model, "predicted": False},
                    )
                )
        if progress_callback is not None:
            progress_callback(index, total_candidates)

    graph_bundle.metadata.setdefault("reports", {})["explicit_relations"] = {
        "unresolved_references": unresolved,
        "resolved_edge_count": sum(1 for edge in graph_bundle.edges if edge.type in {"REFERENCES", "INTERPRETS", "AMENDS", "REPEALS"}),
    }
    graph_bundle.metadata["stage"] = "explicit_relations"
    return deduplicate_graph(graph_bundle)
