from __future__ import annotations

from .helpers import ancestor_at_level, previous_sibling
from .types import ReferenceCandidate, ResolvedReference


def resolve_candidates(
    candidates: list[ReferenceCandidate],
    *,
    node_index: dict[str, object],
    parent_by_child: dict[str, str],
    article_index: dict[str, dict[str, str]],
    title_to_document_ids: dict[str, list[str]],
    current_document_id: str,
) -> list[ResolvedReference]:
    resolved: list[ResolvedReference] = []
    current_article_id = ""
    current_paragraph_id = ""
    if candidates:
        source_node_id = candidates[0].source_node_id
        current_article_id = ancestor_at_level(source_node_id, "article", node_index, parent_by_child)
        current_paragraph_id = (
            source_node_id
            if node_index[source_node_id].level == "paragraph"
            else ancestor_at_level(source_node_id, "paragraph", node_index, parent_by_child)
        )

    for candidate in candidates:
        target_node_id = ""
        if candidate.kind == "absolute_article":
            for document_id in title_to_document_ids.get(candidate.document_title, []):
                target_node_id = article_index.get(document_id, {}).get(candidate.article_label, "")
                if target_node_id:
                    break
        elif candidate.kind == "local_article":
            target_node_id = article_index.get(current_document_id, {}).get(candidate.article_label, "")
        elif candidate.kind == "this_article":
            target_node_id = current_article_id
        elif candidate.kind == "this_paragraph":
            target_node_id = current_paragraph_id
        elif candidate.kind == "previous_paragraph" and current_paragraph_id:
            target_node_id = previous_sibling(
                current_paragraph_id,
                parent_by_child.get(current_paragraph_id, ""),
                node_index,
                parent_by_child,
            )
        resolved.append(ResolvedReference(candidate=candidate, target_node_id=target_node_id))
    return dedupe_resolved_references(resolved)


def dedupe_resolved_references(items: list[ResolvedReference]) -> list[ResolvedReference]:
    deduped: list[ResolvedReference] = []
    seen: set[tuple[str, str]] = set()
    for item in items:
        key = (item.candidate.target_ref_text, item.target_node_id)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped
