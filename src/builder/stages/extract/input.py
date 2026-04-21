from __future__ import annotations

from collections import defaultdict

from ...contracts import ExtractInputRecord
from ...utils.reference import is_excluded_reference_document
from ...utils.locator import node_locator_from_node_id, node_sort_key, source_id_from_node_id
from ...utils.numbers import int_to_cn

TOC_LEVELS = {"part", "chapter", "section"}


def filter_extract_source_ids(
    graph_bundle,
    *,
    active_source_ids: set[str] | None = None,
    progress_callback=None,
    checkpoint_every: int = 0,
    checkpoint_callback=None,
) -> list[str]:
    document_nodes = collect_extract_scope_document_nodes(
        graph_bundle,
        active_source_ids=active_source_ids,
    )
    total_documents = max(len(document_nodes), 1)
    if progress_callback is not None:
        progress_callback(0, total_documents)
    filtered_source_ids: list[str] = []
    for index, document_node in enumerate(document_nodes, start=1):
        if not is_excluded_reference_document(document_node):
            filtered_source_ids.append(source_id_from_node_id(document_node.id))
        if progress_callback is not None:
            progress_callback(index, total_documents)
        if checkpoint_callback is not None and checkpoint_every > 0 and (
            index % checkpoint_every == 0 or index == len(document_nodes)
        ):
            checkpoint_callback(
                [
                    source_id_from_node_id(node.id)
                    for node in document_nodes[:index]
                ],
                list(filtered_source_ids),
            )
    return filtered_source_ids


def collect_extract_scope_document_nodes(
    graph_bundle,
    *,
    active_source_ids: set[str] | None = None,
) -> list[object]:
    if active_source_ids is not None and not active_source_ids:
        return []
    active_sources = {value for value in (active_source_ids or set()) if value}
    return sorted(
        (
            node
            for node in graph_bundle.nodes
            if node.level == "document"
            and (not active_sources or source_id_from_node_id(node.id) in active_sources)
        ),
        key=node_sort_key,
    )


def collect_extract_document_nodes(
    graph_bundle,
    *,
    active_source_ids: set[str] | None = None,
) -> list[object]:
    return [
        node
        for node in collect_extract_scope_document_nodes(
            graph_bundle,
            active_source_ids=active_source_ids,
        )
        if not is_excluded_reference_document(node)
    ]


def build_extract_inputs(
    graph_bundle,
    *,
    active_source_ids: set[str] | None = None,
    progress_callback=None,
    checkpoint_every: int = 0,
    checkpoint_callback=None,
) -> list[ExtractInputRecord]:
    if active_source_ids is not None and not active_source_ids:
        return []
    active_sources = {value for value in (active_source_ids or set()) if value}
    node_index = {node.id: node for node in graph_bundle.nodes}
    parent_by_child: dict[str, str] = {}
    children_by_parent: dict[str, list[object]] = defaultdict(list)
    for edge in graph_bundle.edges:
        if edge.type != "CONTAINS":
            continue
        parent = node_index.get(edge.source)
        child = node_index.get(edge.target)
        if parent is None or child is None:
            continue
        parent_by_child[child.id] = parent.id
        children_by_parent[parent.id].append(child)
    for children in children_by_parent.values():
        children.sort(key=node_sort_key)

    document_nodes = collect_extract_document_nodes(
        graph_bundle,
        active_source_ids=None if active_source_ids is None else active_sources,
    )
    records: list[ExtractInputRecord] = []
    processed_source_ids: list[str] = []
    total_documents = max(len(document_nodes), 1)
    if progress_callback is not None:
        progress_callback(0, total_documents)
    for index, document_node in enumerate(document_nodes, start=1):
        document_source_id = source_id_from_node_id(document_node.id)
        processed_source_ids.append(document_source_id)
        unit_nodes = _collect_extract_units(document_node, children_by_parent)
        for unit_node in unit_nodes:
            content = _render_block(unit_node, children_by_parent)
            if content:
                records.append(
                    ExtractInputRecord(
                        id=unit_node.id,
                        hierarchy=_build_hierarchy(unit_node, node_index, parent_by_child),
                        content=content,
                    )
                )
        if progress_callback is not None:
            progress_callback(index, total_documents)
        if checkpoint_callback is not None and checkpoint_every > 0 and (
            index % checkpoint_every == 0 or index == len(document_nodes)
        ):
            checkpoint_callback(list(records), list(processed_source_ids))
    return records


def count_extract_units(graph_bundle, active_source_ids: set[str] | None = None) -> int:
    if active_source_ids is not None and not active_source_ids:
        return 0
    return len(build_extract_inputs(graph_bundle, active_source_ids=active_source_ids))


def _collect_extract_units(document_node, children_by_parent: dict[str, list[object]]) -> list[object]:
    toc_nodes = _collect_toc_descendants(document_node.id, children_by_parent)
    if not toc_nodes:
        return [document_node]
    return [
        node
        for node in toc_nodes
        if not any(child.level in TOC_LEVELS for child in children_by_parent.get(node.id, []))
    ]


def _collect_toc_descendants(node_id: str, children_by_parent: dict[str, list[object]]) -> list[object]:
    collected: list[object] = []
    for child in children_by_parent.get(node_id, []):
        if child.level in TOC_LEVELS:
            collected.append(child)
            collected.extend(_collect_toc_descendants(child.id, children_by_parent))
    return collected


def _render_block(node, children_by_parent: dict[str, list[object]]) -> str:
    if node.level == "appendix":
        return ""
    if node.level in TOC_LEVELS | {"document"}:
        child_blocks = [
            rendered
            for child in children_by_parent.get(node.id, [])
            if child.level != "appendix"
            for rendered in [_render_block(child, children_by_parent)]
            if rendered
        ]
        return "\n\n".join(child_blocks).strip()
    if node.level == "article":
        header = str(getattr(node, "name", "")).strip()
        text = str(getattr(node, "text", "")).strip()
        line = f"{header} {text}".strip() if text else header
        child_blocks = [
            rendered
            for child in children_by_parent.get(node.id, [])
            if child.level != "appendix"
            for rendered in [_render_block(child, children_by_parent)]
            if rendered
        ]
        return "\n".join(part for part in [line, *child_blocks] if part).strip()
    if node.level in {"segment", "paragraph"}:
        sections: list[str] = []
        if str(getattr(node, "text", "")).strip():
            sections.append(str(node.text).strip())
        child_blocks = [
            rendered
            for child in children_by_parent.get(node.id, [])
            if child.level != "appendix"
            for rendered in [_render_block(child, children_by_parent)]
            if rendered
        ]
        if child_blocks:
            sections.append("\n".join(child_blocks))
        return "\n".join(section for section in sections if section).strip()
    if node.level == "item":
        marker = _format_item_marker(node.id)
        line = f"{marker}{str(getattr(node, 'text', '')).strip()}".strip()
        child_blocks = [
            rendered
            for child in children_by_parent.get(node.id, [])
            for rendered in [_render_block(child, children_by_parent)]
            if rendered
        ]
        return "\n".join(part for part in [line, *child_blocks] if part).strip()
    if node.level == "sub_item":
        marker = _format_sub_item_marker(node.id)
        text = str(getattr(node, "text", "")).strip()
        return f"{marker} {text}".strip()
    return str(getattr(node, "text", "")).strip()


def _format_item_marker(node_id: str) -> str:
    locator = node_locator_from_node_id(node_id)
    item_no = int(locator.item_no or 0) if locator is not None else 0
    return f"（{int_to_cn(item_no or 1)}）"


def _format_sub_item_marker(node_id: str) -> str:
    locator = node_locator_from_node_id(node_id)
    sub_item_no = int(locator.sub_item_no or 0) if locator is not None else 0
    return f"{sub_item_no or 1}."


def _build_hierarchy(node, node_index: dict[str, object], parent_by_child: dict[str, str]) -> str:
    parts: list[str] = []
    current = node
    while current is not None:
        if current.level == "document":
            title = str(getattr(current, "name", "")).strip()
            if title:
                parts.append(_format_document_title(title))
            break
        if current.level in TOC_LEVELS:
            name = str(getattr(current, "name", "")).strip()
            if name:
                parts.append(name)
        parent_id = parent_by_child.get(current.id)
        current = node_index.get(parent_id) if parent_id else None
    return " > ".join(reversed(parts))


def _format_document_title(title: str) -> str:
    normalized = title.strip()
    if not normalized:
        return ""
    if normalized.startswith("《") and normalized.endswith("》"):
        return normalized
    return f"《{normalized}》"
