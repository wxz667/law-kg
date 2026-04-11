from __future__ import annotations

from ...contracts import EdgeRecord, NodeRecord, build_edge_id
from ...utils.document_layout import match_heading_level, normalize_segment_heading
from .patterns import LEVEL_ORDER, STRUCTURAL_EDGES


def structural_edge_type(parent_level: str, child_level: str) -> str:
    edge_type = STRUCTURAL_EDGES.get((parent_level, child_level))
    if edge_type is None:
        raise ValueError(f"Unsupported structural edge from {parent_level} to {child_level}")
    return edge_type


def find_parent(level_stack: dict[str, NodeRecord], child_level: str) -> NodeRecord:
    child_index = LEVEL_ORDER.index(child_level)
    for level in reversed(LEVEL_ORDER[:child_index]):
        if level in level_stack:
            return level_stack[level]
    return level_stack["document"]


def clear_lower_levels(level_stack: dict[str, NodeRecord], current_level: str) -> None:
    current_index = LEVEL_ORDER.index(current_level)
    for level in LEVEL_ORDER[current_index + 1 :]:
        level_stack.pop(level, None)


def build_edge(source_id: str, target_id: str, edge_type: str) -> EdgeRecord:
    return EdgeRecord(id=build_edge_id(source_id, target_id, edge_type), source=source_id, target=target_id, type=edge_type)
