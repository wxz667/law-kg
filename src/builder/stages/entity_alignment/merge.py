from __future__ import annotations

from ...contracts import EdgeRecord, NodeRecord, build_edge_id
from ...utils.ids import slugify


class UnionFind:
    def __init__(self, items: list[str]) -> None:
        self.parent = {item: item for item in items}

    def find(self, item: str) -> str:
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, left: str, right: str) -> None:
        left_parent = self.find(left)
        right_parent = self.find(right)
        if left_parent != right_parent:
            self.parent[right_parent] = left_parent

    def groups(self) -> list[list[str]]:
        grouped: dict[str, list[str]] = {}
        for item in self.parent:
            grouped.setdefault(self.find(item), []).append(item)
        return [sorted(items) for items in grouped.values()]


def build_canonical_graph_parts(graph_bundle, candidate_nodes: list[object], parent_by_child: dict[str, str], union_find: UnionFind) -> tuple[list[NodeRecord], list[EdgeRecord]]:
    canonical_nodes: list[NodeRecord] = []
    canonical_edges: list[EdgeRecord] = []
    groups = union_find.groups()

    for index, member_ids in enumerate(groups, start=1):
        members = [candidate for candidate in candidate_nodes if candidate.id in member_ids]
        aliases = sorted({member.text or member.name for member in members if member.text or member.name})
        normalized_values = sorted(
            {str(member.normalized_text or member.name) for member in members if member.name}
        )
        canonical_name = normalized_values[0] if normalized_values else aliases[0]
        source_node_ids = sorted({parent_by_child[member.id] for member in members if member.id in parent_by_child})
        canonical_id = f"concept:{index:04d}"
        canonical_nodes.append(
            NodeRecord(
                id=canonical_id,
                type="ConceptNode",
                name=canonical_name,
                level="concept",
                text=" / ".join(aliases or [canonical_name]),
                aliases=aliases or [canonical_name],
                normalized_values=normalized_values or [canonical_name],
                source_members=member_ids,
                alignment_status="canonical",
                order=index,
            )
        )
        for source_node_id in source_node_ids:
            canonical_edges.append(
                EdgeRecord(
                    id=build_edge_id(source_node_id, canonical_id, "MENTIONS"),
                    source=source_node_id,
                    target=canonical_id,
                    type="MENTIONS",
                    canonical=True,
                )
            )
    return canonical_nodes, canonical_edges
