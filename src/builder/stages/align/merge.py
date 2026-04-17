from __future__ import annotations

from collections import Counter

from ...contracts import EdgeRecord, EmbeddedConceptRecord, NodeRecord, build_edge_id
from ...utils.ids import checksum_text


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


def build_canonical_nodes(
    embedded_concepts: list[EmbeddedConceptRecord],
    union_find: UnionFind,
) -> tuple[list[NodeRecord], dict[str, str]]:
    concept_by_id = {row.id: row for row in embedded_concepts}
    canonical_nodes: list[NodeRecord] = []
    canonical_by_member: dict[str, str] = {}
    for index, member_ids in enumerate(union_find.groups(), start=1):
        members = [concept_by_id[member_id] for member_id in member_ids if member_id in concept_by_id]
        aliases = sorted({member.text for member in members if member.text})
        canonical_name = choose_canonical_name(members)
        canonical_id = build_canonical_id(member_ids)
        canonical_nodes.append(
            NodeRecord(
                id=canonical_id,
                type="ConceptNode",
                name=canonical_name,
                level="concept",
                text=" / ".join(aliases or [canonical_name]),
                aliases=aliases or [canonical_name],
                normalized_values=[],
                source_members=member_ids,
                alignment_status="canonical",
                order=index,
            )
        )
        for member_id in member_ids:
            canonical_by_member[member_id] = canonical_id
    return canonical_nodes, canonical_by_member


def build_mentions_edges(
    embedded_concepts: list[EmbeddedConceptRecord],
    canonical_by_member: dict[str, str],
) -> list[EdgeRecord]:
    mention_pairs = sorted(
        {
            (row.source_node_id, canonical_by_member[row.id])
            for row in embedded_concepts
            if row.id in canonical_by_member
        }
    )
    return [
        EdgeRecord(
            id=build_edge_id(source_node_id, canonical_id, "MENTIONS"),
            source=source_node_id,
            target=canonical_id,
            type="MENTIONS",
            canonical=True,
            model="builder-align-resolve",
        )
        for source_node_id, canonical_id in mention_pairs
    ]


def build_related_edges(
    related_pairs: list[tuple[str, str]],
) -> list[EdgeRecord]:
    deduped_pairs = sorted({tuple(sorted((left_id, right_id))) for left_id, right_id in related_pairs if left_id != right_id})
    return [
        EdgeRecord(
            id=build_edge_id(left_id, right_id, "RELATED_TO"),
            source=left_id,
            target=right_id,
            type="RELATED_TO",
            canonical=True,
            model="builder-align-resolve",
        )
        for left_id, right_id in deduped_pairs
    ]


def build_canonical_id(member_ids: list[str]) -> str:
    token = checksum_text("\n".join(sorted(member_ids)))[:24]
    return f"concept:{token}"


def choose_canonical_name(members: list[EmbeddedConceptRecord]) -> str:
    counts = Counter(member.text for member in members if member.text)
    ordered = sorted(counts.items(), key=lambda item: (-item[1], len(item[0]), item[0]))
    return ordered[0][0] if ordered else "未命名概念"
