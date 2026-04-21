from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

from ...contracts import (
    AlignConceptRecord,
    AlignPairRecord,
    AlignRelationRecord,
    EdgeRecord,
    EquivalenceRecord,
    GraphBundle,
    NodeRecord,
    build_edge_id,
    deduplicate_graph,
)
from ...utils.ids import checksum_text


@dataclass
class ResolveResult:
    equivalence: list[EquivalenceRecord] = field(default_factory=list)
    pairs: list[AlignPairRecord] = field(default_factory=list)
    relations: list[AlignRelationRecord] = field(default_factory=list)
    graph_bundle: GraphBundle = field(default_factory=GraphBundle)
    stats: dict[str, int] = field(default_factory=dict)


def run(
    base_graph: GraphBundle,
    concepts: list[AlignConceptRecord],
    pairs: list[AlignPairRecord],
    existing_equivalence: list[EquivalenceRecord],
) -> ResolveResult:
    concept_by_id = {row.id: row for row in concepts}
    union_find = UnionFind(sorted(concept_by_id))
    existing_by_id = {row.id: row for row in existing_equivalence}
    for row in existing_equivalence:
        member_ids = [member_id for member_id in row.member_ids if member_id in concept_by_id]
        for member_id in member_ids[1:]:
            union_find.union(member_ids[0], member_id)
    for pair in pairs:
        if pair.relation != "equivalent":
            continue
        left_members = expand_member_ids(pair.left_id, concept_by_id, existing_by_id)
        right_members = expand_member_ids(pair.right_id, concept_by_id, existing_by_id)
        if not left_members or not right_members:
            continue
        anchor = left_members[0]
        for member_id in left_members[1:] + right_members:
            union_find.union(anchor, member_id)

    groups = union_find.groups()
    member_to_existing_ids = build_member_to_existing_ids(existing_equivalence)
    equivalence: list[EquivalenceRecord] = []
    raw_to_equivalence: dict[str, str] = {}
    old_to_new: dict[str, str] = {}
    for member_ids in groups:
        previous_ids = sorted(
            {
                existing_id
                for member_id in member_ids
                for existing_id in member_to_existing_ids.get(member_id, [])
            }
        )
        equivalence_id = choose_concept_cluster_id(previous_ids, member_ids)
        for previous_id in previous_ids:
            old_to_new[previous_id] = equivalence_id
        members = [concept_by_id[member_id] for member_id in member_ids if member_id in concept_by_id]
        name = choose_equivalence_name(members)
        description = choose_equivalence_description(members, name)
        representative_member_id = choose_equivalence_representative_member(members, name)
        root_ids = sorted({member.root for member in members if member.root})
        equivalence.append(
            EquivalenceRecord(
                id=equivalence_id,
                name=name,
                description=description,
                member_ids=sorted(member_ids),
                root_ids=root_ids,
                representative_member_id=representative_member_id,
            )
        )
        for member_id in member_ids:
            raw_to_equivalence[member_id] = equivalence_id
    equivalence.sort(key=lambda row: row.id)

    normalized_pairs = normalize_pairs(pairs, raw_to_equivalence, old_to_new)
    relations = build_relations(concepts, normalized_pairs, raw_to_equivalence, old_to_new)
    graph_bundle = materialize_graph(base_graph, equivalence, concepts, relations, raw_to_equivalence)
    return ResolveResult(
        equivalence=equivalence,
        pairs=dedupe_source_pairs(pairs),
        relations=relations,
        graph_bundle=graph_bundle,
        stats=build_resolve_stats(
            concept_count=len(concepts),
            pair_count=len(dedupe_source_pairs(pairs)),
            relation_count=len(relations),
            graph_bundle=graph_bundle,
        ),
    )


def expand_member_ids(
    item_id: str,
    concept_by_id: dict[str, AlignConceptRecord],
    existing_by_id: dict[str, EquivalenceRecord],
) -> list[str]:
    if item_id in concept_by_id:
        return [item_id]
    if item_id in existing_by_id:
        return [member_id for member_id in existing_by_id[item_id].member_ids if member_id in concept_by_id]
    return []


def build_member_to_existing_ids(equivalence: list[EquivalenceRecord]) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    for row in equivalence:
        for member_id in row.member_ids:
            mapping.setdefault(member_id, []).append(row.id)
    return mapping


def normalize_pairs(
    pairs: list[AlignPairRecord],
    raw_to_equivalence: dict[str, str],
    old_to_new: dict[str, str],
) -> list[AlignPairRecord]:
    deduped: dict[tuple[str, str, str], AlignPairRecord] = {}
    for row in pairs:
        left_id = normalize_endpoint_id(row.left_id, raw_to_equivalence, old_to_new)
        right_id = normalize_endpoint_id(row.right_id, raw_to_equivalence, old_to_new)
        if not left_id or not right_id or left_id == right_id:
            continue
        key = (left_id, right_id, row.relation)
        previous = deduped.get(key)
        if previous is None or float(row.similarity) > float(previous.similarity):
            deduped[key] = AlignPairRecord(
                left_id=left_id,
                right_id=right_id,
                relation=row.relation,
                similarity=float(row.similarity),
            )
    return [deduped[key] for key in sorted(deduped)]


def dedupe_source_pairs(pairs: list[AlignPairRecord]) -> list[AlignPairRecord]:
    deduped: dict[tuple[str, str, str], AlignPairRecord] = {}
    for row in pairs:
        key = (row.left_id, row.right_id, row.relation)
        previous = deduped.get(key)
        if previous is None or float(row.similarity) > float(previous.similarity):
            deduped[key] = row
    return [deduped[key] for key in sorted(deduped)]


def normalize_endpoint_id(
    item_id: str,
    raw_to_equivalence: dict[str, str],
    old_to_new: dict[str, str],
) -> str:
    if item_id in raw_to_equivalence:
        return raw_to_equivalence[item_id]
    return old_to_new.get(item_id, item_id)


def build_relations(
    concepts: list[AlignConceptRecord],
    pairs: list[AlignPairRecord],
    raw_to_equivalence: dict[str, str],
    old_to_new: dict[str, str],
) -> list[AlignRelationRecord]:
    directional: dict[tuple[str, str], set[str]] = {}

    def add_relation(left_id: str, right_id: str, relation: str) -> None:
        if not left_id or not right_id or left_id == right_id:
            return
        directional.setdefault((left_id, right_id), set()).add(relation)

    for concept in concepts:
        if not concept.parent:
            continue
        child_id = raw_to_equivalence.get(concept.id, "")
        parent_id = raw_to_equivalence.get(concept.parent, "")
        add_relation(child_id, parent_id, "is_subordinate")
    for pair in pairs:
        if pair.relation in {"", "equivalent", "none"}:
            continue
        left_id = map_to_equivalence_endpoint(pair.left_id, raw_to_equivalence, old_to_new)
        right_id = map_to_equivalence_endpoint(pair.right_id, raw_to_equivalence, old_to_new)
        add_relation(left_id, right_id, pair.relation)

    final_rows: list[AlignRelationRecord] = []
    handled_unordered: set[tuple[str, str]] = set()
    all_keys = set(directional) | {(right, left) for left, right in directional}
    for left_id, right_id in sorted(all_keys):
        unordered = tuple(sorted((left_id, right_id)))
        if left_id == right_id or unordered in handled_unordered:
            continue
        handled_unordered.add(unordered)
        forward = directional.get((left_id, right_id), set())
        backward = directional.get((right_id, left_id), set())
        subordinate_forward = "is_subordinate" in forward or "has_subordinate" in backward
        subordinate_backward = "has_subordinate" in forward or "is_subordinate" in backward
        related = "related" in forward or "related" in backward
        if subordinate_forward and subordinate_backward:
            final_rows.append(AlignRelationRecord(left_id=unordered[0], right_id=unordered[1], relation="related"))
            continue
        if subordinate_forward:
            final_rows.append(AlignRelationRecord(left_id=left_id, right_id=right_id, relation="is_subordinate"))
            continue
        if subordinate_backward:
            final_rows.append(AlignRelationRecord(left_id=right_id, right_id=left_id, relation="is_subordinate"))
            continue
        if related:
            final_rows.append(AlignRelationRecord(left_id=unordered[0], right_id=unordered[1], relation="related"))
    return final_rows


def map_to_equivalence_endpoint(
    item_id: str,
    raw_to_equivalence: dict[str, str],
    old_to_new: dict[str, str],
) -> str:
    if item_id in raw_to_equivalence:
        return raw_to_equivalence[item_id]
    return old_to_new.get(item_id, item_id)


def materialize_graph(
    base_graph: GraphBundle,
    equivalence: list[EquivalenceRecord],
    concepts: list[AlignConceptRecord],
    relations: list[AlignRelationRecord],
    raw_to_equivalence: dict[str, str],
) -> GraphBundle:
    nodes = list(base_graph.nodes) + [
        NodeRecord(
            id=row.id,
            type="ConceptNode",
            name=row.name,
            level="concept",
            description=row.description,
        )
        for row in equivalence
    ]
    mention_pairs = sorted(
        {
            (concept.root, raw_to_equivalence[concept.id])
            for concept in concepts
            if concept.id in raw_to_equivalence and concept.root
        }
    )
    edges = list(base_graph.edges)
    edges.extend(
        EdgeRecord(
            id=build_edge_id(root_id, equivalence_id, "MENTIONS"),
            source=root_id,
            target=equivalence_id,
            type="MENTIONS",
        )
        for root_id, equivalence_id in mention_pairs
    )
    for row in relations:
        if row.relation == "is_subordinate":
            edges.append(
                EdgeRecord(
                    id=build_edge_id(row.right_id, row.left_id, "HAS_SUBORDINATE"),
                    source=row.right_id,
                    target=row.left_id,
                    type="HAS_SUBORDINATE",
                )
            )
        elif row.relation == "has_subordinate":
            edges.append(
                EdgeRecord(
                    id=build_edge_id(row.left_id, row.right_id, "HAS_SUBORDINATE"),
                    source=row.left_id,
                    target=row.right_id,
                    type="HAS_SUBORDINATE",
                )
            )
        elif row.relation == "related":
            left_id, right_id = sorted((row.left_id, row.right_id))
            edges.append(
                EdgeRecord(
                    id=build_edge_id(left_id, right_id, "RELATED_TO"),
                    source=left_id,
                    target=right_id,
                    type="RELATED_TO",
                )
            )
    return deduplicate_graph(GraphBundle(nodes=nodes, edges=edges))


def build_resolve_stats(
    *,
    concept_count: int,
    pair_count: int,
    relation_count: int,
    graph_bundle: GraphBundle,
) -> dict[str, int]:
    return {
        "concept_count": concept_count,
        "pair_count": pair_count,
        "relation_count": relation_count,
        "updated_nodes": len([node for node in graph_bundle.nodes if node.level == "concept"]),
        "updated_edges": len([edge for edge in graph_bundle.edges if edge.type in {"MENTIONS", "HAS_SUBORDINATE", "RELATED_TO"}]),
    }


def choose_concept_cluster_id(previous_ids: list[str], member_ids: list[str]) -> str:
    normalized_previous = [normalize_cluster_id(value) for value in previous_ids if str(value).strip()]
    if normalized_previous:
        return sorted(normalized_previous)[0]
    return build_concept_cluster_id(member_ids)


def normalize_cluster_id(value: str) -> str:
    normalized = str(value).strip()
    if normalized.startswith("concept:"):
        return normalized
    if normalized.startswith("equivalence:"):
        return f"concept:{normalized.split(':', 1)[1]}"
    return f"concept:{checksum_text(normalized)[:24]}"


def build_concept_cluster_id(member_ids: list[str]) -> str:
    return f"concept:{checksum_text('\n'.join(sorted(member_ids)))[:24]}"


def choose_equivalence_name(members: list[AlignConceptRecord]) -> str:
    counts = Counter(member.name for member in members if member.name)
    ordered = sorted(counts.items(), key=lambda item: (-item[1], len(item[0]), item[0]))
    return ordered[0][0] if ordered else "未命名概念"


def choose_equivalence_description(members: list[AlignConceptRecord], name: str) -> str:
    descriptions = [
        member.description
        for member in members
        if member.name == name and str(member.description).strip()
    ]
    if descriptions:
        return sorted(descriptions, key=lambda item: (-len(item), item))[0]
    fallback = [member.description for member in members if str(member.description).strip()]
    return sorted(fallback, key=lambda item: (-len(item), item))[0] if fallback else ""


def choose_equivalence_representative_member(
    members: list[AlignConceptRecord],
    canonical_name: str,
) -> str:
    if not members:
        return ""
    exact_name_matches = sorted(member.id for member in members if member.name == canonical_name and member.id)
    if exact_name_matches:
        return exact_name_matches[0]
    available = sorted(member.id for member in members if member.id)
    return available[0] if available else ""


class UnionFind:
    def __init__(self, items: list[str]) -> None:
        self.parent = {item: item for item in items}

    def find(self, item: str) -> str:
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, left: str, right: str) -> None:
        if left not in self.parent or right not in self.parent:
            return
        left_parent = self.find(left)
        right_parent = self.find(right)
        if left_parent != right_parent:
            winner = min(left_parent, right_parent)
            loser = max(left_parent, right_parent)
            self.parent[loser] = winner

    def groups(self) -> list[list[str]]:
        grouped: dict[str, list[str]] = {}
        for item in self.parent:
            grouped.setdefault(self.find(item), []).append(item)
        return [sorted(items) for _, items in sorted(grouped.items())]
