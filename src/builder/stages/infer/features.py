from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field

from ...contracts import AlignRelationRecord, ConceptVectorRecord, EquivalenceRecord, GraphBundle, NodeRecord
from ...utils.math import cosine_similarity
from .types import InferBridgeEvidence

LOCAL_STRUCTURAL_LEVELS = {"part", "chapter", "section"}
BRIDGE_LEVELS = {"document"} | LOCAL_STRUCTURAL_LEVELS


@dataclass
class InferFeatureStore:
    concept_vectors: dict[str, list[float]] = field(default_factory=dict)
    concept_root_ids: dict[str, tuple[str, ...]] = field(default_factory=dict)
    aux_neighbors: dict[str, set[str]] = field(default_factory=dict)
    concept_ancestors: dict[str, set[str]] = field(default_factory=dict)
    root_ancestors: dict[str, set[str]] = field(default_factory=dict)
    concept_structural_contexts: dict[str, set[str]] = field(default_factory=dict)
    concept_document_contexts: dict[str, set[str]] = field(default_factory=dict)
    root_bridge_contexts: dict[str, tuple[str, ...]] = field(default_factory=dict)
    bridge_counts: dict[tuple[str, str], int] = field(default_factory=dict)
    bridge_samples: dict[tuple[str, str], tuple[str, str, str]] = field(default_factory=dict)
    max_bridge_count: int = 0
    node_index: dict[str, NodeRecord] = field(default_factory=dict)


def build_concept_vector_lookup(
    concepts: list[EquivalenceRecord],
    raw_vectors: list[ConceptVectorRecord],
) -> dict[str, list[float]]:
    raw_vector_by_id = {row.id: list(row.vector) for row in raw_vectors if row.id}
    concept_vectors: dict[str, list[float]] = {}
    missing: list[str] = []
    for row in concepts:
        representative_member_id = str(row.representative_member_id).strip()
        if not representative_member_id:
            missing.append(row.id)
            continue
        vector = raw_vector_by_id.get(representative_member_id)
        if vector is None:
            missing.append(row.id)
            continue
        concept_vectors[row.id] = vector
    if missing:
        raise ValueError(
            "Infer requires representative_member_id vectors for all canonical concepts; "
            f"missing {len(missing)} concept(s), e.g. {', '.join(sorted(missing)[:5])}."
        )
    return concept_vectors


def build_feature_store(
    concepts: list[EquivalenceRecord],
    raw_vectors: list[ConceptVectorRecord],
    relations: list[AlignRelationRecord],
    graph_bundle: GraphBundle,
) -> InferFeatureStore:
    node_index = {node.id: node for node in graph_bundle.nodes}
    parent_by_child = {
        edge.target: edge.source
        for edge in graph_bundle.edges
        if edge.type == "CONTAINS"
    }
    concept_vectors = build_concept_vector_lookup(concepts, raw_vectors)
    concept_root_ids = {
        row.id: tuple(sorted({root_id for root_id in row.root_ids if str(root_id).strip()}))
        for row in concepts
        if row.id
    }
    relation_neighbors = build_relation_neighbors(relations)
    concept_ancestors = build_concept_ancestors(relations)
    root_ancestors = build_root_ancestors(concept_root_ids, graph_bundle, node_index)
    concept_structural_contexts, concept_document_contexts, root_bridge_contexts = build_root_context_indexes(
        concept_root_ids,
        parent_by_child,
        node_index,
    )
    aux_neighbors = build_auxiliary_neighbors(concept_structural_contexts, relation_neighbors)
    bridge_counts, bridge_samples = build_bridge_index(graph_bundle, node_index, parent_by_child)
    return InferFeatureStore(
        concept_vectors=concept_vectors,
        concept_root_ids=concept_root_ids,
        aux_neighbors=aux_neighbors,
        concept_ancestors=concept_ancestors,
        root_ancestors=root_ancestors,
        concept_structural_contexts=concept_structural_contexts,
        concept_document_contexts=concept_document_contexts,
        root_bridge_contexts=root_bridge_contexts,
        bridge_counts=bridge_counts,
        bridge_samples=bridge_samples,
        max_bridge_count=max(bridge_counts.values(), default=0),
        node_index=node_index,
    )


def build_relation_neighbors(relations: list[AlignRelationRecord]) -> dict[str, set[str]]:
    neighbors: dict[str, set[str]] = defaultdict(set)
    for row in relations:
        if not row.left_id or not row.right_id or row.left_id == row.right_id:
            continue
        neighbors[row.left_id].add(row.right_id)
        neighbors[row.right_id].add(row.left_id)
    return {key: set(value) for key, value in neighbors.items()}


def build_concept_ancestors(relations: list[AlignRelationRecord]) -> dict[str, set[str]]:
    parents_by_child: dict[str, set[str]] = defaultdict(set)
    concept_ids: set[str] = set()
    for row in relations:
        if row.relation != "is_subordinate" or not row.left_id or not row.right_id:
            continue
        parents_by_child[row.left_id].add(row.right_id)
        concept_ids.add(row.left_id)
        concept_ids.add(row.right_id)
    ancestors: dict[str, set[str]] = {}
    for concept_id in concept_ids:
        seen: set[str] = set()
        stack = list(parents_by_child.get(concept_id, set()))
        while stack:
            parent_id = stack.pop()
            if parent_id in seen:
                continue
            seen.add(parent_id)
            stack.extend(parents_by_child.get(parent_id, set()))
        ancestors[concept_id] = seen
    return ancestors


def build_root_ancestors(
    concept_root_ids: dict[str, tuple[str, ...]],
    graph_bundle: GraphBundle,
    node_index: dict[str, NodeRecord],
) -> dict[str, set[str]]:
    parent_by_child = {
        edge.target: edge.source
        for edge in graph_bundle.edges
        if edge.type == "CONTAINS"
    }
    cache: dict[str, set[str]] = {}

    def ancestors_for_root(root_id: str) -> set[str]:
        cached = cache.get(root_id)
        if cached is not None:
            return set(cached)
        current = root_id
        ancestors: set[str] = set()
        while current in parent_by_child:
            current = parent_by_child[current]
            level = str(getattr(node_index.get(current), "level", "")).strip()
            if level in LOCAL_STRUCTURAL_LEVELS:
                ancestors.add(current)
        cache[root_id] = set(ancestors)
        return ancestors

    root_ancestors: dict[str, set[str]] = {}
    for concept_id, root_ids in concept_root_ids.items():
        combined: set[str] = set()
        for root_id in root_ids:
            combined.update(ancestors_for_root(root_id))
        root_ancestors[concept_id] = combined
    return root_ancestors


def build_auxiliary_neighbors(
    concept_structural_contexts: dict[str, set[str]],
    relation_neighbors: dict[str, set[str]],
) -> dict[str, set[str]]:
    neighbors: dict[str, set[str]] = defaultdict(set)
    for concept_id, structural_contexts in concept_structural_contexts.items():
        neighbors[concept_id].update(structural_contexts)
        for context_id in structural_contexts:
            neighbors[context_id].add(concept_id)
    for concept_id, relation_ids in relation_neighbors.items():
        neighbors[concept_id].update(relation_ids)
    return {key: set(value) for key, value in neighbors.items()}


def build_root_context_indexes(
    concept_root_ids: dict[str, tuple[str, ...]],
    parent_by_child: dict[str, str],
    node_index: dict[str, NodeRecord],
) -> tuple[dict[str, set[str]], dict[str, set[str]], dict[str, tuple[str, ...]]]:
    root_bridge_contexts: dict[str, tuple[str, ...]] = {}
    root_structural_contexts: dict[str, set[str]] = {}
    root_document_contexts: dict[str, set[str]] = {}

    def contexts_for_root(root_id: str) -> tuple[tuple[str, ...], set[str], set[str]]:
        bridge_cached = root_bridge_contexts.get(root_id)
        structural_cached = root_structural_contexts.get(root_id)
        document_cached = root_document_contexts.get(root_id)
        if bridge_cached is not None and structural_cached is not None and document_cached is not None:
            return bridge_cached, set(structural_cached), set(document_cached)
        bridge_units: list[str] = []
        structural_units: set[str] = set()
        document_units: set[str] = set()
        current = root_id
        visited: set[str] = set()
        while current and current not in visited:
            visited.add(current)
            level = str(getattr(node_index.get(current), "level", "")).strip()
            if level in BRIDGE_LEVELS and current not in bridge_units:
                bridge_units.append(current)
            if level in LOCAL_STRUCTURAL_LEVELS:
                structural_units.add(current)
            if level == "document":
                document_units.add(current)
            current = parent_by_child.get(current, "")
        if not bridge_units and root_id:
            bridge_units = [root_id]
        root_bridge_contexts[root_id] = tuple(bridge_units)
        root_structural_contexts[root_id] = set(structural_units)
        root_document_contexts[root_id] = set(document_units)
        return root_bridge_contexts[root_id], set(structural_units), set(document_units)

    concept_structural_contexts: dict[str, set[str]] = {}
    concept_document_contexts: dict[str, set[str]] = {}
    for concept_id, root_ids in concept_root_ids.items():
        combined_structural: set[str] = set()
        combined_documents: set[str] = set()
        for root_id in root_ids:
            _, structural_units, document_units = contexts_for_root(root_id)
            combined_structural.update(structural_units)
            combined_documents.update(document_units)
        concept_structural_contexts[concept_id] = combined_structural
        concept_document_contexts[concept_id] = combined_documents
    return concept_structural_contexts, concept_document_contexts, root_bridge_contexts


def build_bridge_index(
    graph_bundle: GraphBundle,
    node_index: dict[str, NodeRecord],
    parent_by_child: dict[str, str],
) -> tuple[dict[tuple[str, str], int], dict[tuple[str, str], tuple[str, str, str]]]:
    counts: dict[tuple[str, str], int] = defaultdict(int)
    samples: dict[tuple[str, str], tuple[str, str, str]] = {}

    def anchor_for_node(node_id: str) -> str:
        current = node_id
        visited: set[str] = set()
        document_id = ""
        while current and current not in visited:
            visited.add(current)
            level = str(getattr(node_index.get(current), "level", "")).strip()
            if level in LOCAL_STRUCTURAL_LEVELS:
                return current
            if level == "document":
                document_id = current
            current = parent_by_child.get(current, "")
        return document_id or node_id

    for edge in graph_bundle.edges:
        if edge.type not in {"REFERENCES", "INTERPRETS"}:
            continue
        source_anchor = anchor_for_node(edge.source)
        target_anchor = anchor_for_node(edge.target)
        if not source_anchor or not target_anchor or source_anchor == target_anchor:
            continue
        key = tuple(sorted((source_anchor, target_anchor)))
        counts[key] += 1
        if key not in samples:
            source_name = str(getattr(node_index.get(edge.source), "name", "")).strip() or edge.source
            target_name = str(getattr(node_index.get(edge.target), "name", "")).strip() or edge.target
            samples[key] = (edge.type, source_name, target_name)
    return dict(counts), dict(samples)


def semantic_score(left_id: str, right_id: str, store: InferFeatureStore) -> float:
    score = cosine_similarity(
        store.concept_vectors.get(left_id, []),
        store.concept_vectors.get(right_id, []),
    )
    return max(0.0, min(1.0, (score + 1.0) / 2.0))


def aa_score(left_id: str, right_id: str, store: InferFeatureStore) -> float:
    left_neighbors = store.aux_neighbors.get(left_id, set())
    right_neighbors = store.aux_neighbors.get(right_id, set())
    common = left_neighbors & right_neighbors
    if not common:
        return 0.0
    score = 0.0
    for neighbor_id in common:
        degree = len(store.aux_neighbors.get(neighbor_id, set()))
        if degree <= 1:
            continue
        score += 1.0 / math.log(degree)
    return max(0.0, min(1.0, 1.0 - math.exp(-score)))


def ca_score(left_id: str, right_id: str, store: InferFeatureStore) -> float:
    left_ancestors = set(store.concept_ancestors.get(left_id, set())) | set(store.root_ancestors.get(left_id, set()))
    right_ancestors = set(store.concept_ancestors.get(right_id, set())) | set(store.root_ancestors.get(right_id, set()))
    if not left_ancestors or not right_ancestors:
        return 0.0
    denominator = max(len(left_ancestors), len(right_ancestors), 1)
    if denominator <= 0:
        return 0.0
    return float(len(left_ancestors & right_ancestors) / denominator)


def bridge_evidence(left_id: str, right_id: str, store: InferFeatureStore) -> InferBridgeEvidence:
    left_roots = store.concept_root_ids.get(left_id, ())
    right_roots = store.concept_root_ids.get(right_id, ())
    if not left_roots or not right_roots:
        return InferBridgeEvidence()
    best_unit_pair: tuple[str, str] | None = None
    best_root_pair: tuple[str, str] | None = None
    best_count = 0
    for left_root in left_roots:
        left_units = store.root_bridge_contexts.get(left_root, (left_root,))
        for right_root in right_roots:
            right_units = store.root_bridge_contexts.get(right_root, (right_root,))
            for left_unit in left_units:
                for right_unit in right_units:
                    if not left_unit or not right_unit or left_unit == right_unit:
                        continue
                    unit_pair = tuple(sorted((left_unit, right_unit)))
                    count = int(store.bridge_counts.get(unit_pair, 0))
                    if count > best_count or (
                        count == best_count
                        and best_root_pair is not None
                        and (left_root, right_root, left_unit, right_unit) < (
                            best_root_pair[0],
                            best_root_pair[1],
                            best_unit_pair[0] if best_unit_pair else "",
                            best_unit_pair[1] if best_unit_pair else "",
                        )
                    ):
                        best_count = count
                        best_root_pair = (left_root, right_root)
                        best_unit_pair = unit_pair
    if best_root_pair is None or best_unit_pair is None or best_count <= 0:
        return InferBridgeEvidence()
    edge_type, source_name, target_name = store.bridge_samples.get(best_unit_pair, ("", "", ""))
    left_title = str(getattr(store.node_index.get(best_unit_pair[0]), "name", "")).strip() or best_unit_pair[0]
    right_title = str(getattr(store.node_index.get(best_unit_pair[1]), "name", "")).strip() or best_unit_pair[1]
    max_count = max(int(store.max_bridge_count or 0), best_count, 1)
    summary = (
        f"{left_title} 与 {right_title} 之间存在 {best_count} 条显式桥接，"
        f"示例为 {edge_type}: {source_name} -> {target_name}"
    )
    return InferBridgeEvidence(
        count=best_count,
        score=max(0.0, min(1.0, math.log1p(best_count) / math.log1p(max_count))),
        evidence_root_ids=best_root_pair,
        summary=summary,
    )


def shares_local_structural_context(left_id: str, right_id: str, store: InferFeatureStore) -> bool:
    return bool(store.concept_structural_contexts.get(left_id, set()) & store.concept_structural_contexts.get(right_id, set()))


def shares_document_context(left_id: str, right_id: str, store: InferFeatureStore) -> bool:
    return bool(store.concept_document_contexts.get(left_id, set()) & store.concept_document_contexts.get(right_id, set()))
