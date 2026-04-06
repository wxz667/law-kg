from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, fields
from functools import lru_cache
from pathlib import Path
from typing import Any

from ..utils.ids import project_root, slugify


@lru_cache(maxsize=1)
def load_graph_schema() -> dict[str, Any]:
    path = Path(project_root()) / "resources" / "schema.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _node_allowed_fields() -> dict[str, tuple[str, ...]]:
    return {
        node_type: tuple(field_names)
        for node_type, field_names in load_graph_schema().get("node_type_fields", {}).items()
    }


def _edge_types() -> set[str]:
    return set(load_graph_schema().get("edge_types", []))


def _levels() -> set[str]:
    return set(load_graph_schema().get("levels", []))


def _level_to_node_type() -> dict[str, str]:
    return dict(load_graph_schema().get("level_to_node_type", {}))


def _allowed_outgoing_edges() -> dict[str, set[str]]:
    return {
        node_type: set(edge_types)
        for node_type, edge_types in load_graph_schema().get("allowed_outgoing_edges", {}).items()
    }


def _structural_edge_rules() -> set[tuple[str, str, str]]:
    return {
        (rule["parent_level"], rule["child_level"], rule["edge_type"])
        for rule in load_graph_schema().get("structural_edges", [])
    }


def _structural_edge_types() -> set[str]:
    return {
        edge_type
        for edge_type, category in load_graph_schema().get("edge_type_categories", {}).items()
        if category == "structural"
    }


@dataclass
class PhysicalSourceRecord:
    source_id: str
    title: str
    source_path: str
    source_type: str
    checksum: str
    paragraphs: list[str] = field(default_factory=list)
    preface_text: str = ""
    toc_lines: list[str] = field(default_factory=list)
    body_lines: list[str] = field(default_factory=list)
    appendix_lines: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PhysicalSourceRecord":
        field_names = {item.name for item in fields(cls)}
        filtered = {key: value for key, value in payload.items() if key in field_names}
        return cls(**filtered)


SourceDocumentRecord = PhysicalSourceRecord


@dataclass
class LogicalDocumentRecord:
    source_id: str
    title: str
    source_type: str
    paragraphs: list[str] = field(default_factory=list)
    appendix_lines: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "LogicalDocumentRecord":
        field_names = {item.name for item in fields(cls)}
        filtered = {key: value for key, value in payload.items() if key in field_names}
        return cls(**filtered)


@dataclass
class NodeRecord:
    id: str
    type: str
    name: str
    level: str
    text: str = ""
    category: str = ""
    document_type: str = ""
    document_subtype: str = ""
    status: str = ""
    source_id: str = ""
    issuer: str = ""
    publish_date: str = ""
    effective_date: str = ""
    source_url: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        payload: dict[str, Any] = {
            "id": self.id,
            "type": self.type,
            "name": self.name,
            "level": self.level,
        }
        if self.text:
            payload["text"] = self.text
        if self.category:
            payload["category"] = self.category
        if self.document_type:
            payload["document_type"] = self.document_type
        if self.document_subtype:
            payload["document_subtype"] = self.document_subtype
        if self.status:
            payload["status"] = self.status
        if self.source_id:
            payload["source_id"] = self.source_id
        if self.issuer:
            payload["issuer"] = self.issuer
        if self.publish_date:
            payload["publish_date"] = self.publish_date
        if self.effective_date:
            payload["effective_date"] = self.effective_date
        if self.source_url:
            payload["source_url"] = self.source_url
        payload["metadata"] = self.metadata
        allowed_fields = set(_allowed_fields_for_type(self.type))
        return {key: value for key, value in payload.items() if key in allowed_fields}

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "NodeRecord":
        field_names = {item.name for item in fields(cls)}
        filtered = {key: value for key, value in payload.items() if key in field_names}
        node = cls(**filtered)
        node.validate()
        return node

    def validate(self) -> None:
        if self.level not in _levels():
            raise ValueError(f"Unsupported node level: {self.level}")
        if self.type not in load_graph_schema().get("node_types", []):
            raise ValueError(f"Unsupported node type: {self.type}")
        expected_node_type = _level_to_node_type().get(self.level)
        if expected_node_type != self.type:
            raise ValueError(
                f"Node {self.id} has level {self.level} but type {self.type}; "
                f"schema expects {expected_node_type}."
            )
        allowed_fields = _allowed_fields_for_type(self.type)
        if self.text and "text" not in allowed_fields:
            raise ValueError(
                f"Node {self.id} of type {self.type} contains illegal populated field: text"
            )
        for field_name in (
            "document_type",
            "document_subtype",
            "category",
            "status",
            "source_id",
            "issuer",
            "publish_date",
            "effective_date",
            "source_url",
        ):
            field_value = getattr(self, field_name)
            if field_value and field_name not in allowed_fields:
                raise ValueError(
                    f"Node {self.id} of type {self.type} contains illegal populated field: {field_name}"
                )


@dataclass
class EdgeRecord:
    id: str
    source: str
    target: str
    type: str
    weight: float = 1.0
    evidence: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "EdgeRecord":
        field_names = {item.name for item in fields(cls)}
        filtered = {key: value for key, value in payload.items() if key in field_names}
        edge = cls(**filtered)
        edge.validate()
        return edge

    def validate(self) -> None:
        if self.type not in _edge_types():
            raise ValueError(f"Unsupported edge type: {self.type}")


@dataclass
class GraphBundle:
    bundle_id: str
    document_id: str
    nodes: list[NodeRecord] = field(default_factory=list)
    edges: list[EdgeRecord] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "bundle_id": self.bundle_id,
            "document_id": self.document_id,
            "nodes": [node.to_dict() for node in self.nodes],
            "edges": [edge.to_dict() for edge in self.edges],
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "GraphBundle":
        return cls(
            bundle_id=payload["bundle_id"],
            document_id=payload["document_id"],
            nodes=[NodeRecord.from_dict(node) for node in payload.get("nodes", [])],
            edges=[EdgeRecord.from_dict(edge) for edge in payload.get("edges", [])],
            metadata=dict(payload.get("metadata", {})),
        )

    def validate_edge_references(self) -> None:
        for node in self.nodes:
            node.validate()
        for edge in self.edges:
            edge.validate()

        node_index = {node.id: node for node in self.nodes}
        missing = [
            edge.id
            for edge in self.edges
            if edge.source not in node_index or edge.target not in node_index
        ]
        if missing:
            raise ValueError(f"Graph bundle contains edges with missing node references: {missing}")

        structural_rules = _structural_edge_rules()
        structural_edge_types = _structural_edge_types()
        allowed_outgoing_edges = _allowed_outgoing_edges()

        for edge in self.edges:
            source_node = node_index[edge.source]
            target_node = node_index[edge.target]
            allowed_for_source = allowed_outgoing_edges.get(source_node.type, set())
            if edge.type not in allowed_for_source:
                raise ValueError(
                    f"Edge {edge.id} of type {edge.type} is not allowed from node type {source_node.type}."
                )
            if edge.type in structural_edge_types and (
                source_node.level,
                target_node.level,
                edge.type,
            ) not in structural_rules:
                raise ValueError(
                    f"Structural edge {edge.id} violates schema rule: "
                    f"{source_node.level} -> {target_node.level} via {edge.type}."
                )


def deduplicate_graph(bundle: GraphBundle) -> GraphBundle:
    node_index: dict[str, NodeRecord] = {}
    for node in bundle.nodes:
        node_index[node.id] = node

    edge_index: dict[tuple[str, str, str], EdgeRecord] = {}
    for edge in bundle.edges:
        key = (edge.source, edge.target, edge.type)
        if key not in edge_index or edge.weight > edge_index[key].weight:
            edge_index[key] = edge

    deduped = GraphBundle(
        bundle_id=bundle.bundle_id,
        document_id=bundle.document_id,
        nodes=list(node_index.values()),
        edges=list(edge_index.values()),
        metadata=dict(bundle.metadata),
    )
    deduped.validate_edge_references()
    return deduped


def merge_graph_bundles(
    bundles: list[GraphBundle],
    *,
    bundle_id: str,
    document_id: str = "corpus",
    metadata: dict[str, Any] | None = None,
) -> GraphBundle:
    merged = GraphBundle(
        bundle_id=bundle_id,
        document_id=document_id,
        nodes=[node for bundle in bundles for node in bundle.nodes],
        edges=[edge for bundle in bundles for edge in bundle.edges],
        metadata=metadata or {},
    )
    return deduplicate_graph(merged)


def build_edge_id(source: str, target: str, edge_type: str, suffix: str = "") -> str:
    base = f"edge:{slugify(edge_type)}:{slugify(source)}:{slugify(target)}"
    if suffix:
        return f"{base}:{suffix}"
    return base


def _allowed_fields_for_type(node_type: str) -> tuple[str, ...]:
    node_allowed_fields = _node_allowed_fields()
    if node_type not in node_allowed_fields:
        raise ValueError(f"Unsupported node type: {node_type}")
    return node_allowed_fields[node_type]
