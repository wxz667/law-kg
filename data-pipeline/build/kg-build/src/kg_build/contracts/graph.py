from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, fields
from functools import lru_cache
from pathlib import Path
from typing import Any

from ..utils.ids import project_root


@lru_cache(maxsize=1)
def load_graph_schema() -> dict[str, Any]:
    path = Path(project_root()) / "resources" / "schema.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _node_allowed_fields() -> dict[str, tuple[str, ...]]:
    schema = load_graph_schema()
    return {
        node_type: tuple(field_names)
        for node_type, field_names in schema.get("node_type_fields", {}).items()
    }


def _edge_types() -> set[str]:
    return set(load_graph_schema().get("edge_types", []))


def _levels() -> set[str]:
    return set(load_graph_schema().get("levels", []))


def _level_to_node_type() -> dict[str, str]:
    return dict(load_graph_schema().get("level_to_node_type", {}))


def _allowed_outgoing_edges() -> dict[str, set[str]]:
    schema = load_graph_schema()
    return {
        node_type: set(edge_types)
        for node_type, edge_types in schema.get("allowed_outgoing_edges", {}).items()
    }


def _structural_edge_rules() -> set[tuple[str, str, str]]:
    rules = load_graph_schema().get("structural_edges", [])
    return {
        (rule["parent_level"], rule["child_level"], rule["edge_type"])
        for rule in rules
    }


def _structural_edge_types() -> set[str]:
    schema = load_graph_schema()
    return {
        edge_type
        for edge_type, category in schema.get("edge_type_categories", {}).items()
        if category == "structural"
    }


@dataclass
class SourceDocumentRecord:
    source_id: str
    title: str
    source_path: str
    source_type: str
    checksum: str
    preface_text: str = ""
    toc_lines: list[str] = field(default_factory=list)
    body_lines: list[str] = field(default_factory=list)
    appendix_lines: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "SourceDocumentRecord":
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
    document_type: str = ""
    document_subtype: str = ""
    status: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        payload: dict[str, Any] = {
            "id": self.id,
            "type": self.type,
            "name": self.name,
            "level": self.level,
            "metadata": self.metadata,
        }
        if self.text:
            payload["text"] = self.text
        if self.document_type:
            payload["document_type"] = self.document_type
        if self.document_subtype:
            payload["document_subtype"] = self.document_subtype
        if self.status:
            payload["status"] = self.status
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
        for field_name in ("document_type", "document_subtype", "status"):
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
    graph_id: str
    nodes: list[NodeRecord]
    edges: list[EdgeRecord]

    def to_dict(self) -> dict[str, Any]:
        return {
            "graph_id": self.graph_id,
            "nodes": [node.to_dict() for node in self.nodes],
            "edges": [edge.to_dict() for edge in self.edges],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "GraphBundle":
        return cls(
            graph_id=payload["graph_id"],
            nodes=[NodeRecord.from_dict(node) for node in payload.get("nodes", [])],
            edges=[EdgeRecord.from_dict(edge) for edge in payload.get("edges", [])],
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
        allowed_outgoing_edges = _allowed_outgoing_edges()
        structural_edge_types = _structural_edge_types()

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


def _allowed_fields_for_type(node_type: str) -> tuple[str, ...]:
    node_allowed_fields = _node_allowed_fields()
    if node_type not in node_allowed_fields:
        raise ValueError(f"Unsupported node type: {node_type}")
    return node_allowed_fields[node_type]
