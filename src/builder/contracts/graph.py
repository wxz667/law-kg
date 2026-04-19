from __future__ import annotations

import json
from dataclasses import dataclass, field, fields
from functools import lru_cache
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..utils.ids import project_root


@lru_cache(maxsize=1)
def load_graph_schema() -> dict[str, Any]:
    path = Path(project_root()) / "configs" / "schema.json"
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


def _semantic_edge_rules() -> set[tuple[str, str, str]]:
    return {
        (rule["source_type"], rule["target_type"], rule["edge_type"])
        for rule in load_graph_schema().get("semantic_edge_rules", [])
    }


@dataclass
class NodeRecord:
    id: str
    type: str
    name: str
    level: str
    text: str = ""
    description: str = ""
    category: str = ""
    status: str = ""
    issuer: str = ""
    publish_date: str = ""
    effective_date: str = ""
    source_url: str = ""
    order: int = 0
    article_suffix: int = 0
    candidate: bool = False
    alignment_status: str = ""
    normalized_text: str = ""
    aliases: list[str] = field(default_factory=list)
    normalized_values: list[str] = field(default_factory=list)
    source_members: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        payload: dict[str, Any] = {
            "id": self.id,
            "type": self.type,
            "name": self.name,
            "level": self.level,
        }
        for field_name in (
            "text",
            "description",
            "category",
            "status",
            "issuer",
            "publish_date",
            "effective_date",
            "source_url",
            "alignment_status",
            "normalized_text",
        ):
            value = getattr(self, field_name)
            if value not in {"", None}:
                payload[field_name] = value
        for field_name in ("order", "article_suffix"):
            value = int(getattr(self, field_name) or 0)
            if value > 0:
                payload[field_name] = value
        if self.candidate:
            payload["candidate"] = True
        if self.aliases:
            payload["aliases"] = list(self.aliases)
        if self.normalized_values:
            payload["normalized_values"] = list(self.normalized_values)
        if self.source_members:
            payload["source_members"] = list(self.source_members)
        allowed_fields = set(_allowed_fields_for_type(self.type))
        return {key: value for key, value in payload.items() if key in allowed_fields}

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "NodeRecord":
        return cls._from_dict(payload, validate=True)

    @classmethod
    def from_dict_unchecked(cls, payload: dict[str, Any]) -> "NodeRecord":
        return cls._from_dict(payload, validate=False)

    @classmethod
    def _from_dict(cls, payload: dict[str, Any], *, validate: bool) -> "NodeRecord":
        node_type = str(payload.get("type", "") or "")
        field_names = {item.name for item in fields(cls)}
        allowed_payload_fields = {"id", "type", "name", "level"}
        if node_type:
            allowed_payload_fields |= set(_allowed_fields_for_type(node_type))
        filtered = {
            key: value
            for key, value in payload.items()
            if key in field_names and key in allowed_payload_fields
        }
        node = cls(**filtered)
        if validate:
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
        for field_name in (
            "text",
            "description",
            "category",
            "status",
            "issuer",
            "publish_date",
            "effective_date",
            "source_url",
            "order",
            "article_suffix",
            "candidate",
            "alignment_status",
            "normalized_text",
            "aliases",
            "normalized_values",
            "source_members",
        ):
            field_value = getattr(self, field_name)
            if not _is_empty_value(field_value) and field_name not in allowed_fields:
                raise ValueError(
                    f"Node {self.id} of type {self.type} contains illegal populated field: {field_name}"
                )


@dataclass
class EdgeRecord:
    id: str
    source: str
    target: str
    type: str

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "id": self.id,
            "source": self.source,
            "target": self.target,
            "type": self.type,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "EdgeRecord":
        return cls._from_dict(payload, validate=True)

    @classmethod
    def from_dict_unchecked(cls, payload: dict[str, Any]) -> "EdgeRecord":
        return cls._from_dict(payload, validate=False)

    @classmethod
    def _from_dict(cls, payload: dict[str, Any], *, validate: bool) -> "EdgeRecord":
        field_names = {item.name for item in fields(cls)}
        filtered = {key: value for key, value in payload.items() if key in field_names}
        edge = cls(**filtered)
        if validate:
            edge.validate()
        return edge

    def validate(self) -> None:
        if self.type not in _edge_types():
            raise ValueError(f"Unsupported edge type: {self.type}")


@dataclass
class GraphBundle:
    nodes: list[NodeRecord] = field(default_factory=list)
    edges: list[EdgeRecord] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "nodes": [node.to_dict() for node in self.nodes],
            "edges": [edge.to_dict() for edge in self.edges],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "GraphBundle":
        return cls(
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
        structural_edge_types = _structural_edge_types()
        semantic_edge_rules = _semantic_edge_rules()
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
            if edge.type not in structural_edge_types and semantic_edge_rules and (
                source_node.type,
                target_node.type,
                edge.type,
            ) not in semantic_edge_rules:
                matching_rules = [rule for rule in semantic_edge_rules if rule[2] == edge.type]
                if matching_rules:
                    raise ValueError(
                        f"Semantic edge {edge.id} violates schema rule: "
                        f"{source_node.type} -> {target_node.type} via {edge.type}."
                    )


def deduplicate_graph(bundle: GraphBundle) -> GraphBundle:
    node_index: dict[str, NodeRecord] = {}
    for node in bundle.nodes:
        node_index[node.id] = node

    edge_index: dict[tuple[str, str, str], EdgeRecord] = {}
    for edge in bundle.edges:
        key = (edge.source, edge.target, edge.type)
        if key not in edge_index:
            edge_index[key] = edge

    deduped = GraphBundle(nodes=list(node_index.values()), edges=list(edge_index.values()))
    deduped.validate_edge_references()
    return deduped

def build_edge_id(source: str = "", target: str = "", edge_type: str = "", suffix: str = "") -> str:
    del source, target, edge_type, suffix
    return f"edge:{uuid4()}"


def _allowed_fields_for_type(node_type: str) -> tuple[str, ...]:
    node_allowed_fields = _node_allowed_fields()
    if node_type not in node_allowed_fields:
        raise ValueError(f"Unsupported node type: {node_type}")
    return node_allowed_fields[node_type]


def _is_empty_value(value: Any) -> bool:
    if value in ("", None, 0, False):
        return True
    if isinstance(value, list):
        return len(value) == 0
    return False
