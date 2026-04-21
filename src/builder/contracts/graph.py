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


def _node_schema() -> dict[str, dict[str, Any]]:
    payload = load_graph_schema().get("nodes", {})
    return {
        str(node_type): dict(config)
        for node_type, config in payload.items()
        if isinstance(config, dict)
    }


def _edge_schema() -> dict[str, dict[str, Any]]:
    payload = load_graph_schema().get("edges", {})
    return {
        str(edge_type): dict(config)
        for edge_type, config in payload.items()
        if isinstance(config, dict)
    }


def _node_allowed_fields() -> dict[str, tuple[str, ...]]:
    return {
        node_type: tuple(str(field_name) for field_name in config.get("fields", []))
        for node_type, config in _node_schema().items()
    }


def _edge_types() -> set[str]:
    return set(_edge_schema())


def _levels() -> set[str]:
    return {
        str(level)
        for config in _node_schema().values()
        for level in config.get("levels", [])
    }


def graph_level_order() -> list[str]:
    schema = load_graph_schema()
    raw = schema.get("level_order", [])
    if isinstance(raw, list) and raw:
        return [str(level) for level in raw]
    return sorted(_levels())


def level_to_node_type() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for node_type, config in _node_schema().items():
        for level in config.get("levels", []):
            mapping[str(level)] = node_type
    return mapping


def contains_edge_by_levels() -> dict[tuple[str, str], str]:
    return {
        (source_level, target_level): edge_type
        for source_level, target_level, edge_type, _source_type, _target_type in _edge_rules()
        if source_level and target_level and edge_type == "CONTAINS"
    }


def _edge_rules() -> set[tuple[str, str, str, str, str]]:
    rules: set[tuple[str, str, str, str, str]] = set()
    for edge_type, config in _edge_schema().items():
        for rule in config.get("rules", []):
            if not isinstance(rule, dict):
                continue
            source = rule.get("source", {})
            target = rule.get("target", {})
            if not isinstance(source, dict) or not isinstance(target, dict):
                continue
            rules.add(
                (
                    str(source.get("level", "") or ""),
                    str(target.get("level", "") or ""),
                    edge_type,
                    str(source.get("type", "") or ""),
                    str(target.get("type", "") or ""),
                )
            )
    return rules


def _edge_rule_matches(node: NodeRecord, constraint: dict[str, Any]) -> bool:
    source_type = str(constraint.get("type", "") or "")
    source_level = str(constraint.get("level", "") or "")
    if source_type and node.type != source_type:
        return False
    if source_level and node.level != source_level:
        return False
    return True


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
        ):
            value = getattr(self, field_name)
            if value not in {"", None}:
                payload[field_name] = value
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
        if self.type not in _node_schema():
            raise ValueError(f"Unsupported node type: {self.type}")
        allowed_levels = {str(level) for level in _node_schema().get(self.type, {}).get("levels", [])}
        if self.level not in allowed_levels:
            raise ValueError(
                f"Node {self.id} has level {self.level} but type {self.type}; "
                f"schema allows levels {sorted(allowed_levels)}."
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

        edge_schema = _edge_schema()
        for edge in self.edges:
            source_node = node_index[edge.source]
            target_node = node_index[edge.target]
            rules = edge_schema.get(edge.type, {}).get("rules", [])
            if not isinstance(rules, list) or not rules:
                raise ValueError(
                    f"Edge {edge.id} of type {edge.type} has no schema rules."
                )
            if not any(
                isinstance(rule, dict)
                and _edge_rule_matches(source_node, rule.get("source", {}))
                and _edge_rule_matches(target_node, rule.get("target", {}))
                for rule in rules
            ):
                raise ValueError(
                    f"Edge {edge.id} violates schema rule: "
                    f"{source_node.type}({source_node.level}) -> "
                    f"{target_node.type}({target_node.level}) via {edge.type}."
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
