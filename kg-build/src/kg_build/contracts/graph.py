from __future__ import annotations

from dataclasses import asdict, dataclass, field, fields
from typing import Any

COMMON_NODE_FIELDS = ("id", "type", "name", "level", "source_id", "metadata")

NODE_ALLOWED_FIELDS: dict[str, tuple[str, ...]] = {
    "DocumentNode": COMMON_NODE_FIELDS,
    "TocNode": COMMON_NODE_FIELDS + ("summary", "address"),
    "ProvisionNode": COMMON_NODE_FIELDS + ("text", "summary", "address"),
    "EntityNode": COMMON_NODE_FIELDS + ("summary", "description", "embedding_ref"),
    "AppendixNode": COMMON_NODE_FIELDS + ("text", "summary", "address"),
    "AppendixItemNode": COMMON_NODE_FIELDS + ("text", "summary", "address"),
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
    source_id: str
    text: str = ""
    summary: str = ""
    description: str = ""
    embedding_ref: str = ""
    address: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        payload = {
            "id": self.id,
            "type": self.type,
            "name": self.name,
            "level": self.level,
            "source_id": self.source_id,
            "metadata": self.metadata,
        }
        if self.text:
            payload["text"] = self.text
        if self.summary:
            payload["summary"] = self.summary
        if self.description:
            payload["description"] = self.description
        if self.embedding_ref:
            payload["embedding_ref"] = self.embedding_ref
        if self.address:
            payload["address"] = self.address
        allowed_fields = set(NODE_ALLOWED_FIELDS[self.type])
        return {key: value for key, value in payload.items() if key in allowed_fields}

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "NodeRecord":
        field_names = {item.name for item in fields(cls)}
        filtered = {key: value for key, value in payload.items() if key in field_names}
        node = cls(**filtered)
        node.validate()
        return node

    def validate(self) -> None:
        if self.type not in NODE_ALLOWED_FIELDS:
            raise ValueError(f"Unsupported node type: {self.type}")
        illegal_populated_fields: list[str] = []
        if self.text and "text" not in NODE_ALLOWED_FIELDS[self.type]:
            illegal_populated_fields.append("text")
        if self.summary and "summary" not in NODE_ALLOWED_FIELDS[self.type]:
            illegal_populated_fields.append("summary")
        if self.description and "description" not in NODE_ALLOWED_FIELDS[self.type]:
            illegal_populated_fields.append("description")
        if self.embedding_ref and "embedding_ref" not in NODE_ALLOWED_FIELDS[self.type]:
            illegal_populated_fields.append("embedding_ref")
        if self.address and "address" not in NODE_ALLOWED_FIELDS[self.type]:
            illegal_populated_fields.append("address")
        if illegal_populated_fields:
            field_list = ", ".join(illegal_populated_fields)
            raise ValueError(
                f"Node {self.id} of type {self.type} contains illegal populated fields: {field_list}"
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
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "EdgeRecord":
        field_names = {item.name for item in fields(cls)}
        filtered = {key: value for key, value in payload.items() if key in field_names}
        return cls(**filtered)


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
        node_ids = {node.id for node in self.nodes}
        missing = [
            edge.id
            for edge in self.edges
            if edge.source not in node_ids or edge.target not in node_ids
        ]
        if missing:
            raise ValueError(f"Graph bundle contains edges with missing node references: {missing}")
