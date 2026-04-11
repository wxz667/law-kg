from __future__ import annotations

from dataclasses import asdict, dataclass, field, fields
from typing import Any


def _filter_dataclass_payload(payload: dict[str, Any], dataclass_type: type) -> dict[str, Any]:
    field_names = {item.name for item in fields(dataclass_type)}
    return {key: value for key, value in payload.items() if key in field_names}


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
        return cls(**_filter_dataclass_payload(payload, cls))


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
        return cls(**_filter_dataclass_payload(payload, cls))


@dataclass
class AstNodeRecord:
    node_id: str
    level: str
    heading: str
    text: str = ""
    parent_id: str = ""
    start_line: int = 0
    end_line: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "AstNodeRecord":
        return cls(**_filter_dataclass_payload(payload, cls))


@dataclass
class NormalizedDocumentRecord:
    source_id: str
    title: str
    content: str
    appendix_lines: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "source_id": self.source_id,
            "title": self.title,
            "content": self.content,
            "appendix_lines": list(self.appendix_lines),
        }
        payload.update(self.metadata)
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "NormalizedDocumentRecord":
        metadata = dict(payload)
        source_id = str(metadata.pop("source_id"))
        title = str(metadata.pop("title"))
        content = str(metadata.pop("content"))
        appendix_lines = list(metadata.pop("appendix_lines", []))
        return cls(
            source_id=source_id,
            title=title,
            content=content,
            appendix_lines=appendix_lines,
            metadata=metadata,
        )


@dataclass(frozen=True)
class DocumentUnitRecord:
    source_id: str
    title: str
    source_type: str
    body_lines: list[str]
    appendix_lines: list[str]
    metadata: dict[str, Any]


@dataclass
class NormalizeIndexEntry:
    source_id: str
    status: str
    title: str = ""
    document_path: str = ""
    artifact_path: str = ""
    message: str = ""
    error_type: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "source_id": self.source_id,
            "status": self.status,
            "title": self.title,
            "document_path": self.document_path,
            "artifact_path": self.artifact_path,
            "message": self.message,
            "error_type": self.error_type,
            "details": self.details,
        }
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "NormalizeIndexEntry":
        return cls(
            source_id=str(payload["source_id"]),
            status=str(payload["status"]),
            title=str(payload.get("title", "")),
            document_path=str(payload.get("document_path", "")),
            artifact_path=str(payload.get("artifact_path", "")),
            message=str(payload.get("message", "")),
            error_type=str(payload.get("error_type", "")),
            details=dict(payload.get("details", {})),
        )


@dataclass
class NormalizeStageIndex:
    stage: str
    entries: list[NormalizeIndexEntry] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "entries": [entry.to_dict() for entry in self.entries],
            "stats": dict(self.stats),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "NormalizeStageIndex":
        return cls(
            stage=str(payload["stage"]),
            entries=[NormalizeIndexEntry.from_dict(item) for item in payload.get("entries", [])],
            stats=dict(payload.get("stats", {})),
        )


@dataclass(frozen=True)
class ReferenceCandidateRecord:
    id: str
    source_node_id: str
    text: str
    target_node_ids: list[str]
    target_categories: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source_node_id": self.source_node_id,
            "text": self.text,
            "target_node_ids": list(self.target_node_ids),
            "target_categories": list(self.target_categories),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ReferenceCandidateRecord":
        return cls(
            id=str(payload["id"]),
            source_node_id=str(payload["source_node_id"]),
            text=str(payload["text"]),
            target_node_ids=[str(value) for value in payload.get("target_node_ids", []) if str(value).strip()],
            target_categories=[str(value) for value in payload.get("target_categories", []) if str(value).strip()],
        )


@dataclass(frozen=True)
class RelationClassifyRecord:
    id: str
    source_node_id: str
    text: str
    target_node_ids: list[str]
    target_categories: list[str]
    label: str
    score: float
    source: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source_node_id": self.source_node_id,
            "text": self.text,
            "target_node_ids": list(self.target_node_ids),
            "target_categories": list(self.target_categories),
            "label": self.label,
            "score": self.score,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "RelationClassifyRecord":
        return cls(
            id=str(payload["id"]),
            source_node_id=str(payload["source_node_id"]),
            text=str(payload["text"]),
            target_node_ids=[str(value) for value in payload.get("target_node_ids", []) if str(value).strip()],
            target_categories=[str(value) for value in payload.get("target_categories", []) if str(value).strip()],
            label=str(payload["label"]),
            score=float(payload.get("score", 0.0)),
            source=str(payload.get("source", "")),
        )


@dataclass(frozen=True)
class LlmJudgeDetailRecord:
    source_id: str
    text: str
    label: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "text": self.text,
            "label": self.label,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "LlmJudgeDetailRecord":
        return cls(
            source_id=str(payload["source_id"]),
            text=str(payload["text"]),
            label=str(payload["label"]),
            reason=str(payload.get("reason", "")),
        )
