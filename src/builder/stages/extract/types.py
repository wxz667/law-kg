from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ...contracts import ExtractConceptRecord, ExtractInputRecord


@dataclass
class ExtractResult:
    inputs: list[ExtractInputRecord] = field(default_factory=list)
    concepts: list[ExtractConceptRecord] = field(default_factory=list)
    processed_source_ids: list[str] = field(default_factory=list)
    processed_input_ids: list[str] = field(default_factory=list)
    successful_input_ids: list[str] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)
    llm_errors: list[dict[str, Any]] = field(default_factory=list)
