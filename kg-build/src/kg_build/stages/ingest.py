from __future__ import annotations

from pathlib import Path

from ..contracts import SourceDocumentRecord
from ..io import read_source_document


def run(source_path: Path) -> SourceDocumentRecord:
    return read_source_document(source_path)
