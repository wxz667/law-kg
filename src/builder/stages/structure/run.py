from __future__ import annotations

from pathlib import Path
from typing import Callable

from ...contracts import GraphBundle
from .graph_builder import run_structure
from .loader import load_document_units


def run(
    data_root: Path,
    *,
    source_ids: list[str] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
    checkpoint_every: int = 0,
    checkpoint_callback: Callable[[GraphBundle, list[str], int, int], None] | None = None,
) -> GraphBundle:
    data_root = data_root.resolve()
    units = load_document_units((data_root / "intermediate" / "builder" / "01_normalize" / "normalize_index.json"), source_ids=source_ids)
    return run_structure(
        units,
        progress_callback=progress_callback,
        checkpoint_every=checkpoint_every,
        checkpoint_callback=checkpoint_callback,
    )
