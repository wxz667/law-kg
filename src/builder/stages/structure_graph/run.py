from __future__ import annotations

from pathlib import Path
from typing import Callable

from ...contracts import GraphBundle
from ...io import BuildLayout
from .graph_builder import run_structure_graph
from .loader import load_document_units


def run(
    data_root: Path,
    *,
    source_ids: list[str] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> GraphBundle:
    data_root = data_root.resolve()
    layout = BuildLayout(data_root)
    units = load_document_units(layout.normalize_index_path(), source_ids=source_ids)
    return run_structure_graph(units, progress_callback=progress_callback)
