from __future__ import annotations

from ..contracts import GraphBundle


TODO_NOTE = "TODO: implement production-grade entity deduplication and SAME_AS consolidation."


def run(bundle: GraphBundle) -> tuple[GraphBundle, str]:
    return bundle, TODO_NOTE
