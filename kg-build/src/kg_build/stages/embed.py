from __future__ import annotations

from ..contracts import GraphBundle


TODO_NOTE = "TODO: implement production-grade node embedding generation for legal graph entities."


def run(bundle: GraphBundle) -> tuple[GraphBundle, str]:
    return bundle, TODO_NOTE
