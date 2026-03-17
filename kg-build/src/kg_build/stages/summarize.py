from __future__ import annotations

from ..contracts import GraphBundle


TODO_NOTE = "TODO: implement production-grade legal summarization for provisions and catalog nodes."


def run(bundle: GraphBundle) -> tuple[GraphBundle, str]:
    return bundle, TODO_NOTE
