from __future__ import annotations

from ..contracts import GraphBundle
from ..llm import resolve_stage_model


TODO_NOTE = "TODO: implement production-grade entity aggregation and has_subordinate construction."


def run(bundle: GraphBundle) -> tuple[GraphBundle, str]:
    config = resolve_stage_model("aggr")
    note = (
        f"{TODO_NOTE} "
        f"[provider={config.provider} model={config.model} purpose={config.purpose}]"
    )
    return bundle, note
