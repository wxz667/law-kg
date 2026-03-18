from __future__ import annotations

from pathlib import Path

from ..contracts import GraphBundle
from ..io import write_graph_bundle, write_json


def run(
    bundle: GraphBundle,
    graph_dir: Path,
    source_stage: str = "",
    source_bundle_path: Path | None = None,
) -> dict[str, str]:
    graph_dir.mkdir(parents=True, exist_ok=True)
    bundle.validate_edge_references()
    # 最终交付物只保留纯图结构，不携带任何中间阶段上下文。
    delivery_bundle = GraphBundle(
        graph_id=bundle.graph_id,
        nodes=bundle.nodes,
        edges=bundle.edges,
    )

    bundle_path = graph_dir / "graph.bundle.json"
    status_path = graph_dir / "serialize_result.json"

    write_graph_bundle(bundle_path, delivery_bundle)
    write_json(
        status_path,
        {
            "stage": "serialize",
            "status": "completed",
            "notes": (
                "Serialized the latest available graph bundle."
                if not source_stage
                else f"Serialized the latest available graph bundle from stage '{source_stage}'."
            ),
            "source_stage": source_stage,
            "source_bundle_path": "" if source_bundle_path is None else str(source_bundle_path.resolve()),
            "artifact_paths": {
                "graph_bundle": str(bundle_path.resolve()),
            },
        },
    )
    return {
        "graph_bundle": str(bundle_path.resolve()),
        "serialize_result": str(status_path.resolve()),
    }
