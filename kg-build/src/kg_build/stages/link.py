from __future__ import annotations

from pathlib import Path
from typing import Any

from ..contracts import GraphBundle
from ..io import write_json, write_jsonl
from ..utils.ids import repo_root


def run(bundle: GraphBundle, stage_dir: Path) -> tuple[GraphBundle, dict[str, Any]]:
    stage_dir.mkdir(parents=True, exist_ok=True)

    samples_path = stage_dir / "relation_samples.jsonl"
    predictions_path = stage_dir / "relation_predictions.jsonl"

    samples = build_relation_samples(bundle)
    write_jsonl(samples_path, samples)

    predictions = predict_with_kg_link(samples_path, predictions_path)
    write_jsonl(predictions_path, predictions)

    return bundle, {
        "sample_count": len(samples),
        "prediction_count": len(predictions),
        "samples_path": str(samples_path.resolve()),
        "predictions_path": str(predictions_path.resolve()),
    }


def build_relation_samples(bundle: GraphBundle) -> list[dict[str, Any]]:
    node_index = {node.id: node for node in bundle.nodes}
    child_targets: set[str] = {edge.target for edge in bundle.edges if edge.type == "HAS_CHILD"}
    rows: list[dict[str, Any]] = []

    for node in bundle.nodes:
        if node.type not in {"DocumentNode", "ProvisionNode", "AppendixNode"}:
            continue
        parent_id = next(
            (
                edge.source
                for edge in bundle.edges
                if edge.type == "HAS_CHILD" and edge.target == node.id
            ),
            "",
        )
        rows.append(
            {
                "sample_id": f"sample:{node.id}",
                "source_node_id": node.id,
                "source_node_type": node.type,
                "source_name": node.name,
                "source_text": node.text,
                "parent_node_id": parent_id,
                "document_node_id": resolve_document_node_id(node.id, node_index, bundle),
                "has_children": node.id in child_targets,
                "metadata": node.metadata,
            }
        )
    return rows


def predict_with_kg_link(samples_path: Path, predictions_path: Path) -> list[dict[str, Any]]:
    kg_link_src = repo_root() / "kg-link" / "src"
    if not kg_link_src.exists():
        write_json(
            predictions_path.with_suffix(".meta.json"),
            {
                "status": "skipped",
                "reason": "kg-link project is not available",
            },
        )
        return []

    import sys

    kg_link_src_text = str(kg_link_src.resolve())
    if kg_link_src_text not in sys.path:
        sys.path.insert(0, kg_link_src_text)

    from kg_link.api import predict_relations  # type: ignore

    return predict_relations(
        samples_path=samples_path,
        output_path=predictions_path,
        model_name="placeholder",
        config={"mode": "bootstrap"},
    )


def resolve_document_node_id(
    node_id: str,
    node_index: dict[str, Any],
    bundle: GraphBundle,
) -> str:
    if node_id.startswith("document:"):
        return node_id
    parent_map = {edge.target: edge.source for edge in bundle.edges if edge.type == "HAS_CHILD"}
    current = node_id
    while current in parent_map:
        current = parent_map[current]
        node = node_index.get(current)
        if node is not None and node.type == "DocumentNode":
            return current
    return ""
