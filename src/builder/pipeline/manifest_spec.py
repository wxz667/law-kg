from __future__ import annotations

from ..contracts import GraphBundle
from ..io import BuildLayout

STAGE_UNIT_KIND = {
    "normalize": "source",
    "structure": "source",
    "detect": "node",
    "classify": "candidate",
    "extract": "node",
    "aggregate": "node",
    "align": "node",
}

SUBSTAGE_UNIT_KIND = {
    ("classify", "model"): "candidate",
    ("classify", "llm_judge"): "candidate",
    ("extract", "input"): "source",
    ("extract", "extract"): "node",
    ("align", "embed"): "concept",
    ("align", "recall"): "concept",
    ("align", "judge"): "pair",
}


def stage_inputs(layout: BuildLayout, stage_name: str) -> list[str]:
    data_root = layout.data_root
    if stage_name == "normalize":
        return [
            str(data_root / "source" / "metadata"),
            str(data_root / "source" / "docs"),
        ]
    if stage_name == "structure":
        return [
            str(layout.normalize_documents_dir()),
            str(layout.normalize_index_path()),
        ]
    if stage_name == "detect":
        return [str(layout.stage_nodes_path("structure")), str(layout.stage_edges_path("structure"))]
    if stage_name == "classify":
        return [
            str(layout.detect_candidates_path()),
            str(layout.stage_nodes_path("structure")),
            str(layout.stage_edges_path("structure")),
        ]
    if stage_name == "extract":
        return [str(layout.stage_nodes_path("classify")), str(layout.stage_edges_path("classify"))]
    if stage_name == "aggregate":
        return [
            str(layout.stage_nodes_path("classify")),
            str(layout.stage_edges_path("classify")),
            str(layout.extract_inputs_path()),
            str(layout.extract_concepts_path()),
        ]
    if stage_name == "align":
        return [
            str(layout.stage_nodes_path("classify")),
            str(layout.stage_edges_path("classify")),
            str(layout.aggregate_concepts_path()),
        ]
    raise ValueError(f"Unsupported stage: {stage_name}")


def stage_artifacts(layout: BuildLayout, stage_name: str) -> list[str]:
    if stage_name == "normalize":
        return [str(layout.normalize_documents_dir()), str(layout.normalize_index_path())]
    if stage_name == "structure":
        return [str(layout.stage_nodes_path("structure")), str(layout.stage_edges_path("structure"))]
    if stage_name == "detect":
        return [str(layout.detect_candidates_path())]
    if stage_name == "classify":
        return [
            str(layout.classify_results_path()),
            str(layout.classify_pending_path()),
            str(layout.classify_llm_judge_path()),
            str(layout.stage_edges_path("classify")),
        ]
    if stage_name == "extract":
        return [str(layout.extract_inputs_path()), str(layout.extract_concepts_path())]
    if stage_name == "aggregate":
        return [str(layout.aggregate_concepts_path())]
    if stage_name == "align":
        return [
            str(layout.aggregate_concepts_path()),
            str(layout.align_vectors_path()),
            str(layout.align_pairs_path()),
            str(layout.align_concepts_path()),
            str(layout.align_relations_path()),
            str(layout.stage_nodes_path("align")),
            str(layout.stage_edges_path("align")),
        ]
    raise ValueError(f"Unsupported stage: {stage_name}")


def substage_inputs(layout: BuildLayout, parent_stage: str, substage_name: str) -> list[str]:
    if (parent_stage, substage_name) == ("classify", "model"):
        return [
            str(layout.detect_candidates_path()),
            str(layout.stage_nodes_path("structure")),
            str(layout.stage_edges_path("structure")),
        ]
    if (parent_stage, substage_name) == ("classify", "llm_judge"):
        return [str(layout.classify_pending_path())]
    if (parent_stage, substage_name) == ("extract", "input"):
        return [str(layout.stage_nodes_path("classify")), str(layout.stage_edges_path("classify"))]
    if (parent_stage, substage_name) == ("extract", "extract"):
        return [str(layout.extract_inputs_path())]
    if (parent_stage, substage_name) == ("align", "embed"):
        return [str(layout.aggregate_concepts_path())]
    if (parent_stage, substage_name) == ("align", "recall"):
        return [
            str(layout.aggregate_concepts_path()),
            str(layout.align_vectors_path()),
            str(layout.align_concepts_path()),
        ]
    if (parent_stage, substage_name) == ("align", "judge"):
        return [
            str(layout.aggregate_concepts_path()),
            str(layout.align_pairs_path()),
            str(layout.align_concepts_path()),
        ]
    raise ValueError(f"Unsupported substage: {parent_stage}::{substage_name}")


def substage_artifacts(layout: BuildLayout, parent_stage: str, substage_name: str) -> list[str]:
    if (parent_stage, substage_name) == ("classify", "model"):
        return [str(layout.classify_results_path()), str(layout.classify_pending_path())]
    if (parent_stage, substage_name) == ("classify", "llm_judge"):
        return [
            str(layout.classify_results_path()),
            str(layout.classify_llm_judge_path()),
            str(layout.stage_edges_path("classify")),
        ]
    if (parent_stage, substage_name) == ("extract", "input"):
        return [str(layout.extract_inputs_path())]
    if (parent_stage, substage_name) == ("extract", "extract"):
        return [str(layout.extract_concepts_path())]
    if (parent_stage, substage_name) == ("align", "embed"):
        return [str(layout.align_vectors_path())]
    if (parent_stage, substage_name) == ("align", "recall"):
        return [str(layout.align_pairs_path())]
    if (parent_stage, substage_name) == ("align", "judge"):
        return [str(layout.align_pairs_path())]
    raise ValueError(f"Unsupported substage: {parent_stage}::{substage_name}")


def stage_unit(stage_name: str) -> str:
    return STAGE_UNIT_KIND[stage_name]


def substage_unit(parent_stage: str, substage_name: str) -> str:
    return SUBSTAGE_UNIT_KIND[(parent_stage, substage_name)]


def graph_type_stats(graph_bundle: GraphBundle) -> dict[str, object]:
    node_type_counts: dict[str, int] = {}
    edge_type_counts: dict[str, int] = {}
    for node in graph_bundle.nodes:
        node_type_counts[node.type] = int(node_type_counts.get(node.type, 0)) + 1
    for edge in graph_bundle.edges:
        edge_type_counts[edge.type] = int(edge_type_counts.get(edge.type, 0)) + 1
    return {
        "node_count": len(graph_bundle.nodes),
        "edge_count": len(graph_bundle.edges),
        "node_type_counts": dict(sorted(node_type_counts.items())),
        "edge_type_counts": dict(sorted(edge_type_counts.items())),
    }
