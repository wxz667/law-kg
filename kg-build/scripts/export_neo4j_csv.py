#!/usr/bin/env python3
"""Export a graph bundle into Neo4j-friendly CSV files."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Iterable


NODE_FIELD_ORDER = [
    "id",
    "type",
    "name",
    "level",
    "source_id",
    "text",
    "summary",
    "description",
    "embedding_ref",
    "address",
]

EDGE_FIELD_ORDER = [
    "id",
    "source",
    "target",
    "type",
    "weight",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export a law-kg graph bundle to Neo4j CSV files."
    )
    parser.add_argument(
        "--bundle",
        required=True,
        help="Path to graph.bundle.json (for example data/intermediate/04_extract/graph.bundle.json).",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where nodes.csv and edges.csv will be written.",
    )
    return parser.parse_args()


def load_bundle(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def collect_present_fields(records: Iterable[dict], preferred_order: list[str]) -> list[str]:
    present = []
    seen = set()
    for field in preferred_order:
        for record in records:
            if field in record:
                present.append(field)
                seen.add(field)
                break
    extra = sorted(
        {
            key
            for record in records
            for key in record.keys()
            if key not in seen and key not in {"metadata", "evidence"}
        }
    )
    return present + extra


def normalize_scalar(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def export_nodes(nodes: list[dict], output_path: Path) -> int:
    fields = collect_present_fields(nodes, NODE_FIELD_ORDER)
    headers = fields + ["display_label", "metadata_json"]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for node in nodes:
            row = {field: normalize_scalar(node.get(field)) for field in fields}
            row["display_label"] = normalize_scalar(node.get("name") or node.get("id"))
            row["metadata_json"] = normalize_scalar(node.get("metadata") or {})
            writer.writerow(row)
    return len(nodes)


def export_edges(edges: list[dict], output_path: Path) -> int:
    fields = collect_present_fields(edges, EDGE_FIELD_ORDER)
    headers = fields + ["evidence_text", "evidence_json", "metadata_json"]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for edge in edges:
            evidence = edge.get("evidence") or []
            if isinstance(evidence, list):
                evidence_text = " | ".join(str(item) for item in evidence if item)
            else:
                evidence_text = str(evidence)
            row = {field: normalize_scalar(edge.get(field)) for field in fields}
            row["evidence_text"] = evidence_text
            row["evidence_json"] = normalize_scalar(evidence)
            row["metadata_json"] = normalize_scalar(edge.get("metadata") or {})
            writer.writerow(row)
    return len(edges)


def main() -> int:
    args = parse_args()
    bundle_path = Path(args.bundle).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    bundle = load_bundle(bundle_path)
    nodes = bundle["nodes"]
    edges = bundle["edges"]

    node_count = export_nodes(nodes, output_dir / "nodes.csv")
    edge_count = export_edges(edges, output_dir / "edges.csv")

    manifest = {
        "bundle_path": str(bundle_path),
        "graph_id": bundle.get("graph_id", ""),
        "node_count": node_count,
        "edge_count": edge_count,
        "generated_files": ["nodes.csv", "edges.csv"],
    }
    (output_dir / "export_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "status": "ok",
                "output_dir": str(output_dir),
                "node_count": node_count,
                "edge_count": edge_count,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
