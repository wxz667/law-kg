from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .pipeline.orchestrator import (
    STAGE_SEQUENCE,
    build_batch_knowledge_graph,
    build_knowledge_graph,
    resolve_source_id,
)
from .pipeline.progress import StageBarDisplay
from .io import read_graph_bundle, write_jsonl


def add_stage_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--start",
        "--start-stage",
        dest="start_stage",
        choices=STAGE_SEQUENCE,
        help="Optional explicit stage to start from.",
    )
    parser.add_argument(
        "--end",
        "--through-stage",
        dest="through_stage",
        choices=STAGE_SEQUENCE,
        default=STAGE_SEQUENCE[-1],
        help="The last processing stage to execute.",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Rebuild selected stages instead of reusing existing artifacts.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build legal knowledge graph bundle artifacts."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build", help="Build artifacts for a single metadata source_id.")
    build_parser.add_argument(
        "--data-root",
        default="data",
        help="Path to the project data directory. Builder expects metadata under <data-root>/source/metadata.",
    )
    build_parser.add_argument(
        "--source-id",
        dest="source_id",
        required=True,
        help="Metadata source_id to build.",
    )
    add_stage_arguments(build_parser)

    batch_parser = subparsers.add_parser(
        "build-batch",
        help="Build artifacts for metadata-discovered source documents.",
    )
    batch_parser.add_argument(
        "--data-root",
        default="data",
        help="Path to the project data directory. Metadata lists are discovered under <data-root>/source/metadata.",
    )
    batch_parser.add_argument(
        "--category",
        help="Optional metadata category filter, such as '法律' or '司法解释'.",
    )
    batch_parser.add_argument(
        "--glob",
        default="*.docx",
        help="Deprecated compatibility flag; metadata-driven discovery ignores this value.",
    )
    add_stage_arguments(batch_parser)

    split_parser = subparsers.add_parser(
        "split-export",
        help="Split a final graph JSON into Neo4j and Elasticsearch import files.",
    )
    split_parser.add_argument("--graph", required=True, help="Path to a final graph JSON export.")
    split_parser.add_argument(
        "--output-root",
        required=True,
        help="Directory for generated neo4j/es import JSONL files.",
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "build":
        data_root = Path(args.data_root)
        source_id = resolve_source_id(args.source_id, data_root)
        display = StageBarDisplay()
        display.announce_discovery(1)
        try:
            result = build_knowledge_graph(
                source_id=source_id,
                data_root=data_root,
                start_stage=args.start_stage,
                through_stage=args.through_stage,
                force_rebuild=args.rebuild,
                report_progress=False,
                stage_progress_callback=display.handle,
                stage_summary_callback=display.stage_summary,
                finalizing_callback=display.finalizing_hint,
            )
        except Exception as exc:
            print(f"error: {source_id} ({exc.__class__.__name__}): {exc}", file=sys.stderr)
            return 1
        display.print_final_summary(result)
        return 0

    if args.command == "build-batch":
        data_root = Path(args.data_root)
        display = StageBarDisplay()

        def handle_progress(stage_name: str, current: int, total: int) -> None:
            if current == 0:
                display.start_stage(stage_name)
            display.update(current, total)

        result = build_batch_knowledge_graph(
            data_root=data_root,
            pattern=args.glob,
            category=args.category,
            start_stage=args.start_stage,
            through_stage=args.through_stage,
            force_rebuild=args.rebuild,
            report_progress=False,
            discovery_callback=display.announce_discovery,
            progress_callback=handle_progress,
            stage_summary_callback=display.stage_summary,
            finalizing_callback=display.finalizing_hint,
        )
        display.print_final_summary(result)
        return 0 if result["completed_count"] > 0 else 1

    if args.command == "split-export":
        split_graph_export(Path(args.graph), Path(args.output_root))
        print("status: completed")
        print(f"graph: {args.graph}")
        print(f"output: {args.output_root}")
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 1


def split_graph_export(graph_path: Path, output_root: Path) -> None:
    bundle = read_graph_bundle(graph_path)
    neo4j_nodes = [
        {
            "id": node.id,
            "type": node.type,
            "name": node.name,
            "level": node.level,
            "text": node.text,
            "category": node.category,
            "status": node.status,
            "issuer": node.issuer,
            "publish_date": node.publish_date,
            "effective_date": node.effective_date,
            "source_url": node.source_url,
            "metadata": node.metadata,
        }
        for node in bundle.nodes
    ]
    neo4j_edges = [
        {
            "id": edge.id,
            "source": edge.source,
            "target": edge.target,
            "type": edge.type,
            "weight": edge.weight,
            "evidence": edge.evidence,
            "metadata": edge.metadata,
        }
        for edge in bundle.edges
    ]
    search_documents = [
        {
            "id": node.id,
            "type": node.type,
            "level": node.level,
            "name": node.name,
            "text": node.text,
            "category": node.category,
            "status": node.status,
            "issuer": node.issuer,
            "publish_date": node.publish_date,
            "effective_date": node.effective_date,
            "source_url": node.source_url,
            "metadata": node.metadata,
        }
        for node in bundle.nodes
        if node.text or node.level == "document"
    ]
    write_jsonl(output_root / "neo4j" / "nodes.jsonl", neo4j_nodes)
    write_jsonl(output_root / "neo4j" / "edges.jsonl", neo4j_edges)
    write_jsonl(output_root / "elasticsearch" / "documents.jsonl", search_documents)


if __name__ == "__main__":
    raise SystemExit(main())
