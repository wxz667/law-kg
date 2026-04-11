from __future__ import annotations

import argparse
import signal
import sys
import threading
from pathlib import Path

from .pipeline.orchestrator import (
    STAGE_SEQUENCE,
    build_batch_knowledge_graph,
    build_knowledge_graph,
    resolve_source_id,
)
from .pipeline.progress import StageBarDisplay
from .io import read_stage_edges, read_stage_nodes, write_jsonl


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
        help="Rebuild selected stages for the matched sources instead of reusing existing artifacts.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Deprecated alias of the default merge-and-skip behavior.",
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
        nargs="+",
        help="Optional metadata category filters, such as '法律' '司法解释'.",
    )
    batch_parser.add_argument(
        "--glob",
        default="*.docx",
        help="Deprecated compatibility flag; metadata-driven discovery ignores this value.",
    )
    add_stage_arguments(batch_parser)

    split_parser = subparsers.add_parser(
        "split-export",
        help="Split final stage JSONL artifacts into Neo4j and Elasticsearch import files.",
    )
    split_parser.add_argument("--graph", required=True, help="Path to a final graph export directory or stage directory.")
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
        cancel_event = threading.Event()
        previous_handler = install_interrupt_handler(cancel_event)
        try:
            result = build_knowledge_graph(
                source_id=source_id,
                data_root=data_root,
                start_stage=args.start_stage,
                through_stage=args.through_stage,
                force_rebuild=args.rebuild,
                incremental=args.incremental,
                report_progress=False,
                stage_progress_callback=display.handle,
                stage_summary_callback=display.stage_summary,
                finalizing_callback=display.finalizing_hint,
                cancel_event=cancel_event,
            )
        except KeyboardInterrupt:
            print("interrupted", file=sys.stderr)
            return 130
        except Exception as exc:
            print(f"error: {source_id} ({exc.__class__.__name__}): {exc}", file=sys.stderr)
            return 1
        finally:
            signal.signal(signal.SIGINT, previous_handler)
            display.close()
        display.print_final_summary(result)
        return 0

    if args.command == "build-batch":
        data_root = Path(args.data_root)
        display = StageBarDisplay()
        cancel_event = threading.Event()
        previous_handler = install_interrupt_handler(cancel_event)

        def handle_progress(stage_name: str, current: int, total: int) -> None:
            display.handle(stage_name, current, total)

        try:
            result = build_batch_knowledge_graph(
                data_root=data_root,
                pattern=args.glob,
                category=args.category,
                start_stage=args.start_stage,
                through_stage=args.through_stage,
                force_rebuild=args.rebuild,
                incremental=args.incremental,
                report_progress=False,
                discovery_callback=display.announce_discovery,
                progress_callback=handle_progress,
                stage_summary_callback=display.stage_summary,
                finalizing_callback=display.finalizing_hint,
                cancel_event=cancel_event,
            )
        except KeyboardInterrupt:
            print("interrupted", file=sys.stderr)
            return 130
        finally:
            signal.signal(signal.SIGINT, previous_handler)
            display.close()
        display.print_final_summary(result)
        return 0 if result["status"] == "completed" else 1

    if args.command == "split-export":
        split_graph_export(Path(args.graph), Path(args.output_root))
        print("status: completed")
        print(f"graph: {args.graph}")
        print(f"output: {args.output_root}")
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 1


def split_graph_export(graph_path: Path, output_root: Path) -> None:
    graph_path = graph_path.resolve()
    if graph_path.is_dir():
        nodes_path = graph_path / "nodes.jsonl"
        edges_path = graph_path / "edges.jsonl"
    else:
        nodes_path = graph_path.parent / "nodes.jsonl"
        edges_path = graph_path.parent / "edges.jsonl"
    nodes = read_stage_nodes(nodes_path)
    edges = read_stage_edges(edges_path)
    neo4j_nodes = [node.to_dict() for node in nodes]
    neo4j_edges = [
        {
            "id": edge.id,
            "source": edge.source,
            "target": edge.target,
            "type": edge.type,
            "weight": edge.weight,
            "predicted": edge.predicted,
            "canonical": edge.canonical,
            "model": edge.model,
            "label": edge.label,
            "concept_id": edge.concept_id,
            "start_offset": edge.start_offset,
            "end_offset": edge.end_offset,
        }
        for edge in edges
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
            "alignment_status": node.alignment_status,
            "normalized_text": node.normalized_text,
        }
        for node in nodes
        if node.text or node.level == "document"
    ]
    write_jsonl(output_root / "neo4j" / "nodes.jsonl", neo4j_nodes)
    write_jsonl(output_root / "neo4j" / "edges.jsonl", neo4j_edges)
    write_jsonl(output_root / "elasticsearch" / "documents.jsonl", search_documents)


def install_interrupt_handler(cancel_event: threading.Event) -> signal.Handlers:
    previous_handler = signal.getsignal(signal.SIGINT)
    interrupt_state = {"count": 0}

    def handle_interrupt(signum: int, frame: object) -> None:
        del frame
        interrupt_state["count"] += 1
        cancel_event.set()
        if interrupt_state["count"] >= 2:
            if callable(previous_handler):
                previous_handler(signum, None)
                return
            raise KeyboardInterrupt

    signal.signal(signal.SIGINT, handle_interrupt)
    return previous_handler


if __name__ == "__main__":
    raise SystemExit(main())
