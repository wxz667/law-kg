from __future__ import annotations

import argparse
import signal
import sys
import threading
from pathlib import Path

from .io import read_stage_edges, read_stage_nodes, write_jsonl
from .pipeline.orchestrator import (
    STAGE_SEQUENCE,
    build_batch_knowledge_graph,
    build_knowledge_graph,
    resolve_source_id,
)
from .pipeline.progress import StageBarDisplay


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
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--rebuild",
        action="store_true",
        help="Rebuild selected stages for the matched sources after confirmation.",
    )
    mode_group.add_argument(
        "--incremental",
        action="store_true",
        help="Explicitly use the default incremental merge-and-skip behavior.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build legal knowledge graph bundle artifacts.")
    parser.add_argument(
        "command",
        nargs="?",
        default="build",
        choices=("build",),
        help="Builder command. The default and only supported command is 'build'.",
    )
    parser.add_argument(
        "--data-root",
        default="data",
        help="Path to the project data directory. Builder expects metadata under <data-root>/source/metadata.",
    )
    scope_group = parser.add_mutually_exclusive_group(required=True)
    scope_group.add_argument(
        "--source-id",
        dest="source_ids",
        nargs="+",
        help="One or more metadata source_id values to build.",
    )
    scope_group.add_argument(
        "--category",
        nargs="+",
        help="Build all metadata sources whose category matches any of these values.",
    )
    scope_group.add_argument(
        "--all",
        action="store_true",
        help="Build all discovered metadata sources.",
    )
    add_stage_arguments(parser)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    data_root = Path(args.data_root)

    if args.command != "build":
        parser.error(f"Unsupported command: {args.command}")
        return 1

    if args.rebuild and not confirm_rebuild(args):
        print("cancelled", file=sys.stderr)
        return 1

    display = StageBarDisplay()
    cancel_event = threading.Event()
    previous_handler = install_interrupt_handler(cancel_event)
    try:
        result = run_build_command(args, data_root, display, cancel_event)
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"error: {exc.__class__.__name__}: {exc}", file=sys.stderr)
        return 1
    finally:
        signal.signal(signal.SIGINT, previous_handler)
        display.close()
    display.print_final_summary(result)
    return 0 if result.get("status") == "completed" else 1


def run_build_command(
    args: argparse.Namespace,
    data_root: Path,
    display: StageBarDisplay,
    cancel_event: threading.Event,
) -> dict[str, object]:
    if args.source_ids:
        resolved_source_ids = [resolve_source_id(value, data_root) for value in args.source_ids]
        resolved_source_ids = list(dict.fromkeys(resolved_source_ids))
        display.announce_discovery(len(resolved_source_ids))
        return build_knowledge_graph(
            source_id=resolved_source_ids,
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

    def handle_progress(stage_name: str, current: int, total: int) -> None:
        display.handle(stage_name, current, total)

    category = list(args.category) if args.category else None
    return build_batch_knowledge_graph(
        data_root=data_root,
        category=category,
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


def confirm_rebuild(args: argparse.Namespace) -> bool:
    target = describe_build_scope(args)
    start_stage = args.start_stage or STAGE_SEQUENCE[0]
    through_stage = args.through_stage
    prompt = (
        "Rebuild requested.\n"
        f"This will delete existing builder artifacts for the selected target within stages {start_stage} -> {through_stage}.\n"
        f"Target: {target}\n"
        "Continue? Type 'yes' to proceed [yes/No]: "
    )
    try:
        answer = input(prompt)
    except EOFError:
        return False
    return answer.strip() == "yes"


def describe_build_scope(args: argparse.Namespace) -> str:
    if args.source_ids:
        return f"source-id(s): {', '.join(str(value) for value in args.source_ids)}"
    if args.category:
        return f"category: {', '.join(str(value) for value in args.category)}"
    return "all sources"


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
