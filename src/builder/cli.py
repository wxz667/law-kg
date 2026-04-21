from __future__ import annotations

import argparse
import signal
import sys
import threading
from pathlib import Path

from .io import BuildLayout, write_stage_edges, write_stage_nodes
from .pipeline.orchestrator import (
    BuilderConfig,
    STAGE_SEQUENCE,
    build_knowledge_graph,
    load_builder_config,
    resolve_source_id,
)
from .pipeline.handlers.common import load_graph_snapshot
from .pipeline.progress import StageBarDisplay

GRAPH_EXPORT_STAGES = {"structure", "classify", "align", "infer"}
EXPORT_STAGE_GRAPH_VIEW = {
    "structure": "structure",
    "detect": "structure",
    "classify": "classify",
    "extract": "classify",
    "aggregate": "classify",
    "align": "align",
    "infer": "infer",
}
DEFAULT_EXPORT_STAGE_ORDER = ("infer", "align", "classify", "structure")


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
        help="Override builder.data from configs/config.json.",
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


def build_export_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export the latest available graph from builder stage artifacts.")
    parser.add_argument(
        "command",
        choices=("export",),
        help="Export stage graph artifacts without running the builder pipeline.",
    )
    parser.add_argument(
        "--data-root",
        help="Override builder.data from configs/config.json.",
    )
    parser.add_argument(
        "--stage",
        choices=STAGE_SEQUENCE,
        help="Stage view to export. Defaults to the latest available graph stage.",
    )
    parser.add_argument(
        "--target",
        default="data/exports",
        help="Directory where nodes.jsonl and edges.jsonl will be written.",
    )
    return parser


def main() -> int:
    raw_args = sys.argv[1:]
    if raw_args and raw_args[0] == "export":
        parser = build_export_parser()
        args = parser.parse_args(raw_args)
        try:
            result = run_export_command(args)
        except Exception as exc:
            print(f"error: {exc.__class__.__name__}: {exc}", file=sys.stderr)
            return 1
        print(
            "exported "
            f"{result['stage']} graph to {result['target']} "
            f"({result['node_count']} nodes, {result['edge_count']} edges)"
        )
        return 0

    parser = build_parser()
    args = parser.parse_args(raw_args)
    builder_config = load_builder_config(data_override=Path(args.data_root) if args.data_root else None)

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
        result = run_build_command(args, builder_config, display, cancel_event)
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
    builder_config: BuilderConfig,
    display: StageBarDisplay,
    cancel_event: threading.Event,
) -> dict[str, object]:
    if args.source_ids:
        resolved_source_ids = [resolve_source_id(value, builder_config.metadata) for value in args.source_ids]
        resolved_source_ids = list(dict.fromkeys(resolved_source_ids))
        display.announce_discovery(len(resolved_source_ids))
        return build_knowledge_graph(
            source_id=resolved_source_ids,
            builder_config=builder_config,
            start_stage=args.start_stage,
            through_stage=args.through_stage,
            force_rebuild=args.rebuild,
            incremental=args.incremental,
            report_progress=False,
            stage_progress_callback=display.handle,
            stage_summary_callback=display.stage_summary,
            finalizing_callback=display.finalizing_hint,
            stage_error_callback=display.stage_error,
            cancel_event=cancel_event,
        )

    category = list(args.category) if args.category else None
    return build_knowledge_graph(
        builder_config=builder_config,
        category=category,
        all_sources=bool(args.all),
        start_stage=args.start_stage,
        through_stage=args.through_stage,
        force_rebuild=args.rebuild,
        incremental=args.incremental,
        report_progress=False,
        discovery_callback=display.announce_discovery,
        stage_progress_callback=display.handle,
        stage_summary_callback=display.stage_summary,
        finalizing_callback=display.finalizing_hint,
        stage_error_callback=display.stage_error,
        cancel_event=cancel_event,
    )


def run_export_command(args: argparse.Namespace) -> dict[str, object]:
    builder_config = load_builder_config(data_override=Path(args.data_root) if args.data_root else None)
    data_root = builder_config.data
    target = Path(args.target).resolve()
    layout = BuildLayout(data_root)
    stage_name = resolve_export_stage(layout, args.stage)
    graph = load_graph_snapshot(
        layout,
        stage_name,
        stage_sequence=tuple(STAGE_SEQUENCE),
        graph_stages=set(GRAPH_EXPORT_STAGES),
    )
    if not graph.nodes:
        raise FileNotFoundError(f"No graph nodes are available for stage view: {stage_name}")
    if not graph.edges:
        raise FileNotFoundError(f"No graph edges are available for stage view: {stage_name}")
    graph.validate_edge_references()
    write_stage_nodes(target / "nodes.jsonl", graph.nodes)
    write_stage_edges(target / "edges.jsonl", graph.edges)
    return {
        "stage": stage_name,
        "target": str(target),
        "node_count": len(graph.nodes),
        "edge_count": len(graph.edges),
    }


def resolve_export_stage(layout: BuildLayout, requested_stage: str | None) -> str:
    if requested_stage:
        if requested_stage == "normalize":
            raise ValueError("normalize does not produce a graph artifact and cannot be exported.")
        return EXPORT_STAGE_GRAPH_VIEW[requested_stage]
    for stage_name in DEFAULT_EXPORT_STAGE_ORDER:
        if layout.stage_nodes_path(stage_name).exists() or layout.stage_edges_path(stage_name).exists():
            return stage_name
    raise FileNotFoundError(
        "No graph stage artifacts found. Run builder through structure, classify, align, or infer first."
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
