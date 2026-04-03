from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .pipeline import (
    PROCESSING_STAGE_SEQUENCE,
    build_batch_knowledge_graph,
    build_knowledge_graph,
    discover_source_files,
)


def add_stage_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--start",
        "--start-stage",
        dest="start_stage",
        choices=PROCESSING_STAGE_SEQUENCE,
        help=(
            "Optional explicit stage to start from. "
            "If omitted, the pipeline resumes from the latest reusable artifact for the current source."
        ),
    )
    parser.add_argument(
        "--end",
        "--through-stage",
        dest="through_stage",
        choices=PROCESSING_STAGE_SEQUENCE,
        default="link",
        help="The last processing stage to execute.",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Force a rebuild from ingest and do not reuse previous cached artifacts.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build structure-first legal knowledge graph artifacts."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build", help="Build artifacts for a single source file.")
    build_parser.add_argument(
        "--data-root",
        required=True,
        help="Path to the project data directory. Source files are resolved under <data-root>/raw.",
    )
    build_parser.add_argument(
        "--source",
        required=True,
        help="Source file path relative to <data-root>/raw, or an absolute path.",
    )
    add_stage_arguments(build_parser)

    batch_parser = subparsers.add_parser(
        "build-batch",
        help="Build artifacts for matching source files under <data-root>/raw.",
    )
    batch_parser.add_argument(
        "--data-root",
        required=True,
        help="Path to the project data directory. Source files are discovered under <data-root>/raw.",
    )
    batch_parser.add_argument(
        "--category",
        help="Optional subdirectory under <data-root>/raw to limit discovery, such as 'law' or 'interpretation'.",
    )
    batch_parser.add_argument(
        "--glob",
        default="*.docx",
        help="Glob pattern used to discover source files recursively. Defaults to '*.docx'.",
    )
    add_stage_arguments(batch_parser)

    return parser


def render_progress(current: int, total: int, *, prefix: str) -> None:
    total = max(total, 1)
    current = min(max(current, 0), total)
    width = 28
    ratio = current / total
    filled = int(width * ratio)
    bar = "#" * filled + "-" * (width - filled)
    _ = prefix
    print(f"\r[{bar}]", end="", file=sys.stderr, flush=True)
    if current >= total:
        print(file=sys.stderr, flush=True)


class StageBarDisplay:
    def __init__(self) -> None:
        self.current_stage: str | None = None
        self.finalizing = False

    def start_stage(self, stage_name: str) -> None:
        if self.current_stage == stage_name:
            return
        self.current_stage = stage_name
        self.finalizing = False
        print(stage_name, file=sys.stderr, flush=True)

    def update(self, current: int, total: int) -> None:
        self.finalizing = False
        render_progress(current, total, prefix="")

    def finalizing_hint(self, message: str) -> None:
        if self.finalizing:
            return
        self.finalizing = True
        print(message, file=sys.stderr, flush=True)


def resolve_source_path(data_root: Path, source_arg: str) -> Path:
    candidate = Path(source_arg)
    if candidate.is_absolute():
        return candidate
    return data_root / "raw" / candidate


def print_single_summary(result: dict[str, object]) -> None:
    print(f"status: {result['status']}")
    print(f"start: {result['start_stage']}")
    print(f"end: {result['through_stage']}")


def print_batch_summary(result: dict[str, object]) -> None:
    print(f"status: {result['status']}")
    print(f"start: {result['start_stage']}")
    print(f"end: {result['through_stage']}")
    print(f"completed: {result['completed_count']}")
    print(f"failed: {result['failed_count']}")
    print(f"log: {result['report_path']}")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    data_root = Path(args.data_root)

    if args.command == "build":
        source_path = resolve_source_path(data_root, args.source)
        stage_total = PROCESSING_STAGE_SEQUENCE.index(args.through_stage) + 1
        display = StageBarDisplay()
        try:
            result = build_knowledge_graph(
                source_path=source_path,
                data_root=data_root,
                start_stage=args.start_stage,
                through_stage=args.through_stage,
                force_rebuild=args.rebuild,
                report_progress=False,
                stage_callback=lambda current, total: display.update(current, total),
                stage_name_callback=display.start_stage,
                finalizing_callback=display.finalizing_hint,
            )
        except Exception as exc:
            display.update(stage_total, stage_total)
            print(f"error: {source_path} ({exc.__class__.__name__}): {exc}", file=sys.stderr)
            return 1
        display.update(stage_total, stage_total)
        print_single_summary(result)
        return 0

    if args.command == "build-batch":
        sources = discover_source_files(data_root, pattern=args.glob, category=args.category)
        display = StageBarDisplay()

        result = build_batch_knowledge_graph(
            data_root=data_root,
            pattern=args.glob,
            category=args.category,
            start_stage=args.start_stage,
            through_stage=args.through_stage,
            force_rebuild=args.rebuild,
            report_progress=False,
            progress_callback=lambda stage_name, current, total: (
                display.start_stage(stage_name),
                display.update(current, total),
            ),
            finalizing_callback=display.finalizing_hint,
        )
        print_batch_summary(result)
        return 0 if result["failed_count"] == 0 else 1

    parser.error(f"Unsupported command: {args.command}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
