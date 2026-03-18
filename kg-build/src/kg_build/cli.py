from __future__ import annotations

import argparse
import json
from pathlib import Path

from .pipeline import STAGE_SEQUENCE, build_knowledge_graph


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a Criminal Law Tree-KG artifact set.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build", help="Run the complete graph build pipeline.")
    build_parser.add_argument("--source", required=True, help="Path to the source .docx file.")
    build_parser.add_argument("--data-root", required=True, help="Path to the data directory.")
    build_parser.add_argument(
        "--start-stage",
        choices=STAGE_SEQUENCE,
        help=(
            "Optional explicit stage to start from. "
            "If omitted, the pipeline resumes from the latest available implemented artifact."
        ),
    )
    build_parser.add_argument(
        "--end-stage",
        choices=STAGE_SEQUENCE,
        default="serialize",
        help="Stage to stop at.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "build":
        result = build_knowledge_graph(
            source_path=Path(args.source),
            data_root=Path(args.data_root),
            start_stage=args.start_stage,
            end_stage=args.end_stage,
            report_progress=True,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
