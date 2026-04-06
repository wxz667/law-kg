from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

from .client import FlkClient
from .flk_api import FlkApi
from .logging_utils import RunLogger
from .models import CrawlerConfig
from .pipeline import CrawlerPipeline, normalize_categories
from .storage import CrawlerStorage

BAR_WIDTH = 42
FILLED_CHAR = "━"
EMPTY_CHAR = "─"
RESET = "\033[0m"
CYAN = "\033[36m"
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
DIM = "\033[2m"


def colorize(text: str, color: str, *, enabled: bool) -> str:
    if not enabled:
        return text
    return f"{color}{text}{RESET}"


def format_elapsed(seconds: float) -> str:
    total_seconds = max(int(seconds), 0)
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:d}:{seconds:02d}"


class ProgressReporter:
    def __init__(self) -> None:
        self.current_category: str | None = None
        self.current_stage: str | None = None
        self.bar_open = False
        self.last_signature: tuple[str | None, int, int] | None = None
        self.stage_started_at = time.monotonic()
        self.use_color = sys.stderr.isatty()
        self.stage_counts: dict[str, tuple[int, int]] = {}
        self._ticker_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._ticker_task is None:
            self._ticker_task = asyncio.create_task(self._ticker())

    async def stop(self) -> None:
        if self._ticker_task is None:
            return
        self._ticker_task.cancel()
        try:
            await self._ticker_task
        except asyncio.CancelledError:
            pass
        self._ticker_task = None

    def stage(self, name: str) -> None:
        stage_name, _, category = name.partition(": ")
        category = category.strip()
        if category and category != self.current_category:
            self._close_bar()
            self.current_category = category
            print(
                f"{colorize('category', CYAN, enabled=self.use_color)}: {category}",
                file=sys.stderr,
                flush=True,
            )
        if stage_name in {"元数据抓取", "文档下载"}:
            stage_key = "metadata" if stage_name == "元数据抓取" else "document"
            if stage_key != self.current_stage:
                self._close_bar()
                self.current_stage = stage_key
                self.last_signature = None
                self.stage_started_at = time.monotonic()

    def update(self, stage: str, current: int, total: int) -> None:
        label = self._stage_label(stage)
        if label is None:
            return
        total = max(total, 1)
        current = min(max(current, 0), total)
        self.stage_counts[label] = (current, total)
        self._render(label, current, total)

    def summarize_stage(self, category: str, stage: str, succeeded: int, skipped: int, failed: int) -> None:
        del category
        self._close_bar()
        if stage in self.stage_counts:
            del self.stage_counts[stage]
        print(
            f"\n",
            f"{colorize(str(succeeded), GREEN, enabled=self.use_color)} added, "
            f"{colorize(str(skipped), YELLOW, enabled=self.use_color)} skipped, "
            f"{colorize(str(failed), RED, enabled=self.use_color)} failed",
            file=sys.stderr,
            flush=True,
        )

    def final_summary(self, result) -> None:
        self._close_bar()
        separator = colorize("─" * 56, DIM, enabled=self.use_color)
        print(separator, file=sys.stderr, flush=True)
        summary_path = result.log_paths.get("summary")
        if summary_path:
            print(
                f"{colorize('log', CYAN, enabled=self.use_color)}: {summary_path}",
                file=sys.stderr,
                flush=True,
            )

    def _render(self, label: str, current: int, total: int, *, force: bool = False) -> None:
        signature = (label, current, total)
        if signature == self.last_signature and not force:
            return
        self.last_signature = signature
        ratio = current / total
        filled = int(BAR_WIDTH * ratio)
        bar = FILLED_CHAR * filled + EMPTY_CHAR * (BAR_WIDTH - filled)
        elapsed_text = format_elapsed(time.monotonic() - self.stage_started_at)
        print(
            "\r"
            + colorize(f"[{label}]:", CYAN, enabled=self.use_color)
            + " "
            + colorize(bar, GREEN, enabled=self.use_color)
            + " "
            + colorize(f"{current}/{total}", YELLOW, enabled=self.use_color)
            + " "
            + colorize(elapsed_text, YELLOW, enabled=self.use_color),
            end="",
            file=sys.stderr,
            flush=True,
        )
        self.bar_open = current < total
        if current >= total:
            self._close_bar()

    def _stage_label(self, stage: str) -> str | None:
        label, _, _category = stage.partition(":")
        if label == "metadata":
            return "metadata"
        if label == "docs":
            return "document"
        return None

    def _close_bar(self) -> None:
        if self.bar_open:
            print(file=sys.stderr, flush=True)
            self.bar_open = False

    async def _ticker(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            if self.current_stage is None:
                continue
            counts = self.stage_counts.get(self.current_stage)
            if counts is None:
                continue
            self._render(self.current_stage, counts[0], counts[1], force=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Crawl FLK metadata and docx documents.")
    parser.add_argument("--category", default="all", help="One category name or 'all'.")
    add_common_arguments(parser)
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing metadata and docs for selected stages.")
    parser.add_argument("--metadata", action="store_true", help="Run metadata stage.")
    parser.add_argument("--docs", action="store_true", help="Run docs stage.")
    return parser


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--data-root", default="data", help="Path to the project data directory.")
    parser.add_argument("--concurrency", type=int, default=6, help="Max concurrent item workers.")
    parser.add_argument("--retries", type=int, default=4, help="Max retries per network request.")
    parser.add_argument("--timeout", type=float, default=20.0, help="Request timeout in seconds.")
    parser.add_argument("--metadata-shard-size", type=int, default=1000, help="Records per metadata shard.")
    parser.add_argument("--page-size", type=int, default=20, help="Category list page size.")
    parser.add_argument("--batch", type=int, default=50, help="Flush metadata index to disk every N records.")
    parser.add_argument("--limit", type=int, help="Optional max number of records to process per category.")


async def run_command(args: argparse.Namespace) -> int:
    run_metadata = args.metadata or not args.docs
    run_docs = args.docs or not args.metadata
    config = CrawlerConfig(
        data_root=Path(args.data_root),
        overwrite_metadata=args.overwrite,
        overwrite_docs=args.overwrite,
        concurrency=args.concurrency,
        retries=args.retries,
        timeout=args.timeout,
        metadata_shard_size=args.metadata_shard_size,
        page_size=args.page_size,
        checkpoint_every=args.batch,
        limit=args.limit,
    )
    storage = CrawlerStorage(config.data_root, metadata_shard_size=config.metadata_shard_size)
    storage.ensure_directories()
    logger = RunLogger(
        storage.logs_dir,
        command="crawl",
        arguments=vars(args),
    )

    async with FlkClient("https://flk.npc.gov.cn", timeout=config.timeout, retries=config.retries) as client:
        api = FlkApi(client)
        reporter = ProgressReporter()
        await reporter.start()
        try:
            pipeline = CrawlerPipeline(
                api=api,
                storage=storage,
                config=config,
                logger=logger,
                stage_callback=reporter.stage,
                progress_callback=reporter.update,
                stage_summary_callback=reporter.summarize_stage,
            )

            categories = normalize_categories(args.category)
            if run_metadata and run_docs:
                result = await pipeline.crawl_categories(categories)
            elif run_metadata:
                result = await pipeline.crawl_metadata(categories)
            else:
                result = await pipeline.crawl_docs(categories)
        finally:
            await reporter.stop()

    reporter.final_summary(result)
    return 0 if result.stats.failed == 0 else 1


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return asyncio.run(run_command(args))


if __name__ == "__main__":
    raise SystemExit(main())
