from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

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
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "config.json"


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
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing metadata and documents for selected stages.")
    parser.add_argument("--metadata", action="store_true", help="Run metadata stage.")
    parser.add_argument("--document", action="store_true", help="Run document stage.")
    parser.add_argument("--docs", dest="document", action="store_true", help=argparse.SUPPRESS)
    return parser


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to the project config file.")
    parser.add_argument("--data-root", help="Override crawler.data_root from configs/config.json.")
    parser.add_argument("--base-url", help="Override crawler.base_url from configs/config.json.")
    parser.add_argument("--metadata-dir", help="Override crawler.metadata_dir from configs/config.json.")
    parser.add_argument("--document-dir", help="Override crawler.document_dir from configs/config.json.")
    parser.add_argument("--concurrency", type=int, help="Max concurrent item workers.")
    parser.add_argument("--retries", type=int, help="Max retries per network request.")
    parser.add_argument("--timeout", type=float, help="Request timeout in seconds.")
    parser.add_argument("--request-delay", type=float, help="Minimum delay between FLK requests in seconds.")
    parser.add_argument("--request-jitter", type=float, help="Additional random delay between FLK requests in seconds.")
    parser.add_argument("--warmup-timeout", type=float, help="Warm-up page request timeout in seconds.")
    parser.add_argument("--bootstrap-api-probe", action="store_true", help="Probe aggregateData during client bootstrap.")
    parser.add_argument("--metadata-shard-size", type=int, help="Records per metadata shard.")
    parser.add_argument("--page-size", type=int, help="Category list page size.")
    parser.add_argument("--batch", type=int, help="Flush metadata index to disk every N records.")
    parser.add_argument("--limit", type=int, help="Optional max number of records to process per category.")


def load_crawler_config(config_path: Path) -> CrawlerConfig:
    path = config_path.resolve()
    payload: dict[str, Any] = {}
    if path.exists():
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            crawler = loaded.get("crawler", {})
            payload = dict(crawler) if isinstance(crawler, dict) else {}
    elif config_path != DEFAULT_CONFIG_PATH:
        raise FileNotFoundError(f"Missing config file: {path}")

    data_root = resolve_config_path(payload.get("data_root", "data"))
    metadata_dir = resolve_config_path(payload["metadata_dir"]) if str(payload.get("metadata_dir", "")).strip() else None
    document_dir = resolve_config_path(payload["document_dir"]) if str(payload.get("document_dir", "")).strip() else None
    return CrawlerConfig(
        data_root=data_root,
        base_url=str(payload.get("base_url", "https://flk.npc.gov.cn") or "https://flk.npc.gov.cn").rstrip("/"),
        metadata_dir=metadata_dir,
        document_dir=document_dir,
        concurrency=max(int(payload.get("concurrency", 2) or 2), 1),
        retries=max(int(payload.get("retries", 3) or 3), 1),
        timeout=max(float(payload.get("timeout", 10.0) or 10.0), 0.1),
        request_delay=max(float(payload.get("request_delay", 0.3) or 0.3), 0.0),
        request_jitter=max(float(payload.get("request_jitter", 0.5) or 0.5), 0.0),
        warmup_timeout=max(float(payload.get("warmup_timeout", 5.0) or 5.0), 0.1),
        bootstrap_api_probe=bool(payload.get("bootstrap_api_probe", False)),
        metadata_shard_size=max(int(payload.get("metadata_shard_size", 1000) or 1000), 1),
        page_size=max(int(payload.get("page_size", 20) or 20), 1),
        checkpoint_every=max(int(payload.get("checkpoint_every", 50) or 50), 1),
    )


def apply_cli_overrides(config: CrawlerConfig, args: argparse.Namespace) -> CrawlerConfig:
    data_root_overridden = bool(args.data_root)
    data_root = resolve_config_path(args.data_root) if data_root_overridden else config.data_root
    metadata_dir = config.metadata_dir
    document_dir = config.document_dir
    if data_root_overridden:
        metadata_dir = data_root / "source" / "metadata"
        document_dir = data_root / "source" / "documents"
    return CrawlerConfig(
        data_root=data_root,
        base_url=str(args.base_url or config.base_url).rstrip("/"),
        metadata_dir=resolve_config_path(args.metadata_dir) if args.metadata_dir else metadata_dir,
        document_dir=resolve_config_path(args.document_dir) if args.document_dir else document_dir,
        overwrite_metadata=args.overwrite,
        overwrite_docs=args.overwrite,
        concurrency=max(args.concurrency if args.concurrency is not None else config.concurrency, 1),
        retries=max(args.retries if args.retries is not None else config.retries, 1),
        timeout=max(args.timeout if args.timeout is not None else config.timeout, 0.1),
        request_delay=max(args.request_delay if args.request_delay is not None else config.request_delay, 0.0),
        request_jitter=max(args.request_jitter if args.request_jitter is not None else config.request_jitter, 0.0),
        warmup_timeout=max(args.warmup_timeout if args.warmup_timeout is not None else config.warmup_timeout, 0.1),
        bootstrap_api_probe=args.bootstrap_api_probe or config.bootstrap_api_probe,
        metadata_shard_size=max(
            args.metadata_shard_size if args.metadata_shard_size is not None else config.metadata_shard_size,
            1,
        ),
        page_size=max(args.page_size if args.page_size is not None else config.page_size, 1),
        checkpoint_every=max(args.batch if args.batch is not None else config.checkpoint_every, 1),
        limit=args.limit,
    )


def resolve_config_path(value: Any) -> Path:
    path = Path(str(value).strip())
    if not str(path):
        raise ValueError("Crawler path configuration cannot be empty.")
    return path if path.is_absolute() else (DEFAULT_CONFIG_PATH.parents[1] / path).resolve()


async def run_command(args: argparse.Namespace) -> int:
    run_metadata = args.metadata or not args.document
    run_docs = args.document or not args.metadata
    config = apply_cli_overrides(load_crawler_config(Path(args.config)), args)
    storage = CrawlerStorage(
        config.data_root,
        metadata_dir=config.metadata_dir,
        document_dir=config.document_dir,
        metadata_shard_size=config.metadata_shard_size,
    )
    storage.ensure_directories()
    logger = RunLogger(
        storage.logs_dir,
        command="crawl",
        arguments=vars(args),
    )

    async with FlkClient(
        config.base_url,
        timeout=config.timeout,
        retries=config.retries,
        request_delay=config.request_delay,
        request_jitter=config.request_jitter,
        warmup_timeout=config.warmup_timeout,
        bootstrap_api_probe=config.bootstrap_api_probe,
    ) as client:
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
    try:
        return asyncio.run(run_command(args))
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
