from __future__ import annotations

import asyncio
from dataclasses import dataclass
from collections import Counter
from typing import Callable, Iterable

from .flk_api import FlkApi
from .logging_utils import RunLogger
from .models import CATEGORY_ID_MAP, CrawlStats, CrawlerConfig, LawMetadata
from .storage import CrawlerStorage, build_doc_filename


ProgressCallback = Callable[[str, int, int], None]
StageCallback = Callable[[str], None]
StageSummaryCallback = Callable[[str, str, int, int, int], None]


@dataclass(slots=True)
class PipelineResult:
    stats: CrawlStats
    log_paths: dict[str, str]


@dataclass(slots=True)
class CategoryMetadataPlan:
    items: list[LawMetadata]
    skipped_existing: int = 0
    skipped_duplicate: int = 0


class CrawlerPipeline:
    def __init__(
        self,
        *,
        api: FlkApi,
        storage: CrawlerStorage,
        config: CrawlerConfig,
        logger: RunLogger,
        stage_callback: StageCallback | None = None,
        progress_callback: ProgressCallback | None = None,
        stage_summary_callback: StageSummaryCallback | None = None,
    ) -> None:
        self.api = api
        self.storage = storage
        self.config = config
        self.logger = logger
        self.stage_callback = stage_callback
        self.progress_callback = progress_callback
        self.stage_summary_callback = stage_summary_callback
        self._metadata_lock = asyncio.Lock()
        self._document_lock = asyncio.Lock()
        self._metadata_since_flush = 0
        self._document_since_flush = 0
        self._metadata_dirty = False
        self._document_manifest_dirty = False

    async def crawl_categories(self, categories: Iterable[str]) -> PipelineResult:
        metadata_result = await self.crawl_metadata(categories)
        docs_result = await self.crawl_docs(categories)
        combined = CrawlStats(
            fetched_metadata=metadata_result.stats.fetched_metadata,
            downloaded_docs=docs_result.stats.downloaded_docs,
            skipped_metadata=metadata_result.stats.skipped_metadata,
            skipped_docs=docs_result.stats.skipped_docs,
            failed=metadata_result.stats.failed + docs_result.stats.failed,
        )
        return PipelineResult(stats=combined, log_paths=docs_result.log_paths)

    async def crawl_metadata(self, categories: Iterable[str]) -> PipelineResult:
        stats = CrawlStats()
        self.storage.ensure_directories()
        metadata_index = self.storage.load_metadata_index()

        for category in categories:
            plan = await self._collect_category_items(category, metadata_index)
            stats.skipped_metadata += plan.skipped_existing + plan.skipped_duplicate
            self._announce_stage(f"元数据抓取: {category}")
            self._metadata_since_flush = 0
            self._metadata_dirty = False
            fetched_before = stats.fetched_metadata
            failed_before = stats.failed
            await self._fetch_metadata_for_items(category, plan.items, metadata_index, stats)
            self.storage.write_metadata_index(metadata_index)
            self._summarize_stage(
                category,
                "metadata",
                succeeded=stats.fetched_metadata - fetched_before,
                skipped=plan.skipped_existing + plan.skipped_duplicate,
                failed=stats.failed - failed_before,
            )
            self.logger.checkpoint(stats)

        return self._finish(stats)

    async def crawl_docs(self, categories: Iterable[str]) -> PipelineResult:
        stats = CrawlStats()
        self.storage.ensure_directories()
        metadata_index, dropped_metadata = self.storage.load_metadata_index_with_drops()
        allowed = set(categories)
        all_entries = [
            metadata
            for metadata in sorted(metadata_index.values(), key=lambda item: item.source_id)
            if metadata.category in allowed
        ]
        duplicate_filenames = self._collect_duplicate_doc_filenames(metadata_index.values())
        document_manifest = self.storage.load_document_manifest()
        grouped: dict[str, list[LawMetadata]] = {category: [] for category in allowed}
        skipped_by_dedup = Counter(
            metadata.category
            for metadata in dropped_metadata
            if metadata.category in allowed and self._matches_document_status(metadata)
        )
        for category in categories:
            candidates = [
                metadata
                for metadata in all_entries
                if metadata.category == category and self._matches_document_status(metadata)
            ]
            stats.skipped_docs += skipped_by_dedup.get(category, 0)
            if self.config.limit is not None:
                candidates = candidates[: self.config.limit]
            grouped[category] = candidates

        for category in categories:
            metadata_items = grouped.get(category, [])
            self._announce_stage(f"文档下载: {category}")
            self._document_since_flush = 0
            self._document_manifest_dirty = False
            downloaded_before = stats.downloaded_docs
            skipped_before = stats.skipped_docs
            failed_before = stats.failed
            await self._download_docs_for_metadata(
                category,
                metadata_items,
                stats,
                duplicate_filenames,
                document_manifest,
                metadata_index,
            )
            self.storage.write_document_manifest(document_manifest, metadata_index)
            self._summarize_stage(
                category,
                "document",
                succeeded=stats.downloaded_docs - downloaded_before,
                skipped=stats.skipped_docs - skipped_before,
                failed=stats.failed - failed_before,
            )
            self.logger.checkpoint(stats)
        return self._finish(stats)

    async def _collect_category_items(
        self,
        category: str,
        metadata_index: dict[str, LawMetadata],
    ) -> CategoryMetadataPlan:
        page_num = 1
        total_hint: int | None = None
        collected: list[LawMetadata] = []
        seen: set[str] = set()
        skipped_existing = 0

        while True:
            page = await self.api.fetch_page(category, page_num, self.config.page_size)
            if total_hint is None:
                total_hint = page.total
            if not page.items:
                break
            for item in page.items:
                if item.source_id in seen:
                    continue
                seen.add(item.source_id)
                if not self.storage.should_fetch_metadata(
                    item.source_id,
                    metadata_index,
                    overwrite=self.config.overwrite_metadata,
                ):
                    skipped_existing += 1
                    continue
                collected.append(self.api.metadata_from_list_item(item))
                if self.config.limit is not None and len(collected) >= self.config.limit:
                    self._progress(f"list:{category}", len(collected), self.config.limit)
                    deduplicated, dropped = self.storage.deduplicate_metadata_index(
                        {metadata.source_id: metadata for metadata in collected}
                    )
                    return CategoryMetadataPlan(
                        items=list(deduplicated.values()),
                        skipped_existing=skipped_existing,
                        skipped_duplicate=dropped,
                    )
            self._progress(f"list:{category}", len(collected), self.config.limit or total_hint or len(collected))
            if len(page.items) < self.config.page_size:
                break
            if total_hint is not None and len(seen) >= total_hint:
                break
            page_num += 1
        deduplicated, dropped = self.storage.deduplicate_metadata_index(
            {metadata.source_id: metadata for metadata in collected}
        )
        return CategoryMetadataPlan(
            items=list(deduplicated.values()),
            skipped_existing=skipped_existing,
            skipped_duplicate=dropped,
        )

    async def _fetch_metadata_for_items(
        self,
        category: str,
        items: list[LawMetadata],
        metadata_index: dict[str, LawMetadata],
        stats: CrawlStats,
    ) -> list[LawMetadata]:
        semaphore = asyncio.Semaphore(self.config.concurrency)
        total = len(items)
        resolved: list[LawMetadata | None] = [None] * total

        async def worker(position: int, list_metadata: LawMetadata) -> None:
            async with semaphore:
                source_id = list_metadata.source_id
                metadata_needed = self.storage.should_fetch_metadata(
                    source_id,
                    metadata_index,
                    overwrite=self.config.overwrite_metadata,
                )
                metadata = metadata_index.get(source_id)
                if metadata_needed:
                    try:
                        metadata = list_metadata
                        async with self._metadata_lock:
                            metadata_index[source_id] = metadata
                            stats.fetched_metadata += 1
                            self._metadata_dirty = True
                            self._checkpoint_metadata_progress(metadata_index, stats)
                    except Exception as exc:
                        stats.failed += 1
                        self.logger.log_failure(source_id, category, "metadata", str(exc))
                        self.logger.checkpoint(stats)
                        self._progress(f"metadata:{category}", position, total)
                        return
                else:
                    stats.skipped_metadata += 1
                    async with self._metadata_lock:
                        self._checkpoint_metadata_progress(metadata_index, stats)

                resolved[position - 1] = metadata
                self._progress(f"metadata:{category}", position, total)

        await asyncio.gather(
            *(worker(index, metadata) for index, metadata in enumerate(items, start=1))
        )
        return [metadata for metadata in resolved if metadata is not None]

    def _checkpoint_metadata_progress(
        self,
        metadata_index: dict[str, LawMetadata],
        stats: CrawlStats,
    ) -> None:
        self._metadata_since_flush += 1
        if self._metadata_since_flush >= self.config.checkpoint_every:
            if self._metadata_dirty:
                self.storage.write_metadata_index(metadata_index)
                self._metadata_dirty = False
            self.logger.checkpoint(stats)
            self._metadata_since_flush = 0

    async def _download_docs_for_metadata(
        self,
        category: str,
        metadata_items: list[LawMetadata],
        stats: CrawlStats,
        duplicate_filenames: set[str],
        document_manifest: set[str],
        metadata_index: dict[str, LawMetadata],
    ) -> None:
        semaphore = asyncio.Semaphore(self.config.concurrency)
        total = len(metadata_items)

        async def worker(position: int, metadata: LawMetadata) -> None:
            async with semaphore:
                if not self.config.overwrite_docs and self.storage.manifest_has_doc(metadata, document_manifest):
                    stats.skipped_docs += 1
                    await self._checkpoint_document_progress(
                        stats,
                        document_manifest,
                        metadata_index,
                        manifest_changed=False,
                    )
                    self._progress(f"docs:{category}", position, total)
                    return
                try:
                    content = await self.api.download_docx(metadata.source_id, metadata.source_url)
                    self.storage.save_doc(metadata, content, duplicate_filenames)
                    stats.downloaded_docs += 1
                    document_manifest.add(metadata.source_id)
                    await self._checkpoint_document_progress(
                        stats,
                        document_manifest,
                        metadata_index,
                        manifest_changed=True,
                    )
                except Exception as exc:
                    stats.failed += 1
                    self.logger.log_failure(metadata.source_id, metadata.category, "download", str(exc))
                    self.logger.checkpoint(stats)
                    await self._checkpoint_document_progress(
                        stats,
                        document_manifest,
                        metadata_index,
                        manifest_changed=False,
                    )
                finally:
                    self._progress(f"docs:{category}", position, total)

        await asyncio.gather(
            *(worker(index, metadata) for index, metadata in enumerate(metadata_items, start=1))
        )

    async def _checkpoint_document_progress(
        self,
        stats: CrawlStats,
        document_manifest: set[str],
        metadata_index: dict[str, LawMetadata],
        *,
        manifest_changed: bool,
    ) -> None:
        async with self._document_lock:
            self._document_manifest_dirty = self._document_manifest_dirty or manifest_changed
            self._document_since_flush += 1
            if self._document_since_flush >= self.config.checkpoint_every:
                if self._document_manifest_dirty:
                    self.storage.write_document_manifest(document_manifest, metadata_index)
                    self._document_manifest_dirty = False
                self.logger.checkpoint(stats)
                self._document_since_flush = 0

    def _finish(self, stats: CrawlStats) -> PipelineResult:
        paths = self.logger.flush(stats)
        return PipelineResult(
            stats=stats,
            log_paths={key: str(path) for key, path in paths.items()},
        )

    def _announce_stage(self, stage: str) -> None:
        if self.stage_callback is not None:
            self.stage_callback(stage)

    def _progress(self, stage: str, current: int, total: int) -> None:
        if self.progress_callback is not None and total > 0:
            self.progress_callback(stage, current, total)

    def _summarize_stage(self, category: str, stage: str, *, succeeded: int, skipped: int, failed: int) -> None:
        if self.stage_summary_callback is not None:
            self.stage_summary_callback(category, stage, succeeded, skipped, failed)

    def _collect_duplicate_doc_filenames(self, metadata_items: Iterable[LawMetadata]) -> set[str]:
        counter = Counter(build_doc_filename(metadata.title or metadata.source_id) for metadata in metadata_items)
        return {name for name, count in counter.items() if count > 1}

    def _matches_document_status(self, metadata: LawMetadata) -> bool:
        status = metadata.status or ""
        if self.config.status and status not in self.config.status:
            return False
        if self.config.status_except and status in self.config.status_except:
            return False
        return True


def normalize_categories(category_args: Iterable[str] | str | None, *, exclude: Iterable[str] | None = None) -> list[str]:
    if category_args is None:
        selected = list(CATEGORY_ID_MAP)
    elif isinstance(category_args, str):
        selected = list(CATEGORY_ID_MAP) if category_args == "all" else [category_args]
    else:
        values = list(category_args)
        selected = list(CATEGORY_ID_MAP) if not values or "all" in values else values
    excluded = set(exclude or [])
    unsupported = [category for category in selected if category not in CATEGORY_ID_MAP]
    unsupported.extend(category for category in excluded if category not in CATEGORY_ID_MAP)
    if unsupported:
        raise ValueError(f"Unsupported category: {', '.join(sorted(set(unsupported)))}")
    categories = [category for category in selected if category not in excluded]
    if not categories:
        raise ValueError("No categories selected.")
    return categories
