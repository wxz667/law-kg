from __future__ import annotations

import asyncio
from pathlib import Path

from crawler.logging_utils import RunLogger
from crawler.models import CrawlStats, CrawlerConfig, LawListItem, LawMetadata, PageResult
from crawler.pipeline import CrawlerPipeline
from crawler.storage import CrawlerStorage


class FakeApi:
    def __init__(self) -> None:
        self.metadata_calls: list[str] = []
        self.download_calls: list[str] = []

    async def fetch_page(self, category: str, page_num: int, page_size: int) -> PageResult:
        items = [
            LawListItem(
                source_id="law-1",
                title="法规一",
                category=category,
                issuer="全国人大常委会",
                publish_date="2024-01-01",
                effective_date="2024-02-01",
                status="现行有效",
            ),
            LawListItem(
                source_id="law-2",
                title="法规二",
                category=category,
                issuer="全国人大常委会",
                publish_date="2024-01-01",
                effective_date="2024-02-01",
                status="现行有效",
            ),
            LawListItem(
                source_id="law-3",
                title="法规三",
                category=category,
                issuer="全国人大常委会",
                publish_date="2024-01-01",
                effective_date="2024-02-01",
                status="现行有效",
            ),
            LawListItem(
                source_id="law-4",
                title="法规四",
                category=category,
                issuer="全国人大常委会",
                publish_date="2024-01-01",
                effective_date="2024-02-01",
                status="现行有效",
            ),
        ]
        start = (page_num - 1) * page_size
        end = start + page_size
        page_items = items[start:end]
        if not page_items:
            return PageResult(items=[], total=len(items))
        return PageResult(items=page_items, total=len(items))

    async def fetch_metadata(self, source_id: str, category: str) -> LawMetadata:
        self.metadata_calls.append(source_id)
        return LawMetadata(
            source_id=source_id,
            title=f"标题-{source_id}",
            issuer="全国人大常委会",
            publish_date="2024-01-01",
            effective_date="2024-02-01",
            category=category,
            status="现行有效",
            source_url=f"https://flk.npc.gov.cn/detail2.html?{source_id}",
        )

    def metadata_from_list_item(self, item: LawListItem) -> LawMetadata:
        return LawMetadata(
            source_id=item.source_id,
            title=f"标题-{item.source_id}",
            issuer=item.issuer,
            publish_date=item.publish_date,
            effective_date=item.effective_date,
            category=item.category,
            status=item.status,
            source_url=f"https://flk.npc.gov.cn/detail2.html?{item.source_id}",
        )

    async def fetch_metadata_by_title(self, title: str, category_hint: str = "法律") -> LawMetadata:
        return await self.fetch_metadata("resolved-by-title", category_hint)

    async def download_docx(self, source_id: str, source_url: str | None = None) -> bytes:
        self.download_calls.append(source_id)
        return f"docx:{source_id}".encode("utf-8")


def test_pipeline_crawls_category_and_writes_outputs(tmp_path: Path) -> None:
    api = FakeApi()
    storage = CrawlerStorage(tmp_path, metadata_shard_size=10)
    logger = RunLogger(tmp_path / "log" / "crawler", command="crawl-category", arguments={})
    config = CrawlerConfig(data_root=tmp_path, concurrency=2, metadata_shard_size=10)
    pipeline = CrawlerPipeline(api=api, storage=storage, config=config, logger=logger)

    result = asyncio.run(pipeline.crawl_categories(["法律"]))

    assert result.stats.fetched_metadata == 4
    assert result.stats.downloaded_docs == 4
    assert (tmp_path / "source" / "docs" / "标题-law-1.docx").exists()
    assert (tmp_path / "source" / "metadata" / "metadata-0001.json").exists()


def test_pipeline_sync_docs_only_downloads_missing_files(tmp_path: Path) -> None:
    api = FakeApi()
    storage = CrawlerStorage(tmp_path, metadata_shard_size=10)
    storage.ensure_directories()
    storage.save_doc(
        LawMetadata(
            source_id="law-1",
            title="标题-law-1",
            issuer="全国人大常委会",
            publish_date="2024-01-01",
            effective_date="2024-02-01",
            category="法律",
            status="现行有效",
            source_url="https://flk.npc.gov.cn/detail2.html?law-1",
        ),
        b"existing",
    )
    storage.write_metadata_index(
        {
            "law-1": LawMetadata(
                source_id="law-1",
                title="标题-law-1",
                issuer="全国人大常委会",
                publish_date="2024-01-01",
                effective_date="2024-02-01",
                category="法律",
                status="现行有效",
                source_url="https://flk.npc.gov.cn/detail2.html?law-1",
            ),
            "law-2": LawMetadata(
                source_id="law-2",
                title="标题-law-2",
                issuer="全国人大常委会",
                publish_date="2024-01-01",
                effective_date="2024-02-01",
                category="法律",
                status="现行有效",
                source_url="https://flk.npc.gov.cn/detail2.html?law-2",
            ),
        }
    )
    logger = RunLogger(tmp_path / "log" / "crawler", command="crawl-docs", arguments={})
    config = CrawlerConfig(data_root=tmp_path, concurrency=2, metadata_shard_size=10)
    pipeline = CrawlerPipeline(api=api, storage=storage, config=config, logger=logger)

    result = asyncio.run(pipeline.crawl_docs(["法律"]))

    assert result.stats.skipped_docs == 1
    assert result.stats.downloaded_docs == 1
    assert api.download_calls == ["law-2"]


def test_pipeline_metadata_limit_advances_past_existing_records(tmp_path: Path) -> None:
    api = FakeApi()
    storage = CrawlerStorage(tmp_path, metadata_shard_size=10)
    storage.ensure_directories()
    storage.write_metadata_index(
        {
            "law-1": LawMetadata(
                source_id="law-1",
                title="标题-law-1",
                issuer="全国人大常委会",
                publish_date="2024-01-01",
                effective_date="2024-02-01",
                category="法律",
                status="现行有效",
                source_url="https://flk.npc.gov.cn/detail2.html?law-1",
            ),
            "law-2": LawMetadata(
                source_id="law-2",
                title="标题-law-2",
                issuer="全国人大常委会",
                publish_date="2024-01-01",
                effective_date="2024-02-01",
                category="法律",
                status="现行有效",
                source_url="https://flk.npc.gov.cn/detail2.html?law-2",
            ),
        }
    )
    logger = RunLogger(tmp_path / "log" / "crawler", command="crawl-metadata", arguments={})
    config = CrawlerConfig(data_root=tmp_path, concurrency=2, metadata_shard_size=10, limit=2, page_size=2)
    pipeline = CrawlerPipeline(api=api, storage=storage, config=config, logger=logger)

    result = asyncio.run(pipeline.crawl_metadata(["法律"]))
    reloaded = storage.load_metadata_index()

    assert result.stats.fetched_metadata == 2
    assert result.stats.skipped_metadata == 2
    assert sorted(reloaded) == ["law-1", "law-2", "law-3", "law-4"]


def test_pipeline_docs_limit_advances_past_existing_files(tmp_path: Path) -> None:
    api = FakeApi()
    storage = CrawlerStorage(tmp_path, metadata_shard_size=10)
    storage.ensure_directories()
    metadata_index = {
        law_id: LawMetadata(
            source_id=law_id,
            title=f"标题-{law_id}",
            issuer="全国人大常委会",
            publish_date="2024-01-01",
            effective_date="2024-02-01",
            category="法律",
            status="现行有效",
            source_url=f"https://flk.npc.gov.cn/detail2.html?{law_id}",
        )
        for law_id in ("law-1", "law-2", "law-3", "law-4")
    }
    storage.write_metadata_index(metadata_index)
    storage.save_doc(metadata_index["law-1"], b"existing-1")
    storage.save_doc(metadata_index["law-2"], b"existing-2")
    logger = RunLogger(tmp_path / "log" / "crawler", command="crawl-docs", arguments={})
    config = CrawlerConfig(data_root=tmp_path, concurrency=2, metadata_shard_size=10, limit=2)
    pipeline = CrawlerPipeline(api=api, storage=storage, config=config, logger=logger)

    result = asyncio.run(pipeline.crawl_docs(["法律"]))

    assert result.stats.downloaded_docs == 2
    assert result.stats.skipped_docs == 2
    assert api.download_calls == ["law-3", "law-4"]


def test_pipeline_docs_only_downloads_latest_metadata_for_duplicate_titles(tmp_path: Path) -> None:
    api = FakeApi()
    storage = CrawlerStorage(tmp_path, metadata_shard_size=10)
    storage.ensure_directories()
    storage.write_metadata_index(
        {
            "law-1": LawMetadata(
                source_id="law-1",
                title="同名法规",
                issuer="全国人大常委会",
                publish_date="2024-01-01",
                effective_date="2024-02-01",
                category="法律",
                status="现行有效",
                source_url="https://flk.npc.gov.cn/detail?id=law-1&title=%E5%90%8C%E5%90%8D%E6%B3%95%E8%A7%84",
            ),
            "law-2": LawMetadata(
                source_id="law-2",
                title="同名法规",
                issuer="全国人大常委会",
                publish_date="2024-01-01",
                effective_date="2024-02-01",
                category="法律",
                status="已修改",
                source_url="https://flk.npc.gov.cn/detail?id=law-2&title=%E5%90%8C%E5%90%8D%E6%B3%95%E8%A7%84",
            ),
        }
    )
    logger = RunLogger(tmp_path / "log" / "crawler", command="crawl-docs", arguments={})
    config = CrawlerConfig(data_root=tmp_path, concurrency=2, metadata_shard_size=10)
    pipeline = CrawlerPipeline(api=api, storage=storage, config=config, logger=logger)

    result = asyncio.run(pipeline.crawl_docs(["法律"]))

    assert result.stats.downloaded_docs == 1
    assert result.stats.failed == 0
    assert (tmp_path / "source" / "docs" / "同名法规.docx").exists()
