from __future__ import annotations

from pathlib import Path

from crawler.models import LawMetadata
from crawler.storage import CrawlerStorage, build_doc_filename, deduplicate_metadata_index, sanitize_doc_title


def build_metadata(source_id: str, category: str = "法律") -> LawMetadata:
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


def test_storage_writes_and_reads_metadata_shards(tmp_path: Path) -> None:
    storage = CrawlerStorage(tmp_path, metadata_shard_size=2)
    storage.ensure_directories()
    index = {
        "a": build_metadata("a"),
        "b": build_metadata("b"),
        "c": build_metadata("c"),
    }

    written = storage.write_metadata_index(index)

    assert len(written) == 2
    reloaded = storage.load_metadata_index()
    assert sorted(reloaded) == ["a", "b", "c"]
    assert reloaded["b"].title == "标题-b"


def test_storage_skip_logic_respects_overwrite(tmp_path: Path) -> None:
    storage = CrawlerStorage(tmp_path, metadata_shard_size=100)
    storage.ensure_directories()
    metadata = build_metadata("abc")
    index = {"abc": metadata}
    storage.write_metadata_index(index)
    storage.save_doc(metadata, b"docx")

    loaded = storage.load_metadata_index()
    assert storage.should_fetch_metadata("abc", loaded, overwrite=False) is False
    assert storage.should_fetch_metadata("abc", loaded, overwrite=True) is True
    assert storage.should_fetch_doc(metadata, overwrite=False) is False
    assert storage.should_fetch_doc(metadata, overwrite=True) is True


def test_storage_uses_human_readable_doc_title(tmp_path: Path) -> None:
    storage = CrawlerStorage(tmp_path, metadata_shard_size=100)
    storage.ensure_directories()
    metadata = build_metadata("abc")
    metadata.title = "中华人民共和国民族团结进步促进法"

    path = storage.save_doc(metadata, b"docx")

    assert path.name == f"{sanitize_doc_title(metadata.title)}.docx"


def test_storage_repartitions_all_shards_when_appending(tmp_path: Path) -> None:
    storage = CrawlerStorage(tmp_path, metadata_shard_size=2)
    storage.ensure_directories()
    storage.write_metadata_index(
        {
            "a": build_metadata("a"),
            "b": build_metadata("b"),
        }
    )

    loaded = storage.load_metadata_index()
    loaded["c"] = build_metadata("c")
    written = storage.write_metadata_index(loaded)

    assert len(written) == 2
    assert [path.name for path in written] == ["metadata-0001.json", "metadata-0002.json"]
    reloaded = storage.load_metadata_index()
    assert sorted(reloaded) == ["a", "b", "c"]


def test_storage_truncates_overlong_doc_filename(tmp_path: Path) -> None:
    storage = CrawlerStorage(tmp_path, metadata_shard_size=100)
    storage.ensure_directories()
    metadata = build_metadata("402881e45ffb5c4c015ffb8729950404")
    metadata.title = "最高人民法院、最高人民检察院关于办理利用互联网、移动通讯终端、声讯台制作、复制、出版、贩卖、传播淫秽电子信息刑事案件具体应用法律若干问题的解释（二）" * 3

    path = storage.save_doc(metadata, b"docx")

    assert path.exists()
    assert len(path.name.encode("utf-8")) <= 255
    assert path.suffix == ".docx"


def test_storage_uses_short_suffix_for_duplicate_title(tmp_path: Path) -> None:
    storage = CrawlerStorage(tmp_path, metadata_shard_size=100)
    storage.ensure_directories()
    first = build_metadata("first-source-id-0001")
    second = build_metadata("second-source-id-0002")
    first.title = "同名法规"
    second.title = "同名法规"

    first_path = storage.save_doc(first, b"docx-1")
    second_path = storage.save_doc(second, b"docx-2")

    assert first_path.name == "同名法规.docx"
    assert second_path.name == build_doc_filename(second.title, second.source_id)
    assert "__" in second_path.stem


def test_storage_keeps_latest_metadata_for_same_title_by_publish_date(tmp_path: Path) -> None:
    older = build_metadata("old-law")
    newer = build_metadata("new-law")
    older.title = "中华人民共和国国务院组织法"
    newer.title = "中华人民共和国国务院组织法"
    older.publish_date = "1982-12-10"
    newer.publish_date = "2024-03-11"
    older.effective_date = "1982-12-10"
    newer.effective_date = "2024-03-12"

    deduplicated = deduplicate_metadata_index(
        {
            older.source_id: older,
            newer.source_id: newer,
        }
    )

    assert sorted(deduplicated) == ["new-law"]


def test_storage_keeps_latest_metadata_for_same_title_by_effective_date_when_publish_matches(tmp_path: Path) -> None:
    earlier = build_metadata("law-a")
    later = build_metadata("law-b")
    earlier.title = "中华人民共和国网络安全法"
    later.title = "中华人民共和国网络安全法"
    earlier.publish_date = "2025-10-28"
    later.publish_date = "2025-10-28"
    earlier.effective_date = "2025-12-01"
    later.effective_date = "2026-01-01"

    deduplicated = deduplicate_metadata_index(
        {
            earlier.source_id: earlier,
            later.source_id: later,
        }
    )

    assert sorted(deduplicated) == ["law-b"]
