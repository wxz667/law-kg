from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CATEGORY_ID_MAP: dict[str, int] = {
    "宪法": 1,
    "法律": 2,
    "行政法规": 14,
    "监察法规": 15,
    "地方法规": 16,
    "司法解释": 27,
}

METADATA_FIELDS: tuple[str, ...] = (
    "source_id",
    "title",
    "issuer",
    "publish_date",
    "effective_date",
    "category",
    "status",
    "source_url",
    "source_format",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(slots=True)
class LawMetadata:
    source_id: str
    title: str
    issuer: str | None
    publish_date: str | None
    effective_date: str | None
    category: str
    status: str | None
    source_url: str
    source_format: str = "docx"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        return {key: value for key, value in payload.items() if key in METADATA_FIELDS}

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "LawMetadata":
        normalized = {field: payload.get(field) for field in METADATA_FIELDS}
        return cls(
            source_id=str(normalized["source_id"] or ""),
            title=str(normalized["title"] or ""),
            issuer=_optional_string(normalized["issuer"]),
            publish_date=_optional_string(normalized["publish_date"]),
            effective_date=_optional_string(normalized["effective_date"]),
            category=str(normalized["category"] or ""),
            status=_optional_string(normalized["status"]),
            source_url=str(normalized["source_url"] or ""),
            source_format=str(normalized["source_format"] or "docx"),
        )

    def is_complete(self) -> bool:
        return bool(self.source_id and self.title and self.category and self.source_url)


@dataclass(slots=True)
class LawListItem:
    source_id: str
    title: str
    category: str
    issuer: str | None = None
    publish_date: str | None = None
    effective_date: str | None = None
    status: str | None = None


@dataclass(slots=True)
class PageResult:
    items: list[LawListItem]
    total: int | None


@dataclass(slots=True)
class CrawlerConfig:
    data_root: Path
    base_url: str = "https://flk.npc.gov.cn"
    metadata_dir: Path | None = None
    document_dir: Path | None = None
    overwrite_metadata: bool = False
    overwrite_docs: bool = False
    concurrency: int = 2
    retries: int = 3
    timeout: float = 10.0
    request_delay: float = 0.3
    request_jitter: float = 0.5
    warmup_timeout: float = 5.0
    bootstrap_api_probe: bool = False
    metadata_shard_size: int = 1000
    page_size: int = 20
    checkpoint_every: int = 50
    limit: int | None = None


@dataclass(slots=True)
class FailureRecord:
    source_id: str | None
    category: str | None
    stage: str
    error: str
    occurred_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class CrawlStats:
    fetched_metadata: int = 0
    downloaded_docs: int = 0
    skipped_metadata: int = 0
    skipped_docs: int = 0
    failed: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "fetched_metadata": self.fetched_metadata,
            "downloaded_docs": self.downloaded_docs,
            "skipped_metadata": self.skipped_metadata,
            "skipped_docs": self.skipped_docs,
            "failed": self.failed,
        }


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
