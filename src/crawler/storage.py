from __future__ import annotations

import math
import re
from datetime import date
from pathlib import Path

from builder.io.json_store import read_json, write_json

from .models import LawMetadata

DOC_EXTENSION = ".docx"
DOC_ID_SUFFIX_LENGTH = 12
MAX_FILENAME_BYTES = 255


class CrawlerStorage:
    def __init__(self, data_root: Path, metadata_shard_size: int = 1000) -> None:
        self.data_root = data_root
        self.docs_dir = self.data_root / "source" / "docs"
        self.metadata_dir = self.data_root / "source" / "metadata"
        self.logs_dir = self.data_root.parent / "logs" / "crawler"
        self.metadata_shard_size = metadata_shard_size

    def ensure_directories(self) -> None:
        self.docs_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

    def doc_path(self, metadata: LawMetadata, duplicate_filenames: set[str] | None = None) -> Path:
        duplicate_filenames = duplicate_filenames or set()
        preferred_name = build_doc_filename(metadata.title or metadata.source_id)
        if preferred_name in duplicate_filenames:
            return self.docs_dir / build_doc_filename(
                metadata.title or metadata.source_id,
                source_id=metadata.source_id,
            )
        preferred = self.docs_dir / preferred_name
        if preferred.exists():
            return self.docs_dir / build_doc_filename(
                metadata.title or metadata.source_id,
                source_id=metadata.source_id,
            )
        suffixed = self.docs_dir / build_doc_filename(
            metadata.title or metadata.source_id,
            source_id=metadata.source_id,
        )
        if suffixed.exists():
            return suffixed
        return preferred

    def find_doc_path(
        self,
        metadata: LawMetadata | None,
        duplicate_filenames: set[str] | None = None,
    ) -> Path | None:
        if metadata is None:
            return None
        duplicate_filenames = duplicate_filenames or set()
        preferred_name = build_doc_filename(metadata.title or metadata.source_id)
        if preferred_name in duplicate_filenames:
            candidates = [
                self.docs_dir / build_doc_filename(metadata.title or metadata.source_id, source_id=metadata.source_id),
                self.docs_dir / f"{metadata.source_id}{DOC_EXTENSION}",
            ]
        else:
            candidates = [
                self.docs_dir / preferred_name,
                self.docs_dir / build_doc_filename(metadata.title or metadata.source_id, source_id=metadata.source_id),
                self.docs_dir / f"{metadata.source_id}{DOC_EXTENSION}",
            ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None

    def has_doc(self, metadata: LawMetadata | None, duplicate_filenames: set[str] | None = None) -> bool:
        return self.find_doc_path(metadata, duplicate_filenames) is not None

    def save_doc(self, metadata: LawMetadata, content: bytes, duplicate_filenames: set[str] | None = None) -> Path:
        path = self.doc_path(metadata, duplicate_filenames)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(f".tmp-{short_source_suffix(metadata.source_id)}")
        temp_path.write_bytes(content)
        temp_path.replace(path)
        return path

    def list_doc_paths(self) -> list[Path]:
        if not self.docs_dir.exists():
            return []
        return sorted(self.docs_dir.glob("*.docx"))

    def load_metadata_index(self) -> dict[str, LawMetadata]:
        raw_index: dict[str, LawMetadata] = {}
        for path in sorted(self.metadata_dir.glob("metadata-*.json")):
            payload = read_json(path)
            if not isinstance(payload, list):
                continue
            for item in payload:
                if not isinstance(item, dict):
                    continue
                metadata = LawMetadata.from_dict(item)
                if metadata.source_id:
                    raw_index[metadata.source_id] = metadata
        deduplicated, _ = split_deduplicated_metadata(raw_index)
        return deduplicated

    def load_metadata_index_with_drops(self) -> tuple[dict[str, LawMetadata], list[LawMetadata]]:
        raw_index: dict[str, LawMetadata] = {}
        for path in sorted(self.metadata_dir.glob("metadata-*.json")):
            payload = read_json(path)
            if not isinstance(payload, list):
                continue
            for item in payload:
                if not isinstance(item, dict):
                    continue
                metadata = LawMetadata.from_dict(item)
                if metadata.source_id:
                    raw_index[metadata.source_id] = metadata
        return split_deduplicated_metadata(raw_index)

    def deduplicate_metadata_index(self, index: dict[str, LawMetadata]) -> tuple[dict[str, LawMetadata], int]:
        deduplicated, dropped = split_deduplicated_metadata(index)
        return deduplicated, len(dropped)

    def write_metadata_index(self, index: dict[str, LawMetadata]) -> list[Path]:
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        existing = sorted(self.metadata_dir.glob("metadata-*.json"))
        for path in existing:
            path.unlink()

        deduplicated = deduplicate_metadata_index(index)
        rows = [deduplicated[key].to_dict() for key in sorted(deduplicated)]
        if not rows:
            output = self.metadata_dir / "metadata-0001.json"
            write_json(output, [])
            return [output]

        shard_count = math.ceil(len(rows) / self.metadata_shard_size)
        written: list[Path] = []
        for shard_no in range(shard_count):
            start = shard_no * self.metadata_shard_size
            end = start + self.metadata_shard_size
            path = self.metadata_dir / f"metadata-{shard_no + 1:04d}.json"
            write_json(path, rows[start:end])
            written.append(path)
        return written

    def should_fetch_metadata(
        self,
        source_id: str,
        metadata_index: dict[str, LawMetadata],
        overwrite: bool,
    ) -> bool:
        if overwrite:
            return True
        existing = metadata_index.get(source_id)
        return existing is None or not existing.is_complete()

    def should_fetch_doc(
        self,
        metadata: LawMetadata | None,
        overwrite: bool,
        duplicate_filenames: set[str] | None = None,
    ) -> bool:
        if overwrite:
            return True
        return not self.has_doc(metadata, duplicate_filenames)


def sanitize_doc_title(title: str) -> str:
    text = title.strip()
    text = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = text.rstrip(". ")
    return text or "untitled"


def build_doc_filename(title: str, source_id: str | None = None) -> str:
    safe_title = sanitize_doc_title(title)
    suffix = ""
    if source_id:
        suffix = f"__{short_source_suffix(source_id)}"
    max_title_bytes = MAX_FILENAME_BYTES - len(DOC_EXTENSION.encode("utf-8")) - len(suffix.encode("utf-8"))
    truncated_title = truncate_utf8_bytes(safe_title, max_title_bytes)
    if not truncated_title:
        truncated_title = "untitled"
    return f"{truncated_title}{suffix}{DOC_EXTENSION}"


def short_source_suffix(source_id: str) -> str:
    clean = re.sub(r"[^0-9A-Za-z]+", "", source_id)
    if not clean:
        clean = "id"
    return clean[-DOC_ID_SUFFIX_LENGTH:]


def truncate_utf8_bytes(text: str, max_bytes: int) -> str:
    if max_bytes <= 0:
        return ""
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    truncated = encoded[:max_bytes]
    while truncated:
        try:
            return truncated.decode("utf-8").rstrip(". ")
        except UnicodeDecodeError:
            truncated = truncated[:-1]
    return ""


def deduplicate_metadata_index(index: dict[str, LawMetadata]) -> dict[str, LawMetadata]:
    deduplicated, _ = split_deduplicated_metadata(index)
    return deduplicated


def split_deduplicated_metadata(index: dict[str, LawMetadata]) -> tuple[dict[str, LawMetadata], list[LawMetadata]]:
    latest_by_title: dict[str, LawMetadata] = {}
    unique_without_title: dict[str, LawMetadata] = {}
    dropped_by_title: dict[str, list[LawMetadata]] = {}
    for metadata in index.values():
        title_key = metadata.title.strip() if metadata.title else ""
        if not title_key:
            unique_without_title[metadata.source_id] = metadata
            continue
        current = latest_by_title.get(title_key)
        if current is None or metadata_sort_key(metadata) > metadata_sort_key(current):
            if current is not None:
                dropped_by_title.setdefault(title_key, []).append(current)
            latest_by_title[title_key] = metadata
            if current is not None:
                retained = metadata
                dropped_by_title[title_key] = [
                    item for item in dropped_by_title.get(title_key, []) if item.source_id != retained.source_id
                ]
        else:
            dropped_by_title.setdefault(title_key, []).append(metadata)
    deduplicated = {metadata.source_id: metadata for metadata in latest_by_title.values()}
    deduplicated.update(unique_without_title)
    dropped: list[LawMetadata] = []
    for items in dropped_by_title.values():
        dropped.extend(items)
    return deduplicated, dropped


def metadata_sort_key(metadata: LawMetadata) -> tuple[date, date, str]:
    return (
        parse_metadata_date(metadata.publish_date),
        parse_metadata_date(metadata.effective_date),
        metadata.source_id,
    )


def parse_metadata_date(value: str | None) -> date:
    if not value:
        return date.min
    try:
        return date.fromisoformat(value.strip())
    except ValueError:
        return date.min
