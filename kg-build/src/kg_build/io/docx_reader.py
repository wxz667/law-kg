from __future__ import annotations

import re
from pathlib import Path

from docx import Document

from ..utils.ids import checksum_text, slugify
from ..contracts import SourceDocumentRecord

PART_RE = re.compile(r"^第[一二三四五六七八九十百零]+编\s+.+$")
APPENDIX_RE = re.compile(r"^附件[一二三四五六七八九十百千万零两〇0-9]+$")
DATE_RE = re.compile(r"(\d{4}年\d{1,2}月\d{1,2}日)([^、；。）]+)")


def read_source_document(source_path: Path) -> SourceDocumentRecord:
    if source_path.suffix.lower() != ".docx":
        raise ValueError(f"Unsupported source format: {source_path.suffix}")
    document = Document(str(source_path))
    paragraphs = [paragraph.text.strip() for paragraph in document.paragraphs if paragraph.text.strip()]
    title = paragraphs[0] if paragraphs else source_path.stem
    preface_text, toc_lines, body_lines, appendix_lines = split_document_sections(paragraphs)
    revision_events = parse_revision_events(preface_text)
    return SourceDocumentRecord(
        source_id=f"{source_path.parent.name}:{slugify(source_path.stem)}",
        title=title,
        source_path=str(source_path.resolve()),
        source_type=source_path.parent.name,
        checksum=checksum_text(normalize_text("\n".join(paragraphs))),
        preface_text=preface_text,
        toc_lines=toc_lines,
        body_lines=body_lines,
        appendix_lines=appendix_lines,
        metadata={
            "file_name": source_path.name,
            "paragraph_count": len(paragraphs),
            "revision_events": revision_events,
        },
    )


def normalize_text(text: str) -> str:
    lines = [line.strip() for line in text.replace("\r\n", "\n").split("\n")]
    normalized = "\n".join(line for line in lines if line)
    return normalized.strip() + ("\n" if normalized else "")


def split_document_sections(
    paragraphs: list[str],
) -> tuple[str, list[str], list[str], list[str]]:
    if not paragraphs:
        return "", [], [], []

    toc_index = next((index for index, line in enumerate(paragraphs) if normalize_heading(line) == "目录"), None)
    preface_lines = paragraphs[1:toc_index] if toc_index is not None else paragraphs[1:2]
    preface_text = normalize_text("\n".join(preface_lines)).strip()

    toc_lines: list[str] = []
    content_lines: list[str] = paragraphs[1:]
    if toc_index is not None:
        body_start = find_body_start_index(paragraphs, toc_index)
        toc_lines = paragraphs[toc_index + 1 : body_start]
        content_lines = paragraphs[body_start:]

    appendix_start = next(
        (index for index, line in enumerate(content_lines) if APPENDIX_RE.match(line)),
        None,
    )
    if appendix_start is None:
        return preface_text, toc_lines, content_lines, []
    return (
        preface_text,
        toc_lines,
        content_lines[:appendix_start],
        content_lines[appendix_start:],
    )


def find_body_start_index(paragraphs: list[str], toc_index: int) -> int:
    part_indices = [index for index, line in enumerate(paragraphs) if PART_RE.match(line)]
    if len(part_indices) >= 2:
        first_part_line = paragraphs[part_indices[0]]
        for later_index in part_indices[1:]:
            if paragraphs[later_index] == first_part_line and later_index > toc_index:
                return later_index
    return toc_index + 1


def normalize_heading(text: str) -> str:
    return text.replace(" ", "").replace("　", "")


def parse_revision_events(preface_text: str) -> list[dict[str, str]]:
    events: list[dict[str, str]] = []
    if not preface_text:
        return events
    for date_text, description in DATE_RE.findall(preface_text):
        cleaned_description = description.strip("　 ")
        events.append(
            {
                "date": date_text,
                "event_type": classify_revision_event(cleaned_description),
                "description": cleaned_description,
            }
        )
    return events


def classify_revision_event(description: str) -> str:
    if "修订" in description:
        return "revision"
    if "修正" in description or "修正案" in description:
        return "amendment"
    if "通过" in description:
        return "adoption"
    return "other"
