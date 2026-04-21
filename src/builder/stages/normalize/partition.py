from __future__ import annotations

import re

from ...contracts import DocumentUnitRecord, LogicalDocumentRecord
from ...utils.layout import APPENDIX_RE, SEGMENT_HEADING_RE, match_heading_level, normalize_segment_heading


def build_document_unit(logical_document: LogicalDocumentRecord) -> DocumentUnitRecord:
    role = str(logical_document.metadata.get("document_role", "substantive"))
    body_lines = trim_body_lines(logical_document.paragraphs, role=role)
    appendix_lines = list(logical_document.appendix_lines)
    if not appendix_lines:
        body_lines, appendix_lines = split_body_and_appendix(body_lines)
    return DocumentUnitRecord(
        source_id=logical_document.source_id,
        title=logical_document.title,
        source_type=logical_document.source_type,
        body_lines=body_lines,
        appendix_lines=appendix_lines,
        metadata=dict(logical_document.metadata),
    )


def trim_body_lines(lines: list[str], *, role: str) -> list[str]:
    body_lines = [line.strip() for line in lines if line.strip()]
    if not body_lines:
        return []
    body_lines = strip_leading_cover_lines(body_lines)
    body_lines = strip_trailing_attachment_markers(body_lines)
    body_lines = strip_trailing_formal_closure_block(body_lines)
    if role == "reply":
        return body_lines

    structural_start = next((index for index, line in enumerate(body_lines) if match_heading_level(line) is not None), None)
    if structural_start is not None:
        return body_lines[structural_start:]

    candidate_start = next((index for index, line in enumerate(body_lines) if SEGMENT_HEADING_RE.match(line)), None)
    if candidate_start is not None:
        return body_lines[candidate_start:]
    return body_lines


def split_body_and_appendix(lines: list[str]) -> tuple[list[str], list[str]]:
    body_lines = [line.strip() for line in lines if line.strip()]
    if not body_lines:
        return [], []
    appendix_index = next((index for index, line in enumerate(body_lines) if APPENDIX_RE.match(normalize_segment_heading(line))), None)
    if appendix_index is None:
        return body_lines, []
    return body_lines[:appendix_index], body_lines[appendix_index:]


def strip_leading_cover_lines(lines: list[str]) -> list[str]:
    start = 0
    while start < len(lines):
        line = lines[start].strip()
        if not line:
            start += 1
            continue
        if is_cover_line(line):
            start += 1
            continue
        break
    return lines[start:]


def strip_trailing_attachment_markers(lines: list[str]) -> list[str]:
    trimmed = list(lines)
    while trimmed and is_attachment_marker_line(trimmed[-1]):
        trimmed.pop()
    return trimmed


def strip_trailing_formal_closure_block(lines: list[str]) -> list[str]:
    trimmed = list(lines)
    while trimmed and is_signature_line(trimmed[-1]):
        trimmed.pop()
    while trimmed and is_formal_closure_line(trimmed[-1]):
        trimmed.pop()
    while trimmed and is_signature_line(trimmed[-1]):
        trimmed.pop()
    return trimmed


def is_cover_line(line: str) -> bool:
    normalized = normalize_segment_heading(line)
    if not normalized:
        return True
    if is_attachment_marker_line(line):
        return True
    return False


def is_attachment_marker_line(line: str) -> bool:
    return bool(re.match(r"^附(?:件)?(?:[：:]|[一二三四五六七八九十百千万零两〇0-9]+)?$", normalize_segment_heading(line)))


def is_signature_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return True
    compact = normalize_segment_heading(stripped)
    return compact.endswith(("人民法院", "人民检察院", "办公厅", "委员会", "人民政府"))


def is_formal_closure_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped or len(normalize_segment_heading(stripped)) > 8:
        return False
    if match_heading_level(stripped) or SEGMENT_HEADING_RE.match(stripped):
        return False
    return bool(re.match(r"^(?:此[复函令致]|特此\S{0,4}|现予\S{0,4})[。.]?$", stripped))
