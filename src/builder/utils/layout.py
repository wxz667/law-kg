from __future__ import annotations

import re

INVISIBLE_RE = re.compile(r"[\u200b\u200c\u200d\ufeff\xa0]")
SPACE_RE = re.compile(r"[ \t]+")

PART_RE = re.compile(r"^第[一二三四五六七八九十百零]+编(?:\s*.+)?$")
CHAPTER_RE = re.compile(r"^第[一二三四五六七八九十百零]+章(?:\s*.+)?$")
SECTION_RE = re.compile(r"^第[一二三四五六七八九十百零]+节(?:\s*.+)?$")
ARTICLE_RE = re.compile(
    r"^(第[一二三四五六七八九十百千万零两〇0-9]+条(?:之[一二三四五六七八九十百千万零两〇0-9]+)?)(?:[\s　]+(.*))?$"
)
ARTICLE_TITLE_SUFFIX_RE = re.compile(
    r"^第[一二三四五六七八九十百千万零两〇0-9]+条"
    r"(?:、第[一二三四五六七八九十百千万零两〇0-9]+条)*的解释$"
)
APPENDIX_RE = re.compile(r"^附件([一二三四五六七八九十百千万零两〇0-9]+)$")
ITEM_MARKER_RE = re.compile(r"((?:（|\()[一二三四五六七八九十]+(?:）|\))|[一二三四五六七八九十]+、)")
SUB_ITEM_MARKER_RE = re.compile(r"((?:[0-9０-９]+[.．、])|(?:（|\()[0-9０-９]+(?:）|\)))")
SEGMENT_HEADING_RE = re.compile(r"^[一二三四五六七八九十百千]+、.+$")
PARAGRAPH_HEADING_RE = re.compile(r"^(?:（|\()[一二三四五六七八九十]+(?:）|\)).+$")
NUMBERED_LIST_RE = re.compile(r"^(?P<index>[0-9０-９]+)[.．、]\s*(?P<body>.+)$")


def clean_text(text: str) -> str:
    text = INVISIBLE_RE.sub("", text or "")
    text = SPACE_RE.sub(" ", text)
    text = text.replace("\u3000", " ")
    return text.strip()


def normalize_heading_text(text: str) -> str:
    return (text or "").replace(" ", "").replace("　", "").strip()


def compact_text_key(text: str) -> str:
    return re.sub(r"\s+", "", (text or "").replace("\u3000", " ").strip())


def match_heading_level(line: str) -> str | None:
    if PART_RE.match(line):
        return "part"
    if CHAPTER_RE.match(line):
        return "chapter"
    if SECTION_RE.match(line):
        return "section"
    if ARTICLE_RE.match(line):
        return "article"
    return None


def normalize_segment_heading(text: str) -> str:
    return normalize_heading_text(text)


def is_structural_body_start(line: str) -> bool:
    return bool(
        PART_RE.match(line)
        or CHAPTER_RE.match(line)
        or SECTION_RE.match(line)
        or ARTICLE_RE.match(line)
    )


def looks_like_heading_continuation(text: str) -> bool:
    if not text:
        return False
    if any(marker in text for marker in ("。", "；", "：", "，")):
        return False
    if ITEM_MARKER_RE.match(text) or SUB_ITEM_MARKER_RE.match(text):
        return False
    return len(normalize_segment_heading(text)) <= 24


def merge_structural_heading_continuations(lines: list[str]) -> list[str]:
    merged: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index].strip()
        if not line:
            index += 1
            continue
        if match_heading_level(line) in {"part", "chapter", "section"} or SEGMENT_HEADING_RE.match(line) or PARAGRAPH_HEADING_RE.match(line):
            parts = [line]
            look_ahead = index + 1
            while look_ahead < len(lines):
                candidate = lines[look_ahead].strip()
                if not candidate:
                    look_ahead += 1
                    continue
                if match_heading_level(candidate) is not None:
                    break
                if SEGMENT_HEADING_RE.match(candidate) or PARAGRAPH_HEADING_RE.match(candidate):
                    break
                if ARTICLE_RE.match(candidate):
                    break
                next_candidate = ""
                probe_index = look_ahead + 1
                while probe_index < len(lines):
                    probe_line = lines[probe_index].strip()
                    if probe_line:
                        next_candidate = probe_line
                        break
                    probe_index += 1
                if next_candidate and NUMBERED_LIST_RE.match(next_candidate):
                    break
                if looks_like_heading_continuation(candidate):
                    parts.append(candidate)
                    look_ahead += 1
                    continue
                break
            merged.append("".join(parts))
            index = look_ahead
            continue
        merged.append(line)
        index += 1
    return merged
