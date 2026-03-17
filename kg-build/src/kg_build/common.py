from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path

CN_DIGITS = {
    "零": 0,
    "〇": 0,
    "一": 1,
    "二": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "两": 2,
}

CN_UNITS = {
    "十": 10,
    "百": 100,
    "千": 1000,
    "万": 10000,
}


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def repo_root() -> Path:
    return project_root().parent


def timestamp_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def slugify(value: str) -> str:
    compact = re.sub(r"\s+", "-", value.strip().lower())
    compact = re.sub(r"[^\w\u4e00-\u9fff-]", "", compact, flags=re.UNICODE)
    compact = re.sub(r"-{2,}", "-", compact)
    return compact.strip("-") or "artifact"


def checksum_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def checksum_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_id_from_source(source_path: Path) -> str:
    digest = checksum_file(source_path)[:12]
    return f"build-{slugify(source_path.stem)}-{digest}"


def chinese_number_to_int(text: str) -> int:
    normalized = text.strip()
    if not normalized:
        raise ValueError("Chinese numeral text cannot be empty")
    if normalized.isdigit():
        return int(normalized)

    total = 0
    section = 0
    number = 0
    for char in normalized:
        if char in CN_DIGITS:
            number = CN_DIGITS[char]
            continue
        if char in CN_UNITS:
            unit = CN_UNITS[char]
            if unit == 10000:
                section = (section + (number or 0)) * unit
                total += section
                section = 0
                number = 0
            else:
                section += (number or 1) * unit
                number = 0
            continue
        raise ValueError(f"Unsupported Chinese numeral character: {char}")
    return total + section + number


def to_fullwidth_digit_text(text: str) -> str:
    return text.translate(str.maketrans("０１２３４５６７８９", "0123456789"))


def parse_article_components(article_label: str) -> tuple[int, int | None]:
    match = re.match(
        r"^第([一二三四五六七八九十百千万零两〇0-9]+)条(?:之([一二三四五六七八九十百千万零两〇0-9]+))?$",
        article_label,
    )
    if not match:
        raise ValueError(f"Invalid article label: {article_label}")
    base = chinese_number_to_int(match.group(1))
    suffix = chinese_number_to_int(match.group(2)) if match.group(2) else None
    return base, suffix


def format_article_key(article_no: int, article_suffix: int | None) -> str:
    if article_suffix is None:
        return f"{article_no:04d}"
    return f"{article_no:04d}-{article_suffix:02d}"


def int_to_cn(number: int) -> str:
    if number <= 0:
        raise ValueError("Only positive integers are supported")
    digits = "零一二三四五六七八九"
    units = ["", "十", "百", "千"]
    if number < 10:
        return digits[number]
    if number < 10000:
        pieces: list[str] = []
        chars = list(str(number))
        length = len(chars)
        zero_pending = False
        for index, char in enumerate(chars):
            value = int(char)
            unit = units[length - index - 1]
            if value == 0:
                zero_pending = bool(pieces)
                continue
            if zero_pending:
                pieces.append("零")
                zero_pending = False
            if value == 1 and unit == "十" and not pieces:
                pieces.append("十")
            else:
                pieces.append(digits[value] + unit)
        return "".join(pieces)
    if number < 100000000:
        high, low = divmod(number, 10000)
        high_text = int_to_cn(high) + "万"
        if low == 0:
            return high_text
        if low < 1000:
            return high_text + "零" + int_to_cn(low)
        return high_text + int_to_cn(low)
    raise ValueError("Numbers above 99,999,999 are not supported")
