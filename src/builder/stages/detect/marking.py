from __future__ import annotations

import re

TARGET_OPEN = "[T]"
TARGET_CLOSE = "[/T]"
TARGET_MARKED_RE = re.compile(r"\[T\](?P<target>.+?)\[/T\]", re.DOTALL)


def has_single_target_marker(text: str) -> bool:
    value = str(text or "")
    return value.count(TARGET_OPEN) == 1 and value.count(TARGET_CLOSE) == 1


def mark_target_text(
    text: str,
    start: int,
    end: int,
    *,
    replacement_text: str | None = None,
    fallback_to_full_text: bool = False,
) -> str:
    value = str(text or "")
    if start < 0 or end <= start or end > len(value):
        if not fallback_to_full_text:
            return value
        target_text = replacement_text if replacement_text is not None else value
        return f"{TARGET_OPEN}{target_text}{TARGET_CLOSE}"

    target_text = replacement_text if replacement_text is not None else value[start:end]
    if replacement_text:
        overlap = overlapping_prefix_length(value[:start], replacement_text)
        if overlap > 0:
            start -= overlap
    return f"{value[:start]}{TARGET_OPEN}{target_text}{TARGET_CLOSE}{value[end:]}"


def overlapping_prefix_length(prefix_text: str, replacement_text: str) -> int:
    upper_bound = min(len(prefix_text), len(replacement_text))
    for overlap in range(upper_bound, 0, -1):
        if prefix_text.endswith(replacement_text[:overlap]):
            return overlap
    return 0


def extract_target_text(marked_text: str) -> str:
    match = TARGET_MARKED_RE.search(str(marked_text or ""))
    return match.group("target").strip() if match else ""
