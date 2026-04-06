from __future__ import annotations

import re

ABSOLUTE_ARTICLE_RE = re.compile(
    r"(?P<prefix>依照|根据|按照|参照)?《(?P<title>[^》]+)》第(?P<article>[一二三四五六七八九十百千万零两〇0-9]+条(?:之[一二三四五六七八九十百千万零两〇0-9]+)?)"
)
LOCAL_ARTICLE_RE = re.compile(r"本法第(?P<article>[一二三四五六七八九十百千万零两〇0-9]+条(?:之[一二三四五六七八九十百千万零两〇0-9]+)?)")
THIS_ARTICLE_RE = re.compile(r"本条")
THIS_PARAGRAPH_RE = re.compile(r"本款")
PREVIOUS_PARAGRAPH_RE = re.compile(r"前款")
SENTENCE_SPLIT_RE = re.compile(r"[。；;]\s*")
