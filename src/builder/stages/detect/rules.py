from __future__ import annotations

import re

ORDINARY_SKIP_PATTERNS = (
    re.compile(r"^(序号|法律规定|刑法条文|罪名|发文日期|废止理由)[:：]"),
    re.compile(r"（以下简称[^）]{1,20}）"),
)
MEANINGLESS_SELF_REFERENCES = {"本法", "本条例", "本规定", "本解释", "本办法", "本决定"}


def should_skip_ordinary_reference_sentence(sentence: str) -> bool:
    compact = re.sub(r"\s+", "", sentence)
    if any(pattern.search(compact) for pattern in ORDINARY_SKIP_PATTERNS):
        return True
    if any(marker in compact for marker in ("修改为", "修正为", "修订为", "废止", "停止适用", "简称")):
        return True
    return False


def is_meaningless_self_reference(text: str) -> bool:
    compact = re.sub(r"\s+", "", text)
    compact = compact.strip("，,。；;：:()（）")
    return compact in MEANINGLESS_SELF_REFERENCES
