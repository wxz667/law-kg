from __future__ import annotations

import re

NUMERAL = r"[一二三四五六七八九十百千万零两〇0-9]+"
PAREN_NUMERAL = r"(?:[（(][一二三四五六七八九十百千万零两〇0-9]+[）)]|[一二三四五六七八九十百千万零两〇0-9]+)"
OMIT_PAREN_NUMERAL = (
    r"(?:"
    r"第[一二三四五六七八九十百千万零两〇0-9]+"
    r"|第?[（(][一二三四五六七八九十百千万零两〇0-9]+[）)]"
    r")"
)
ARTICLE_BODY = (
    rf"第(?P<article>{NUMERAL})条(?:之(?P<article_suffix>{NUMERAL}))?"
    rf"(?P<paragraph>第{NUMERAL}款)?"
    rf"(?P<item>第{PAREN_NUMERAL}项)?"
    rf"(?P<sub_item>第{PAREN_NUMERAL}目)?"
)
BARE_STATUTE_TOKEN = (
    r"(?:"
    r"民法典"
    r"|刑法"
    r"|宪法"
    r")"
)
CONTINUATION_BODY = (
    rf"(?:"
    rf"第{NUMERAL}条(?:之{NUMERAL})?(?:第{NUMERAL}款)?(?:第{PAREN_NUMERAL}项)?(?:第{PAREN_NUMERAL}目)?"
    rf"|第{NUMERAL}款(?:第{PAREN_NUMERAL}项)?(?:第{PAREN_NUMERAL}目)?"
    rf"|{OMIT_PAREN_NUMERAL}项(?:第{PAREN_NUMERAL}目)?"
    rf"|第{PAREN_NUMERAL}目"
    rf")"
)
CONNECTOR = r"(?:或者|以及|、|，|和|及|或)"
RANGE_CONNECTOR = r"(?:至|-|—)"
REFERENCE_ANCHOR_RE = re.compile(
    r"(?:《[^》]+》|本法|本条例|本办法|本规定|本决定|本解释|第[一二三四五六七八九十百千万零两〇0-9（()）]+(?:条|款|项|目))"
)
REFERENCE_START_RE = re.compile(
    r"^(?:《[^》]+》|[^\s，,。；;：:]{1,24}(?:法|法典|条例|规定|办法|解释)|本法|本条例|本办法|本规定|本决定|本解释|第[一二三四五六七八九十百千万零两〇0-9（()）]+(?:条|款|项|目))"
)
SHARED_ITEM_GROUP_RE = re.compile(
    r"第(?:[（(][一二三四五六七八九十百千万零两〇0-9]+[）)]|[一二三四五六七八九十百千万零两〇0-9]+)"
    r"(?:"
    r"(?:或者|以及|、|，|和|及|或)"
    r"(?:第)?(?:[（(][一二三四五六七八九十百千万零两〇0-9]+[）)]|[一二三四五六七八九十百千万零两〇0-9]+)"
    r")+项"
)
PARALLEL_ITEM_GROUP_RE = re.compile(
    rf"(?P<prefix>(?:(?:第{NUMERAL}条(?:之{NUMERAL})?(?:第{NUMERAL}款)?|第{NUMERAL}款))?)"
    rf"(?P<first>第?(?:{PAREN_NUMERAL}))"
    rf"(?P<rest>(?:[、，](?:第)?(?:{PAREN_NUMERAL}))+)"
    r"项"
)
SHARED_DOCUMENT_GAP_RE = re.compile(
    r"^\s*(?:"
    r"[“\"‘][^”\"’]{0,80}[”\"’]"
    r"|（[^）]{0,40}）"
    r")*\s*(?:或者|以及|、|，|和|及|或)?\s*$"
)
NON_TARGET_PAREN_RE = re.compile(
    r"(?:（(?![一二三四五六七八九十百千万零两〇0-9]+）)[^）]{1,40}）|\((?![一二三四五六七八九十百千万零两〇0-9]+\))[^)]{1,40}\))"
)
CONTINUATION_TARGET_RE = re.compile(rf"(?:{CONTINUATION_BODY})")

ABSOLUTE_ARTICLE_RE = re.compile(
    rf"(?P<prefix>依照|根据|按照|参照)?《(?P<title>[^》]+)》(?P<ref>{ARTICLE_BODY})"
)
SHARED_DOCUMENT_ARTICLE_RE = re.compile(
    rf"(?P<ref>{ARTICLE_BODY})(?P<tail>(?:(?:{CONNECTOR}|{RANGE_CONNECTOR})\s*{CONTINUATION_BODY})*)"
)
BARE_ARTICLE_RE = re.compile(
    rf"(?P<prefix>依照|根据|按照|参照)?(?P<title>{BARE_STATUTE_TOKEN})(?P<ref>{ARTICLE_BODY})"
)
LOCAL_ARTICLE_RE = re.compile(
    rf"(?P<doc_token>本法|本条例|本办法|本规定|本决定|本解释)(?P<ref>{ARTICLE_BODY})"
)
RELATIVE_ARTICLE_DETAIL_RE = re.compile(
    rf"本条(?!例|文|法|规|办法|规定|决定|解释|例第)(?P<ref>(?:第{NUMERAL}款)?(?:第{PAREN_NUMERAL}项)?(?:第{PAREN_NUMERAL}目)?)"
    rf"(?P<tail>(?:(?:{CONNECTOR}|{RANGE_CONNECTOR})\s*{CONTINUATION_BODY})*)"
)
RELATIVE_PARAGRAPH_DETAIL_RE = re.compile(
    rf"(本款|前款)(?P<ref>(?:第{PAREN_NUMERAL}项)?(?:第{PAREN_NUMERAL}目)?)"
    rf"(?P<tail>(?:(?:{CONNECTOR}|{RANGE_CONNECTOR})\s*{CONTINUATION_BODY})*)"
)
PREVIOUS_MULTI_RE = re.compile(
    rf"前(?P<count>{NUMERAL})(?P<unit>款|项)"
)
STANDALONE_DETAIL_RE = re.compile(
    rf"(?<!第)(?P<ref>"
    rf"第{NUMERAL}款(?:第{PAREN_NUMERAL}项)?(?:第{PAREN_NUMERAL}目)?"
    rf"|{OMIT_PAREN_NUMERAL}项(?:第{PAREN_NUMERAL}目)?"
    rf"|第{PAREN_NUMERAL}目"
    rf")(?P<tail>(?:(?:{CONNECTOR}|{RANGE_CONNECTOR})\s*{CONTINUATION_BODY})*)"
)
CONTINUATION_REF_RE = re.compile(
    rf"(?:{CONNECTOR}|{RANGE_CONNECTOR})\s*(?P<ref>{CONTINUATION_BODY})"
)
THIS_ARTICLE_RE = re.compile(r"本条(?!例|款|项|目|文|法|规|办|决定|解释|例第)")
THIS_PARAGRAPH_RE = re.compile(r"本款(?!项|目)")
PREVIOUS_PARAGRAPH_RE = re.compile(r"前款")
SENTENCE_SPLIT_RE = re.compile(r"[。；;]\s*")
ALIAS_ARTICLE_TEMPLATE = (
    r"(?P<alias>{alias})(?P<ref>"
    rf"{ARTICLE_BODY}"
    r")"
)
