from __future__ import annotations

import re
from typing import Any

from .neo4j_store import run_query


_ARTICLE_TOKEN_RE = re.compile(r"(第?\s*[一二三四五六七八九十百千万零〇两\d]{1,12}\s*条(?:之\s*[一二三四五六七八九十百千万零〇两\d]{1,3})?)")
_ARABIC_RE = re.compile(r"^\s*(?:第)?\s*(\d{1,5})\s*条(?:之\s*(\d{1,2}))?\s*$")


def _int_to_cn(n: int) -> str:
    digits = "零一二三四五六七八九"
    units = ["", "十", "百", "千", "万"]
    if n == 0:
        return digits[0]
    s = str(n)
    out: list[str] = []
    length = len(s)
    for i, ch in enumerate(s):
        d = int(ch)
        pos = length - i - 1
        if d == 0:
            if out and out[-1] != digits[0] and pos != 0:
                out.append(digits[0])
            continue
        out.append(digits[d] + units[pos])
    joined = "".join(out).rstrip(digits[0])
    joined = joined.replace("一十", "十")
    return joined


def normalize_article(article: str) -> str:
    a = (article or "").strip()
    if not a:
        return ""
    m = _ARTICLE_TOKEN_RE.search(a)
    if m:
        a = m.group(1).replace(" ", "")
    m2 = _ARABIC_RE.match(a)
    if m2:
        main = int(m2.group(1))
        sub = m2.group(2)
        main_cn = _int_to_cn(main)
        if sub:
            sub_cn = _int_to_cn(int(sub))
            return f"第{main_cn}条之{sub_cn}"
        return f"第{main_cn}条"
    if "条" in a and not a.startswith("第"):
        return f"第{a}"
    return a


def lookup_provision_by_law_and_article(law_name: str, article: str) -> dict[str, Any] | None:
    ln = (law_name or "").strip().replace("《", "").replace("》", "")
    art = normalize_article(article)
    if not ln or not art:
        return None

    query = """
    MATCH (law:Node {type:'DocumentNode'})-[:CONTAINS*]->(n:Node {type:'ProvisionNode'})
    WHERE toLower(law.name) CONTAINS toLower($law_name)
      AND (
        toLower(n.name) CONTAINS toLower($article)
        OR toLower(coalesce(n.full_name, '')) CONTAINS toLower($article)
      )
    RETURN n, law.name AS law_name
    LIMIT 1
    """
    rows = run_query(query, {"law_name": ln, "article": art})
    if not rows:
        return None
    node = rows[0].get("n")
    if node is None:
        return None
    law = rows[0].get("law_name") or ""
    node_id = str(getattr(node, "element_id", "") or "")
    name = str(node.get("name", "") or "")
    text = str(node.get("text", "") or "")
    full_name = (f"《{law}》{name}" if law else name) or name or node_id
    return {"provision_id": node_id, "full_name": full_name, "text": text}

