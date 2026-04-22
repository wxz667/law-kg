from __future__ import annotations

import asyncio
import re
from typing import Any

from .ai_keyword_extractor import get_ai_keyword_extractor
from .ai_recommender import build_ai_recommender_from_env
from .neo4j_store import run_query
from .provision_lookup import lookup_provision_by_law_and_article
from .recommendation_cache import get_recommendation_cache
from .rrf_fusion import apply_cited_law_penalty, fuse_with_rrf
from kg_api.config import settings


_SMART_SEMAPHORE = asyncio.Semaphore(3)


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "")


def _extract_cited_laws(text: str) -> list[str]:
    cited: set[str] = set()
    t = text or ""

    for m in re.findall(r"《([^《》]{2,50})》", t):
        s = str(m).strip()
        if s:
            cited.add(s)

    for m in re.findall(r"(?:依照|根据|依据|按照|适用)\s*《?([^《》\s]{2,30})》?", t):
        s = str(m).strip()
        if s:
            cited.add(s)

    noise = {"判决书", "起诉书", "裁定书", "决定书", "通知书"}
    out = [x for x in cited if not any(n in x for n in noise)]
    return sorted(out)


def _filter_ai_keywords(keywords: list[str]) -> list[str]:
    if not keywords:
        return []
    stop = {
        "本院认为",
        "经审理查明",
        "依法",
        "依照",
        "依据",
        "根据",
        "相关规定",
        "有关规定",
        "判决如下",
        "现已审理终结",
        "事实清楚",
        "证据确实充分",
        "构成犯罪",
        "应予惩处",
    }
    out: list[str] = []
    seen = set()
    for kw in keywords:
        k = str(kw).strip()
        if not k or k in seen:
            continue
        seen.add(k)
        if len(k) < 2 or len(k) > 10:
            continue
        if k in stop:
            continue
        out.append(k)
    return out[:10]


def _keyword_weight(kw: str) -> float:
    k = str(kw or "").strip()
    if not k:
        return 0.0
    if k.endswith("罪"):
        return 2.0
    core = [
        "毒品",
        "诈骗",
        "盗窃",
        "抢劫",
        "伤害",
        "非法经营",
        "贩卖",
        "运输",
        "走私",
        "赌博",
        "受贿",
        "行贿",
        "故意",
        "过失",
        "金融",
        "烟草",
        "卷烟",
        "专卖",
        "枪支",
        "爆炸",
        "网络",
        "洗钱",
    ]
    if any(x in k for x in core):
        return 1.5
    regulator = ["未经许可", "许可证", "许可", "专营", "专卖"]
    if any(x in k for x in regulator):
        return 1.1
    return 1.0


def _law_from_full_name(full_name: str) -> str:
    m = re.search(r"《([^》]{2,60})》", full_name or "")
    if not m:
        return ""
    return str(m.group(1)).strip()


def _infer_allowed_laws(text: str, case_type: str) -> list[str]:
    if (case_type or "").lower() != "criminal":
        return []
    allowed = ["中华人民共和国刑法", "中华人民共和国刑事诉讼法"]
    t = text or ""
    if any(x in t for x in ["烟草", "卷烟"]):
        allowed.append("中华人民共和国烟草专卖法")
    if "毒品" in t:
        allowed.append("中华人民共和国禁毒法")
    if any(x in t for x in ["交通", "驾驶", "机动车", "酒驾"]):
        allowed.append("中华人民共和国道路交通安全法")
    return allowed


def _apply_allowed_law_penalty(items: list[dict[str, Any]], allowed_laws: list[str], factor: float) -> list[dict[str, Any]]:
    if not items or not allowed_laws:
        return items
    out: list[dict[str, Any]] = []
    for it in items:
        name = str(it.get("full_name") or it.get("name") or "")
        law = _law_from_full_name(name)
        copied = dict(it)
        if law and all(a not in law for a in allowed_laws):
            copied["score"] = float(copied.get("score") or 0.0) * float(factor)
        out.append(copied)
    return out


def _neo4j_recommend_by_keywords(keywords: list[str], limit: int) -> list[dict[str, Any]]:
    if not keywords:
        return []
    conds = []
    params: dict[str, Any] = {"limit": int(limit)}
    for i, kw in enumerate(keywords[:12]):
        key = f"kw{i}"
        conds.append(
            f"(toLower(n.name) CONTAINS toLower(${key}) OR toLower(n.text) CONTAINS toLower(${key}) OR toLower(coalesce(n.full_name,'')) CONTAINS toLower(${key}))"
        )
        params[key] = kw
    where = " OR ".join(conds) if conds else "false"
    query = f"""
    MATCH (n:Node)
    WHERE n.type = 'ProvisionNode' AND ({where})
    OPTIONAL MATCH (law:Node {{type:'DocumentNode'}})-[:CONTAINS*]->(n)
    RETURN n, law.name AS law_name
    LIMIT $limit
    """
    rows = run_query(query, params)
    out: list[dict[str, Any]] = []
    for r in rows:
        node = r.get("n")
        if node is None:
            continue
        node_id = str(getattr(node, "element_id", "") or "")
        if not node_id:
            continue
        law_name = str(r.get("law_name") or "")
        name = str(node.get("name", "") or "")
        text = str(node.get("text", "") or "")
        full_name = (f"《{law_name}》{name}" if law_name else name) or name or node_id
        out.append({"provision_id": node_id, "full_name": full_name, "text": text})
    return out[: int(limit)]


def _mechanical_keywords(text: str) -> list[str]:
    t = re.sub(r"\s+", " ", text or "")
    kws = re.findall(r"[\u4e00-\u9fff]{2,8}", t)
    out: list[str] = []
    seen = set()
    for kw in kws:
        if kw in seen:
            continue
        seen.add(kw)
        out.append(kw)
        if len(out) >= 12:
            break
    return out


def _mmr_diversify(items: list[dict[str, Any]], top_k: int, max_same_law: int = 2) -> list[dict[str, Any]]:
    if not items:
        return []
    selected: list[dict[str, Any]] = []
    law_streak = 0
    last_law = ""
    pool = list(items)
    while pool and len(selected) < top_k:
        pick_index = 0
        if last_law and law_streak >= max_same_law:
            for idx, it in enumerate(pool):
                law = _law_from_full_name(str(it.get("full_name") or it.get("name") or ""))
                if law and law != last_law:
                    pick_index = idx
                    break
        it = pool.pop(pick_index)
        law = _law_from_full_name(str(it.get("full_name") or it.get("name") or ""))
        if law and law == last_law:
            law_streak += 1
        else:
            last_law = law
            law_streak = 1 if law else 0
        selected.append(it)
    return selected


async def smart_recommend(
    content: str,
    case_type: str = "criminal",
    current_paragraph: str | None = None,
    top_k: int = 10,
) -> dict[str, Any]:
    async with _SMART_SEMAPHORE:
        text = _strip_html(content)
        k = max(1, int(top_k))

        cache = get_recommendation_cache(ttl_seconds=int(getattr(settings, "RECOMMENDATION_CACHE_TTL", 300)))
        cached = cache.get(text, case_type, current_paragraph, k)
        if cached is not None:
            meta = dict(cached.get("metadata") or {})
            meta["cache_hit"] = True
            cached["metadata"] = meta
            return cached

        cited_laws = _extract_cited_laws(text)
        allowed_laws = _infer_allowed_laws(text, case_type)

        ai_extractor = get_ai_keyword_extractor()
        ai_keywords: list[str] = []
        if ai_extractor.enabled:
            try:
                ai_keywords = await asyncio.wait_for(
                    ai_extractor.extract_keywords(document_text=text[:800], case_type=case_type, max_keywords=10),
                    timeout=35,
                )
            except Exception:
                ai_keywords = []
        ai_keywords = _filter_ai_keywords(ai_keywords)

        ai_keyword_recs: list[dict[str, Any]] = []
        if ai_keywords:
            ai_keyword_recs = await asyncio.to_thread(_neo4j_recommend_by_keywords, ai_keywords, min(18, max(12, k * 2)))
            for item in ai_keyword_recs:
                name = str(item.get("full_name") or "")
                body = str(item.get("text") or "")
                match_score = 0.0
                for kw in ai_keywords:
                    if kw and (kw in name or kw in body):
                        match_score += _keyword_weight(kw)
                item["score"] = float(match_score)
                item["source"] = "ai_keyword"
                item["reason"] = "AI关键词匹配"
            if allowed_laws:
                ai_keyword_recs = _apply_allowed_law_penalty(ai_keyword_recs, allowed_laws, factor=0.15)
            ai_keyword_recs.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
            filtered = [x for x in ai_keyword_recs if float(x.get("score") or 0.0) >= 2.0]
            if len(filtered) >= max(5, min(8, k)):
                ai_keyword_recs = filtered

        mech_keywords = _mechanical_keywords(text)
        keyword_recs = await asyncio.to_thread(_neo4j_recommend_by_keywords, mech_keywords, min(18, max(10, k * 2)))
        for item in keyword_recs:
            item["score"] = 1.0
            item["source"] = "keyword"
            item["reason"] = "内容相关"
        if allowed_laws:
            keyword_recs = _apply_allowed_law_penalty(keyword_recs, allowed_laws, factor=0.4)

        ai_engine = build_ai_recommender_from_env()
        ai_recs: list[dict[str, Any]] = []
        if ai_engine.enabled:
            try:
                ai_raw = await ai_engine.recommend(
                    document_text=text[:800],
                    case_type=case_type,
                    current_paragraph=current_paragraph,
                    top_k=5,
                    cited_laws=cited_laws,
                )
                for r in ai_raw:
                    law_name = str(r.get("law_name") or "").strip()
                    article = str(r.get("article") or "").strip()
                    if not law_name or not article:
                        continue
                    hit = await asyncio.to_thread(lookup_provision_by_law_and_article, law_name, article)
                    if not hit:
                        ai_recs.append(
                            {
                                "provision_id": f"ai::{law_name}::{article}",
                                "full_name": f"《{law_name}》{article}",
                                "text": "",
                                "score": float(r.get("score") or 80),
                                "source": "ai",
                                "reason": str(r.get("reason") or "AI推荐（当前数据库未命中对应法条）"),
                                "insert_modes": [],
                            }
                        )
                    else:
                        ai_recs.append(
                            {
                                "provision_id": hit["provision_id"],
                                "full_name": hit["full_name"],
                                "text": hit["text"],
                                "score": float(r.get("score") or 80),
                                "source": "ai",
                                "reason": str(r.get("reason") or "AI推荐"),
                                "insert_modes": ["cursor", "appendix"],
                            }
                        )
            except Exception:
                ai_recs = []

        ranked_lists: list[list[dict[str, Any]]] = []
        rrf_weights: list[float] = []
        if ai_recs:
            ranked_lists.append(sorted(ai_recs, key=lambda x: float(x.get("score") or 0.0), reverse=True))
            rrf_weights.append(2.2)
        if ai_keyword_recs:
            ranked_lists.append(ai_keyword_recs)
            rrf_weights.append(1.2)
        if keyword_recs:
            ranked_lists.append(sorted(keyword_recs, key=lambda x: float(x.get("score") or 0.0), reverse=True))
            rrf_weights.append(0.6)

        merged = fuse_with_rrf(ranked_lists, id_field="provision_id", k=20, weights=rrf_weights) if ranked_lists else []
        merged = apply_cited_law_penalty(merged, cited_laws, law_name_field="full_name", penalty_factor=0.7)
        merged = _mmr_diversify(merged, top_k=min(len(merged), k), max_same_law=2)

        recommendations: list[dict[str, Any]] = []
        for r in merged[:k]:
            pid = str(r.get("provision_id") or r.get("id") or "").strip()
            if not pid:
                continue
            sources = r.get("sources")
            src = ""
            if isinstance(sources, list) and sources:
                if "ai" in sources:
                    src = "ai"
                else:
                    src = str(sources[0] or "")
            else:
                src = str(r.get("source") or "keyword")
            recommendations.append(
                {
                    "provision_id": pid,
                    "full_name": r.get("full_name") or r.get("name") or pid,
                    "article_name": r.get("article_name") or "",
                    "text": r.get("text") or "",
                    "score": float(r.get("rrf_score") or 0.0),
                    "source": src or "keyword",
                    "reason": r.get("reason") or "内容相关",
                    "insert_modes": r.get("insert_modes") if isinstance(r.get("insert_modes"), list) else ["cursor", "appendix"],
                }
            )

        ai_count = sum(1 for x in recommendations if x.get("source") == "ai")
        keyword_count = sum(1 for x in recommendations if x.get("source") in {"keyword", "ai_keyword"})
        result = {
            "recommendations": recommendations[:k],
            "metadata": {
                "total": len(recommendations[:k]),
                "ai_count": ai_count,
                "rule_count": 0,
                "keyword_count": keyword_count,
                "cache_hit": False,
            },
        }
        cache.set(text, case_type, current_paragraph, k, result)
        return result
