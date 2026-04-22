from __future__ import annotations

import asyncio
import json
import re
from typing import Any
from urllib.request import Request, urlopen

from kg_api.config import settings


_AI_SEMAPHORE = asyncio.Semaphore(2)


def _extract_json_object(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None


async def _post_json(url: str, headers: dict[str, str], payload: dict[str, Any], timeout_seconds: int) -> dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    def _sync() -> dict[str, Any]:
        req = Request(url=url, data=body, method="POST")
        for k, v in headers.items():
            req.add_header(k, v)
        with urlopen(req, timeout=timeout_seconds) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        return json.loads(raw)

    return await asyncio.to_thread(_sync)


class QwenAiRecommender:
    def __init__(self, api_key: str, base_url: str, model: str):
        self._api_key = (api_key or "").strip()
        self._base_url = (base_url or "").strip().rstrip("/")
        self._model = (model or "").strip()

    @property
    def enabled(self) -> bool:
        return bool(self._api_key)

    async def recommend(
        self,
        document_text: str,
        case_type: str = "criminal",
        current_paragraph: str | None = None,
        top_k: int = 5,
        cited_laws: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        if not (document_text or "").strip():
            return []

        k = max(1, min(5, int(top_k)))
        doc_excerpt = (document_text or "")[:800]
        para_excerpt = ((current_paragraph or "").strip())[:400]

        allowed_laws = []
        if (case_type or "").lower() == "criminal":
            allowed_laws = ["中华人民共和国刑法", "中华人民共和国刑事诉讼法"]

        system = (
            "你是专业的中国法律助手，擅长分析案件文书并推荐适用的法条。\n"
            "输出要求：\n"
            "- 必须输出纯JSON格式，不要包含任何其他文字\n"
            "- 最多推荐5条法条，按相关度从高到低排序\n"
            "- 每条必须包含：law_name、article、score、reason\n"
            "- article必须是“第…条/…条”格式\n"
            "- score范围：0-100（整数）\n"
        )
        if allowed_laws:
            system += "对刑事案件：law_name必须从以下列表中选择：\n" + "\n".join([f"- {x}" for x in allowed_laws]) + "\n"
        if cited_laws:
            quoted = "\n".join([f"- 《{x}》" for x in cited_laws if str(x).strip()])
            if quoted:
                system += (
                    "\n以下法条已在文书中被引用过，请降低它们的推荐优先级：\n"
                    f"{quoted}\n"
                    "对已引用法条：score较同等未引用法条降低20-30分。\n"
                )

        user = f"请分析以下{case_type}案件文书，推荐适用的法条：\n\n【文书内容】\n{doc_excerpt}\n\n"
        if para_excerpt:
            user += f"【当前段落】\n{para_excerpt}\n\n"
        user += (
            "{\n"
            "  \"crime_name\": \"识别出的罪名\",\n"
            "  \"recommendations\": [\n"
            "    {\"law_name\":\"法律全称\",\"article\":\"条文号\",\"score\":95,\"reason\":\"不少于20字的具体理由\"}\n"
            "  ]\n"
            "}\n"
            f"recommendations数量不超过{k}。"
        )

        url = f"{self._base_url}/chat/completions"
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self._api_key}"}
        payload = {
            "model": self._model,
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
            "temperature": 0.2,
        }

        try:
            async with _AI_SEMAPHORE:
                resp = await asyncio.wait_for(
                    _post_json(url, headers, payload, timeout_seconds=35),
                    timeout=35,
                )
        except Exception:
            return []

        content = ""
        try:
            content = resp["choices"][0]["message"]["content"]
        except Exception:
            content = ""

        parsed = _extract_json_object(content)
        if not parsed:
            return []
        recs = parsed.get("recommendations")
        if not isinstance(recs, list):
            return []

        out: list[dict[str, Any]] = []
        for item in recs[:k]:
            if not isinstance(item, dict):
                continue
            law_name = str(item.get("law_name") or "").strip()
            article = str(item.get("article") or "").strip()
            if not law_name or not article:
                continue
            score = item.get("score")
            try:
                score_num = int(float(score))
            except Exception:
                score_num = 0
            reason = str(item.get("reason") or "").strip()
            out.append({"law_name": law_name, "article": article, "score": max(0, min(100, score_num)), "reason": reason, "source": "ai"})
        return out


def build_ai_recommender_from_env() -> QwenAiRecommender:
    provider = (getattr(settings, "AI_PROVIDER", "qwen") or "qwen").lower()
    if provider != "qwen":
        return QwenAiRecommender(api_key="", base_url="https://dashscope.aliyuncs.com/compatible-mode/v1", model="qwen-plus")
    api_key = getattr(settings, "QWEN_API_KEY", "")
    base_url = getattr(settings, "QWEN_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
    model = getattr(settings, "QWEN_MODEL", "qwen-plus")
    return QwenAiRecommender(api_key=api_key, base_url=base_url, model=model)

