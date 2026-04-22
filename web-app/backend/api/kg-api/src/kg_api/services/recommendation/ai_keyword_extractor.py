from __future__ import annotations

import asyncio
import json
import re
from typing import Any
from urllib.request import Request, urlopen

_KW_SEMAPHORE = asyncio.Semaphore(2)


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


class AiKeywordExtractor:
    def __init__(self, api_key: str, base_url: str, model: str):
        self._api_key = (api_key or "").strip()
        self._base_url = (base_url or "").strip().rstrip("/")
        self._model = (model or "").strip()

    @property
    def enabled(self) -> bool:
        return bool(self._api_key)

    async def extract_keywords(self, document_text: str, case_type: str = "criminal", max_keywords: int = 10) -> list[str]:
        if not self.enabled:
            return []
        text = (document_text or "").strip()
        if not text:
            return []

        k = max(3, min(12, int(max_keywords)))

        system = (
            "你是专业的中国法律助手。\n"
            "任务：从文书内容中提取用于检索法条的关键词。\n"
            "要求：\n"
            "- 必须输出纯JSON，不要任何解释\n"
            "- 输出keywords数组，长度不超过10\n"
            "- 关键词要具体、可检索：优先罪名、行为方式、对象、领域词（如毒品、卷烟、网络诈骗等）\n"
            "- 避免输出套话与程序性表述（如本院认为、经审理查明、依法、依照、相关规定）\n"
        )
        if (case_type or "").lower() == "criminal":
            system += "刑事案件：优先输出罪名与犯罪要件相关词。\n"

        user = (
            f"请从以下{case_type}案件文书内容中提取不超过{k}个检索关键词：\n\n"
            f"{text[:800]}\n\n"
            "{\n"
            "  \"keywords\": [\"关键词1\",\"关键词2\"]\n"
            "}"
        )

        url = f"{self._base_url}/chat/completions"
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self._api_key}"}
        payload = {
            "model": self._model,
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
            "temperature": 0.2,
        }

        try:
            async with _KW_SEMAPHORE:
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
        kws = parsed.get("keywords")
        if not isinstance(kws, list):
            return []

        out: list[str] = []
        seen = set()
        for kw in kws:
            s = str(kw).strip()
            if not s or s in seen:
                continue
            seen.add(s)
            if len(s) < 2 or len(s) > 10:
                continue
            out.append(s)
            if len(out) >= k:
                break
        return out


_global_extractor: AiKeywordExtractor | None = None


def get_ai_keyword_extractor() -> AiKeywordExtractor:
    global _global_extractor
    if _global_extractor is None:
        from kg_api.config import settings

        api_key = getattr(settings, "QWEN_API_KEY", "")
        base_url = getattr(settings, "QWEN_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
        model = getattr(settings, "QWEN_MODEL", "qwen-plus")
        _global_extractor = AiKeywordExtractor(api_key=api_key, base_url=base_url, model=model)
    return _global_extractor
