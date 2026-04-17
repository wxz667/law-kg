from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from utils.llm.base import (
    ProviderRequestConfig,
    ProviderResponseError,
    build_provider_request_config,
    validate_provider_api_params,
)


PROMPT = """你是法律知识图谱中的显式关系仲裁模型。

任务：对每个样本判断 `[T]...[/T]` 标记目标在当前文本中是被解释对象，还是仅被普通引用。

输出：
- `true`：应构建 `INTERPRETS`
- `false`：应构建 `REFERENCES`

判 `true`：
- 当前文本在释明 `[T]` 中的概念、术语、范围、含义、认定标准、包括情形、排除条件或适用边界
- 即使是否定式，只要是在说明何种情形“认定为 / 不认定为 / 属于 / 不属于 / 视为 / 不视为” `[T]` 规定的某概念，也判 `true`
- 立法解释标题、司法解释标题、实施条例/实施细则/实施办法等，对上位规范条款作具体化说明，也可能判 `true`

判 `false`：
- 当前文本只是把 `[T]` 当作依据条款、程序规则、处罚依据、条件来源、数量门槛、比较基准或参照标准
- “依照 / 根据 / 按照 / 参照 / 适用 / 依该条处理”通常是 `false`
- 当前文本定义的是来源文件自身概念，只是借 `[T]` 作为定义依据，例如“本法所称……是指 `[T]` 规定的……”，判 `false`

注意：
- 每个样本都已经是小模型低置信样本，你要直接给最终裁决，不要受小模型预测牵制
- 不要因为同时出现“本解释第三条”“前款”等其他引用，就忽略其对 `[T]` 的解释关系
- `[T]` 可能是一个引用组，仍按整体判断
- 请结合 `source_category`、`target_categories`、`is_legislative_interpretation` 一起判断
- `reason` 必须是简短中文理由，不能留空

示例：
- `[T]刑法第一百九十六条第一款第三项[/T]所称“冒用他人信用卡”，包括以下情形：` -> `true`
- `有下列情形之一的，应当认定为[T]刑法第一百七十七条之一第一款[/T]规定的“数量巨大”：` -> `true`
- `根据[T]《中华人民共和国行政诉讼法》第九十条[/T]的规定` -> `false`
- `补选程序参照[T]本法第十八条[/T]的规定办理` -> `false`
- `本法所称公职人员，是指[T]《中华人民共和国监察法》第十五条[/T]规定的人员` -> `false`

只返回严格 JSON：
{"items":[{"sample_id":"...","label":"true","reason":"简短中文理由"}]}
"""


@dataclass(frozen=True)
class RelationConflictDecision:
    is_interprets: bool
    score: float
    model: str
    reason: str = ""


@dataclass(frozen=True)
class RelationConflictRuntimeConfig:
    provider: str
    model: str
    batch_size: int
    concurrent_requests: int
    request_timeout_seconds: int
    max_retries: int
    params: dict[str, Any]
    rate_limit: dict[str, Any]


def resolve_llm_conflict_runtime_config(runtime: Any, *, default_batch_size: int = 1) -> RelationConflictRuntimeConfig:
    classify_config = runtime.classify_config()
    raw = dict(classify_config.get("llm_conflict", {}))
    provider = str(raw.get("provider", "")).strip()
    model = str(raw.get("model", "")).strip()
    if not provider or not model:
        raise ValueError("builder.classify.llm_conflict must define non-empty provider and model.")

    batch_size = max(int(raw.get("batch_size", default_batch_size or 1)), 1)
    concurrent_requests = max(int(raw.get("concurrent_requests", 1)), 1)
    request_timeout_seconds = max(int(raw.get("request_timeout_seconds", 60)), 1)
    max_retries = max(int(raw.get("max_retries", 2)), 1)
    params = validate_provider_api_params(raw.get("params", {}))
    params.setdefault("temperature", 0.0)
    params.setdefault("max_tokens", 512)
    max_tokens = max(int(params.get("max_tokens", 512)), 1)
    minimum_required_tokens = 48 + (batch_size * 64)
    if max_tokens < minimum_required_tokens:
        raise ValueError(
            "builder.classify.llm_conflict params are inconsistent: "
            f"batch_size={batch_size} requires max_tokens>={minimum_required_tokens}, got {max_tokens}."
        )
    return RelationConflictRuntimeConfig(
        provider=provider,
        model=model,
        batch_size=batch_size,
        concurrent_requests=concurrent_requests,
        request_timeout_seconds=request_timeout_seconds,
        max_retries=max_retries,
        params=params,
        rate_limit=dict(raw.get("rate_limit", {})) if isinstance(raw.get("rate_limit", {}), dict) else {},
    )


def build_request_config(config: RelationConflictRuntimeConfig) -> ProviderRequestConfig:
    return build_provider_request_config(
        provider=config.provider,
        model=config.model,
        params=config.params,
        timeout_seconds=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit=config.rate_limit,
    )


def judge_relation_conflicts(
    runtime: Any,
    payloads: list[dict[str, Any]],
    *,
    default_batch_size: int = 1,
) -> list[RelationConflictDecision]:
    if not payloads:
        return []
    runtime_config = resolve_llm_conflict_runtime_config(runtime, default_batch_size=default_batch_size)
    request_config = build_request_config(runtime_config)
    prompt = build_relation_conflict_prompt(payloads)
    errors: list[Exception] = []
    for _attempt in range(runtime_config.max_retries):
        try:
            raw = runtime.generate_text(prompt, request_config)
            return parse_relation_conflict_response(raw, payloads, request_config.model)
        except (ProviderResponseError, ValueError, json.JSONDecodeError) as exc:
            errors.append(exc)
            continue
    if errors:
        raise errors[-1]
    raise ValueError("Failed to judge relation conflicts: empty retry loop.")


def build_relation_conflict_prompt(payloads: list[dict[str, Any]]) -> list[dict[str, str]]:
    compact_payload = [
        {
            "sample_id": str(payload.get("sample_id", "")),
            "text": str(payload.get("text", "")),
            "source_category": str(payload.get("source_category", "")),
            "target_categories": [str(value) for value in payload.get("target_categories", [])],
            "is_legislative_interpretation": bool(payload.get("is_legislative_interpretation", False)),
            "model_prediction": {
                "is_interprets": bool(payload.get("model_is_interprets", False)),
                "score": float(payload.get("model_score", 0.0)),
            },
        }
        for payload in payloads
    ]
    user = (
        "请按输入顺序逐条裁决以下低置信样本，并按相同顺序返回 sample_id、label、reason。"
        "不要遗漏项目，不要修改 sample_id，reason 不能为空：\n"
        + json.dumps(compact_payload, ensure_ascii=False)
    )
    return [{"role": "system", "content": PROMPT}, {"role": "user", "content": user}]


def parse_relation_conflict_response(
    raw: str,
    payloads: list[dict[str, Any]],
    model_name: str,
) -> list[RelationConflictDecision]:
    content = strip_markdown_fence(raw)
    payload = json.loads(content)
    items = payload if isinstance(payload, list) else payload.get("items", [])
    if not isinstance(items, list):
        raise ValueError("Relation conflict response must contain a list of items.")
    if len(items) != len(payloads):
        raise ValueError(
            f"Relation conflict response item count mismatch: expected {len(payloads)}, got {len(items)}"
        )

    expected_ids = [str(item.get("sample_id", "")).strip() for item in payloads]
    decisions_by_id: dict[str, RelationConflictDecision] = {}
    for item in items:
        sample_id = str(item.get("sample_id", "")).strip()
        if not sample_id:
            raise ValueError("Relation conflict response item is missing sample_id.")
        if sample_id in decisions_by_id:
            raise ValueError(f"Duplicate sample_id in relation conflict response: {sample_id}")
        is_interprets = parse_relation_label(item)
        reason = normalize_reason(str(item.get("reason", "")).strip(), is_interprets)
        decisions_by_id[sample_id] = RelationConflictDecision(
            is_interprets=is_interprets,
            score=1.0,
            model=model_name,
            reason=reason,
        )

    missing_ids = [sample_id for sample_id in expected_ids if sample_id and sample_id not in decisions_by_id]
    if missing_ids:
        raise ValueError(f"Relation conflict response is missing sample_ids: {missing_ids[:5]}")
    return [decisions_by_id[sample_id] for sample_id in expected_ids]


def strip_markdown_fence(content: str) -> str:
    stripped = content.strip()
    if stripped.startswith("```"):
        lines = [line for line in stripped.splitlines() if not line.strip().startswith("```")]
        return "\n".join(lines).strip()
    return stripped


def parse_relation_label(item: dict[str, Any]) -> bool:
    if "label" in item:
        value = str(item.get("label", "")).strip().lower()
        if value in {"true", "1", "yes", "y", "interprets"}:
            return True
        if value in {"false", "0", "no", "n", "references"}:
            return False
        raise ValueError(f"Unsupported relation conflict label: {item.get('label')}")
    return bool(item.get("is_interprets", False))


def normalize_reason(reason: str, is_interprets: bool) -> str:
    if reason:
        return reason
    if is_interprets:
        return "LLM判定当前文本在释明目标条款的概念、范围或认定标准。"
    return "LLM判定当前文本只是援引目标条款作为依据、条件或参照。"
