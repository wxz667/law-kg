from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from utils.llm.base import ProviderRequestConfig, ProviderResponseError
from utils.llm.factory import build_provider_client

from .config import canonical_label, resolve_distill_runtime_config


PROMPT = """你是规范性文件解释关系教师模型。

任务：看 `[T]...[/T]` 标记的目标引用，判断当前规范性文件文本是否在解释它，只做 true / false 二分类。

判 true：
- 条文在释明 `[T]` 中概念、术语、范围、含义或适用边界
- 条文在给 `[T]` 中概念提供认定标准、包括情形、排除条件、具体化标准
- 即使是否定式表达，只要是在说明何种情形“认定为 / 不认定为 / 属于 / 不属于 / 视为 / 不视为” `[T]` 规定的某个概念，也判 true
- 立法解释标题、司法解释标题、实施条例/实施细则/实施办法等对上位规范条款作具体化说明，也可能判 true

判 false：
- 只是把 `[T]` 当作依据条款、程序规则、处罚依据、审查标准、例外条件
- “依照 / 根据 / 按照 / 参照 / 适用 / 依该条处理”通常是 false
- 如果当前条文只是把 `[T]` 当作比较基准、数量门槛、参照标准，用来推出另一概念或另一条款的认定结果，也判 false
- 如果当前条文是在定义“本法 / 本条例 / 本规定”自己的概念，只是借 `[T]` 作为定义来源，例如“本法所称……是指 `[T]` 规定的……”，也判 false

注意：
- “所称 / 是指 / 系指”不是充分条件，必须确认被解释对象就是 `[T]`
- 不要因为句中同时出现“本解释第三条”“前款”等其他引用，就忽略其对 `[T]` 的解释关系
- `[T]` 可能是一个引用组，仍按一个整体判断
- 如果当前条文最终是在说某情形应当“认定为 / 不认定为 / 属于 / 不属于 / 视为 / 不视为” `[T]` 规定的某概念，即使前半句还引用其他条款或标准作为门槛，仍判 true
- 不要因为来源文件是法律或法规，就默认判 false

示例：
- `[T]刑法第一百九十六条第一款第三项[/T]所称“冒用他人信用卡”，包括以下情形：` -> true
- `有下列情形之一的，应当认定为[T]刑法第一百七十七条之一第一款[/T]规定的“数量巨大”：` -> true
- `符合本解释第三条规定情形的，可以不认定为[T]刑法第三百条第一款[/T]规定的“情节特别严重”：` -> true
- `诈骗数额接近本解释第一条规定的“数额巨大”标准，并具有前款规定情形之一的，应当认定为[T]刑法第二百六十六条[/T]规定的“其他严重情节”：` -> true
- `诈骗数额接近[T]本解释第一条[/T]规定的“数额巨大”标准，并具有前款规定情形之一的，应当认定为刑法第二百六十六条规定的“其他严重情节”：` -> false
- `全国人民代表大会常务委员会关于[T]《中华人民共和国刑事诉讼法》第二百九十二条[/T]的解释` -> true
- `根据[T]《中华人民共和国行政诉讼法》第九十条、《最高人民法院关于办理行政申请再审案件若干问题的规定》第三条[/T]的规定` -> false
- `地方人民法院可以管辖，但[T]本规定第一条第一款第二项[/T]规定的案件除外` -> false
- `补选程序参照[T]本法第十八条[/T]的规定办理` -> false
- `本法所称公职人员，是指[T]《中华人民共和国监察法》第十五条[/T]规定的人员` -> false

只返回 JSON：
{"items":[{"label":"true","reason":"简短中文理由"}]}
"""

EXPLICIT_TARGET_INTERPRET_PATTERNS = (
    r"(?:不)?属于<TARGET>规定的",
    r"(?:不)?认定为<TARGET>规定的",
    r"(?:不)?视为<TARGET>规定的",
    r"<TARGET>所称",
    r"<TARGET>规定的[“\"《][^”\"》]{1,30}[”\"》]",
)

SOURCE_TERM_DEFINED_BY_TARGET_PATTERNS = (
    r"(?:本法|本条例|本规定|本解释|本办法|本细则|本决定|本通则).{0,40}(?:所称|是指|系指).{0,80}<TARGET>(?:所)?规定的",
    r"(?:本法|本条例|本规定|本解释|本办法|本细则|本决定|本通则).{0,40}包括.{0,40}<TARGET>(?:所)?规定的",
)


def build_distill_messages(samples: list[dict[str, Any]]) -> list[dict[str, str]]:
    payload = [
        {
            "text": sample.get("text", ""),
            "source_category": sample.get("source_category", ""),
            "target_categories": sample.get("target_categories", []),
            "is_title_level_candidate": bool(sample.get("is_title_level_candidate", False)),
            "is_legislative_interpretation_source": bool(sample.get("is_legislative_interpretation_source", False)),
        }
        for sample in samples
    ]
    user_content = (
        "请按输入顺序判断以下样本是否在解释 [T] 标记目标，并按相同顺序返回 label、reason。"
        "不要返回 sample_id，不要改动顺序，不要遗漏项目：\n"
        + json.dumps(payload, ensure_ascii=False)
    )
    return [{"role": "system", "content": PROMPT}, {"role": "user", "content": user_content}]


def parse_distill_response(content: str, expected_count: int) -> list[tuple[str, str]]:
    content = content.strip()
    if content.startswith("```"):
        lines = [line for line in content.splitlines() if not line.strip().startswith("```")]
        content = "\n".join(lines).strip()
    payload = json.loads(content)
    items = payload if isinstance(payload, list) else payload.get("items", [])
    if not isinstance(items, list):
        raise ValueError("Distill response must contain a list of items.")
    if len(items) != expected_count:
        raise ValueError(f"Distill response item count mismatch: expected {expected_count}, got {len(items)}")
    parsed: list[tuple[str, str]] = []
    for item in items:
        label = canonical_label(item.get("label", False))
        reason = str(item.get("reason", "")).strip()
        parsed.append((label, reason))
    return parsed


def has_explicit_target_interpretation(marked_text: str) -> bool:
    compact = re.sub(r"\s+", "", str(marked_text or ""))
    compact = re.sub(r"\[T\].*?\[/T\]", "<TARGET>", compact)
    return any(re.search(pattern, compact) for pattern in EXPLICIT_TARGET_INTERPRET_PATTERNS)


def has_source_term_defined_by_target(marked_text: str) -> bool:
    compact = re.sub(r"\s+", "", str(marked_text or ""))
    compact = re.sub(r"\[T\].*?\[/T\]", "<TARGET>", compact)
    return any(re.search(pattern, compact) for pattern in SOURCE_TERM_DEFINED_BY_TARGET_PATTERNS)


def build_request_config(distill_config: dict[str, Any]) -> tuple[ProviderRequestConfig, int, int, int]:
    runtime = resolve_distill_runtime_config(distill_config)
    request_config = ProviderRequestConfig(
        provider=runtime.provider,
        model=runtime.model,
        params={**runtime.params, "timeout_seconds": runtime.request_timeout_seconds, "max_retries": runtime.max_retries},
    )
    return request_config, runtime.max_retries, runtime.batch_size, runtime.concurrent_requests


def distill_sample_batch(
    samples: list[dict[str, Any]],
    distill_config: dict[str, Any],
    *,
    cancel_event: Any | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    if not samples:
        return [], {"failed": 0, "succeeded": 0}
    if cancel_event is not None and getattr(cancel_event, "is_set", lambda: False)():
        raise KeyboardInterrupt

    request_config, max_retries, _batch_size, _concurrent_requests = build_request_config(distill_config)
    ordered_samples = [{**sample, "batch_index": index} for index, sample in enumerate(samples)]
    distilled_rows, failures = distill_batch(ordered_samples, request_config, max_retries, cancel_event=cancel_event)
    distilled_rows.sort(key=lambda row: row.get("batch_index", 0))
    for row in distilled_rows:
        row.pop("batch_index", None)
        row["teacher_model"] = request_config.model
    return distilled_rows, {"failed": failures, "succeeded": len(distilled_rows)}


def distill_samples(
    samples: list[dict[str, Any]],
    distill_config: dict[str, Any],
    progress_callback: Any | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    if not samples:
        return [], {"failed": 0, "succeeded": 0}

    request_config, max_retries, batch_size, concurrent_requests = build_request_config(distill_config)
    ordered_samples = [{**sample, "batch_index": index} for index, sample in enumerate(samples)]
    batches = [ordered_samples[index : index + batch_size] for index in range(0, len(ordered_samples), batch_size)]

    distilled_rows: list[dict[str, Any]] = []
    failures = 0
    with ThreadPoolExecutor(max_workers=concurrent_requests) as executor:
        future_to_batch = {executor.submit(distill_batch, batch, request_config, max_retries): batch for batch in batches}
        completed = 0
        for future in as_completed(future_to_batch):
            batch = future_to_batch[future]
            try:
                batch_rows, batch_failures = future.result()
            except Exception:
                batch_rows, batch_failures = [], len(batch)
            distilled_rows.extend(batch_rows)
            failures += batch_failures
            completed += len(batch)
            if progress_callback is not None:
                progress_callback(completed, len(samples))

    distilled_rows.sort(key=lambda row: row.get("batch_index", 0))
    for row in distilled_rows:
        row.pop("batch_index", None)
        row["teacher_model"] = request_config.model
    return distilled_rows, {"failed": failures, "succeeded": len(distilled_rows)}


def distill_batch(
    batch: list[dict[str, Any]],
    request_config: ProviderRequestConfig,
    max_retries: int,
    *,
    cancel_event: Any | None = None,
) -> tuple[list[dict[str, Any]], int]:
    client = build_provider_client(request_config)
    pending = list(batch)
    succeeded: list[dict[str, Any]] = []
    failures = 0

    for _attempt in range(max_retries):
        if cancel_event is not None and getattr(cancel_event, "is_set", lambda: False)():
            raise KeyboardInterrupt
        if not pending:
            break
        try:
            response_text = client.generate_text(build_distill_messages(pending), model=request_config.model)
            parsed = parse_distill_response(response_text, len(pending))
        except (ProviderResponseError, ValueError, json.JSONDecodeError):
            parsed = []
        if len(parsed) != len(pending):
            next_pending = list(pending)
        else:
            next_pending = []
        for sample, parsed_item in zip(pending, parsed, strict=False):
            label, reason = normalize_distilled_label(sample, *parsed_item)
            succeeded.append(
                {
                    **sample,
                    "label": label,
                    "teacher_reason": reason,
                    "batch_index": sample.get("batch_index", 0),
                }
            )
        pending = next_pending

    if pending:
        for sample in pending:
            fallback = distill_single_sample(sample, request_config, max_retries, cancel_event=cancel_event)
            if fallback is None:
                failures += 1
                continue
            succeeded.append(fallback)
    return succeeded, failures


def distill_single_sample(
    sample: dict[str, Any],
    request_config: ProviderRequestConfig,
    max_retries: int,
    *,
    cancel_event: Any | None = None,
) -> dict[str, Any] | None:
    client = build_provider_client(request_config)
    for _attempt in range(max_retries):
        if cancel_event is not None and getattr(cancel_event, "is_set", lambda: False)():
            raise KeyboardInterrupt
        try:
            response_text = client.generate_text(build_distill_messages([sample]), model=request_config.model)
            parsed = parse_distill_response(response_text, 1)
            label, reason = normalize_distilled_label(sample, *parsed[0])
            return {
                **sample,
                "label": label,
                "teacher_reason": reason,
                "batch_index": sample.get("batch_index", 0),
            }
        except (ProviderResponseError, ValueError, json.JSONDecodeError, KeyError):
            continue
    return None


def normalize_distilled_label(sample: dict[str, Any], label: str, reason: str) -> tuple[str, str]:
    hint = canonical_label(sample.get("interpret_hint", False))
    if label == "true" and has_source_term_defined_by_target(str(sample.get("text", ""))):
        return "false", f"{reason}（当前条文在定义来源文件自身概念，[T] 仅作为定义依据，回退为false）"
    if (
        label == "true"
        and hint == "false"
        and bool(sample.get("hard_negative"))
        and not has_explicit_target_interpretation(str(sample.get("text", "")))
    ):
        return "false", f"{reason}（按高精确度硬负例策略回退为false）"
    return label, reason
