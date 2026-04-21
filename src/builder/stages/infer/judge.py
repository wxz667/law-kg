from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace

from utils.llm.base import ProviderResponseError

from ...contracts import EquivalenceRecord, InferPairRecord
from .types import InferJudgeResult, InferJudgeRuntimeConfig

PROMPT = """判断法律概念之间是否应补充隐式关系。
输入示例：
{"id":1,"left":{"name":"概念A","description":"描述A"},"right":{"name":"概念B","description":"描述B"}}

输出示例：
{"id":1,"relation":"related","strength":8}

relation 只能是：
- none：不建议补边
- related：概念相关，但不是从属
- is_subordinate：左侧从属于右侧
- has_subordinate：左侧包含右侧作为下位概念

要求：
1. 只返回合法 JSON 数组。
2. 输出条数与输入一致，顺序一一对应。
3. strength 必须是 0 到 10 的整数。
4. 不要输出额外字段。
"""

VALID_RELATIONS = {"none", "related", "is_subordinate", "has_subordinate"}


def resolve_infer_judge_runtime_config(runtime) -> InferJudgeRuntimeConfig:
    payload = dict(runtime.infer_config().get("judge", {}))
    provider = str(payload.get("provider", "")).strip()
    model = str(payload.get("model", "")).strip()
    if not provider or not model:
        raise ValueError("builder.infer.judge must define non-empty provider and model.")
    return InferJudgeRuntimeConfig(
        provider=provider,
        model=model,
        batch_size=max(int(payload.get("batch_size", 20) or 20), 1),
        concurrent_requests=max(int(payload.get("concurrent_requests", 1) or 1), 1),
        request_timeout_seconds=max(int(payload.get("request_timeout_seconds", 90) or 90), 1),
        max_retries=max(int(payload.get("max_retries", 2) or 2), 1),
        min_strength=max(int(payload.get("min_strength", 7) or 7), 0),
        params=dict(payload.get("params", {})),
        rate_limit=dict(payload.get("rate_limit", {})) if isinstance(payload.get("rate_limit", {}), dict) else {},
    )


def judge_pairs(
    pairs: list[InferPairRecord],
    *,
    concepts: list[EquivalenceRecord],
    runtime,
    progress_callback=None,
    checkpoint_every: int = 0,
    checkpoint_callback=None,
    cancel_event: threading.Event | None = None,
) -> InferJudgeResult:
    total_pairs = len(pairs)
    if progress_callback is not None:
        progress_callback(0, total_pairs)
    if not pairs:
        return InferJudgeResult(pairs=[], processed_pair_ids=[], stats=build_judge_stats([], 0, 0, []), llm_errors=[])

    config = resolve_infer_judge_runtime_config(runtime)
    request_config = runtime.build_request_config(
        {
            "provider": config.provider,
            "model": config.model,
            "params": config.params,
            "request_timeout_seconds": config.request_timeout_seconds,
            "max_retries": config.max_retries,
            "rate_limit": config.rate_limit,
        }
    )
    concepts_by_id = {row.id: row for row in concepts}
    batches = [
        pairs[index : index + config.batch_size]
        for index in range(0, len(pairs), config.batch_size)
    ]
    judged_by_key: dict[tuple[str, str], InferPairRecord] = {}
    llm_errors: list[dict[str, object]] = []
    processed_pair_ids: list[str] = []
    completed = 0
    total_requests = 0
    retry_count = 0
    next_checkpoint = max(int(checkpoint_every or 0), 0)
    max_workers = max(1, min(config.concurrent_requests, len(batches) or 1))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {
            executor.submit(
                judge_batch,
                runtime,
                batch,
                request_config,
                config.max_retries,
                concepts_by_id,
            ): batch
            for batch in batches
        }
        for future in as_completed(future_to_batch):
            if cancel_event is not None and cancel_event.is_set():
                raise KeyboardInterrupt
            batch_result = future.result()
            llm_errors.extend(batch_result["errors"])
            total_requests += int(batch_result["request_count"])
            retry_count += int(batch_result["retry_count"])
            if batch_result["failed_pair_ids"]:
                raise ProviderResponseError(
                    f"Infer judge failed for pairs: {', '.join(batch_result['failed_pair_ids'][:5])}"
                )
            for pair in batch_result["pairs"]:
                judged_by_key[(pair.left_id, pair.right_id)] = pair
                processed_pair_ids.append(pair_id(pair.left_id, pair.right_id))
            completed += len(future_to_batch[future])
            if progress_callback is not None:
                progress_callback(completed, total_pairs)
            if checkpoint_callback is not None and checkpoint_every > 0:
                should_checkpoint = completed >= total_pairs or (next_checkpoint > 0 and completed >= next_checkpoint)
                if should_checkpoint:
                    snapshot = [judged_by_key[key] for key in sorted(judged_by_key)]
                    checkpoint_callback(
                        snapshot,
                        build_judge_stats(snapshot, total_requests, retry_count, llm_errors),
                        sorted(dict.fromkeys(processed_pair_ids)),
                        summarize_llm_errors(llm_errors),
                    )
                    while next_checkpoint > 0 and completed >= next_checkpoint:
                        next_checkpoint += checkpoint_every

    final_pairs = [judged_by_key[key] for key in sorted(judged_by_key)]
    return InferJudgeResult(
        pairs=final_pairs,
        processed_pair_ids=sorted(dict.fromkeys(processed_pair_ids)),
        stats=build_judge_stats(final_pairs, total_requests, retry_count, llm_errors),
        llm_errors=summarize_llm_errors(llm_errors),
    )


def judge_batch(
    runtime,
    batch: list[InferPairRecord],
    request_config,
    max_retries: int,
    concepts_by_id: dict[str, EquivalenceRecord],
) -> dict[str, object]:
    errors: list[dict[str, object]] = []
    batch_pair_ids = [pair_id(row.left_id, row.right_id) for row in batch]
    for attempt in range(1, max_retries + 1):
        try:
            prompt = build_judge_prompt(batch, concepts_by_id)
            raw = runtime.generate_text(prompt, request_config)
            pairs = parse_judge_response(raw, batch)
            return {
                "pairs": pairs,
                "errors": [],
                "failed_pair_ids": [],
                "request_count": attempt,
                "retry_count": max(attempt - 1, 0),
            }
        except (ProviderResponseError, ValueError, json.JSONDecodeError) as exc:
            errors.append(
                {
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "attempt": attempt,
                    "pair_ids": batch_pair_ids,
                }
            )
    return {
        "pairs": [],
        "errors": errors,
        "failed_pair_ids": batch_pair_ids,
        "request_count": max_retries,
        "retry_count": max(max_retries - 1, 0),
    }


def build_judge_prompt(
    batch: list[InferPairRecord],
    concepts_by_id: dict[str, EquivalenceRecord],
) -> list[dict[str, str]]:
    payload = []
    for index, pair in enumerate(batch, start=1):
        left = concepts_by_id[pair.left_id]
        right = concepts_by_id[pair.right_id]
        payload.append(
            {
                "id": index,
                "left": {
                    "name": left.name,
                    "description": left.description,
                },
                "right": {
                    "name": right.name,
                    "description": right.description,
                },
            }
        )
    return [
        {"role": "system", "content": PROMPT},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False, separators=(",", ":"))},
    ]


def parse_judge_response(raw: str, batch: list[InferPairRecord]) -> list[InferPairRecord]:
    payload = decode_first_json_payload(strip_markdown_fence(raw))
    items = payload if isinstance(payload, list) else payload.get("items", [])
    if not isinstance(items, list):
        raise ValueError("Infer judge response must be a JSON array.")
    if len(items) != len(batch):
        raise ValueError("Infer judge response length does not match request batch size.")
    pairs: list[InferPairRecord] = []
    for pair, item in zip(batch, items):
        if not isinstance(item, dict):
            raise ValueError("Infer judge response items must be objects.")
        relation = str(item.get("relation", "")).strip()
        if relation not in VALID_RELATIONS:
            raise ValueError(f"Invalid infer relation: {relation}")
        strength = int(item.get("strength", 0) or 0)
        pairs.append(
            replace(
                pair,
                relation=relation,
                strength=max(0, min(10, strength)),
            )
        )
    return pairs


def build_judge_stats(
    pairs: list[InferPairRecord],
    total_requests: int,
    retry_count: int,
    llm_errors: list[dict[str, object]],
) -> dict[str, int]:
    relation_counts = {relation: 0 for relation in VALID_RELATIONS}
    accepted_count = 0
    for row in pairs:
        relation_counts[row.relation] = int(relation_counts.get(row.relation, 0)) + 1
        if pair_is_accepted(row):
            accepted_count += 1
    return {
        "judgment_count": len(pairs),
        "result_count": len(pairs),
        "accepted_count": accepted_count,
        "none_count": relation_counts.get("none", 0),
        "related_count": relation_counts.get("related", 0),
        "is_subordinate_count": relation_counts.get("is_subordinate", 0),
        "has_subordinate_count": relation_counts.get("has_subordinate", 0),
        "llm_request_count": int(total_requests),
        "llm_error_count": len(llm_errors),
        "retry_count": int(retry_count),
    }


def pair_is_accepted(pair: InferPairRecord, *, min_strength: int = 7) -> bool:
    return bool(pair.relation) and pair.relation != "none" and int(pair.strength) >= int(min_strength)


def pair_id(left_id: str, right_id: str) -> str:
    return f"{left_id}\t{right_id}"


def summarize_llm_errors(errors: list[dict[str, object]], *, limit: int = 10) -> list[dict[str, object]]:
    summarized: dict[tuple[str, str], dict[str, object]] = {}
    for item in errors:
        error_type = str(item.get("error_type", "") or "")
        message = str(item.get("message", "") or "")
        key = (error_type, message)
        entry = summarized.get(key)
        if entry is None:
            entry = {
                "error_type": error_type,
                "message": message,
                "count": 0,
                "pair_ids": [str(value) for value in item.get("pair_ids", [])[:3]],
            }
            summarized[key] = entry
        entry["count"] = int(entry.get("count", 0)) + 1
    return list(sorted(summarized.values(), key=lambda item: (-int(item["count"]), item["error_type"], item["message"])))[:limit]


def strip_markdown_fence(raw: str) -> str:
    text = str(raw or "").strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text


def decode_first_json_payload(raw: str):
    text = raw.strip()
    if not text:
        raise ValueError("Infer judge returned empty response.")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start_candidates = [index for index in (text.find("["), text.find("{")) if index >= 0]
        if not start_candidates:
            raise
        start = min(start_candidates)
        end_brace = text.rfind("}")
        end_bracket = text.rfind("]")
        end = max(end_brace, end_bracket)
        if end <= start:
            raise
        return json.loads(text[start : end + 1])
