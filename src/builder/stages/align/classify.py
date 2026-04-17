from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from typing import Any, Callable

from utils.llm.base import ProviderResponseError

from ...contracts import AlignPairRecord

PROMPT = """判断法律概念对关系。
输入：
{"id": 1, "left_text": "概念A", "right_text": "概念B"}
输出：
{"id": 1, "relation": "equivalent"}

relation 只能是：
- equivalent: 表示两边概念在法律意义上等价，可以合并为同一节点，不要求完全等同，但必须在绝大多数语境下可以互换使用，不会引起歧义。
- related: 表示两边概念在法律意义上相关但不等价
- ignore: 表示两边概念在法律意义上不相关

要求：
1. 只返回合法 JSON 数组。
2. 输出条数与输入一致，顺序一一对应。
"""


@dataclass(frozen=True)
class AlignClassifyRuntimeConfig:
    provider: str
    model: str
    batch_size: int
    concurrent_requests: int
    request_timeout_seconds: int
    max_retries: int
    params: dict[str, Any]
    rate_limit: dict[str, Any]


@dataclass(frozen=True)
class AlignClassifyBatchResult:
    records: list[tuple[str, str]]
    errors: list[dict[str, Any]]
    failed_pair_ids: list[str]
    request_count: int = 0
    retry_count: int = 0


@dataclass
class AlignClassifyResult:
    pairs: list[AlignPairRecord]
    processed_pair_ids: list[str]
    stats: dict[str, int]
    llm_errors: list[dict[str, Any]]


def resolve_align_classify_runtime_config(runtime: Any) -> AlignClassifyRuntimeConfig:
    payload = dict(runtime.align_config().get("classify", {}))
    provider = str(payload.get("provider", "")).strip()
    model = str(payload.get("model", "")).strip()
    if not provider or not model:
        raise ValueError("builder.align.classify must define non-empty provider and model.")
    return AlignClassifyRuntimeConfig(
        provider=provider,
        model=model,
        batch_size=max(int(payload.get("batch_size", 20) or 20), 1),
        concurrent_requests=max(int(payload.get("concurrent_requests", 1) or 1), 1),
        request_timeout_seconds=max(int(payload.get("request_timeout_seconds", 90) or 90), 1),
        max_retries=max(int(payload.get("max_retries", 2) or 2), 1),
        params=dict(payload.get("params", {})),
        rate_limit=dict(payload.get("rate_limit", {})) if isinstance(payload.get("rate_limit", {}), dict) else {},
    )


def classify_pairs(
    pairs: list[AlignPairRecord],
    runtime: Any,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
    checkpoint_every: int = 0,
    checkpoint_callback: Callable[[list[AlignPairRecord], dict[str, int], list[str], list[dict[str, Any]]], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> AlignClassifyResult:
    pending_pairs = [row for row in pairs if row.relation == "pending"]
    total_pending = len(pending_pairs)
    if progress_callback is not None:
        progress_callback(0, max(total_pending, 1))
    if not pending_pairs:
        if progress_callback is not None:
            progress_callback(1, 1)
        return AlignClassifyResult(
            pairs=list(pairs),
            processed_pair_ids=[],
            stats=build_classify_stats(pairs, 0, 0, []),
            llm_errors=[],
        )

    config = resolve_align_classify_runtime_config(runtime)
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
    batches = [
        pending_pairs[index : index + config.batch_size]
        for index in range(0, len(pending_pairs), config.batch_size)
    ]
    processed_relations: dict[str, str] = {}
    llm_errors: list[dict[str, Any]] = []
    processed_pair_ids: list[str] = []
    completed = 0
    total_requests = 0
    retry_count = 0
    next_checkpoint = max(int(checkpoint_every or 0), 0)

    max_workers = max(1, min(config.concurrent_requests, len(batches) or 1))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {
            executor.submit(classify_batch, runtime, batch, request_config, config.max_retries): batch
            for batch in batches
        }
        for future in as_completed(future_to_batch):
            if cancel_event is not None and cancel_event.is_set():
                raise KeyboardInterrupt
            batch = future_to_batch[future]
            batch_result = future.result()
            llm_errors.extend(batch_result.errors)
            total_requests += int(batch_result.request_count)
            retry_count += int(batch_result.retry_count)
            if batch_result.failed_pair_ids:
                raise ProviderResponseError(
                    f"Align classify failed for pending pairs: {', '.join(batch_result.failed_pair_ids[:5])}"
                )
            for pair_id, relation in batch_result.records:
                processed_relations[pair_id] = relation
                processed_pair_ids.append(pair_id)
            completed += len(batch)
            if progress_callback is not None:
                progress_callback(completed, max(total_pending, 1))
            if checkpoint_callback is not None and checkpoint_every > 0:
                if completed >= total_pending:
                    checkpoint_callback(
                        apply_classify_results(pairs, processed_relations),
                        build_classify_stats(pairs, total_requests, retry_count, llm_errors),
                        sorted(dict.fromkeys(processed_pair_ids)),
                        summarize_llm_errors(llm_errors),
                    )
                elif next_checkpoint > 0 and completed >= next_checkpoint:
                    checkpoint_callback(
                        apply_classify_results(pairs, processed_relations),
                        build_classify_stats(pairs, total_requests, retry_count, llm_errors),
                        sorted(dict.fromkeys(processed_pair_ids)),
                        summarize_llm_errors(llm_errors),
                    )
                    while next_checkpoint > 0 and completed >= next_checkpoint:
                        next_checkpoint += checkpoint_every

    updated_pairs = apply_classify_results(pairs, processed_relations)
    if any(row.relation == "pending" for row in updated_pairs):
        raise ValueError("Align classify completed with unresolved pending pairs.")
    return AlignClassifyResult(
        pairs=updated_pairs,
        processed_pair_ids=sorted(dict.fromkeys(processed_pair_ids)),
        stats=build_classify_stats(updated_pairs, total_requests, retry_count, llm_errors),
        llm_errors=summarize_llm_errors(llm_errors),
    )


def classify_batch(
    runtime: Any,
    batch: list[AlignPairRecord],
    request_config: Any,
    max_retries: int,
) -> AlignClassifyBatchResult:
    errors: list[dict[str, Any]] = []
    pair_ids = [pair_id(row) for row in batch]
    for attempt in range(1, max_retries + 1):
        try:
            prompt = build_classify_prompt(batch)
            raw = runtime.generate_text(prompt, request_config)
            relations = parse_classify_response(raw, batch)
            return AlignClassifyBatchResult(
                records=list(relations.items()),
                errors=[],
                failed_pair_ids=[],
                request_count=attempt,
                retry_count=max(attempt - 1, 0),
            )
        except (ProviderResponseError, ValueError, json.JSONDecodeError) as exc:
            errors.append(
                {
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "attempt": attempt,
                    "pair_ids": pair_ids,
                }
            )
    return AlignClassifyBatchResult(
        records=[],
        errors=errors,
        failed_pair_ids=pair_ids,
        request_count=max_retries,
        retry_count=max(max_retries - 1, 0),
    )


def build_classify_prompt(pairs: list[AlignPairRecord]) -> list[dict[str, str]]:
    payload = [
        {
            "id": index,
            "left_text": row.left_text,
            "right_text": row.right_text,
        }
        for index, row in enumerate(pairs, start=1)
    ]
    user = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return [{"role": "system", "content": PROMPT}, {"role": "user", "content": user}]


def parse_classify_response(raw: str, pairs: list[AlignPairRecord]) -> dict[str, str]:
    payload = decode_first_json_payload(strip_markdown_fence(raw))
    items = payload if isinstance(payload, list) else payload.get("items", [])
    if not isinstance(items, list):
        raise ValueError("Align classify response must contain a list of items.")
    results: dict[str, str] = {}
    for item in items:
        prompt_id = normalize_prompt_item_id(item.get("id"))
        if prompt_id is None or prompt_id <= 0 or prompt_id > len(pairs):
            continue
        relation = str(item.get("relation", "")).strip().lower()
        if relation not in {"equivalent", "related", "ignore"}:
            raise ValueError(f"Unsupported align classify relation: {relation or '<empty>'}")
        results[pair_id(pairs[prompt_id - 1])] = relation
    missing = [pair_id(row) for row in pairs if pair_id(row) not in results]
    if missing:
        raise ValueError(f"Align classify response is missing pair ids: {', '.join(missing[:5])}")
    return results


def apply_classify_results(pairs: list[AlignPairRecord], processed_relations: dict[str, str]) -> list[AlignPairRecord]:
    updated: list[AlignPairRecord] = []
    for row in pairs:
        key = pair_id(row)
        relation = processed_relations.get(key, row.relation)
        updated.append(replace(row, relation=relation))
    return updated


def build_classify_stats(
    pairs: list[AlignPairRecord],
    total_requests: int,
    retry_count: int,
    llm_errors: list[dict[str, Any]],
) -> dict[str, int]:
    equivalent_count = sum(1 for row in pairs if row.relation == "equivalent")
    related_count = sum(1 for row in pairs if row.relation == "related")
    ignore_count = sum(1 for row in pairs if row.relation == "ignore")
    pending_count = sum(1 for row in pairs if row.relation == "pending")
    return {
        "pair_count": len(pairs),
        "equivalent_count": equivalent_count,
        "related_count": related_count,
        "ignore_count": ignore_count,
        "pending_count": pending_count,
        "llm_request_count": int(total_requests),
        "retry_count": int(retry_count),
        "llm_error_count": len(llm_errors),
    }


def summarize_llm_errors(errors: list[dict[str, Any]], *, limit: int = 10) -> list[dict[str, Any]]:
    summarized: dict[tuple[str, str], dict[str, Any]] = {}
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
                "pair_ids": [],
            }
            summarized[key] = entry
        entry["count"] = int(entry["count"]) + 1
        for pair_id_value in item.get("pair_ids", [])[:3]:
            if pair_id_value not in entry["pair_ids"] and len(entry["pair_ids"]) < 3:
                entry["pair_ids"].append(pair_id_value)
    return sorted(summarized.values(), key=lambda item: (-int(item["count"]), str(item["error_type"])))[:limit]


def strip_markdown_fence(content: str) -> str:
    stripped = content.strip()
    if stripped.startswith("```"):
        lines = [line for line in stripped.splitlines() if not line.strip().startswith("```")]
        return "\n".join(lines).strip()
    return stripped


def decode_first_json_payload(content: str) -> Any:
    stripped = content.strip()
    if not stripped:
        raise json.JSONDecodeError("Empty content", content, 0)
    decoder = json.JSONDecoder()
    start_positions = [index for index, char in enumerate(stripped) if char in "[{"]
    last_error: json.JSONDecodeError | None = None
    for start in start_positions:
        try:
            payload, _end = decoder.raw_decode(stripped, idx=start)
            return payload
        except json.JSONDecodeError as exc:
            last_error = exc
            continue
    if last_error is not None:
        raise last_error
    raise json.JSONDecodeError("No JSON object or array found", content, 0)


def normalize_prompt_item_id(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    text = str(value).strip()
    return int(text) if text.isdigit() else None


def pair_id(row: AlignPairRecord) -> str:
    return f"{row.left_id}|{row.right_id}"
