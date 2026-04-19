from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from typing import Any, Callable

from utils.llm.base import ProviderResponseError

from ...contracts import AlignConceptRecord, AlignPairRecord, EquivalenceRecord

PROMPT = """判断法律概念对关系。
输入示例：
{"id":1,"left":{"name":"概念A","description":"描述A"},"right":{"name":"概念B","description":"描述B"}}

输出示例：
{"id":1,"relation":"equivalent"}

relation 只能是：
- equivalent：两边法律概念等价，可以合并
- is_subordinate：左侧从属于右侧
- has_subordinate：右侧从属于左侧
- related：两边相关但不等价、也不是从属
- none：两边不存在上述关系

要求：
1. 只返回合法 JSON 数组。
2. 输出条数与输入一致，顺序一一对应。
3. 不要输出额外字段。
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
    payload = dict(runtime.align_config().get("judge", {}))
    provider = str(payload.get("provider", "")).strip()
    model = str(payload.get("model", "")).strip()
    if not provider or not model:
        raise ValueError("builder.align.judge must define non-empty provider and model.")
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
    concepts: list[AlignConceptRecord],
    equivalence: list[EquivalenceRecord],
    runtime: Any,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
    checkpoint_every: int = 0,
    checkpoint_callback: Callable[[list[AlignPairRecord], dict[str, int], list[str], list[dict[str, Any]]], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> AlignClassifyResult:
    pending_pairs = [row for row in pairs if row.relation == ""]
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

    semantics = build_semantics_lookup(concepts, equivalence)
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
            executor.submit(classify_batch, runtime, batch, request_config, config.max_retries, semantics): batch
            for batch in batches
        }
        for future in as_completed(future_to_batch):
            if cancel_event is not None and cancel_event.is_set():
                raise KeyboardInterrupt
            batch_result = future.result()
            llm_errors.extend(batch_result.errors)
            total_requests += int(batch_result.request_count)
            retry_count += int(batch_result.retry_count)
            if batch_result.failed_pair_ids:
                raise ProviderResponseError(
                    f"Align judge failed for pairs: {', '.join(batch_result.failed_pair_ids[:5])}"
                )
            for pair_id_value, relation in batch_result.records:
                processed_relations[pair_id_value] = relation
                processed_pair_ids.append(pair_id_value)
            completed += len(future_to_batch[future])
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
    if any(row.relation == "" for row in updated_pairs):
        raise ValueError("Align judge completed with unresolved pending pairs.")
    return AlignClassifyResult(
        pairs=updated_pairs,
        processed_pair_ids=sorted(dict.fromkeys(processed_pair_ids)),
        stats=build_classify_stats(updated_pairs, total_requests, retry_count, llm_errors),
        llm_errors=summarize_llm_errors(llm_errors),
    )


def build_semantics_lookup(
    concepts: list[AlignConceptRecord],
    equivalence: list[EquivalenceRecord],
) -> dict[str, tuple[str, str]]:
    lookup = {
        row.id: (row.name, row.description)
        for row in concepts
    }
    lookup.update(
        {
            row.id: (row.name, row.description)
            for row in equivalence
        }
    )
    return lookup


def classify_batch(
    runtime: Any,
    batch: list[AlignPairRecord],
    request_config: Any,
    max_retries: int,
    semantics: dict[str, tuple[str, str]],
) -> AlignClassifyBatchResult:
    errors: list[dict[str, Any]] = []
    pair_ids = [pair_id(row) for row in batch]
    for attempt in range(1, max_retries + 1):
        try:
            prompt = build_classify_prompt(batch, semantics)
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


def build_classify_prompt(
    pairs: list[AlignPairRecord],
    semantics: dict[str, tuple[str, str]],
) -> list[dict[str, str]]:
    payload = []
    for index, row in enumerate(pairs, start=1):
        left_name, left_description = semantics.get(row.left_id, ("", ""))
        right_name, right_description = semantics.get(row.right_id, ("", ""))
        payload.append(
            {
                "id": index,
                "left": {"name": left_name, "description": left_description},
                "right": {"name": right_name, "description": right_description},
            }
        )
    user = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return [{"role": "system", "content": PROMPT}, {"role": "user", "content": user}]


def parse_classify_response(raw: str, pairs: list[AlignPairRecord]) -> dict[str, str]:
    payload = decode_first_json_payload(strip_markdown_fence(raw))
    items = payload if isinstance(payload, list) else payload.get("items", [])
    if not isinstance(items, list):
        raise ValueError("Align judge response must contain a list of items.")
    results: dict[str, str] = {}
    allowed = {"equivalent", "is_subordinate", "has_subordinate", "related", "none"}
    for item in items:
        prompt_id = normalize_prompt_item_id(item.get("id"))
        if prompt_id is None or prompt_id <= 0 or prompt_id > len(pairs):
            continue
        relation = str(item.get("relation", "")).strip().lower()
        if relation not in allowed:
            raise ValueError(f"Unsupported align relation: {relation or '<empty>'}")
        results[pair_id(pairs[prompt_id - 1])] = relation
    missing = [pair_id(row) for row in pairs if pair_id(row) not in results]
    if missing:
        raise ValueError(f"Align judge response is missing pair ids: {', '.join(missing[:5])}")
    return results


def apply_classify_results(pairs: list[AlignPairRecord], processed_relations: dict[str, str]) -> list[AlignPairRecord]:
    updated: list[AlignPairRecord] = []
    for row in pairs:
        updated.append(replace(row, relation=processed_relations.get(pair_id(row), row.relation)))
    return updated


def build_classify_stats(
    pairs: list[AlignPairRecord],
    total_requests: int,
    retry_count: int,
    llm_errors: list[dict[str, Any]],
) -> dict[str, int]:
    counts = {
        "pair_count": len(pairs),
        "pending_count": sum(1 for row in pairs if row.relation == ""),
        "equivalent_count": sum(1 for row in pairs if row.relation == "equivalent"),
        "is_subordinate_count": sum(1 for row in pairs if row.relation == "is_subordinate"),
        "has_subordinate_count": sum(1 for row in pairs if row.relation == "has_subordinate"),
        "related_count": sum(1 for row in pairs if row.relation == "related"),
        "none_count": sum(1 for row in pairs if row.relation == "none"),
        "llm_request_count": int(total_requests),
        "llm_error_count": len(llm_errors),
        "retry_count": int(retry_count),
    }
    counts["result_count"] = counts["pair_count"]
    return counts


def pair_id(row: AlignPairRecord) -> str:
    return f"{row.left_id}\t{row.right_id}"


def strip_markdown_fence(raw: str) -> str:
    text = str(raw or "").strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3 and lines[-1].strip() == "```":
            return "\n".join(lines[1:-1]).strip()
    return text


def decode_first_json_payload(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        start = min([index for index in [raw.find("["), raw.find("{")] if index >= 0], default=-1)
        if start < 0:
            raise
        end = max(raw.rfind("]"), raw.rfind("}"))
        if end < start:
            raise
        return json.loads(raw[start : end + 1])


def normalize_prompt_item_id(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def summarize_llm_errors(errors: list[dict[str, object]], *, limit: int = 10) -> list[dict[str, object]]:
    summarized: dict[tuple[str, str], dict[str, object]] = {}
    for item in errors:
        error_type = str(item.get("error_type", "") or "")
        message = str(item.get("message", "") or "")
        key = (error_type, message)
        entry = summarized.get(key)
        if entry is None:
            sample_ids = [str(value) for value in item.get("pair_ids", [])[:3]]
            entry = {
                "error_type": error_type,
                "message": message,
                "count": 0,
                "sample_pair_ids": sample_ids,
            }
            summarized[key] = entry
        entry["count"] = int(entry.get("count", 0)) + 1
    ordered = sorted(
        summarized.values(),
        key=lambda item: (-int(item.get("count", 0)), str(item.get("error_type", "")), str(item.get("message", ""))),
    )
    return ordered[:limit]
