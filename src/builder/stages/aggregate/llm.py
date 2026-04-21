from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from utils.llm.base import ProviderRequestConfig, ProviderResponseError, build_provider_request_config, validate_provider_api_params

from ...contracts import AggregateConceptRecord, AggregateCoreConcept, AggregateSubordinateConcept
from ..extract.postprocess import normalize_text, postprocess_concept_name, postprocess_description
from .concepts import flatten_structured_concepts
from .types import AggregateInputRecord

PROMPT = """# 角色
你是法律知识图谱聚合与消歧工程师。

# 输入
每个输入单元包含：
- `hierarchy`
- `concepts[].name`
- `concepts[].description`

# 任务
基于 `hierarchy` 和结构化概念，完成以下工作：
1. 合并语义重复或可归并的概念
2. 丢弃不能作为法条标识的概念
3. 划分核心概念和附属概念
4. 概念消歧，用上下文增强概念名称和描述，增加领域限定、补充缺失信息，确保不存在适用和指代歧义
5. 优化概念的名称，使其更简洁、准确、适合作为知识图谱节点标识
6. 优化描述信息，使其更简洁、准确、适合作为知识图谱节点属性

# 规则
- 只根据输入内容判断，不要补写输入中没有的信息
- 核心概念应适合被章节节点直接标识
- 无法判断从属关系保持为并列核心概念
- 概念需要有明确的字面和语义信息，丢弃“xx法一般规定”“xxx术语/术语解释”这类无实际意义的概念
- 概念需要具有独立的法律概念地位，删除只有局部语义成立的部分以及代词，例如“本法”、“各级各类学校”应该丢弃
- 概念的名称和描述必须清晰、准确，避免歧义
- 核心概念和附属概念的**名称(concept)的领域限定一致**，不要省略，名称信息具备完整语义信息，独立取用时无指代和适用歧义
- 核心概念和附属概念的**描述(description)互补**，应明确侧重点（参考示例“共同海损”和“共同海损分摊”）。
- 描述必须直接说明内容，不要写“是指 / 表示 / 定义为 / 系指 / 即”等废话开头。
- 描述不要有直接的法条引用，也尽量减少使用模糊代词(本法/章/条)。
- 描述不要包含具体的数字、时间等信息。
- `subordinates` 必须始终返回数组，没有从属时返回 `[]`。

# 输出格式
只返回严格 JSON 数组，不要输出任何额外说明：
[
  {
    "id": 1,
    "concepts": [
      {
        "concept": "共同海损",
        "description": "同一海上航程中为共同安全故意合理造成的特别牺牲或者特别费用",
        "subordinates": [
          {"concept": "共同海损分摊", "description": "受益各方按共同海损规则分担补偿的机制"}
        ]
      }
    ]
  }
]
"""


@dataclass(frozen=True)
class AggregateRuntimeConfig:
    provider: str
    model: str
    batch_size: int
    concurrent_requests: int
    request_timeout_seconds: int
    max_retries: int
    params: dict[str, Any]
    rate_limit: dict[str, Any]


@dataclass(frozen=True)
class AggregateBatchResult:
    concepts: list[AggregateConceptRecord]
    errors: list[dict[str, Any]]
    failed_input_ids: list[str]
    request_count: int = 0
    retry_count: int = 0


class MissingAggregateInputIdsError(ValueError):
    def __init__(self, missing_input_ids: list[str], concepts: list[AggregateConceptRecord]) -> None:
        self.missing_input_ids = list(missing_input_ids)
        self.concepts = list(concepts)
        first_missing = self.missing_input_ids[0] if self.missing_input_ids else ""
        super().__init__(f"Aggregate response is missing input id: {first_missing}")


def resolve_aggregate_runtime_config(runtime: Any) -> AggregateRuntimeConfig:
    raw = dict(runtime.aggregate_config())
    provider = str(raw.get("provider", "")).strip()
    model = str(raw.get("model", "")).strip()
    if not provider or not model:
        raise ValueError("builder.aggregate must define non-empty provider and model.")
    params = validate_provider_api_params(raw.get("params", {}))
    params.setdefault("temperature", 0.0)
    params.setdefault("max_tokens", 2048)
    return AggregateRuntimeConfig(
        provider=provider,
        model=model,
        batch_size=max(int(raw.get("batch_size", 1)), 1),
        concurrent_requests=max(int(raw.get("concurrent_requests", 1)), 1),
        request_timeout_seconds=max(int(raw.get("request_timeout_seconds", 90)), 1),
        max_retries=max(int(raw.get("max_retries", 2)), 1),
        params=params,
        rate_limit=dict(raw.get("rate_limit", {})) if isinstance(raw.get("rate_limit", {}), dict) else {},
    )


def build_request_config(config: AggregateRuntimeConfig) -> ProviderRequestConfig:
    return build_provider_request_config(
        provider=config.provider,
        model=config.model,
        params=config.params,
        timeout_seconds=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit=config.rate_limit,
    )


def aggregate_concepts_batch(runtime: Any, inputs: list[AggregateInputRecord]) -> AggregateBatchResult:
    if not inputs:
        return AggregateBatchResult(concepts=[], errors=[], failed_input_ids=[])
    runtime_config = resolve_aggregate_runtime_config(runtime)
    request_config = build_request_config(runtime_config)
    errors: list[dict[str, Any]] = []
    concepts_by_input_id: dict[str, list[AggregateConceptRecord]] = {}
    pending_inputs: list[AggregateInputRecord] = []
    request_count = 0

    try:
        request_count += 1
        raw = runtime.generate_text(build_aggregate_prompt(inputs), request_config)
        concepts, missing_input_ids = parse_aggregate_response_parts(raw, inputs)
        _store_aggregate_concepts(concepts_by_input_id, concepts)
        if not missing_input_ids:
            return AggregateBatchResult(
                concepts=_ordered_aggregate_concepts(inputs, concepts_by_input_id),
                errors=[],
                failed_input_ids=[],
                request_count=request_count,
                retry_count=0,
            )
        missing_error = MissingAggregateInputIdsError(missing_input_ids, concepts)
        errors.append(_format_aggregate_error(missing_error, 1, inputs))
        missing_set = set(missing_input_ids)
        pending_inputs = [row for row in inputs if row.id in missing_set]
    except (ProviderResponseError, ValueError, json.JSONDecodeError) as exc:
        errors.append(_format_aggregate_error(exc, 1, inputs))
        pending_inputs = list(inputs)
    except Exception as exc:
        errors.append(_format_aggregate_error(exc, 1, inputs))
        pending_inputs = list(inputs)

    failed_input_ids: list[str] = []
    for row in pending_inputs:
        recovered = False
        for attempt in range(2, runtime_config.max_retries + 1):
            try:
                request_count += 1
                raw = runtime.generate_text(build_aggregate_prompt([row]), request_config)
                concepts = parse_aggregate_response(raw, [row])
                concepts_by_input_id[row.id] = list(concepts)
                recovered = True
                break
            except (ProviderResponseError, ValueError, json.JSONDecodeError) as exc:
                errors.append(_format_aggregate_error(exc, attempt, [row]))
            except Exception as exc:
                errors.append(_format_aggregate_error(exc, attempt, [row]))
        if not recovered:
            failed_input_ids.append(row.id)

    concepts = _ordered_aggregate_concepts(inputs, concepts_by_input_id)
    if not failed_input_ids:
        errors = []
    return AggregateBatchResult(
        concepts=concepts,
        errors=errors,
        failed_input_ids=failed_input_ids,
        request_count=request_count,
        retry_count=max(request_count - 1, 0),
    )


def _format_aggregate_error(exc: Exception, attempt: int, inputs: list[AggregateInputRecord]) -> dict[str, Any]:
    return {
        "error_type": exc.__class__.__name__,
        "message": str(exc),
        "attempt": attempt,
        "input_ids": [row.id for row in inputs],
    }


def _store_aggregate_concepts(
    concepts_by_input_id: dict[str, list[AggregateConceptRecord]],
    concepts: list[AggregateConceptRecord],
) -> None:
    for concept in concepts:
        concepts_by_input_id.setdefault(concept.root, []).append(concept)


def _ordered_aggregate_concepts(
    inputs: list[AggregateInputRecord],
    concepts_by_input_id: dict[str, list[AggregateConceptRecord]],
) -> list[AggregateConceptRecord]:
    concepts: list[AggregateConceptRecord] = []
    for row in inputs:
        concepts.extend(concepts_by_input_id.get(row.id, []))
    return concepts


def build_aggregate_prompt(inputs: list[AggregateInputRecord]) -> list[dict[str, str]]:
    prompt_id_map = build_prompt_id_map(inputs)
    payload = [
        {
            "id": prompt_id_map[row.id],
            "hierarchy": row.hierarchy,
            "concepts": [item.to_dict() for item in row.concepts],
        }
        for row in inputs
    ]
    user = (
        "请对以下结构化法律概念做聚合。"
        "返回时必须保留输入中的数字 id，不要改写成其他 id。"
        "\n"
        + json.dumps(payload, ensure_ascii=False)
    )
    return [{"role": "system", "content": PROMPT}, {"role": "user", "content": user}]


def parse_aggregate_response(raw: str, inputs: list[AggregateInputRecord]) -> list[AggregateConceptRecord]:
    concepts, missing_input_ids = parse_aggregate_response_parts(raw, inputs)
    if missing_input_ids:
        raise MissingAggregateInputIdsError(missing_input_ids, concepts)
    return concepts


def parse_aggregate_response_parts(
    raw: str,
    inputs: list[AggregateInputRecord],
) -> tuple[list[AggregateConceptRecord], list[str]]:
    payload = decode_first_json_payload(strip_markdown_fence(raw))
    items = payload if isinstance(payload, list) else payload.get("items", [])
    if not isinstance(items, list):
        raise ValueError("Aggregate response must contain a list of items.")
    prompt_id_map = build_prompt_id_map(inputs)
    reverse_prompt_id_map = {str(value): key for key, value in prompt_id_map.items()}
    items_by_id: dict[str, dict[str, Any]] = {}
    for item in items:
        normalized_prompt_id = normalize_prompt_item_id(item.get("id"))
        if normalized_prompt_id is None:
            continue
        source_id = reverse_prompt_id_map.get(normalized_prompt_id)
        if source_id is None:
            continue
        items_by_id[source_id] = item

    records: list[AggregateConceptRecord] = []
    missing_input_ids: list[str] = []
    for expected in inputs:
        item = items_by_id.get(expected.id)
        if item is None:
            missing_input_ids.append(expected.id)
            continue
        raw_concepts = item.get("concepts", [])
        if not isinstance(raw_concepts, list):
            raise ValueError(f"Aggregate response concepts for {expected.id} must be a list.")
        records.extend(flatten_structured_concepts(expected.id, postprocess_aggregate_concepts(raw_concepts)))
    return records, missing_input_ids


def postprocess_aggregate_concepts(raw_concepts: list[dict[str, Any]]) -> list[AggregateCoreConcept]:
    normalized: list[AggregateCoreConcept] = []
    seen_core_names: set[str] = set()
    for value in raw_concepts:
        if not isinstance(value, dict):
            continue
        concept_name = postprocess_concept_name(str(value.get("concept", "")))
        if not concept_name or concept_name in seen_core_names:
            continue
        description = postprocess_description(str(value.get("description", "")), concept_name)
        if not description:
            continue
        subordinates = postprocess_subordinates(value.get("subordinates", []), parent_name=concept_name)
        seen_core_names.add(concept_name)
        normalized.append(
            AggregateCoreConcept(
                concept=concept_name,
                description=description,
                subordinates=subordinates,
            )
        )
    return normalized


def postprocess_subordinates(raw_subordinates: Any, *, parent_name: str) -> list[AggregateSubordinateConcept]:
    if not isinstance(raw_subordinates, list):
        return []
    normalized: list[AggregateSubordinateConcept] = []
    seen_names: set[str] = set()
    for value in raw_subordinates:
        if not isinstance(value, dict):
            continue
        concept_name = postprocess_concept_name(str(value.get("concept", "")))
        if not concept_name or concept_name == parent_name or concept_name in seen_names:
            continue
        description = postprocess_description(str(value.get("description", "")), concept_name)
        if not description:
            continue
        seen_names.add(concept_name)
        normalized.append(
            AggregateSubordinateConcept(
                concept=concept_name,
                description=description,
            )
        )
    return normalized


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


def build_prompt_id_map(inputs: list[AggregateInputRecord]) -> dict[str, int]:
    return {row.id: index for index, row in enumerate(inputs, start=1)}


def normalize_prompt_item_id(value: Any) -> str | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return None
    text = normalize_text(str(value))
    if not text or not text.isdigit():
        return None
    normalized = text.lstrip("0")
    return normalized or "0"
