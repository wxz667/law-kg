from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from utils.llm.base import ProviderRequestConfig, ProviderResponseError, build_provider_request_config, validate_provider_api_params

from ...pipeline.runtime import resolve_builder_substage_config
from ...contracts import ExtractConceptItem, ExtractConceptRecord, ExtractInputRecord
from .postprocess import normalize_text, postprocess_concept_items

PROMPT = """# 角色
你是专业的法律知识图谱本体架构师。

# 任务
为给定的法律文本，提取出最能代表该文本核心的“宏观法律实体”，作为知识图谱的节点标识，并提供简要描述。
- **数量限制**：必须强制收敛，每个章节仅提取 **1-6 个** 核心概念，绝对上限为 8 个。
- **返回空值**：如果该章节无核心实体，请勇敢地返回 `[]`。

# 核心概念的正向画像
1. **法定制度名**：如“海难救助”、“宣告失踪”、“船舶融资租赁”、“民事诉讼简易程序”。
2. **核心法律客体/主体**：如“危险货物”、“船舶所有权”、“遗产管理人”。
3. **独立的法定状态**：如“财产无主”、“合同效力”。

# 要求
- 概念名称必须是有字面意涵的法律术语，禁止"xx法一般规定"“xxx术语”等无实际意义的名称。
- 不要抽取法律名称、时间地点等具体对象
- 不要抽取局部语义成立的概念，例如“本法”、“各级各类学校”等应当丢弃
- 不要单独的处罚后果（罚款、拘留等,如果有必须包含前件）
- 只保留核心术语“不安抗辩权”，清理冗余后缀（“定义”、“规定”、“法律”、“制度”、“机制”等）
- 每个概念的描述请控制在 **30 - 100 字**。
- 描述必须直接说明内容，不要写“是指 / 表示 / 定义为 / 系指 / 即”等废话开头。
- 不要复制整句法条，不要写成长段解释，做总结归纳。

# 输出格式和结果示例
只返回严格 JSON 数组，不要输出任何额外说明：
[
  {
    "id": 1,
    "concepts": [
      {"name": "不安抗辩权", "description": "在双务合同中，应当先履行债务的当事人，有确切证据证明对方丧失履行能力时，在对方恢复履行能力或提供担保前，有权中止履行合同"}
    ]
  }
]
"""


@dataclass(frozen=True)
class ExtractRuntimeConfig:
    provider: str
    model: str
    batch_size: int
    concurrent_requests: int
    request_timeout_seconds: int
    max_retries: int
    params: dict[str, Any]
    rate_limit: dict[str, Any]


@dataclass(frozen=True)
class ExtractBatchResult:
    concepts: list[ExtractConceptRecord]
    errors: list[dict[str, Any]]
    failed_input_ids: list[str]
    request_count: int = 0
    retry_count: int = 0


class MissingExtractInputIdsError(ValueError):
    def __init__(self, missing_input_ids: list[str], concepts: list[ExtractConceptRecord]) -> None:
        self.missing_input_ids = list(missing_input_ids)
        self.concepts = list(concepts)
        first_missing = self.missing_input_ids[0] if self.missing_input_ids else ""
        super().__init__(f"Extract response is missing input id: {first_missing}")


def resolve_extract_runtime_config(runtime: Any) -> ExtractRuntimeConfig:
    raw = resolve_builder_substage_config(runtime, "extract", "extract")
    provider = str(raw.get("provider", "")).strip()
    model = str(raw.get("model", "")).strip()
    if not provider or not model:
        raise ValueError("builder.extract.extract must define non-empty provider and model.")
    params = validate_provider_api_params(raw.get("params", {}))
    params.setdefault("temperature", 0.0)
    params.setdefault("max_tokens", 2048)
    return ExtractRuntimeConfig(
        provider=provider,
        model=model,
        batch_size=max(int(raw.get("batch_size", 1)), 1),
        concurrent_requests=max(int(raw.get("concurrent_requests", 1)), 1),
        request_timeout_seconds=max(int(raw.get("request_timeout_seconds", 90)), 1),
        max_retries=max(int(raw.get("max_retries", 2)), 1),
        params=params,
        rate_limit=dict(raw.get("rate_limit", {})) if isinstance(raw.get("rate_limit", {}), dict) else {},
    )


def build_request_config(config: ExtractRuntimeConfig) -> ProviderRequestConfig:
    return build_provider_request_config(
        provider=config.provider,
        model=config.model,
        params=config.params,
        timeout_seconds=config.request_timeout_seconds,
        max_retries=config.max_retries,
        rate_limit=config.rate_limit,
    )


def extract_concepts_batch(
    runtime: Any,
    inputs: list[ExtractInputRecord],
) -> ExtractBatchResult:
    if not inputs:
        return ExtractBatchResult(concepts=[], errors=[], failed_input_ids=[])
    runtime_config = resolve_extract_runtime_config(runtime)
    request_config = build_request_config(runtime_config)
    errors: list[dict[str, Any]] = []
    concepts_by_input_id: dict[str, ExtractConceptRecord] = {}
    pending_inputs: list[ExtractInputRecord] = []
    request_count = 0

    try:
        request_count += 1
        raw = runtime.generate_text(build_extract_prompt(inputs), request_config)
        concepts, missing_input_ids = parse_extract_response_parts(raw, inputs)
        concepts_by_input_id.update({row.id: row for row in concepts})
        if not missing_input_ids:
            return ExtractBatchResult(
                concepts=[concepts_by_input_id[row.id] for row in inputs if row.id in concepts_by_input_id],
                errors=[],
                failed_input_ids=[],
                request_count=request_count,
                retry_count=0,
            )
        missing_error = MissingExtractInputIdsError(missing_input_ids, concepts)
        errors.append(_format_extract_error(missing_error, 1, inputs))
        missing_set = set(missing_input_ids)
        pending_inputs = [row for row in inputs if row.id in missing_set]
    except (ProviderResponseError, ValueError, json.JSONDecodeError) as exc:
        errors.append(_format_extract_error(exc, 1, inputs))
        pending_inputs = list(inputs)
    except Exception as exc:
        errors.append(_format_extract_error(exc, 1, inputs))
        pending_inputs = list(inputs)

    failed_input_ids: list[str] = []
    for row in pending_inputs:
        recovered = False
        for attempt in range(2, runtime_config.max_retries + 1):
            try:
                request_count += 1
                raw = runtime.generate_text(build_extract_prompt([row]), request_config)
                concepts = parse_extract_response(raw, [row])
                if concepts:
                    concepts_by_input_id[row.id] = concepts[0]
                else:
                    concepts_by_input_id[row.id] = ExtractConceptRecord(id=row.id, concepts=[])
                recovered = True
                break
            except (ProviderResponseError, ValueError, json.JSONDecodeError) as exc:
                errors.append(_format_extract_error(exc, attempt, [row]))
            except Exception as exc:
                errors.append(_format_extract_error(exc, attempt, [row]))
        if not recovered:
            failed_input_ids.append(row.id)

    concepts = [concepts_by_input_id[row.id] for row in inputs if row.id in concepts_by_input_id]
    if not failed_input_ids:
        errors = []
    return ExtractBatchResult(
        concepts=concepts,
        errors=errors,
        failed_input_ids=failed_input_ids,
        request_count=request_count,
        retry_count=max(request_count - 1, 0),
    )


def _format_extract_error(exc: Exception, attempt: int, inputs: list[ExtractInputRecord]) -> dict[str, Any]:
    return {
        "error_type": exc.__class__.__name__,
        "message": str(exc),
        "attempt": attempt,
        "input_ids": [row.id for row in inputs],
    }


def build_extract_prompt(inputs: list[ExtractInputRecord]) -> list[dict[str, str]]:
    prompt_id_map = build_prompt_id_map(inputs)
    payload = [
        {
            "id": prompt_id_map[row.id],
            "hierarchy": row.hierarchy,
            "content": row.content,
        }
        for row in inputs
    ]
    user = (
        "请为以下法律结构单元输出结构化概念"
        "严格遵守要求和输出格式"
        "\n"
        + json.dumps(payload, ensure_ascii=False)
    )
    return [{"role": "system", "content": PROMPT}, {"role": "user", "content": user}]


def parse_extract_response(raw: str, inputs: list[ExtractInputRecord]) -> list[ExtractConceptRecord]:
    concepts, missing_input_ids = parse_extract_response_parts(raw, inputs)
    if missing_input_ids:
        raise MissingExtractInputIdsError(missing_input_ids, concepts)
    return concepts


def parse_extract_response_parts(
    raw: str,
    inputs: list[ExtractInputRecord],
) -> tuple[list[ExtractConceptRecord], list[str]]:
    payload = decode_first_json_payload(strip_markdown_fence(raw))
    items = payload if isinstance(payload, list) else payload.get("items", [])
    if not isinstance(items, list):
        raise ValueError("Extract response must contain a list of items.")
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

    concepts: list[ExtractConceptRecord] = []
    missing_input_ids: list[str] = []
    for expected in inputs:
        item = items_by_id.get(expected.id)
        if item is None:
            missing_input_ids.append(expected.id)
            continue
        raw_concepts = item.get("concepts", [])
        if not isinstance(raw_concepts, list):
            raise ValueError(f"Extract response concepts for {expected.id} must be a list.")
        concept_items = postprocess_concept_items(
            [
                ExtractConceptItem(
                    name=normalize_text(str(concept_item.get("name", ""))),
                    description=normalize_text(str(concept_item.get("description", ""))),
                )
                for concept_item in raw_concepts
                if isinstance(concept_item, dict)
            ]
        )
        concepts.append(ExtractConceptRecord(id=expected.id, concepts=concept_items))
    return concepts, missing_input_ids


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


def build_prompt_id_map(inputs: list[ExtractInputRecord]) -> dict[str, int]:
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
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        normalized = text.lstrip("0")
        return normalized or "0"
    return None
