from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from utils.llm.base import ProviderRequestConfig, ProviderResponseError, build_provider_request_config, validate_provider_api_params

from ...contracts import ExtractConceptRecord, ExtractInputRecord
from .postprocess import postprocess_concept_list

PROMPT = """# 角色
你是专业的法律知识图谱本体架构师。

# 任务
为给定的法律章节（JSON输入），提取出最能代表该章节核心的“宏观法律实体”，作为知识图谱的节点标识。
- **数量限制**：必须强制收敛，每个章节仅提取 **1-5 个** 核心概念，绝对上限为 6 个。
- **返回空值**：如果该章节只是细则补充或无核心实体，请勇敢地返回 `[]`。

# 核心概念的“正向画像”（你要找的东西长这样）
1. **法定制度名**：如“海难救助”、“宣告失踪”、“船舶融资租赁”、“民事诉讼简易程序”。
2. **核心法律客体/主体**：如“危险货物”、“船舶所有权”、“遗产管理人”。
3. **独立的法定状态**：如“财产无主”、“合同效力”。

# 标准提取工作流（SOP）

**第一步：锚定领域（Hierarchy 限定）**
读取 `hierarchy` 字段。如果提取出的概念是一个多领域通用的词汇，你**必须**将 `hierarchy` 中的领域词作为前缀补充上去（例如：将“管辖”转化为“民事诉讼地域管辖”）。
注意抽取多个概念时，**每个概念都必须**进行领域锚定，不能只锚定其中一个。

**第二步：向上聚合（寻找最大公约数）**
当前是“章/节”级别的宏观视角。如果你发现自己想提取 5 个以上的概念，说明你陷入了底层细节！
- 将一组连续的操作步骤向上归并为所属的程序。
- 将一个制度的不同情形归并为该制度的核心名词。

**第三步：原子术语化（剥离动态与修饰）**
- 剔除底层的操作细则。
- 剔除无意义的修饰后缀。
- 如果同一组概念之间存在包含关系，请剔除掉被包含的那。

# 排除项（不属于图谱节点的内容）
- 不要带有具体的期限与数字计算（如：期间顺延、审限、十五日）。
- 不要带有细微的执行程序碎片（如：送达回证、移送案件）。
- 不要带有泛化的章节占位标题（如：一般规定、附则、其他情形）。
- 不要带有单独的法律结果后件（如：罚款拘留）。

# 输出格式
仅返回如下严格的 JSON 数组格式，禁止输出任何多余的解释文本：
[
  {"id":"输入id1","concepts":["主干概念1", "主干概念2"]},
  {"id":"输入id2","concepts":[]} 
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


def resolve_extract_runtime_config(runtime: Any) -> ExtractRuntimeConfig:
    raw = dict(runtime.extract_config().get("llm_extract", {}))
    provider = str(raw.get("provider", "")).strip()
    model = str(raw.get("model", "")).strip()
    if not provider or not model:
        raise ValueError("builder.extract.llm_extract must define non-empty provider and model.")
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
    prompt = build_extract_prompt(inputs)
    errors: list[dict[str, Any]] = []
    for attempt in range(1, runtime_config.max_retries + 1):
        try:
            raw = runtime.generate_text(prompt, request_config)
            concepts = parse_extract_response(raw, inputs)
            return ExtractBatchResult(
                concepts=concepts,
                errors=[],
                failed_input_ids=[],
                request_count=attempt,
                retry_count=max(attempt - 1, 0),
            )
        except (ProviderResponseError, ValueError, json.JSONDecodeError) as exc:
            errors.append(
                {
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                    "attempt": attempt,
                    "input_ids": [row.id for row in inputs],
                }
            )
    return ExtractBatchResult(
        concepts=[],
        errors=errors,
        failed_input_ids=[row.id for row in inputs],
        request_count=runtime_config.max_retries,
        retry_count=max(runtime_config.max_retries - 1, 0),
    )


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
        "请对以下法律结构单元输出概念数组。"
        "返回时必须保留输入中的数字 id，不要改写成其他 id。"
        "输出中不区分结构概念和内容概念，只返回统一的 concepts 数组。"
        "\n"
        + json.dumps(payload, ensure_ascii=False)
    )
    return [{"role": "system", "content": PROMPT}, {"role": "user", "content": user}]


def parse_extract_response(
    raw: str,
    inputs: list[ExtractInputRecord],
) -> list[ExtractConceptRecord]:
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
    for expected in inputs:
        item = items_by_id.get(expected.id)
        if item is None:
            raise ValueError(f"Extract response is missing input id: {expected.id}")
        raw_concepts = item.get("concepts", [])
        if not isinstance(raw_concepts, list):
            raise ValueError(f"Extract response concepts for {expected.id} must be a list.")
        concepts.append(
            ExtractConceptRecord(
                id=expected.id,
                concepts=postprocess_concept_list([
                    normalize_text(str(concept_item).strip())
                    for concept_item in raw_concepts
                    if normalize_text(str(concept_item).strip())
                ]),
            )
        )
    return concepts


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


def normalize_text(text: str) -> str:
    return " ".join(text.split())


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
