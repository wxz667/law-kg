from __future__ import annotations

from typing import Any

from .client import LLMClient
from .config import StageModelConfig

LEAF_SUMMARY_SYSTEM_PROMPT = """
你是法律知识图谱构建器中的摘要模块。
你的任务是对单个法律结构节点生成简洁、可抽取、可追溯的摘要。
只概括文本明示内容，保留法律术语，不做扩展解释，不引入条外事实。
""".strip()

PARENT_SUMMARY_SYSTEM_PROMPT = """
你是法律知识图谱构建器中的目录聚合摘要模块。
你的任务是基于子节点摘要，自底向上生成父级目录摘要。
只概括共同主题和规制范围，不扩展解释，不引入未出现在子摘要中的新事实。
""".strip()


def summarize_leaf_node(
    node_context: dict[str, Any],
    model_config: StageModelConfig,
    client: LLMClient,
) -> str:
    prompt = (
        f"节点类型: {node_context['type']}\n"
        f"层级: {node_context['level']}\n"
        f"名称: {node_context['name']}\n"
        f"上级路径: {node_context.get('path', '')}\n"
        f"原文:\n{node_context['text']}\n\n"
        "请输出一段中文摘要，控制在120字以内，保留法律术语，只概括明示内容。"
    )
    return client.generate_text(prompt=prompt, system_prompt=LEAF_SUMMARY_SYSTEM_PROMPT)


def summarize_parent_from_children(
    parent_context: dict[str, Any],
    child_summaries: list[dict[str, str]],
    model_config: StageModelConfig,
    client: LLMClient,
) -> str:
    child_lines = "\n".join(
        f"- {item['name']}: {item['summary']}" for item in child_summaries if item.get("summary")
    )
    prompt = (
        f"父节点类型: {parent_context['type']}\n"
        f"层级: {parent_context['level']}\n"
        f"名称: {parent_context['name']}\n"
        f"上级路径: {parent_context.get('path', '')}\n"
        "子节点摘要如下:\n"
        f"{child_lines}\n\n"
        "请基于这些子节点摘要生成父节点摘要，控制在120字以内。"
    )
    return client.generate_text(prompt=prompt, system_prompt=PARENT_SUMMARY_SYSTEM_PROMPT)
