from __future__ import annotations

from ..contracts import GraphBundle
from ..llm import resolve_stage_model

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


TODO_NOTE = "TODO: implement production-grade legal summarization for provisions and catalog nodes."


def run(bundle: GraphBundle) -> tuple[GraphBundle, str]:
    config = resolve_stage_model("summarize")
    note = (
        f"{TODO_NOTE} "
        f"[provider={config.provider} model={config.model} purpose={config.purpose}]"
    )
    return bundle, note


def build_leaf_summary_prompt(node_context: dict[str, str]) -> tuple[str, str]:
    prompt = (
        f"节点类型: {node_context['type']}\n"
        f"层级: {node_context['level']}\n"
        f"名称: {node_context['name']}\n"
        f"上级路径: {node_context.get('path', '')}\n"
        f"原文:\n{node_context['text']}\n\n"
        "请输出一段中文摘要，控制在120字以内，保留法律术语，只概括明示内容。"
    )
    return LEAF_SUMMARY_SYSTEM_PROMPT, prompt


def build_parent_summary_prompt(
    parent_context: dict[str, str],
    child_summaries: list[dict[str, str]],
) -> tuple[str, str]:
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
    return PARENT_SUMMARY_SYSTEM_PROMPT, prompt
