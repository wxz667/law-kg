from __future__ import annotations

import re

from ..contracts import GraphBundle, NodeRecord
from ..llm import build_llm_client, resolve_stage_model
from ..pipeline.executor import (
    clear_checkpoint_files,
    resolve_execution_config,
    run_layered_tasks,
)
from ..utils.progress import StageProgressReporter
from ..utils.semantics import (
    build_structural_maps,
    get_semantic_input,
    is_semantic_aggregate,
    is_semantic_leaf,
)

PARENT_SUMMARY_SYSTEM_PROMPT = """
你是法律知识图谱构建器中的目录聚合摘要模块。
你的任务是基于当前节点的引导性正文与下级规范文本，自底向上生成父级聚合摘要。
只概括共同主题、规制对象和适用范围，不做解释，不做推理，不引入条外事实。
不要逐条复述输入内容，不要使用“本条规定”“本款明确”这类近原文改写句式。
""".strip()


def run(
    bundle: GraphBundle,
    show_progress: bool = False,
    stage_dir=None,
    reporter: StageProgressReporter | None = None,
) -> tuple[GraphBundle, str]:
    config = resolve_stage_model("summarize")
    client = build_llm_client(config)
    node_index = {node.id: node for node in bundle.nodes}
    node_order = {node.id: index for index, node in enumerate(bundle.nodes)}
    children, parent_of = build_structural_maps(bundle)

    semantic_leaf_count = count_semantic_leaf_nodes(node_index, children)
    aggregate_layers = build_aggregate_layers(node_index, children, parent_of, node_order)
    aggregate_nodes = [task["node_id"] for layer in aggregate_layers for task in layer]
    aggregate_done = 0
    if reporter is not None and show_progress:
        reporter.stage_started("summarize", total_items=len(aggregate_nodes))

    execution_config = resolve_execution_config("summarize", config.provider, config.model, config.params)

    def execute_task(task: dict[str, str]) -> dict[str, str]:
        node = node_index[task["node_id"]]
        aggregate_inputs = build_aggregate_inputs(node, node_index, children)
        if not aggregate_inputs:
            raise ValueError(f"Aggregate summary node {node.id} does not have summarized structural children.")
        system_prompt, prompt = build_parent_summary_prompt(
            build_node_context(node, parent_of, node_index),
            aggregate_inputs,
        )
        summary = client.generate_text(prompt=prompt, system_prompt=system_prompt)
        return {
            "task_id": task["task_id"],
            "node_id": node.id,
            "summary": normalize_summary(summary, source_text="", node_id=node.id),
        }

    def apply_result(result: dict[str, str]) -> None:
        nonlocal aggregate_done
        node_index[result["node_id"]].summary = result["summary"]
        aggregate_done += 1

    if stage_dir is None:
        for layer in aggregate_layers:
            for task in layer:
                apply_result(execute_task(task))
    else:
        run_layered_tasks(
            execution_config=execution_config,
            stage_dir=stage_dir,
            task_layers=aggregate_layers,
            execute_task=execute_task,
            apply_result=apply_result,
            reporter=reporter if show_progress else None,
        )
        clear_checkpoint_files(stage_dir)

    note = (
        "completed legal aggregate summarization "
        f"[provider={config.provider} model={config.model} purpose={config.purpose}] "
        f"[semantic_leaf_nodes={semantic_leaf_count} "
        f"aggregate_candidates={len(aggregate_nodes)} aggregate_completed={aggregate_done}]"
    )
    return bundle, note


def build_parent_summary_prompt(
    parent_context: dict[str, str],
    aggregate_inputs: list[dict[str, str]],
) -> tuple[str, str]:
    child_lines = "\n".join(
        f"- {item['name']}: {item['content']}" for item in aggregate_inputs if item.get("content")
    )
    prompt = (
        f"父节点类型: {parent_context['type']}\n"
        f"层级: {parent_context['level']}\n"
        f"名称: {parent_context['name']}\n"
        f"上级路径: {parent_context.get('path', '')}\n"
        "聚合输入如下:\n"
        f"{child_lines}\n\n"
        "请基于这些输入生成一段40到80字的中文聚合摘要。"
        "摘要应压缩共同主题、规制对象和适用范围，不要逐条复述，不要写成法条释义。"
    )
    return PARENT_SUMMARY_SYSTEM_PROMPT, prompt


def select_aggregate_summary_nodes(
    node_index: dict[str, NodeRecord],
    children: dict[str, list[str]],
) -> list[NodeRecord]:
    selected = []
    for node in node_index.values():
        if is_semantic_aggregate(node.id, node_index, children):
            selected.append(node)
    return selected


def build_aggregate_layers(
    node_index: dict[str, NodeRecord],
    children: dict[str, list[str]],
    parent_of: dict[str, str],
    node_order: dict[str, int],
) -> list[list[dict[str, str]]]:
    aggregate_nodes = select_aggregate_summary_nodes(node_index, children)
    if not aggregate_nodes:
        return []
    grouped: dict[int, list[NodeRecord]] = {}
    for node in aggregate_nodes:
        depth = node_depth(node.id, parent_of)
        grouped.setdefault(depth, []).append(node)
    layers: list[list[dict[str, str]]] = []
    for depth in sorted(grouped, reverse=True):
        layer_nodes = sorted(grouped[depth], key=lambda item: node_order[item.id])
        layers.append(
            [
                {"task_id": f"summarize:{node.id}", "node_id": node.id}
                for node in layer_nodes
            ]
        )
    return layers


def count_semantic_leaf_nodes(
    node_index: dict[str, NodeRecord],
    children: dict[str, list[str]],
) -> int:
    return sum(1 for node_id in node_index if is_semantic_leaf(node_id, node_index, children))


def build_aggregate_inputs(
    node: NodeRecord,
    node_index: dict[str, NodeRecord],
    children: dict[str, list[str]],
) -> list[dict[str, str]]:
    aggregate_inputs: list[dict[str, str]] = []
    if node.text.strip():
        aggregate_inputs.append(
            {
                "name": f"{node.name}前导文本",
                "content": node.text.strip(),
            }
        )
    for child_id in children.get(node.id, []):
        child_input = get_semantic_input(child_id, node_index, children)
        if not child_input:
            continue
        aggregate_inputs.append(
            {
                "name": node_index[child_id].name,
                "content": child_input,
            }
        )
    return aggregate_inputs


def build_node_context(node, parent_of: dict[str, str], node_index: dict[str, object]) -> dict[str, str]:
    return {
        "id": node.id,
        "type": node.type,
        "level": node.level,
        "name": node.name,
        "text": node.text,
        "path": build_node_path(node.id, parent_of, node_index),
    }


def build_node_path(node_id: str, parent_of: dict[str, str], node_index: dict[str, object]) -> str:
    path_names: list[str] = []
    current_id = parent_of.get(node_id)
    while current_id:
        current_node = node_index[current_id]
        if current_node.level != "document":
            path_names.append(current_node.name)
        current_id = parent_of.get(current_id)
    return " > ".join(reversed(path_names))


def node_depth(node_id: str, parent_of: dict[str, str]) -> int:
    depth = 0
    current_id = node_id
    while current_id in parent_of:
        depth += 1
        current_id = parent_of[current_id]
    return depth


def normalize_summary(raw_summary: str, source_text: str, node_id: str) -> str:
    summary = raw_summary.strip()
    summary = summary.replace("\r", " ").replace("\n", " ")
    summary = re.sub(r"\s+", " ", summary)
    summary = summary.removeprefix("摘要：").removeprefix("摘要:").strip()
    summary = summary.strip("“”\"' ")
    if not summary:
        raise ValueError(f"Summary is empty for node {node_id}.")
    if len(summary) > 180:
        raise ValueError(f"Summary is too long for node {node_id}: {len(summary)} characters.")
    normalized_source = re.sub(r"\s+", " ", source_text.strip())
    if normalized_source and summary == normalized_source:
        raise ValueError(f"Summary for node {node_id} is a raw copy of the source text.")
    return summary
