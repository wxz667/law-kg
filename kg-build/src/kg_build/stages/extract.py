from __future__ import annotations

import json
import re
from typing import Any

from ..contracts import EdgeRecord, GraphBundle, NodeRecord
from ..llm import build_llm_client, resolve_stage_model
from ..pipeline.executor import (
    clear_checkpoint_files,
    resolve_execution_config,
    run_independent_tasks,
)
from ..utils.ids import checksum_text, slugify
from ..utils.numbers import chinese_number_to_int
from ..utils.progress import StageProgressReporter
from ..utils.semantics import build_structural_maps, is_semantic_leaf

EXTRACT_SYSTEM_PROMPT = """
你是法律知识图谱构建器中的实体与显式关系抽取模块。
请仅依据当前叶子节点原文抽取实体与显式关系，父级摘要仅用于理解上下文，不得作为独立证据。
只输出当前文本能直接支持的实体和关系，不得做学理扩展或常识推断。
若关系不明确，请不要输出该关系。
请严格输出 JSON，不要附加解释文字。
""".strip()

ALLOWED_RELATION_TYPES = {
    "REFERENCE_TO",
    "ENTITY_RELATED",
    "CONDITION_OF",
    "PENALTY_OF",
    "EXCEPTION_TO",
}

ENTITY_TYPE_CHOICES = (
    "subject",
    "action",
    "object",
    "condition",
    "penalty",
    "exception",
    "concept",
    "reference",
)

ARTICLE_REFERENCE_RE = re.compile(
    r"(第([一二三四五六七八九十百千万零两〇0-9]+)条"
    r"(?:之([一二三四五六七八九十百千万零两〇0-9]+))?"
    r"(?:第([一二三四五六七八九十百千万零两〇0-9]+)款)?"
    r"(?:第([一二三四五六七八九十百千万零两〇0-9]+)项)?"
    r"(?:第([一二三四五六七八九十百千万零两〇0-9]+)目)?)"
)


def run(
    bundle: GraphBundle,
    show_progress: bool = False,
    stage_dir=None,
    reporter: StageProgressReporter | None = None,
) -> tuple[GraphBundle, str]:
    config = resolve_stage_model("extract")
    client = build_llm_client(config)
    node_index = {node.id: node for node in bundle.nodes}
    children, parent_of = build_structural_maps(bundle)
    target_nodes = select_extract_nodes(node_index, children)

    existing_node_ids = {node.id for node in bundle.nodes}
    existing_edge_ids = {edge.id for edge in bundle.edges}
    extracted_entities = 0
    extracted_relations = 0
    extracted_references = 0
    reference_lookup = build_reference_lookup(bundle)

    if reporter is not None and show_progress:
        reporter.stage_started("extract", total_items=len(target_nodes))

    execution_config = resolve_execution_config("extract", config.provider, config.model, config.params)
    tasks = [{"task_id": f"extract:{node.id}", "node_id": node.id} for node in target_nodes]

    def execute_task(task: dict[str, str]) -> dict[str, Any]:
        node = node_index[task["node_id"]]
        system_prompt, prompt = build_extract_prompt(node, node_index, children, parent_of)
        raw_output = client.generate_text(prompt=prompt, system_prompt=system_prompt)
        payload = parse_model_payload(raw_output, node.id)
        return {
            "task_id": task["task_id"],
            "node_id": node.id,
            "payload": payload,
        }

    def apply_result(result: dict[str, Any]) -> None:
        nonlocal extracted_entities, extracted_relations, extracted_references
        owner_node = node_index[result["node_id"]]
        local_entities = register_entities_for_node(
            owner_node=owner_node,
            payload=result["payload"],
            bundle=bundle,
            existing_node_ids=existing_node_ids,
            existing_edge_ids=existing_edge_ids,
        )
        extracted_entities += len(local_entities)
        extracted_relations += register_relations_for_node(
            owner_node=owner_node,
            payload=result["payload"],
            local_entities=local_entities,
            bundle=bundle,
            existing_edge_ids=existing_edge_ids,
        )
        extracted_references += register_reference_edges_for_node(
            owner_node=owner_node,
            bundle=bundle,
            existing_edge_ids=existing_edge_ids,
            reference_lookup=reference_lookup,
        )

    if stage_dir is None:
        for task in tasks:
            apply_result(execute_task(task))
    else:
        run_independent_tasks(
            execution_config=execution_config,
            stage_dir=stage_dir,
            tasks=tasks,
            execute_task=execute_task,
            apply_result=apply_result,
            reporter=reporter if show_progress else None,
        )
        clear_checkpoint_files(stage_dir)

    note = (
        "completed legal entity extraction "
        f"[provider={config.provider} model={config.model} purpose={config.purpose}] "
        f"[target_nodes={len(target_nodes)} entities_created={extracted_entities} "
        f"explicit_relations_created={extracted_relations} references_created={extracted_references}]"
    )
    return bundle, note


def select_extract_nodes(
    node_index: dict[str, NodeRecord],
    children: dict[str, list[str]],
) -> list[NodeRecord]:
    selected: list[NodeRecord] = []
    for node_id, node in node_index.items():
        if node.type not in {"ProvisionNode", "AppendixNode", "AppendixItemNode"}:
            continue
        if not is_semantic_leaf(node_id, node_index, children):
            continue
        if not node.text.strip():
            continue
        selected.append(node)
    return selected


def build_extract_prompt(
    node: NodeRecord,
    node_index: dict[str, NodeRecord],
    children: dict[str, list[str]],
    parent_of: dict[str, str],
) -> tuple[str, str]:
    parent_summary = nearest_parent_summary(node.id, node_index, parent_of)
    toc_summary = nearest_toc_summary(node.id, node_index, parent_of)
    prompt = (
        f"节点名称: {node.name}\n"
        f"节点层级: {node.level}\n"
        f"节点原文:\n{node.text.strip()}\n\n"
        f"父级聚合摘要: {parent_summary}\n"
        f"章节聚合摘要: {toc_summary}\n\n"
        "请抽取当前节点文本中明确出现的法律实体和显式关系。\n"
        f"实体类型可从以下集合中选择最接近的一类: {', '.join(ENTITY_TYPE_CHOICES)}。\n"
        "返回 JSON，格式如下：\n"
        "{\n"
        '  "entities": [\n'
        '    {"name": "实体名", "entity_type": "concept", "description": "短描述", "evidence": "来自当前原文的证据片段"}\n'
        "  ],\n"
        '  "relations": [\n'
        '    {"type": "ENTITY_RELATED", "source_name": "实体A", "target_name": "实体B", "evidence": "来自当前原文的证据片段"}\n'
        "  ]\n"
        "}\n"
        "只允许输出以下关系类型：ENTITY_RELATED、CONDITION_OF、PENALTY_OF、EXCEPTION_TO。\n"
        "不要输出 REFERENCE_TO，文本引用关系由系统规则单独处理。\n"
        "如果没有明确关系，relations 返回空数组。"
    )
    return EXTRACT_SYSTEM_PROMPT, prompt


def nearest_parent_summary(
    node_id: str,
    node_index: dict[str, NodeRecord],
    parent_of: dict[str, str],
) -> str:
    current_id = parent_of.get(node_id)
    while current_id:
        current = node_index[current_id]
        if current.summary.strip():
            return current.summary.strip()
        current_id = parent_of.get(current_id)
    return ""


def nearest_toc_summary(
    node_id: str,
    node_index: dict[str, NodeRecord],
    parent_of: dict[str, str],
) -> str:
    current_id = parent_of.get(node_id)
    while current_id:
        current = node_index[current_id]
        if current.type == "TocNode" and current.summary.strip():
            return current.summary.strip()
        current_id = parent_of.get(current_id)
    return ""


def parse_model_payload(raw_output: str, node_id: str) -> dict[str, Any]:
    content = raw_output.strip()
    fenced_match = re.search(r"```(?:json)?\s*(\{.*\})\s*```", content, flags=re.DOTALL)
    if fenced_match:
        content = fenced_match.group(1)
    else:
        start = content.find("{")
        end = content.rfind("}")
        if start != -1 and end != -1 and start < end:
            content = content[start : end + 1]
    try:
        payload = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON extraction output for node {node_id}: {raw_output}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Extraction output for node {node_id} must be a JSON object.")
    payload.setdefault("entities", [])
    payload.setdefault("relations", [])
    return payload


def register_entities_for_node(
    owner_node: NodeRecord,
    payload: dict[str, Any],
    bundle: GraphBundle,
    existing_node_ids: set[str],
    existing_edge_ids: set[str],
) -> dict[str, NodeRecord]:
    local_entities: dict[str, NodeRecord] = {}
    for row in payload.get("entities", []):
        normalized = normalize_entity_row(row, owner_node)
        if not normalized:
            continue
        entity_node = build_entity_node(owner_node, normalized)
        existing_node = next((node for node in bundle.nodes if node.id == entity_node.id), None)
        if existing_node is None:
            bundle.nodes.append(entity_node)
            existing_node_ids.add(entity_node.id)
        else:
            entity_node = existing_node
        local_entities[normalized["canonical_name"]] = entity_node
        has_entity_edge = build_has_entity_edge(owner_node.id, entity_node.id, normalized["evidence"])
        if has_entity_edge.id not in existing_edge_ids:
            bundle.edges.append(has_entity_edge)
            existing_edge_ids.add(has_entity_edge.id)
    return local_entities


def normalize_entity_row(row: Any, owner_node: NodeRecord) -> dict[str, str] | None:
    if not isinstance(row, dict):
        return None
    name = str(row.get("name", "")).strip()
    if not name:
        return None
    entity_type = str(row.get("entity_type", "concept")).strip().lower() or "concept"
    if entity_type not in ENTITY_TYPE_CHOICES:
        entity_type = "concept"
    description = str(row.get("description", "")).strip()
    evidence = str(row.get("evidence", "")).strip() or name
    if owner_node.text.strip():
        if evidence not in owner_node.text:
            if name in owner_node.text:
                evidence = name
            else:
                return None
    canonical_name = re.sub(r"\s+", " ", name)
    return {
        "canonical_name": canonical_name,
        "entity_type": entity_type,
        "description": description[:120],
        "evidence": evidence,
    }


def build_entity_node(
    owner_node: NodeRecord,
    normalized: dict[str, str],
) -> NodeRecord:
    digest = checksum_text(
        f"{owner_node.id}|{normalized['canonical_name']}|{normalized['entity_type']}"
    )[:12]
    entity_id = f"entity:{owner_node.source_id}:{slugify(normalized['entity_type'])}:{digest}"
    description = normalized["description"] or normalized["evidence"]
    return NodeRecord(
        id=entity_id,
        type="EntityNode",
        name=normalized["canonical_name"],
        level="entity",
        source_id=owner_node.source_id,
        description=description,
        metadata={
            "entity_type": normalized["entity_type"],
            "source_node_id": owner_node.id,
            "extracted_by": "llm_extract",
        },
    )


def build_has_entity_edge(source_id: str, target_id: str, evidence_text: str) -> EdgeRecord:
    edge_id = f"edge:has_entity:{checksum_text(f'{source_id}|{target_id}')[:12]}"
    return EdgeRecord(
        id=edge_id,
        source=source_id,
        target=target_id,
        type="HAS_ENTITY",
        evidence=[{"source_node_id": source_id, "text": evidence_text}],
        metadata={"extracted_by": "llm_extract"},
    )


def register_relations_for_node(
    owner_node: NodeRecord,
    payload: dict[str, Any],
    local_entities: dict[str, NodeRecord],
    bundle: GraphBundle,
    existing_edge_ids: set[str],
) -> int:
    created = 0
    for row in payload.get("relations", []):
        normalized = normalize_relation_row(row, owner_node)
        if not normalized:
            continue
        source_entity = local_entities.get(normalized["source_name"])
        target_entity = local_entities.get(normalized["target_name"])
        if source_entity is None or target_entity is None:
            continue
        edge = build_relation_edge(
            relation_type=normalized["type"],
            source_id=source_entity.id,
            target_id=target_entity.id,
            owner_node_id=owner_node.id,
            evidence_text=normalized["evidence"],
        )
        if edge.id in existing_edge_ids:
            continue
        bundle.edges.append(edge)
        existing_edge_ids.add(edge.id)
        created += 1
    return created


def normalize_relation_row(row: Any, owner_node: NodeRecord) -> dict[str, str] | None:
    if not isinstance(row, dict):
        return None
    relation_type = str(row.get("type", "")).strip()
    if relation_type not in ALLOWED_RELATION_TYPES:
        return None
    source_name = re.sub(r"\s+", " ", str(row.get("source_name", "")).strip())
    target_name = re.sub(r"\s+", " ", str(row.get("target_name", "")).strip())
    if not source_name or not target_name:
        return None
    evidence = str(row.get("evidence", "")).strip()
    if not evidence:
        return None
    if evidence not in owner_node.text:
        return None
    return {
        "type": relation_type,
        "source_name": source_name,
        "target_name": target_name,
        "evidence": evidence,
    }


def build_reference_lookup(bundle: GraphBundle) -> dict[str, Any]:
    document_node = next((node for node in bundle.nodes if node.type == "DocumentNode"), None)
    appendix_by_label = {
        str(node.metadata.get("appendix_label", "")).strip(): node
        for node in bundle.nodes
        if node.level == "appendix" and str(node.metadata.get("appendix_label", "")).strip()
    }
    provision_by_address: dict[tuple[int, int | None, int | None, int | None, int | None], NodeRecord] = {}
    for node in bundle.nodes:
        if node.type != "ProvisionNode":
            continue
        address = node.address or {}
        article_no = address.get("article_no")
        if article_no is None:
            continue
        key = (
            article_no,
            address.get("article_suffix"),
            address.get("paragraph_no"),
            address.get("item_no"),
            address.get("sub_item_no"),
        )
        provision_by_address[key] = node
    return {
        "document": document_node,
        "appendix_by_label": appendix_by_label,
        "provision_by_address": provision_by_address,
    }


def register_reference_edges_for_node(
    owner_node: NodeRecord,
    bundle: GraphBundle,
    existing_edge_ids: set[str],
    reference_lookup: dict[str, Any],
) -> int:
    created = 0
    for target_id, evidence in resolve_reference_targets(owner_node, reference_lookup):
        edge = build_reference_edge(owner_node.id, target_id, evidence)
        if edge.id in existing_edge_ids:
            continue
        bundle.edges.append(edge)
        existing_edge_ids.add(edge.id)
        created += 1
    return created


def resolve_reference_targets(
    owner_node: NodeRecord,
    reference_lookup: dict[str, Any],
) -> list[tuple[str, str]]:
    text = owner_node.text.strip()
    if not text:
        return []
    resolved: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    document_node = reference_lookup.get("document")
    if document_node is not None and "本法" in text:
        item = (document_node.id, "本法")
        if item not in seen:
            resolved.append(item)
            seen.add(item)

    for label, appendix_node in reference_lookup.get("appendix_by_label", {}).items():
        if label and label in text:
            item = (appendix_node.id, label)
            if item not in seen:
                resolved.append(item)
                seen.add(item)

    for match in ARTICLE_REFERENCE_RE.finditer(text):
        full_text = match.group(1)
        article_no = chinese_number_to_int(match.group(2))
        article_suffix = chinese_number_to_int(match.group(3)) if match.group(3) else None
        paragraph_no = chinese_number_to_int(match.group(4)) if match.group(4) else None
        item_no = chinese_number_to_int(match.group(5)) if match.group(5) else None
        sub_item_no = chinese_number_to_int(match.group(6)) if match.group(6) else None
        key = (article_no, article_suffix, paragraph_no, item_no, sub_item_no)
        target = reference_lookup["provision_by_address"].get(key)
        if target is None and paragraph_no is None and item_no is None and sub_item_no is None:
            key = (article_no, article_suffix, None, None, None)
            target = reference_lookup["provision_by_address"].get(key)
        if target is None:
            continue
        item = (target.id, full_text)
        if item not in seen:
            resolved.append(item)
            seen.add(item)
    return resolved


def build_reference_edge(source_id: str, target_id: str, evidence_text: str) -> EdgeRecord:
    edge_id = f"edge:{slugify('REFERENCE_TO')}:{checksum_text(f'{source_id}|{target_id}|{evidence_text}')[:12]}"
    return EdgeRecord(
        id=edge_id,
        source=source_id,
        target=target_id,
        type="REFERENCE_TO",
        evidence=[{"source_node_id": source_id, "text": evidence_text}],
        metadata={"extracted_by": "extract_rule_reference"},
    )


def build_relation_edge(
    relation_type: str,
    source_id: str,
    target_id: str,
    owner_node_id: str,
    evidence_text: str,
) -> EdgeRecord:
    edge_id = f"edge:{slugify(relation_type)}:{checksum_text(f'{source_id}|{target_id}|{relation_type}|{owner_node_id}')[:12]}"
    return EdgeRecord(
        id=edge_id,
        source=source_id,
        target=target_id,
        type=relation_type,
        evidence=[{"source_node_id": owner_node_id, "text": evidence_text}],
        metadata={"extracted_by": "llm_extract"},
    )
