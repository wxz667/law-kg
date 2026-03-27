from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import re
from typing import Any

from ..config import load_schema
from ..contracts import EdgeRecord, GraphBundle, NodeRecord
from ..llm import build_llm_client, resolve_stage_model
from ..pipeline.executor import (
    clear_checkpoint_files,
    resolve_execution_config,
    run_independent_tasks,
)
from ..utils.ids import checksum_text, slugify
from ..utils.locator import (
    build_reference_lookup,
    resolve_reference_targets,
)
from ..utils.progress import StageProgressReporter
from ..utils.semantics import build_structural_maps, is_semantic_leaf

EXTRACT_SYSTEM_PROMPT = """
你是法律知识图谱构建器中的抽取模块。
只依据当前叶子节点原文抽取实体与显式关系；父级摘要只用于理解上下文，不能作为独立证据。
不要做学理扩展、常识推断或格式外说明。关系不明确时不要输出。

要求：
1. 实体必须是当前条文中可成立的法律语义单元，类型只能是：
subject|action|condition|penalty|concept
2. 处罚实体优先保留完整法定后果表达，例如“三年以上十年以下有期徒刑”“五年以下有期徒刑或者拘役，并处罚金”。
3. description 必须是简短法律化定义，不能写“法律概念/法律行为/处罚概念”等空标签，也不要加入“本法”“本条规定”“这是”“主要指”等冗语。
4. concept 只用于明确定义的法律术语或罪名；如果某短语能直接作为主体、行为、条件或后果成分，则不要输出为 concept。
5. 若某实体的语义成立依赖其他条款内容，例如“依照前款的规定处罚”“本法第七十九条规定的程序”“前款所犯罪行”，则该实体必须标记 is_ref=true。
6. 不要把裸引用锚点直接当实体输出，例如“前款”“第一款”“本法”；只有当引用内容在当前条文中承担明确规范角色时，才输出该实体。
7. 关系类型只能是：
HAS_ACTION|WITH_CONDITION|HAS_PENALTY
8. 关系骨架遵循以下约束：
   - HAS_ACTION: 主体 -> 行为
   - WITH_CONDITION: 行为 -> 条件
   - HAS_PENALTY: 条件 -> 后果；若原文未显式给出条件分支，也可用 行为 -> 后果
9. 若同一行为在不同条件下对应不同后果，必须把每一组“条件 -> 后果”分别显式输出，不能把多个条件和多个后果混成一团。
10. 若一个行为有多个并列条件分支，且每个条件分支对应专门的处罚，则每个条件分支都必须由该行为显式连出：
   - action -WITH_CONDITION-> condition_1
   - condition_1 -HAS_PENALTY-> penalty_1
   - action -WITH_CONDITION-> condition_2
   - condition_2 -HAS_PENALTY-> penalty_2
   不能只输出部分条件分支，也不能只输出 condition -> penalty 而漏掉 action -> condition。
11. 若一个条件短语本身包含多个并列限制，例如“数额较大，拒不退还”“数额巨大或者有其他严重情节”，可作为一个整体 condition 输出；不要为了拆分而拆分。
12. 不要输出与上述骨架无关的弱关系；concept 默认不参与本地骨架关系，除非它在原文中被明确定义并承担必要语义角色。

示例：
- “将代为保管的他人财物非法占为己有，数额较大，拒不退还的，处二年以下有期徒刑、拘役或者罚金；数额巨大或者有其他严重情节的，处二年以上五年以下有期徒刑，并处罚金”
  应输出：
  - action: 将代为保管的他人财物非法占为己有
  - condition: 数额较大，拒不退还
  - penalty: 二年以下有期徒刑、拘役或者罚金
  - condition: 数额巨大或者有其他严重情节
  - penalty: 二年以上五年以下有期徒刑，并处罚金
  - relation: action -WITH_CONDITION-> condition(数额较大，拒不退还)
  - relation: condition(数额较大，拒不退还) -HAS_PENALTY-> penalty(二年以下...)
  - relation: action -WITH_CONDITION-> condition(数额巨大或者有其他严重情节)
  - relation: condition(数额巨大或者有其他严重情节) -HAS_PENALTY-> penalty(二年以上五年以下...)

输出必须是 JSON 对象，且只包含 entities 和 relations 两个字段。

entities 中每个对象格式：
{"name":"...","entity_type":"subject|action|condition|penalty|concept","description":"...","evidence":"...","is_ref":true|false}

relations 中每个对象格式：
{"type":"HAS_ACTION|WITH_CONDITION|HAS_PENALTY","source":"...","target":"...","evidence":"..."}

除 JSON 外不要输出任何文字。
""".strip()

ALLOWED_RELATION_TYPES = {
    "HAS_ACTION",
    "WITH_CONDITION",
    "HAS_PENALTY",
}

ALLOWED_RELATION_SHAPES = {
    "HAS_ACTION": {("subject", "action")},
    "WITH_CONDITION": {("action", "condition")},
    "HAS_PENALTY": {("condition", "penalty"), ("action", "penalty")},
}

@dataclass(frozen=True)
class ExtractContext:
    node_id: str
    node_name: str
    node_level: str
    node_text: str
    parent_summary: str
    toc_summary: str

    def to_prompt_text(self) -> str:
        return "\n".join(
            [
                f"节点名称：{self.node_name}",
                f"节点层级：{self.node_level}",
                f"当前原文：{self.node_text}",
                f"父级摘要：{self.parent_summary or '无'}",
                f"章节摘要：{self.toc_summary or '无'}",
                "",
                "请严格按照系统要求输出 JSON。",
            ]
        )


@dataclass(frozen=True)
class ExtractedEntity:
    canonical_name: str
    surface_text: str
    entity_type: str
    description_seed: str
    evidence_text: str
    owner_node_id: str
    is_ref: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExtractedRelation:
    type: str
    evidence_text: str
    owner_node_id: str
    source: str = ""
    target: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_name(value: str) -> str:
    text = re.sub(r"\s+", "", value.strip())
    text = text.strip("，。；：、,.!?！？（）()[]【】“”\"'《》")
    return text


def fallback_description(entity_type: str, canonical_name: str) -> str:
    return canonical_name


def normalize_entity_type(raw_type: str, allowed_entity_types: set[str]) -> str | None:
    normalized_type = raw_type.strip().lower()
    return normalized_type if normalized_type in allowed_entity_types else None


def normalize_description(
    *,
    entity_type: str,
    canonical_name: str,
    raw_description: str,
) -> str:
    description = raw_description.strip()
    description = re.sub(r"\s+", "", description)
    description = description.strip("，。；：")
    if not description:
        return fallback_description(entity_type, canonical_name)
    return description


def run(
    bundle: GraphBundle,
    show_progress: bool = False,
    stage_dir=None,
    reporter: StageProgressReporter | None = None,
) -> tuple[GraphBundle, str]:
    schema = load_schema()
    config = resolve_stage_model("extract")
    client = build_llm_client(config)
    node_index = {node.id: node for node in bundle.nodes}
    children, parent_of = build_structural_maps(bundle)
    target_nodes = select_extract_nodes(node_index, children)

    existing_node_ids = {node.id for node in bundle.nodes}
    existing_edge_ids = {edge.id for edge in bundle.edges}
    extracted_entities = 0
    extracted_relations = 0
    reference_lookup = build_reference_lookup(bundle)

    if reporter is not None and show_progress:
        reporter.stage_started("extract", total_items=len(target_nodes))

    execution_config = resolve_execution_config("extract", config.provider, config.model, config.params)
    tasks = [{"task_id": f"extract:{node.id}", "node_id": node.id} for node in target_nodes]

    def execute_task(task: dict[str, str]) -> dict[str, Any]:
        node = node_index[task["node_id"]]
        context = build_extract_context(node, node_index, parent_of)
        raw_output = client.generate_text(
            prompt=context.to_prompt_text(),
            system_prompt=EXTRACT_SYSTEM_PROMPT,
        )
        payload = parse_model_payload(raw_output, node.id)
        entities = normalize_entities(
            payload.get("entities", []),
            context,
            allowed_entity_types=set(schema.get("entity_types", [])),
        )
        relations = normalize_relations(
            payload.get("relations", []),
            context,
            entities,
        )
        return {
            "task_id": task["task_id"],
            "node_id": node.id,
            "entities": [item.to_dict() for item in entities.values()],
            "relations": [item.to_dict() for item in relations],
        }

    def apply_result(result: dict[str, Any]) -> None:
        nonlocal extracted_entities, extracted_relations
        owner_node = node_index[result["node_id"]]
        local_entities = materialize_entities(
            owner_node=owner_node,
            entity_rows=result.get("entities", []),
            reference_lookup=reference_lookup,
            bundle=bundle,
            existing_node_ids=existing_node_ids,
            existing_edge_ids=existing_edge_ids,
        )
        extracted_entities += len(local_entities)
        extracted_relations += materialize_relations(
            relation_rows=result.get("relations", []),
            local_entities=local_entities,
            bundle=bundle,
            existing_edge_ids=existing_edge_ids,
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
        f"explicit_relations_created={extracted_relations}]"
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


def build_extract_context(
    node: NodeRecord,
    node_index: dict[str, NodeRecord],
    parent_of: dict[str, str],
) -> ExtractContext:
    return ExtractContext(
        node_id=node.id,
        node_name=node.name,
        node_level=node.level,
        node_text=node.text.strip(),
        parent_summary=nearest_parent_summary(node.id, node_index, parent_of),
        toc_summary=nearest_toc_summary(node.id, node_index, parent_of),
    )


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
        payload = {
            "entities": _safe_parse_json_array_field(content, "entities"),
            "relations": _safe_parse_json_array_field(content, "relations"),
        }
        if not payload["entities"] and not payload["relations"]:
            raise ValueError(f"Invalid JSON extraction output for node {node_id}: {raw_output}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Extraction output for node {node_id} must be a JSON object.")
    payload.setdefault("entities", [])
    payload.setdefault("relations", [])
    return payload


def _safe_parse_json_array_field(content: str, field_name: str) -> list[Any]:
    pattern = f'"{field_name}"'
    start = content.find(pattern)
    if start == -1:
        return []
    bracket_start = content.find("[", start)
    if bracket_start == -1:
        return []
    bracket_end = _find_matching_bracket(content, bracket_start)
    if bracket_end == -1:
        return []
    fragment = content[bracket_start : bracket_end + 1]
    try:
        payload = json.loads(fragment)
    except json.JSONDecodeError:
        return []
    return payload if isinstance(payload, list) else []


def _find_matching_bracket(content: str, start_index: int) -> int:
    depth = 0
    in_string = False
    escaped = False
    for index in range(start_index, len(content)):
        char = content[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
            continue
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                return index
    return -1


def normalize_entities(
    rows: list[Any],
    context: ExtractContext,
    *,
    allowed_entity_types: set[str],
) -> dict[str, ExtractedEntity]:
    normalized: dict[str, ExtractedEntity] = {}
    for row in rows:
        entities = normalize_entity_row(
            row,
            context,
            allowed_entity_types=allowed_entity_types,
        )
        if not entities:
            continue
        for entity in entities:
            current = normalized.get(entity.canonical_name)
            if current is None or _entity_priority(entity) > _entity_priority(current):
                normalized[entity.canonical_name] = entity
    return normalized


def normalize_entity_row(
    row: Any,
    context: ExtractContext,
    *,
    allowed_entity_types: set[str],
) -> list[ExtractedEntity]:
    if not isinstance(row, dict):
        return []
    name = normalize_name(str(row.get("name", "")))
    if not name:
        return []
    evidence = normalize_name(str(row.get("evidence", "")) or name)
    if evidence not in context.node_text:
        if name in context.node_text:
            evidence = name
        else:
            return []
    entity_type = normalize_entity_type(str(row.get("entity_type", "")), allowed_entity_types)
    if entity_type is None:
        return []
    raw_description = str(row.get("description", "")).strip()
    is_ref = bool(row.get("is_ref", False))
    return [
        ExtractedEntity(
            canonical_name=name,
            surface_text=name,
            entity_type=entity_type,
            description_seed=normalize_description(
                entity_type=entity_type,
                canonical_name=name,
                raw_description=raw_description,
            ),
            evidence_text=evidence,
            owner_node_id=context.node_id,
            is_ref=is_ref,
        )
    ]


def normalize_relations(
    rows: list[Any],
    context: ExtractContext,
    local_entities: dict[str, ExtractedEntity],
) -> list[ExtractedRelation]:
    normalized: list[ExtractedRelation] = []
    seen: set[tuple[str, str, str, str]] = set()
    for row in rows:
        relation = normalize_relation_row(row, context)
        if relation is None:
            continue
        source_entity = local_entities.get(relation.source)
        target_entity = local_entities.get(relation.target)
        if source_entity is None or target_entity is None:
            continue
        if not is_allowed_relation_shape(
            relation.type,
            source_entity.entity_type,
            target_entity.entity_type,
        ):
            continue
        key = (relation.type, relation.source, relation.target, relation.evidence_text)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(relation)
    normalized = complete_relation_branches(
        relations=normalized,
        context=context,
        local_entities=local_entities,
    )
    return normalized


def normalize_relation_row(
    row: Any,
    context: ExtractContext,
) -> ExtractedRelation | None:
    if not isinstance(row, dict):
        return None
    relation_type = normalize_relation_type(str(row.get("type", "")))
    if relation_type not in ALLOWED_RELATION_TYPES:
        return None
    source = normalize_name(str(row.get("source", "")))
    target = normalize_name(str(row.get("target", "")))
    if not source or not target:
        return None
    evidence = normalize_name(str(row.get("evidence", "")))
    if not evidence or evidence not in context.node_text:
        return None
    return ExtractedRelation(
        type=relation_type,
        source=source,
        target=target,
        evidence_text=evidence,
        owner_node_id=context.node_id,
    )


def normalize_relation_type(raw_type: str) -> str:
    normalized = raw_type.strip().upper()
    normalized = re.sub(r"[^A-Z_]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def is_allowed_relation_shape(
    relation_type: str,
    source_entity_type: str,
    target_entity_type: str,
) -> bool:
    allowed_shapes = ALLOWED_RELATION_SHAPES.get(relation_type)
    if allowed_shapes is None:
        return False
    return (source_entity_type, target_entity_type) in allowed_shapes


def complete_relation_branches(
    *,
    relations: list[ExtractedRelation],
    context: ExtractContext,
    local_entities: dict[str, ExtractedEntity],
) -> list[ExtractedRelation]:
    completed = list(relations)
    seen = {
        (relation.type, relation.source, relation.target, relation.evidence_text)
        for relation in completed
    }
    actions = [entity for entity in local_entities.values() if entity.entity_type == "action"]
    if len(actions) != 1:
        return completed
    sole_action = actions[0]
    condition_targets = {
        relation.target
        for relation in completed
        if relation.type == "WITH_CONDITION" and relation.source == sole_action.canonical_name
    }
    penalty_targets = {
        relation.target
        for relation in completed
        if relation.type == "HAS_PENALTY" and relation.source == sole_action.canonical_name
    }
    condition_penalty_sources = {
        relation.source
        for relation in completed
        if relation.type == "HAS_PENALTY"
        and local_entities.get(relation.source, sole_action).entity_type == "condition"
    }
    for condition_name in sorted(condition_penalty_sources):
        if condition_name in condition_targets:
            continue
        condition_entity = local_entities.get(condition_name)
        if condition_entity is None:
            continue
        key = ("WITH_CONDITION", sole_action.canonical_name, condition_name, condition_entity.evidence_text)
        if key in seen or condition_entity.evidence_text not in context.node_text:
            continue
        completed.append(
            ExtractedRelation(
                type="WITH_CONDITION",
                source=sole_action.canonical_name,
                target=condition_name,
                evidence_text=condition_entity.evidence_text,
                owner_node_id=context.node_id,
            )
        )
        seen.add(key)
        condition_targets.add(condition_name)
    referenced_penalties = {
        relation.target
        for relation in completed
        if relation.type == "HAS_PENALTY"
    }
    for entity in local_entities.values():
        if entity.entity_type != "penalty":
            continue
        if entity.canonical_name in referenced_penalties or entity.canonical_name in penalty_targets:
            continue
        key = ("HAS_PENALTY", sole_action.canonical_name, entity.canonical_name, entity.evidence_text)
        if key in seen or entity.evidence_text not in context.node_text:
            continue
        completed.append(
            ExtractedRelation(
                type="HAS_PENALTY",
                source=sole_action.canonical_name,
                target=entity.canonical_name,
                evidence_text=entity.evidence_text,
                owner_node_id=context.node_id,
            )
        )
        seen.add(key)
        penalty_targets.add(entity.canonical_name)
    return completed


def materialize_entities(
    *,
    owner_node: NodeRecord,
    entity_rows: list[dict[str, Any]],
    reference_lookup: dict[str, Any],
    bundle: GraphBundle,
    existing_node_ids: set[str],
    existing_edge_ids: set[str],
) -> dict[str, NodeRecord]:
    local_entities: dict[str, NodeRecord] = {}
    for row in entity_rows:
        entity = ExtractedEntity(**row)
        entity_node = build_entity_node(owner_node, entity)
        existing_node = next((node for node in bundle.nodes if node.id == entity_node.id), None)
        if existing_node is None:
            bundle.nodes.append(entity_node)
            existing_node_ids.add(entity_node.id)
        else:
            entity_node = existing_node
        local_entities[entity.canonical_name] = entity_node
        has_entity_edge = build_has_entity_edge(owner_node.id, entity_node.id, entity.evidence_text)
        if has_entity_edge.id not in existing_edge_ids:
            bundle.edges.append(has_entity_edge)
            existing_edge_ids.add(has_entity_edge.id)
        if entity.is_ref:
            for edge in build_reference_edges_from_entity(entity, entity_node.id, reference_lookup):
                if edge.id in existing_edge_ids:
                    continue
                bundle.edges.append(edge)
                existing_edge_ids.add(edge.id)
    return local_entities


def build_entity_node(owner_node: NodeRecord, entity: ExtractedEntity) -> NodeRecord:
    digest = checksum_text(f"{owner_node.id}|{entity.canonical_name}|{entity.entity_type}")[:12]
    entity_id = f"entity:{owner_node.source_id}:{slugify(entity.entity_type)}:{digest}"
    metadata = {
        "entity_type": entity.entity_type,
        "owner_node_id": owner_node.id,
        "owner_level": owner_node.level,
        "is_ref": entity.is_ref,
    }
    return NodeRecord(
        id=entity_id,
        type="EntityNode",
        name=entity.canonical_name,
        level="entity",
        source_id=owner_node.source_id,
        description=entity.description_seed,
        metadata=metadata,
    )


def build_has_entity_edge(source_id: str, target_id: str, evidence_text: str) -> EdgeRecord:
    edge_id = f"edge:has_entity:{checksum_text(f'{source_id}|{target_id}')[:12]}"
    return EdgeRecord(
        id=edge_id,
        source=source_id,
        target=target_id,
        type="HAS_ENTITY",
        evidence=[{"source_node_id": source_id, "text": evidence_text}],
        metadata={},
    )


def materialize_relations(
    *,
    relation_rows: list[dict[str, Any]],
    local_entities: dict[str, NodeRecord],
    bundle: GraphBundle,
    existing_edge_ids: set[str],
) -> int:
    created = 0
    for row in relation_rows:
        relation = ExtractedRelation(**row)
        source_entity = local_entities.get(relation.source)
        target_entity = local_entities.get(relation.target)
        if source_entity is None or target_entity is None:
            continue
        edge = build_relation_edge(
            relation_type=relation.type,
            source_id=source_entity.id,
            target_id=target_entity.id,
            owner_node_id=relation.owner_node_id,
            evidence_text=relation.evidence_text,
        )
        if edge.id in existing_edge_ids:
            continue
        bundle.edges.append(edge)
        existing_edge_ids.add(edge.id)
        created += 1
    return created


def build_reference_edges_from_entity(
    entity: ExtractedEntity,
    source_id: str,
    reference_lookup: dict[str, Any],
) -> list[EdgeRecord]:
    target_ids = resolve_reference_targets(
        owner_node_id=entity.owner_node_id,
        evidence_text=entity.evidence_text,
        reference_lookup=reference_lookup,
    )
    edges: list[EdgeRecord] = []
    for target_id in target_ids:
        edge_id = f"edge:{slugify('REFERENCE_TO')}:{checksum_text(f'{source_id}|{target_id}|{entity.evidence_text}')[:12]}"
        edges.append(
            EdgeRecord(
                id=edge_id,
                source=source_id,
                target=target_id,
                type="REFERENCE_TO",
                evidence=[{"source_node_id": entity.owner_node_id, "text": entity.evidence_text}],
                metadata={},
            )
        )
    return edges


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
        metadata={},
    )


def _entity_priority(entity: ExtractedEntity) -> tuple[int, int, int]:
    return (
        1 if entity.entity_type == "penalty" else 0,
        len(entity.evidence_text),
        len(entity.description_seed),
    )
