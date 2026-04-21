from __future__ import annotations

import re
from dataclasses import dataclass

from ..contracts import GraphBundle
from .locator import node_sort_key

ALIAS_PATTERNS = (
    re.compile(r"《(?P<title>[^》]+)》\s*（以下简称(?P<alias>[^）]{1,20})）"),
    re.compile(r"《(?P<title>[^》]+)》\s*\((?:以下简称)(?P<alias>[^)]{1,20})\)"),
    re.compile(r"《(?P<title>[^》]+)》\s*（简称(?P<alias>[^）]{1,20})）"),
    re.compile(r"《(?P<title>[^》]+)》\s*\((?:简称)(?P<alias>[^)]{1,20})\)"),
    re.compile(r"《(?P<title>[^》]+)》[^。；;\n]{0,40}?（以下简称(?P<alias>[^）]{1,20})）"),
    re.compile(r"《(?P<title>[^》]+)》[^。；;\n]{0,40}?\((?:以下简称)(?P<alias>[^)]{1,20})\)"),
    re.compile(r"《(?P<title>[^》]+)》[^。；;\n]{0,40}?（简称(?P<alias>[^）]{1,20})）"),
    re.compile(r"《(?P<title>[^》]+)》[^。；;\n]{0,40}?\((?:简称)(?P<alias>[^)]{1,20})\)"),
)
SPECIAL_DECISION_TITLE_PATTERNS = (
    re.compile(r"关于(?:废止|修改).+的决定"),
    re.compile(r"^废止.+的决定$"),
)
AMENDMENT_TITLE_PATTERNS = (
    re.compile(r"修正案"),
    re.compile(r"修订草案"),
)
LEGISLATIVE_INTERPRETATION_TITLE_RE = re.compile(
    r"全国人民代表大会常务委员会关于《[^》]+》第[一二三四五六七八九十百千万零两〇0-9]+条(?:之[一二三四五六七八九十百千万零两〇0-9]+)?(?:第[一二三四五六七八九十百千万零两〇0-9]+款)?(?:第(?:[（(][一二三四五六七八九十百千万零两〇0-9]+[）)]|[一二三四五六七八九十百千万零两〇0-9]+)项)?的解释"
)
REFERENCE_CATEGORY_MAP = {
    "宪法": "constitution",
    "法律": "law",
    "司法解释": "interpretation",
    "行政法规": "regulation",
    "地方法规": "regulation",
    "监察法规": "regulation",
    "constitution": "constitution",
    "law": "law",
    "interpretation": "interpretation",
    "regulation": "regulation",
}
CATEGORY_RANK = {
    "constitution": 0,
    "law": 1,
    "regulation": 2,
    "interpretation": 3,
}
TITLE_PREFIX_PATTERNS = (
    "中华人民共和国",
    "最高人民法院关于",
    "最高人民检察院关于",
    "国务院关于",
    "全国人民代表大会常务委员会关于",
    "全国人民代表大会关于",
)


def document_title(document_node: object) -> str:
    title = str(getattr(document_node, "name", "") or "").strip()
    return title if title.startswith("《") else f"《{title}》"


def normalize_reference_category(document_node: object | None) -> str:
    if document_node is None:
        raise ValueError("Missing document node for reference category normalization.")
    raw_category = str(getattr(document_node, "category", "") or "").strip()
    normalized = REFERENCE_CATEGORY_MAP.get(raw_category)
    if not normalized:
        raise ValueError(f"Unsupported reference document category: {raw_category or '<empty>'}")
    return normalized


def candidate_source_prefix(document_node: object | None) -> str:
    return normalize_reference_category(document_node)


def candidate_source_category(candidate_or_id: object) -> str:
    candidate_id = getattr(candidate_or_id, "id", candidate_or_id)
    prefix = str(candidate_id or "").split(":", 1)[0].strip()
    if prefix == "judicial":
        prefix = "interpretation"
    if prefix == "local":
        prefix = "regulation"
    if prefix not in CATEGORY_RANK:
        raise ValueError(f"Unsupported candidate source category prefix: {prefix or '<empty>'}")
    return prefix


def is_judicial_interpretation_document(document_node: object | None) -> bool:
    if document_node is None:
        return False
    category = str(getattr(document_node, "category", "") or "")
    issuer = str(getattr(document_node, "issuer", "") or "")
    name = str(getattr(document_node, "name", "") or "")
    document_kind = " ".join(str(value) for value in (category, issuer))
    return (
        "司法解释" in document_kind
        or "最高人民法院" in issuer
        or "最高人民检察院" in issuer
        or any(marker in name for marker in ("解释", "批复", "答复"))
    )


def is_legislative_interpretation_document(document_node: object | None) -> bool:
    if document_node is None:
        return False
    if is_excluded_reference_document(document_node):
        return False
    if normalize_reference_category(document_node) != "law":
        return False
    title = str(getattr(document_node, "name", "") or "").strip()
    return LEGISLATIVE_INTERPRETATION_TITLE_RE.search(title) is not None


def should_scan_title_candidates(document_node: object | None) -> bool:
    return is_judicial_interpretation_document(document_node) or is_legislative_interpretation_document(document_node)


def should_use_title_candidates_exclusively(document_node: object | None) -> bool:
    return should_scan_title_candidates(document_node)


def is_special_decision_document(document_node: object | None) -> bool:
    if document_node is None:
        return False
    status = str(getattr(document_node, "status", "") or "").strip()
    title = str(getattr(document_node, "name", "") or "").strip()
    if status in {"已废止", "已修改"}:
        return True
    return any(pattern.search(title) for pattern in SPECIAL_DECISION_TITLE_PATTERNS)


def is_excluded_reference_document(document_node: object | None) -> bool:
    if document_node is None:
        return False
    title = str(getattr(document_node, "name", "") or "").strip()
    category = str(getattr(document_node, "category", "") or "").strip()
    if is_special_decision_document(document_node):
        return True
    if any(pattern.search(title) for pattern in AMENDMENT_TITLE_PATTERNS):
        return True
    return "修正案" in category


def extract_document_aliases(text: str) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for pattern in ALIAS_PATTERNS:
        for match in pattern.finditer(text):
            alias = match.group("alias").strip("“”\"'《》()（） ")
            title = f"《{match.group('title').strip()}》"
            if alias:
                aliases[alias] = title
    return aliases


def title_variants(document_node: object) -> set[str]:
    full = document_title(document_node)
    bare = normalize_title_variant(full.strip("《》").strip())
    if not bare:
        return set()

    variants = {bare}
    working_set = {bare}
    for variant in list(working_set):
        versionless = strip_version_suffix(variant)
        if versionless:
            variants.add(versionless)
            working_set.add(versionless)

    prefix_variants: set[str] = set()
    for variant in list(variants):
        prefix_variants.update(strip_title_prefixes(variant))
    variants.update(prefix_variants)

    normalized = {normalize_title_variant(variant) for variant in variants if normalize_title_variant(variant)}
    return {variant for variant in normalized if len(variant) >= 2}


def normalize_title_variant(text: str) -> str:
    return re.sub(r"\s+", "", str(text or "")).strip("《》“”\"' ")


def strip_version_suffix(title: str) -> str:
    stripped = re.sub(r"[（(](?:\d{4}年)?(?:修正文本|修正版|修订版|修正版文本|草案|征求意见稿|试行|暂行|修正案).*?[）)]$", "", title)
    stripped = re.sub(r"[（(]\d{4}年[）)]$", "", stripped)
    return stripped.strip()


def strip_title_prefixes(title: str) -> set[str]:
    variants: set[str] = set()
    for prefix in TITLE_PREFIX_PATTERNS:
        if title.startswith(prefix) and len(title) > len(prefix):
            variants.add(title.removeprefix(prefix).strip())
    if title.endswith("法典") and len(title) > 2:
        variants.add(title)
    return {normalize_title_variant(variant) for variant in variants if normalize_title_variant(variant)}


def build_alias_groups(alias_map: dict[str, str]) -> dict[str, list[tuple[str, str]]]:
    groups: dict[str, list[tuple[str, str]]] = {}
    for alias, full_title in alias_map.items():
        if not alias:
            continue
        groups.setdefault(alias[0], []).append((alias, full_title))
    for key, items in groups.items():
        groups[key] = sorted(items, key=lambda item: len(item[0]), reverse=True)
    return groups


@dataclass(frozen=True)
class ReferenceGraphContext:
    node_index: dict[str, object]
    parent_by_child: dict[str, str]
    owner_document_by_node: dict[str, str]
    document_nodes: dict[str, object]
    provision_index: dict[str, dict[tuple[str, str, str, str], str]]
    title_to_document_ids: dict[str, list[str]]
    children_by_parent_level: dict[tuple[str, str], list[str]]
    merged_document_aliases: dict[str, dict[str, str]]
    document_alias_groups: dict[str, dict[str, list[tuple[str, str]]]]
    global_document_alias_groups: dict[str, list[tuple[str, str]]]


def previous_sibling(node_id: str, parent_id: str, node_index: dict[str, object], parent_by_child: dict[str, str]) -> str:
    if not parent_id:
        return ""
    siblings = [
        node_index[child_id]
        for child_id, owner_id in parent_by_child.items()
        if owner_id == parent_id and node_index[child_id].level == node_index[node_id].level
    ]
    ordered = sorted(siblings, key=node_sort_key)
    previous = ""
    for sibling in ordered:
        if sibling.id == node_id:
            return previous
        previous = sibling.id
    return ""


def ancestor_at_level(node_id: str, level: str, node_index: dict[str, object], parent_by_child: dict[str, str]) -> str:
    current = node_id
    while current in parent_by_child:
        current = parent_by_child[current]
        if node_index[current].level == level:
            return current
    return ""


def owner_document_id(node_id: str, node_index: dict[str, object], parent_by_child: dict[str, str]) -> str:
    current = node_id
    if node_index[current].level == "document":
        return current
    while current in parent_by_child:
        current = parent_by_child[current]
        if node_index[current].level == "document":
            return current
    return ""


def tail_label(text: str, suffix: str) -> str:
    matches = re.findall(rf"第(?:[（(])?[一二三四五六七八九十百千万零两〇0-9]+(?:[）)])?{suffix}", text)
    return matches[-1] if matches else ""


def node_label_path(node_id: str, node_index: dict[str, object], parent_by_child: dict[str, str]) -> tuple[str, str, str, str]:
    node = node_index[node_id]
    article_label = ""
    paragraph_label = ""
    item_label = ""
    sub_item_label = ""

    current = node_id
    while True:
        current_node = node_index[current]
        if current_node.level == "article":
            article_label = current_node.name
        elif current_node.level == "paragraph":
            paragraph_label = tail_label(current_node.name, "款")
        elif current_node.level == "item":
            item_label = tail_label(current_node.name, "项")
        elif current_node.level == "sub_item":
            sub_item_label = tail_label(current_node.name, "目")
        if current not in parent_by_child:
            break
        current = parent_by_child[current]
    return article_label, paragraph_label, item_label, sub_item_label


def build_reference_graph_context(graph_bundle: GraphBundle) -> ReferenceGraphContext:
    node_index = {node.id: node for node in graph_bundle.nodes}
    parent_by_child = {
        edge.target: edge.source
        for edge in graph_bundle.edges
        if edge.type == "CONTAINS"
    }
    owner_document_by_node = {
        node.id: owner_document_id(node.id, node_index, parent_by_child)
        for node in graph_bundle.nodes
    }
    document_nodes = {
        node.id: node
        for node in graph_bundle.nodes
        if node.level == "document"
    }
    provision_index: dict[str, dict[tuple[str, str, str, str], str]] = {}
    children_by_parent_level: dict[tuple[str, str], list[str]] = {}
    title_to_document_ids: dict[str, list[str]] = {}
    document_aliases: dict[str, dict[str, str]] = {}
    merged_document_aliases: dict[str, dict[str, str]] = {}
    global_document_aliases: dict[str, str] = {}

    for document_id, document_node in document_nodes.items():
        title_to_document_ids.setdefault(document_title(document_node), []).append(document_id)
        document_aliases[document_id] = {}
        for variant in sorted(title_variants(document_node), key=len, reverse=True):
            document_aliases[document_id][variant] = document_title(document_node)
            global_document_aliases.setdefault(variant, document_title(document_node))

    document_texts: dict[str, list[str]] = {document_id: [] for document_id in document_nodes}
    for child_id, parent_id in parent_by_child.items():
        level = node_index[child_id].level
        children_by_parent_level.setdefault((parent_id, level), []).append(child_id)
    for node in graph_bundle.nodes:
        document_id = owner_document_by_node.get(node.id, "")
        if not document_id:
            continue
        if node.text:
            document_texts.setdefault(document_id, []).append(node.text)
        if node.level in {"article", "paragraph", "item", "sub_item"}:
            provision_index.setdefault(document_id, {})[node_label_path(node.id, node_index, parent_by_child)] = node.id
    for key, child_ids in children_by_parent_level.items():
        children_by_parent_level[key] = sorted(
            child_ids,
            key=lambda child_id: node_sort_key(node_index[child_id]),
        )
    for document_id, chunks in document_texts.items():
        document_aliases[document_id].update(extract_document_aliases("\n".join(chunks)))
    for document_id, alias_map in document_aliases.items():
        merged_document_aliases[document_id] = {**global_document_aliases, **alias_map}
    document_alias_groups = {
        document_id: build_alias_groups(alias_map)
        for document_id, alias_map in document_aliases.items()
    }
    return ReferenceGraphContext(
        node_index=node_index,
        parent_by_child=parent_by_child,
        owner_document_by_node=owner_document_by_node,
        document_nodes=document_nodes,
        provision_index=provision_index,
        title_to_document_ids=title_to_document_ids,
        children_by_parent_level=children_by_parent_level,
        merged_document_aliases=merged_document_aliases,
        document_alias_groups=document_alias_groups,
        global_document_alias_groups=build_alias_groups(global_document_aliases),
    )
