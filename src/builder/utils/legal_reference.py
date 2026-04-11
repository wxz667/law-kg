from __future__ import annotations

import re

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
