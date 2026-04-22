from __future__ import annotations

import json
import random
import re
from pathlib import Path
from typing import Any

from .io import write_jsonl


CN_DIGITS = {
    "零": 0,
    "〇": 0,
    "一": 1,
    "二": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "两": 2,
}

CN_UNITS = {
    "十": 10,
    "百": 100,
    "千": 1000,
    "万": 10000,
}

ARTICLE_REF_RE = re.compile(r"(?:第([一二三四五六七八九十百千万零两〇0-9]+)条|本法([一二三四五六七八九十百千万零两〇0-9]+)条)")
INTERPRETS_RE = re.compile(r"(?:本法所称[^。；\n]{0,80}?是指[^。；\n]{0,120}|所称[^。；\n]{0,80}?是指[^。；\n]{0,120})")
AMENDS_RE = re.compile(r"(?:修订|修正|修改)(?:本法|如下)？")
REPEALS_RE = re.compile(r"(予以废止|失效|废止本法)")
LAW_QUOTE_RE = re.compile(r"《([^》]{2,60})》")
ARTICLE_IN_LAW_WINDOW_RE = re.compile(r"第([一二三四五六七八九十百千万零两〇0-9]+)条")


def build_relation_testset(
    *,
    input_path: Path,
    output_path: Path,
    refers_to_count: int = 1500,
    interprets_count: int = 1500,
    amends_count: int = 200,
    repeals_count: int = 200,
    none_count: int = 600,
    seed: int = 7,
) -> dict[str, Any]:
    bundle_paths: list[Path] = []
    if input_path.is_dir():
        bundle_paths.extend(input_path.glob("*.bundle-*.json"))
    elif input_path.is_file():
        bundle_paths.append(input_path)

    all_refers: list[dict[str, Any]] = []
    all_interprets: list[dict[str, Any]] = []
    all_amends: list[dict[str, Any]] = []
    all_repeals: list[dict[str, Any]] = []
    all_cross_refers: list[dict[str, Any]] = []
    
    sources_by_doc: dict[str, list[dict[str, Any]]] = {}
    article_targets_by_doc: dict[str, list[dict[str, Any]]] = {}
    article_targets_by_doc_and_chapter: dict[tuple[str, str], list[dict[str, Any]]] = {}
    
    total_document_count = 0
    doc_name_to_id: dict[str, str] = {}
    global_node_index: dict[str, dict[str, Any]] = {}
    global_article_no_to_id_by_doc: dict[str, dict[int, str]] = {}
    global_sources: list[dict[str, Any]] = []

    for path in bundle_paths:
        bundle = _read_json(path)
        node_index, parent_map, article_no_to_id_by_doc = _build_indexes(bundle)
        sources = _iter_source_nodes(node_index)

        document_cache: dict[str, str] = {}

        def document_of(node_id: str) -> str:
            if node_id not in document_cache:
                document_cache[node_id] = _resolve_document_id(node_id, node_index, parent_map)
            return document_cache[node_id]

        def chapter_of(node_id: str) -> str:
            return _resolve_chapter_id(node_id, node_index, parent_map)

        for node in node_index.values():
            if node.get("type") == "DocumentNode":
                name = str(node.get("name", "") or "").strip()
                if name and name not in doc_name_to_id:
                    doc_name_to_id[name] = node["id"]
                global_node_index.setdefault(node["id"], node)
            if node.get("type") == "ProvisionNode" and node.get("level") == "article":
                global_node_index.setdefault(node["id"], node)

        for doc_id, no_map in article_no_to_id_by_doc.items():
            merged = global_article_no_to_id_by_doc.setdefault(doc_id, {})
            for no, nid in no_map.items():
                merged.setdefault(no, nid)

        for source in sources:
            doc_id = document_of(source["id"])
            if not doc_id:
                continue
            sources_by_doc.setdefault(doc_id, []).append(source)
            global_sources.append(
                {
                    "id": source["id"],
                    "name": source.get("name", ""),
                    "text": source.get("text", ""),
                    "document_node_id": doc_id,
                }
            )

        for node in node_index.values():
            if node.get("type") != "ProvisionNode" or node.get("level") != "article":
                continue
            if not (node.get("text") or "").strip():
                continue
            doc_id = document_of(node["id"])
            if not doc_id:
                continue
            article_targets_by_doc.setdefault(doc_id, []).append(node)
            chapter_id = chapter_of(node["id"])
            article_targets_by_doc_and_chapter.setdefault((doc_id, chapter_id), []).append(node)

        total_document_count += len({document_of(s["id"]) for s in sources if document_of(s["id"])})

        all_refers.extend(
            _build_refers_to_samples(
                sources=sources,
                node_index=node_index,
                article_no_to_id_by_doc=article_no_to_id_by_doc,
                document_of=document_of,
            )
        )
        all_interprets.extend(
            _build_interprets_samples(
                sources=sources,
                node_index=node_index,
                document_of=document_of,
            )
        )
        amends, repeals = _build_amends_repeals_samples(
            sources=sources,
            node_index=node_index,
            document_of=document_of,
        )
        all_amends.extend(amends)
        all_repeals.extend(repeals)

    all_cross_refers.extend(
        _build_cross_law_refers_to_samples(
            sources=global_sources,
            doc_name_to_id=doc_name_to_id,
            article_no_to_id_by_doc=global_article_no_to_id_by_doc,
            node_index=global_node_index,
        )
    )

    all_refers = _dedup(all_refers)
    all_interprets = _dedup(all_interprets)
    all_amends = _dedup(all_amends)
    all_repeals = _dedup(all_repeals)
    all_cross_refers = _dedup(all_cross_refers)

    rng = random.Random(seed)
    rng.shuffle(all_refers)
    rng.shuffle(all_interprets)
    rng.shuffle(all_amends)
    rng.shuffle(all_repeals)
    rng.shuffle(all_cross_refers)
    
    refers = all_refers[: max(int(refers_to_count), 0)]
    interprets = all_interprets[: max(int(interprets_count), 0)]
    amends = all_amends[: max(int(amends_count), 0)]
    repeals = all_repeals[: max(int(repeals_count), 0)]
    cross_refers = all_cross_refers[: max(int(refers_to_count), 0)]
    refers = _dedup(refers + cross_refers)

    positives = {(row["source_node_id"], row["target_node_id"]) for row in refers + interprets + amends + repeals}
    none_rows = _sample_global_none(
        rng=rng,
        sources_by_doc=sources_by_doc,
        article_targets_by_doc=article_targets_by_doc,
        article_targets_by_doc_and_chapter=article_targets_by_doc_and_chapter,
        positives=positives,
        none_count=max(int(none_count), 0),
    )

    rows = refers + interprets + amends + repeals + none_rows
    rng.shuffle(rows)
    write_jsonl(output_path, rows)

    meta = {
        "input_path": str(input_path.resolve()),
        "output": str(output_path.resolve()),
        "counts": {
            "REFERS_TO": sum(1 for r in rows if r["relation_type"] == "REFERS_TO"),
            "INTERPRETS": sum(1 for r in rows if r["relation_type"] == "INTERPRETS"),
            "AMENDS": sum(1 for r in rows if r["relation_type"] == "AMENDS"),
            "REPEALS": sum(1 for r in rows if r["relation_type"] == "REPEALS"),
            "NONE": sum(1 for r in rows if r["relation_type"] == "NONE"),
            "TOTAL": len(rows),
        },
        "document_count": total_document_count,
        "seed": seed,
    }
    output_path.with_suffix(".meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return meta


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _build_indexes(
    bundle: dict[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, str], dict[str, dict[int, str]]]:
    nodes = bundle.get("nodes", [])
    edges = bundle.get("edges", [])

    node_index: dict[str, dict[str, Any]] = {node["id"]: node for node in nodes}
    parent_map: dict[str, str] = {}
    for edge in edges:
        if edge.get("type") != "HAS_CHILD":
            continue
        parent_map[edge["target"]] = edge["source"]

    document_ids = [node["id"] for node in nodes if node.get("type") == "DocumentNode"]
    if not document_ids:
        raise ValueError("No DocumentNode found in bundle.")

    article_no_to_id_by_doc: dict[str, dict[int, str]] = {doc_id: {} for doc_id in document_ids}
    document_cache: dict[str, str] = {}

    def document_of(node_id: str) -> str:
        if node_id not in document_cache:
            document_cache[node_id] = _resolve_document_id(node_id, node_index, parent_map)
        return document_cache[node_id]

    for node in nodes:
        if node.get("type") != "ProvisionNode" or node.get("level") != "article":
            continue
        node_id = node.get("id", "")
        parts = node_id.split(":")
        if len(parts) < 3:
            continue
        article_key = parts[-1]
        base_key = article_key.split("-", 1)[0]
        if not base_key.isdigit():
            continue
        doc_id = document_of(node_id)
        if not doc_id:
            continue
        article_no_to_id_by_doc.setdefault(doc_id, {})[int(base_key)] = node_id

    return node_index, parent_map, article_no_to_id_by_doc


def _resolve_document_id(
    node_id: str,
    node_index: dict[str, dict[str, Any]],
    parent_map: dict[str, str],
) -> str:
    current = node_id
    visited: set[str] = set()
    while True:
        node = node_index.get(current)
        if node and node.get("type") == "DocumentNode":
            return current
        if current not in parent_map:
            return ""
        if current in visited:
            return ""
        visited.add(current)
        current = parent_map[current]


def _resolve_chapter_id(
    node_id: str,
    node_index: dict[str, dict[str, Any]],
    parent_map: dict[str, str],
) -> str:
    current = node_id
    visited: set[str] = set()
    while current in parent_map and current not in visited:
        visited.add(current)
        current = parent_map[current]
        node = node_index.get(current)
        if node and node.get("level") == "chapter":
            return current
    return ""


def _iter_source_nodes(node_index: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    for node in node_index.values():
        if node.get("type") != "ProvisionNode":
            continue
        text = (node.get("text") or "").strip()
        if not text:
            continue
        sources.append(node)
    return sources


def _build_refers_to_samples(
    *,
    sources: list[dict[str, Any]],
    node_index: dict[str, dict[str, Any]],
    article_no_to_id_by_doc: dict[str, dict[int, str]],
    document_of: Any,
) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for source in sources:
        source_text = (source.get("text") or "").strip()
        if not source_text:
            continue
        hits = ARTICLE_REF_RE.findall(source_text)
        if not hits:
            continue
            
        doc_id = document_of(source["id"])
        if not doc_id:
            continue
        article_no_to_id = article_no_to_id_by_doc.get(doc_id, {})
        if not article_no_to_id:
            continue
            
        for match_groups in hits:
            cn_no = match_groups[0] if match_groups[0] else match_groups[1]
            if not cn_no:
                continue
            try:
                no = _chinese_number_to_int(cn_no)
            except Exception:
                continue
            target_id = article_no_to_id.get(no)
            if not target_id:
                continue
            target = node_index.get(target_id, {})
            target_text = (target.get("text") or "").strip()
            evidence = f"第{cn_no}条" if match_groups[0] else f"本法{cn_no}条"
            samples.append(
                {
                    "sample_id": f"pair:{source['id']}->{target_id}:{len(samples):06d}",
                    "source_node_id": source["id"],
                    "target_node_id": target_id,
                    "relation_type": "REFERS_TO",
                    "source_text": source_text,
                    "target_text": target_text,
                    "evidence": evidence,
                    "metadata": {
                        "document_node_id": doc_id,
                        "source_name": source.get("name", ""),
                        "target_name": target.get("name", ""),
                    },
                }
            )
    return samples


def _normalize_law_title(title: str) -> str:
    t = (title or "").strip()
    if not t:
        return ""
    for sep in ("（", "(", "【", "["):
        if sep in t:
            t = t.split(sep, 1)[0].strip()
    return t


def _build_cross_law_refers_to_samples(
    *,
    sources: list[dict[str, Any]],
    doc_name_to_id: dict[str, str],
    article_no_to_id_by_doc: dict[str, dict[int, str]],
    node_index: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for source in sources:
        source_text = str(source.get("text", "") or "").strip()
        if not source_text:
            continue
        source_doc_id = str(source.get("document_node_id", "") or "").strip()
        if not source_doc_id:
            continue

        for m in LAW_QUOTE_RE.finditer(source_text):
            raw_title = m.group(1)
            law_title = _normalize_law_title(raw_title)
            if not law_title:
                continue
            target_doc_id = doc_name_to_id.get(law_title)
            if not target_doc_id:
                continue

            window = source_text[m.end() : m.end() + 80]
            for am in ARTICLE_IN_LAW_WINDOW_RE.finditer(window):
                cn_no = am.group(1)
                if not cn_no:
                    continue
                try:
                    no = _chinese_number_to_int(cn_no)
                except Exception:
                    continue
                target_article_id = article_no_to_id_by_doc.get(target_doc_id, {}).get(no)
                if not target_article_id:
                    continue
                if target_article_id == source.get("id"):
                    continue

                target = node_index.get(target_article_id, {})
                target_text = (target.get("text") or "").strip()
                evidence = f"《{law_title}》第{cn_no}条"
                samples.append(
                    {
                        "sample_id": f"pair:{source['id']}->{target_article_id}:cross:{len(samples):06d}",
                        "source_node_id": source["id"],
                        "target_node_id": target_article_id,
                        "relation_type": "REFERS_TO",
                        "source_text": source_text,
                        "target_text": target_text,
                        "evidence": evidence,
                        "metadata": {
                            "document_node_id": source_doc_id,
                            "target_document_node_id": target_doc_id,
                            "cross_law": True,
                            "source_name": source.get("name", ""),
                            "target_name": target.get("name", ""),
                            "target_law_title": law_title,
                        },
                    }
                )
    return samples


def _build_interprets_samples(
    *,
    sources: list[dict[str, Any]],
    node_index: dict[str, dict[str, Any]],
    document_of: Any,
) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for source in sources:
        source_text = (source.get("text") or "").strip()
        if not source_text:
            continue
        
        matches = INTERPRETS_RE.finditer(source_text)
        doc_id = document_of(source["id"])
        if not doc_id:
            continue
        document = node_index.get(doc_id, {})
        document_text = (document.get("text") or "").strip()
        
        for match in matches:
            evidence = match.group(0)
            samples.append(
                {
                    "sample_id": f"pair:{source['id']}->{doc_id}:{len(samples):06d}",
                    "source_node_id": source["id"],
                    "target_node_id": doc_id,
                    "relation_type": "INTERPRETS",
                    "source_text": source_text,
                    "target_text": document_text,
                    "evidence": evidence,
                    "metadata": {
                        "document_node_id": doc_id,
                        "source_name": source.get("name", ""),
                        "target_name": document.get("name", ""),
                    },
                }
            )
    return samples


def _build_amends_repeals_samples(
    *,
    sources: list[dict[str, Any]],
    node_index: dict[str, dict[str, Any]],
    document_of: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    amends: list[dict[str, Any]] = []
    repeals: list[dict[str, Any]] = []
    for source in sources:
        source_text = (source.get("text") or "").strip()
        if not source_text:
            continue
        doc_id = document_of(source["id"])
        if not doc_id:
            continue
        document = node_index.get(doc_id, {})
        document_text = (document.get("text") or "").strip()

        amends_match = AMENDS_RE.search(source_text)
        if amends_match:
            amends.append(
                {
                    "sample_id": f"pair:{source['id']}->{doc_id}:amends:{len(amends):06d}",
                    "source_node_id": source["id"],
                    "target_node_id": doc_id,
                    "relation_type": "AMENDS",
                    "source_text": source_text,
                    "target_text": document_text,
                    "evidence": amends_match.group(0),
                    "metadata": {
                        "document_node_id": doc_id,
                        "source_name": source.get("name", ""),
                        "target_name": document.get("name", ""),
                    },
                }
            )
            continue
            
        repeals_match = REPEALS_RE.search(source_text)
        if repeals_match:
            repeals.append(
                {
                    "sample_id": f"pair:{source['id']}->{doc_id}:repeals:{len(repeals):06d}",
                    "source_node_id": source["id"],
                    "target_node_id": doc_id,
                    "relation_type": "REPEALS",
                    "source_text": source_text,
                    "target_text": document_text,
                    "evidence": repeals_match.group(0),
                    "metadata": {
                        "document_node_id": doc_id,
                        "source_name": source.get("name", ""),
                        "target_name": document.get("name", ""),
                    },
                }
            )
    return amends, repeals


def _dedup(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        key = (row["source_node_id"], row["target_node_id"], row["relation_type"])
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _sample_global_none(
    *,
    rng: random.Random,
    sources_by_doc: dict[str, list[dict[str, Any]]],
    article_targets_by_doc: dict[str, list[dict[str, Any]]],
    article_targets_by_doc_and_chapter: dict[tuple[str, str], list[dict[str, Any]]],
    positives: set[tuple[str, str]],
    none_count: int,
) -> list[dict[str, Any]]:
    none_rows: list[dict[str, Any]] = []
    
    all_sources: list[tuple[str, dict[str, Any]]] = []
    for doc_id, sources in sources_by_doc.items():
        if not article_targets_by_doc.get(doc_id):
            continue
        for source in sources:
            all_sources.append((doc_id, source))
            
    if not all_sources:
        return []

    tries = 0
    cap = max(none_count * 50, 2000)
    while len(none_rows) < none_count and tries < cap:
        tries += 1
        doc_id, source = rng.choice(all_sources)
        source_id = source["id"]
        source_text = (source.get("text") or "").strip()
        if not source_text:
            continue

        doc_targets = article_targets_by_doc.get(doc_id, [])
        if not doc_targets:
            continue
        
        target = rng.choice(doc_targets)
        target_id = target["id"]
        
        if target_id == source_id:
            continue
        if (source_id, target_id) in positives:
            continue

        none_rows.append(
            {
                "sample_id": f"pair:{source_id}->{target_id}:none:{len(none_rows):06d}",
                "source_node_id": source_id,
                "target_node_id": target_id,
                "relation_type": "NONE",
                "source_text": source_text,
                "target_text": (target.get("text") or "").strip(),
                "evidence": "",
                "metadata": {
                    "document_node_id": doc_id,
                    "source_name": source.get("name", ""),
                    "target_name": target.get("name", ""),
                    "hard_negative": False,
                },
            }
        )
    return none_rows


def _chinese_number_to_int(text: str) -> int:
    normalized = text.strip()
    if not normalized:
        raise ValueError("Chinese numeral text cannot be empty")
    if normalized.isdigit():
        return int(normalized)

    total = 0
    section = 0
    number = 0
    for char in normalized:
        if char in CN_DIGITS:
            number = CN_DIGITS[char]
            continue
        if char in CN_UNITS:
            unit = CN_UNITS[char]
            if unit == 10000:
                section = (section + (number or 0)) * unit
                total += section
                section = 0
                number = 0
            else:
                section += (number or 1) * unit
                number = 0
            continue
        raise ValueError(f"Unsupported Chinese numeral character: {char}")
    return total + section + number

