from __future__ import annotations

from typing import Any


def fuse_with_rrf(
    ranked_lists: list[list[dict[str, Any]]],
    id_field: str = "provision_id",
    k: int = 20,
    weights: list[float] | None = None,
) -> list[dict[str, Any]]:
    if not ranked_lists:
        return []

    w = weights or [1.0] * len(ranked_lists)
    if len(w) != len(ranked_lists):
        w = [1.0] * len(ranked_lists)

    agg: dict[str, dict[str, Any]] = {}
    for li, items in enumerate(ranked_lists):
        weight = float(w[li])
        for rank, it in enumerate(items, start=1):
            pid = str(it.get(id_field) or it.get("id") or "").strip()
            if not pid:
                continue
            base = agg.get(pid)
            if base is None:
                base = dict(it)
                base["sources"] = []
                base["rrf_score"] = 0.0
                agg[pid] = base
            src = str(it.get("source") or "")
            if src and src not in base["sources"]:
                base["sources"].append(src)
            base["rrf_score"] = float(base.get("rrf_score") or 0.0) + weight * (1.0 / (float(k) + float(rank)))

    return sorted(agg.values(), key=lambda x: float(x.get("rrf_score") or 0.0), reverse=True)


def apply_cited_law_penalty(
    items: list[dict[str, Any]],
    cited_laws: list[str],
    law_name_field: str = "full_name",
    penalty_factor: float = 0.7,
) -> list[dict[str, Any]]:
    if not items or not cited_laws:
        return items
    out: list[dict[str, Any]] = []
    for it in items:
        name = str(it.get(law_name_field) or it.get("name") or "")
        hit = any(law and law in name for law in cited_laws)
        copied = dict(it)
        if hit:
            copied["rrf_score"] = float(copied.get("rrf_score") or copied.get("score") or 0.0) * float(penalty_factor)
        out.append(copied)
    return sorted(out, key=lambda x: float(x.get("rrf_score") or x.get("score") or 0.0), reverse=True)

