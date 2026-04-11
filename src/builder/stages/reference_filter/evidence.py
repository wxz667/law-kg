from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class AugmentedEvidenceText:
    text: str
    offset: int = 0


def build_augmented_evidence_text(
    sentence: str,
    source_node_id: str,
    node_index: dict[str, object],
    parent_by_child: dict[str, str],
) -> AugmentedEvidenceText:
    source_node = node_index.get(source_node_id)
    if source_node is None or getattr(source_node, "level", "") not in {"item", "sub_item"}:
        return AugmentedEvidenceText(text=sentence, offset=0)

    sentence_compact = re.sub(r"\s+", "", sentence)
    context_parts: list[str] = []
    seen: set[str] = set()
    current = source_node_id
    while current in parent_by_child:
        current = parent_by_child[current]
        current_node = node_index[current]
        if getattr(current_node, "level", "") not in {"paragraph", "article"}:
            continue
        text = str(getattr(current_node, "text", "") or "").strip()
        if not text:
            continue
        compact = re.sub(r"\s+", "", text)
        if not compact or compact == sentence_compact or compact in seen:
            continue
        seen.add(compact)
        context_parts.append(text)
        if getattr(current_node, "level", "") == "article" or len(context_parts) >= 2:
            break

    if not context_parts:
        return AugmentedEvidenceText(text=sentence, offset=0)
    joined = "".join(reversed(context_parts))
    if len(joined) > 220:
        joined = joined[:220].rstrip("，,；; ") + "..."
    compact_sentence = re.sub(r"\s+", "", sentence)
    compact_joined = re.sub(r"\s+", "", joined)
    if compact_sentence.startswith(compact_joined):
        return AugmentedEvidenceText(text=sentence, offset=0)
    return AugmentedEvidenceText(text=f"{joined}{sentence}", offset=len(joined))
