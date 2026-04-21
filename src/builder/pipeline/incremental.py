from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from dataclasses import replace
from typing import Literal

from ..contracts import (
    StageStateManifest,
    SubstageStateManifest,
)
from ..io import read_stage_manifest, write_stage_manifest


ReuseKind = Literal["full", "partial", "none"]


@dataclass(frozen=True)
class ReuseDecision:
    kind: ReuseKind
    reusable_unit_ids: tuple[str, ...] = ()
    upstream_signature: str = ""
    reason: str = ""

    @property
    def is_full(self) -> bool:
        return self.kind == "full"

    @property
    def is_partial(self) -> bool:
        return self.kind == "partial"

    @property
    def is_none(self) -> bool:
        return self.kind == "none"


IGNORED_SIGNATURE_FIELDS = {"updated_at", "inputs", "artifacts"}


def _normalize_unit_ids(unit_ids: list[str] | tuple[str, ...] | set[str]) -> list[str]:
    return sorted(dict.fromkeys(str(value).strip() for value in unit_ids if str(value).strip()))


def _stable_manifest_payload(manifest: StageStateManifest | SubstageStateManifest) -> dict[str, object]:
    payload: dict[str, object] = {
        "unit": manifest.unit,
        "stats": _stable_json_value(manifest.stats),
        "metadata": _stable_metadata(manifest.metadata),
        "processed_units": _normalize_unit_ids(tuple(manifest.processed_units)),
    }
    if isinstance(manifest, StageStateManifest):
        payload["stage"] = manifest.stage
    if manifest.substages:
        payload["substages"] = {
            name: _stable_manifest_payload(state)
            for name, state in sorted(manifest.substages.items())
        }
    return payload


def _stable_metadata(metadata: dict[str, object]) -> dict[str, object]:
    return {
        str(key): _stable_json_value(value)
        for key, value in sorted(metadata.items())
        if str(key) not in IGNORED_SIGNATURE_FIELDS
    }


def _stable_json_value(value):
    if isinstance(value, dict):
        return {
            str(key): _stable_json_value(item)
            for key, item in sorted(value.items())
            if str(key) not in IGNORED_SIGNATURE_FIELDS
        }
    if isinstance(value, (list, tuple, set)):
        return [_stable_json_value(item) for item in value]
    return value


def stable_payload_signature(payload: object) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_upstream_signature(manifest: StageStateManifest) -> str:
    return stable_payload_signature(_stable_manifest_payload(manifest))


def build_upstream_signature_for_stage(layout, stage_name: str) -> str:
    manifest_path = layout.stage_manifest_path(stage_name)
    if not manifest_path.exists():
        return ""
    return build_upstream_signature(read_stage_manifest(manifest_path))


def get_reusable_units(
    layout,
    stage_name: str,
    unit_ids: list[str],
    *,
    substage_name: str | None = None,
    force_rebuild: bool = False,
) -> list[str]:
    if force_rebuild:
        return []
    normalized_units = _normalize_unit_ids(tuple(unit_ids))
    if not normalized_units:
        return []
    manifest_path = layout.stage_manifest_path(stage_name)
    if not manifest_path.exists():
        return []
    manifest = read_stage_manifest(manifest_path)
    state: StageStateManifest | SubstageStateManifest | None = manifest
    if substage_name is not None:
        state = manifest.substages.get(substage_name)
    if state is None:
        return []
    processed = set(_normalize_unit_ids(tuple(state.processed_units)))
    return [unit_id for unit_id in normalized_units if unit_id in processed]


def get_stage_reuse_decision(
    layout,
    stage_name: str,
    unit_ids: list[str],
    *,
    substage_name: str | None = None,
    force_rebuild: bool = False,
    handler=None,
) -> ReuseDecision:
    normalized_units = _normalize_unit_ids(tuple(unit_ids))
    if handler is not None and hasattr(handler, "get_stage_reuse_decision"):
        return handler.get_stage_reuse_decision(
            layout,
            stage_name,
            normalized_units,
            substage_name=substage_name,
            force_rebuild=force_rebuild,
        )
    if handler is not None and hasattr(handler, "get_reusable_units"):
        reusable = tuple(
            handler.get_reusable_units(
                layout,
                stage_name,
                normalized_units,
                substage_name=substage_name,
                force_rebuild=force_rebuild,
            )
        )
        if force_rebuild or not reusable:
            return ReuseDecision(kind="none", reusable_unit_ids=reusable, reason="force_rebuild" if force_rebuild else "no_reusable_units")
        if len(reusable) == len(normalized_units):
            return ReuseDecision(kind="full", reusable_unit_ids=reusable, reason="all_units_reusable")
        return ReuseDecision(kind="partial", reusable_unit_ids=reusable, reason="some_units_reusable")
    reusable = tuple(get_reusable_units(
        layout,
        stage_name,
        normalized_units,
        substage_name=substage_name,
        force_rebuild=force_rebuild,
    ))
    if force_rebuild or not reusable:
        return ReuseDecision(kind="none", reusable_unit_ids=reusable, reason="force_rebuild" if force_rebuild else "no_reusable_units")
    if len(reusable) == len(normalized_units):
        return ReuseDecision(kind="full", reusable_unit_ids=reusable, reason="all_units_reusable")
    return ReuseDecision(kind="partial", reusable_unit_ids=reusable, reason="some_units_reusable")


def get_infer_reuse_decision(layout, *, force_rebuild: bool = False) -> ReuseDecision:
    if force_rebuild:
        return ReuseDecision(kind="none", reason="force_rebuild")
    infer_manifest_path = layout.stage_manifest_path("infer")
    align_manifest_path = layout.stage_manifest_path("align")
    if not infer_manifest_path.exists():
        return ReuseDecision(kind="none", reason="missing_manifest")
    infer_manifest = read_stage_manifest(infer_manifest_path)
    stored_signature = str(infer_manifest.metadata.get("upstream_signature", "") or "")
    if not align_manifest_path.exists():
        if not stored_signature and _infer_manifest_has_complete_judgments(infer_manifest):
            return ReuseDecision(kind="full", reason="legacy_manifest_without_upstream_manifest")
        return ReuseDecision(kind="none", reason="missing_upstream_manifest")
    align_manifest = read_stage_manifest(align_manifest_path)
    upstream_signature = build_upstream_signature(align_manifest)
    if stored_signature == upstream_signature:
        return ReuseDecision(kind="full", upstream_signature=upstream_signature, reason="upstream_unchanged")
    if stored_signature and align_manifest.stage == "align" and align_manifest.unit == "concept":
        legacy_align_manifest = replace(align_manifest, unit="node")
        if stored_signature == build_upstream_signature(legacy_align_manifest):
            return ReuseDecision(kind="full", upstream_signature=upstream_signature, reason="align_unit_metadata_migrated")
    if not stored_signature:
        return ReuseDecision(kind="full", upstream_signature=upstream_signature, reason="legacy_manifest_without_signature")
    return ReuseDecision(kind="none", upstream_signature=upstream_signature, reason="upstream_changed")


def _infer_manifest_has_complete_judgments(manifest: StageStateManifest) -> bool:
    if not manifest.substages:
        return False
    for pass_state in manifest.substages.values():
        recall_state = pass_state.substages.get("recall")
        judge_state = pass_state.substages.get("judge")
        recalled_pairs = int((recall_state.stats if recall_state is not None else pass_state.stats).get("pair_count", 0) or 0)
        judged_pairs = int((judge_state.stats if judge_state is not None else pass_state.stats).get("judgment_count", 0) or 0)
        if judged_pairs < recalled_pairs:
            return False
    return True


def merge_stage_manifest(layout, manifest: StageStateManifest) -> StageStateManifest:
    write_stage_manifest(layout.stage_manifest_path(manifest.stage), manifest)
    return manifest


def write_stage_artifacts(*args, **kwargs):
    writer = kwargs.pop("writer", None)
    if writer is None:
        raise ValueError("write_stage_artifacts requires a writer callable.")
    return writer(*args, **kwargs)


def merge_stage_artifacts(*args, **kwargs):
    merger = kwargs.pop("merger", None)
    if merger is None:
        raise ValueError("merge_stage_artifacts requires a merger callable.")
    return merger(*args, **kwargs)
