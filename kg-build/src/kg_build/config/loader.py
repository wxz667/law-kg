from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..common import project_root


def resources_root() -> Path:
    return project_root() / "resources"


def load_resource_json(filename: str) -> dict[str, Any]:
    path = resources_root() / filename
    return json.loads(path.read_text(encoding="utf-8"))


def load_schema() -> dict[str, Any]:
    return load_resource_json("schema.json")


def load_regex_patterns() -> dict[str, Any]:
    return load_resource_json("regex_patterns.json")


def load_models() -> dict[str, Any]:
    return load_resource_json("models.json")


def load_thresholds() -> dict[str, Any]:
    return load_resource_json("thresholds.json")


def snapshot_config() -> dict[str, Any]:
    return {
        "schema": load_schema(),
        "regex_patterns": load_regex_patterns(),
        "models": load_models(),
        "thresholds": load_thresholds(),
    }
