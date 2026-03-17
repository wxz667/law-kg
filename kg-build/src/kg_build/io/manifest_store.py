from __future__ import annotations

from pathlib import Path

from ..contracts import BuildManifest
from .json_store import write_json


def save_manifest(path: Path, manifest: BuildManifest) -> None:
    write_json(path, manifest.to_dict())
