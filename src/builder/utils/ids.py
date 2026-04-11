from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path


def project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def timestamp_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def slugify(value: str) -> str:
    compact = re.sub(r"\s+", "-", value.strip().lower())
    compact = re.sub(r"[^\w\u4e00-\u9fff-]", "", compact, flags=re.UNICODE)
    compact = re.sub(r"-{2,}", "-", compact)
    return compact.strip("-") or "artifact"


def checksum_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def checksum_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
