from __future__ import annotations

import json
from pathlib import Path


def build_dataset(source_path: Path, output_path: Path, limit: int = 5000) -> dict[str, int]:
    rows = json.loads(source_path.read_text(encoding="utf-8"))
    output_rows = rows[:limit]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"samples": len(output_rows)}
