from __future__ import annotations

import json
from pathlib import Path


def train(dataset_path: Path, output_dir: Path) -> Path:
    rows = json.loads(dataset_path.read_text(encoding="utf-8"))
    output_dir.mkdir(parents=True, exist_ok=True)
    model_path = output_dir / "model.json"
    model_path.write_text(
        json.dumps({"model_name": "rgcn", "task": "implicit_reasoning", "sample_count": len(rows)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return model_path
