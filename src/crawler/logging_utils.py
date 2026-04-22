from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

from .io import write_json
from .models import CrawlStats, FailureRecord, utc_now_iso


class RunLogger:
    def __init__(self, logs_dir: Path, command: str, arguments: dict[str, Any]) -> None:
        self.logs_dir = logs_dir
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.command = command
        self.arguments = arguments
        self.started_at = utc_now_iso()
        stamp = self.started_at.replace(":", "").replace("-", "")
        self.summary_path = self.logs_dir / f"run-{stamp}.json"
        self.failures: list[FailureRecord] = []

    def log_failure(self, source_id: str | None, category: str | None, stage: str, error: str) -> None:
        self.failures.append(
            FailureRecord(
                source_id=source_id,
                category=category,
                stage=stage,
                error=error,
                occurred_at=utc_now_iso(),
            )
        )

    def checkpoint(self, stats: CrawlStats, *, finished: bool = False) -> Path:
        summary_payload = {
            "command": self.command,
            "arguments": self.arguments,
            "started_at": self.started_at,
            "updated_at": utc_now_iso(),
            "stats": stats.to_dict(),
            "failure_count": len(self.failures),
            "failures": [asdict(record) for record in self.failures],
        }
        if finished:
            summary_payload["finished_at"] = utc_now_iso()
        write_json(self.summary_path, summary_payload)
        return self.summary_path

    def flush(self, stats: CrawlStats) -> dict[str, Path]:
        summary_path = self.checkpoint(stats, finished=True)
        return {
            "summary": summary_path,
        }
