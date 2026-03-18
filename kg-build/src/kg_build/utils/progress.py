from __future__ import annotations

import sys
from dataclasses import dataclass


def emit_status(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


@dataclass
class StageProgressReporter:
    def stage_started(self, name: str, total_items: int | None = None) -> None:
        return None

    def stage_progress(self, current: int, total: int, detail: str = "") -> None:
        return None

    def stage_completed(self, name: str, note: str = "") -> None:
        return None

    def stage_reused(self, name: str) -> None:
        return None

    def stage_skipped(self, name: str, reason: str = "") -> None:
        return None


class ConsoleStageProgressReporter(StageProgressReporter):
    def __init__(self, width: int = 28) -> None:
        self.width = width
        self.enabled = sys.stderr.isatty()
        self._active_stage: str | None = None
        self._active_total = 0
        self._progress_mode = False
        self._finished_line = True

    def stage_started(self, name: str, total_items: int | None = None) -> None:
        if total_items is None:
            self._reset_progress()
            emit_status(f"[{name}] starting")
            self._active_stage = name
            return
        if self._active_stage != name:
            emit_status(f"[{name}] starting")
        self._active_stage = name
        self._active_total = max(total_items, 0)
        self._progress_mode = self._active_total > 0
        self._finished_line = not self.enabled
        if self._active_total == 0:
            emit_status(f"{name}: no work items.")
            return
        if self.enabled:
            self._render(0, self._active_total, "")
        else:
            emit_status(f"{name}: 0/{self._active_total}")

    def stage_progress(self, current: int, total: int, detail: str = "") -> None:
        if not self._progress_mode or self._active_stage is None or total <= 0:
            return
        current = min(max(current, 0), total)
        if self.enabled:
            self._render(current, total, detail)
            return
        if current == total or current == 1 or current % 25 == 0:
            suffix = f" {detail}" if detail else ""
            emit_status(f"{self._active_stage}: {current}/{total}{suffix}")

    def stage_completed(self, name: str, note: str = "") -> None:
        if self._progress_mode and self._active_stage == name and self._active_total > 0:
            if self.enabled:
                self._render(self._active_total, self._active_total, "")
                print(file=sys.stderr, flush=True)
            elif note:
                emit_status(f"{name}: {self._active_total}/{self._active_total}")
        message = f"[{name}] completed"
        if note:
            message = f"{message} {note}"
        emit_status(message)
        self._reset_progress()

    def stage_reused(self, name: str) -> None:
        self._reset_progress()
        emit_status(f"[{name}] reused existing artifact")

    def stage_skipped(self, name: str, reason: str = "") -> None:
        self._reset_progress()
        suffix = f" ({reason})" if reason else ""
        emit_status(f"[{name}] skipped{suffix}")

    def _render(self, current: int, total: int, detail: str) -> None:
        ratio = current / total
        filled = int(self.width * ratio)
        bar = "#" * filled + "-" * (self.width - filled)
        percent = int(ratio * 100)
        suffix = f" {detail}" if detail else ""
        print(
            f"\r{self._active_stage} [{bar}] {current}/{total} {percent:3d}%{suffix}",
            end="",
            file=sys.stderr,
            flush=True,
        )
        self._finished_line = current == total

    def _reset_progress(self) -> None:
        if self.enabled and self._progress_mode and not self._finished_line:
            print(file=sys.stderr, flush=True)
        self._active_stage = None
        self._active_total = 0
        self._progress_mode = False
        self._finished_line = True
