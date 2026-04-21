from __future__ import annotations

import atexit
import sys
import threading
import time
from pathlib import Path


BAR_WIDTH = 42
FILLED_CHAR = "━"
EMPTY_CHAR = "─"
RESET = "\033[0m"
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
DIM = "\033[2m"


def format_elapsed(seconds: float) -> str:
    total_seconds = max(int(seconds), 0)
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:d}:{seconds:02d}"


def colorize(text: str, color: str, *, enabled: bool) -> str:
    if not enabled:
        return text
    return f"{color}{text}{RESET}"


def render_progress(
    label: str,
    current: int,
    total: int,
    elapsed_seconds: float,
    *,
    use_color: bool,
    final: bool = False,
) -> None:
    total = max(int(total), 0)
    current = 0 if total <= 0 else min(max(int(current), 0), total)
    ratio = 0.0 if total <= 0 else current / total
    filled = int(BAR_WIDTH * ratio)
    bar = FILLED_CHAR * filled + EMPTY_CHAR * (BAR_WIDTH - filled)
    stage_label = format_stage_label(label)
    count_text = f"{current}/{total}"
    elapsed_text = format_elapsed(elapsed_seconds)
    print(
        "\r"
        + colorize(stage_label, CYAN, enabled=use_color)
        + "    "
        + colorize(bar, GREEN, enabled=use_color)
        + " "
        + colorize(count_text.rjust(len(count_text)), YELLOW, enabled=use_color)
        + " "
        + colorize(elapsed_text, YELLOW, enabled=use_color),
        end="",
        file=sys.stderr,
        flush=True,
    )
    if final:
        print(file=sys.stderr, flush=True)


def format_stage_label(label: str) -> str:
    if "::" not in label:
        return f"[{label}]:"
    stage_name, phase_name = label.split("::", 1)
    stage_name = stage_name.strip() or "stage"
    phase_name = phase_name.strip()
    if not phase_name:
        return f"[{stage_name}]:"
    return f"[{stage_name}]: {phase_name}"


class StageBarDisplay:
    def __init__(self) -> None:
        self.current_stage: str | None = None
        self.finalizing = False
        self.bar_open = False
        self.last_signature: tuple[int, int] | None = None
        self.stage_started_at: float = time.monotonic()
        self.current_progress: tuple[int, int] = (0, 1)
        self.use_color = sys.stderr.isatty()
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._refresh_thread = threading.Thread(target=self._refresh_loop, daemon=False)
        self._refresh_thread.start()
        atexit.register(self.close)

    def announce_discovery(self, total_sources: int) -> None:
        print(
            colorize(f"law-kg build: {total_sources} documents", CYAN, enabled=self.use_color),
            file=sys.stderr,
            flush=True,
        )

    def _close_bar(self) -> None:
        with self._lock:
            was_open = self.bar_open
            self.bar_open = False
        if was_open:
            print(file=sys.stderr, flush=True)

    def start_stage(self, stage_name: str) -> None:
        if self.current_stage == stage_name:
            return
        if self.current_stage is not None and self.current_stage != stage_name:
            self._close_bar()
        with self._lock:
            self.current_stage = stage_name
            self.finalizing = False
            self.last_signature = None
            self.stage_started_at = time.monotonic()
            self.current_progress = (0, 0)
            self.bar_open = True

    def update(self, current: int, total: int) -> None:
        self.finalizing = False
        total = max(int(total), 0)
        current = 0 if total <= 0 else min(max(int(current), 0), total)
        signature = (current, total)
        with self._lock:
            self.current_progress = signature
            should_render = signature != self.last_signature
            was_open = self.bar_open
            self.last_signature = signature
            self.bar_open = total > 0 and current < total
            current_stage = self.current_stage or "stage"
            started_at = self.stage_started_at
        if should_render or ((total <= 0 or current >= total) and was_open):
            render_progress(
                current_stage,
                current,
                total,
                time.monotonic() - started_at,
                use_color=self.use_color,
                final=total <= 0 or current >= total,
            )

    def handle(self, stage_name: str, current: int, total: int) -> None:
        self.start_stage(stage_name)
        self.update(current, total)

    def finalizing_hint(self, message: str) -> None:
        del message
        if self.finalizing:
            return
        self._close_bar()
        self.finalizing = True

    def stage_summary(self, stage_name: str, summary: dict[str, int]) -> None:
        del stage_name
        self._close_bar()
        fragments = [
            colorize(f"{summary.get('succeeded', 0)} succeed", GREEN, enabled=self.use_color),
            colorize(f"{summary.get('failed', 0)} failed", RED, enabled=self.use_color),
        ]
        skipped = int(summary.get("skipped", 0))
        if skipped:
            fragments.append(colorize(f"{skipped} skipped", YELLOW, enabled=self.use_color))
        print(", ".join(fragments), file=sys.stderr, flush=True)

    def stage_error(self, stage_name: str, error_text: str, job_log_path: str) -> None:
        self._close_bar()
        header = colorize(f"[{stage_name}] failed", RED, enabled=self.use_color)
        print(header, file=sys.stderr, flush=True)
        error_text = str(error_text or "").strip()
        if error_text:
            summary = next((line.strip() for line in reversed(error_text.splitlines()) if line.strip()), "")
            if summary:
                print(summary, file=sys.stderr, flush=True)
        if job_log_path:
            print(
                f"{colorize('log', CYAN, enabled=self.use_color)}: {Path(job_log_path)}",
                file=sys.stderr,
                flush=True,
            )

    def print_final_summary(self, result: dict[str, object]) -> None:
        self._close_bar()
        separator = colorize("─" * 56, DIM, enabled=self.use_color)
        status = str(result.get("status", "completed"))
        source_count = int(result.get("source_count", 0))
        updated_nodes = int(result.get("updated_nodes", 0))
        updated_edges = int(result.get("updated_edges", 0))
        node_count = int(result.get("node_count", 0))
        edge_count = int(result.get("edge_count", 0))
        job_log_path = str(result.get("manifest_path", ""))
        final_nodes = str(result.get("artifact_paths", {}).get("final_nodes", ""))
        final_edges = str(result.get("artifact_paths", {}).get("final_edges", ""))
        print(separator, file=sys.stderr, flush=True)
        print(
            f"{colorize('status', CYAN, enabled=self.use_color)}: {status}",
            file=sys.stderr,
            flush=True,
        )
        print(
            f"{colorize('scope', CYAN, enabled=self.use_color)}: "
            f"{colorize(str(source_count), GREEN, enabled=self.use_color)} documents",
            file=sys.stderr,
            flush=True,
        )
        print(
            f"{colorize('updated', CYAN, enabled=self.use_color)}: "
            f"{colorize(str(updated_nodes), GREEN, enabled=self.use_color)} nodes, "
            f"{colorize(str(updated_edges), GREEN, enabled=self.use_color)} edges",
            file=sys.stderr,
            flush=True,
        )
        print(
            f"{colorize('graph', CYAN, enabled=self.use_color)}: "
            f"{colorize(str(node_count), GREEN, enabled=self.use_color)} nodes, "
            f"{colorize(str(edge_count), GREEN, enabled=self.use_color)} edges",
            file=sys.stderr,
            flush=True,
        )
        if job_log_path:
            print(f"{colorize('log', CYAN, enabled=self.use_color)}: {job_log_path}", file=sys.stderr, flush=True)
        if final_nodes:
            print(f"{colorize('nodes', CYAN, enabled=self.use_color)}: {final_nodes}", file=sys.stderr, flush=True)
        if final_edges:
            print(f"{colorize('edges', CYAN, enabled=self.use_color)}: {final_edges}", file=sys.stderr, flush=True)

    def _refresh_loop(self) -> None:
        while not self._stop_event.wait(0.25):
            with self._lock:
                if not self.bar_open:
                    continue
                current, total = self.current_progress
                stage = self.current_stage or "stage"
                started_at = self.stage_started_at
            if total <= 0 or current >= total:
                continue
            render_progress(
                stage,
                current,
                total,
                time.monotonic() - started_at,
                use_color=self.use_color,
                final=False,
            )

    def close(self) -> None:
        if self._stop_event.is_set():
            return
        self._stop_event.set()
        with self._lock:
            self.bar_open = False
        if self._refresh_thread.is_alive() and threading.current_thread() is not self._refresh_thread:
            self._refresh_thread.join(timeout=1.0)
