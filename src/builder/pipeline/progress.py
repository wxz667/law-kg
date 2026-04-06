from __future__ import annotations

import sys
import time


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


def render_progress(label: str, current: int, total: int, elapsed_seconds: float, *, use_color: bool) -> None:
    total = max(total, 1)
    current = min(max(current, 0), total)
    ratio = current / total
    filled = int(BAR_WIDTH * ratio)
    bar = FILLED_CHAR * filled + EMPTY_CHAR * (BAR_WIDTH - filled)
    stage_label = f"[{label}]:"
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
    if current >= total:
        print(file=sys.stderr, flush=True)


class StageBarDisplay:
    def __init__(self) -> None:
        self.current_stage: str | None = None
        self.finalizing = False
        self.bar_open = False
        self.last_signature: tuple[int, int] | None = None
        self.stage_started_at: float = time.monotonic()
        self.use_color = sys.stderr.isatty()

    def announce_discovery(self, total_sources: int) -> None:
        print(
            colorize(f"law-kg build: {total_sources} documents", CYAN, enabled=self.use_color),
            file=sys.stderr,
            flush=True,
        )

    def _close_bar(self) -> None:
        if self.bar_open:
            print(file=sys.stderr, flush=True)
            self.bar_open = False

    def start_stage(self, stage_name: str) -> None:
        if self.current_stage == stage_name:
            return
        if self.current_stage is not None:
            self._close_bar()
        self.current_stage = stage_name
        self.finalizing = False
        self.last_signature = None
        self.stage_started_at = time.monotonic()

    def update(self, current: int, total: int) -> None:
        self.finalizing = False
        total = max(total, 1)
        current = min(max(current, 0), total)
        signature = (current, total)
        if signature == self.last_signature:
            return
        self.last_signature = signature
        render_progress(
            self.current_stage or "stage",
            current,
            total,
            time.monotonic() - self.stage_started_at,
            use_color=self.use_color,
        )
        self.bar_open = current < total

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
        reused = int(summary.get("reused", 0))
        if reused:
            fragments.append(colorize(f"{reused} reused", YELLOW, enabled=self.use_color))
        print(", ".join(fragments), file=sys.stderr, flush=True)

    def print_final_summary(self, result: dict[str, object]) -> None:
        self._close_bar()
        separator = colorize("─" * 56, DIM, enabled=self.use_color)
        status = str(result.get("status", "completed"))
        succeeded = int(result.get("completed_count", 0))
        failed = int(result.get("failed_count", 0))
        manifest_path = str(result.get("manifest_path", ""))
        final_graph = str(result.get("artifact_paths", {}).get("final_graph_bundle", ""))
        print(separator, file=sys.stderr, flush=True)
        print(
            f"{colorize('status', CYAN, enabled=self.use_color)}: {status}",
            file=sys.stderr,
            flush=True,
        )
        print(
            f"{colorize('result', CYAN, enabled=self.use_color)}: "
            f"{colorize(str(succeeded), GREEN, enabled=self.use_color)} succeed, "
            f"{colorize(str(failed), RED, enabled=self.use_color)} failed",
            file=sys.stderr,
            flush=True,
        )
        if manifest_path:
            print(f"{colorize('manifest', CYAN, enabled=self.use_color)}: {manifest_path}", file=sys.stderr, flush=True)
        if final_graph:
            print(f"{colorize('graph', CYAN, enabled=self.use_color)}: {final_graph}", file=sys.stderr, flush=True)
