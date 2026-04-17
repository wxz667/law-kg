from __future__ import annotations

import argparse
import json
import signal
import shutil
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from .api import InterpretFilterInput
from .config import DEFAULT_CONFIG_PATH
from .dataset import build_dataset
from .predict import predict
from .train import train

BAR_WIDTH = 36
FILLED_CHAR = "━"
EMPTY_CHAR = "─"
RESET = "\033[0m"
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
DIM = "\033[2m"


def colorize(text: str, color: str, *, enabled: bool) -> str:
    if not enabled:
        return text
    return f"{color}{text}{RESET}"


def format_elapsed(seconds: float) -> str:
    total_seconds = max(int(seconds), 0)
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:d}:{seconds:02d}"


class StageReporter:
    def __init__(self) -> None:
        self.use_color = sys.stderr.isatty()
        self.started_at = time.monotonic()
        self.last_signature: tuple[str, int, int] | None = None
        self.bar_open = False

    def stage(self, name: str) -> None:
        self._close_bar()
        print(colorize(f"[{name}] start", CYAN, enabled=self.use_color), file=sys.stderr, flush=True)

    def update(self, stage: str, current: int, total: int) -> None:
        total = max(total, 1)
        current = min(max(current, 0), total)
        signature = (stage, current, total)
        if signature == self.last_signature:
            return
        self.last_signature = signature
        ratio = current / total
        filled = int(BAR_WIDTH * ratio)
        bar = FILLED_CHAR * filled + EMPTY_CHAR * (BAR_WIDTH - filled)
        elapsed = format_elapsed(time.monotonic() - self.started_at)
        print(
            "\r"
            + colorize(f"[{stage}]", CYAN, enabled=self.use_color)
            + " "
            + colorize(bar, GREEN, enabled=self.use_color)
            + " "
            + colorize(f"{current}/{total}", YELLOW, enabled=self.use_color)
            + " "
            + colorize(elapsed, DIM, enabled=self.use_color),
            end="",
            file=sys.stderr,
            flush=True,
        )
        self.bar_open = current < total
        if current >= total:
            self._close_bar()

    def complete(self, stage: str, summary: str) -> None:
        self._close_bar()
        print(colorize(f"[{stage}] {summary}", GREEN, enabled=self.use_color), file=sys.stderr, flush=True)

    def _close_bar(self) -> None:
        if self.bar_open:
            print(file=sys.stderr, flush=True)
            self.bar_open = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build datasets, train, or predict with the interprets filter.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run dataset build, training, or both.")
    run_parser.add_argument("--stage", choices=("all", "dataset", "train"), default="all", help="Pipeline stage to execute.")
    run_parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to the project config file.")
    run_parser.add_argument("--data-root", default="data", help="Path to the project data directory.")
    run_parser.add_argument("--model-dir", default="models/interprets_filter", help="Directory for model artifacts.")
    run_parser.add_argument("--sample-size", type=int, help="Dataset size for this run.")
    run_parser.add_argument("--rebuild", action="store_true", help="Clear stage outputs before running.")
    run_parser.add_argument("--incremental", action="store_true", help="Incrementally extend an existing dataset instead of rebuilding from scratch.")

    predict_parser = subparsers.add_parser("predict", help="Run one-off prediction.")
    predict_parser.add_argument("--text", required=True, help="Input text with [T] markers.")
    predict_parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to the project config file.")
    predict_parser.add_argument("--model-dir", default="models/interprets_filter", help="Directory for model artifacts.")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "predict":
        results = predict([InterpretFilterInput(text=args.text)], model_dir=Path(args.model_dir), config_path=Path(args.config))
        print(json.dumps([result.__dict__ for result in results], ensure_ascii=False))
        return 0

    reporter = StageReporter()
    cancel_event = threading.Event()
    previous_handler = install_interrupt_handler(cancel_event)
    data_root = Path(args.data_root)
    dataset_dir = data_root / "train" / "interprets_filter"
    detect_dir = data_root / "intermediate" / "builder" / "03_detect"
    intermediate_dir = data_root / "intermediate" / "interprets_filter"
    logs_dir = Path("logs/interprets_filter")
    model_dir = Path(args.model_dir)

    if args.rebuild:
        clear_stage_outputs(args.stage, dataset_dir, intermediate_dir, model_dir, logs_dir)

    dataset_stats: dict[str, Any] | None = None
    model_metrics: dict[str, Any] | None = None

    try:
        if args.stage in {"all", "dataset"}:
            reporter.stage("dataset")
            dataset_stats = build_dataset(
                detect_dir=detect_dir,
                output_dir=dataset_dir,
                config_path=Path(args.config),
                limit=args.sample_size,
                intermediate_dir=intermediate_dir,
                progress_callback=lambda current, total: reporter.update("dataset", current, total),
                incremental=args.incremental,
                cancel_event=cancel_event,
            )
            reporter.update("dataset", dataset_stats.get("distilled", 0), max(dataset_stats.get("requested", 0), 1))
            reporter.complete("dataset", f"sampled={dataset_stats.get('sampled_candidates')} distilled={dataset_stats.get('distilled')}")

        if args.stage in {"all", "train"}:
            reporter.stage("train")
            reporter.update("train", 0, 1)
            trained_model_dir = train(dataset_dir=dataset_dir, output_dir=model_dir, config_path=Path(args.config))
            reporter.update("train", 1, 1)
            model_metrics = load_json_if_exists(trained_model_dir / "metrics.json")
            reporter.complete("train", summarize_metrics(model_metrics))

        summary_path = write_run_summary(
            logs_dir=logs_dir,
            stage=args.stage,
            sample_size=args.sample_size,
            dataset_stats=dataset_stats,
            model_dir=model_dir if args.stage in {"all", "train"} else None,
            model_metrics=model_metrics,
        )
        print(summary_path)
        return 0
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    finally:
        signal.signal(signal.SIGINT, previous_handler)
        reporter._close_bar()


def clear_stage_outputs(stage: str, dataset_dir: Path, intermediate_dir: Path, model_dir: Path, logs_dir: Path) -> None:
    if stage in {"all", "dataset"}:
        for path in (dataset_dir, intermediate_dir):
            if path.exists():
                shutil.rmtree(path)
    if stage in {"all", "train"} and model_dir.exists():
        shutil.rmtree(model_dir)
    if logs_dir.exists():
        for child in logs_dir.iterdir():
            if child.name == ".gitkeep":
                continue
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()


def install_interrupt_handler(cancel_event: threading.Event) -> signal.Handlers:
    previous_handler = signal.getsignal(signal.SIGINT)
    interrupt_state = {"count": 0}

    def handle_interrupt(signum: int, frame: object) -> None:
        del frame
        interrupt_state["count"] += 1
        cancel_event.set()
        if interrupt_state["count"] >= 2:
            if callable(previous_handler):
                previous_handler(signum, None)
                return
            raise KeyboardInterrupt

    signal.signal(signal.SIGINT, handle_interrupt)
    return previous_handler


def load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def summarize_metrics(model_metrics: dict[str, Any] | None) -> str:
    if not model_metrics:
        return "metrics unavailable"
    test_metrics = dict(model_metrics.get("test", {}))
    dev_metrics = dict(model_metrics.get("dev", {}))
    parts: list[str] = []
    if "device" in model_metrics:
        parts.append(f"device={model_metrics['device']}")
    if dev_metrics:
        if "recall" in dev_metrics:
            parts.append(f"dev_recall={dev_metrics['recall']:.4f}")
        if "f1" in dev_metrics:
            parts.append(f"dev_f1={dev_metrics['f1']:.4f}")
    if test_metrics:
        if "recall" in test_metrics:
            parts.append(f"test_recall={test_metrics['recall']:.4f}")
        if "f1" in test_metrics:
            parts.append(f"test_f1={test_metrics['f1']:.4f}")
    return " ".join(parts) if parts else "metrics unavailable"


def write_run_summary(
    logs_dir: Path,
    stage: str,
    sample_size: int | None,
    dataset_stats: dict[str, Any] | None,
    model_dir: Path | None,
    model_metrics: dict[str, Any] | None,
) -> Path:
    logs_dir.mkdir(parents=True, exist_ok=True)
    for child in logs_dir.iterdir():
        if child.name == ".gitkeep":
            continue
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    summary_path = logs_dir / f"interprets_filter-{timestamp}.md"
    lines = [
        "# interprets_filter run summary",
        "",
        f"- stage: `{stage}`",
        f"- sample_size: `{sample_size}`",
    ]
    if dataset_stats:
        lines.extend(
            [
                f"- sampled_candidates: `{dataset_stats.get('sampled_candidates')}`",
                f"- distilled: `{dataset_stats.get('distilled')}`",
                f"- label_distribution: `{json.dumps(dataset_stats.get('label_distribution', {}), ensure_ascii=False)}`",
            ]
        )
    if model_dir is not None:
        lines.append(f"- model_dir: `{model_dir}`")
    if model_metrics:
        lines.append(f"- metrics: `{json.dumps(model_metrics, ensure_ascii=False)}`")
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return summary_path

if __name__ == "__main__":
    raise SystemExit(main())
