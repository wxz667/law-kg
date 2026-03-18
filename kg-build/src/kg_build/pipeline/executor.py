from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from ..io import read_json, write_json
from ..utils.ids import checksum_text
from ..utils.progress import StageProgressReporter

Task = dict[str, Any]
TaskResult = dict[str, Any]

DEFAULT_BATCH_SIZE = 10
DEFAULT_CONCURRENCY = 3


@dataclass(frozen=True)
class StageExecutionConfig:
    stage_name: str
    provider: str
    model: str
    batch_size: int
    concurrency: int

    @property
    def fingerprint(self) -> str:
        payload = {
            "stage_name": self.stage_name,
            "provider": self.provider,
            "model": self.model,
            "batch_size": self.batch_size,
            "concurrency": self.concurrency,
        }
        return checksum_text(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def resolve_execution_config(stage_name: str, provider: str, model: str, params: dict[str, Any]) -> StageExecutionConfig:
    return StageExecutionConfig(
        stage_name=stage_name,
        provider=provider,
        model=model,
        batch_size=max(int(params.get("batch_size", DEFAULT_BATCH_SIZE)), 1),
        concurrency=max(int(params.get("concurrency", DEFAULT_CONCURRENCY)), 1),
    )


def run_layered_tasks(
    *,
    execution_config: StageExecutionConfig,
    stage_dir: Path,
    task_layers: list[list[Task]],
    execute_task: Callable[[Task], TaskResult],
    apply_result: Callable[[TaskResult], None],
    reporter: StageProgressReporter | None = None,
) -> list[TaskResult]:
    results_path = stage_dir / "task_results.jsonl"
    checkpoint_path = stage_dir / "checkpoint.json"
    total_tasks = sum(len(layer) for layer in task_layers)
    stored_results = load_stage_results(results_path)
    checkpoint = load_checkpoint(checkpoint_path, execution_config, total_tasks)
    completed_ids = set(checkpoint.get("completed_task_ids", []))
    result_index = {row["task_id"]: row for row in stored_results}

    for layer in task_layers:
        for task in layer:
            if task["task_id"] in completed_ids:
                apply_result(result_index[task["task_id"]])

    if reporter is not None:
        reporter.stage_started(execution_config.stage_name, total_items=total_tasks)
        if completed_ids:
            reporter.stage_progress(len(completed_ids), total_tasks)

    all_results = list(stored_results)
    for layer in task_layers:
        pending = [task for task in layer if task["task_id"] not in completed_ids]
        for batch in chunked(pending, execution_config.batch_size):
            batch_results = execute_batch(batch, execute_task, execution_config.concurrency)
            append_stage_results(results_path, batch_results)
            for result in batch_results:
                apply_result(result)
                completed_ids.add(result["task_id"])
                all_results.append(result)
            save_checkpoint(checkpoint_path, execution_config, total_tasks, completed_ids)
            if reporter is not None:
                reporter.stage_progress(len(completed_ids), total_tasks)
    return all_results


def run_independent_tasks(
    *,
    execution_config: StageExecutionConfig,
    stage_dir: Path,
    tasks: list[Task],
    execute_task: Callable[[Task], TaskResult],
    apply_result: Callable[[TaskResult], None],
    reporter: StageProgressReporter | None = None,
) -> list[TaskResult]:
    return run_layered_tasks(
        execution_config=execution_config,
        stage_dir=stage_dir,
        task_layers=[tasks],
        execute_task=execute_task,
        apply_result=apply_result,
        reporter=reporter,
    )


def execute_batch(
    tasks: list[Task],
    execute_task: Callable[[Task], TaskResult],
    concurrency: int,
) -> list[TaskResult]:
    if not tasks:
        return []
    if concurrency == 1 or len(tasks) == 1:
        return [execute_task(task) for task in tasks]
    results_by_index: dict[int, TaskResult] = {}
    with ThreadPoolExecutor(max_workers=min(concurrency, len(tasks))) as pool:
        future_map = {
            pool.submit(execute_task, task): index
            for index, task in enumerate(tasks)
        }
        for future, index in ((future, future_map[future]) for future in future_map):
            results_by_index[index] = future.result()
    return [results_by_index[index] for index in sorted(results_by_index)]


def load_stage_results(path: Path) -> list[TaskResult]:
    if not path.exists():
        return []
    rows: list[TaskResult] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def append_stage_results(path: Path, results: Iterable[TaskResult]) -> None:
    rows = list(results)
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_checkpoint(
    path: Path,
    execution_config: StageExecutionConfig,
    total_tasks: int,
) -> dict[str, Any]:
    if not path.exists():
        return {
            "stage_name": execution_config.stage_name,
            "fingerprint": execution_config.fingerprint,
            "total_tasks": total_tasks,
            "completed_task_ids": [],
        }
    payload = read_json(path)
    if payload.get("stage_name") != execution_config.stage_name:
        raise ValueError(f"Checkpoint stage mismatch for {path}.")
    if payload.get("fingerprint") != execution_config.fingerprint:
        raise ValueError(
            f"Checkpoint configuration mismatch for stage {execution_config.stage_name}. "
            "Please remove the checkpoint and rerun."
        )
    return payload


def save_checkpoint(
    path: Path,
    execution_config: StageExecutionConfig,
    total_tasks: int,
    completed_task_ids: set[str],
) -> None:
    write_json(
        path,
        {
            "stage_name": execution_config.stage_name,
            "fingerprint": execution_config.fingerprint,
            "provider": execution_config.provider,
            "model": execution_config.model,
            "batch_size": execution_config.batch_size,
            "concurrency": execution_config.concurrency,
            "total_tasks": total_tasks,
            "completed_task_ids": sorted(completed_task_ids),
        },
    )


def clear_checkpoint_files(stage_dir: Path) -> None:
    for filename in ("checkpoint.json", "task_results.jsonl"):
        path = stage_dir / filename
        if path.exists():
            path.unlink()


def chunked(items: list[Task], size: int) -> Iterable[list[Task]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]
