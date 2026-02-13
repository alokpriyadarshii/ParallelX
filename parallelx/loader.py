from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .workflow import TaskSpec, Workflow


class WorkflowValidationError(ValueError):
    pass


def load_workflow(path: str) -> Workflow:
    p = Path(path)
    data = json.loads(p.read_text(encoding="utf-8"))
    return parse_workflow(data, default_name=p.stem)


def parse_workflow(data: dict[str, Any], default_name: str = "workflow") -> Workflow:
    if not isinstance(data, dict):
        raise WorkflowValidationError("Workflow JSON must be an object.")
    name = str(data.get("name") or default_name)
    tasks_raw = data.get("tasks")
    if not isinstance(tasks_raw, list) or not tasks_raw:
        raise WorkflowValidationError("'tasks' must be a non-empty list.")
    tasks: list[TaskSpec] = []
    seen: set[str] = set()
    for i, t in enumerate(tasks_raw):
        if not isinstance(t, dict):
            raise WorkflowValidationError(f"Task at index {i} must be an object.")
        tid = t.get("id")
        func = t.get("func")
        if not isinstance(tid, str) or not tid.strip():
            raise WorkflowValidationError(f"Task at index {i} missing valid 'id'.")
        if tid in seen:
            raise WorkflowValidationError(f"Duplicate task id '{tid}'.")
        seen.add(tid)
        if not isinstance(func, str) or ":" not in func:
            raise WorkflowValidationError(f"Task '{tid}' missing valid 'func' (module:function).")

        deps = t.get("deps") or []
        args = t.get("args") or {}
        retries = _parse_int_field(tid, "retries", t.get("retries"), default=0, minimum=0)
        backoff = _parse_float_field(
            tid,
            "retry_backoff_seconds",
            t.get("retry_backoff_seconds"),
            default=0.0,
            minimum=0.0,
        )
        timeout = t.get("timeout_seconds")
        timeout_f = _parse_float_field(tid, "timeout_seconds", timeout, default=None, minimum=0.0)
        tags = t.get("tags") or []
        if not isinstance(deps, list) or any(not isinstance(d, str) for d in deps):
            raise WorkflowValidationError(f"Task '{tid}': 'deps' must be a list of strings.")
        if not isinstance(args, dict):
            raise WorkflowValidationError(f"Task '{tid}': 'args' must be an object.")
        if not isinstance(tags, list) or any(not isinstance(x, str) for x in tags):
            raise WorkflowValidationError(f"Task '{tid}': 'tags' must be a list of strings.")
        tasks.append(TaskSpec(
            id=tid,
            func=func,
            deps=list(deps),
            args=dict(args),
            retries=retries,
            retry_backoff_seconds=backoff,
            timeout_seconds=timeout_f,
            tags=list(tags),
        ))

    # Validate deps exist and detect cycles via DFS
    by_id = {t.id: t for t in tasks}
    for t in tasks:
        for d in t.deps:
            if d not in by_id:
                raise WorkflowValidationError(f"Task '{t.id}' depends on unknown task '{d}'.")

    _assert_acyclic(tasks)
    return Workflow(name=name, tasks=tasks)


def _assert_acyclic(tasks: list[TaskSpec]) -> None:
    by_id = {t.id: t for t in tasks}
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = {tid: WHITE for tid in by_id}

    def dfs(u: str, stack: list[str]) -> None:
        color[u] = GRAY
        stack.append(u)
        for v in by_id[u].deps:
            if color[v] == WHITE:
                dfs(v, stack)
            elif color[v] == GRAY:
                cycle = [*stack[stack.index(v):], v]
                raise WorkflowValidationError(f"Cycle detected: {' -> '.join(cycle)}")
        stack.pop()
        color[u] = BLACK

    for tid in by_id:
        if color[tid] == WHITE:
            dfs(tid, [])


def _parse_int_field(
    task_id: str,
    field_name: str,
    raw: Any,
    *,
    default: int,
    minimum: int,
) -> int:
    if raw is None:
        return default
    if isinstance(raw, bool):
        raise WorkflowValidationError(f"Task '{task_id}': '{field_name}' must be an integer.")
    try:
        value = int(raw)
    except (TypeError, ValueError) as e:
        raise WorkflowValidationError(f"Task '{task_id}': '{field_name}' must be an integer.") from e
    if value < minimum:
        raise WorkflowValidationError(
            f"Task '{task_id}': '{field_name}' must be >= {minimum}."
        )
    return value


def _parse_float_field(
    task_id: str,
    field_name: str,
    raw: Any,
    *,
    default: float | None,
    minimum: float,
) -> float | None:
    if raw is None:
        return default
    if isinstance(raw, bool):
        raise WorkflowValidationError(f"Task '{task_id}': '{field_name}' must be a number.")
    try:
        value = float(raw)
    except (TypeError, ValueError) as e:
        raise WorkflowValidationError(f"Task '{task_id}': '{field_name}' must be a number.") from e
    if value < minimum:
        raise WorkflowValidationError(
            f"Task '{task_id}': '{field_name}' must be >= {minimum}."
        )
    return value


def _parse_optional_float_field(
    task_id: str,
    field_name: str,
    raw: Any,
    *,
    minimum: float,
) -> float | None:
    if raw is None:
        return None
    return _parse_float_field(task_id, field_name, raw, default=0.0, minimum=minimum)
