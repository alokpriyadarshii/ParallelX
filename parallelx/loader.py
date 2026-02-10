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
        retries = int(t.get("retries") or 0)
        backoff = float(t.get("retry_backoff_seconds") or 0.0)
        timeout = t.get("timeout_seconds")
        timeout_f = float(timeout) if timeout is not None else None
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
            retries=max(0, retries),
            retry_backoff_seconds=max(0.0, backoff),
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
