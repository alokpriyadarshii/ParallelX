from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class TaskSpec:
    id: str
    func: str
    deps: list[str] = field(default_factory=list)
    args: dict[str, Any] = field(default_factory=dict)
    retries: int = 0
    retry_backoff_seconds: float = 0.0
    timeout_seconds: float | None = None
    tags: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Workflow:
    name: str
    tasks: list[TaskSpec]

    def ids(self) -> set[str]:
        return {t.id for t in self.tasks}

    def by_id(self) -> dict[str, TaskSpec]:
        return {t.id: t for t in self.tasks}
