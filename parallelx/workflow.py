from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set


@dataclass(frozen=True)
class TaskSpec:
    id: str
    func: str
    deps: List[str] = field(default_factory=list)
    args: Dict[str, Any] = field(default_factory=dict)
    retries: int = 0
    retry_backoff_seconds: float = 0.0
    timeout_seconds: Optional[float] = None
    tags: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class Workflow:
    name: str
    tasks: List[TaskSpec]

    def ids(self) -> Set[str]:
        return {t.id for t in self.tasks}

    def by_id(self) -> Dict[str, TaskSpec]:
        return {t.id: t for t in self.tasks}
