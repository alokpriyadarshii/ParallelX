from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass(frozen=True)
class ErrorInfo:
    exc_type: str
    message: str
    traceback: str


@dataclass(frozen=True)
class TaskOutcome:
    status: TaskStatus
    value: Any
    error: Optional[ErrorInfo]
    started_at: float
    finished_at: float
    attempts: int

    @property
    def duration_seconds(self) -> float:
        return max(0.0, self.finished_at - self.started_at)


@dataclass(frozen=True)
class RunSummary:
    workflow_name: str
    started_at_iso: str
    finished_at_iso: str
    statuses: Dict[str, TaskStatus]
    durations: Dict[str, float]
    cache_hits: int
    cache_misses: int
