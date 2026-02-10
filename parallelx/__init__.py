"""ParallelX: a production-grade parallel workflow engine."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from .engine import Engine, EngineConfig
from .types import TaskStatus
from .workflow import TaskSpec, Workflow

try:
    __version__ = version("parallelx")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"

__all__ = ["Engine", "EngineConfig", "TaskSpec", "TaskStatus", "Workflow", "__version__"]
