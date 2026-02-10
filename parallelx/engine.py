from __future__ import annotations

import concurrent.futures as cf
import dataclasses
import json
import os
import sys
import time
import traceback as tb
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .types import ErrorInfo, RunSummary, TaskOutcome, TaskStatus
from .utils import DiskCache, import_func, now_ts, to_cache_key
from .workflow import TaskSpec, Workflow


@dataclass(frozen=True)
class EngineConfig:
    max_workers: int = max(1, (os.cpu_count() or 2) - 1)
    executor: str = "process"  # "process" or "thread"
    cache_dir: str | None = None
    # Tag-based concurrency limits, e.g. {"io": 2, "cpu": 8}
    max_concurrency_by_tag: dict[str, int] = dataclasses.field(default_factory=dict)
    # Set to True to also log task stdout/stderr (best effort)
    verbose: bool = False
    # Set to False to disable engine JSON-line logs (useful for tests/integration)
    emit_logs: bool = True


def _resolve_refs(obj: Any, results: dict[str, TaskOutcome]) -> Any:
    """Replace {"ref": "<task_id>"} with that task's output value."""
    if isinstance(obj, dict) and set(obj.keys()) == {"ref"} and isinstance(obj["ref"], str):
        ref_id = obj["ref"]
        if ref_id not in results:
            raise KeyError(f"Unknown ref '{ref_id}'.")
        out = results[ref_id]
        if out.status != TaskStatus.SUCCESS:
            raise RuntimeError(f"Ref '{ref_id}' is not successful (status={out.status}).")
        return out.value
    if isinstance(obj, dict):
        return {k: _resolve_refs(v, results) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_refs(v, results) for v in obj]
    return obj


def _worker_call(func_path: str, kwargs: dict[str, Any], timeout_seconds: float | None) -> Any:
    """Executed in worker process/thread.

    Notes on timeouts:
    - For **process** workers on Unix-like systems, we best-effort enforce timeouts via SIGALRM.
    - For threads (or platforms without SIGALRM), timeouts are soft and may not interrupt execution.
    """
    fn = import_func(func_path)

    if timeout_seconds is None or timeout_seconds <= 0:
        return fn(**kwargs)

    # Best-effort hard timeout in a worker process (Unix)
    try:
        import signal

        if hasattr(signal, "SIGALRM"):
            def _handler(_signum: int, _frame: Any) -> None:  # pragma: no cover
                raise TimeoutError(f"Task timed out after {timeout_seconds} seconds")

            old_handler = signal.signal(signal.SIGALRM, _handler)
            signal.setitimer(signal.ITIMER_REAL, float(timeout_seconds))
            try:
                return fn(**kwargs)
            finally:
                signal.setitimer(signal.ITIMER_REAL, 0)
                signal.signal(signal.SIGALRM, old_handler)
    except Exception:
        # Fall back to no hard timeout.
        pass

    return fn(**kwargs)


class Engine:
    def __init__(self, config: EngineConfig) -> None:
        self.config = config
        self.cache = DiskCache(config.cache_dir) if config.cache_dir else None

   def run(self, workflow: Workflow) -> tuple[dict[str, TaskOutcome], RunSummary]:        
        started = datetime.now(timezone.utc)
        t0 = now_ts()

        by_id = workflow.by_id()
        deps_left: dict[str, set[str]] = {t.id: set(t.deps) for t in workflow.tasks}
        dependents: dict[str, set[str]] = {t.id: set() for t in workflow.tasks}
        for t in workflow.tasks:
            for d in t.deps:
                dependents[d].add(t.id)

        ready: set[str] = {tid for tid, deps in deps_left.items() if not deps}
        running: dict[cf.Future, str] = {}
        outcomes: dict[str, TaskOutcome] = {}
        attempts: dict[str, int] = {t.id: 0 for t in workflow.tasks}
        cache_hits = 0
        cache_misses = 0

        tag_inflight: dict[str, int] = {k: 0 for k in tag_limits}
        tag_inflight: Dict[str, int] = {k: 0 for k in tag_limits}

        def can_run(t: TaskSpec) -> bool:
            for tag in t.tags:
                if tag in tag_limits and tag_inflight[tag] >= tag_limits[tag]:
                    return False
            return True

        def on_start(t: TaskSpec) -> None:
            for tag in t.tags:
                if tag in tag_limits:
                    tag_inflight[tag] += 1

        def on_finish(t: TaskSpec) -> None:
            for tag in t.tags:
                if tag in tag_limits:
                    tag_inflight[tag] = max(0, tag_inflight[tag] - 1)

        def submit_one(executor: cf.Executor, tid: str) -> None:
            nonlocal cache_hits, cache_misses
            t = by_id[tid]
            attempts[tid] += 1
            started_at = now_ts()

            # Resolve refs in kwargs
            resolved_kwargs = _resolve_refs(t.args, outcomes)

            # Cache check
            cache_key = None
            if self.cache is not None:
                cache_key = to_cache_key(t.func, resolved_kwargs)
                hit, cached = self.cache.get(cache_key)
                if hit:
                    cache_hits += 1
                    outcomes[tid] = TaskOutcome(
                        status=TaskStatus.SUCCESS,
                        value=cached,
                        error=None,
                        started_at=started_at,
                        finished_at=started_at,
                        attempts=attempts[tid],
                    )
                    # mark dependents
                    for child in dependents[tid]:
                        deps_left[child].discard(tid)
                        if not deps_left[child]:
                            ready.add(child)
                    return
                cache_misses += 1

            on_start(t)
            fut = executor.submit(_worker_call, t.func, resolved_kwargs, t.timeout_seconds)
            # Attach metadata
            fut._parallelx_meta = {  # type: ignore[attr-defined]
                "id": tid,
                "func": t.func,
                "cache_key": cache_key,
                "started_at": started_at,
                "timeout": t.timeout_seconds,
            }
            running[fut] = tid

        executor_type = self.config.executor.lower().strip()
        if executor_type not in {"process", "thread"}:
            raise ValueError("EngineConfig.executor must be 'process' or 'thread'.")

        executor_cls = cf.ProcessPoolExecutor if executor_type == "process" else cf.ThreadPoolExecutor

        # Basic structured logs (JSON lines)
        def log(event: str, **fields: Any) -> None:
            if not self.config.emit_logs:
                return
            payload = {"ts": datetime.now(timezone.utc).isoformat(), "event": event, **fields}
            print(json.dumps(payload, ensure_ascii=False), file=sys.stderr)

        log("run_start", workflow=workflow.name, max_workers=self.config.max_workers, executor=executor_type)

        with executor_cls(max_workers=self.config.max_workers) as ex:
            while ready or running:
                # Submit as many as possible
                submitted = False
                for tid in list(sorted(ready)):
                    if tid in outcomes:
                        ready.discard(tid)
                        continue
                    t = by_id[tid]
                    if not can_run(t):
                        continue
                    ready.discard(tid)
                    submit_one(ex, tid)
                    submitted = True
                    log("task_submitted", task_id=tid, func=t.func, tags=t.tags, attempt=attempts[tid])

                if not running:
                    # Nothing running; avoid tight loop when ready tasks are blocked by tag limits
                    if ready and not submitted:
                        time.sleep(0.01)
                        continue
                    break

                done, _ = cf.wait(running.keys(), timeout=0.05, return_when=cf.FIRST_COMPLETED)

                for fut in list(done):
                    tid = running.pop(fut)
                    t = by_id[tid]
                    meta = getattr(fut, "_parallelx_meta", {})
                    started_at = float(meta.get("started_at") or now_ts())
        
                    try:
                        # Soft timeout: only effective if future has completed by now
                        value = fut.result(timeout=0)  # already done
                        finished_at = now_ts()
                        outcomes[tid] = TaskOutcome(
                            status=TaskStatus.SUCCESS,
                            value=value,
                            error=None,
                            started_at=started_at,
                            finished_at=finished_at,
                            attempts=attempts[tid],
                        )
                        # Write cache
                        ck = meta.get("cache_key")
                        if self.cache is not None and isinstance(ck, str):
                            try:
                                self.cache.set(ck, value)
                            except Exception:
                                # Cache failures should not fail the run
                                log("cache_write_failed", task_id=tid)

                        log("task_success", task_id=tid, duration_seconds=outcomes[tid].duration_seconds, attempt=attempts[tid])
                        on_finish(t)

                        for child in dependents[tid]:
                            deps_left[child].discard(tid)
                            if not deps_left[child]:
                                ready.add(child)

                    except Exception as e:
                        finished_at = now_ts()
                        err = ErrorInfo(
                            exc_type=type(e).__name__,
                            message=str(e),
                            traceback="".join(tb.format_exception(type(e), e, e.__traceback__)),
                        )
                        # Retry?
                        if attempts[tid] <= t.retries:
                            on_finish(t)
                            sleep_s = t.retry_backoff_seconds * (2 ** (attempts[tid] - 1))
                            log("task_retry", task_id=tid, attempt=attempts[tid], retries=t.retries, backoff_seconds=sleep_s)
                            if sleep_s > 0:
                                time.sleep(min(5.0, sleep_s))
                            ready.add(tid)
                            continue

                        outcomes[tid] = TaskOutcome(
                            status=TaskStatus.FAILED,
                            value=None,
                            error=err,
                            started_at=started_at,
                            finished_at=finished_at,
                            attempts=attempts[tid],
                        )
                        fields: dict[str, Any] = {
                            "task_id": tid,
                            "attempt": attempts[tid],
                            "error_type": err.exc_type,
                            "error_message": err.message,
                        }
                        if self.config.verbose:
                            fields["error_traceback"] = err.traceback
                        log("task_failed", **fields)
                        on_finish(t)

                        # If a task fails, mark all downstream as SKIPPED
                        for child in _collect_downstream(tid, dependents):
                            if child in outcomes:
                                continue
                            outcomes[child] = TaskOutcome(
                                status=TaskStatus.SKIPPED,
                                value=None,
                                error=None,
                                started_at=finished_at,
                                finished_at=finished_at,
                                attempts=0,
                            )
                            log("task_skipped", task_id=child, reason=f"upstream_failed:{tid}")

                        # Remove skipped tasks from ready
                        ready -= set(outcomes.keys())

        finished = datetime.now(timezone.utc)
        t1 = now_ts()

        statuses = {tid: out.status for tid, out in outcomes.items()}
        durations = {tid: out.duration_seconds for tid, out in outcomes.items()}

        summary = RunSummary(
            workflow_name=workflow.name,
            started_at_iso=started.isoformat(),
            finished_at_iso=finished.isoformat(),
            statuses=statuses,
            durations=durations,
            cache_hits=cache_hits,
            cache_misses=cache_misses,
        )

        log("run_finished", workflow=workflow.name, wall_seconds=max(0.0, t1 - t0), cache_hits=cache_hits, cache_misses=cache_misses)
        return outcomes, summary


def _collect_downstream(root: str, dependents: dict[str, set[str]]) -> set[str]:
    seen: set[str] = set()
    stack = [root]
    while stack:
        u = stack.pop()
        for v in dependents.get(u, set()):
            if v not in seen:
                seen.add(v)
                stack.append(v)
    return seen
