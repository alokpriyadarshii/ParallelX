# Architecture

ParallelX is intentionally small, but structured like a production service/library.

## Data model
- **Workflow**: named collection of tasks
- **TaskSpec**: user-specified definition (id, function path, deps, args, retries, timeout, tags)
- **TaskOutcome**: execution result (status, value, error, timings)

## Execution pipeline

1) **Load & validate**
- `loader.parse_workflow()` validates schema and builds a `Workflow`.

2) **Plan**
- Build a dependency graph from task `deps`.
- Initialize a *ready* set with tasks that have no remaining dependencies.

3) **Schedule**
- While there are ready tasks or running futures:
  - Submit ready tasks to the executor (respecting tag concurrency limits).
  - Collect completed tasks, record outcomes.
  - On success: release dependent tasks.
  - On failure: optionally retry; otherwise mark downstream tasks as skipped.

4) **Persist (optional)**
- If `--cache-dir` is enabled, successful task outputs are stored on disk by a deterministic key.

## Concurrency model
- **ProcessPoolExecutor**: best for CPU-heavy tasks (GIL bypass).
- **ThreadPoolExecutor**: best for I/O-heavy tasks.

Tag limits let you model mixed workloads:
- Example: `--tag-limits io=2,cpu=8`.

## Observability
- Engine emits JSON-lines logs to stderr (easy to ship to log pipelines).
- Run summary is available programmatically and via `--summary-json`.
