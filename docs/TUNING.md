# Tuning & Scaling

## Choose executor
- `--executor process` (default): best for CPU-heavy tasks (bypasses GIL)
- `--executor thread`: best for I/O-bound tasks (network/disk)

## Concurrency
- `--max-workers N` sets worker count.
- `--tag-limits io=2,cpu=8` caps concurrency for tagged tasks.
  Example: keep I/O pressure low while still using all CPU cores.

## Caching
- `--cache-dir .cache` enables deterministic caching based on function+args.
  Good for incremental development and repeatable pipelines.
