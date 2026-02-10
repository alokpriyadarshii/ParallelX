from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict

from .engine import Engine, EngineConfig
from .loader import WorkflowValidationError, load_workflow


def _parse_tag_limits(s: str) -> Dict[str, int]:
    """Parse 'io=2,cpu=8' into a dict."""
    out: Dict[str, int] = {}
    if not s.strip():
        return out
    for part in s.split(","):
        if not part.strip():
            continue
        if "=" not in part:
            raise ValueError(f"Invalid tag limit '{part}'. Expected key=value.")
        k, v = part.split("=", 1)
        out[k.strip()] = max(1, int(v.strip()))
    return out


def main(argv: Any = None) -> int:
    parser = argparse.ArgumentParser(prog="parallelx", description="ParallelX workflow runner.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    runp = sub.add_parser("run", help="Run a workflow JSON.")
    runp.add_argument("workflow", help="Path to workflow JSON file")
    runp.add_argument("--max-workers", type=int, default=None, help="Max parallel workers")
    runp.add_argument("--executor", choices=["process", "thread"], default="process", help="Executor type")
    runp.add_argument("--cache-dir", default=None, help="Enable disk cache directory")
    runp.add_argument("--tag-limits", default="", help="Tag concurrency limits like 'io=2,cpu=8'")
    runp.add_argument("--summary-json", default=None, help="Write run summary JSON to this path")
    runp.add_argument("--verbose", action="store_true", help="Include full tracebacks in logs")
    runp.add_argument("--quiet", action="store_true", help="Disable engine logs")

    args = parser.parse_args(argv)

    if args.cmd == "run":
        try:
            wf = load_workflow(args.workflow)
        except (OSError, json.JSONDecodeError, WorkflowValidationError) as e:
            print(f"Error: {e}", file=sys.stderr)
            return 2

        try:
            tag_limits = _parse_tag_limits(args.tag_limits)
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            return 2

        cfg = EngineConfig(
            max_workers=args.max_workers or EngineConfig().max_workers,
            executor=args.executor,
            cache_dir=args.cache_dir,
            max_concurrency_by_tag=tag_limits,
            verbose=bool(args.verbose),
            emit_logs=not bool(args.quiet),
        )

        try:
            engine = Engine(cfg)
            outcomes, summary = engine.run(wf)
        except KeyboardInterrupt:
            print("Interrupted", file=sys.stderr)
            return 130

        # Print a human-friendly summary to stdout
        ok = 0
        failed = 0
        skipped = 0
        for tid in sorted(wf.ids()):
            st = outcomes.get(tid).status if tid in outcomes else None
            if st is not None and st.value == "SUCCESS":
                ok += 1
            elif st is not None and st.value == "FAILED":
                failed += 1
            elif st is not None and st.value == "SKIPPED":
                skipped += 1

        print(f"Workflow: {summary.workflow_name}")
        print(f"Cache: hits={summary.cache_hits} misses={summary.cache_misses}")
        print(f"Tasks: success={ok} failed={failed} skipped={skipped}")

        if args.summary_json:
            with open(args.summary_json, "w", encoding="utf-8") as f:
                json.dump(summary.__dict__, f, ensure_ascii=False, indent=2)

        return 1 if failed else 0

    return 0
