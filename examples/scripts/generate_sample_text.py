from __future__ import annotations

import random
from pathlib import Path

WORDS = [
    "parallel", "computing", "workflow", "engine", "task", "dependency",
    "scheduler", "executor", "process", "thread", "cache", "retry",
    "metrics", "logging", "python", "industry", "standard", "scalable"
]

def main() -> None:
    rng = random.Random(42)
    out = []
    for _ in range(50000):
        out.append(rng.choice(WORDS))
    text = " ".join(out)
    Path("examples/data").mkdir(parents=True, exist_ok=True)
    Path("examples/data/sample.txt").write_text(text, encoding="utf-8")
    print("Wrote examples/data/sample.txt")

if __name__ == "__main__":
    main()
