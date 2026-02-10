from __future__ import annotations

import math
import os
import random
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


def read_text(path: str, encoding: str = "utf-8") -> str:
    return Path(path).read_text(encoding=encoding)


def split_words(text: str) -> List[str]:
    # Basic tokenizer (industry projects often replace this with a real tokenizer)
    out: List[str] = []
    word = []
    for ch in text.lower():
        if ch.isalnum():
            word.append(ch)
        elif word:
            out.append("".join(word))
            word.clear()
    if word:
        out.append("".join(word))
    return out


def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    return [items[i:i+chunk_size] for i in range(0, len(items), chunk_size)]


def count_words(words: List[str]) -> Dict[str, int]:
    return dict(Counter(words))


def merge_counts(*counts: Dict[str, int]) -> Dict[str, int]:
    total: Counter[str] = Counter()
    for c in counts:
        total.update(c)
    return dict(total)


def random_points(n: int, seed: int | None = None) -> List[Tuple[float, float]]:
    rng = random.Random(seed)
    return [(rng.random(), rng.random()) for _ in range(n)]


def count_inside_unit_circle(points: List[Tuple[float, float]]) -> int:
    inside = 0
    for x, y in points:
        if x*x + y*y <= 1.0:
            inside += 1
    return inside


def estimate_pi(inside: int, total: int) -> float:
    if total <= 0:
        raise ValueError("total must be > 0")
    return 4.0 * inside / float(total)


def sum_numbers(nums: List[float]) -> float:
    return float(sum(nums))


def gen_numbers(n: int, seed: int | None = None) -> List[float]:
    rng = random.Random(seed)
    return [rng.random() for _ in range(n)]


def save_json(data: Any, path: str) -> str:
    import json
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path

def get_item(items: List[Any], index: int) -> Any:
    return items[index]

def merge_counts_list(counts: List[Dict[str, int]]) -> Dict[str, int]:
    return merge_counts(*counts)
