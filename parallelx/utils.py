from __future__ import annotations

import hashlib
import importlib
import json
import os
import pickle
import time
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Tuple


def import_func(path: str) -> Callable[..., Any]:
    """Import a callable from a string like 'pkg.mod:function'."""
    if ":" not in path:
        raise ValueError(f"Invalid func path '{path}'. Expected 'module:function'.")
    mod_name, fn_name = path.split(":", 1)
    mod = importlib.import_module(mod_name)
    fn = getattr(mod, fn_name, None)
    if fn is None or not callable(fn):
        raise ValueError(f"'{path}' does not resolve to a callable.")
    return fn


def now_ts() -> float:
    return time.time()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def stable_json(obj: Any) -> str:
    """JSON string with stable ordering for hashing."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def to_cache_key(func_path: str, resolved_kwargs: Dict[str, Any]) -> str:
    payload = {"func": func_path, "kwargs": _safe_for_hash(resolved_kwargs)}
    return sha256_bytes(stable_json(payload).encode("utf-8"))


def _safe_for_hash(x: Any) -> Any:
    # Make best effort to convert to a deterministic, JSON-able structure.
    if is_dataclass(x):
        return _safe_for_hash(asdict(x))
    if isinstance(x, (str, int, float, bool)) or x is None:
        return x
    if isinstance(x, (list, tuple)):
        return [_safe_for_hash(v) for v in x]
    if isinstance(x, dict):
        return {str(k): _safe_for_hash(v) for k, v in sorted(x.items(), key=lambda kv: str(kv[0]))}
    # Fallback: repr
    return {"__repr__": repr(x)}


class DiskCache:
    """Deterministic disk cache for task results (pickle-based)."""

    def __init__(self, root: str) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        # fan-out to reduce directory size
        return self.root / key[:2] / key[2:4] / f"{key}.pkl"

    def get(self, key: str) -> Tuple[bool, Any]:
        p = self._path(key)
        if not p.exists():
            return False, None
        with p.open("rb") as f:
            return True, pickle.load(f)

    def set(self, key: str, value: Any) -> None:
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        with tmp.open("wb") as f:
            pickle.dump(value, f, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp, p)
